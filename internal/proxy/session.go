package proxy

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"pgrollback/internal/proxy/gui"
)

// BackendStartupCache holds ParameterStatus and BackendKeyData from the real PostgreSQL
// so we can replay them to clients when they connect to pgrollback instead of hardcoded values.
type BackendStartupCache struct {
	ParameterStatuses []pgproto3.ParameterStatus // order preserved for consistent client behavior
	BackendKeyData    pgproto3.BackendKeyData
}

// Well-known parameter names that PostgreSQL sends after connection (we copy these from the real server).
var backendStartupParameterNames = []string{
	"server_version", "server_encoding", "client_encoding", "DateStyle", "TimeZone", "session_authorization",
	"integer_datetimes", "standard_conforming_strings", "application_name", "default_transaction_read_only",
	"intervalStyle", "is_superuser",
}

type TestSession struct {
	DB                  *realSessionDB // abstraction over connection + transaction; use DB.Query/Exec for all commands
	TestID              string
	CreatedAt           time.Time
	LastActivity        time.Time
	DisconnectRequested bool
	ctx                 context.Context
	cancel              context.CancelFunc
	mu                  sync.RWMutex

	// teardown groups all session-destruction synchronization and connection tracking.
	teardown sessionTeardownState
}

// sessionTeardownState centralizes per-session teardown coordination.
// It keeps active proxy clients and synchronization to safely:
//  1. block new clients while destroying,
//  2. close all current clients,
//  3. wait for their cleanup loops to finish.
type sessionTeardownState struct {
	clientConns map[net.Conn]struct{}
	clientsWG   sync.WaitGroup
	destroying  bool
	destroyWait chan struct{} // closed when destroying finishes; GetOrCreateSession waits on it
}

// registerClientLocked tracks a proxy TCP client unless destroy is in progress.
// Caller must hold session.mu.
func (t *sessionTeardownState) registerClientLocked(c net.Conn) bool {
	if t.destroying {
		return false
	}
	t.clientsWG.Add(1)
	if t.clientConns == nil {
		t.clientConns = make(map[net.Conn]struct{})
	}
	t.clientConns[c] = struct{}{}
	return true
}

// unregisterClientLocked removes a client from tracking and releases one wait-group slot.
// Caller must hold session.mu.
func (t *sessionTeardownState) unregisterClientLocked(c net.Conn) {
	delete(t.clientConns, c)
	t.clientsWG.Done()
}

// destroyWaitChanLocked returns the current destroy wait channel (or nil).
// Caller must hold session.mu.
func (t *sessionTeardownState) destroyWaitChanLocked() chan struct{} {
	if !t.destroying {
		return nil
	}
	return t.destroyWait
}

// beginDestroyLocked marks destroy in progress, ensures wait channel exists,
// and returns a snapshot of currently tracked clients to close.
// Caller must hold session.mu.
func (t *sessionTeardownState) beginDestroyLocked() []net.Conn {
	if t.destroyWait == nil {
		t.destroyWait = make(chan struct{})
	}
	t.destroying = true
	conns := make([]net.Conn, 0, len(t.clientConns))
	for c := range t.clientConns {
		conns = append(conns, c)
	}
	return conns
}

// waitClients blocks until all tracked clients have unregistered.
func (t *sessionTeardownState) waitClients() {
	t.clientsWG.Wait()
}

// finishDestroyLocked clears destroy state and returns the wait channel to close.
// Caller must hold session.mu.
func (t *sessionTeardownState) finishDestroyLocked() chan struct{} {
	t.destroying = false
	ch := t.destroyWait
	t.destroyWait = nil
	return ch
}

type PgRollback struct {
	SessionsByTestID  map[string]*TestSession
	PostgresHost      string
	PostgresPort      int
	PostgresDB        string
	PostgresUser      string
	PostgresPass      string
	Timeout           time.Duration
	SessionTimeout    time.Duration
	KeepaliveInterval time.Duration // intervalo de ping pgrollback->PostgreSQL por conexão; 0 = desligado
	mu                sync.RWMutex

	// backendStartupCache is filled from the first real PostgreSQL connection and replayed to clients.
	backendStartupCache *BackendStartupCache

	// Events broadcasts session/query lifecycle events to GUI subscribers (e.g. the SSE stream).
	Events *gui.Hub

	lockInspector     *lockInspector
	lockInspectorOnce sync.Once

	// lockPoller refreshes cached LockStatus in the background (lock_status_poller.go), off the
	// query-execution and session-creation paths.
	lockPollerOnce     sync.Once
	lockPollerStop     chan struct{}
	lockPollerStopOnce sync.Once
}

// GetLastQueryDuration returns the last query execution duration (e.g. "12.345ms") for GUI, derived from the last history entry.
func (s *TestSession) GetLastQueryDuration() string {
	if s.DB == nil {
		return ""
	}
	return s.DB.Gui.GetLastQueryDuration()
}

// GUIQueryHistory returns a copy of the session query log in the same order as the GUI list (oldest first).
func (s *TestSession) GUIQueryHistory() []QueryHistoryEntry {
	if s == nil || s.DB == nil {
		return nil
	}
	return s.DB.Gui.GetQueryHistory()
}

// GUILastQuery returns the text of the last logged query (GUI “last query” column).
func (s *TestSession) GUILastQuery() string {
	if s == nil || s.DB == nil {
		return ""
	}
	return s.DB.Gui.GetLastQuery()
}

// ClearGUIQueryHistory clears the query log, same effect as POST /api/sessions/clear-history for this session.
func (s *TestSession) ClearGUIQueryHistory() {
	if s == nil || s.DB == nil {
		return
	}
	s.DB.Gui.ClearQueryHistory()
}

func (p *PgRollback) GetTestID(session *TestSession) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for testID, s := range p.SessionsByTestID {
		if s == session {
			return testID
		}
	}
	return ""
}

// MarkDisconnectRequested marks this session so that runDisconnectCleanup will fully destroy it
// (rollback transaction, close connection, remove from map) when the client disconnects.
func (s *TestSession) MarkDisconnectRequested() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.DisconnectRequested = true
}

// ShouldDisconnectOnCleanup reports whether this session was marked for destruction on disconnect.
func (s *TestSession) ShouldDisconnectOnCleanup() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ShouldDisconnectOnCleanupLocked()
}

func (s *TestSession) ShouldDisconnectOnCleanupLocked() bool {
	return s.DisconnectRequested
}

// Context returns the session's context, or context.Background() if not initialized.
func (s *TestSession) Context() context.Context {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

// Cancel cancels the session's context (if any). Safe to call multiple times.
func (s *TestSession) Cancel() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.CancelLocked()
}

func (s *TestSession) CancelLocked() {
	if s.cancel != nil {
		s.cancel()
		// Leave ctx as-is; it remains canceled and useful for checks.
	}
}

// registerProxyClient records this TCP client for teardown ordering. Returns false while the session
// is being destroyed so new proxy clients are rejected.
func (s *TestSession) registerProxyClient(c net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.teardown.registerClientLocked(c)
}

// unregisterProxyClient removes a client from the session; must pair with a successful registerProxyClient.
func (s *TestSession) unregisterProxyClient(c net.Conn) {
	s.mu.Lock()
	s.teardown.unregisterClientLocked(c)
	s.mu.Unlock()
}

func NewPgRollback(postgresHost string, postgresPort int, postgresDB, postgresUser, postgresPass string, timeout time.Duration, sessionTimeout time.Duration, keepaliveInterval time.Duration) *PgRollback {
	p := &PgRollback{
		SessionsByTestID:  make(map[string]*TestSession),
		PostgresHost:      postgresHost,
		PostgresPort:      postgresPort,
		PostgresDB:        postgresDB,
		PostgresUser:      postgresUser,
		PostgresPass:      postgresPass,
		Timeout:           timeout,
		SessionTimeout:    sessionTimeout,
		KeepaliveInterval: keepaliveInterval,
		Events:            gui.NewHub(),
	}
	p.startLockStatusPoller()
	return p
}

// SessionInfoFor builds the GUI snapshot for one session (same shape as GetSessions' per-session
// entry), including a live pg_locks/pg_stat_activity lookup. Returns false if the session does
// not exist. Safe to call while a query is running on that session (no field read here takes the
// query-execution lock), but it does a synchronous Postgres round trip — do not call this from
// the query-execution hot path or session creation (use sessionInfoWithoutLockStatus there, which
// reads the cache lock_status_poller.go keeps warm instead).
func (p *PgRollback) SessionInfoFor(testID string) (gui.SessionInfo, bool) {
	info, ok := p.sessionInfoWithoutLockStatus(testID)
	if !ok {
		return gui.SessionInfo{}, false
	}
	p.enrichLockStatusForSession(&info)
	return info, true
}

// sessionInfoWithoutLockStatus builds SessionInfo entirely from in-memory state: no live
// pg_locks/pg_stat_activity query. LockStatus comes from the cache lock_status_poller.go keeps
// warm (may lag reality by up to one poll interval). Used by the query-execution hot path
// (PublishSessionUpdate) and session creation (PublishSnapshot), which must never block on
// Postgres just to log a query or admit a new connection; GetSessions() overlays a fresh batched
// live lookup on top via enrichSessionsLockStatus for its own response.
func (p *PgRollback) sessionInfoWithoutLockStatus(testID string) (gui.SessionInfo, bool) {
	session := p.GetSession(testID)
	if session == nil {
		return gui.SessionInfo{}, false
	}
	inTransaction := false
	lastQuery := ""
	running := false
	var queryHistory []gui.QueryHistoryItem
	var lockStatus *gui.LockStatus
	lastQueryDuration := session.GetLastQueryDuration()
	if session.DB != nil {
		inTransaction = session.DB.HasOpenUserTransaction()
		lastQuery = session.DB.Gui.GetLastQuery()
		entries := session.DB.Gui.GetQueryHistory()
		queryHistory = make([]gui.QueryHistoryItem, len(entries))
		for i, e := range entries {
			queryHistory[i] = gui.QueryHistoryItem{Query: e.Query, At: e.At.Format(time.RFC3339), Duration: e.Duration, Running: e.Running}
		}
		if n := len(queryHistory); n > 0 {
			running = queryHistory[n-1].Running
		}
		lockStatus = session.DB.Gui.CachedLockStatus()
	}
	return gui.SessionInfo{
		TestID:            testID,
		InTransaction:     inTransaction,
		LockStatus:        lockStatus,
		LastQuery:         lastQuery,
		LastQueryDuration: lastQueryDuration,
		Running:           running,
		QueryHistory:      queryHistory,
	}, true
}

// PublishSessionUpdate emits an EventSessionUpdate for testID (e.g. query started/finished,
// history cleared). No-op if the session no longer exists or Events is unset (e.g. in tests).
// Deliberately in-memory only (sessionInfoWithoutLockStatus, not SessionInfoFor): this runs
// synchronously inline in the query-execution path for every query, so it must never do a
// Postgres round trip.
func (p *PgRollback) PublishSessionUpdate(testID string) {
	if p.Events == nil {
		return
	}
	info, ok := p.sessionInfoWithoutLockStatus(testID)
	if !ok {
		return
	}
	p.Events.Publish(gui.Event{Type: gui.EventSessionUpdate, Session: &info})
}

// PublishSnapshot emits an EventSnapshot with the full current session list (e.g. session
// created/destroyed). No-op if Events is unset (e.g. in tests). In-memory only, same reasoning as
// PublishSessionUpdate: GetOrCreateSession calls this synchronously while admitting a new client.
func (p *PgRollback) PublishSnapshot() {
	if p.Events == nil {
		return
	}
	sessions := p.GetAllSessions()
	list := make([]gui.SessionInfo, 0, len(sessions))
	for testID := range sessions {
		if info, ok := p.sessionInfoWithoutLockStatus(testID); ok {
			list = append(list, info)
		}
	}
	p.Events.Publish(gui.Event{Type: gui.EventSnapshot, Sessions: list})
}

// GetOrCreateSession obtém uma sessão existente ou cria uma nova para o testID
//
// Comportamento de Reutilização:
// - Se já existe sessão para este testID: retorna a sessão existente
//   - A conexão PostgreSQL da sessão é reutilizada (SessionsByTestID[testID].DB)
//   - A transação PostgreSQL continua ativa (não foi commitada nem rollback)
//
// - Se não existe: cria nova sessão
//   - Cria nova conexão PostgreSQL e guarda na sessão
//   - Inicia nova transação na conexão
//
// IMPORTANTE: O mesmo testID sempre usa a mesma conexão porque há apenas uma sessão por testID,
// e a sessão guarda sua DB (connection + transaction). Tudo fica sob TestSession, indexado por testID.
func (p *PgRollback) GetOrCreateSession(testID string) (*TestSession, error) {
	if testID == "" {
		return nil, fmt.Errorf("testID is required")
	}
	for {
		p.mu.Lock()
		session := p.SessionsByTestID[testID]
		waitCh := p.waitForDestroyIfInProgress(session)
		if waitCh != nil {
			p.mu.Unlock()
			<-waitCh
			continue
		}
		if session != nil {
			session.mu.Lock()
			reusable := p.tryReuseSessionLocked(testID, session)
			session.mu.Unlock()
			if reusable != nil {
				p.mu.Unlock()
				return reusable, nil
			}
			// Session existed but is not reusable (closed/broken); retry create path in this loop.
			p.mu.Unlock()
			continue
		}
		newSession, err := p.createAndStoreSessionLocked(testID)
		p.mu.Unlock()
		if err != nil {
			return nil, err
		}
		p.PublishSnapshot()
		return newSession, nil
	}
}

// waitForDestroyIfInProgress returns a channel to wait on when session teardown is in progress.
// Caller must hold p.mu.
func (p *PgRollback) waitForDestroyIfInProgress(session *TestSession) chan struct{} {
	if session == nil {
		return nil
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	return p.waitForDestroyIfInProgressLocked(session)
}

// waitForDestroyIfInProgressLocked checks destroy state without taking session locks.
// Caller must hold p.mu and session.mu.
func (p *PgRollback) waitForDestroyIfInProgressLocked(session *TestSession) chan struct{} {
	return session.teardown.destroyWaitChanLocked()
}

// tryReuseSessionLocked updates activity and returns session when it is usable.
// If unusable, removes it from map and returns nil.
// Caller must hold both p.mu and session.mu.
func (p *PgRollback) tryReuseSessionLocked(testID string, session *TestSession) *TestSession {
	session.LastActivity = time.Now()
	if session.DB != nil && session.DB.PgConnLocked() != nil && !session.DB.PgConnLocked().IsClosed() {
		return session
	}
	delete(p.SessionsByTestID, testID)
	return nil
}

// createAndStoreSessionLocked creates a new session and stores it in map.
// Caller must hold p.mu.
func (p *PgRollback) createAndStoreSessionLocked(testID string) (*TestSession, error) {
	newSession, err := p.createNewSessionLocked(testID)
	if err != nil {
		return nil, err
	}
	p.SessionsByTestID[testID] = newSession
	return newSession, nil
}

func (p *PgRollback) GetSession(testID string) *TestSession {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.GetSessionLocked(testID)
}

func (p *PgRollback) GetSessionLocked(testID string) *TestSession {
	return p.SessionsByTestID[testID]
}

func (p *PgRollback) GetAllSessions() map[string]*TestSession {
	p.mu.RLock()
	defer p.mu.RUnlock()

	sessions := make(map[string]*TestSession)
	for k, v := range p.SessionsByTestID {
		sessions[k] = v
	}
	return sessions
}

// createNewSessionLocked cria uma nova sessão para o testID.
// Só é chamada quando não existe sessão para este testID; a conexão fica na sessão.
func (p *PgRollback) createNewSessionLocked(testID string) (*TestSession, error) {
	if testID == "" {
		return nil, fmt.Errorf("testID is required to create a new session")
	}

	conn, err := newConnectionForTestID(p.PostgresHost, p.PostgresPort, p.PostgresDB, p.PostgresUser, p.PostgresPass, p.SessionTimeout, testID)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection for testID %s: %w", testID, err)
	}

	// Inicia nova transação na conexão
	// IMPORTANTE: Mesmo se reutilizamos a conexão, sempre criamos nova transação
	// A transação anterior (se existia) deve ter sido commitada ou rollback
	ctx, cancel := context.WithCancel(context.Background())
	tx, err := conn.Begin(ctx)
	if err != nil {
		cancel()
		conn.Close(ctx)
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	//#TODO (IA MANTER ISSO AQUI) Danilo Quero revisar pra usar o keep interval
	db := newSessionDB(conn, tx, ctx)
	p.fillBackendStartupCacheIfNeeded(db.PgConn())
	//if p.KeepaliveInterval > 0 {
	//	db.startKeepalive(p.KeepaliveInterval)
	//}

	session := &TestSession{
		DB:           db,
		CreatedAt:    time.Now(),
		LastActivity: time.Now(),
		ctx:          ctx,
		cancel:       cancel,
	}

	return session, nil
}

// fillBackendStartupCacheIfNeeded copies ParameterStatus from the real PostgreSQL connection into the
// cache so we can replay them to clients. Called when creating a new session; only fills once.
// BackendKeyData is not exposed by pgx PgConn, so we keep a default value.
func (p *PgRollback) fillBackendStartupCacheIfNeeded(pgConn *pgconn.PgConn) {
	if pgConn == nil {
		return
	}
	if p.backendStartupCache != nil {
		return
	}
	var params []pgproto3.ParameterStatus
	for _, name := range backendStartupParameterNames {
		value := pgConn.ParameterStatus(name)
		if value != "" {
			params = append(params, pgproto3.ParameterStatus{Name: name, Value: value})
		}
	}
	p.backendStartupCache = &BackendStartupCache{
		ParameterStatuses: params,
		BackendKeyData:    pgproto3.BackendKeyData{ProcessID: 12345, SecretKey: 67890}, // pgx does not expose; keep default
	}
}

// GetBackendStartupCache returns the cached backend startup messages from the real PostgreSQL, or nil if not yet filled.
func (p *PgRollback) GetBackendStartupCache() *BackendStartupCache {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.backendStartupCache
}

// isConnClosedOrFatal indica se o erro significa que a conexão com o PostgreSQL já está morta.
// Nesse caso, DestroySession pode tratar como sucesso ao remover a sessão do mapa.
func isConnClosedOrFatal(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Message == "conn closed" {
		return true
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "conn closed") || strings.Contains(s, "connection reset") ||
		strings.Contains(s, "broken pipe") || strings.Contains(s, "connection refused") ||
		strings.Contains(s, "unexpected eof")
}

// DestroySession destrói completamente uma sessão: faz rollback da transação,
// fecha a conexão da sessão e remove do mapa.
// Se a conexão com o PostgreSQL já estiver morta (ex.: timeout), remove a sessão
// do estado e retorna nil para o cliente ter sucesso (a tarefa "encerrar sessão" foi cumprida).
func (p *PgRollback) DestroySession(testID string) error {
	p.mu.Lock()
	session, exists := p.SessionsByTestID[testID]
	p.mu.Unlock()
	if !exists {
		return fmt.Errorf("session not found for test_id: %s", testID)
	}
	return p.destroySessionCore(session, testID)
}

// destroySessionIgnoreNotFound tears down a session if it is still in the map (used by idle cleanup).
func (p *PgRollback) destroySessionIgnoreNotFound(testID string) error {
	p.mu.Lock()
	session, ok := p.SessionsByTestID[testID]
	p.mu.Unlock()
	if !ok {
		return nil
	}
	return p.destroySessionCore(session, testID)
}

// destroySessionCore closes all proxy TCP clients for the session, waits until their message loops
// (including disconnect cleanup) finish, then closes the shared backend and removes the session.
// Caller must not hold p.mu or session.mu. Safe to call concurrently for the same session; only one
// teardown succeeds.
func (p *PgRollback) destroySessionCore(session *TestSession, testID string) error {
	p.mu.Lock()
	if !p.beginDestroySessionMapGateLocked(session, testID) {
		p.mu.Unlock()
		return nil
	}
	session.mu.Lock()
	conns, done := p.beginDestroySessionBodyLocked(session, testID)
	session.mu.Unlock()
	p.mu.Unlock()

	if done {
		return nil
	}

	for _, c := range conns {
		_ = c.Close()
	}

	session.teardown.waitClients()

	p.mu.Lock()
	curSession := p.SessionsByTestID[testID]
	session.mu.Lock()
	destroyWaitCh, err := p.finishDestroySessionLocked(session, curSession, testID)
	session.mu.Unlock()
	p.mu.Unlock()

	if destroyWaitCh != nil {
		close(destroyWaitCh)
	}
	p.PublishSnapshot()
	return err
}

// destroySessionCoreWithPLock performs destroy while caller currently holds p.mu.
// It temporarily releases p.mu around blocking I/O/wait phases, then re-acquires it.
func (p *PgRollback) destroySessionCoreWithPLock(session *TestSession, testID string) error {
	if !p.beginDestroySessionMapGateLocked(session, testID) {
		return nil
	}
	session.mu.Lock()
	conns, done := p.beginDestroySessionBodyLocked(session, testID)
	session.mu.Unlock()
	if done {
		return nil
	}

	p.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	session.teardown.waitClients()
	p.mu.Lock()

	curSession := p.SessionsByTestID[testID]
	session.mu.Lock()
	destroyWaitCh, err := p.finishDestroySessionLocked(session, curSession, testID)
	session.mu.Unlock()
	if destroyWaitCh != nil {
		close(destroyWaitCh)
	}
	return err
}

// beginDestroySessionMapGateLocked reports whether session is still the map entry for testID.
// Caller must hold p.mu. Does not lock or unlock any mutex.
func (p *PgRollback) beginDestroySessionMapGateLocked(session *TestSession, testID string) bool {
	curSession, ok := p.SessionsByTestID[testID]
	return ok && curSession == session
}

// beginDestroySessionBodyLocked starts teardown for session (snapshot conns, cancel ctx).
// Returns (connections to close, done=true) when no further work is needed (e.g. DB already nil).
// Caller must hold p.mu and session.mu. Does not lock or unlock any mutex.
func (p *PgRollback) beginDestroySessionBodyLocked(session *TestSession, testID string) ([]net.Conn, bool) {
	if session.DB == nil {
		delete(p.SessionsByTestID, testID)
		return nil, true
	}

	conns := session.teardown.beginDestroyLocked()
	session.CancelLocked()
	return conns, false
}

// finishDestroySessionLocked closes backend DB and removes the session after clients finished.
// Caller must hold p.mu and oldSession.mu.
// It returns the destroy-wait channel that caller must close after releasing oldSession.mu.
func (p *PgRollback) finishDestroySessionLocked(oldSession, curSession *TestSession, testID string) (chan struct{}, error) {
	// Between beginDestroy and finishDestroy we release p.mu and wait for proxy clients.
	// During this window, another goroutine may install a new session for testID.
	// In this case, oldSession teardown must finish, but we must not close/remove curSession.
	if curSession != oldSession {
		return p.signalDestroyWaitersLocked(oldSession), nil
	}

	if oldSession.DB == nil {
		delete(p.SessionsByTestID, testID)
		return p.signalDestroyWaitersLocked(oldSession), nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := oldSession.DB.close(ctx)
	if err != nil {
		return p.signalDestroyWaitersLocked(oldSession), fmt.Errorf("failed to close session: '%s': %w", testID, err)
	}

	oldSession.DB = nil
	delete(p.SessionsByTestID, testID)
	return p.signalDestroyWaitersLocked(oldSession), nil
}

func (p *PgRollback) signalDestroyWaiters(session *TestSession) {
	session.mu.Lock()
	ch := p.signalDestroyWaitersLocked(session)
	session.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

// signalDestroyWaitersLocked resets destroy flags and returns wait channel to be closed by caller.
// Caller must hold session.mu.
func (p *PgRollback) signalDestroyWaitersLocked(session *TestSession) chan struct{} {
	return session.teardown.finishDestroyLocked()
}

// RollbackBaseTransaction runs ROLLBACK and begins a new transaction on the session (used by "pgrollback rollback").
func (p *PgRollback) RollbackBaseTransaction(testID string) (string, error) {
	session := p.GetSession(testID)
	if session == nil {
		return "", fmt.Errorf("session not found for test_id: '%s'", testID)
	}
	return session.RollbackBaseTransaction(testID)
}

// RollbackSession é um alias para DestroySession mantido para compatibilidade.
// Deprecated: Use DestroySession em vez disso.
func (p *PgRollback) RollbackSession(testID string) error {
	return p.DestroySession(testID)
}

// sessionIdleExpired reports whether idle time since LastActivity exceeds p.Timeout.
// Caller must hold session.mu (read or write lock).
func (p *PgRollback) sessionIdleExpired(session *TestSession, timeToConsider time.Time) bool {
	return timeToConsider.Sub(session.LastActivity) > p.Timeout
}

func (p *PgRollback) CleanupExpiredSessions() (int, error) {
	now := time.Now()
	var expiredIDs []string
	p.mu.Lock()
	for testID, session := range p.SessionsByTestID {
		session.mu.RLock()
		expired := p.sessionIdleExpired(session, now)
		session.mu.RUnlock()
		if expired {
			expiredIDs = append(expiredIDs, testID)
		}
	}
	p.mu.Unlock()

	cleaned := 0
	for _, testID := range expiredIDs {
		if err := p.destroySessionIgnoreNotFound(testID); err != nil {
			return cleaned, err
		}
		cleaned++
	}
	return cleaned, nil
}

func (p *PgRollback) getAdvisoryLockKey(session *TestSession) int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	hash := fnv.New64a()
	hash.Write([]byte("pgrollback_" + p.GetTestID(session)))
	return int64(hash.Sum64())
}

func (p *PgRollback) acquireAdvisoryLock(session *TestSession) error {
	if session.DB == nil {
		return fmt.Errorf("session DB is nil for session %s", p.GetTestID(session))
	}
	lockKey := p.getAdvisoryLockKey(session)
	return session.DB.acquireAdvisoryLock(session.Context(), lockKey)
}

func (p *PgRollback) releaseAdvisoryLock(session *TestSession) error {
	if session.DB == nil {
		return fmt.Errorf("session DB is nil for session %s", p.GetTestID(session))
	}
	lockKey := p.getAdvisoryLockKey(session)
	return session.DB.releaseAdvisoryLock(session.Context(), lockKey)
}

// ExecuteWithLock runs query on the session's shared backend transaction while holding a per-test_id
// PostgreSQL advisory lock. The query is executed via SafeExec (guard savepoint) so a SQL error does
// not abort the outer transaction — the next advisory lock / proxy command still sees a healthy tx.
func (p *PgRollback) ExecuteWithLock(session *TestSession, query string) error {
	if session.DB == nil {
		return fmt.Errorf("session DB is nil for session %s", p.GetTestID(session))
	}
	if err := p.acquireAdvisoryLock(session); err != nil {
		return fmt.Errorf("failed to acquire advisory lock: %w", err)
	}
	defer p.releaseAdvisoryLock(session)

	session.mu.Lock()
	session.LastActivity = time.Now()
	session.mu.Unlock()

	_, err := session.DB.SafeExec(session.Context(), query)
	return err
}

// handleBegin converte BEGIN em SAVEPOINT
//
// Comportamento:
// - Se não houver transação base, cria uma primeiro (garantia de segurança)
// - Cada BEGIN cria um novo savepoint, permitindo rollback aninhado
// - O primeiro BEGIN (SavepointLevel = 0) marca o "ponto de início" desta conexão/cliente
// - Savepoints subsequentes permitem rollback parcial dentro da mesma conexão
//
// Caso de uso PHP:
// - PHP conecta → executa BEGIN → cria savepoint pgrollback_v_1 (ponto de início)
// - PHP faz comandos → executa BEGIN novamente → cria savepoint pgrollback_v_2
// - PHP executa ROLLBACK → faz rollback até pgrollback_v_2 (não afeta pgrollback_v_1)
// - PHP desconecta → próxima conexão PHP com mesmo testID pode continuar de onde parou
func (s *TestSession) handleBegin(testID string, connID ConnectionID) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.DB == nil {
		return "", fmt.Errorf("Begin TestSession has no connection to DB on ID: %s", testID)
	}
	return s.DB.handleBegin(testID, connID)
}

// handleCommit converte COMMIT em RELEASE SAVEPOINT
func (s *TestSession) handleCommit(testID string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.DB == nil {
		return "", fmt.Errorf("Commit TestSession has no connection to DB on ID: %s", testID)
	}
	return s.DB.handleCommit(testID)
}

// handleRollback converte ROLLBACK em ROLLBACK TO SAVEPOINT
//
// Comportamento:
// - Se SavepointLevel > 0: faz rollback até o último savepoint e o remove
// - Se SavepointLevel = 0: não há savepoints para reverter, apenas retorna sucesso
//
// Caso de uso PHP:
// - PHP executa ROLLBACK → reverte até o último savepoint criado por esta conexão
// - Isso permite que cada conexão/cliente tenha seu próprio rollback isolado
// - O rollback não afeta outras conexões que compartilham a mesma sessão/testID
func (s *TestSession) handleRollback(testID string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.DB == nil {
		return "", fmt.Errorf("Rollback TestSession has no connection to DB on ID: %s", testID)
	}
	return s.DB.handleRollback(testID)
}

// RollbackBaseTransaction runs ROLLBACK and begins a new transaction on the session (used by "pgrollback rollback").
// Returns FULLROLLBACK_SENTINEL so the proxy sends exactly one CommandComplete+ReadyForQuery without
// forwarding to the DB, avoiding response attribution issues with the next query (e.g. ResetSession ping).
func (s *TestSession) RollbackBaseTransaction(testID string) (string, error) {
	return FULLROLLBACK_SENTINEL, s.DB.startNewTx(s.Context())
}

// buildStatusResultSet constrói uma query SELECT para status de uma sessão
func (s *TestSession) buildStatusResultSet(testID string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.DB == nil {
		return "", fmt.Errorf("Session with testID '%s', doesnt have a real connection.", testID)
	}
	return s.DB.buildStatusResultSet(s.CreatedAt, testID)
}
