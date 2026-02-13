package proxy

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// BackendStartupCache holds ParameterStatus and BackendKeyData from the real PostgreSQL
// so we can replay them to clients when they connect to pgtest instead of hardcoded values.
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
	DB           *realSessionDB // abstraction over connection + transaction; use DB.Query/Exec for all commands
	TestID       string
	CreatedAt    time.Time
	LastActivity time.Time
	mu           sync.RWMutex
}

type PGTest struct {
	SessionsByTestID  map[string]*TestSession
	PostgresHost      string
	PostgresPort      int
	PostgresDB        string
	PostgresUser      string
	PostgresPass      string
	Timeout           time.Duration
	SessionTimeout    time.Duration
	KeepaliveInterval time.Duration // intervalo de ping pgtest->PostgreSQL por conexão; 0 = desligado
	mu                sync.RWMutex

	// backendStartupCache is filled from the first real PostgreSQL connection and replayed to clients.
	backendStartupCache *BackendStartupCache
}

func (p *PGTest) GetTestID(session *TestSession) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for testID, s := range p.SessionsByTestID {
		if s == session {
			return testID
		}
	}
	return ""
}

func NewPGTest(postgresHost string, postgresPort int, postgresDB, postgresUser, postgresPass string, timeout time.Duration, sessionTimeout time.Duration, keepaliveInterval time.Duration) *PGTest {
	return &PGTest{
		SessionsByTestID:  make(map[string]*TestSession),
		PostgresHost:      postgresHost,
		PostgresPort:      postgresPort,
		PostgresDB:        postgresDB,
		PostgresUser:      postgresUser,
		PostgresPass:      postgresPass,
		Timeout:           timeout,
		SessionTimeout:    sessionTimeout,
		KeepaliveInterval: keepaliveInterval,
	}
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
func (p *PGTest) GetOrCreateSession(testID string) (*TestSession, error) {
	p.mu.Lock()
	defer func() {
		p.mu.TryLock()
		p.mu.Unlock()
	}()

	// Reutiliza sessão existente se disponível
	// Isso significa que estamos reutilizando a conexão PostgreSQL e a transação
	if session, exists := p.SessionsByTestID[testID]; exists {
		session.mu.Lock()
		session.LastActivity = time.Now()
		session.mu.Unlock()
		// Verifica se a conexão ainda está válida
		if session.DB == nil || session.DB.PgConn() == nil {
			// Remove sessão inválida e cria nova
			delete(p.SessionsByTestID, testID)
		} else {
			return session, nil
		}
	}
	p.mu.Unlock()

	// Cria nova sessão para este testID (conexão fica na sessão)
	session, err := p.createNewSession(testID)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.SessionsByTestID[testID] = session
	p.mu.Unlock()
	return session, nil
}

func (p *PGTest) GetSession(testID string) *TestSession {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.SessionsByTestID[testID]
}

func (p *PGTest) GetAllSessions() map[string]*TestSession {
	p.mu.RLock()
	defer p.mu.RUnlock()

	sessions := make(map[string]*TestSession)
	for k, v := range p.SessionsByTestID {
		sessions[k] = v
	}
	return sessions
}

// createNewSession cria uma nova sessão para o testID.
// Só é chamada quando não existe sessão para este testID; a conexão fica na sessão.
func (p *PGTest) createNewSession(testID string) (*TestSession, error) {
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
	tx, err := conn.Begin(context.Background())
	if err != nil {
		conn.Close(context.Background())
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	db := newSessionDB(conn, tx)
	p.fillBackendStartupCacheIfNeeded(db.PgConn())
	//if p.KeepaliveInterval > 0 {
	//	db.startKeepalive(p.KeepaliveInterval)
	//}

	session := &TestSession{
		DB:           db,
		CreatedAt:    time.Now(),
		LastActivity: time.Now(),
	}

	return session, nil
}

// fillBackendStartupCacheIfNeeded copies ParameterStatus from the real PostgreSQL connection into the
// cache so we can replay them to clients. Called when creating a new session; only fills once.
// BackendKeyData is not exposed by pgx PgConn, so we keep a default value.
func (p *PGTest) fillBackendStartupCacheIfNeeded(pgConn *pgconn.PgConn) {
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
func (p *PGTest) GetBackendStartupCache() *BackendStartupCache {
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
func (p *PGTest) DestroySession(testID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	session, exists := p.SessionsByTestID[testID]
	if !exists {
		return fmt.Errorf("session not found for test_id: %s", testID)
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	if session.DB != nil {
		if err := session.DB.rollbackTx(context.Background()); err != nil && !isConnClosedOrFatal(err) {
			return fmt.Errorf("failed to rollback transaction: %w", err)
		}
		_ = session.DB.close(context.Background())
		session.DB = nil
	}

	delete(p.SessionsByTestID, testID)
	return nil
}

// RollbackBaseTransaction runs ROLLBACK and begins a new transaction on the session (used by "pgtest rollback").
func (p *PGTest) RollbackBaseTransaction(testID string) (string, error) {
	session := p.GetSession(testID)
	if session == nil {
		return "", fmt.Errorf("session not found for test_id: '%s'", testID)
	}
	return session.RollbackBaseTransaction(testID)
}

// RollbackSession é um alias para DestroySession mantido para compatibilidade.
// Deprecated: Use DestroySession em vez disso.
func (p *PGTest) RollbackSession(testID string) error {
	return p.DestroySession(testID)
}

func (p *PGTest) CleanupExpiredSessions() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	cleaned := 0

	for testID, session := range p.SessionsByTestID {
		session.mu.RLock()
		expired := now.Sub(session.LastActivity) > p.Timeout
		session.mu.RUnlock()

		if expired {
			session.mu.Lock()
			if session.DB != nil {
				_ = session.DB.rollbackTx(context.Background())
				_ = session.DB.close(context.Background())
				session.DB = nil
			}
			session.mu.Unlock()
			delete(p.SessionsByTestID, testID)
			cleaned++
		}
	}

	return cleaned
}

func (p *PGTest) getAdvisoryLockKey(session *TestSession) int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	hash := fnv.New64a()
	hash.Write([]byte("pgtest_" + p.GetTestID(session)))
	return int64(hash.Sum64())
}

func (p *PGTest) acquireAdvisoryLock(session *TestSession) error {
	if session.DB == nil {
		return fmt.Errorf("session DB is nil for session %s", p.GetTestID(session))
	}
	lockKey := p.getAdvisoryLockKey(session)
	return session.DB.acquireAdvisoryLock(context.Background(), lockKey)
}

func (p *PGTest) releaseAdvisoryLock(session *TestSession) error {
	if session.DB == nil {
		return fmt.Errorf("session DB is nil for session %s", p.GetTestID(session))
	}
	lockKey := p.getAdvisoryLockKey(session)
	return session.DB.releaseAdvisoryLock(context.Background(), lockKey)
}

func (p *PGTest) ExecuteWithLock(session *TestSession, query string) error {
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

	_, err := session.DB.Exec(context.Background(), query)
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
// - PHP conecta → executa BEGIN → cria savepoint pgtest_v_1 (ponto de início)
// - PHP faz comandos → executa BEGIN novamente → cria savepoint pgtest_v_2
// - PHP executa ROLLBACK → faz rollback até pgtest_v_2 (não afeta pgtest_v_1)
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

// RollbackBaseTransaction runs ROLLBACK and begins a new transaction on the session (used by "pgtest rollback").
// Returns FULLROLLBACK_SENTINEL so the proxy sends exactly one CommandComplete+ReadyForQuery without
// forwarding to the DB, avoiding response attribution issues with the next query (e.g. ResetSession ping).
func (s *TestSession) RollbackBaseTransaction(testID string) (string, error) {
	return FULLROLLBACK_SENTINEL, s.DB.startNewTx(context.Background())
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

//// GetSavepoints retorna a lista de savepoints da sessão
//func (s *TestSession) GetSavepoints() []string {
//	s.mu.RLock()
//	defer s.mu.RUnlock()
//	// Retorna uma cópia para evitar modificações externas
//	result := make([]string, len(s.Savepoints))
//	copy(result, s.Savepoints)
//	return result
//}
