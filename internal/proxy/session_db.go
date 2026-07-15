package proxy

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"pgrollback/internal/proxy/gui"
	sqlpkg "pgrollback/pkg/sql"
)

// ConnectionID is an opaque identifier for a proxy connection, used to allow
// nested BEGIN (same connection) while rejecting BEGIN from a different connection.
type ConnectionID = uintptr

// ErrOnlyOneTransactionAtATime is returned when a second connection tries to BEGIN while another already has an open user transaction on the same session.
var ErrOnlyOneTransactionAtATime = errors.New("only one transaction could start a transaction at a time on our pgrollback")

// guiState holds GUI-observable session fields with its own RWMutex.
// All methods are self-contained (acquire/release the lock internally),
// so callers never need to worry about which lock to hold.
// Safe to call while the parent realSessionDB.mu is held (lock ordering: mu → Gui.mu).
type guiState struct {
	mu           sync.RWMutex
	queryHistory []QueryHistoryEntry
	running      int
	// lockStatus is the most recently known result of a live pg_locks/pg_stat_activity lookup,
	// written by the background poller (lock_status_poller.go) or an on-demand GetSessions() call.
	// Query-hot-path reads (PublishSessionUpdate/PublishSnapshot) use this instead of querying
	// Postgres themselves, so logging a query never blocks the query itself. May lag reality by
	// up to one poll interval; nil means "not waiting on a lock" (or not yet computed).
	lockStatus *gui.LockStatus
}

// realSessionDB encapsulates the PostgreSQL connection and its active transaction.
//
// Lock design:
//   - mu is the main lock: protects conn, tx, SavepointLevel, stopKeepalive, and serializes all SQL I/O.
//   - Gui has its own RWMutex protecting GUI-observable fields (queryHistory, running)
//     so GUI/status reads never block on running queries.
//   - Lock ordering when both are needed: mu first, then Gui.mu. Never the reverse.
type realSessionDB struct {
	conn                 *pgx.Conn
	tx                   pgx.Tx
	mu                   sync.RWMutex // main lock: conn/tx state + serializes SQL I/O
	Gui                  guiState     // GUI-observable state; see guiState doc
	SavepointLevel       int
	connectionWithOpenTx ConnectionID // which connection has the open user transaction; 0 when none (mu)
	inUserTx             atomic.Bool  // mirrors connectionWithOpenTx != 0; lets GUI reads skip mu entirely (see HasOpenUserTransaction)
	stopKeepalive        func()
	ctx                  context.Context
}

func (d *realSessionDB) GetSavepointLevel() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.SavepointLevel
}

// GetSavepointName returns the name for the current savepoint level. Caller must hold d.mu when level may be changing.
func (d *realSessionDB) GetSavepointName() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.getSavepointNameLocked()
}

// GetNextSavepointName returns the name for the next SAVEPOINT (current level + 1) without incrementing.
// Used by the interceptor so SavepointLevel is only incremented when the SAVEPOINT is actually executed.
func (d *realSessionDB) GetNextSavepointName() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.getNextSavepointNameLocked()
}

// IncrementSavepointLevel increments the savepoint level. Call only after a SAVEPOINT has been successfully executed.
func (d *realSessionDB) IncrementSavepointLevel() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.incrementSavepointLevelLocked()
}

// DecrementSavepointLevel decrements the savepoint level. Call only after a RELEASE SAVEPOINT or ROLLBACK TO SAVEPOINT has been successfully executed. No-op if level is already 0.
func (d *realSessionDB) DecrementSavepointLevel() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.decrementSavepointLevelLocked()
}

// DecrementSavepointLevel decrements the savepoint level. Call only after a RELEASE SAVEPOINT or ROLLBACK TO SAVEPOINT has been successfully executed. No-op if level is already 0.
func (d *realSessionDB) decrementSavepointLevelLocked() {
	if d.SavepointLevel > 0 {
		d.SavepointLevel--
	}
}

// getSavepointNameLocked returns the name for the current savepoint level. Caller must hold d.mu.
func (d *realSessionDB) getSavepointNameLocked() string {
	return fmt.Sprintf("pgrollback_v_%d", d.SavepointLevel)
}

// getNextSavepointNameLocked returns the name for the next SAVEPOINT (current level + 1) without incrementing. Caller must hold d.mu.
func (d *realSessionDB) getNextSavepointNameLocked() string {
	return fmt.Sprintf("pgrollback_v_%d", d.SavepointLevel+1)
}

// incrementSavepointLevelLocked increments the savepoint level. Caller must hold d.mu.
func (d *realSessionDB) incrementSavepointLevelLocked() {
	d.SavepointLevel++
}

// LockRun holds d.mu for the duration of using the backend outside SafeExec/SafeQuery/SafeExecTCL (e.g. PgConn().Exec). Unlock with UnlockRun.
func (d *realSessionDB) LockRun() {
	GlobalLockTraceRegistry().LockRWMutex(&d.mu)
}

// UnlockRun releases d.mu held by LockRun.
func (d *realSessionDB) UnlockRun() {
	if d == nil {
		return
	}
	GlobalLockTraceRegistry().UnlockRWMutex(&d.mu)
}

// execTxLocked runs a single SQL command on d.tx. Caller must hold d.mu (e.g. via LockRun).
// Used to run SAVEPOINT/ROLLBACK TO SAVEPOINT/RELEASE SAVEPOINT around a batch without releasing the lock.
func (d *realSessionDB) execTxLocked(ctx context.Context, sql string) (pgconn.CommandTag, error) {
	if !d.hasActiveTransactionLocked() {
		return pgconn.CommandTag{}, fmt.Errorf("no active transaction")
	}
	tag, err := d.tx.Exec(ctx, sql)
	return tag, err
}

// runWithSavepointGuardLocked wraps fn() in a SAVEPOINT/ROLLBACK TO SAVEPOINT/RELEASE SAVEPOINT
// guard so that a failure inside fn() does not leave the base transaction in an aborted state
// (SQLSTATE 25P02). Typical use: wrapping pgConn.Prepare() or any low-level backend call that
// runs directly through pgconn (bypassing SafeExec's own guard).
//
// If the SAVEPOINT cannot be created the function still calls fn() — best-effort behaviour so a
// temporary failure to create the savepoint does not block the operation entirely.
//
// Caller must hold d.mu (via LockRun).
func (d *realSessionDB) runWithSavepointGuardLocked(ctx context.Context, savepointName string, fn func() error) error {
	_, spErr := d.execTxLocked(ctx, "SAVEPOINT "+savepointName)
	guardActive := spErr == nil
	if !guardActive {
		log.Printf("[PROXY] Savepoint guard %q could not be created (running without guard): %v", savepointName, spErr)
	}

	fnErr := fn()

	if fnErr != nil {
		if guardActive {
			if _, rbErr := d.execTxLocked(ctx, "ROLLBACK TO SAVEPOINT "+savepointName); rbErr != nil {
				log.Printf("[PROXY] Failed to roll back savepoint guard %q: %v", savepointName, rbErr)
			} else if _, relErr := d.execTxLocked(ctx, "RELEASE SAVEPOINT "+savepointName); relErr != nil {
				log.Printf("[PROXY] Failed to release savepoint guard %q after rollback: %v", savepointName, relErr)
			}
		}
		return fnErr
	}

	if guardActive {
		if _, relErr := d.execTxLocked(ctx, "RELEASE SAVEPOINT "+savepointName); relErr != nil {
			log.Printf("[PROXY] Failed to release savepoint guard %q on success: %v", savepointName, relErr)
		}
	}
	return nil
}

// HasOpenUserTransaction returns true if a connection has started a user transaction (BEGIN)
// and not yet committed or rolled back. Reads inUserTx instead of taking mu, so GUI/status
// polling never blocks behind a running query (SafeQuery/SafeExec/SafeExecTCL hold mu for the
// whole query). inUserTx is kept in sync with connectionWithOpenTx at its only two write sites.
func (d *realSessionDB) HasOpenUserTransaction() bool {
	return d.inUserTx.Load()
}

// isTransactionHeldByOtherConnection returns true when a connection other than connID has the open transaction.
// Caller must hold d.mu.
func (d *realSessionDB) isTransactionHeldByOtherConnection(connID ConnectionID) bool {
	d.mu.RLock()
	answer := d.isTransactionHeldByOtherConnectionLocked(connID)
	d.mu.RUnlock()
	return answer
}

func (d *realSessionDB) isTransactionHeldByOtherConnectionLocked(connID ConnectionID) bool {
	return d.connectionWithOpenTx != 0 && d.connectionWithOpenTx != connID
}

// IsUserBeginQuery returns true when the query is a user BEGIN (SAVEPOINT pgrollback_v_*).
// Callers use this to decide whether to call ClaimOpenTransaction (e.g. before executing TCL).
func IsUserBeginQuery(query string) bool {
	stmts, err := sqlpkg.ParseStatements(query)
	if err != nil || len(stmts) == 0 || stmts[0].Stmt == nil {
		return false
	}
	return sqlpkg.IsSavepoint(stmts[0].Stmt) && strings.HasPrefix(sqlpkg.GetSavepointName(stmts[0].Stmt), pgrollbackSavepointPrefix)
}

// isUserReleaseQuery returns true when the query is a user COMMIT (RELEASE SAVEPOINT pgrollback_v_*).
func isUserReleaseQuery(query string) bool {
	stmts, err := sqlpkg.ParseStatements(query)
	if err != nil || len(stmts) == 0 || stmts[0].Stmt == nil {
		return false
	}
	return sqlpkg.IsReleaseSavepoint(stmts[0].Stmt) && strings.HasPrefix(sqlpkg.GetSavepointName(stmts[0].Stmt), pgrollbackSavepointPrefix)
}

// IsQueryThatAffectsClaim returns true when the query is one that claimed (BEGIN) or that would release (COMMIT).
// Callers use this to decide whether to call ReleaseOpenTransaction (e.g. on TCL failure).
func IsQueryThatAffectsClaim(query string) bool {
	return IsUserBeginQuery(query) || isUserReleaseQuery(query)
}

// ClaimOpenTransaction records that the given connection is starting a user transaction (BEGIN).
// Nested BEGIN on the same connection is allowed; returns ErrOnlyOneTransactionAtATime only
// when a different connection already has an open transaction.
func (d *realSessionDB) ClaimOpenTransaction(connID ConnectionID) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.isTransactionHeldByOtherConnectionLocked(connID) {
		return ErrOnlyOneTransactionAtATime
	}
	d.connectionWithOpenTx = connID
	d.inUserTx.Store(true)
	return nil
}

// ReleaseOpenTransaction clears the "one connection has open transaction" flag when the
// given connection is the one that had the claim.
func (d *realSessionDB) ReleaseOpenTransaction(connID ConnectionID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.releaseOpenTransactionLocked(connID)
}

// releaseOpenTransactionLocked clears the claim. Caller must hold d.mu.
func (d *realSessionDB) releaseOpenTransactionLocked(connID ConnectionID) {
	if d.connectionWithOpenTx == connID {
		d.connectionWithOpenTx = 0
		d.inUserTx.Store(false)
	}
}

// GetLastQuery returns the last executed query for this session (for GUI/status).
func (g *guiState) GetLastQuery() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if len(g.queryHistory) == 0 {
		return ""
	}
	return g.queryHistory[len(g.queryHistory)-1].Query
}

// Ensure realSessionDB implements pgxQueryer (used by tx_guard).
var _ pgxQueryer = (*realSessionDB)(nil)

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
func (d *realSessionDB) handleRollback(testID string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.SavepointLevel > 0 {
		savepointName := d.getSavepointNameLocked()
		return fmt.Sprintf("ROLLBACK TO SAVEPOINT %s; RELEASE SAVEPOINT %s", savepointName, savepointName), nil
	}

	return DEFAULT_SELECT_ONE, nil
}

func (d *realSessionDB) buildStatusResultSet(createdAt time.Time, testID string) (string, error) {
	d.mu.RLock()
	active := d.hasActiveTransactionLocked()
	level := d.SavepointLevel
	d.mu.RUnlock()

	return fmt.Sprintf(
		"SELECT '%s' AS test_id, %t AS active, %d AS level, '%s' AS created_at",
		testID, active, level, createdAt.Format(time.RFC3339),
	), nil
}

// Query runs a query in the current transaction. Returns an error if there is no active transaction.
func (d *realSessionDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if !d.hasActiveTransactionLocked() {
		return nil, fmt.Errorf("no active transaction: use BeginTx first")
	}
	return d.tx.Query(ctx, sql, args...)
}

func (d *realSessionDB) handleCommit(testID string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.SavepointLevel > 0 {
		savepointName := d.getSavepointNameLocked()
		return fmt.Sprintf("RELEASE SAVEPOINT %s", savepointName), nil
	}

	return DEFAULT_SELECT_ONE, nil
}

func (d *realSessionDB) handleBegin(testID string, connID ConnectionID) (string, error) {
	if !d.HasActiveTransaction() {
		return "", fmt.Errorf("no active transaction: use BeginTx first")
	}

	if connID != 0 {
		heldByOther := d.isTransactionHeldByOtherConnection(connID)
		if heldByOther {
			return "", ErrOnlyOneTransactionAtATime
		}
	}

	if err := d.beginTx(d.contextOrBackground()); err != nil {
		return "", fmt.Errorf("Failed to Begin a transaction: %w", err)
	}

	if d.GetSavepointLevel() >= 1 {
		return DEFAULT_SELECT_ONE, nil
	}
	// Return the next savepoint name without incrementing; level is incremented only when the SAVEPOINT is successfully executed (in query_handler).
	name := d.GetNextSavepointName()
	return fmt.Sprintf("SAVEPOINT %s", name), nil
}

// Exec runs a command in the current transaction. Returns an error if there is no active transaction.
func (d *realSessionDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if !d.hasActiveTransactionLocked() {
		var zero pgconn.CommandTag
		return zero, fmt.Errorf("no active transaction: use BeginTx first")
	}
	return d.tx.Exec(ctx, sql, args...)
}

// Danilo isso aqui é só pra ser usado no savepoint (o commit nele é tratado como releasepoint)
func commitSavePoint(ctx context.Context, savepoint pgx.Tx) {
	if savepoint == nil {
		return
	}
	err := savepoint.Commit(ctx)
	if err != nil {
		log.Fatalf("Failed to remove a savePoint")
	}
}

func (d *realSessionDB) SafeQuery(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	d.Gui.incRunningQueryCount()
	defer d.Gui.decRunningQueryCount()
	d.mu.Lock()
	defer d.mu.Unlock()
	savePoint, err := d.tx.Begin(ctx)
	if err != nil || savePoint == nil {
		return nil, fmt.Errorf("Falha ao iniciar savepoint de guarda: %w, sql: '''%s'''", err, sql)
	}
	rows, err := savePoint.Query(ctx, sql, args...)
	errList := []error{}
	if err != nil {
		errList = append(errList, fmt.Errorf("Falha ao executar consulta due to: %w", err))
		if rollbackErr := savePoint.Rollback(ctx); rollbackErr != nil {
			errList = append(errList, fmt.Errorf("Falha no rollback de guarda: %w", rollbackErr))
		}
	} /* else {
		if commitErr := savePoint.Commit(ctx); commitErr != nil {
			errList = append(errList, fmt.Errorf("Falha no commit de guarda: %w", commitErr))
		}
	}*/

	if len(errList) > 0 {
		errList = append(errList, fmt.Errorf("For sql: %s", sql))
		return nil, errors.Join(errList...)
	}
	return &guardedRows{
		Rows:      rows,
		ctx:       ctx,
		tx:        d.tx,
		savePoint: savePoint,
	}, nil
}

func (d *realSessionDB) SafeExec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	d.Gui.incRunningQueryCount()
	defer d.Gui.decRunningQueryCount()
	d.mu.Lock()
	defer d.mu.Unlock()
	savePoint, execErr := d.tx.Begin(ctx)
	if execErr != nil {
		return pgconn.CommandTag{}, fmt.Errorf("Falha ao iniciar savepoint de guarda: %w, pro sql '''%s'''", execErr, sql)
	}
	result, execErr := savePoint.Exec(ctx, sql, args...)
	if execErr != nil {
		if rbErr := savePoint.Rollback(ctx); rbErr != nil {
			return pgconn.CommandTag{}, errors.Join(
				fmt.Errorf("Safe exec failed: %w; sql=%q", execErr, sql),
				fmt.Errorf("Safe rollback failed: %w", rbErr),
			)
		}
		return pgconn.CommandTag{}, fmt.Errorf("Safe exec failed: %w, sql: '''%s'''", execErr, sql)
	}
	if commitErr := savePoint.Commit(ctx); commitErr != nil {
		if rbErr := savePoint.Rollback(ctx); rbErr != nil {
			return pgconn.CommandTag{}, errors.Join(
				fmt.Errorf("Safe exec failed: %w; sql=%q", commitErr, sql),
				fmt.Errorf("Safe rollback failed: %w", rbErr),
			)
		}
		return pgconn.CommandTag{}, fmt.Errorf("Falha no commit de guarda: %w, sql: '''%s'''", commitErr, sql)
	}
	return result, nil
}

// SafeExecTCL runs all TCL (SAVEPOINT, RELEASE, ROLLBACK, ROLLBACK TO SAVEPOINT). SAVEPOINT
// must run on the main tx so the created savepoint is visible for later ROLLBACK/RELEASE;
// RELEASE and ROLLBACK run inside a guard so a failure does not abort the main transaction.
func (d *realSessionDB) SafeExecTCL(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	d.Gui.incRunningQueryCount()
	defer d.Gui.decRunningQueryCount()
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.safeExecTCLLocked(ctx, sql, args...)
}

// safeExecTCLLocked is the body of SafeExecTCL; caller must hold d.mu (write lock).
func (d *realSessionDB) safeExecTCLLocked(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if isSavepointCommand(sql) {
		return d.tx.Exec(ctx, sql, args...)
	}
	savePoint, execErr := d.tx.Begin(ctx)
	if execErr != nil {
		return pgconn.CommandTag{}, fmt.Errorf("Falha ao iniciar savepoint de guarda: %w, pro sql '''%s'''", execErr, sql)
	}
	result, execErr := savePoint.Exec(ctx, sql, args...)
	if execErr != nil {
		if rbErr := savePoint.Rollback(ctx); rbErr != nil {
			return pgconn.CommandTag{}, errors.Join(
				fmt.Errorf("Safe exec failed: %w; sql=%q", execErr, sql),
				fmt.Errorf("Safe rollback failed: %w", rbErr),
			)
		}
		return pgconn.CommandTag{}, fmt.Errorf("Safe exec failed: %w, sql: '''%s'''", execErr, sql)
	}
	if commandInvalidatesGuardOnSuccess(sql) {
		return result, nil
	}
	if commitErr := savePoint.Commit(ctx); commitErr != nil {
		if rbErr := savePoint.Rollback(ctx); rbErr != nil {
			return pgconn.CommandTag{}, errors.Join(
				fmt.Errorf("Safe exec failed: %w; sql=%q", commitErr, sql),
				fmt.Errorf("Safe rollback failed: %w", rbErr),
			)
		}
		return pgconn.CommandTag{}, fmt.Errorf("Falha no commit de guarda: %w, sql: '''%s'''", commitErr, sql)
	}
	return result, nil
}

// isSavepointCommand returns true for SAVEPOINT <name>. Those must run on the main tx.
func isSavepointCommand(query string) bool {
	stmts, err := sqlpkg.ParseStatements(query)
	if err != nil || len(stmts) == 0 || stmts[0].Stmt == nil {
		return false
	}
	return sqlpkg.IsSavepoint(stmts[0].Stmt)
}

// commandInvalidatesGuardOnSuccess returns true when the command's success invalidates the guard
// (we must not call Commit()). ROLLBACK/ROLLBACK TO SAVEPOINT roll back past the guard;
// RELEASE SAVEPOINT releases a savepoint that was created before the guard, which can merge
// the guard's scope and leave the guard non-existent.
func commandInvalidatesGuardOnSuccess(query string) bool {
	stmts, err := sqlpkg.ParseStatements(query)
	if err != nil || len(stmts) == 0 || stmts[0].Stmt == nil {
		return false
	}
	stmt := stmts[0].Stmt
	return sqlpkg.IsTransactionRollback(stmt) || sqlpkg.IsRollbackToSavepoint(stmt) || sqlpkg.IsReleaseSavepoint(stmt)
}

// RollbackUserSavepointsOnDisconnect rolls back the given number of user-opened savepoints
// (from user BEGINs) without touching the base transaction. Called when a client disconnects
// so that uncommitted work is rolled back, matching real PostgreSQL behavior.
// count is the number of open user transactions on that connection (from the proxy connection's counter).
func (d *realSessionDB) RollbackUserSavepointsOnDisconnect(ctx context.Context, count int) error {
	if count <= 0 {
		return nil
	}
	d.Gui.incRunningQueryCount()
	defer d.Gui.decRunningQueryCount()
	d.mu.Lock()
	defer d.mu.Unlock()
	qntToRollback := min(d.SavepointLevel, count)
	if qntToRollback <= 0 {
		return nil
	}
	newSpQnt := d.SavepointLevel - qntToRollback
	spName := fmt.Sprintf("pgrollback_v_%d", newSpQnt+1)
	sql := fmt.Sprintf("ROLLBACK TO SAVEPOINT %s; RELEASE SAVEPOINT %s", spName, spName)
	if _, err := d.safeExecTCLLocked(ctx, sql); err != nil {
		logIfVerbose("[PROXY] RollbackUserSavepointsOnDisconnect: %v", err)
		return err
	}
	d.SavepointLevel = newSpQnt
	return nil
}

// HasActiveTransaction returns whether there is an active transaction (for status/reporting).
// Exported for tests and callers that need to check session state.
func (d *realSessionDB) HasActiveTransaction() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.hasActiveTransactionLocked()
}

func (d *realSessionDB) hasActiveTransactionLocked() bool {
	return d.tx != nil
}

// beginTx starts a new transaction on the connection. Idempotent if already in a transaction (no-op).
func (d *realSessionDB) beginTx(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.conn == nil {
		return nil
	}
	if d.hasActiveTransactionLocked() {
		return nil
	}
	newTx, err := d.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	d.tx = newTx
	return nil
}

// rollbackTx rolls back the current transaction and clears it. Safe to call if tx is nil.
func (d *realSessionDB) rollbackTx(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.hasActiveTransactionLocked() {
		return nil
	}
	err := d.tx.Rollback(ctx)
	d.tx = nil
	return err
}

// startNewTx runs ROLLBACK on the connection (to clear any failed state) and begins a new transaction.
// Used by "pgrollback rollback" to get a clean transaction.
func (d *realSessionDB) startNewTx(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.conn == nil {
		return nil
	}
	d.conn.PgConn().SyncConn(ctx)
	if d.hasActiveTransactionLocked() {
		if err := d.tx.Rollback(ctx); err != nil {
			logIfVerbose("Failed to rollback on starting a new Tx: %s", err)
		}
	}
	_, err := d.conn.Exec(ctx, "ROLLBACK")
	if err != nil {
		return err
	}
	newTx, err := d.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin new transaction: %w", err)
	}
	d.tx = newTx
	return nil
}

// stopKeepaliveUnlocked clears the keepalive callback under a brief lock, then invokes it
// without holding d.mu. The keepalive goroutine acquires mu for Ping; waiting for it to exit
// while holding mu would deadlock with that goroutine.
func (d *realSessionDB) stopKeepaliveLocked() {
	stopFn := d.stopKeepalive
	d.stopKeepalive = nil
	if stopFn != nil {
		stopFn()
	}
}

// close rolls back the current transaction (if any), stops keepalive, and closes the connection.
func (d *realSessionDB) close(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stopKeepaliveLocked()

	d.Gui.ClearQueryHistory()

	if d.conn == nil {
		return nil
	}
	if err := d.conn.Close(ctx); err != nil {
		return fmt.Errorf("failed to close connection: %w", err)
	}
	d.conn = nil
	return nil
}

// startKeepalive starts a goroutine that pings the connection at the given interval (uses conn only for Ping).
func (d *realSessionDB) startKeepalive(interval time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.conn == nil || interval <= 0 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	ticker := time.NewTicker(interval * 10000) //Danilo só pra n chamar isso mais
	go func() {
		defer close(done)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				d.pingKeepaliveOnce()
			}
		}
	}()
	d.stopKeepalive = func() {
		cancel()
		<-done
	}
}

func (d *realSessionDB) pingKeepaliveOnce() {
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 20*time.Second)
	d.mu.Lock()
	if d.conn != nil {
		_ = d.conn.Ping(pingCtx)
	}
	d.mu.Unlock()
	pingCancel()
}

// acquireAdvisoryLock runs pg_advisory_lock on the connection.
// Does not hold d.mu during the blocking pg_advisory_lock call (can wait for other sessions);
// SafeExec and other paths can still take d.mu without being starved.
func (d *realSessionDB) acquireAdvisoryLock(ctx context.Context, lockKey int64) error {
	d.mu.RLock()
	conn := d.conn
	d.mu.RUnlock()
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}
	_, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockKey)
	return err
}

// releaseAdvisoryLock runs pg_advisory_unlock on the connection.
func (d *realSessionDB) releaseAdvisoryLock(ctx context.Context, lockKey int64) error {
	d.mu.RLock()
	conn := d.conn
	d.mu.RUnlock()
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}
	_, err := conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", lockKey)
	return err
}

// PgConn returns the underlying PgConn for advanced use (e.g. multi-statement batch with MultiResultReader).
// Exported for query_handler batch path and tests. Prefer Query/Exec for normal operations.
func (d *realSessionDB) PgConn() *pgconn.PgConn {
	if d == nil {
		return nil
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.pgConnLockedNoNilCheck()
}

func (d *realSessionDB) PgConnLocked() *pgconn.PgConn {
	if d == nil {
		return nil
	}
	return d.pgConnLockedNoNilCheck()
}

func (d *realSessionDB) pgConnLockedNoNilCheck() *pgconn.PgConn {
	if d.conn == nil {
		return nil
	}
	return d.conn.PgConn()
}

// Tx returns the current transaction for advanced/test use (e.g. testutil helpers that expect pgx.Tx).
// Exported for tests. Prefer Query/Exec for normal operations.
func (d *realSessionDB) Tx() pgx.Tx {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.tx
}

// newSessionDB creates a realSessionDB with the given connection and transaction (caller must have begun tx on conn).
func newSessionDB(conn *pgx.Conn, tx pgx.Tx, ctx context.Context) *realSessionDB {
	d := &realSessionDB{
		conn: conn,
		tx:   tx,
		ctx:  ctx,
	}
	return d
}

// contextOrBackground returns the session context when set, otherwise context.Background().
func (d *realSessionDB) contextOrBackground() context.Context {
	if d == nil || d.ctx == nil {
		return context.Background()
	}
	return d.ctx
}

func (g *guiState) incRunningQueryCount() {
	g.mu.Lock()
	g.running++
	g.mu.Unlock()
}

func (g *guiState) decRunningQueryCount() {
	g.mu.Lock()
	if g.running > 0 {
		g.running--
	}
	g.mu.Unlock()
}

// HasRunningQuery reports whether there is at least one query in flight on this session.
func (g *guiState) HasRunningQuery() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.running > 0
}
