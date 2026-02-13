package proxy

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"unicode"
	"unsafe"

	"github.com/davecgh/go-spew/spew"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// ErrNoOpenUserTransaction is returned when COMMIT or ROLLBACK is executed but there is no open user transaction on this connection.
var ErrNoOpenUserTransaction = errors.New("there is no open transaction on this connection")

const pgtestSavepointPrefix = "pgtest_v_"

// extractSavepointNameFromQuery returns the first pgtest_v_* savepoint name in query, or "" if none.
// Used to ensure we only apply TCL tracking when the query refers to the session's expected savepoint.
func extractSavepointNameFromQuery(query string) string {
	i := strings.Index(query, pgtestSavepointPrefix)
	if i < 0 {
		return ""
	}
	start := i
	i += len(pgtestSavepointPrefix)
	for i < len(query) && unicode.IsDigit(rune(query[i])) {
		i++
	}
	return query[start:i]
}

// PrintR é uma função utilitária similar ao print_r do PHP
// Imprime estruturas de dados de forma legível para debugging
func PrintR(v interface{}) string {
	return spew.Sdump(v)
}

// printR é um alias não-exportado para compatibilidade interna
func printR(v interface{}) string {
	return PrintR(v)
}

// printRLog imprime usando log.Printf (similar ao print_r do PHP)
func printRLog(format string, v interface{}) {
	log.Printf(format, printR(v))
}

// proxyConnection gerencia uma conexão proxy entre cliente e PostgreSQL
// Intercepta apenas SQL (Query) para modificar, mantendo resto transparente.
// userOpenTransactionCount tracks how many user BEGINs (converted to SAVEPOINT) have not
// been closed by COMMIT/ROLLBACK on this connection; on disconnect we roll back that many
// savepoints to match real PostgreSQL behavior (implicit rollback on disconnect).
// Per-connection statement/portal maps avoid collisions when multiple connections share the same session (testID).
type proxyConnection struct {
	clientConn               net.Conn
	backend                  *pgproto3.Backend
	server                   *Server
	mu                       sync.Mutex
	userOpenTransactionCount int

	// Per-connection Extended Query state (statement/portal names are client names; backend uses prefixed names).
	preparedStatements   map[string]string
	statementDescs       map[string]*pgconn.StatementDescription
	portalToStatement    map[string]string
	portalParams             map[string][][]byte
	portalFormatCodes        map[string][]int16
	portalResultFormats      map[string][]int16
	multiStatementStatements map[string]struct{} // statement names that are multi-statement (not prepared on backend)
}

// startProxy inicia o proxy usando a sessão existente
// A sessão já tem conexão PostgreSQL autenticada e transação ativa
func (server *Server) startProxy(testID string, clientConn net.Conn, backend *pgproto3.Backend) {
	proxy := &proxyConnection{
		clientConn:            clientConn,
		backend:               backend,
		server:                server,
		preparedStatements:    make(map[string]string),
		statementDescs:        make(map[string]*pgconn.StatementDescription),
		portalToStatement:     make(map[string]string),
		portalParams:          make(map[string][][]byte),
		portalFormatCodes:        make(map[string][]int16),
		portalResultFormats:      make(map[string][]int16),
		multiStatementStatements: make(map[string]struct{}),
	}

	if err := proxy.sendInitialProtocolMessages(); err != nil {
		log.Printf("[PROXY] Failed to send initial protocol messages: %v", err)
		return
	}

	// Inicia o loop de mensagens refatorado em message_loop.go
	proxy.RunMessageLoop(testID)
}

// connectionID returns an opaque id for this connection so session_db can allow nested BEGIN
// on the same connection while rejecting BEGIN from a different connection.
func (p *proxyConnection) connectionID() ConnectionID {
	return ConnectionID(uintptr(unsafe.Pointer(p)))
}

// backendStmtName returns the name to use on the backend for a client statement name.
// Empty name stays empty (unnamed statement per protocol). Named statements are prefixed so
// multiple connections sharing the same backend do not collide.
func (p *proxyConnection) backendStmtName(clientName string) string {
	if clientName == "" {
		return ""
	}
	return fmt.Sprintf("c%d_%s", p.connectionID(), clientName)
}

// SetPreparedStatement stores the intercepted query for the given statement name (Extended Query).
func (p *proxyConnection) SetPreparedStatement(statementName, query string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.preparedStatements[statementName] = query
}

// GetStatementDescription returns the cached StatementDescription for the given statement name, or nil.
func (p *proxyConnection) GetStatementDescription(name string) *pgconn.StatementDescription {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.statementDescs[name]
}

// SetStatementDescription caches a pgconn.StatementDescription for later use by Describe and ExecPrepared.
func (p *proxyConnection) SetStatementDescription(name string, sd *pgconn.StatementDescription) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.statementDescs[name] = sd
}

// BindPortal records which portal is bound to which statement and stores the bound parameters and format codes.
func (p *proxyConnection) BindPortal(portalName, statementName string, parameters [][]byte, formatCodes []int16, resultFormatCodes ...[]int16) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.portalToStatement[portalName] = statementName
	if parameters != nil {
		dup := make([][]byte, len(parameters))
		for i, param := range parameters {
			if param != nil {
				dup[i] = make([]byte, len(param))
				copy(dup[i], param)
			}
		}
		p.portalParams[portalName] = dup
	} else {
		p.portalParams[portalName] = nil
	}
	if formatCodes != nil {
		dup := make([]int16, len(formatCodes))
		copy(dup, formatCodes)
		p.portalFormatCodes[portalName] = dup
	} else {
		delete(p.portalFormatCodes, portalName)
	}
	if len(resultFormatCodes) > 0 && resultFormatCodes[0] != nil {
		dup := make([]int16, len(resultFormatCodes[0]))
		copy(dup, resultFormatCodes[0])
		p.portalResultFormats[portalName] = dup
	} else {
		delete(p.portalResultFormats, portalName)
	}
}

// QueryForPortal returns the query, bound parameters, and format codes for the given portal, or ("", nil, nil, false) if not found.
func (p *proxyConnection) QueryForPortal(portalName string) (query string, params [][]byte, formatCodes []int16, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	statementName := p.portalToStatement[portalName]
	query = p.preparedStatements[statementName]
	params = p.portalParams[portalName]
	formatCodes = p.portalFormatCodes[portalName]
	return query, params, formatCodes, query != ""
}

// PortalStatementName returns the statement name bound to the given portal.
func (p *proxyConnection) PortalStatementName(portalName string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.portalToStatement[portalName]
}

// PortalResultFormats returns the ResultFormatCodes for the given portal, or nil.
func (p *proxyConnection) PortalResultFormats(portalName string) []int16 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.portalResultFormats[portalName]
}

// GetStatementDescriptionForPortal returns the cached StatementDescription for the statement bound to the given portal.
func (p *proxyConnection) GetStatementDescriptionForPortal(portalName string) *pgconn.StatementDescription {
	p.mu.Lock()
	defer p.mu.Unlock()
	stmtName := p.portalToStatement[portalName]
	return p.statementDescs[stmtName]
}

// CloseStatementOrPortal removes the statement or portal from the per-connection maps (objectType 'S' or 'P').
func (p *proxyConnection) CloseStatementOrPortal(objectType byte, name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch objectType {
	case 'S':
		delete(p.preparedStatements, name)
		delete(p.statementDescs, name)
		delete(p.multiStatementStatements, name)
	case 'P':
		delete(p.portalToStatement, name)
		delete(p.portalParams, name)
		delete(p.portalFormatCodes, name)
		delete(p.portalResultFormats, name)
	}
}

// copyPreparedStatementNames returns a copy of all prepared statement names (for disconnect cleanup).
func (p *proxyConnection) copyPreparedStatementNames() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	names := make([]string, 0, len(p.preparedStatements))
	for n := range p.preparedStatements {
		names = append(names, n)
	}
	return names
}

// clearStatementPortalState clears all per-connection statement/portal maps (call after deallocate on disconnect).
func (p *proxyConnection) clearStatementPortalState() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.preparedStatements = make(map[string]string)
	p.statementDescs = make(map[string]*pgconn.StatementDescription)
	p.portalToStatement = make(map[string]string)
	p.portalParams = make(map[string][][]byte)
	p.portalFormatCodes = make(map[string][]int16)
	p.portalResultFormats = make(map[string][]int16)
	p.multiStatementStatements = make(map[string]struct{})
}

// deallocateBackendStatementsOnDisconnect deallocates all of this connection's backend prepared
// statements (skips multi-statement; those were never prepared) and clears per-connection state.
// Call on disconnect so the shared backend is clean. Uses session from p.server.Pgtest.GetSession(testID).
func (p *proxyConnection) deallocateBackendStatementsOnDisconnect(testID string) {
	session := p.server.Pgtest.GetSession(testID)
	if session == nil || session.DB == nil {
		return
	}
	pgConn := session.DB.PgConn()
	if pgConn == nil {
		p.clearStatementPortalState()
		return
	}
	for _, name := range p.copyPreparedStatementNames() {
		if p.IsMultiStatement(name) {
			continue
		}
		backendName := p.backendStmtName(name)
		session.DB.LockRun()
		_ = pgConn.Deallocate(context.Background(), backendName)
		session.DB.UnlockRun()
	}
	p.clearStatementPortalState()
}

// rollbackUserSavepointsOnDisconnect rolls back any open user savepoints (from BEGIN not yet
// COMMIT/ROLLBACK) when this connection disconnects, so the session state matches real PostgreSQL
// behavior (implicit rollback on disconnect). Uses session from p.server.Pgtest.GetSession(testID).
func (p *proxyConnection) rollbackUserSavepointsOnDisconnect(testID string) {
	session := p.server.Pgtest.GetSession(testID)
	if session == nil || session.DB == nil {
		return
	}
	if count := p.GetUserOpenTransactionCount(); count > 0 {
		_ = session.DB.RollbackUserSavepointsOnDisconnect(context.Background(), count)
	}
}

// releaseOpenTransactionOnDisconnect releases this connection's claim on the session's open
// transaction when the connection disconnects. Uses session from p.server.Pgtest.GetSession(testID).
func (p *proxyConnection) releaseOpenTransactionOnDisconnect(testID string) {
	session := p.server.Pgtest.GetSession(testID)
	if session == nil || session.DB == nil {
		return
	}
	session.DB.ReleaseOpenTransaction(p.connectionID())
}

// SetMultiStatement marks the given statement name as multi-statement (not prepared on backend).
func (p *proxyConnection) SetMultiStatement(statementName string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.multiStatementStatements[statementName] = struct{}{}
}

// IsMultiStatement returns true if the statement is a multi-statement (run via batch, not ExecPrepared).
func (p *proxyConnection) IsMultiStatement(statementName string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.multiStatementStatements[statementName]
	return ok
}

// IncrementUserOpenTransactionCount is called when a user BEGIN (converted to SAVEPOINT) is executed on this connection.
func (p *proxyConnection) IncrementUserOpenTransactionCount() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.userOpenTransactionCount++
}

// DecrementUserOpenTransactionCount is called when a user COMMIT or ROLLBACK (RELEASE or ROLLBACK TO SAVEPOINT) is executed.
// Returns ErrNoOpenUserTransaction if the count is already 0.
func (p *proxyConnection) DecrementUserOpenTransactionCount() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.userOpenTransactionCount <= 0 {
		return ErrNoOpenUserTransaction
	}
	p.userOpenTransactionCount--
	return nil
}

// GetUserOpenTransactionCount returns how many user transactions are still open on this connection (for rollback on disconnect).
func (p *proxyConnection) GetUserOpenTransactionCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.getUserOpenTransactionCountLocked()
}

func (p *proxyConnection) getUserOpenTransactionCountLocked() int {
	return p.userOpenTransactionCount
}

// ApplyTCLSuccessTracking is called only after a TCL command (SAVEPOINT/RELEASE/ROLLBACK TO) has been successfully executed.
// It updates session SavepointLevel (increment on SAVEPOINT), per-connection user transaction count, and releases the
// session claim when the connection's count drops to zero. Returns ErrNoOpenUserTransaction if COMMIT/ROLLBACK is applied with count already 0.
func (p *proxyConnection) ApplyTCLSuccessTracking(query string, session *TestSession) error {
	if session == nil || session.DB == nil {
		return nil
	}
	trimmed := strings.TrimSpace(query)
	savepointName := extractSavepointNameFromQuery(trimmed)
	if savepointName == "" {
		return nil
	}

	// User BEGIN -> SAVEPOINT pgtest_v_N: only apply if the name matches the session's expected next (GetNextSavepointName).
	if strings.HasPrefix(trimmed, "SAVEPOINT "+pgtestSavepointPrefix) {
		if savepointName != session.DB.GetNextSavepointName() {
			return nil
		}
		session.DB.IncrementSavepointLevel()
		p.IncrementUserOpenTransactionCount()
		return nil
	}

	// User ROLLBACK -> "ROLLBACK TO SAVEPOINT pgtest_v_N; RELEASE SAVEPOINT pgtest_v_N": name must match current (GetSavepointName); then decrement level only now (after successful execution).
	if strings.Contains(query, "ROLLBACK TO SAVEPOINT "+pgtestSavepointPrefix) {
		if savepointName != session.DB.GetSavepointName() {
			return nil
		}
		//if err := p.DecrementUserOpenTransactionCount(); err != nil {
		//	return err
		//}
		//session.DB.DecrementSavepointLevel()
		//if p.GetUserOpenTransactionCount() == 0 {
		//	session.DB.ReleaseOpenTransaction(p.connectionID(), "")
		//}
		return nil
	}

	// User COMMIT -> "RELEASE SAVEPOINT pgtest_v_N": same as above.
	if strings.HasPrefix(trimmed, "RELEASE SAVEPOINT "+pgtestSavepointPrefix) {
		if savepointName != session.DB.GetSavepointName() {
			return nil
		}
		if err := p.DecrementUserOpenTransactionCount(); err != nil {
			return err
		}
		session.DB.decrementSavepointLevelLocked()
		if p.getUserOpenTransactionCountLocked() == 0 {
			session.DB.releaseOpenTransactionLocked(p.connectionID())
		}
		return nil
	}
	return nil
}

// ApplyTCLSuccessTrackingLocked is like ApplyTCLSuccessTracking but uses only session.DB methods that do not acquire d.mu
// (getSavepointNameLocked, getNextSavepointNameLocked, incrementSavepointLevelLocked, decrementSavepointLevelLocked,
// releaseOpenTransactionLocked). Call only while the caller holds session.DB's lock (e.g. LockRun).
func (p *proxyConnection) ApplyTCLSuccessTrackingLocked(query string, session *TestSession) error {
	if session == nil || session.DB == nil {
		return nil
	}
	trimmed := strings.TrimSpace(query)
	savepointName := extractSavepointNameFromQuery(trimmed)
	if savepointName == "" {
		return nil
	}

	if strings.HasPrefix(trimmed, "SAVEPOINT "+pgtestSavepointPrefix) {
		if savepointName != session.DB.getNextSavepointNameLocked() {
			return nil
		}
		session.DB.incrementSavepointLevelLocked()
		p.IncrementUserOpenTransactionCount()
		return nil
	}

	if strings.Contains(query, "ROLLBACK TO SAVEPOINT "+pgtestSavepointPrefix) {
		if savepointName != session.DB.getSavepointNameLocked() {
			return nil
		}
		return nil
	}

	if strings.HasPrefix(trimmed, "RELEASE SAVEPOINT "+pgtestSavepointPrefix) {
		if savepointName != session.DB.getSavepointNameLocked() {
			return nil
		}
		if err := p.DecrementUserOpenTransactionCount(); err != nil {
			return err
		}
		session.DB.decrementSavepointLevelLocked()
		if p.getUserOpenTransactionCountLocked() == 0 {
			session.DB.releaseOpenTransactionLocked(p.connectionID())
		}
		return nil
	}
	return nil
}

// sendInitialProtocolMessages sends the initial PostgreSQL protocol messages to the client.
// When we have a cache from the real PostgreSQL (first connection), we replay those;
// otherwise we fall back to hardcoded defaults.
func (p *proxyConnection) sendInitialProtocolMessages() error {
	cache := p.server.Pgtest.GetBackendStartupCache()
	if cache != nil && len(cache.ParameterStatuses) > 0 {
		for i := range cache.ParameterStatuses {
			ps := &cache.ParameterStatuses[i]
			p.backend.Send(&pgproto3.ParameterStatus{Name: ps.Name, Value: ps.Value})
		}
		p.backend.Send(&pgproto3.BackendKeyData{ProcessID: cache.BackendKeyData.ProcessID, SecretKey: cache.BackendKeyData.SecretKey})
	} else {
		p.backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "14.0"})
		p.backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
		p.backend.Send(&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO"})
		p.backend.Send(&pgproto3.BackendKeyData{ProcessID: 12345, SecretKey: 67890})
	}
	p.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	if err := p.backend.Flush(); err != nil {
		return fmt.Errorf("failed to flush initial protocol messages: %w", err)
	}
	return nil
}
