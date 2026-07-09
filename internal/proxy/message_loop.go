package proxy

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	pg_query "github.com/pganalyze/pg_query_go/v5"

	"pgrollback/pkg/logger"
	"pgrollback/pkg/protocol"
	"pgrollback/pkg/sql"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// DescribeRowFieldsForQuery returns the RowDescription fields to send for Describe (Portal/Statement)
// when the query returns a result set (e.g. INSERT/UPDATE/DELETE ... RETURNING). Clients that rely
// on Describe (e.g. PHP PDO / Laravel Eloquent) need this so they get the correct result shape and
// do not receive an empty result set. Returns nil if the query does not return rows or we cannot parse RETURNING.
func DescribeRowFieldsForQuery(query string) []pgproto3.FieldDescription {
	if query == "" {
		return nil
	}
	var stmt *pg_query.Node
	if stmts, err := sql.ParseStatements(query); err == nil && len(stmts) > 0 {
		stmt = stmts[0].Stmt
	}
	if stmt != nil {
		if !sql.StmtReturnsResultSet(stmt) {
			return nil
		}
		cols := sql.GetReturningColumns(stmt)
		if len(cols) == 0 {
			return nil
		}
		names := make([]string, len(cols))
		oids := make([]uint32, len(cols))
		for i, c := range cols {
			names[i] = c.Name
			oids[i] = c.OID
		}
		return protocol.FieldDescriptionsFromNamesAndOIDs(names, oids)
	}
	// Fallback when parse fails
	if !sql.ReturnsResultSetFallback(query) {
		return nil
	}
	cols := sql.ReturningColumnsFallback(query)
	if len(cols) == 0 {
		return nil
	}
	names := make([]string, len(cols))
	oids := make([]uint32, len(cols))
	for i, c := range cols {
		names[i] = c.Name
		oids[i] = c.OID
	}
	return protocol.FieldDescriptionsFromNamesAndOIDs(names, oids)
}

// maxParameterIndex returns the maximum $n placeholder index in query (e.g. $1 $2 $1 -> 2). Returns 0 if none.
func maxParameterIndex(query string) int {
	stmts, err := sql.ParseStatements(query)
	if err != nil || len(stmts) == 0 || stmts[0].Stmt == nil {
		return 0
	}
	return sql.MaxParamIndex(stmts[0].Stmt)
}

// pgconnFieldToProto converts a pgconn.FieldDescription to a pgproto3.FieldDescription.
func pgconnFieldToProto(f pgconn.FieldDescription) pgproto3.FieldDescription {
	return pgproto3.FieldDescription{
		Name:                 []byte(f.Name),
		TableOID:             f.TableOID,
		TableAttributeNumber: f.TableAttributeNumber,
		DataTypeOID:          f.DataTypeOID,
		DataTypeSize:         f.DataTypeSize,
		TypeModifier:         f.TypeModifier,
		Format:               f.Format,
	}
}

// sendDescribeFromSD sends ParameterDescription + RowDescription/NoData to the client
// based on the cached StatementDescription. Used for Describe(S) and Describe(P).
// For Describe(P), resultFormats from the Bind message are applied to the field Format field
// so the client knows which encoding the data will use.
func (p *proxyConnection) sendDescribeFromSD(sd *pgconn.StatementDescription, objectType byte, resultFormats []int16) {
	if objectType == 'S' {
		// Statement Describe: ParameterDescription + RowDescription/NoData
		p.backend.Send(&pgproto3.ParameterDescription{ParameterOIDs: sd.ParamOIDs})
	}
	if len(sd.Fields) > 0 {
		fields := make([]pgproto3.FieldDescription, len(sd.Fields))
		for i, f := range sd.Fields {
			fields[i] = pgconnFieldToProto(f)
			// For Portal Describe, apply the result format codes from Bind.
			if objectType == 'P' && len(resultFormats) > 0 {
				if len(resultFormats) == 1 {
					fields[i].Format = resultFormats[0]
				} else if i < len(resultFormats) {
					fields[i].Format = resultFormats[i]
				}
			}
		}
		p.backend.Send(&pgproto3.RowDescription{Fields: fields})
	} else {
		p.backend.Send(&pgproto3.NoData{})
	}
	p.backend.Flush()
}

// executeViaExecPrepared calls PgConn.ExecPrepared for the given portal, reads all results,
// and sends DataRow + CommandComplete to the client. Returns an error if the execution fails.
func (p *proxyConnection) executeViaExecPrepared(ctx context.Context, pgConn *pgconn.PgConn, stmtName string, params [][]byte, paramFormats []int16, resultFormats []int16) error {
	rr := pgConn.ExecPrepared(ctx, stmtName, params, paramFormats, resultFormats)
	// Read all rows and forward as DataRow messages.
	for rr.NextRow() {
		values := rr.Values()
		p.backend.Send(&pgproto3.DataRow{Values: values})
	}
	// Close finishes reading (CommandComplete + ReadyForQuery internally).
	tag, err := rr.Close()
	if err != nil {
		return err
	}
	p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag.String())})
	p.backend.Flush()
	return nil
}

// runDisconnectCleanup runs rollback, release, and deallocate cleanup for this connection.
// Call on disconnect so the shared session/backend state is correct. Run inline (before return).
// We run deallocate first (it holds the session lock for the whole DEALLOCATE batch) so that
// any other connection (e.g. conn2 polling prepared-statement count) is blocked until
// statements are deallocated; then we run rollback/release (which take the lock briefly).
func (p *proxyConnection) destroySessionIfRequested(testID string) bool {
	remoteAddr := p.clientConn.RemoteAddr().String()
	p.server.PgRollback.mu.Lock()
	defer p.server.PgRollback.mu.Unlock()
	session := p.server.PgRollback.GetSessionLocked(testID)
	if session == nil {
		return false
	}
	session.mu.RLock()
	disconnect := session.ShouldDisconnectOnCleanupLocked()
	session.mu.RUnlock()
	if !disconnect {
		return false
	}

	log.Printf("[PROXY] destroy session on disconnect (testID=%s, conn=%s)", testID, remoteAddr)
	if err := p.server.PgRollback.destroySessionCoreWithPLock(session, testID); err != nil {
		log.Printf("[PROXY] error destroying session on disconnect (testID=%s, conn=%s): %v", testID, remoteAddr, err)
	}
	return true
}

func (p *proxyConnection) runDisconnectCleanup(testID string) {
	remoteAddr := p.clientConn.RemoteAddr().String()
	log.Printf("[PROXY] disconnect cleanup starting (testID=%s, conn=%s)", testID, remoteAddr)
	p.deallocateBackendStatementsOnDisconnect(testID)
	p.rollbackUserSavepointsOnDisconnect(testID)
	p.releaseOpenTransactionOnDisconnect(testID)
	if !p.destroySessionIfRequested(testID) {
		log.Printf("[PROXY] disconnect cleanup done (testID=%s, conn=%s)", testID, remoteAddr)
		return
	}
	p.server.PgRollback.PublishSnapshot()
}

// RunMessageLoop é o loop principal que processa as mensagens do cliente.
// Ele mantém a conexão aberta e despacha cada mensagem para o handler apropriado.
// testID is derived from session via PgRollback.GetTestID; callers must not pass both.
func (p *proxyConnection) RunMessageLoop(session *TestSession) {
	defer p.clientConn.Close()
	if session == nil {
		log.Printf("[PROXY] RunMessageLoop: nil session")
		return
	}
	testID := p.server.PgRollback.GetTestID(session)
	if testID == "" {
		log.Printf("[PROXY] RunMessageLoop: session not found in SessionsByTestID map")
		session.unregisterProxyClient(p.clientConn)
		return
	}
	defer p.runDisconnectCleanup(testID)
	defer session.unregisterProxyClient(p.clientConn)
	// Extended Query protocol (e.g. pgx for QueryContext("SELECT 1")) typically sends:
	//   Parse → Describe(S) → Sync → Bind → Describe(P) → Execute → Sync
	// We forward each message to the real PostgreSQL (via the session's PgConn) and relay
	// the backend's response to the client. Query interception (BEGIN→SAVEPOINT etc.) is applied
	// at Parse time: we forward the modified Parse, so the backend never sees the raw client query.
	// Simple Query (pgproto3.Query) continues to use the pgx Tx API via ProcessSimpleQuery.
	for {
		msg, err := p.backend.Receive()
		if err != nil {
			return
		}

		switch msg := msg.(type) {
		case *pgproto3.Query:
			logger.Debug("[PROXY-ML] Query recebido: %s", msg.String)
			p.handleMessageQuery(testID, msg)

		case *pgproto3.Parse:
			logger.Debug("[PROXY-ML] Parse recebido: %s", msg.Query)
			p.handleMessageParse(testID, msg)

		case *pgproto3.Bind:
			logger.Debug("[PROXY-ML] Bind recebido: %s", msg.PreparedStatement)
			p.handleMessageBind(msg)

		case *pgproto3.Execute:
			logger.Debug("[PROXY-ML] Execute recebido: %s", msg.Portal)
			p.handleMessageExecute(testID, msg)

		case *pgproto3.Describe:
			logger.Debug("[PROXY-ML] Describe recebido: %s", msg.Name)
			p.handleMessageDescribe(msg)

		case *pgproto3.Sync:
			logger.Debug("[PROXY-ML] Sync recebido")
			p.handleMessageSync()

		case *pgproto3.Terminate:
			logger.Debug("[PROXY-ML] Terminate recebido")
			return

		case *pgproto3.Flush:
			logger.Debug("[PROXY-ML] Flush recebido")
			p.handleMessageFlush(testID)

		case *pgproto3.Close:
			logger.Debug("[PROXY-ML] Close recebido: %s", msg.Name)
			p.handleMessageClose(testID, msg)

		case *pgproto3.CopyData:
			logger.Debug("[PROXY-ML] CopyData recebido")
			p.handleMessageCopyData(testID)

		default:
			logger.Warn("[PROXY-ML] Mensagem desconhecida recebida: %T", msg)
			p.handleMessageDefault(testID, msg)
		}
	}
}

func (p *proxyConnection) handleMessageDefault(testID string, msg pgproto3.FrontendMessage) {
	// Captura qualquer outra mensagem não tratada explicitamente.
	p.SendReadyForQuery()
	p.backend.Flush()
}

func (p *proxyConnection) handleMessageCopyData(testID string) {
	remoteAddr := p.clientConn.RemoteAddr().String()
	// Mensagens de tráfego de dados (COPY). Ignoramos no log para evitar spam,
	// mas mantemos o fallback seguro de enviar ReadyForQuery para não travar.
	log.Printf("[PROXY] CopyData ignorado (testID=%s, conn=%s)", testID, remoteAddr)
	p.SendReadyForQuery()
	p.backend.Flush()
}

func (p *proxyConnection) handleMessageClose(testID string, msg *pgproto3.Close) {
	// Deallocate on backend using connection-prefixed name (only if we prepared it); clean up per-connection maps.
	session := p.server.PgRollback.GetSession(testID)
	if session != nil && session.DB != nil {
		db := session.DB
		pgConn := db.PgConn()
		if msg.ObjectType == 'S' && pgConn != nil && !p.IsMultiStatement(msg.Name) {
			backendName := p.backendStmtName(msg.Name)
			db.LockRun()
			if err := pgConn.Deallocate(session.Context(), backendName); err != nil {
				log.Printf("[PROXY] Deallocate failed: %v", err)
			}
			db.UnlockRun()
		}
		p.CloseStatementOrPortal(msg.ObjectType, msg.Name)
	}
	p.backend.Send(&pgproto3.CloseComplete{})
	p.backend.Flush()
}

func (p *proxyConnection) handleMessageFlush(testID string) {
	remoteAddr := p.clientConn.RemoteAddr().String()
	log.Printf("[PROXY] Flush recebido (testID=%s, conn=%s)", testID, remoteAddr)
	p.backend.Flush()
}

func (p *proxyConnection) handleMessageSync() {
	// The real Sync+ReadyForQuery were already consumed by PgConn.Prepare() or ExecPrepared().
	// Clear any pending extended-query error and send a single ReadyForQuery to the client.
	// This is the ONLY place ReadyForQuery is sent for the extended-query protocol.
	p.extendedQueryPendingError = nil
	p.SendReadyForQuery()
}

func (p *proxyConnection) handleMessageDescribe(msg *pgproto3.Describe) {
	// If a previous message in this extended-query cycle failed, propagate that same error.
	// Do NOT send ReadyForQuery — only Sync does that.
	if p.extendedQueryPendingError != nil {
		p.sendExtendedQueryErr(p.extendedQueryPendingError)
		return
	}

	// Use per-connection cached StatementDescription to respond with ParameterDescription + RowDescription/NoData.
	// Multi-statement "prepared" queries have no backend SD; send empty params + NoData.
	var stmtName string
	if msg.ObjectType == 'S' {
		stmtName = msg.Name
	} else {
		stmtName = p.PortalStatementName(msg.Name)
	}
	if p.IsMultiStatement(stmtName) {
		p.backend.Send(&pgproto3.ParameterDescription{ParameterOIDs: nil})
		p.backend.Send(&pgproto3.NoData{})
		p.backend.Flush()
		return
	}
	var sd *pgconn.StatementDescription
	var resultFormats []int16
	if msg.ObjectType == 'S' {
		sd = p.GetStatementDescription(msg.Name)
	} else {
		sd = p.GetStatementDescriptionForPortal(msg.Name)
		resultFormats = p.PortalResultFormats(msg.Name)
	}
	if sd == nil {
		p.sendExtendedQueryErr(fmt.Errorf("statement description not found for Describe (objectType=%c, name=%q)", msg.ObjectType, msg.Name))
		return
	}
	p.sendDescribeFromSD(sd, msg.ObjectType, resultFormats)
}

func (p *proxyConnection) handleMessageExecute(testID string, msg *pgproto3.Execute) {
	// If a previous message in this extended-query cycle failed, propagate the error (no RFQ).
	if p.extendedQueryPendingError != nil {
		p.sendExtendedQueryErr(p.extendedQueryPendingError)
		return
	}
	// Execute the prepared statement via PgConn.ExecPrepared() using per-connection
	// portal/statement state and backend-prefixed statement name. LockRun serializes backend use.
	session := p.server.PgRollback.GetSession(testID)
	if session == nil || session.DB == nil || session.DB.PgConn() == nil {
		p.sendExtendedQueryErr(fmt.Errorf("sessão não encontrada para testID: %s", testID))
		return
	}
	stmtName := p.PortalStatementName(msg.Portal)
	query, params, formatCodes, ok := p.QueryForPortal(msg.Portal)
	if !ok {
		p.sendExtendedQueryErr(fmt.Errorf("portal ou statement não encontrado para execução (portal=%q)", msg.Portal))
		return
	}
	if query != "" && session.DB != nil {
		args := bindParamsToArgs(params, formatCodes)
		connLabel := ""
		if p.clientConn != nil {
			connLabel = p.clientConn.RemoteAddr().String()
		}
		session.DB.SetLastQueryWithParams(query, args, connLabel)
		p.server.PgRollback.PublishSessionUpdate(testID)
	}
	if p.IsMultiStatement(stmtName) {
		// Run as batch and send only the last result (same behavior as Simple Query multi-statement).
		var commands []string
		if stmts, err := sql.ParseStatements(query); err == nil && len(stmts) > 0 {
			if len(stmts) > 1 {
				// Use quote-aware split so we don't cut inside e.g. SET client_encoding='utf-8'
				commands = sql.SplitCommandsFallback(query)
			} else {
				if c := sql.CommandStringFromRaw(query, stmts[0]); c != "" {
					commands = []string{c}
				}
			}
		}
		if len(commands) == 0 {
			commands = sql.SplitCommandsFallback(query)
		}
		if err := p.SafeForwardMultipleCommandsToDB(testID, commands, false); err != nil {
			log.Printf("[PROXY] multi-statement Execute failed: %v", err)
			p.sendExtendedQueryErr(err)
			recoverSessionTxAfterDirectExec(session)
		}
		return
	}
	resultFormats := p.PortalResultFormats(msg.Portal)
	pgConn := session.DB.PgConn()
	backendStmtName := p.backendStmtName(stmtName)
	session.DB.LockRun()
	start := time.Now()
	err := p.executeViaExecPrepared(session.Context(), pgConn, backendStmtName, params, formatCodes, resultFormats)
	elapsed := time.Since(start)
	session.DB.UnlockRun()
	session.DB.Gui.UpdateLastQueryHistoryDuration(elapsed)
	p.server.PgRollback.PublishSessionUpdate(testID)
	if err != nil {
		log.Printf("[PROXY] ExecPrepared failed: %v", err)
		p.sendExtendedQueryErr(err)
		recoverSessionTxAfterDirectExec(session)
	}
}

func (p *proxyConnection) handleMessageBind(msg *pgproto3.Bind) {
	// If a previous message in this extended-query cycle failed, propagate the error (no RFQ).
	if p.extendedQueryPendingError != nil {
		p.sendExtendedQueryErr(p.extendedQueryPendingError)
		return
	}
	// Store portal mapping per-connection. The actual Bind to PostgreSQL happens when
	// Execute arrives (via ExecPrepared which uses backend-prefixed statement name).
	p.BindPortal(msg.DestinationPortal, msg.PreparedStatement, msg.Parameters, msg.ParameterFormatCodes, msg.ResultFormatCodes)
	p.backend.Send(&pgproto3.BindComplete{})
	p.backend.Flush()
}

func (p *proxyConnection) handleMessageParse(testID string, msg *pgproto3.Parse) {
	// Extended Query: intercept query, store per-connection, call PgConn.Prepare() with
	// connection-prefixed name so concurrent connections don't collide. LockRun serializes
	// use of the shared backend. Do NOT call any session.DB method that takes d.mu while holding LockRun.
	//
	// If a previous message in this extended-query cycle already failed, short-circuit with
	// the same error (no RFQ — only Sync sends ReadyForQuery).
	if p.extendedQueryPendingError != nil {
		p.sendExtendedQueryErr(p.extendedQueryPendingError)
		return
	}
	session := p.server.PgRollback.GetSession(testID)
	if session == nil || session.DB == nil || session.DB.PgConn() == nil {
		p.sendExtendedQueryErr(fmt.Errorf("sessão não encontrada para testID: %s", testID))
		return
	}
	// Capture DB pointer for LockRun/defer: if another goroutine runs disconnect-all, session.DB
	// becomes nil before defer runs; defer session.DB.UnlockRun() would then call UnlockRun on nil.
	db := session.DB
	interceptedQuery, err := p.server.PgRollback.InterceptQuery(testID, msg.Query, p.connectionID())
	if err != nil {
		p.sendExtendedQueryErr(err)
		return
	}
	p.SetPreparedStatement(msg.Name, interceptedQuery)
	var numStmts int
	if stmts, err := sql.ParseStatements(interceptedQuery); err == nil {
		numStmts = len(stmts)
	} else {
		numStmts = len(sql.SplitCommandsFallback(interceptedQuery))
	}
	if numStmts > 1 {
		// PostgreSQL does not allow multiple commands in a prepared statement. Run as batch on Execute.
		p.SetMultiStatement(msg.Name)
		p.backend.Send(&pgproto3.ParseComplete{})
		p.backend.Flush()
		return
	}
	backendName := p.backendStmtName(msg.Name)
	session.DB.LockRun()
	defer session.DB.UnlockRun()
	var existingSD *pgconn.StatementDescription
	if msg.Name != "" {
		existingSD = p.GetStatementDescriptionLocked(msg.Name)
	}
	pgConn := session.DB.PgConnLocked()
	ctx := session.Context()
	if msg.Name != "" && existingSD != nil && pgConn != nil {
		_ = pgConn.Deallocate(ctx, backendName)
	}
	if pgConn == nil {
		p.sendExtendedQueryErr(fmt.Errorf("conexão backend indisponível"))
		return
	}

	// Wrap pgConn.Prepare in a savepoint guard: a failed parse (e.g. table/column does not exist)
	// would otherwise leave the base transaction in aborted state (SQLSTATE 25P02).
	// runWithSavepointGuardLocked is safe here because LockRun() holds d.mu.
	var sd *pgconn.StatementDescription
	prepErr := db.runWithSavepointGuardLocked(ctx, "pgrollback_prepare_guard", func() error {
		var err error
		sd, err = pgConn.Prepare(ctx, backendName, interceptedQuery, msg.ParameterOIDs)
		return err
	})
	if prepErr != nil {
		log.Printf("[PROXY] Prepare failed: %v", prepErr)
		p.sendExtendedQueryErr(prepErr)
		return
	}
	p.SetStatementDescriptionLocked(msg.Name, sd)
	p.backend.Send(&pgproto3.ParseComplete{})
	p.backend.Flush()
}

func (p *proxyConnection) handleMessageQuery(testID string, msg *pgproto3.Query) {
	// Flow "Simple Query": O cliente envia uma string SQL direta.
	// Espera-se que retornemos RowDescription, DataRow(s), CommandComplete e ReadyForQuery.
	queryStr := msg.String
	remoteAddr := p.clientConn.RemoteAddr().String()
	log.Printf("[PROXY] Query Simples Recebida (testID=%s, conn=%s): %s", testID, remoteAddr, queryStr)
	if os.Getenv("PGROLLBACK_LOG_MESSAGE_ORDER") == "1" {
		preview := queryStr
		if len(preview) > 60 {
			preview = strings.TrimSpace(preview[:60]) + "..."
		}
		log.Printf("[MSG_ORDER] RECV SimpleQuery: %s", preview)
	}
	//p.mu.Lock()
	//p.lastQuery = "" // Limpa a query armazenada para evitar execução duplicada
	//p.inExtendedQuery = false
	//p.mu.Unlock()
	// Query history Running/Duration is finalized inside ForwardCommandToDB/ExecuteSelectQuery/
	// SafeForwardMultipleCommandsToDB (via defer, on every exit path); nothing to finalize here.
	if err := p.ProcessSimpleQuery(testID, queryStr); err != nil {
		log.Printf("[PROXY] Erro ao processar Query Simples: %v", err)
		p.SendErrorResponse(err)
	} else {
		log.Printf("[PROXY] Query Simples processada com sucesso: %s", queryStr)
	}
	p.backend.Flush()
}

// ProcessSimpleQuery lida com o fluxo de "Simple Query" (pgproto3.Query).
// Intercepta o SQL, executa e garante o envio de ReadyForQuery ao final via executeQuery(..., true).
func (p *proxyConnection) ProcessSimpleQuery(testID string, query string) error {
	session := p.server.PgRollback.GetSession(testID)
	if session == nil {
		return fmt.Errorf("sessão não encontrada para testID: %s", testID)
	}
	interceptedQuery, err := p.server.PgRollback.InterceptQuery(testID, query, p.connectionID())
	if err != nil {
		return err
	}

	if interceptedQuery == FULLROLLBACK_SENTINEL && session.DB != nil {
		session.DB.Gui.ClearLastQuery()
	}
	// Se a interceptação "engoliu" a query (retornou vazia ou marcador), apenas finalizamos.
	// Isso acontece com comandos pgrollback internos ou quando queremos silenciar uma query.
	if interceptedQuery == "" || interceptedQuery == FULLROLLBACK_SENTINEL || interceptedQuery == DISCONNECT_SENTINEL {
		if os.Getenv("PGROLLBACK_LOG_MESSAGE_ORDER") == "1" {
			log.Printf("[MSG_ORDER] SEND CommandComplete: SELECT (intercepted)")
			log.Printf("[MSG_ORDER] SEND ReadyForQuery")
		}
		p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT")})
		p.SendReadyForQuery()
		return nil
	}

	// Run via session (Exec/ForwardMultipleCommandsToDB) so we use the same connection/transaction
	// as the rest of the session. Forwarding raw Simple Query on PgConn can conflict with the
	// connection state and cause long delays.
	return p.ExecuteInterpretedQuery(testID, interceptedQuery, true)
}

// bindParamsToArgs converts wire-format Bind parameters (text or binary) to []any for pgx.
// formatCodes: 0 = text, 1 = binary; nil means all text.
// Binary: 4 bytes -> int32 (int4), 8 bytes -> int64 (int8/bigint). Other lengths are passed as text.
func bindParamsToArgs(params [][]byte, formatCodes []int16) []any {
	if len(params) == 0 {
		return nil
	}
	args := make([]any, len(params))
	for i, p := range params {
		if p == nil {
			args[i] = nil
			continue
		}
		isBinary := false
		if len(formatCodes) == 1 {
			isBinary = formatCodes[0] == 1
		} else if i < len(formatCodes) {
			isBinary = formatCodes[i] == 1
		}
		if isBinary {
			switch len(p) {
			case 4:
				args[i] = int32(binary.BigEndian.Uint32(p))
				continue
			case 8:
				args[i] = int64(binary.BigEndian.Uint64(p))
				continue
			}
		}
		args[i] = string(p)
	}
	return args
}
