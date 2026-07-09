package proxy

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	pg_query "github.com/pganalyze/pg_query_go/v5"

	"pgrollback/pkg/protocol"
	"pgrollback/pkg/sql"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// recoverSessionTxAfterDirectExec rolls back the aborted transaction and starts a new one
// after a direct backend execution failed (e.g. pgConn.Exec or ExecPrepared), so the next
// command does not see "current transaction is aborted" (SQLSTATE 25P02).
func recoverSessionTxAfterDirectExec(session *TestSession) {
	if session == nil || session.DB == nil {
		return
	}
	if err := session.DB.startNewTx(session.Context()); err != nil {
		log.Printf("[PROXY] Recover tx after direct exec error: %v", err)
	}
}

// multiCommandBatchNeedsSequentialExec is true when the batch contains client transaction
// control (BEGIN/COMMIT/ROLLBACK) that must be rewritten per statement. A single pgConn.Exec
// of the full string would send raw COMMIT to PostgreSQL and end the session transaction.
func multiCommandBatchNeedsSequentialExec(commands []string) bool {
	for _, cmd := range commands {
		c := strings.TrimSpace(cmd)
		if c == "" {
			continue
		}
		stmts, err := sql.ParseStatements(c)
		if err == nil && len(stmts) > 0 && stmts[0].Stmt != nil {
			s := stmts[0].Stmt
			if sql.IsTransactionCommit(s) || sql.IsTransactionBegin(s) || sql.IsTransactionRollback(s) {
				return true
			}
			continue
		}
		u := strings.ToUpper(c)
		if strings.HasPrefix(u, "COMMIT") {
			return true
		}
		if strings.HasPrefix(u, "BEGIN") && !strings.Contains(u, "SAVEPOINT") {
			return true
		}
		if strings.HasPrefix(u, "ROLLBACK") && !strings.Contains(u, "TO SAVEPOINT") {
			return true
		}
	}
	return false
}

// ExecuteInterpretedQuery recebe uma query que já passou por parse e interceptação.
// Ela decide se é comando único ou múltiplos comandos e encaminha para o banco.
// args são os parâmetros bound (Extended Query); para Simple Query não passar args.
//
// DEALLOCATE is rewritten so the backend sees this connection's prefixed statement names
// (multiple client connections with the same testID share one backend but use per-connection prefixes).
//
// sendReadyForQuery:
//   - true para fluxo "Simple Query" (envia ReadyForQuery ao final).
//   - false para fluxo "Extended Query" (não envia, espera-se recebimento de Sync depois).
func (p *proxyConnection) ExecuteInterpretedQuery(testID string, query string, sendReadyForQuery bool, args ...any) error {
	var deallocatedInQuery []string
	stmts, err := sql.ParseStatements(query)
	if err != nil || len(stmts) == 0 {
		// Fallback to string split when parse fails or empty (respects quotes, e.g. SET client_encoding='utf-8')
		commands := sql.SplitCommandsFallback(query)
		var expanded []string
		for _, cmd := range commands {
			c := strings.TrimSpace(cmd)
			if c == "" {
				continue
			}
			rewritten, isDEALLOCATE, names := p.rewriteDEALLOCATEForBackend(c)
			if isDEALLOCATE {
				expanded = append(expanded, rewritten...)
				deallocatedInQuery = append(deallocatedInQuery, names...)
			} else {
				expanded = append(expanded, c)
			}
		}
		if len(expanded) == 0 {
			return p.ForwardCommandToDB(testID, query, sendReadyForQuery, args...)
		}
		err := p.executeExpandedCommands(testID, expanded, sendReadyForQuery)
		if err != nil {
			return err
		}
		p.RemovePreparedStatements(deallocatedInQuery)
		return nil
	}
	var expanded []string
	if len(stmts) > 1 {
		// Multiple statements: use quote-aware split so we don't cut inside e.g. SET client_encoding='utf-8'
		commands := sql.SplitCommandsFallback(query)
		for _, cmd := range commands {
			c := strings.TrimSpace(cmd)
			if c == "" {
				continue
			}
			singleStmts, _ := sql.ParseStatements(c)
			if len(singleStmts) > 0 && singleStmts[0].Stmt != nil {
				_, _, isDEALLOCATE := sql.ParseDeallocate(singleStmts[0].Stmt)
				if isDEALLOCATE {
					rewritten, _, names := p.rewriteDEALLOCATEForBackend(c)
					expanded = append(expanded, rewritten...)
					deallocatedInQuery = append(deallocatedInQuery, names...)
				} else {
					expanded = append(expanded, c)
				}
			} else {
				expanded = append(expanded, c)
			}
		}
	} else {
		// Single statement: use CommandStringFromRaw when parser gave StmtLocation/StmtLen;
		// otherwise (ErrNoStatementBounds) use full query when this is the only statement.
		for _, raw := range stmts {
			c := sql.CommandStringFromRaw(query, raw)
			if c == "" {
				continue
			}
			stmt := raw.Stmt
			if stmt != nil {
				_, _, isDEALLOCATE := sql.ParseDeallocate(stmt)
				if isDEALLOCATE {
					rewritten, _, names := p.rewriteDEALLOCATEForBackend(c)
					expanded = append(expanded, rewritten...)
					deallocatedInQuery = append(deallocatedInQuery, names...)
				} else {
					expanded = append(expanded, c)
				}
			} else {
				expanded = append(expanded, c)
			}
		}
	}
	if len(expanded) == 0 {
		return p.ForwardCommandToDB(testID, query, sendReadyForQuery, args...)
	}
	err = p.executeExpandedCommands(testID, expanded, sendReadyForQuery)
	if err != nil {
		return err
	}
	p.RemovePreparedStatements(deallocatedInQuery)
	return nil
}

// executeExpandedCommands runs the expanded command list (single or multiple).
func (p *proxyConnection) executeExpandedCommands(testID string, expanded []string, sendReadyForQuery bool) error {
	if len(expanded) == 1 {
		return p.ForwardCommandToDB(testID, expanded[0], sendReadyForQuery)
	}
	return p.SafeForwardMultipleCommandsToDB(testID, expanded, sendReadyForQuery)
}

// ForwardCommandToDB executa um único comando SQL na conexão/transação ativa.
// args são os parâmetros para query parametrizada (Extended Query); opcional.
func (p *proxyConnection) ForwardCommandToDB(testID string, query string, sendReadyForQuery bool, args ...any) error {
	session := p.server.PgRollback.GetSession(testID)
	if session == nil || session.DB == nil {
		return fmt.Errorf("sessão não encontrada para testID: %s", testID)
	}

	intercepted, err := p.server.PgRollback.InterceptQuery(testID, query, p.connectionID())
	if err != nil {
		return err
	}
	query = intercepted

	// All commands run inside the transaction (session.DB uses tx for Query/Exec).
	if !session.DB.HasActiveTransaction() {
		return fmt.Errorf("sessão sem transação ativa para testID: %s", testID)
	}

	// SELECT and INSERT/UPDATE/DELETE ... RETURNING return rows; use Query path and send result set.
	var stmt *pg_query.Node
	if stmts, parseErr := sql.ParseStatements(query); parseErr == nil && len(stmts) > 0 && stmts[0].Stmt != nil {
		stmt = stmts[0].Stmt
	}
	if stmt != nil && sql.StmtReturnsResultSet(stmt) {
		return p.ExecuteSelectQuery(testID, query, sendReadyForQuery, args...)
	}
	if stmt == nil && sql.ReturnsResultSetFallback(query) {
		return p.ExecuteSelectQuery(testID, query, sendReadyForQuery, args...)
	}

	var tag pgconn.CommandTag

	log.Printf("[PROXY] ForwardCommandToDB: Executando via transação: %s", query)
	session.DB.Gui.SetLastQuery(query)
	p.server.PgRollback.PublishSessionUpdate(testID)
	start := time.Now()
	// Finalize on every exit path (success or error) so a failed query is never left showing
	// as Running in the GUI history.
	defer func() {
		session.DB.Gui.UpdateLastQueryHistoryDuration(time.Since(start))
		p.server.PgRollback.PublishSessionUpdate(testID)
	}()

	// All TCL (SAVEPOINT, RELEASE, ROLLBACK) goes to SafeExecTCL, which runs inside a guard
	// so failed TCL does not abort the main transaction; it skips guard Commit on success
	// for ROLLBACK/ROLLBACK TO SAVEPOINT (those commands roll back past the guard).
	var cmdType string
	if stmt != nil {
		cmdType = sql.ClassifyStatement(stmt)
	} else {
		cmdType = sql.CommandTypeFromQueryFallback(query)
	}
	isTransactionControl := cmdType == "SAVEPOINT" || cmdType == "RELEASE" || cmdType == "ROLLBACK"

	isUserBegin := false
	if stmt != nil {
		isUserBegin = sql.IsSavepoint(stmt) && strings.HasPrefix(sql.GetSavepointName(stmt), "pgrollback_v_")
	} else {
		isUserBegin = IsUserBeginQuery(query)
	}

	if isTransactionControl {
		if isUserBegin {
			if err := session.DB.ClaimOpenTransaction(p.connectionID()); err != nil {
				return err
			}
		}
		tag, err = session.DB.SafeExecTCL(session.Context(), query, args...)
		if err != nil {
			affectsClaim := isUserBegin
			if stmt != nil {
				affectsClaim = affectsClaim || (sql.IsReleaseSavepoint(stmt) && strings.HasPrefix(sql.GetSavepointName(stmt), "pgrollback_v_"))
			} else {
				affectsClaim = IsQueryThatAffectsClaim(query)
			}
			if affectsClaim {
				session.DB.ReleaseOpenTransaction(p.connectionID())
			}
			log.Printf("[PROXY] Erro na execução transacional (TCL): %v", err)
			err = fmt.Errorf("falha ao executar comando TCL: %w", err)
			return err
		}
		// Apply all TCL side effects in one place: SavepointLevel, per-connection count, and release claim when count drops to 0.
		if err := p.ApplyTCLSuccessTracking(query, session); err != nil {
			return err
		}
	} else {
		tag, err = session.DB.SafeExec(session.Context(), query, args...)
		if err != nil {
			return err
		}
	}

	// Envia o CommandTag real ANTES do ReadyForQuery.
	// Aplica workaround para INSERT com oid=0 para compatibilidade com drivers.
	tagStr := tag.String()
	rowsAffected := tag.RowsAffected()
	originalTagStr := tagStr

	if strings.HasPrefix(tagStr, "INSERT 0 ") && rowsAffected >= 0 {
		tagStr = fmt.Sprintf("INSERT %d", rowsAffected)
		log.Printf("[PROXY] Workaround INSERT aplicado: '%s' -> '%s' (linhas=%d)", originalTagStr, tagStr, rowsAffected)
	}

	if tagStr != "" {
		//log.Printf("[PROXY] Enviando CommandComplete: '%s'", tagStr)
		if os.Getenv("PGROLLBACK_LOG_MESSAGE_ORDER") == "1" {
			log.Printf("[MSG_ORDER] SEND CommandComplete: %s", tagStr)
		}
		p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tagStr)})
	} else {
		//log.Printf("[PROXY] Tag vazia recebida, enviando SelectZero default")
		if os.Getenv("PGROLLBACK_LOG_MESSAGE_ORDER") == "1" {
			log.Printf("[MSG_ORDER] SEND CommandComplete: -- ping")
		}
		p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("-- ping")})
	}

	if sendReadyForQuery {
		if os.Getenv("PGROLLBACK_LOG_MESSAGE_ORDER") == "1" {
			log.Printf("[MSG_ORDER] SEND ReadyForQuery")
		}
		log.Printf("[PROXY] Enviando ReadyForQuery (Simple Query)")
		p.SendReadyForQuery()
	} else {
		log.Printf("[PROXY] Não enviando ReadyForQuery (Fluxo Estendido)")
	}
	return nil
}

// safeForwardMultipleCommandsSequential runs each statement through ForwardCommandToDB so InterceptQuery
// rewrites COMMIT/BEGIN/ROLLBACK. LockRun cannot be held across ForwardCommandToDB (SafeExec also locks d.mu).
// The batch-level history entry (and its Running flag) is finalized by the caller, SafeForwardMultipleCommandsToDB,
// via defer, on every exit path of this function.
func (p *proxyConnection) safeForwardMultipleCommandsSequential(
	testID string,
	commands []string,
	sendReadyForQuery bool,
	ctx context.Context,
	session *TestSession,
	multiCommandSavepointName string,
) error {
	session.DB.LockRun()
	if session.DB == nil {
		session.DB.UnlockRun()
		return fmt.Errorf("sessão sem DB para testID: %s", testID)
	}

	if _, err := session.DB.execTxLocked(ctx, "SAVEPOINT "+multiCommandSavepointName); err != nil {
		session.DB.UnlockRun()
		return fmt.Errorf("criar savepoint para múltiplos comandos: %w", err)
	}
	session.DB.UnlockRun()

	var nonEmpty []string
	for _, cmd := range commands {
		if t := strings.TrimSpace(cmd); t != "" {
			nonEmpty = append(nonEmpty, t)
		}
	}

	rollbackMulti := func() {
		session.DB.LockRun()
		defer session.DB.UnlockRun()
		_, _ = session.DB.execTxLocked(ctx, "ROLLBACK TO SAVEPOINT "+multiCommandSavepointName+"; RELEASE SAVEPOINT "+multiCommandSavepointName)
	}

	for i, cmd := range nonEmpty {
		last := i == len(nonEmpty)-1
		if err := p.ForwardCommandToDB(testID, cmd, sendReadyForQuery && last); err != nil {
			rollbackMulti()
			return err
		}
	}

	session.DB.LockRun()
	_, relErr := session.DB.execTxLocked(ctx, "RELEASE SAVEPOINT "+multiCommandSavepointName)
	session.DB.UnlockRun()
	if relErr != nil {
		return fmt.Errorf("release multi-command savepoint: %w", relErr)
	}

	if err := p.backend.Flush(); err != nil {
		return fmt.Errorf("falha no flush de múltiplos resultados: %w", err)
	}
	log.Printf("[PROXY] Multi-command batch executed")
	if sendReadyForQuery {
		p.SendReadyForQuery()
	}
	return nil
}

// SafeForwardMultipleCommandsToDB lida com strings contendo múltiplos comandos separados por ponto e vírgula.
// Runs the whole batch inside a savepoint: either all commands succeed (RELEASE SAVEPOINT) or none apply (ROLLBACK TO SAVEPOINT).
// The real transaction is never aborted; only the savepoint is rolled back on failure.
func (p *proxyConnection) SafeForwardMultipleCommandsToDB(testID string, commands []string, sendReadyForQuery bool) error {
	const multiCommandSavepointName = "pgrollback_multi_guard"
	session := p.server.PgRollback.GetSession(testID)
	if session == nil {
		return fmt.Errorf("sessão não encontrada para testID: '%s'", testID)
	}
	if session.DB == nil {
		return fmt.Errorf("sessão sem DB para testID: '%s'", testID)
	}
	ctx := session.Context()
	pgConn := session.DB.PgConn()
	if pgConn == nil {
		return fmt.Errorf("sessão sem conexão para testID: '%s'", testID)
	}
	if !session.DB.HasActiveTransaction() {
		return fmt.Errorf("sessão existe mas sem transaction: '%s'", testID)
	}

	// Gui methods are self-contained, safe to call before LockRun.
	fullQuery := strings.Join(commands, "; ")
	session.DB.Gui.SetLastQuery(fullQuery)
	p.server.PgRollback.PublishSessionUpdate(testID)
	if !strings.HasSuffix(fullQuery, ";") {
		fullQuery += ";"
	}
	start := time.Now()
	// Finalize the batch-level history entry on every exit path (success or error, sequential or
	// batched) so a failed batch is never left showing as Running in the GUI history.
	defer func() {
		session.DB.Gui.UpdateLastQueryHistoryDuration(time.Since(start))
		p.server.PgRollback.PublishSessionUpdate(testID)
	}()

	// Same as sequential ForwardCommandToDB: claim session before user SAVEPOINT (from BEGIN) runs.
	for _, cmd := range commands {
		c := strings.TrimSpace(cmd)
		if c == "" {
			continue
		}
		if IsUserBeginQuery(c) {
			if err := session.DB.ClaimOpenTransaction(p.connectionID()); err != nil {
				return err
			}
		}
	}

	// When we run our synthetic ROLLBACK (ROLLBACK TO SAVEPOINT; RELEASE SAVEPOINT), the client
	// sent a single "ROLLBACK" and expects a single CommandComplete("ROLLBACK") + ReadyForQuery.
	isRollbackPair := len(commands) == 2 &&
		strings.Contains(strings.ToUpper(strings.TrimSpace(commands[0])), "ROLLBACK TO SAVEPOINT ") &&
		strings.Contains(strings.ToUpper(strings.TrimSpace(commands[1])), "RELEASE SAVEPOINT ")

	if multiCommandBatchNeedsSequentialExec(commands) && !isRollbackPair {
		return p.safeForwardMultipleCommandsSequential(testID, commands, sendReadyForQuery, ctx, session, multiCommandSavepointName)
	}

	session.DB.LockRun()
	defer session.DB.UnlockRun()

	// Guard the whole batch with a savepoint: all run or none.
	if _, err := session.DB.execTxLocked(ctx, "SAVEPOINT "+multiCommandSavepointName); err != nil {
		return fmt.Errorf("criar savepoint para múltiplos comandos: %w", err)
	}

	rollbackSavepoint := func() {
		_, _ = session.DB.execTxLocked(ctx, "ROLLBACK TO SAVEPOINT "+multiCommandSavepointName+"; RELEASE SAVEPOINT "+multiCommandSavepointName)
	}

	mrr := pgConn.Exec(ctx, fullQuery)
	defer mrr.Close()

	// Track only the last command's result (may be a SELECT with rows, SELECT with no rows, or a non-SELECT).
	var lastResultRowDesc *pgproto3.RowDescription
	var lastResultRows []*pgproto3.DataRow
	var lastResultTag []byte

	for mrr.NextResult() {
		rr := mrr.ResultReader()
		if rr == nil {
			continue
		}

		fieldDescs := rr.FieldDescriptions()
		if len(fieldDescs) > 0 {
			// SELECT: keep this as the last result (rows may be empty).
			fields := protocol.ConvertFieldDescriptions(fieldDescs)
			lastResultRowDesc = &pgproto3.RowDescription{Fields: fields}
			lastResultRows = nil
			for rr.NextRow() {
				values := rr.Values()
				valuesCopy := make([][]byte, len(values))
				for i, v := range values {
					if v != nil {
						valuesCopy[i] = make([]byte, len(v))
						copy(valuesCopy[i], v)
					}
				}
				lastResultRows = append(lastResultRows, &pgproto3.DataRow{Values: valuesCopy})
			}
			tag, err := rr.Close()
			if err != nil {
				rollbackSavepoint()
				return fmt.Errorf("erro ao fechar result reader: %w", err)
			}
			lastResultTag = []byte(tag.String())
		} else {
			// Non-SELECT (UPDATE, INSERT, SET, etc): last result is just the tag.
			lastResultRowDesc = nil
			lastResultRows = nil
			tag, err := rr.Close()
			if err != nil {
				rollbackSavepoint()
				return fmt.Errorf("erro ao fechar result reader: %w", err)
			}
			lastResultTag = []byte(tag.String())
		}
	}

	// Send only the last command's result.
	if isRollbackPair {
		p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")})
	} else if lastResultRowDesc != nil {
		p.backend.Send(lastResultRowDesc)
		for _, row := range lastResultRows {
			p.backend.Send(row)
		}
		p.backend.Send(&pgproto3.CommandComplete{CommandTag: lastResultTag})
	} else if len(lastResultTag) > 0 {
		p.backend.Send(&pgproto3.CommandComplete{CommandTag: lastResultTag})
	}

	if err := mrr.Close(); err != nil {
		rollbackSavepoint()
		return fmt.Errorf("erro ao processar múltiplos resultados: %w", err)
	}

	// All commands succeeded; release savepoint so changes are kept.
	_, secondGuardErr := session.DB.execTxLocked(ctx, "SAVEPOINT "+multiCommandSavepointName+"_inside")
	if secondGuardErr == nil {
		_, firstGuardErr := session.DB.execTxLocked(ctx, "RELEASE SAVEPOINT "+multiCommandSavepointName) //It can fail due to rollback before this point
		if firstGuardErr != nil {
			_, errRollbackSecondGuard := session.DB.execTxLocked(ctx, "ROLLBACK TO SAVEPOINT "+multiCommandSavepointName+"_inside; RELEASE SAVEPOINT "+multiCommandSavepointName+"_inside")
			if errRollbackSecondGuard != nil {
				return fmt.Errorf("erro ao executar ROLLBACK TO SAVEPOINT e RELEASE SAVEPOINT: %w", errRollbackSecondGuard)
			}
		}
	} else {
		return fmt.Errorf("erro ao executar RELEASE SAVEPOINT: %w", secondGuardErr)
	}

	// Apply TCL side effects (e.g. ROLLBACK ...; RELEASE ... from handleRollback) so session level stays in sync.
	// Use Locked variant: caller holds LockRun (d.mu), so we must not call session.DB methods that take d.mu again.
	for _, cmd := range commands {
		if trimmed := strings.TrimSpace(cmd); trimmed != "" {
			_ = p.ApplyTCLSuccessTrackingLocked(trimmed, session)
		}
	}

	if err := p.backend.Flush(); err != nil {
		return fmt.Errorf("falha no flush de múltiplos resultados: %w", err)
	}

	log.Printf("[PROXY] Multi-command batch executed")
	if sendReadyForQuery {
		p.SendReadyForQuery()
	}
	return nil
}

// ExecuteSelectQuery executa um SELECT e envia os resultados. args são parâmetros (Extended Query); opcional.
func (p *proxyConnection) ExecuteSelectQuery(testID string, query string, sendReadyForQuery bool, args ...any) error {
	session := p.server.PgRollback.GetSession(testID)
	if session == nil {
		return fmt.Errorf("sessão não encontrada para testID: %s", testID)
	}
	start := time.Now()
	if session.DB != nil && query != "" {
		session.DB.Gui.SetLastQuery(query)
		p.server.PgRollback.PublishSessionUpdate(testID)
		// Finalize on every exit path (success or error) so a failed query is never left
		// showing as Running in the GUI history.
		defer func() {
			session.DB.Gui.UpdateLastQueryHistoryDuration(time.Since(start))
			p.server.PgRollback.PublishSessionUpdate(testID)
		}()
	}

	rows, err := session.DB.SafeQuery(session.Context(), query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if err := p.SendSelectResultsWithQuery(rows, query); err != nil {
		return err
	}

	log.Printf("[PROXY] Query executed")
	if sendReadyForQuery {
		p.SendReadyForQuery()
	}
	return nil
}
