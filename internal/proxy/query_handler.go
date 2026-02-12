package proxy

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"pgtest-sandbox/pkg/protocol"
	"pgtest-sandbox/pkg/sql"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// ExecuteInterpretedQuery recebe uma query que já passou por parse e interceptação.
// Ela decide se é comando único ou múltiplos comandos e encaminha para o banco.
// args são os parâmetros bound (Extended Query); para Simple Query não passar args.
//
// sendReadyForQuery:
//   - true para fluxo "Simple Query" (envia ReadyForQuery ao final).
//   - false para fluxo "Extended Query" (não envia, espera-se recebimento de Sync depois).
func (p *proxyConnection) ExecuteInterpretedQuery(testID string, query string, sendReadyForQuery bool, args ...any) error {
	commands := sql.SplitCommands(query)
	if len(commands) > 1 {
		return p.ForwardMultipleCommandsToDB(testID, commands, sendReadyForQuery)
	}
	return p.ForwardCommandToDB(testID, query, sendReadyForQuery, args...)
}

// ForwardCommandToDB executa um único comando SQL na conexão/transação ativa.
// args são os parâmetros para query parametrizada (Extended Query); opcional.
func (p *proxyConnection) ForwardCommandToDB(testID string, query string, sendReadyForQuery bool, args ...any) error {
	session := p.server.Pgtest.GetSession(testID)
	if session == nil || session.DB == nil {
		return fmt.Errorf("sessão não encontrada para testID: %s", testID)
	}

	// All commands run inside the transaction (session.DB uses tx for Query/Exec).
	if !session.DB.HasActiveTransaction() {
		return fmt.Errorf("sessão sem transação ativa para testID: %s", testID)
	}

	// SELECT and INSERT/UPDATE/DELETE ... RETURNING return rows; use Query path and send result set.
	if sql.ReturnsResultSet(query) {
		return p.ExecuteSelectQuery(testID, query, sendReadyForQuery, args...)
	}

	var tag pgconn.CommandTag
	var err error

	log.Printf("[PROXY] ForwardCommandToDB: Executando via transação: %s", query)
	session.DB.SetLastQuery(query)

	// All TCL (SAVEPOINT, RELEASE, ROLLBACK) goes to SafeExecTCL, which runs inside a guard
	// so failed TCL does not abort the main transaction; it skips guard Commit on success
	// for ROLLBACK/ROLLBACK TO SAVEPOINT (those commands roll back past the guard).
	cmdType := sql.AnalyzeCommand(query).Type
	isTransactionControl := cmdType == "SAVEPOINT" || cmdType == "RELEASE" || cmdType == "ROLLBACK"

	if isTransactionControl {
		if IsUserBeginQuery(query) {
			if err := session.DB.ClaimOpenTransaction(p.connectionID()); err != nil {
				return err
			}
		}
		tag, err = session.DB.SafeExecTCL(context.Background(), query, args...)
		if err != nil {
			if IsQueryThatAffectsClaim(query) {
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
		tag, err = session.DB.SafeExec(context.Background(), query, args...)
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
		if os.Getenv("PGTEST_LOG_MESSAGE_ORDER") == "1" {
			log.Printf("[MSG_ORDER] SEND CommandComplete: %s", tagStr)
		}
		p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tagStr)})
	} else {
		//log.Printf("[PROXY] Tag vazia recebida, enviando SelectZero default")
		if os.Getenv("PGTEST_LOG_MESSAGE_ORDER") == "1" {
			log.Printf("[MSG_ORDER] SEND CommandComplete: -- ping")
		}
		p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("-- ping")})
	}

	if sendReadyForQuery {
		if os.Getenv("PGTEST_LOG_MESSAGE_ORDER") == "1" {
			log.Printf("[MSG_ORDER] SEND ReadyForQuery")
		}
		log.Printf("[PROXY] Enviando ReadyForQuery (Simple Query)")
		p.SendReadyForQuery()
	} else {
		log.Printf("[PROXY] Não enviando ReadyForQuery (Fluxo Estendido)")
	}
	return nil
}

// ForwardMultipleCommandsToDB lida com strings contendo múltiplos comandos separados por ponto e vírgula.
func (p *proxyConnection) ForwardMultipleCommandsToDB(testID string, commands []string, sendReadyForQuery bool) error {
	session := p.server.Pgtest.GetSession(testID)
	if session == nil {
		return fmt.Errorf("sessão não encontrada para testID: '%s'", testID)
	}

	pgConn := session.DB.PgConn()
	if pgConn == nil {
		return fmt.Errorf("sessão sem conexão para testID: '%s'", testID)
	}
	if !session.DB.HasActiveTransaction() {
		return fmt.Errorf("sessão existe mas sem transaction: '%s'", testID)
	}

	// SetLastQuery uses d.mu.Lock(); do not call it while holding LockRun() (same mutex) to avoid deadlock.
	fullQuery := strings.Join(commands, "; ")
	session.DB.SetLastQuery(fullQuery)
	if !strings.HasSuffix(fullQuery, ";") {
		fullQuery += ";"
	}
	session.DB.LockRun()

	mrr := pgConn.Exec(context.Background(), fullQuery)
	defer mrr.Close()

	// When we run our synthetic ROLLBACK (ROLLBACK TO SAVEPOINT; RELEASE SAVEPOINT), the client
	// sent a single "ROLLBACK" and expects a single CommandComplete("ROLLBACK") + ReadyForQuery.
	// We must not send two CommandComplete tags or the driver can hang.
	isRollbackPair := len(commands) == 2 &&
		strings.Contains(strings.ToUpper(strings.TrimSpace(commands[0])), "ROLLBACK TO SAVEPOINT ") &&
		strings.Contains(strings.ToUpper(strings.TrimSpace(commands[1])), "RELEASE SAVEPOINT ")

	var lastSelectResult *pgproto3.RowDescription
	var lastSelectRows []*pgproto3.DataRow
	var lastSelectTag []byte

	// Itera sobre todos os resultados
	for mrr.NextResult() {
		rr := mrr.ResultReader()
		if rr == nil {
			continue
		}

		fieldDescs := rr.FieldDescriptions()
		if len(fieldDescs) > 0 {
			// É um SELECT. O protocolo simples do Postgres normalmente retorna apenas
			// o resultado do último comando se forem múltiplos SELECTs, ou todos se o cliente suportar.
			// Aqui acumulamos para enviar o último (comportamento comum de drivers simples).
			fields := protocol.ConvertFieldDescriptions(fieldDescs)
			rowDesc := &pgproto3.RowDescription{Fields: fields}
			var rows []*pgproto3.DataRow

			rowCount := 0
			for rr.NextRow() {
				rowCount++
				values := rr.Values()
				valuesCopy := make([][]byte, len(values))
				for i, v := range values {
					if v != nil {
						valuesCopy[i] = make([]byte, len(v))
						copy(valuesCopy[i], v)
					}
				}
				rows = append(rows, &pgproto3.DataRow{Values: valuesCopy})
			}

			tag, err := rr.Close()
			if err != nil {
				session.DB.UnlockRun()
				return fmt.Errorf("erro ao fechar result reader: %w", err)
			}

			if rowCount > 0 {
				lastSelectResult = rowDesc
				lastSelectRows = rows
				lastSelectTag = []byte(tag.String())
			}
		} else {
			// Comando sem retorno de linhas (UPDATE, INSERT, SET, etc).
			tag, err := rr.Close()
			if err != nil {
				session.DB.UnlockRun()
				return fmt.Errorf("erro ao fechar result reader: %w", err)
			}
			// For our rollback pair, do not send per-command tags; we send a single "ROLLBACK" below.
			if !isRollbackPair {
				if tagStr := tag.String(); tagStr != "" {
					p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tagStr)})
				}
			}
		}
	}

	// Se houve algum SELECT com resultados, envia agora o último acumulado.
	if lastSelectResult != nil {
		p.backend.Send(lastSelectResult)
		for _, row := range lastSelectRows {
			p.backend.Send(row)
		}
		p.backend.Send(&pgproto3.CommandComplete{CommandTag: lastSelectTag})
	} else if isRollbackPair {
		// Client sent one ROLLBACK; respond with one CommandComplete.
		p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")})
	}

	if err := mrr.Close(); err != nil {
		session.DB.UnlockRun()
		return fmt.Errorf("erro ao processar múltiplos resultados: %w", err)
	}

	// Release run lock before TCL tracking: ApplyTCLSuccessTracking calls DecrementSavepointLevel etc., which take session.DB.mu.
	session.DB.UnlockRun()

	// Apply TCL side effects (e.g. ROLLBACK ...; RELEASE ... from handleRollback) so session level stays in sync.
	for _, cmd := range commands {
		if trimmed := strings.TrimSpace(cmd); trimmed != "" {
			_ = p.ApplyTCLSuccessTracking(trimmed, session)
		}
	}

	if err := p.backend.Flush(); err != nil {
		return fmt.Errorf("falha no flush de múltiplos resultados: %w", err)
	}

	if sendReadyForQuery {
		p.SendReadyForQuery()
	}
	return nil
}

// ExecuteSelectQuery executa um SELECT e envia os resultados. args são parâmetros (Extended Query); opcional.
func (p *proxyConnection) ExecuteSelectQuery(testID string, query string, sendReadyForQuery bool, args ...any) error {
	session := p.server.Pgtest.GetSession(testID)
	if session == nil {
		return fmt.Errorf("sessão não encontrada para testID: %s", testID)
	}
	if session.DB != nil && query != "" {
		session.DB.SetLastQuery(query)
	}

	rows, err := session.DB.SafeQuery(context.Background(), query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if err := p.SendSelectResultsWithQuery(rows, query); err != nil {
		return err
	}

	if sendReadyForQuery {
		p.SendReadyForQuery()
	}
	return nil
}
