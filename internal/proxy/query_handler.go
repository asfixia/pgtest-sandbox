package proxy

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"pgtest-transient/pkg/protocol"
	"pgtest-transient/pkg/sql"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// ExecuteInterpretedQuery recebe uma query que já passou por parse e interceptação.
// Ela decide se é comando único ou múltiplos comandos e encaminha para o banco.
//
// sendReadyForQuery:
//   - true para fluxo "Simple Query" (envia ReadyForQuery ao final).
//   - false para fluxo "Extended Query" (não envia, espera-se recebimento de Sync depois).
func (p *proxyConnection) ExecuteInterpretedQuery(testID string, query string, sendReadyForQuery bool) error {
	commands := sql.SplitCommands(query)
	if len(commands) > 1 {
		return p.ForwardMultipleCommandsToDB(testID, commands, sendReadyForQuery)
	}
	return p.ForwardCommandToDB(testID, query, sendReadyForQuery)
}

// ForwardCommandToDB executa um único comando SQL na conexão/transação ativa.
func (p *proxyConnection) ForwardCommandToDB(testID string, query string, sendReadyForQuery bool) error {
	session := p.server.Pgtest.GetSession(testID)
	if session == nil || session.DB == nil {
		return fmt.Errorf("sessão não encontrada para testID: %s", testID)
	}

	// All commands run inside the transaction (session.DB uses tx for Query/Exec).
	if !session.DB.HasActiveTransaction() {
		return fmt.Errorf("sessão sem transação ativa para testID: %s", testID)
	}

	if sql.IsSelect(query) {
		return p.ExecuteSelectQuery(testID, query, sendReadyForQuery)
	}

	var tag pgconn.CommandTag
	var err error

	log.Printf("[PROXY] ForwardCommandToDB: Executando via transação: %s", query)

	// All TCL (SAVEPOINT, RELEASE, ROLLBACK) goes to SafeExecTCL, which runs inside a guard
	// so failed TCL does not abort the main transaction; it skips guard Commit on success
	// for ROLLBACK/ROLLBACK TO SAVEPOINT (those commands roll back past the guard).
	cmdType := sql.AnalyzeCommand(query).Type
	isTransactionControl := cmdType == "SAVEPOINT" || cmdType == "RELEASE" || cmdType == "ROLLBACK"

	if isTransactionControl {
		tag, err = session.DB.SafeExecTCL(context.Background(), query)
		if err != nil {
			log.Printf("[PROXY] Erro na execução transacional (TCL): %v", err)
			err = fmt.Errorf("falha ao executar comando TCL: %w", err)
			return err
		}
	} else {
		tag, err = session.DB.SafeExec(context.Background(), query)
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

	fullQuery := strings.Join(commands, "; ")
	if !strings.HasSuffix(fullQuery, ";") {
		fullQuery += ";"
	}

	pgConn := session.DB.PgConn()
	if pgConn == nil {
		return fmt.Errorf("sessão sem conexão para testID: '%s'", testID)
	}
	if !session.DB.HasActiveTransaction() {
		return fmt.Errorf("sessão existe mas sem transaction: '%s'", testID)
	}
	//mrr := pgConn.Exec(context.Background(), "savepoint ")

	mrr := pgConn.Exec(context.Background(), fullQuery)
	defer mrr.Close()

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
				return fmt.Errorf("erro ao fechar result reader: %w", err)
			}

			if rowCount > 0 {
				lastSelectResult = rowDesc
				lastSelectRows = rows
				lastSelectTag = []byte(tag.String())
			}
		} else {
			// Comando sem retorno de linhas (UPDATE, INSERT, SET, etc).
			// Envia o CommandComplete imediatamente.
			tag, err := rr.Close()
			if err != nil {
				return fmt.Errorf("erro ao fechar result reader: %w", err)
			}
			if tagStr := tag.String(); tagStr != "" {
				p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tagStr)})
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
	}

	if err := mrr.Close(); err != nil {
		return fmt.Errorf("erro ao processar múltiplos resultados: %w", err)
	}

	if err := p.backend.Flush(); err != nil {
		return fmt.Errorf("falha no flush de múltiplos resultados: %w", err)
	}

	if sendReadyForQuery {
		p.SendReadyForQuery()
	}
	return nil
}

//func (p *proxyConnection) ExecuteSelectQueryFromPreparedStatement(testID string, preparedStatement string, sendReadyForQuery bool) (pgx.Rows, error) {
//	session := p.server.Pgtest.GetSession(testID)
//	if session == nil {
//		return nil, fmt.Errorf("sessão não encontrada para testID: %s", testID)
//	}
//
//	query := fmt.Sprintf(`
//		SELECT
//			statement
//		FROM pg_prepared_statements
//		WHERE name = '%s';
//		`, preparedStatement)
//	uqSavepointID := fmt.Sprintf("pgtest_exec_guard_%s", rand.Int31())
//	rows, err := SafeQuery(context.Background(), session.DB, uqSavepointID, query)
//	//if err != nil {
//	//	return err
//	//}
//	//return p.ExecuteSelectQuery(tag.String(), sendReadyForQuery)
//	return rows, err
//}

// ExecuteSelectQuery executa um SELECT simples e envia os resultados.
func (p *proxyConnection) ExecuteSelectQuery(testID string, query string, sendReadyForQuery bool) error {
	session := p.server.Pgtest.GetSession(testID)
	if session == nil {
		return fmt.Errorf("sessão não encontrada para testID: %s", testID)
	}

	rows, err := session.DB.SafeQuery(context.Background(), query)
	if err != nil {
		return err
	}
	defer rows.Close()

	if err := p.SendSelectResults(rows); err != nil {
		return err
	}

	if sendReadyForQuery {
		p.SendReadyForQuery()
	}
	return nil
}
