package proxy

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"

	"pgtest-sandbox/pkg/protocol"
	"pgtest-sandbox/pkg/sql"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// DescribeRowFieldsForQuery returns the RowDescription fields to send for Describe (Portal/Statement)
// when the query returns a result set (e.g. INSERT/UPDATE/DELETE ... RETURNING). Clients that rely
// on Describe (e.g. PHP PDO / Laravel Eloquent) need this so they get the correct result shape and
// do not receive an empty result set. Returns nil if the query does not return rows or we cannot parse RETURNING.
func DescribeRowFieldsForQuery(query string) []pgproto3.FieldDescription {
	if query == "" || !sql.ReturnsResultSet(query) {
		return nil
	}
	cols := sql.ReturningColumns(query)
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
	re := regexp.MustCompile(`\$([0-9]+)`)
	max := 0
	for _, m := range re.FindAllStringSubmatch(query, -1) {
		if len(m) >= 2 {
			if n, err := strconv.Atoi(m[1]); err == nil && n > max {
				max = n
			}
		}
	}
	return max
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

// RunMessageLoop é o loop principal que processa as mensagens do cliente.
// Ele mantém a conexão aberta e despacha cada mensagem para o handler apropriado.
func (p *proxyConnection) RunMessageLoop(testID string) {
	defer p.clientConn.Close()
	defer func() {
		session := p.server.Pgtest.GetSession(testID)
		if session != nil && session.DB != nil {
			if count := p.GetUserOpenTransactionCount(); count > 0 {
				_ = session.DB.RollbackUserSavepointsOnDisconnect(context.Background(), count)
			}
			session.DB.ReleaseOpenTransaction(p.connectionID())
		}
	}()

	// Log para rastrear qual conexão TCP está processando mensagens
	remoteAddr := p.clientConn.RemoteAddr().String()
	log.Printf("[PROXY] Iniciando loop de mensagens (testID=%s, conn=%s)", testID, remoteAddr)
	defer log.Printf("[PROXY] Finalizando loop de mensagens (testID=%s, conn=%s)", testID, remoteAddr)

	// Extended Query protocol (e.g. pgx for QueryContext("SELECT 1")) typically sends:
	//   Parse → Describe(S) → Sync → Bind → Describe(P) → Execute → Sync
	// We forward each message to the real PostgreSQL (via the session's PgConn) and relay
	// the backend's response to the client. Query interception (BEGIN→SAVEPOINT etc.) is applied
	// at Parse time: we forward the modified Parse, so the backend never sees the raw client query.
	// Simple Query (pgproto3.Query) continues to use the pgx Tx API via ProcessSimpleQuery.
	for {
		msg, err := p.backend.Receive()
		if err != nil {
			//if err != io.EOF {
			log.Printf("[PROXY] xxxxxxx Erro ao receber mensagem do cliente (testID=%s, conn=%s): %v", testID, remoteAddr, err)
			//}
			return
		}

		switch msg := msg.(type) {
		case *pgproto3.Query:
			// Flow "Simple Query": O cliente envia uma string SQL direta.
			// Espera-se que retornemos RowDescription, DataRow(s), CommandComplete e ReadyForQuery.
			queryStr := msg.String
			log.Printf("[PROXY] Query Simples Recebida (testID=%s, conn=%s): %s", testID, remoteAddr, queryStr)
			if os.Getenv("PGTEST_LOG_MESSAGE_ORDER") == "1" {
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
			if err := p.ProcessSimpleQuery(testID, queryStr); err != nil {
				log.Printf("[PROXY] Erro ao processar Query Simples: %v", err)
				p.SendErrorResponse(err)
			} else {
				log.Printf("[PROXY] Query Simples processada com sucesso: %s", queryStr)
			}
			p.backend.Flush()

		case *pgproto3.Parse:
			// Extended Query: intercept query, store in session, call PgConn.Prepare() to create
			// the statement on the real PostgreSQL and cache the StatementDescription.
			session := p.server.Pgtest.GetSession(testID)
			if session == nil || session.DB == nil || session.DB.PgConn() == nil {
				p.SendErrorResponse(fmt.Errorf("sessão não encontrada para testID: %s", testID))
				continue
			}
			interceptedQuery, err := p.server.Pgtest.InterceptQuery(testID, msg.Query, p.connectionID())
			if err != nil {
				p.SendErrorResponse(err)
				continue
			}
			session.DB.SetPreparedStatement(msg.Name, interceptedQuery)
			// Use PgConn.Prepare() which sends Parse+Describe(S)+Sync internally and handles
			// the bg reader / lock correctly. This creates the prepared statement on PostgreSQL
			// and gives us the StatementDescription (param OIDs, field descriptions).
			//
			// If the statement already exists on PostgreSQL (e.g. pgx reuses statement names
			// across proxy connections sharing the same PgConn), deallocate it first.
			if msg.Name != "" {
				if existingSD := session.DB.GetStatementDescription(msg.Name); existingSD != nil {
					_ = session.DB.PgConn().Deallocate(context.Background(), msg.Name)
				}
			}
			sd, prepErr := session.DB.PgConn().Prepare(context.Background(), msg.Name, interceptedQuery, msg.ParameterOIDs)
			if prepErr != nil {
				log.Printf("[PROXY] Prepare failed: %v", prepErr)
				p.SendErrorResponse(prepErr)
				continue
			}
			session.DB.SetStatementDescription(msg.Name, sd)
			p.backend.Send(&pgproto3.ParseComplete{})
			p.backend.Flush()

		case *pgproto3.Bind:
			// Store portal mapping locally. The actual Bind to PostgreSQL will happen when
			// Execute arrives (via ExecPrepared which sends its own Bind).
			if session := p.server.Pgtest.GetSession(testID); session != nil && session.DB != nil {
				session.DB.BindPortal(msg.DestinationPortal, msg.PreparedStatement, msg.Parameters, msg.ParameterFormatCodes, msg.ResultFormatCodes)
			}
			p.backend.Send(&pgproto3.BindComplete{})
			p.backend.Flush()

		case *pgproto3.Execute:
			// Execute the prepared statement via PgConn.ExecPrepared() which sends
			// Bind+Describe(P)+Execute+Sync internally and returns a ResultReader.
			session := p.server.Pgtest.GetSession(testID)
			if session == nil || session.DB == nil || session.DB.PgConn() == nil {
				p.SendErrorResponse(fmt.Errorf("sessão não encontrada para testID: %s", testID))
				continue
			}
			stmtName := session.DB.PortalStatementName(msg.Portal)
			query, params, formatCodes, ok := session.DB.QueryForPortal(msg.Portal)
			if !ok {
				p.SendErrorResponse(fmt.Errorf("portal ou statement não encontrado para execução (portal=%q)", msg.Portal))
				continue
			}
			// Record extended-query in session for GUI (last query + history), with params substituted for display.
			if query != "" {
				args := bindParamsToArgs(params, formatCodes)
				session.DB.SetLastQueryWithParams(query, args)
			}
			resultFormats := session.DB.PortalResultFormats(msg.Portal)
			if err := p.executeViaExecPrepared(context.Background(), session.DB.PgConn(), stmtName, params, formatCodes, resultFormats); err != nil {
				log.Printf("[PROXY] ExecPrepared failed: %v", err)
				p.SendErrorResponse(err)
			}

		case *pgproto3.Describe:
			// Use the cached StatementDescription from PgConn.Prepare() to respond with real
			// ParameterDescription + RowDescription/NoData from PostgreSQL.
			session := p.server.Pgtest.GetSession(testID)
			if session == nil || session.DB == nil {
				p.SendErrorResponse(fmt.Errorf("sessão não encontrada para testID: %s", testID))
				continue
			}
			var sd *pgconn.StatementDescription
			var resultFormats []int16
			if msg.ObjectType == 'S' {
				sd = session.DB.GetStatementDescription(msg.Name)
			} else {
				sd = session.DB.GetStatementDescriptionForPortal(msg.Name)
				resultFormats = session.DB.PortalResultFormats(msg.Name)
			}
			if sd == nil {
				p.SendErrorResponse(fmt.Errorf("statement description not found for Describe (objectType=%c, name=%q)", msg.ObjectType, msg.Name))
				continue
			}
			p.sendDescribeFromSD(sd, msg.ObjectType, resultFormats)

		case *pgproto3.Sync:
			// The real Sync+ReadyForQuery were already consumed by PgConn.Prepare() or ExecPrepared().
			// We just send a synthetic ReadyForQuery to the client.
			p.SendReadyForQuery()
			//p.backend.Flush()

		case *pgproto3.Terminate:
			return

		case *pgproto3.Flush:
			log.Printf("[PROXY] Flush recebido (testID=%s, conn=%s)", testID, remoteAddr)
			p.backend.Flush()

		case *pgproto3.Close:
			// Deallocate the statement on PostgreSQL (for statement close) and clean up local state.
			session := p.server.Pgtest.GetSession(testID)
			if session != nil && session.DB != nil {
				if msg.ObjectType == 'S' && session.DB.PgConn() != nil {
					if err := session.DB.PgConn().Deallocate(context.Background(), msg.Name); err != nil {
						log.Printf("[PROXY] Deallocate failed: %v", err)
					}
				}
				session.DB.CloseStatementOrPortal(msg.ObjectType, msg.Name)
			}
			p.backend.Send(&pgproto3.CloseComplete{})
			p.backend.Flush()

		case *pgproto3.CopyData:
			// Mensagens de tráfego de dados (COPY). Ignoramos no log para evitar spam,
			// mas mantemos o fallback seguro de enviar ReadyForQuery para não travar.
			log.Printf("[PROXY] CopyData ignorado (testID=%s, conn=%s)", testID, remoteAddr)
			p.SendReadyForQuery()
			p.backend.Flush()

		default:
			// Captura qualquer outra mensagem não tratada explicitamente.
			log.Printf("[PROXY] ----------------- Mensagem não tratada: %T (testID=%s, conn=%s) - Enviando ReadyForQuery como fallback", msg, testID, remoteAddr)
			p.SendReadyForQuery()
			p.backend.Flush()
		}
	}
}

// ProcessSimpleQuery lida com o fluxo de "Simple Query" (pgproto3.Query).
// Intercepta o SQL, executa e garante o envio de ReadyForQuery ao final via executeQuery(..., true).
func (p *proxyConnection) ProcessSimpleQuery(testID string, query string) error {
	session := p.server.Pgtest.GetSession(testID)
	if session == nil {
		return fmt.Errorf("sessão não encontrada para testID: %s", testID)
	}
	interceptedQuery, err := p.server.Pgtest.InterceptQuery(testID, query, p.connectionID())
	if err != nil {
		return err
	}

	if interceptedQuery == FULLROLLBACK_SENTINEL && session.DB != nil {
		session.DB.ClearLastQuery()
	}
	// Se a interceptação "engoliu" a query (retornou vazia ou marcador), apenas finalizamos.
	// Isso acontece com comandos pgtest internos ou quando queremos silenciar uma query.
	if interceptedQuery == "" || interceptedQuery == FULLROLLBACK_SENTINEL {
		if os.Getenv("PGTEST_LOG_MESSAGE_ORDER") == "1" {
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

//// ProcessExtendedQuery lida com a fase de execução do fluxo estendido (pgproto3.Execute).
//// Executa a query com os parâmetros bound; NÃO envia ReadyForQuery, pois o cliente enviará um Sync depois.
//func (p *proxyConnection) ProcessExtendedQuery(testID string, query string, params [][]byte, formatCodes []int16) error {
//	if p.server.Pgtest.GetSession(testID) == nil {
//		return fmt.Errorf("sessão não encontrada para testID: %s", testID)
//	}
//
//	interceptedQuery, err := p.server.Pgtest.InterceptQuery(testID, query, p.connectionID())
//	if err != nil {
//		return err
//	}
//
//	if interceptedQuery == "" || interceptedQuery == "-- intercepted" {
//		p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT")})
//		return nil
//	}
//
//	args := bindParamsToArgs(params, formatCodes)
//	// false = NÃO enviar ReadyForQuery (esperar Sync)
//	return p.ExecuteInterpretedQuery(testID, interceptedQuery, false, args...)
//}
