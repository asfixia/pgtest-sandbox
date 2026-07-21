package proxy

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"pgrollback/pkg/protocol"
	"pgrollback/pkg/sql"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// SendSelectResults itera sobre as linhas de um resultado pgx e envia para o cliente.
// Envia RowDescription e DataRow(s), seguido de CommandComplete.
func (p *proxyConnection) SendSelectResults(rows pgx.Rows) (time.Duration, error) {
	return p.SendSelectResultsWithQuery(rows, "")
}

// resolveFieldDescriptions determines the RowDescription fields and optional return OIDs
// for a query result. It parses the query to detect RETURNING clauses and builds synthetic
// field descriptions when needed; otherwise falls back to the backend's FieldDescriptions.
func resolveFieldDescriptions(query string, rows pgx.Rows) (fields []pgproto3.FieldDescription, returnOIDs []uint32, returnsSet bool) {
	var cols []sql.ReturningColumn
	if query != "" {
		if stmts, err := sql.ParseStatements(query); err == nil && len(stmts) > 0 && stmts[0].Stmt != nil {
			stmt := stmts[0].Stmt
			returnsSet = sql.StmtReturnsResultSet(stmt)
			cols = sql.GetReturningColumns(stmt)
		} else {
			returnsSet = sql.ReturnsResultSetFallback(query)
			cols = sql.ReturningColumnsFallback(query)
		}
	}
	if returnsSet && len(cols) > 0 {
		// Use synthetic RowDescription (name, type, Format 0) so client gets consistent result.
		names := make([]string, len(cols))
		oids := make([]uint32, len(cols))
		for i, c := range cols {
			names[i] = c.Name
			oids[i] = c.OID
		}
		fields = protocol.FieldDescriptionsFromNamesAndOIDs(names, oids)
		returnOIDs = oids
	}
	if fields == nil {
		fieldDescs := rows.FieldDescriptions()
		// Single-column result: ensure correct name and text format for Eloquent/PHP.
		// Backend sometimes returns a truncated query as column name for RETURNING; use canonical "id" + text.
		// Also handle pgx returning empty FieldDescriptions before Next(): single int column is typically RETURNING id.
		if len(fieldDescs) == 1 {
			oid := fieldDescs[0].DataTypeOID
			if oid == 20 || oid == 23 || strings.Contains(strings.ToUpper(fieldDescs[0].Name), "RETURNING") {
				if oid != 20 && oid != 23 {
					oid = 20
				}
				fields = protocol.FieldDescriptionsFromNamesAndOIDs([]string{"id"}, []uint32{oid})
				returnOIDs = []uint32{oid}
			}
		}
		if fields == nil {
			fields = protocol.ConvertFieldDescriptions(fieldDescs)
		}
	}
	return fields, returnOIDs, returnsSet
}

// SendSelectResultsWithQuery envia resultados; se query tiver RETURNING, usa o mesmo RowDescription
// sintético do Describe para que clientes (ex.: PHP PDO) que dependem da consistência recebam a linha.
//
// Returns dbWait: time spent inside rows.Next() (waiting on/reading from PostgreSQL), separate from
// the row-conversion and backend.Send() calls in the same loop (proxy-side work), so callers can
// report accurate DB-vs-proxy-overhead timing even though both happen interleaved per row.
func (p *proxyConnection) SendSelectResultsWithQuery(rows pgx.Rows, query string) (time.Duration, error) {
	fields, returnOIDs, returnsSet := resolveFieldDescriptions(query, rows)
	if os.Getenv("PGROLLBACK_LOG_MESSAGE_ORDER") == "1" {
		log.Printf("[MSG_ORDER] SEND RowDescription: %d cols", len(fields))
	}
	p.backend.Send(&pgproto3.RowDescription{Fields: fields})

	var dbWait time.Duration
	rowCount := 0
	for {
		t0 := time.Now()
		hasNext := rows.Next()
		dbWait += time.Since(t0)
		if !hasNext {
			break
		}
		rowCount++
		rawValues := rows.RawValues()
		if len(returnOIDs) > 0 && len(rawValues) == len(returnOIDs) {
			// Synthetic RowDescription uses Format 0 (text); convert binary backend values to text
			textValues := make([][]byte, len(rawValues))
			for i, raw := range rawValues {
				oid := uint32(25)
				if i < len(returnOIDs) {
					oid = returnOIDs[i]
				}
				textValues[i] = protocol.RawValueToText(oid, raw)
			}
			rawValues = textValues
		}
		p.backend.Send(&pgproto3.DataRow{Values: rawValues})
	}
	if query != "" && returnsSet && rowCount == 0 {
		preview := strings.TrimSpace(query)
		if len(preview) > 120 {
			preview = preview[:120] + "..."
		}
		log.Printf("[PROXY] INSERT/UPDATE/DELETE RETURNING returned 0 rows (cols=%d); client may get empty result; query: %s", len(fields), preview)
	}
	if os.Getenv("PGROLLBACK_LOG_MESSAGE_ORDER") == "1" {
		log.Printf("[MSG_ORDER] SEND DataRows: %d", rowCount)
		log.Printf("[MSG_ORDER] SEND CommandComplete: SELECT %d", rowCount)
	}
	p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", rowCount))})
	if err := p.backend.Flush(); err != nil {
		return dbWait, fmt.Errorf("falha no flush dos resultados do select: %w", err)
	}
	return dbWait, nil
}

// SendCommandComplete envia a mensagem de completamento de comando.
func (p *proxyConnection) SendCommandComplete(cmd string) {
	tag := sql.GetCommandTagFallback(cmd)
	p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
}

// ReadyForQueryTxStatus returns the transaction status byte for ReadyForQuery.
// 'I' = idle, 'T' = in transaction. Used so libpq's PQtransactionStatus() (and thus PDO's
// pdo_is_in_transaction()) matches the connection's user-transaction count. Exported for tests.
func (p *proxyConnection) ReadyForQueryTxStatus() byte {
	if p.GetUserOpenTransactionCount() > 0 {
		return 'T'
	}
	return 'I'
}

// SendReadyForQuery sends a ReadyForQuery message and flushes.
// The TxStatus byte drives libpq's PQtransactionStatus() and therefore PDO's
// pdo_is_in_transaction() check. We send:
//   - 'T' (in transaction) when the connection has open user transactions (userOpenTransactionCount > 0)
//   - 'I' (idle)           when no user transaction is active
//
// This ensures PDO/libpq see the correct transaction state after BEGIN and COMMIT/ROLLBACK.
func (p *proxyConnection) SendReadyForQuery() {
	status := p.ReadyForQueryTxStatus()
	p.backend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
	if err := p.backend.Flush(); err != nil {
		log.Printf("[PROXY] Erro no flush do ReadyForQuery: %v", err)
	}
}

// SendErrorResponse constrói e envia uma mensagem de erro PostgreSQL padrão.
// Seguido por ReadyForQuery para garantir que o cliente possa continuar.
func (p *proxyConnection) SendErrorResponse(err error) {
	p.backend.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Message:  err.Error(),
		Code:     "XX000", // Internal Error como default
	})
	p.SendReadyForQuery()
}

// sendExtendedQueryErr sends an ErrorResponse for an extended-query pipeline message (Parse,
// Describe, Bind, Execute) WITHOUT ReadyForQuery. ReadyForQuery is sent only on Sync.
// It also caches the error so subsequent messages in the same pipeline cycle (before Sync)
// are short-circuited with the original error, preventing confusing secondary errors.
func (p *proxyConnection) sendExtendedQueryErr(err error) {
	p.extendedQueryPendingError = err
	p.backend.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Message:  err.Error(),
		Code:     "XX000",
	})
	p.backend.Flush()
}
