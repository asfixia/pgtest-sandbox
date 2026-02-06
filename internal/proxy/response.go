package proxy

import (
	"fmt"
	"log"
	"os"

	"pgtest-transient/pkg/protocol"
	"pgtest-transient/pkg/sql"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// SendSelectResults itera sobre as linhas de um resultado pgx e envia para o cliente.
// Envia RowDescription e DataRow(s), seguido de CommandComplete.
func (p *proxyConnection) SendSelectResults(rows pgx.Rows) error {
	fieldDescs := rows.FieldDescriptions()
	fields := protocol.ConvertFieldDescriptions(fieldDescs)
	if os.Getenv("PGTEST_LOG_MESSAGE_ORDER") == "1" {
		log.Printf("[MSG_ORDER] SEND RowDescription: %d cols", len(fields))
	}
	p.backend.Send(&pgproto3.RowDescription{Fields: fields})

	rowCount := 0
	for rows.Next() {
		rowCount++
		// Usa RawValues() para obter bytes brutos no formato correto (text ou binary)
		// Isso evita problemas de conversão, pois já vem no formato do protocolo PostgreSQL
		rawValues := rows.RawValues()
		p.backend.Send(&pgproto3.DataRow{Values: rawValues})
	}
	if os.Getenv("PGTEST_LOG_MESSAGE_ORDER") == "1" {
		log.Printf("[MSG_ORDER] SEND DataRows: %d", rowCount)
		log.Printf("[MSG_ORDER] SEND CommandComplete: SELECT %d", rowCount)
	}
	p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", rowCount))})
	if err := p.backend.Flush(); err != nil {
		return fmt.Errorf("falha no flush dos resultados do select: %w", err)
	}
	return nil
}

// SendCommandComplete envia a mensagem de completamento de comando.
func (p *proxyConnection) SendCommandComplete(cmd string) {
	tag := sql.GetCommandTag(cmd)
	p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
}

// SendReadyForQuery envia a mensagem ReadyForQuery ('I' = Idle) e força o flush.
// Essencial para manter o protocolo sincronizado no fluxo de Query Simples.
func (p *proxyConnection) SendReadyForQuery() {
	p.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
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
