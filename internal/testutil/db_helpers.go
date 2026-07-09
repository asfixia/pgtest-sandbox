package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// MetadataQueryTimeout bounds catalog/metadata lookups (table existence, etc.).
// These reads can block when DDL or long transactions hold locks on pg_catalog.
const MetadataQueryTimeout = 5 * time.Second

// DBExecutor é uma interface comum para executar queries SQL.
// Aceita tanto *sql.DB quanto pgx.Tx através de type assertion.
type DBExecutor interface{}

// pgxResult adapta pgconn.CommandTag para sql.Result
type pgxResult struct {
	tag pgconn.CommandTag
}

func (r *pgxResult) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("LastInsertId not supported for pgx")
}

func (r *pgxResult) RowsAffected() (int64, error) {
	return r.tag.RowsAffected(), nil
}

// execQuery executa uma query SQL usando o executor apropriado (*sql.DB ou pgx.Tx).
func execQuery(ctx context.Context, executor DBExecutor, query string, args ...interface{}) (sql.Result, error) {
	switch e := executor.(type) {
	case *sql.DB:
		return e.ExecContext(ctx, query, args...)
	case pgx.Tx:
		tag, err := e.Exec(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		return &pgxResult{tag: tag}, nil
	default:
		return nil, fmt.Errorf("unsupported executor type: %T", executor)
	}
}

// queryRow executa uma query SQL que retorna uma linha usando o executor apropriado.
func queryRow(ctx context.Context, executor DBExecutor, query string, args ...interface{}) interface{} {
	switch e := executor.(type) {
	case *sql.DB:
		return e.QueryRowContext(ctx, query, args...)
	case pgx.Tx:
		return e.QueryRow(ctx, query, args...)
	default:
		panic(fmt.Sprintf("unsupported executor type: %T", executor))
	}
}

// scanRow escaneia uma linha do resultado (funciona tanto com *sql.Row quanto com pgx.Row).
func scanRow(row interface{}, dest ...interface{}) error {
	switch r := row.(type) {
	case *sql.Row:
		return r.Scan(dest...)
	case pgx.Row:
		return r.Scan(dest...)
	default:
		return fmt.Errorf("unsupported row type: %T", row)
	}
}

// CreateTable cria uma tabela com as colunas especificadas.
func CreateTable(t *testing.T, executor DBExecutor, tableName string, columns string) {
	t.Helper()
	createTableQuery := fmt.Sprintf("CREATE TABLE %s (%s)", tableName, columns)
	ctx := context.Background()
	_, err := execQuery(ctx, executor, createTableQuery)
	if err != nil {
		t.Fatalf("Failed to create table %s: %v", tableName, err)
	}
}

// CreateTableWithIdAndName cria uma tabela com colunas (id SERIAL PRIMARY KEY, name VARCHAR(100)).
func CreateTableWithIdAndName(t *testing.T, executor DBExecutor, tableName string) {
	t.Helper()
	CreateTable(t, executor, tableName, "id SERIAL PRIMARY KEY, name VARCHAR(100)")
}

// CreateTableWithIdAndData cria uma tabela com colunas (id SERIAL PRIMARY KEY, data VARCHAR(100)).
func CreateTableWithIdAndData(t *testing.T, executor DBExecutor, tableName string) {
	t.Helper()
	CreateTable(t, executor, tableName, "id SERIAL PRIMARY KEY, data VARCHAR(100)")
}

// CreateTableWithId cria uma tabela com apenas coluna id INT.
func CreateTableWithId(t *testing.T, executor DBExecutor, tableName string) {
	t.Helper()
	CreateTable(t, executor, tableName, "id INT")
}

// CreateTableWithValueColumn cria uma tabela com colunas (id SERIAL PRIMARY KEY, value TEXT).
func CreateTableWithValueColumn(t *testing.T, executor DBExecutor, tableName string) {
	t.Helper()
	CreateTable(t, executor, tableName, "id SERIAL PRIMARY KEY, value TEXT")
}

// InsertRow insere uma linha na tabela com os valores especificados.
func InsertRow(t *testing.T, executor DBExecutor, tableName string, values string, contextMessage string) {
	t.Helper()
	insertQuery := fmt.Sprintf("INSERT INTO %s %s", tableName, values)
	ctx := context.Background()
	result, err := execQuery(ctx, executor, insertQuery)
	if err != nil {
		msg := fmt.Sprintf("Failed to insert row into %s", tableName)
		if contextMessage != "" {
			msg = fmt.Sprintf("%s (%s)", msg, contextMessage)
		}
		t.Fatalf("%s: %v", msg, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		msg := fmt.Sprintf("Failed to get rows affected from INSERT into %s", tableName)
		if contextMessage != "" {
			msg = fmt.Sprintf("%s (%s)", msg, contextMessage)
		}
		t.Fatalf("%s: %v", msg, err)
	}
	if n != 1 {
		msg := fmt.Sprintf("INSERT into %s should affect 1 row, got: %d", tableName, n)
		if contextMessage != "" {
			msg = fmt.Sprintf("%s (%s)", msg, contextMessage)
		}
		t.Fatalf("%s", msg)
	}
}

// InsertRowWithName insere uma linha na tabela com coluna name.
func InsertRowWithName(t *testing.T, executor DBExecutor, tableName string, nameValue string, contextMessage string) {
	t.Helper()
	escaped := strings.ReplaceAll(nameValue, "'", "''")
	InsertRow(t, executor, tableName, fmt.Sprintf("(name) VALUES ('%s')", escaped), contextMessage)
}

// InsertRowWithData insere uma linha na tabela com coluna data.
func InsertRowWithData(t *testing.T, executor DBExecutor, tableName string, dataValue string, contextMessage string) {
	t.Helper()
	escaped := strings.ReplaceAll(dataValue, "'", "''")
	InsertRow(t, executor, tableName, fmt.Sprintf("(data) VALUES ('%s')", escaped), contextMessage)
}

// InsertOneRow insere uma linha na tabela com coluna value (id SERIAL PRIMARY KEY, value TEXT).
func InsertOneRow(t *testing.T, executor DBExecutor, tableName string, value string, contextMessage string) {
	t.Helper()
	escaped := strings.ReplaceAll(value, "'", "''")
	InsertRow(t, executor, tableName, fmt.Sprintf("(value) VALUES ('%s')", escaped), contextMessage)
}

// AssertTableCount verifica que a contagem de linhas na tabela corresponde ao valor esperado.
// Funciona tanto com *sql.DB quanto com *pgx.Tx.
func AssertTableCount(t *testing.T, executor DBExecutor, tableName string, expectedCount int, contextMsg string) {
	t.Helper()
	checkQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	ctx := context.Background()
	row := queryRow(ctx, executor, checkQuery)
	count := -1
	err := scanRow(row, &count)
	if err != nil {
		msg := fmt.Sprintf("Failed to check table count for %s", tableName)
		if contextMsg != "" {
			msg = fmt.Sprintf("%s (%s)", msg, contextMsg)
		}
		t.Fatalf("%s: %v", msg, err)
	}
	if count != expectedCount {
		msg := fmt.Sprintf("Table %s count = %d, want %d", tableName, count, expectedCount)
		if contextMsg != "" {
			msg = fmt.Sprintf("%s (%s)", msg, contextMsg)
		}
		t.Fatalf("%s", msg)
	}
}

// AssertRowCountWithCondition verifica que a contagem de linhas com uma condição WHERE corresponde ao valor esperado.
// Funciona tanto com *sql.DB quanto com *pgx.Tx.
func AssertRowCountWithCondition(t *testing.T, executor DBExecutor, tableName string, whereClause string, expectedCount int, contextMsg string) {
	t.Helper()
	checkQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", tableName, whereClause)
	ctx := context.Background()
	row := queryRow(ctx, executor, checkQuery)
	var count int
	err := scanRow(row, &count)
	if err != nil {
		msg := fmt.Sprintf("Failed to check row count for %s", tableName)
		if contextMsg != "" {
			msg = fmt.Sprintf("%s (%s)", msg, contextMsg)
		}
		t.Fatalf("%s: %v", msg, err)
	}
	if count != expectedCount {
		msg := fmt.Sprintf("Row count for %s (%s) = %d, want %d", tableName, whereClause, count, expectedCount)
		if contextMsg != "" {
			msg = fmt.Sprintf("%s (%s)", msg, contextMsg)
		}
		t.Errorf("%s", msg)
	}
}

// TableExistsInPublic reports whether tableName exists in schema public.
// Uses to_regclass (single catalog lookup) instead of scanning information_schema.
func TableExistsInPublic(ctx context.Context, executor DBExecutor, tableName string) (bool, error) {
	const query = `SELECT to_regclass(format('%I.%I', 'public', $1::text)) IS NOT NULL`
	row := queryRow(ctx, executor, query, tableName)
	var exists bool
	err := scanRow(row, &exists)
	return exists, err
}

// AssertTableExists verifica que a tabela existe no schema public.
// Funciona tanto com *sql.DB quanto com *pgx.Tx.
func AssertTableExists(t *testing.T, executor DBExecutor, tableName string, contextMsg string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), MetadataQueryTimeout)
	defer cancel()

	exists, err := TableExistsInPublic(ctx, executor, tableName)
	if err != nil {
		msg := fmt.Sprintf("Failed to check if table %s exists", tableName)
		if contextMsg != "" {
			msg = fmt.Sprintf("%s (%s)", msg, contextMsg)
		}
		t.Fatalf("%s: %v", msg, err)
	}
	if !exists {
		msg := fmt.Sprintf("Table %s should exist", tableName)
		if contextMsg != "" {
			msg = fmt.Sprintf("%s (%s)", msg, contextMsg)
		}
		t.Errorf("%s", msg)
	}
}

// AssertSavepointQuery verifica se a query contém SAVEPOINT (case-insensitive) e se contém o nível esperado.
// Pode ser usada em testes de qualquer pacote para verificar queries de savepoint.
func AssertSavepointQuery(t *testing.T, query string, expectedLevel int) {
	t.Helper()
	queryUpper := strings.ToUpper(query)
	if !strings.Contains(queryUpper, "SAVEPOINT") {
		t.Errorf("Query should contain SAVEPOINT (case-insensitive), got: %s", query)
		return
	}
	// Verifica se o nível esperado está contido na query
	levelStr := fmt.Sprintf("%d", expectedLevel)
	if !strings.Contains(query, levelStr) {
		t.Errorf("Query should contain level %d, got: %s", expectedLevel, query)
	}
}

// AssertReleaseSavepointQuery verifica se a query contém RELEASE SAVEPOINT (case-insensitive) e se contém o nível esperado.
// Pode ser usada em testes de qualquer pacote para verificar queries de release savepoint.
func AssertReleaseSavepointQuery(t *testing.T, query string, expectedLevel int) {
	t.Helper()
	queryUpper := strings.ToUpper(query)
	if !strings.Contains(queryUpper, "RELEASE") || !strings.Contains(queryUpper, "SAVEPOINT") {
		t.Errorf("Query should contain RELEASE SAVEPOINT (case-insensitive), got: %s", query)
		return
	}
	// Verifica se o nível esperado está contido na query
	levelStr := fmt.Sprintf("%d", expectedLevel)
	if !strings.Contains(queryUpper, levelStr) {
		t.Errorf("Query should contain level %d, got: %s", expectedLevel, query)
	}
}

// AssertRollbackToSavepointQuery verifica se a query contém ROLLBACK TO SAVEPOINT (case-insensitive) e se contém o nível esperado.
// Pode ser usada em testes de qualquer pacote para verificar queries de rollback to savepoint.
func AssertRollbackToSavepointQuery(t *testing.T, query string, expectedLevel int) {
	t.Helper()
	queryUpper := strings.ToUpper(query)
	if !strings.Contains(queryUpper, "ROLLBACK") || !strings.Contains(queryUpper, "SAVEPOINT") {
		t.Errorf("Query should contain ROLLBACK TO SAVEPOINT (case-insensitive), got: %s", query)
		return
	}
	// Verifica se o nível esperado está contido na query
	levelStr := fmt.Sprintf("%d", expectedLevel)
	if !strings.Contains(query, levelStr) {
		t.Errorf("Query should contain level %d, got: %s", expectedLevel, query)
	}
}
