package tstproxy

import (
	"context"
	"fmt"
	"testing"

	"pgtest-transient/internal/proxy"
	"pgtest-transient/internal/testutil"
)

// createTable cria uma tabela usando ExecuteWithLock e falha o teste se houver erro.
// Usa a função unificada do pacote testutil internamente.
func createTable(t *testing.T, pgtest *proxy.PGTest, session *proxy.TestSession, tableName string, columns string) {
	t.Helper()
	createTableQuery := fmt.Sprintf("CREATE TABLE %s (%s)", tableName, columns)
	err := pgtest.ExecuteWithLock(session, createTableQuery)
	if err != nil {
		t.Fatalf("Failed to create table %s: %v", tableName, err)
	}
}

// createTableWithIdAndName cria uma tabela com colunas (id SERIAL PRIMARY KEY, name VARCHAR(100)).
// Usa a função unificada do pacote testutil.
func createTableWithIdAndName(t *testing.T, pgtest *proxy.PGTest, session *proxy.TestSession, tableName string) {
	t.Helper()
	if session == nil || session.DB == nil {
		t.Fatalf("Session or session.DB is nil")
	}
	if !session.DB.HasActiveTransaction() {
		t.Fatalf("Session.DB has no active transaction - cannot create table")
	}
	testutil.CreateTableWithIdAndName(t, session.DB.Tx(), tableName)
}

// createTableWithIdAndData cria uma tabela com colunas (id SERIAL PRIMARY KEY, data VARCHAR(100)).
// Usa a função unificada do pacote testutil.
func createTableWithIdAndData(t *testing.T, pgtest *proxy.PGTest, session *proxy.TestSession, tableName string) {
	t.Helper()
	if session.DB != nil && session.DB.HasActiveTransaction() {
		testutil.CreateTableWithIdAndData(t, session.DB.Tx(), tableName)
	} else {
		createTable(t, pgtest, session, tableName, "id SERIAL PRIMARY KEY, data VARCHAR(100)")
	}
}

// createTableWithId cria uma tabela com apenas coluna id INT.
// Usa a função unificada do pacote testutil.
func createTableWithId(t *testing.T, pgtest *proxy.PGTest, session *proxy.TestSession, tableName string) {
	t.Helper()
	if session.DB != nil && session.DB.HasActiveTransaction() {
		testutil.CreateTableWithId(t, session.DB.Tx(), tableName)
	} else {
		createTable(t, pgtest, session, tableName, "id INT")
	}
}

// insertRow insere uma linha na tabela usando ExecuteWithLock e falha o teste se houver erro.
func insertRow(t *testing.T, pgtest *proxy.PGTest, session *proxy.TestSession, tableName string, values string) {
	t.Helper()
	insertQuery := fmt.Sprintf("INSERT INTO %s %s", tableName, values)
	err := pgtest.ExecuteWithLock(session, insertQuery)
	if err != nil {
		t.Fatalf("Failed to insert row into %s: %v", tableName, err)
	}
}

// insertRowWithName insere uma linha na tabela com coluna name.
// Usa a função unificada do pacote testutil quando possível.
func insertRowWithName(t *testing.T, pgtest *proxy.PGTest, session *proxy.TestSession, tableName string, nameValue string) {
	t.Helper()
	if session == nil || session.DB == nil {
		t.Fatalf("Session or session.DB is nil")
	}
	if !session.DB.HasActiveTransaction() {
		t.Fatalf("Session.DB has no active transaction - cannot insert row with name")
	}
	testutil.InsertRowWithName(t, session.DB.Tx(), tableName, nameValue, "")
}

// insertRowWithData insere uma linha na tabela com coluna data.
// Usa a função unificada do pacote testutil quando possível.
func insertRowWithData(t *testing.T, pgtest *proxy.PGTest, session *proxy.TestSession, tableName string, dataValue string) {
	t.Helper()
	if session.DB != nil && session.DB.HasActiveTransaction() {
		testutil.InsertRowWithData(t, session.DB.Tx(), tableName, dataValue, "")
	} else {
		insertRow(t, pgtest, session, tableName, fmt.Sprintf("(data) VALUES ('%s')", dataValue))
	}
}

// assertTableCount verifica que a contagem de linhas na tabela corresponde ao valor esperado.
// Usa a função unificada do pacote testutil.
func assertTableCount(t *testing.T, session *proxy.TestSession, tableName string, expectedCount int, contextMsg string) {
	t.Helper()
	if session.DB == nil || !session.DB.HasActiveTransaction() {
		t.Fatalf("Session.DB has no active transaction - cannot assert table count")
	}
	testutil.AssertTableCount(t, session.DB.Tx(), tableName, expectedCount, contextMsg)
}

// assertRowCountWithCondition verifica que a contagem de linhas com uma condição WHERE corresponde ao valor esperado.
// Usa a função unificada do pacote testutil.
func assertRowCountWithCondition(t *testing.T, session *proxy.TestSession, tableName string, whereClause string, expectedCount int, contextMsg string) {
	t.Helper()
	if session == nil || session.DB == nil {
		t.Fatalf("Session or session.DB is nil")
	}
	if !session.DB.HasActiveTransaction() {
		t.Fatalf("Session.DB has no active transaction - cannot assert row count")
	}
	testutil.AssertRowCountWithCondition(t, session.DB.Tx(), tableName, whereClause, expectedCount, contextMsg)
}

// assertTableExists verifica que a tabela existe usando information_schema.
// Usa a função unificada do pacote testutil.
func assertTableExists(t *testing.T, session *proxy.TestSession, tableName string, contextMsg string) {
	t.Helper()
	if session.DB != nil && session.DB.HasActiveTransaction() {
		testutil.AssertTableExists(t, session.DB.Tx(), tableName, contextMsg)
	} else {
		t.Fatalf("Session.DB has no active transaction - cannot assert table existence")
	}
}

// execBeginAndVerify executa BEGIN através do interceptor e verifica o nível de savepoint.
func execBeginAndVerify(t *testing.T, pgtest *proxy.PGTest, session *proxy.TestSession, expectedLevel int, contextMsg string) {
	t.Helper()
	query, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "BEGIN")
	if err != nil {
		msg := "InterceptQuery(BEGIN) error"
		if contextMsg != "" {
			msg = fmt.Sprintf("%s (%s)", msg, contextMsg)
		}
		t.Fatalf("%s = %v", msg, err)
	}
	if session.DB == nil || !session.DB.HasActiveTransaction() {
		t.Fatalf("Session.DB has no active transaction - cannot execute BEGIN")
	}
	_, err = session.DB.Tx().Exec(context.Background(), query)
	if err != nil {
		msg := "Failed to execute BEGIN"
		if contextMsg != "" {
			msg = fmt.Sprintf("%s (%s)", msg, contextMsg)
		}
		t.Fatalf("%s: %v", msg, err)
	}
	if session.DB.GetSavepointLevel() != expectedLevel {
		msg := fmt.Sprintf("SavepointLevel after BEGIN = %v, want %d", session.DB.GetSavepointLevel(), expectedLevel)
		if contextMsg != "" {
			msg = fmt.Sprintf("%s (%s)", msg, contextMsg)
		}
		t.Errorf("%s", msg)
	}
}

// execCommitOnLevel executa COMMIT através do interceptor e verifica o nível de savepoint.
func execCommitOnLevel(t *testing.T, pgtest *proxy.PGTest, session *proxy.TestSession, expectedLevel int, contextMsg string) {
	t.Helper()
	query, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "COMMIT")
	if err != nil {
		msg := "InterceptQuery(COMMIT) error"
		if contextMsg != "" {
			msg = fmt.Sprintf("%s (%s)", msg, contextMsg)
		}
		t.Fatalf("%s = %v", msg, err)
	}
	if session.DB == nil || !session.DB.HasActiveTransaction() {
		t.Fatalf("Session.DB has no active transaction - cannot execute COMMIT")
	}
	_, err = session.DB.Tx().Exec(context.Background(), query)
	if err != nil {
		msg := "Failed to execute COMMIT"
		if contextMsg != "" {
			msg = fmt.Sprintf("%s (%s)", msg, contextMsg)
		}
		t.Fatalf("%s: %v", msg, err)
	}
	if session.DB.GetSavepointLevel() != expectedLevel {
		msg := fmt.Sprintf("SavepointLevel after COMMIT = %v, want %d", session.DB.GetSavepointLevel(), expectedLevel)
		if contextMsg != "" {
			msg = fmt.Sprintf("%s (%s)", msg, contextMsg)
		}
		t.Errorf("%s", msg)
	}
}

// execRollbackAndVerify executa ROLLBACK através do interceptor e verifica o nível de savepoint.
func execRollbackAndVerify(t *testing.T, pgtest *proxy.PGTest, session *proxy.TestSession, expectedLevel int, contextMsg string) {
	t.Helper()
	query, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "ROLLBACK")
	if err != nil {
		msg := "InterceptQuery(ROLLBACK) error"
		if contextMsg != "" {
			msg = fmt.Sprintf("%s (%s)", msg, contextMsg)
		}
		t.Fatalf("%s = %v", msg, err)
	}
	if session.DB == nil || !session.DB.HasActiveTransaction() {
		t.Fatalf("Session.DB has no active transaction - cannot execute ROLLBACK")
	}
	_, err = session.DB.Tx().Exec(context.Background(), query)
	if err != nil {
		msg := "Failed to execute ROLLBACK"
		if contextMsg != "" {
			msg = fmt.Sprintf("%s (%s)", msg, contextMsg)
		}
		t.Fatalf("%s: %v", msg, err)
	}
	if session.DB.GetSavepointLevel() != expectedLevel {
		msg := fmt.Sprintf("SavepointLevel after ROLLBACK = %v, want %d", session.DB.GetSavepointLevel(), expectedLevel)
		if contextMsg != "" {
			msg = fmt.Sprintf("%s (%s)", msg, contextMsg)
		}
		t.Errorf("%s", msg)
	}
}

// assertCommitBlocked verifica que COMMIT está bloqueado
func assertCommitBlocked(t *testing.T, pgtest *proxy.PGTest, session *proxy.TestSession, contextMsg string) {
	t.Helper()
	query, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "COMMIT")
	if err != nil {
		msg := "InterceptQuery(COMMIT) error"
		if contextMsg != "" {
			msg = fmt.Sprintf("%s (%s)", msg, contextMsg)
		}
		t.Fatalf("%s = %v", msg, err)
	}
	if query != proxy.DEFAULT_SELECT_ONE {
		msg := fmt.Sprintf("InterceptQuery(COMMIT at level 0) = %v, want %s (blocked)", query, proxy.DEFAULT_SELECT_ONE)
		if contextMsg != "" {
			msg = fmt.Sprintf("%s (%s)", msg, contextMsg)
		}
		t.Errorf("%s", msg)
	}
}

// assertRollbackBlocked verifica que ROLLBACK está bloqueado
func assertRollbackBlocked(t *testing.T, pgtest *proxy.PGTest, session *proxy.TestSession, contextMsg string) {
	t.Helper()
	query, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "ROLLBACK")
	if err != nil {
		msg := "InterceptQuery(ROLLBACK) error"
		if contextMsg != "" {
			msg = fmt.Sprintf("%s (%s)", msg, contextMsg)
		}
		t.Fatalf("%s = %v", msg, err)
	}
	if query != proxy.DEFAULT_SELECT_ONE {
		msg := fmt.Sprintf("InterceptQuery(ROLLBACK at level 0) = %v, want %s (blocked)", query, proxy.DEFAULT_SELECT_ONE)
		if contextMsg != "" {
			msg = fmt.Sprintf("%s (%s)", msg, contextMsg)
		}
		t.Errorf("%s", msg)
	}
}
