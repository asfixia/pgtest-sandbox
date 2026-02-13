// Testes de integração do pgtest: conectam ao proxy e ao PostgreSQL real.
//
// Estrutura do arquivo:
// 1. Variáveis globais e TestMain — inicia o servidor pgtest antes dos testes
// 2. Testes — TestProtectionAgainstAccidentalCommit, etc.
//
// Funções utilitárias estão em integration_test_helpers.go
package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"pgtest-sandbox/internal/proxy"
	"pgtest-sandbox/pkg/logger"
	"pgtest-sandbox/pkg/postgres"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var pgtestServer *proxy.Server

// --- 1. TestMain e helpers de debug ---

// PrintR é um helper para usar proxy.PrintR no painel de watch do debugger
// Permite usar PrintR(v) diretamente sem precisar do prefixo proxy.
// Exemplo no watch: PrintR(tag), PrintR(session), etc.
func PrintR(v interface{}) string {
	return proxy.PrintR(v)
}

func TestMain(m *testing.M) {
	// Permite especificar caminho do config via variável de ambiente
	cfg := getConfig()

	// Inicializa o logger a partir da configuração
	if err := logger.InitFromConfig(cfg); err != nil {
		fmt.Printf("Failed to initialize logger: %v\n", err)
		// Não falha o teste se o logger não inicializar, apenas usa padrão
	}

	// Usa porta da configuração
	pgtestListenPort := cfg.Proxy.ListenPort
	if pgtestListenPort == 0 {
		// Se não configurada, usa 5433 como padrão para testes
		pgtestListenPort = 5433
	}

	// Avisa se estiver usando porta 5432 (pode conflitar com PostgreSQL real)
	if pgtestListenPort == 5432 {
		logger.Warn("PGTest está usando porta 5432, que pode conflitar com PostgreSQL real")
		logger.Warn("Considere usar uma porta diferente (ex: 5433) para testes")
	}

	useExternalServer := os.Getenv("PGTEST_USE_EXTERNAL_SERVER") == "1" || os.Getenv("PGTEST_USE_EXTERNAL_SERVER") == "true"
	if useExternalServer {
		// Servidor pgtest já rodando em outro processo (ex.: para debug sem timeout de conexão).
		// Não inicia nem encerra o servidor; os testes usam a porta do config.
		code := m.Run()
		os.Exit(code)
	}

	keepaliveInterval := time.Duration(0)
	if cfg.Proxy.KeepaliveInterval.Duration > 0 {
		keepaliveInterval = cfg.Proxy.KeepaliveInterval.Duration
	}
	pgtestServer = proxy.NewServer(
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Proxy.Timeout,
		cfg.Postgres.SessionTimeout.Duration,
		keepaliveInterval,
		cfg.Proxy.ListenHost,
		pgtestListenPort,
		false,
	)
	if err := pgtestServer.StartError(); err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		os.Exit(1)
	}

	time.Sleep(100 * time.Millisecond)

	code := m.Run()

	pgtestServer.Stop()
	os.Exit(code)
}

// --- Testes ---

func TestProtectionAgainstAccidentalCommit(t *testing.T) {
	testID := "test_commit_protection"
	pgtestDB := connectToPGTestProxy(t, testID)
	defer pgtestDB.Close()
	execBegin(t, pgtestDB, "First BEGIN: pgtest converts to SAVEPOINT (creates base transaction if needed)")
	schema := getTestSchema()
	tableName := postgres.QuoteQualifiedName(schema, "pgtest_commit_protection")
	createTableWithValueColumn(t, pgtestDB, tableName)
	insertOneRow(t, pgtestDB, tableName, "before_commit", "Insert row before COMMIT to test commit protection")
	assertTableRowCount(t, pgtestDB, tableName, 1, "Table has 1 row: CREATE TABLE + INSERT in base transaction")
	execCommit(t, pgtestDB)
	assertTableRowCount(t, pgtestDB, tableName, 1, "After COMMIT (RELEASE SAVEPOINT): table still exists with 1 row - COMMIT only releases savepoint, changes remain in base transaction")
	execRollback(t, pgtestDB) //No transaction to rollback
	assertTableRowCount(t, pgtestDB, tableName, 1, "No Transaction to rollback, table still exists with 1 row")

	execCommit(t, pgtestDB)
	execRollback(t, pgtestDB)
	assertTableRowCount(t, pgtestDB, tableName, 1, "No change was made 1 row still exists")

	execBegin(t, pgtestDB, "Second BEGIN after COMMIT: pgtest converts to SAVEPOINT (SavepointLevel becomes 1 again)")
	pingWithTimeout(t, pgtestDB, 5*time.Second, false, "Connection check before second BEGIN")
	insertOneRow(t, pgtestDB, tableName, "test_value", "Insert row in second transaction after COMMIT")
	assertTableRowCount(t, pgtestDB, tableName, 2, "Table has 2 rows after INSERT in second transaction")
	pingWithTimeout(t, pgtestDB, 5*time.Second, true, "Connection check after INSERT in second transaction")
	postgresDBDirect := connectToRealPostgres(t)
	defer postgresDBDirect.Close()
	pingWithTimeout(t, postgresDBDirect, 5*time.Second, false)
	assertTableDoesNotExist(t, postgresDBDirect, tableName, "Table does not exist in real PostgreSQL - data only exists in pgtest transaction (not committed)")
	execRollback(t, pgtestDB)
	assertTableRowCount(t, pgtestDB, tableName, 1, "After ROLLBACK blocked: table still exists with 1 row - ROLLBACK only reverts INSERT from second transaction, CREATE TABLE and first INSERT remain")
	execPgTestFullRollback(t, pgtestDB)
	pingWithTimeout(t, postgresDBDirect, 5*time.Second, false)
	assertTableDoesNotExist(t, postgresDBDirect, tableName, "After pgtest rollback: table does not exist in real PostgreSQL - base transaction was rolled back")
	assertTableDoesNotExist(t, pgtestDB, tableName, "After pgtest rollback: table does not exist in pgtest - CREATE TABLE was reverted (new empty transaction created)")
	t.Logf("meupirugluglu: %s", tableName)
}

func TestProtectionAgainstAccidentalRollback(t *testing.T) {
	testID := "test_rollback_protection"
	pgtestProxyDSN := getPGTestProxyDSN(testID)
	pgtestDB, err := sql.Open("pgx", pgtestProxyDSN)
	if err != nil {
		t.Fatalf("Failed to connect to PGTest: %v", err)
	}
	defer pgtestDB.Close()
	schema := getTestSchema()
	tableName := postgres.QuoteQualifiedName(schema, "pgtest_rollback_protection")
	createTableWithValueColumn(t, pgtestDB, tableName)
	insertOneRow(t, pgtestDB, tableName, "test_value", "insert row before testing ROLLBACK protection")
	assertTableRowCount(t, pgtestDB, tableName, 1, "")
	execRollback(t, pgtestDB)
	assertTableRowCount(t, pgtestDB, tableName, 1, "Data still exists after ROLLBACK attempt")
	execPgtestRollback(t, pgtestDB)
	assertTableDoesNotExist(t, pgtestDB, tableName, "Table should not exist after the full pgtest rollback")
}

func TestTransactionSharing(t *testing.T) {
	testID := "test_sharing"
	pgtestProxyDSN := getPGTestProxyDSN(testID)
	pgtestDB1, err := sql.Open("pgx", pgtestProxyDSN)
	if err != nil {
		t.Fatalf("Failed to connect to PGTest proxy: %v", err)
	}
	defer pgtestDB1.Close()
	pgtestDB2, err := sql.Open("pgx", pgtestProxyDSN)
	if err != nil {
		t.Fatalf("Failed to connect to PGTest proxy: %v", err)
	}
	defer pgtestDB2.Close()
	schema := getTestSchema()
	tableName := postgres.QuoteQualifiedName(schema, "pgtest_sharing")
	createTableWithValueColumn(t, pgtestDB1, tableName)
	insertOneRow(t, pgtestDB1, tableName, "from_pgtestDB1", "insert row from first connection to test transaction sharing")
	assertTableRowCount(t, pgtestDB2, tableName, 1, "Transaction is shared between connections")
	execPgtestRollback(t, pgtestDB1)
	assertTableDoesNotExist(t, pgtestDB1, tableName, "Table should not exist on second connection")
	assertTableDoesNotExist(t, pgtestDB2, tableName, "Table should not exist on second connection")
}

func TestIsolationBetweenTestIDs(t *testing.T) {
	testID1 := "test_isolation_1"
	testID2 := "test_isolation_2"
	pgtestProxyDSN1 := getPGTestProxyDSN(testID1)
	pgtestProxyDSN2 := getPGTestProxyDSN(testID2)

	pgtestDB1, err := sql.Open("pgx", pgtestProxyDSN1)
	if err != nil {
		t.Fatalf("Failed to connect to PGTest proxy: %v", err)
	}
	defer pgtestDB1.Close()

	pgtestDB2, err := sql.Open("pgx", pgtestProxyDSN2)
	if err != nil {
		t.Fatalf("Failed to connect to PGTest proxy: %v", err)
	}
	defer pgtestDB2.Close()

	schema := getTestSchema()
	tableName := postgres.QuoteQualifiedName(schema, "pgtest_isolation")
	createTableWithValueColumn(t, pgtestDB1, tableName)
	insertOneRow(t, pgtestDB1, tableName, "from_test1", "insert row in testID1 to verify isolation between testIDs")
	assertTableDoesNotExist(t, pgtestDB2, tableName, "Table should not exist on second connection")

	execPgtestRollback(t, pgtestDB1)
	execPgtestRollback(t, pgtestDB2)

	assertTableDoesNotExist(t, pgtestDB1, tableName, "Table should not exist on first connection")
	assertTableDoesNotExist(t, pgtestDB2, tableName, "Table should not exist on second connection")
}

func TestBeginToSavepointConversion(t *testing.T) {
	testID := "test_savepoint"
	pgtestProxyDSN := getPGTestProxyDSN(testID)

	pgtestDB, err := sql.Open("pgx", pgtestProxyDSN)
	if err != nil {
		t.Fatalf("Failed to connect to PGTest proxy: %v", err)
	}
	defer pgtestDB.Close()

	schema := getTestSchema()
	tableName := postgres.QuoteQualifiedName(schema, "pgtest_savepoint")
	createTableWithValueColumn(t, pgtestDB, tableName)

	insertOneRow(t, pgtestDB, tableName, "before_begin", "insert row before BEGIN to test savepoint conversion")

	execBegin(t, pgtestDB, "")

	insertOneRow(t, pgtestDB, tableName, "after_begin", "insert row after BEGIN (savepoint) to test savepoint conversion")

	execCommit(t, pgtestDB)

	assertTableRowCount(t, pgtestDB, tableName, 2, "")

	execPgtestRollback(t, pgtestDB)
}

func TestPGTestCommands(t *testing.T) {
	testID := "test_commands"
	pgtestProxyDSN := getPGTestProxyDSN(testID)

	pgtestDB, err := sql.Open("pgx", pgtestProxyDSN)
	if err != nil {
		t.Fatalf("Failed to connect to PGTest proxy: %v", err)
	}
	defer pgtestDB.Close()

	// O testID já está na connection string (application_name), então não precisa passar como parâmetro
	_, err = pgtestDB.Exec("pgtest begin")
	if err != nil {
		t.Logf("pgtest begin: %v", err)
	}

	var testIDCol string
	var active bool
	var level int
	var createdAt string

	// O testID já está na connection string (application_name), então não precisa passar como parâmetro
	err = pgtestDB.QueryRow("pgtest status").Scan(&testIDCol, &active, &level, &createdAt)
	if err != nil {
		t.Logf("pgtest status: %v", err)
	} else {
		if testIDCol != testID {
			t.Errorf("Status test_id = %v, want %v", testIDCol, testID)
		}
		if !active {
			t.Error("Status active should be true")
		}
	}

	execPgtestRollback(t, pgtestDB)
}

func TestTransactionPersistenceAcrossReconnections(t *testing.T) {
	testID := "test_reconnection_persistence"

	// Primeira conexão - cria tabela e insere dados
	pgtestDB1 := connectToPGTestProxy(t, testID)

	schema := getTestSchema()
	tableName := postgres.QuoteQualifiedName(schema, "pgtest_reconnection")
	createTableWithValueColumn(t, pgtestDB1, tableName)

	insertOneRow(t, pgtestDB1, tableName, "created_in_first_connection", "insert row in first connection to test transaction persistence across reconnections")

	// Verifica que os dados estão visíveis na primeira conexão
	assertTableRowCount(t, pgtestDB1, tableName, 1, "")

	// Fecha a primeira conexão
	pgtestDB1.Close()
	t.Log("First connection closed")

	// Reconecta com o mesmo testID
	pgtestDB2 := connectToPGTestProxy(t, testID)
	defer pgtestDB2.Close()

	// Verifica que os dados criados na primeira conexão ainda estão visíveis
	assertTableRowCount(t, pgtestDB2, tableName, 1, "Data persisted across reconnection")

	// Insere mais dados na segunda conexão
	insertOneRow(t, pgtestDB2, tableName, "created_in_second_connection", "insert row in second connection to verify shared transaction")

	// Verifica que agora temos 2 linhas
	assertTableRowCount(t, pgtestDB2, tableName, 2, "Both connections share the same transaction")

	// Limpa a transação
	execPgtestRollback(t, pgtestDB2)
}

// TestConcurrentConnectionsSameSession runs multiple connections (same testID) that each execute
// a query at the same time. Without serialization the backend returns "conn busy" for some.
// With serialization they run one after another; all must succeed.
// Uses prepared statements so all use the Extended Query path (Parse + Execute).
func TestConcurrentConnectionsSameSession(t *testing.T) {
	testID := "test_concurrent_same_session"
	dsn := getPGTestProxyDSN(testID)
	const numConcurrentConnections = 10

	dbs := make([]*sql.DB, numConcurrentConnections)
	for i := range dbs {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("Failed to open connection %d: %v", i+1, err)
		}
		defer db.Close()
		dbs[i] = db
		pingConnection(t, db)
	}

	query := "SELECT 1 FROM pg_sleep(0.15)"
	stmts := make([]*sql.Stmt, numConcurrentConnections)
	for i, db := range dbs {
		stmt, err := db.Prepare(query)
		if err != nil {
			t.Fatalf("Prepare on conn %d: %v", i+1, err)
		}
		defer stmt.Close()
		stmts[i] = stmt
	}

	results := make([]int, numConcurrentConnections)
	errs := make([]error, numConcurrentConnections)
	done := make([]chan struct{}, numConcurrentConnections)
	for i := range done {
		done[i] = make(chan struct{})
	}
	for i := 0; i < numConcurrentConnections; i++ {
		idx := i
		go func() {
			defer close(done[idx])
			errs[idx] = stmts[idx].QueryRow().Scan(&results[idx])
		}()
	}
	for i := range done {
		<-done[i]
	}

	for i := 0; i < numConcurrentConnections; i++ {
		if errs[i] != nil {
			t.Errorf("Connection %d query failed (conn busy or other): %v", i+1, errs[i])
		}
		if results[i] != 1 {
			t.Errorf("Connection %d result = %d, want 1", i+1, results[i])
		}
	}
}

// TestTwoConnectionsSamePreparedStatementName verifies that two different connections to the
// same testID (session) can each prepare a statement with the same client-side name (e.g.
// PDO's "pdo_stmt_00000004") without colliding. Previously they shared session-level maps
// and the second Prepare would overwrite the first, causing "bind message supplies N parameters,
// but prepared statement requires M" when the first connection executed.
func TestTwoConnectionsSamePreparedStatementName(t *testing.T) {
	testID := "test_same_stmt_name"
	dsn := getPGTestProxyDSN(testID)
	ctx := context.Background()

	conn1, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connection 1: %v", err)
	}
	defer conn1.Close(ctx)

	conn2, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connection 2: %v", err)
	}
	defer conn2.Close(ctx)

	// Same statement name on both connections (PDO-style); different queries: one with 1 param, one with 0.
	const stmtName = "pdo_stmt_00000004"
	_, err = conn1.Prepare(ctx, stmtName, "SELECT $1::int", nil)
	if err != nil {
		t.Fatalf("conn1 Prepare: %v", err)
	}
	_, err = conn2.Prepare(ctx, stmtName, "SELECT 42", nil)
	if err != nil {
		t.Fatalf("conn2 Prepare: %v", err)
	}

	// Execute on conn1 with one parameter; must return 123 (not 42 and not "wrong parameter count").
	rr1 := conn1.ExecPrepared(ctx, stmtName, [][]byte{[]byte("123")}, nil, nil)
	var val1 int
	if rr1.NextRow() {
		vals := rr1.Values()
		if len(vals) > 0 && vals[0] != nil {
			fmt.Sscanf(string(vals[0]), "%d", &val1)
		}
	}
	if _, err := rr1.Close(); err != nil {
		t.Fatalf("conn1 ExecPrepared close: %v", err)
	}
	if val1 != 123 {
		t.Errorf("conn1 result = %d, want 123 (collision would give 42 or wrong param count)", val1)
	}

	// Execute on conn2 with no parameters; must return 42.
	rr2 := conn2.ExecPrepared(ctx, stmtName, nil, nil, nil)
	var val2 int
	if rr2.NextRow() {
		vals := rr2.Values()
		if len(vals) > 0 && vals[0] != nil {
			fmt.Sscanf(string(vals[0]), "%d", &val2)
		}
	}
	if _, err := rr2.Close(); err != nil {
		t.Fatalf("conn2 ExecPrepared close: %v", err)
	}
	if val2 != 42 {
		t.Errorf("conn2 result = %d, want 42", val2)
	}
}

// TestIntegrationDEALLOCATEOnlyAffectsOwnConnection: conn1 prepares a statement; conn2 sends
// DEALLOCATE <name> as a simple query. The proxy rewrites to conn2's backend name, which
// does not exist, so the backend returns an error. conn1's statement must still work.
func TestIntegrationDEALLOCATEOnlyAffectsOwnConnection(t *testing.T) {
	testID := "test_dealloc_own_only"
	dsn := getPGTestProxyDSN(testID)
	ctx := context.Background()

	conn1, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connection 1: %v", err)
	}
	defer conn1.Close(ctx)

	conn2, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connection 2: %v", err)
	}
	defer conn2.Close(ctx)

	const stmtName = "pdo_stmt_00000001"
	_, err = conn1.Prepare(ctx, stmtName, "SELECT 111", nil)
	if err != nil {
		t.Fatalf("conn1 Prepare: %v", err)
	}

	// conn2 tries to DEALLOCATE the same name (simple query, like PHP PDO). Proxy rewrites
	// to conn2's backend name, which does not exist → backend error.
	mrr := conn2.Exec(ctx, "DEALLOCATE "+stmtName)
	err = mrr.Close()
	if err == nil {
		t.Fatal("conn2 DEALLOCATE of conn1's statement should fail (prepared statement does not exist)")
	}
	errStr := err.Error()
	if !strings.Contains(errStr, "does not exist") && !strings.Contains(errStr, "26000") && !strings.Contains(errStr, "prepared statement") {
		t.Errorf("expected 'does not exist' or SQLSTATE 26000, got: %v", err)
	}

	// conn1's statement must still work.
	rr := conn1.ExecPrepared(ctx, stmtName, nil, nil, nil)
	var val int
	if rr.NextRow() {
		vals := rr.Values()
		if len(vals) > 0 && vals[0] != nil {
			fmt.Sscanf(string(vals[0]), "%d", &val)
		}
	}
	if _, err := rr.Close(); err != nil {
		t.Fatalf("conn1 ExecPrepared after conn2 DEALLOCATE: %v", err)
	}
	if val != 111 {
		t.Errorf("conn1 result = %d, want 111 (conn2 must not have deallocated conn1's statement)", val)
	}
}

// TestIntegrationDEALLOCATEALLWithSameNameOnTwoConnections: both connections prepare the
// same statement name. conn1 runs DEALLOCATE ALL (only its own is deallocated). conn2
// must still be able to use its statement, then conn2 runs DEALLOCATE <name> and succeeds.
func TestIntegrationDEALLOCATEALLWithSameNameOnTwoConnections(t *testing.T) {
	testID := "test_dealloc_all_same_name"
	dsn := getPGTestProxyDSN(testID)
	ctx := context.Background()

	conn1, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connection 1: %v", err)
	}
	defer conn1.Close(ctx)

	conn2, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connection 2: %v", err)
	}
	defer conn2.Close(ctx)

	const stmtName = "pdo_stmt_00000002"
	_, err = conn1.Prepare(ctx, stmtName, "SELECT 201", nil)
	if err != nil {
		t.Fatalf("conn1 Prepare: %v", err)
	}
	_, err = conn2.Prepare(ctx, stmtName, "SELECT 202", nil)
	if err != nil {
		t.Fatalf("conn2 Prepare: %v", err)
	}

	// conn1 runs DEALLOCATE ALL. Only conn1's backend statement is deallocated; conn2's remains.
	mrr := conn1.Exec(ctx, "DEALLOCATE ALL")
	if err := mrr.Close(); err != nil {
		t.Fatalf("conn1 DEALLOCATE ALL: %v", err)
	}

	// conn2 must still be able to execute (its statement was not touched by conn1's DEALLOCATE ALL).
	rr2 := conn2.ExecPrepared(ctx, stmtName, nil, nil, nil)
	var val2 int
	if rr2.NextRow() {
		vals := rr2.Values()
		if len(vals) > 0 && vals[0] != nil {
			fmt.Sscanf(string(vals[0]), "%d", &val2)
		}
	}
	if _, err := rr2.Close(); err != nil {
		t.Fatalf("conn2 ExecPrepared after conn1 DEALLOCATE ALL: %v", err)
	}
	if val2 != 202 {
		t.Errorf("conn2 result = %d, want 202", val2)
	}

	// conn2 runs DEALLOCATE <name> (same client name); must succeed (deallocates only conn2's).
	mrr2 := conn2.Exec(ctx, "DEALLOCATE "+stmtName)
	if err := mrr2.Close(); err != nil {
		t.Fatalf("conn2 DEALLOCATE %q: %v", stmtName, err)
	}
}

// TestMultipleQueriesReturnsLastOnly ensures the proxy returns only the last result for a
// multi-statement Simple Query. Example: "SELECT 1 as val; SELECT 2 as val;" must return
// a single row with val = 2, not 1.
func TestMultipleQueriesReturnsLastOnly(t *testing.T) {
	testID := "test_multi_last_only"
	db := connectToPGTestProxySingleConn(t, testID)
	defer db.Close()

	execBegin(t, db, "")

	rows, err := db.Query("SELECT 1 as val; SELECT 2 as val;")
	if err != nil {
		t.Fatalf("multi-query failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatalf("expected exactly one row (last result), got 0")
	}
	var val int
	if err := rows.Scan(&val); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if val != 2 {
		t.Errorf("result val = %d, want 2 (must be last query result only)", val)
	}
	if rows.Next() {
		t.Fatalf("expected exactly one row, got more (proxy must return last result only)")
	}
}

// TestResetSessionPingBeforeQuery reproduces the response-attribution bug: after full rollback,
// db.Query(tableExistenceQuery) triggers ResetSession (which sends "-- ping") then the query.
// This test uses a single connection to rule out pool reordering and asserts we get exactly
// one row with value 0 or 1 (table existence), not the ping response.
func TestResetSessionPingBeforeQuery(t *testing.T) {
	testID := "test_reset_session_ping"
	db := connectToPGTestProxySingleConn(t, testID)
	defer db.Close()

	schema := getTestSchema()
	tableName := postgres.QuoteQualifiedName(schema, "pgtest_nonexistent_for_repro")
	// Table is never created; we expect existence = 0.
	query := fmt.Sprintf("SELECT CASE WHEN to_regclass('%s') IS NOT NULL THEN 1 ELSE 0 END", tableName)

	execPgTestFullRollback(t, db)
	// Immediately after full rollback: next use of db will do ResetSession (Ping) then our query.
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query after full rollback failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatalf("expected exactly one row (table existence), got 0 (possible wrong response attribution)")
	}
	var val int
	if err := rows.Scan(&val); err != nil {
		t.Fatalf("scan table existence: %v", err)
	}
	if val != 0 && val != 1 {
		t.Fatalf("expected 0 or 1, got %d (possible wrong response attribution)", val)
	}
	if rows.Next() {
		t.Fatalf("expected exactly one row, got more")
	}
	t.Logf("SUCCESS: got one row with value %d after full rollback", val)
}

func transactionHandlingTableName() string {
	return postgres.QuoteQualifiedName(getTestSchema(), "pgtest_transaction_test")
}

func TestTransactionHandling_InsertRowAndRollback(t *testing.T) {
	testID := "test_txn_insert_rollback"
	pgtestDB := connectToPGTestProxy(t, testID)
	defer pgtestDB.Close()
	tableName := transactionHandlingTableName()

	assertTableDoesNotExist(t, pgtestDB, tableName, "Table should not exist before test")
	createTableWithValueColumn(t, pgtestDB, tableName)
	execBegin(t, pgtestDB, "")
	insertOneRow(t, pgtestDB, tableName, "alice", "insert row in basic BEGIN/COMMIT test")
	assertTableRowCount(t, pgtestDB, tableName, 1, "Basic BEGIN/COMMIT works correctly")
	execRollbackOrFail(t, pgtestDB)
	assertTableRowCount(t, pgtestDB, tableName, 0, "Basic BEGIN/COMMIT works correctly")
	execPgTestFullRollback(t, pgtestDB)
	pingWithTimeout(t, pgtestDB, 5*time.Second, false, "Table should not exist after pgtest rollback")
	assertTableDoesNotExist(t, pgtestDB, tableName, "Table should not exist after pgtest rollback")
	t.Log("SUCCESS: insert_row_and_rollback correctly")
}

func TestTransactionHandling_ExplicitSavepoint(t *testing.T) {
	testID := "test_txn_explicit_savepoint"
	pgtestDB := connectToPGTestProxy(t, testID)
	defer pgtestDB.Close()
	tableName := transactionHandlingTableName()

	createTableWithValueColumn(t, pgtestDB, tableName)
	execBegin(t, pgtestDB, "")
	insertOneRow(t, pgtestDB, tableName, "charlie", "insert row before explicit savepoint test")
	assertTableRowCount(t, pgtestDB, tableName, 1, "Rollback must return the row quantity to 1")
	execSavepoint(t, pgtestDB, "pgtsp1")
	assertTableRowCount(t, pgtestDB, tableName, 1, "Rollback must return the row quantity to 1")
	insertOneRow(t, pgtestDB, tableName, "david", "insert row after explicit savepoint to test rollback to savepoint")
	assertTableRowCount(t, pgtestDB, tableName, 2, "Inserted row works correctly")
	execRollbackToSavepoint(t, pgtestDB, "pgtsp1")
	assertTableRowCount(t, pgtestDB, tableName, 1, "Rollback must return the row quantity to 1")
	execPgTestFullRollback(t, pgtestDB)
	t.Log("SUCCESS: Explicit savepoint works correctly")
}

func TestTransactionHandling_NestedSavepoints(t *testing.T) {
	testID := "test_txn_nested_savepoints"
	pgtestDB := connectToPGTestProxy(t, testID)
	defer pgtestDB.Close()
	tableName := transactionHandlingTableName()

	createTableWithValueColumn(t, pgtestDB, tableName)
	execBegin(t, pgtestDB, "")
	insertOneRow(t, pgtestDB, tableName, "nested_1", "insert first row in nested savepoints test")
	execSavepoint(t, pgtestDB, "a")
	insertOneRow(t, pgtestDB, tableName, "nested_2", "insert second row after first savepoint in nested savepoints test")
	execSavepoint(t, pgtestDB, "b")
	insertOneRow(t, pgtestDB, tableName, "nested_3", "insert third row after second savepoint in nested savepoints test")
	execRollbackToSavepoint(t, pgtestDB, "b")
	execRollbackToSavepoint(t, pgtestDB, "b")
	assertTableRowCount(t, pgtestDB, tableName, 2, "Rollback to b must return the row quantity to 2")
	assertQueryCount(t, pgtestDB, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE value IN ('nested_1', 'nested_2')", tableName), 2, "Nested SAVEPOINTs work correctly")
	execCommit(t, pgtestDB)
	assertTableRowCount(t, pgtestDB, tableName, 2, "Rollback to b must return the row quantity to 2")
	execRollbackToInvalidSavepoint(t, pgtestDB, "b")
	assertTableRowCount(t, pgtestDB, tableName, 2, "Rollback to b must return the row quantity to 2")
	execRollbackToInvalidSavepoint(t, pgtestDB, "a")
	assertTableRowCount(t, pgtestDB, tableName, 2, "Rollback to b must return the row quantity to 2")
	execPgTestFullRollback(t, pgtestDB)
	t.Log("SUCCESS: Nested savepoints works correctly")
}

func TestTransactionHandling_NestedSavepointsReleaseThenRollbackToA(t *testing.T) {
	testID := "test_txn_nested_release_rollback_a"
	pgtestDB := connectToPGTestProxy(t, testID)
	defer pgtestDB.Close()
	tableName := transactionHandlingTableName()

	createTableWithValueColumn(t, pgtestDB, tableName)
	execBegin(t, pgtestDB, "")
	insertOneRow(t, pgtestDB, tableName, "nested_1", "insert first row")
	execSavepoint(t, pgtestDB, "a")
	insertOneRow(t, pgtestDB, tableName, "nested_2", "insert second row after savepoint a")
	execSavepoint(t, pgtestDB, "b")
	insertOneRow(t, pgtestDB, tableName, "nested_3", "insert third row after savepoint b")
	execRollbackToSavepoint(t, pgtestDB, "b")
	execRollbackToSavepoint(t, pgtestDB, "b")
	assertTableRowCount(t, pgtestDB, tableName, 2, "After rollback to b twice we have 2 rows")
	execReleaseSavepoint(t, pgtestDB, "b")
	assertTableRowCount(t, pgtestDB, tableName, 2, "After RELEASE SAVEPOINT b we still have 2 rows")
	execRollbackToSavepoint(t, pgtestDB, "a")
	assertTableRowCount(t, pgtestDB, tableName, 1, "After ROLLBACK TO SAVEPOINT a we have 1 row")
	execPgTestFullRollback(t, pgtestDB)
	t.Log("SUCCESS: RELEASE SAVEPOINT b then ROLLBACK TO SAVEPOINT a works correctly")
}

func TestTransactionHandling_ReleaseSavepoint(t *testing.T) {
	testID := "test_txn_release_savepoint"
	pgtestDB := connectToPGTestProxy(t, testID)
	defer pgtestDB.Close()
	tableName := transactionHandlingTableName()

	createTableWithValueColumn(t, pgtestDB, tableName)
	execBegin(t, pgtestDB, "")
	insertOneRow(t, pgtestDB, tableName, "release_test", "insert row before RELEASE SAVEPOINT test")
	execSavepoint(t, pgtestDB, "sp_release")
	execReleaseSavepoint(t, pgtestDB, "sp_release")
	assertQueryCount(t, pgtestDB, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE value = 'release_test'", tableName), 1, "RELEASE SAVEPOINT works correctly")
	execReleaseSavepointExpectError(t, pgtestDB, "sp_release")
	assertQueryCount(t, pgtestDB, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE value = 'release_test'", tableName), 1, "State unchanged after invalid RELEASE")
	execCommit(t, pgtestDB)
	execPgTestFullRollback(t, pgtestDB)
	t.Log("SUCCESS: Release savepoint works correctly")
}

func TestTransactionHandling_NestedBeginCommit(t *testing.T) {
	testID := "test_txn_nested_begin_commit"
	pgtestDB := connectToPGTestProxy(t, testID)
	defer pgtestDB.Close()
	tableName := transactionHandlingTableName()

	createTableWithValueColumn(t, pgtestDB, tableName)
	execBegin(t, pgtestDB, "")
	insertOneRow(t, pgtestDB, tableName, "nested_begin_1", "insert first row in nested BEGIN/COMMIT test")
	execBegin(t, pgtestDB, "")
	insertOneRow(t, pgtestDB, tableName, "nested_begin_2", "insert second row after second BEGIN in nested BEGIN/COMMIT test")
	execCommit(t, pgtestDB)
	assertTableRowCount(t, pgtestDB, tableName, 2, "Commit keep the 2 rows")
	execCommit(t, pgtestDB)
	assertTableRowCount(t, pgtestDB, tableName, 2, "Commit keep the 2 rows")
	execPgTestFullRollback(t, pgtestDB)
	t.Log("SUCCESS: Nested BEGIN/COMMIT works correctly")
}

func TestTransactionHandling_ErrorHandlingAbortedTransaction(t *testing.T) {
	testID := "test_txn_error_handling"
	pgtestDB := connectToPGTestProxy(t, testID)
	defer pgtestDB.Close()
	tableName := transactionHandlingTableName()

	dropTableIfExists(t, pgtestDB, tableName)
	createTableWithValueColumn(t, pgtestDB, tableName)
	execBegin(t, pgtestDB, "")
	insertOneRow(t, pgtestDB, tableName, "before_error", "insert valid row before testing error handling")
	insertDuplicateRow(t, pgtestDB, tableName, 1, "duplicate")
	execRollbackOrFail(t, pgtestDB)
	assertQueryCount(t, pgtestDB, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE value = 'before_error'", tableName), 0, "ROLLBACK after error correctly reverted all changes")
	execPgTestFullRollback(t, pgtestDB)
	assertTableDoesNotExist(t, pgtestDB, tableName, "Table does not exist after pgtest rollback")
}

func TestInvalidStatements(t *testing.T) {
	testID := "test_invalid_statements"
	pgtestProxyDSN := getPGTestProxyDSN(testID)

	pgtestDB, err := sql.Open("pgx", pgtestProxyDSN)
	if err != nil {
		t.Fatalf("Failed to connect to PGTest proxy: %v", err)
	}
	defer pgtestDB.Close()

	schema := getTestSchema()
	nonExistTableName := postgres.QuoteQualifiedName(schema, "nonexistent_table")

	assertTableDoesNotExist(t, pgtestDB, nonExistTableName, "Table does not exist after pgtest rollback")

	// Testa query em tabela inexistente
	_, err = pgtestDB.Exec("SELECT * FROM nonexistent_table")
	if err == nil {
		t.Error("Expected error for querying nonexistent table, got nil")
	} else {
		errStr := err.Error()
		if strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "não existe") {
			t.Logf("SUCCESS: pgtest correctly forwarded error for nonexistent table: %v", err)
		} else {
			t.Logf("Note: Received error (may be valid): %v", err)
		}
	}

	createTableWithValueColumn(t, pgtestDB, nonExistTableName)

	// Testa query com atributo inexistente
	_, err = pgtestDB.Exec(fmt.Sprintf("SELECT nonexistent_column FROM %s", nonExistTableName))
	if err == nil {
		t.Error("Expected error for querying nonexistent column, got nil")
	} else {
		errStr := err.Error()
		if strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "não existe") || strings.Contains(errStr, "column") {
			t.Logf("SUCCESS: pgtest correctly forwarded error for nonexistent column: %v", err)
		} else {
			t.Logf("Note: Received error (may be valid): %v", err)
		}
	}

	// Testa INSERT com atributo inexistente
	_, err = pgtestDB.Exec(fmt.Sprintf("INSERT INTO %s (nonexistent_column) VALUES ('test')", nonExistTableName))
	if err == nil {
		t.Error("Expected error for INSERT with nonexistent column, got nil")
	} else {
		errStr := err.Error()
		if strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "não existe") || strings.Contains(errStr, "column") {
			t.Logf("SUCCESS: pgtest correctly forwarded error for INSERT with nonexistent column: %v", err)
		} else {
			t.Logf("Note: Received error (may be valid): %v", err)
		}
	}

	// Testa sintaxe SQL inválida
	_, err = pgtestDB.Exec("SELECT * FROM WHERE invalid_syntax")
	if err == nil {
		t.Error("Expected error for invalid SQL syntax, got nil")
	} else {
		errStr := err.Error()
		if strings.Contains(errStr, "syntax") || strings.Contains(errStr, "sintaxe") || strings.Contains(errStr, "error") {
			t.Logf("SUCCESS: pgtest correctly forwarded error for invalid SQL syntax: %v", err)
		} else {
			t.Logf("Note: Received error (may be valid): %v", err)
		}
	}

	pingWithTimeout(t, pgtestDB, 5*time.Second, false, "Connection remains valid after handling invalid statements")
	// Limpa a transação
	execPgtestRollback(t, pgtestDB)
}

// TestIsolatedRollbackPerBegin valida COMMIT/ROLLBACK com regras de um único nível:
// - Apenas o primeiro BEGIN cria savepoint; BEGINs seguintes são no-op (não dão erro).
// - Apenas o primeiro COMMIT ou ROLLBACK após BEGIN é "real"; com level 0, COMMIT/ROLLBACK são no-op.
func TestIsolatedRollbackPerBegin(t *testing.T) {
	testID := "test_isolated_rollback"
	pgtestProxyDSN := getPGTestProxyDSN(testID)

	pgtestDB, err := sql.Open("pgx", pgtestProxyDSN)
	if err != nil {
		t.Fatalf("Failed to connect to PGTest proxy: %v", err)
	}
	defer pgtestDB.Close()

	schema := getTestSchema()
	tableName := postgres.QuoteQualifiedName(schema, "pgtest_isolated_rollback")

	assertTableDoesNotExist(t, pgtestDB, tableName, "Table should not exist before test")
	createTableWithValueColumn(t, pgtestDB, tableName)

	// Teste 1: BEGIN → INSERT → BEGIN (no-op) → INSERT → ROLLBACK
	// Com um único nível, ROLLBACK reverte tudo desde o único savepoint (ambos INSERTs).
	t.Run("rollback_reverts_only_last_begin", func(t *testing.T) {
		execBegin(t, pgtestDB, "")
		insertOneRow(t, pgtestDB, tableName, "first_insert", "insert first value in rollback_reverts_only_last_begin test")
		execBegin(t, pgtestDB, "") // no-op: single level
		insertOneRow(t, pgtestDB, tableName, "second_insert", "insert second value in rollback_reverts_only_last_begin test")

		execRollbackOrFail(t, pgtestDB)

		// Single level: rollback reverts to the only savepoint, so both inserts are gone.
		assertQueryCount(t, pgtestDB, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE value = 'first_insert'", tableName), 0, "First INSERT should be rolled back (single level)")
		assertQueryCount(t, pgtestDB, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE value = 'second_insert'", tableName), 0, "Second INSERT should be rolled back")

		execCommit(t, pgtestDB) // real: releases the savepoint
		// Second COMMIT/ROLLBACK would be no-op (level 0)
		assertQueryCount(t, pgtestDB, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE value = 'first_insert'", tableName), 0, "Still 0 after COMMIT")
	})

	// Teste 2: BEGIN → INSERT → COMMIT → BEGIN → INSERT → ROLLBACK
	// O ROLLBACK deve reverter apenas o segundo INSERT (o primeiro já foi commitado)
	t.Run("rollback_after_commit_only_affects_uncommitted", func(t *testing.T) {
		// Primeiro BEGIN
		_, err = pgtestDB.Exec("BEGIN")
		if err != nil {
			t.Fatalf("Failed to execute first BEGIN: %v", err)
		}

		// Primeiro INSERT
		insertOneRow(t, pgtestDB, tableName, "committed_insert", "insert committed value in rollback_after_commit_only_affects_uncommitted test")
		if err != nil {
			t.Fatalf("Failed to insert committed value: %v", err)
		}

		// COMMIT do primeiro BEGIN
		execCommit(t, pgtestDB)

		// Segundo BEGIN
		execBegin(t, pgtestDB, "")

		// Segundo INSERT
		insertOneRow(t, pgtestDB, tableName, "uncommitted_insert", "insert uncommitted value in rollback_after_commit_only_affects_uncommitted test")
		if err != nil {
			t.Fatalf("Failed to insert uncommitted value: %v", err)
		}

		// ROLLBACK deve reverter apenas o segundo INSERT
		execRollbackOrFail(t, pgtestDB)

		// Verifica que o primeiro INSERT (commitado) ainda existe
		assertQueryCount(t, pgtestDB, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE value = 'committed_insert'", tableName), 1, "Committed INSERT should still exist")
		// Verifica que o segundo INSERT foi revertido
		assertQueryCount(t, pgtestDB, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE value = 'uncommitted_insert'", tableName), 0, "Uncommitted INSERT should be rolled back")
	})

	// Teste 3: Um único nível — BEGIN → INSERTs → ROLLBACK reverte tudo desde o savepoint
	// Segundo BEGIN é no-op; segundo ROLLBACK é no-op (level 0).
	t.Run("nested_begin_rollback_levels", func(t *testing.T) {
		execBegin(t, pgtestDB, "")
		insertOneRow(t, pgtestDB, tableName, "level1", "insert level 1")
		execBegin(t, pgtestDB, "") // no-op
		insertOneRow(t, pgtestDB, tableName, "level2", "insert level 2")
		_, err = pgtestDB.Exec("BEGIN")
		if err != nil {
			t.Fatalf("Failed to execute BEGIN: %v", err)
		}
		insertOneRow(t, pgtestDB, tableName, "level3", "insert level 3")

		execRollbackOrFail(t, pgtestDB) // real: reverts to only savepoint, so level1/2/3 all gone
		assertQueryCount(t, pgtestDB, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE value = 'level3'", tableName), 0, "Level 3 should be rolled back")
		assertQueryCount(t, pgtestDB, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE value = 'level2'", tableName), 0, "Level 2 should be rolled back (single level)")
		assertQueryCount(t, pgtestDB, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE value = 'level1'", tableName), 0, "Level 1 should be rolled back (single level)")

		execRollbackOrFail(t, pgtestDB) // no-op (level 0)
		execCommit(t, pgtestDB)         // real
		assertQueryCount(t, pgtestDB, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE value = 'level1'", tableName), 0, "Level 1 still 0 after COMMIT")
	})

	// Teste 4: BEGIN → INSERT → COMMIT → BEGIN → INSERT → COMMIT → BEGIN → INSERT → ROLLBACK
	// O ROLLBACK deve reverter apenas o terceiro INSERT
	t.Run("rollback_after_multiple_commits", func(t *testing.T) {
		// Primeiro ciclo: BEGIN → INSERT → COMMIT
		_, err = pgtestDB.Exec("BEGIN")
		if err != nil {
			t.Fatalf("Failed to execute BEGIN cycle 1: %v", err)
		}
		insertOneRow(t, pgtestDB, tableName, "cycle1", "insert cycle 1 in rollback_after_multiple_commits test")
		execCommit(t, pgtestDB)

		// Segundo ciclo: BEGIN → INSERT → COMMIT
		execBegin(t, pgtestDB, "")
		insertOneRow(t, pgtestDB, tableName, "cycle2", "insert cycle 2 in rollback_after_multiple_commits test")
		execCommit(t, pgtestDB)

		// Terceiro ciclo: BEGIN → INSERT → ROLLBACK
		execBegin(t, pgtestDB, "")
		insertOneRow(t, pgtestDB, tableName, "cycle3", "insert cycle 3 in rollback_after_multiple_commits test")
		execRollbackOrFail(t, pgtestDB)

		// Verifica que apenas os dois primeiros ciclos existem
		assertQueryCount(t, pgtestDB, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE value = 'cycle1'", tableName), 1, "Cycle 1 should exist")
		assertQueryCount(t, pgtestDB, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE value = 'cycle2'", tableName), 1, "Cycle 2 should exist")
		assertQueryCount(t, pgtestDB, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE value = 'cycle3'", tableName), 0, "Cycle 3 should be rolled back")
	})

	// Limpa a transação do pgtest
	execPgtestRollback(t, pgtestDB)

	// Verifica que a tabela não existe mais após o rollback do pgtest
	assertTableDoesNotExist(t, pgtestDB, tableName, "Table does not exist after pgtest rollback")
}
