package tstproxy

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"pgtest-sandbox/internal/config"
	"pgtest-sandbox/internal/proxy"

	_ "github.com/jackc/pgx/v5/stdlib" // Driver para database/sql
)

const (
	DEFAULT_SELECT_ONE    = "-- ping"
	DEFAULT_SELECT_ZERO   = "-- ping"
	FULLROLLBACK_SENTINEL = "-- fullrollback"
)

// TestPostgreSQLConnection testa se conseguimos conectar ao PostgreSQL real
// usando a biblioteca padrão database/sql com driver pgx
// Este teste verifica se o PostgreSQL está acessível e se a biblioteca funciona corretamente
func TestPostgreSQLConnection(t *testing.T) {
	// Carrega configuração do pgtest.yaml
	configPath := GetConfigPath()
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		t.Skipf("Skipping test - failed to load config: %v", err)
		return
	}

	// Constrói DSN usando configurações do pgtest.yaml
	dsn := buildDSN(
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
		cfg.Postgres.User,
		cfg.Postgres.Password,
		"", // Sem application_name para teste básico
	)

	// Conecta ao PostgreSQL usando database/sql (biblioteca padrão)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	// Define timeouts para a conexão
	db.SetConnMaxLifetime(time.Second * 30)
	db.SetMaxOpenConns(1)

	// Testa a conexão fazendo um ping
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Failed to ping PostgreSQL database: %v\n"+
			"This test verifies that:\n"+
			"1. PostgreSQL is running and accessible\n"+
			"2. The connection parameters in pgtest.yaml are correct\n"+
			"3. The pgx library can connect to PostgreSQL\n"+
			"\nConfig used:\n"+
			"  Host: %s\n"+
			"  Port: %d\n"+
			"  Database: %s\n"+
			"  User: %s\n",
			err, cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database, cfg.Postgres.User)
	}

	// Executa uma query simples para verificar que a conexão funciona completamente
	var result int
	err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if result != 1 {
		t.Errorf("Expected query result to be 1, got %d", result)
	}

	// Verifica a versão do PostgreSQL
	var version string
	err = db.QueryRowContext(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		t.Fatalf("Failed to get PostgreSQL version: %v", err)
	}

	t.Logf("Successfully connected to PostgreSQL")
	t.Logf("PostgreSQL version: %s", version)
	t.Logf("Connection parameters: host=%s, port=%d, database=%s, user=%s",
		cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database, cfg.Postgres.User)
}

// TestPostgreSQLConnectionWithApplicationName testa conexão com application_name específico
// Isso verifica se o pgtest pode usar application_name ao conectar ao PostgreSQL
func TestPostgreSQLConnectionWithApplicationName(t *testing.T) {
	configPath := GetConfigPath()
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		t.Skipf("Skipping test - failed to load config: %v", err)
		return
	}

	// Constrói DSN com application_name
	dsn := buildDSN(
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
		cfg.Postgres.User,
		cfg.Postgres.Password,
		"pgtest_connection_test", // application_name
	)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	db.SetConnMaxLifetime(time.Second * 30)
	db.SetMaxOpenConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Failed to ping PostgreSQL with application_name: %v", err)
	}

	// Verifica se o application_name foi definido corretamente
	var appName string
	err = db.QueryRowContext(ctx, "SELECT current_setting('application_name')").Scan(&appName)
	if err != nil {
		t.Fatalf("Failed to get application_name: %v", err)
	}

	if appName != "pgtest_connection_test" {
		t.Errorf("Expected application_name to be 'pgtest_connection_test', got '%s'", appName)
	}

	t.Logf("Successfully connected with application_name: %s", appName)
}

// TestBackendStartupCacheFromPostgreSQL verifies that when we connect to the real PostgreSQL
// (via GetOrCreateSession), the backend startup cache is filled with ParameterStatus messages
// from the server so pgtest can replay them to clients instead of hardcoded values.
func TestBackendStartupCacheFromPostgreSQL(t *testing.T) {
	configPath := GetConfigPath()
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		t.Skipf("Skipping test - failed to load config: %v", err)
		return
	}

	pgtest := proxy.NewPGTest(
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
		cfg.Postgres.User,
		cfg.Postgres.Password,
		getOrDefault(cfg.Test.QueryTimeout.Duration, 5*time.Second),
		cfg.Postgres.SessionTimeout.Duration,
		0,
	)

	// Before any session, cache should be nil
	if c := pgtest.GetBackendStartupCache(); c != nil {
		t.Logf("Cache already set before first session (possible from another test); continuing")
	}

	// Connect to real PostgreSQL (creates session and fills cache)
	testID := "pgtest_backend_startup_cache_test"
	_, err = pgtest.GetOrCreateSession(testID)
	if err != nil {
		t.Skipf("Skipping test - PostgreSQL not available: %v", err)
		return
	}
	defer func() { _ = pgtest.DestroySession(testID) }()

	cache := pgtest.GetBackendStartupCache()
	if cache == nil {
		t.Fatal("GetBackendStartupCache() should be non-nil after first connection to PostgreSQL")
	}
	if len(cache.ParameterStatuses) == 0 {
		t.Fatal("Backend startup cache should contain at least one ParameterStatus from the real server")
	}

	// Check that we have at least server_version (expected from any PostgreSQL)
	gotServerVersion := false
	for _, ps := range cache.ParameterStatuses {
		if ps.Name == "server_version" && ps.Value != "" {
			gotServerVersion = true
			t.Logf("server_version from real PostgreSQL: %s", ps.Value)
			break
		}
	}
	if !gotServerVersion {
		names := make([]string, len(cache.ParameterStatuses))
		for i := range cache.ParameterStatuses {
			names[i] = cache.ParameterStatuses[i].Name
		}
		t.Errorf("Cache should include server_version from real server; got %d params: %v",
			len(cache.ParameterStatuses), names)
	}

	t.Logf("Backend startup cache filled with %d ParameterStatus entries from real PostgreSQL", len(cache.ParameterStatuses))
}

// runSessionQueryOne runs "SELECT 1" and "SELECT current_setting('application_name')" on the session,
// returns (1, appName, nil) or (0, "", err). Caller must have a valid session with active transaction.
func runSessionQueryOne(t *testing.T, session *proxy.TestSession, ctx context.Context) (one int, appName string, err error) {
	t.Helper()
	if session == nil || session.DB == nil {
		return 0, "", fmt.Errorf("session or session.DB is nil")
	}
	rows, qerr := session.DB.Query(ctx, "SELECT 1")
	if qerr != nil {
		return 0, "", fmt.Errorf("SELECT 1: %w", qerr)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, "", fmt.Errorf("SELECT 1: no row")
	}
	if serr := rows.Scan(&one); serr != nil {
		return 0, "", fmt.Errorf("SELECT 1 scan: %w", serr)
	}
	if rows.Next() {
		return 0, "", fmt.Errorf("SELECT 1: unexpected second row")
	}

	rows2, qerr2 := session.DB.Query(ctx, "SELECT current_setting('application_name')")
	if qerr2 != nil {
		return one, "", fmt.Errorf("current_setting: %w", qerr2)
	}
	defer rows2.Close()
	if !rows2.Next() {
		return one, "", fmt.Errorf("current_setting: no row")
	}
	if serr2 := rows2.Scan(&appName); serr2 != nil {
		return one, "", fmt.Errorf("current_setting scan: %w", serr2)
	}
	return one, appName, nil
}

// TestPostgreSQLConnectionLifecycle runs a full connection lifecycle: first connection with queries,
// disconnect and reconnect, then two clients (sessions) using the backend simultaneously.
func TestPostgreSQLConnectionLifecycle(t *testing.T) {
	configPath := GetConfigPath()
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		t.Skipf("Skipping test - failed to load config: %v", err)
		return
	}

	pgtest := proxy.NewPGTest(
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
		cfg.Postgres.User,
		cfg.Postgres.Password,
		getOrDefault(cfg.Test.QueryTimeout.Duration, 5*time.Second),
		cfg.Postgres.SessionTimeout.Duration,
		0,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	testID1 := "pgtest_lifecycle_client1"

	// --- Phase 1: First connection ---
	session1, err := pgtest.GetOrCreateSession(testID1)
	if err != nil {
		t.Skipf("Skipping test - PostgreSQL not available: %v", err)
		return
	}

	if session1.DB == nil || !session1.DB.HasActiveTransaction() {
		t.Fatal("New session must have DB and active transaction")
	}

	one, appName, err := runSessionQueryOne(t, session1, ctx)
	if err != nil {
		t.Fatalf("Phase 1 query: %v", err)
	}
	if one != 1 {
		t.Errorf("Phase 1 SELECT 1: got %d, want 1", one)
	}
	wantAppName := "pgtest-" + testID1
	if appName != wantAppName {
		t.Errorf("Phase 1 application_name: got %q, want %q", appName, wantAppName)
	}
	t.Logf("Phase 1 OK: SELECT 1=%d, application_name=%s", one, appName)

	cache := pgtest.GetBackendStartupCache()
	if cache == nil || len(cache.ParameterStatuses) == 0 {
		t.Fatal("Phase 1: backend startup cache should be filled after first connection")
	}
	t.Logf("Phase 1: backend cache has %d parameter statuses", len(cache.ParameterStatuses))

	// --- Phase 2: Disconnect and reconnect ---
	if err := pgtest.DestroySession(testID1); err != nil {
		t.Fatalf("DestroySession: %v", err)
	}
	if pgtest.GetSession(testID1) != nil {
		t.Fatal("Session should be nil after DestroySession")
	}

	session2, err := pgtest.GetOrCreateSession(testID1)
	if err != nil {
		t.Fatalf("Reconnect GetOrCreateSession: %v", err)
	}
	// New session instance (new connection)
	if session2.DB == nil || !session2.DB.HasActiveTransaction() {
		t.Fatal("Reconnected session must have DB and active transaction")
	}

	one2, appName2, err := runSessionQueryOne(t, session2, ctx)
	if err != nil {
		t.Fatalf("Phase 2 query after reconnect: %v", err)
	}
	if one2 != 1 {
		t.Errorf("Phase 2 SELECT 1: got %d, want 1", one2)
	}
	if appName2 != wantAppName {
		t.Errorf("Phase 2 application_name: got %q, want %q", appName2, wantAppName)
	}
	t.Logf("Phase 2 OK: disconnect/reconnect, SELECT 1=%d, application_name=%s", one2, appName2)

	defer func() { _ = pgtest.DestroySession(testID1) }()

	// --- Phase 3: Two clients simultaneously ---
	testIDA := "pgtest_lifecycle_clientA"
	testIDB := "pgtest_lifecycle_clientB"

	type result struct {
		one     int
		appName string
		err     error
	}

	doneA := make(chan result, 1)
	doneB := make(chan result, 1)

	go func() {
		sess, err := pgtest.GetOrCreateSession(testIDA)
		if err != nil {
			doneA <- result{err: err}
			return
		}
		one, app, err := runSessionQueryOne(t, sess, ctx)
		doneA <- result{one: one, appName: app, err: err}
	}()
	go func() {
		sess, err := pgtest.GetOrCreateSession(testIDB)
		if err != nil {
			doneB <- result{err: err}
			return
		}
		one, app, err := runSessionQueryOne(t, sess, ctx)
		doneB <- result{one: one, appName: app, err: err}
	}()

	resA := <-doneA
	resB := <-doneB

	if resA.err != nil {
		t.Fatalf("Phase 3 client A: %v", resA.err)
	}
	if resB.err != nil {
		t.Fatalf("Phase 3 client B: %v", resB.err)
	}
	if resA.one != 1 || resB.one != 1 {
		t.Errorf("Phase 3 SELECT 1: A=%d, B=%d; want both 1", resA.one, resB.one)
	}
	wantAppA, wantAppB := "pgtest-"+testIDA, "pgtest-"+testIDB
	if resA.appName != wantAppA {
		t.Errorf("Phase 3 client A application_name: got %q, want %q", resA.appName, wantAppA)
	}
	if resB.appName != wantAppB {
		t.Errorf("Phase 3 client B application_name: got %q, want %q", resB.appName, wantAppB)
	}
	t.Logf("Phase 3 OK: two clients simultaneously, A=(%d, %s), B=(%d, %s)",
		resA.one, resA.appName, resB.one, resB.appName)

	// Cleanup
	_ = pgtest.DestroySession(testIDA)
	_ = pgtest.DestroySession(testIDB)
}
