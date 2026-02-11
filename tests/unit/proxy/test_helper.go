package tstproxy

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"pgtest-sandbox/internal/config"
	"pgtest-sandbox/internal/proxy"
	"pgtest-sandbox/internal/testutil"
	sqlpkg "pgtest-sandbox/pkg/sql"

	"github.com/jackc/pgx/v5"
)

// proxyTestServer is the shared proxy server started by TestMain for tests that connect without starting their own server.
// When PGPTEST_USE_EXTERNAL_SERVER=1, this is nil and tests assume a server is already running (e.g. for debugging).
var proxyTestServer *proxy.Server

// TestMain starts one proxy server for the package (unless PGPTEST_USE_EXTERNAL_SERVER is set), then runs tests.
// Tests use connectToRunningProxy(t, testID) to connect to this server instead of starting their own.
func TestMain(m *testing.M) {
	cfg, err := config.LoadConfig(testutil.ConfigPath())
	if err != nil {
		fmt.Fprintf(os.Stderr, "proxy test: failed to load config: %v\n", err)
		os.Exit(1)
	}
	useExternal := os.Getenv("PGTEST_USE_EXTERNAL_SERVER") == "1" || os.Getenv("PGTEST_USE_EXTERNAL_SERVER") == "true"
	if useExternal {
		code := m.Run()
		os.Exit(code)
	}
	port := cfg.Proxy.ListenPort
	if port <= 0 {
		port = 5433
	}
	host := cfg.Proxy.ListenHost
	if host == "" || host == "localhost" {
		host = "127.0.0.1" // use IPv4 so client and server match on Windows
	}
	proxyTestServer = proxy.NewServer(
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Proxy.Timeout,
		cfg.Postgres.SessionTimeout.Duration,
		0,
		host,
		port,
		false,
	)
	if err := proxyTestServer.StartError(); err != nil {
		fmt.Fprintf(os.Stderr, "proxy test: failed to start server: %v\n", err)
		os.Exit(1)
	}
	// Wait until the proxy is accepting connections (avoids connection refused in first tests)
	for i := 0; i < 50; i++ {
		if isPortInUse(host, port) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	code := m.Run()
	proxyTestServer.Stop()
	time.Sleep(200 * time.Millisecond)
	os.Exit(code)
}

// getConfigForProxyTest loads config for proxy tests. Skips the test if config cannot be loaded.
func getConfigForProxyTest(t *testing.T) *config.Config {
	t.Helper()
	cfg, err := config.LoadConfig(testutil.ConfigPath())
	if err != nil {
		t.Skipf("Skipping - failed to load config: %v", err)
		return nil
	}
	return cfg
}

// connectToRunningProxyWithAppName connects to the shared proxy server (started in TestMain) with the given application_name.
// Use this when the test needs a specific application_name (e.g. empty for pgAdmin-style tests).
func connectToRunningProxyWithAppName(t *testing.T, applicationName string) *sql.DB {
	t.Helper()
	cfg := getConfigForProxyTest(t)
	if cfg == nil {
		return nil
	}
	dsn := buildDSN(cfg.Proxy.ListenHost, cfg.Proxy.ListenPort, cfg.Postgres.Database, cfg.Postgres.User, cfg.Postgres.Password, applicationName)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("Failed to open connection to proxy: %v", err)
	}
	pingWithTimeout(t, db, 5*time.Second)
	return db
}

// connectToRunningProxy connects to the shared proxy server with application_name = "pgtest_"+testID.
// Use this for tests that run against the server started in TestMain (like integration tests).
func connectToRunningProxy(t *testing.T, testID string) *sql.DB {
	return connectToRunningProxyWithAppName(t, "pgtest_"+testID)
}

// openDBToProxy opens a database connection to the proxy at cfg.Proxy host:port with the given applicationName.
func openDBToProxy(t *testing.T, cfg *config.Config, applicationName string) *sql.DB {
	t.Helper()
	dsn := buildDSN(cfg.Proxy.ListenHost, cfg.Proxy.ListenPort, cfg.Postgres.Database, cfg.Postgres.User, cfg.Postgres.Password, applicationName)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("Failed to open connection to proxy: %v", err)
	}
	pingWithTimeout(t, db, 5*time.Second)
	return db
}

// connectToProxyForTest starts a dedicated proxy for the test, connects with application_name = "pgtest_"+testID,
// and returns (db, ctx, cleanup). Cleanup closes the client first then stops the server to avoid shutdown deadlock.
func connectToProxyForTest(t *testing.T, testID string) (*sql.DB, context.Context, func()) {
	t.Helper()
	cfg := getConfigForProxyTest(t)
	if cfg == nil {
		return nil, nil, func() {}
	}
	proxyServer := proxy.NewServer(
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Proxy.Timeout,
		cfg.Postgres.SessionTimeout.Duration,
		0,
		cfg.Proxy.ListenHost,
		cfg.Proxy.ListenPort,
		false,
	)
	if err := proxyServer.StartError(); err != nil {
		t.Fatalf("Failed to start proxy server: %v", err)
	}
	db := openDBToProxy(t, cfg, "pgtest_"+testID)
	if db == nil {
		return nil, nil, func() {}
	}
	ctx, cancel := context.WithTimeout(context.Background(), getOrDefault(proxy.ConnectionTimeout, 15*time.Second)*300)
	cleanup := func() {
		cancel()
		db.Close()
		stopProxyServer(proxyServer)
	}
	return db, ctx, cleanup
}

// connectToProxyForTestWithAppName starts a dedicated proxy and connects with the given application_name.
func connectToProxyForTestWithAppName(t *testing.T, applicationName string) (*sql.DB, context.Context, func()) {
	t.Helper()
	cfg := getConfigForProxyTest(t)
	if cfg == nil {
		return nil, nil, func() {}
	}
	proxyServer := proxy.NewServer(
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Proxy.Timeout,
		cfg.Postgres.SessionTimeout.Duration,
		0,
		cfg.Proxy.ListenHost,
		cfg.Proxy.ListenPort,
		false,
	)
	if err := proxyServer.StartError(); err != nil {
		t.Fatalf("Failed to start proxy server: %v", err)
	}
	db := openDBToProxy(t, cfg, applicationName)
	if db == nil {
		return nil, nil, func() {}
	}
	ctx, cancel := context.WithTimeout(context.Background(), getOrDefault(proxy.ConnectionTimeout, 15*time.Second)*300)
	cleanup := func() {
		cancel()
		db.Close()
		stopProxyServer(proxyServer)
	}
	return db, ctx, cleanup
}

// connectToProxyForTestWithServer is like connectToProxyForTest but also returns the proxy server (e.g. for GetSession checks).
func connectToProxyForTestWithServer(t *testing.T, testID string) (*sql.DB, context.Context, *proxy.Server, func()) {
	t.Helper()
	cfg := getConfigForProxyTest(t)
	if cfg == nil {
		return nil, nil, nil, func() {}
	}
	proxyServer := proxy.NewServer(
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Proxy.Timeout,
		cfg.Postgres.SessionTimeout.Duration,
		0,
		cfg.Proxy.ListenHost,
		cfg.Proxy.ListenPort,
		false,
	)
	if err := proxyServer.StartError(); err != nil {
		t.Fatalf("Failed to start proxy server: %v", err)
	}
	db := openDBToProxy(t, cfg, "pgtest_"+testID)
	if db == nil {
		return nil, nil, nil, func() {}
	}
	ctx, cancel := context.WithTimeout(context.Background(), getOrDefault(proxy.ConnectionTimeout, 15*time.Second)*300)
	cleanup := func() {
		cancel()
		db.Close()
		stopProxyServer(proxyServer)
	}
	return db, ctx, proxyServer, cleanup
}

// connectToProxyForTestWithAppNameAndServer is like connectToProxyForTestWithAppName but also returns the proxy server.
func connectToProxyForTestWithAppNameAndServer(t *testing.T, applicationName string) (*sql.DB, context.Context, *proxy.Server, func()) {
	t.Helper()
	cfg := getConfigForProxyTest(t)
	if cfg == nil {
		return nil, nil, nil, func() {}
	}
	proxyServer := proxy.NewServer(
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Proxy.Timeout,
		cfg.Postgres.SessionTimeout.Duration,
		0,
		cfg.Proxy.ListenHost,
		cfg.Proxy.ListenPort,
		false,
	)
	if err := proxyServer.StartError(); err != nil {
		t.Fatalf("Failed to start proxy server: %v", err)
	}
	db := openDBToProxy(t, cfg, applicationName)
	if db == nil {
		return nil, nil, nil, func() {}
	}
	ctx, cancel := context.WithTimeout(context.Background(), getOrDefault(proxy.ConnectionTimeout, 15*time.Second)*300)
	cleanup := func() {
		cancel()
		db.Close()
		stopProxyServer(proxyServer)
	}
	return db, ctx, proxyServer, cleanup
}

func newPGTestFromConfig() *proxy.PGTest {
	return proxy.NewPGTestFromConfigForTesting()
}

func newTestSession(pgtest *proxy.PGTest) *proxy.TestSession {
	return proxy.NewTestSessionForTesting(pgtest)
}

func newTestSessionWithLevel(pgtest *proxy.PGTest, testID string) *proxy.TestSession {
	return proxy.NewTestSessionWithLevel(pgtest, testID)
}

func contains(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	if len(s) < len(substr) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func buildDSN(host string, port int, database, user, password, applicationName string) string {
	dsn := "host=" + host + " port=" + strconv.Itoa(port) + " database=" + database + " user=" + user
	if password != "" {
		dsn += " password=" + password
	}
	if applicationName != "" {
		dsn += " application_name=" + applicationName
	}
	return dsn
}

func getOrDefault(value time.Duration, defaultValue time.Duration) time.Duration {
	if value <= 0 {
		return defaultValue
	}
	return value
}

func coalesce(value string, defaultValue string) string {
	if value == "" {
		return defaultValue
	}
	return value
}

// GetConfigPath returns the config file path (unified via testutil). Kept for backward compatibility
// so tests/unit/config and other packages that import tstproxy can keep using GetConfigPath().
func GetConfigPath() string {
	return testutil.ConfigPath()
}

func isPortInUse(host string, port int) bool {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(port)), 100*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func ensurePortIsAvailable(t *testing.T, host string, port int) {
	if isPortInUse(host, port) {
		t.Fatalf("Port %s:%d is already in use", host, port)
	}
}

func waitForProxyServerToListen(t *testing.T, host string, port int) {
	maxAttempts := 20
	for i := 0; i < maxAttempts; i++ {
		if isPortInUse(host, port) {
			return
		}
		time.Sleep(100 * time.Millisecond)
		if i == maxAttempts-1 {
			t.Fatalf("Proxy server is not listening on %s:%d", host, port)
		}
	}
}

func stopProxyServer(proxyServer *proxy.Server) {
	if proxyServer != nil {
		proxyServer.Stop()
		// Aguarda um pouco para garantir que a porta seja liberada
		time.Sleep(200 * time.Millisecond)
	}
}

// pingWithTimeout executa um ping na conexão com timeout especificado.
// Falha o teste se o ping não for bem-sucedido.
func pingWithTimeout(t *testing.T, db *sql.DB, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout*1000)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		t.Fatalf("Failed to ping database connection: %v", err)
	}
}

// QueryContextLastResult runs all statements in the query (multi-statement Exec)
// and returns the last result set only. The last result may have zero or more rows.
// It does not detect "last with results" — it runs all commands and returns the last result.
func QueryContextLastResult(t *testing.T, db *sql.DB, ctx context.Context, query string) (*sql.Rows, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	if conn == nil {
		// Some drivers/pool states can return (nil, nil); treat as failure so callers get a clear error.
		return nil, fmt.Errorf("driver returned nil connection with no error (conn == nil, err == nil); proxy may not be responding")
	}
	defer conn.Close()

	var pgxConn *pgx.Conn
	err = conn.Raw(func(driverConn interface{}) error {
		type pgxDriverConn interface {
			Conn() *pgx.Conn
		}
		if stdlibConn, ok := driverConn.(pgxDriverConn); ok {
			pgxConn = stdlibConn.Conn()
			return nil
		}
		return fmt.Errorf("unable to extract pgx connection from driver")
	})
	if err != nil {
		return nil, fmt.Errorf("failed to extract pgx connection: %w", err)
	}

	// Run all commands once (side effects apply)
	pgConn := pgxConn.PgConn()
	mrr := pgConn.Exec(ctx, query)
	defer mrr.Close()

	// Consume all result sets so the protocol is in a consistent state
	for mrr.NextResult() {
		rr := mrr.ResultReader()
		if rr != nil {
			_, _ = rr.Close()
		}
	}
	if err := mrr.Close(); err != nil {
		return nil, fmt.Errorf("error processing multiple command results: %w", err)
	}

	// Last result = last statement in the query; run only it to get its result (empty or not)
	commands := sqlpkg.SplitCommands(query)
	if len(commands) == 0 {
		return nil, fmt.Errorf("query has no commands")
	}
	lastCmd := commands[len(commands)-1]
	return db.QueryContext(ctx, lastCmd)
}

//// ExecuteMultipleStatements executa múltiplas declarações SQL usando Exec
//// e retorna apenas o número de linhas do último SELECT que retorna resultados.
//// Esta função processa todos os result sets mas retorna apenas o último que tem linhas,
//// que é o comportamento esperado quando usando Exec com múltiplas declarações.
//func ExecuteMultipleStatements(t *testing.T, db *sql.DB, ctx context.Context, query string) int {
//	rows, err := QueryContextLastResult(t, db, ctx, query)
//	if err != nil {
//		t.Fatalf("Failed to execute multiple statements: %v", err)
//	}
//	defer rows.Close()
//
//	rowCount := 0
//	for rows.Next() {
//		rowCount++
//	}
//
//	if err := rows.Err(); err != nil {
//		t.Fatalf("Error iterating rows: %v", err)
//	}
//
//	return rowCount
//}
