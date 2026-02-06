package tstproxy

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"pgtest-transient/internal/proxy"
	sqlpkg "pgtest-transient/pkg/sql"

	"github.com/jackc/pgx/v5"
)

func newPGTestFromConfig() *proxy.PGTest {
	return proxy.NewPGTestFromConfigForTesting()
}

func newTestSession(pgtest *proxy.PGTest) *proxy.TestSession {
	return proxy.NewTestSessionForTesting(pgtest)
}

func newTestSessionWithLevel(pgtest *proxy.PGTest, testID string, savepointQuantity int) *proxy.TestSession {
	return proxy.NewTestSessionWithLevel(pgtest, testID, savepointQuantity)
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

// getConfigPath retorna o caminho do arquivo de config, resolvendo relativo à raiz do projeto
func getConfigPath() string {
	envPath := os.Getenv("PGTEST_CONFIG")
	if envPath != "" {
		if filepath.IsAbs(envPath) {
			return envPath
		}
		projectRoot := findProjectRoot()
		if projectRoot != "" {
			return filepath.Join(projectRoot, envPath)
		}
		return envPath
	}
	projectRoot := findProjectRoot()
	if projectRoot != "" {
		return filepath.Join(projectRoot, "config", "pgtest-transient.yaml")
	}
	return "config/pgtest-transient.yaml"
}

// findProjectRoot encontra a raiz do projeto (onde está go.mod)
func findProjectRoot() string {
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
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
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
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
