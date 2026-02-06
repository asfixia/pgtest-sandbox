package tstproxy

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"pgtest-transient/internal/config"
	"pgtest-transient/internal/proxy"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestPGAdminLikeConnection(t *testing.T) {
	configPath := getConfigPath()
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		t.Skipf("Skipping test - failed to load config: %v", err)
		return
	}

	contextTimeout := getOrDefault(cfg.Test.ContextTimeout.Duration, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	// Usa porta diferente para evitar conflitos com outros testes
	testPort := cfg.Proxy.ListenPort
	testHost := cfg.Proxy.ListenHost
	// Verifica se a porta está disponível antes de iniciar o servidor
	ensurePortIsAvailable(t, testHost, testPort)

	proxyServer := proxy.NewServer(
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Proxy.Timeout,
		cfg.Postgres.SessionTimeout.Duration,
		0, // keepalive desligado no teste
		testHost,
		testPort,
	)
	if err := proxyServer.StartError(); err != nil {
		t.Fatalf("Failed to start proxy server: %v", err)
	}

	defer stopProxyServer(proxyServer)

	// Aguarda o servidor proxy estar pronto para aceitar conexões
	waitForProxyServerToListen(t, testHost, testPort)

	// Verifica se o PostgreSQL real está disponível antes de continuar
	// Se não estiver disponível, faz skip do teste (pode acontecer quando muitos testes rodam simultaneamente)
	if !isPostgreSQLAvailable(t, cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database, cfg.Postgres.User, cfg.Postgres.Password) {
		t.Skipf("Skipping test - PostgreSQL is not available at %s:%d", cfg.Postgres.Host, cfg.Postgres.Port)
		return
	}

	// Usa application_name específico para este teste
	// Isso garante que o teste usa um testID identificável e pode testar reutilização de conexões
	testApplicationName := "pgtest_pgadmin_connection_test"
	db := connectToProxyServer(t, ctx, testHost, testPort, cfg.Postgres.Database, cfg.Postgres.User, cfg.Postgres.Password, testApplicationName, cfg.Test.PingTimeout.Duration)
	defer db.Close()

	queryTimeout := getOrDefault(cfg.Test.QueryTimeout.Duration, 5*time.Second)
	t.Logf("Setting query timeout to: %v", queryTimeout)
	queryCtx, queryCancel := context.WithTimeout(ctx, queryTimeout)
	defer queryCancel()

	t.Logf("About to execute pgAdmin initial query with timeout: %v", queryTimeout)
	startTime := time.Now()
	rowCount := executePGAdminInitialQuery(t, db, queryCtx)
	if rowCount != 1 {
		t.Fatalf("Expected 1 row, got %d", rowCount)
	}
	elapsed := time.Since(startTime)
	t.Logf("Query executed successfully in %v", elapsed)
	t.Logf("pgAdmin initial query executed successfully, returned %d rows", rowCount)

	verifyConnectionStillWorks(t, db, queryCtx)
}

func connectToProxyServer(t *testing.T, ctx context.Context, host string, port int, database, user, password, applicationName string, pingTimeout time.Duration) *sql.DB {
	dsn := buildDSN(host, port, database, user, password, applicationName)

	// IMPORTANTE: sql.Open() NÃO abre a conexão imediatamente
	// Ele apenas valida os parâmetros e prepara o objeto *sql.DB para uso futuro
	// A conexão real só é estabelecida quando você faz uma operação que requer conexão:
	// - db.Ping() ou db.PingContext() (como fazemos abaixo)
	// - db.Query() ou db.QueryContext()
	// - db.Exec() ou db.ExecContext()
	// etc.
	// Por isso é importante fazer um Ping após Open para verificar se a conexão pode ser estabelecida
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("Failed to open database connection to proxy server: %v\n Connection: %s", err, dsn)
	}

	// Verifica se a conexão pode ser realmente estabelecida
	// Isso é crítico porque sql.Open() não retorna erro se o servidor estiver inacessível
	// Quando todos os testes rodam juntos, pode haver problemas de timing, então tentamos algumas vezes
	timeout := getOrDefault(pingTimeout, 3*time.Second)
	maxRetries := 3
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		pingCtx, pingCancel := context.WithTimeout(context.Background(), timeout)
		err := db.PingContext(pingCtx)
		pingCancel()
		if err == nil {
			t.Logf("Successfully connected and pinged pgtest server")
			return db
		}
		lastErr = err
		if i < maxRetries-1 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	db.Close()
	// Se falhou após todas as tentativas, verifica se é problema de PostgreSQL indisponível
	// Se for, faz skip ao invés de falhar
	if lastErr != nil && contains(lastErr.Error(), "failed to create connection for testID") {
		if contains(lastErr.Error(), "connectex: No connection could be made") || contains(lastErr.Error(), "actively refused") {
			t.Skipf("Skipping test - PostgreSQL appears to be unavailable: %v", lastErr)
			return nil
		}
	}
	t.Fatalf("Failed to ping database connection after %d attempts: %v", maxRetries, lastErr)
	return nil // Nunca alcançado devido ao t.Fatalf acima
}

// isPostgreSQLAvailable verifica se o PostgreSQL está disponível fazendo uma conexão de teste
func isPostgreSQLAvailable(t *testing.T, host string, port int, database, user, password string) bool {
	t.Helper()
	testDSN := buildDSN(host, port, database, user, password, "")
	testDB, err := sql.Open("pgx", testDSN)
	if err != nil {
		return false
	}
	defer testDB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := testDB.PingContext(ctx); err != nil {
		return false
	}
	return true
}

// executePGAdminInitialQuery runs the pgAdmin-style initial multi-statement query
// (SET and SELECT commands) and returns the row count of the last result set only.
// The query is defined here; it runs all commands and returns the last result (with rows if any).
func executePGAdminInitialQuery(t *testing.T, db *sql.DB, ctx context.Context) int {
	query := `SET DateStyle=ISO; SET client_min_messages=notice; SELECT set_config('bytea_output','hex',false) FROM pg_show_all_settings() WHERE name = 'bytea_output'; SET client_encoding='utf-8'; SELECT 1 as um;`
	rows, err := QueryContextLastResult(t, db, ctx, query)
	if err != nil {
		t.Fatalf("Failed to execute pgAdmin initial query: %v", err)
	}
	defer rows.Close()

	rowCount := 0
	for rows.Next() {
		rowCount++
		var result string
		if err := rows.Scan(&result); err != nil {
			t.Fatalf("Failed to scan result: %v", err)
		}
		t.Logf("Query result row %d: %s", rowCount, result)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Error iterating rows: %v", err)
	}
	return rowCount
}

func verifyConnectionStillWorks(t *testing.T, db *sql.DB, ctx context.Context) {
	var result int
	err := db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		t.Fatalf("Failed to execute simple query: %v", err)
	}

	if result != 1 {
		t.Errorf("Expected query result to be 1, got %d", result)
	}
}
