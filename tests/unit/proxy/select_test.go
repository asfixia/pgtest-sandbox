package tstproxy

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"pgtest-transient/internal/config"
	"pgtest-transient/internal/proxy"

	_ "github.com/jackc/pgx/v5/stdlib" // Driver para database/sql
)

// TestSelect1AsUm testa conexão ao pgtest usando database/sql e executa SELECT 1 as um
func TestSelect1AsUm(t *testing.T) {
	// Carrega configuração do pgtest.yaml
	configPath := getConfigPath()
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		t.Skipf("Skipping test - failed to load config: %v", err)
		return
	}

	// Usa timeout da configuração ou valor padrão
	contextTimeout := cfg.Test.ContextTimeout.Duration
	if contextTimeout == 0 {
		contextTimeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	// Usa porta diferente para evitar conflitos
	testPort := cfg.Proxy.ListenPort
	if testPort == 5433 {
		testPort = 5438 // Porta diferente para este teste
	}
	testHost := cfg.Proxy.ListenHost
	if testHost == "" {
		testHost = "localhost"
	}

	// Cria servidor proxy (não precisa de conexões ativas) - já inicia automaticamente
	// NewServer verifica se a porta está disponível e trata valores padrão para sessionTimeout e listenPort internamente
	proxyServer := proxy.NewServer(
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Proxy.Timeout,
		cfg.Postgres.SessionTimeout.Duration,
		0, // keepalive desligado no teste
		cfg.Proxy.ListenHost,
		testPort,
	)
	if err := proxyServer.StartError(); err != nil {
		t.Fatalf("Failed to start proxy server: %v", err)
	}

	// Garante que o servidor proxy seja parado após o teste
	defer func() {
		proxyServer.Stop()
		time.Sleep(200 * time.Millisecond)
	}()

	// Conecta ao servidor proxy usando database/sql
	dsn := buildDSN(testHost, testPort, cfg.Postgres.Database, cfg.Postgres.User, cfg.Postgres.Password, "pgtest_select_test")
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	db.SetConnMaxLifetime(time.Second * 30)
	db.SetMaxOpenConns(1)

	queryTimeout := cfg.Test.QueryTimeout.Duration
	if queryTimeout == 0 {
		queryTimeout = 5 * time.Second
	}
	queryCtx, queryCancel := context.WithTimeout(ctx, queryTimeout)
	defer queryCancel()

	var result int
	err = db.QueryRowContext(queryCtx, "SELECT 1 as um").Scan(&result)
	if err != nil {
		t.Fatalf("Failed to execute SELECT 1 as um: %v", err)
	}

	// Verifica que o resultado é 1
	if result != 1 {
		t.Errorf("Expected result to be 1, got %d", result)
	}

	t.Logf("Successfully executed SELECT 1 as um and got result: %d", result)
	t.Logf("This confirms the pgtest server correctly handles SELECT queries with column aliases")
}
