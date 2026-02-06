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

// TestPGTestServerConnection testa se conseguimos conectar ao servidor pgtest
// usando a biblioteca padrão database/sql com driver pgx
// Este teste verifica se o servidor pgtest implementa o protocolo PostgreSQL corretamente
func TestPGTestServerConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Carrega configuração
	configPath := getConfigPath()
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		t.Skipf("Skipping test - failed to load config: %v", err)
		return
	}

	// Usa porta diferente para evitar conflitos
	testPort := cfg.Proxy.ListenPort
	if testPort == 5433 {
		testPort = 5436 // Porta diferente para testes
	}
	testHost := cfg.Proxy.ListenHost
	if testHost == "" {
		testHost = "localhost"
	}

	// Cria servidor proxy (não precisa de conexões ativas - conexões são criadas sob demanda) - já inicia automaticamente
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
		time.Sleep(100 * time.Millisecond)
	}()

	// Conecta ao servidor proxy pgtest usando database/sql (biblioteca padrão)
	dsn := buildDSN(testHost, testPort, cfg.Postgres.Database, cfg.Postgres.User, cfg.Postgres.Password, "pgtest_test_connection")
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("Failed to open database connection to proxy server: %v", err)
	}
	defer db.Close()

	db.SetConnMaxLifetime(time.Second * 30)
	db.SetMaxOpenConns(1)

	// Testa a conexão fazendo um ping
	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pingCancel()

	if err := db.PingContext(pingCtx); err != nil {
		t.Fatalf("Failed to ping pgtest server: %v\n"+
			"This indicates the pgtest server is not implementing the PostgreSQL protocol correctly.\n"+
			"The server should accept connections using standard PostgreSQL libraries.",
			err)
	}

	// Executa uma query simples
	var result int
	err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if result != 1 {
		t.Errorf("Expected query result to be 1, got %d", result)
	}

	t.Logf("Successfully connected to pgtest server using database/sql library")
	t.Logf("This confirms the pgtest server implements PostgreSQL protocol correctly")
}
