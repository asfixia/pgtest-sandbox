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

// TestPGAdminConnectionNoTimeout testa se o pgAdmin consegue conectar sem timeout
// Este teste verifica se o handshake completo está funcionando corretamente
func TestPGAdminConnectionNoTimeout(t *testing.T) {
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
		testPort = 5439 // Porta diferente para este teste
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

	// Conecta ao servidor pgtest usando database/sql (como pgAdmin faz)
	// pgAdmin não especifica application_name quando conecta, então usamos vazio
	dsn := buildDSN(testHost, testPort, cfg.Postgres.Database, cfg.Postgres.User, cfg.Postgres.Password, "")
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	db.SetConnMaxLifetime(time.Second * 30)
	db.SetMaxOpenConns(1)

	// Testa a conexão fazendo um ping (como pgAdmin faz)
	// Este é o ponto crítico onde o timeout estava ocorrendo
	pingTimeout := cfg.Test.PingTimeout.Duration
	if pingTimeout == 0 {
		pingTimeout = 3 * time.Second
	}
	pingCtx, pingCancel := context.WithTimeout(ctx, pingTimeout)
	defer pingCancel()

	startTime := time.Now()
	if err := db.PingContext(pingCtx); err != nil {
		elapsed := time.Since(startTime)
		t.Fatalf("Failed to ping pgtest server (elapsed: %v): %v\n"+
			"This indicates the pgtest server is not completing the handshake correctly.\n"+
			"The server should respond to Ping() within the configured timeout (%v).",
			elapsed, err, pingTimeout)
	}
	elapsed := time.Since(startTime)
	t.Logf("Successfully pinged pgtest server in %v (timeout was %v)", elapsed, pingTimeout)

	// Verifica que o ping não demorou muito (deve ser rápido após handshake completo)
	if elapsed > pingTimeout/2 {
		t.Logf("Warning: Ping took %v, which is more than half of the timeout (%v)", elapsed, pingTimeout)
	}

	// Executa uma query simples para garantir que a conexão está funcionando
	queryTimeout := cfg.Test.QueryTimeout.Duration
	if queryTimeout == 0 {
		queryTimeout = 5 * time.Second
	}
	queryCtx, queryCancel := context.WithTimeout(ctx, queryTimeout)
	defer queryCancel()

	var result int
	err = db.QueryRowContext(queryCtx, "SELECT 1").Scan(&result)
	if err != nil {
		t.Fatalf("Failed to execute query after ping: %v", err)
	}

	if result != 1 {
		t.Errorf("Expected query result to be 1, got %d", result)
	}

	t.Logf("Successfully connected to pgtest server and executed query without timeout")
	t.Logf("This confirms the pgtest server correctly handles the full PostgreSQL handshake")
}
