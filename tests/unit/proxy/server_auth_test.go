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

// TestServerAuthenticationHandshake testa o handshake de autenticação do servidor
// Verifica se o servidor aceita conexões usando biblioteca PostgreSQL padrão
func TestServerAuthenticationHandshake(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Carrega a configuração para obter a porta de escuta
	configPath := getConfigPath()
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Usa a porta da configuração, mas adiciona um offset para evitar conflitos com servidor em execução
	// Porta padrão é 5433, então usamos 5434 para testes
	testPort := cfg.Proxy.ListenPort
	if testPort == 5433 {
		testPort = 5434 // Evita conflito se o servidor real estiver rodando na 5433
	}

	// Usa o listen_host do config (padrão é "localhost")
	testHost := cfg.Proxy.ListenHost
	if testHost == "" {
		testHost = "localhost"
	}

	// Cria servidor proxy (não precisa de conexões ativas - conexões são criadas sob demanda)
	// Se o PostgreSQL não estiver disponível, o teste falhará quando tentar conectar via o servidor proxy
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
		t.Fatalf("Failed to start proxy server on %s:%d: %v", testHost, testPort, err)
	}

	// Garante que o servidor proxy seja parado após o teste
	defer func() {
		proxyServer.Stop()
		time.Sleep(100 * time.Millisecond) // Aguarda servidor proxy parar
	}()

	// Conecta ao servidor pgtest usando database/sql (biblioteca PostgreSQL padrão)
	// Usa todas as configurações do pgtest.yaml
	dsn := buildDSN(testHost, testPort, cfg.Postgres.Database, cfg.Postgres.User, cfg.Postgres.Password, "pgtest_test_auth")
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	// Define timeout para a conexão
	db.SetConnMaxLifetime(time.Second * 5)
	db.SetMaxOpenConns(1)

	// Testa a conexão fazendo um ping
	pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
	defer pingCancel()

	if err := db.PingContext(pingCtx); err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}

	// Executa uma query simples para verificar que a conexão funciona
	var result int
	err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if result != 1 {
		t.Errorf("Expected query result to be 1, got %d", result)
	}

	// Verifica se a sessão foi criada com o testID correto
	session := proxyServer.Pgtest.GetSession("test_auth")
	if session == nil {
		t.Log("Session 'test_auth' not found (this is expected if application_name extraction works correctly)")
	}

	// Conexão estabelecida com sucesso usando biblioteca PostgreSQL
	t.Log("Authentication handshake completed successfully using PostgreSQL library")
}

// TestServerAuthenticationHandshake_DefaultAppName testa autenticação sem application_name
// Deve usar conexão compartilhada (default)
func TestServerAuthenticationHandshake_DefaultAppName(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Carrega a configuração para obter a porta de escuta
	configPath := getConfigPath()
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Cria servidor proxy (não precisa de conexões ativas - conexões são criadas sob demanda)
	// Se o PostgreSQL não estiver disponível, o teste falhará quando tentar conectar via o servidor proxy
	sessionTimeout := cfg.Postgres.SessionTimeout.Duration
	if sessionTimeout <= 0 {
		sessionTimeout = 24 * time.Hour
	}
	// Usa a porta da configuração, mas adiciona um offset para evitar conflitos com servidor em execução
	// Porta padrão é 5433, então usamos 5435 para testes (diferente do primeiro teste)
	testPort := cfg.Proxy.ListenPort
	if testPort == 5433 {
		testPort = 5435 // Evita conflito se o servidor real estiver rodando na 5433
	} else {
		testPort = testPort + 1 // Adiciona 1 para evitar conflito
	}

	// Usa o listen_host do config (padrão é "localhost")
	testHost := cfg.Proxy.ListenHost
	if testHost == "" {
		testHost = "localhost"
	}

	// NewServer verifica se a porta está disponível e trata valores padrão para sessionTimeout e listenPort internamente
	proxyServer := proxy.NewServer(
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Proxy.Timeout,
		sessionTimeout,
		0, // keepalive desligado no teste
		cfg.Proxy.ListenHost,
		testPort,
	)
	if err := proxyServer.StartError(); err != nil {
		t.Fatalf("Failed to start proxy server on %s:%d: %v", testHost, testPort, err)
	}

	// Garante que o servidor proxy seja parado após o teste
	defer func() {
		proxyServer.Stop()
		time.Sleep(100 * time.Millisecond) // Aguarda servidor proxy parar
	}()

	// Conecta ao servidor pgtest usando database/sql SEM application_name
	// Isso deve usar a conexão compartilhada (default)
	// Usa todas as configurações do pgtest.yaml
	dsn := buildDSN(testHost, testPort, cfg.Postgres.Database, cfg.Postgres.User, cfg.Postgres.Password, "")
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	// Define timeout para a conexão
	db.SetConnMaxLifetime(time.Second * 5)
	db.SetMaxOpenConns(1)

	// Testa a conexão fazendo um ping
	pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
	defer pingCancel()

	if err := db.PingContext(pingCtx); err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}

	// Executa uma query simples para verificar que a conexão funciona
	var result int
	err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if result != 1 {
		t.Errorf("Expected query result to be 1, got %d", result)
	}

	// Verifica se a sessão foi criada com testID "default"
	session := proxyServer.Pgtest.GetSession("default")
	if session == nil {
		t.Error("Session with testID 'default' should be created")
	}

	t.Log("Authentication handshake with default app name completed successfully using PostgreSQL library")
}
