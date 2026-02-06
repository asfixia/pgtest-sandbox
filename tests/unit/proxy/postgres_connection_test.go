package tstproxy

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"pgtest-transient/internal/config"

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
	configPath := getConfigPath()
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
	configPath := getConfigPath()
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
