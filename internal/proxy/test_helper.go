package proxy

import (
	"time"

	"pgtest-sandbox/internal/config"
	"pgtest-sandbox/internal/testutil"
)

// logIfVerbose é um wrapper para testutil.LogIfVerbose para manter compatibilidade
// dentro do pacote proxy
func logIfVerbose(format string, args ...interface{}) {
	testutil.LogIfVerbose(format, args...)
}

// newPGTestFromConfig cria uma instância PGTest a partir da configuração
// Tenta carregar de:
// 1. Variável de ambiente PGTEST_CONFIG (se definida)
// 2. config/pgtest-sandbox.yaml (relativo ao diretório de trabalho)
// 3. Busca automática (pasta do executável ou config/)
// Se não conseguir carregar a configuração, usa valores padrão
func newPGTestFromConfig() *PGTest {
	var cfg *config.Config
	var err error
	configPath := testutil.ConfigPath()
	if configPath == "" {
		return nil
	}
	logIfVerbose("Trying config path: %s", configPath)
	cfg, err = config.LoadConfig(configPath)
	if err != nil {
		logIfVerbose("Warning: Failed to load config from %s: %v", configPath, err)
		// Tenta busca automática
		configPath = ""
		cfg, err = config.LoadConfig(configPath)
	}

	if err != nil {
		logIfVerbose("Warning: Failed to load config: %v, using defaults", err)
		// Fallback para valores padrão se não conseguir carregar config
		return NewPGTest("localhost", 5432, "postgres", "postgres", "", 3600*time.Second, 24*time.Hour, 0)
	}

	configPathDisplay := "auto"
	if configPath != "" {
		configPathDisplay = configPath
	}
	logIfVerbose("Loaded config from %s: host=%s, port=%d, database=%s, user=%s",
		configPathDisplay, cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database, cfg.Postgres.User)

	keepaliveInterval := time.Duration(0)
	if cfg.Proxy.KeepaliveInterval.Duration > 0 {
		keepaliveInterval = cfg.Proxy.KeepaliveInterval.Duration
	}
	return NewPGTest(
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Proxy.Timeout,
		cfg.Postgres.SessionTimeout.Duration,
		keepaliveInterval,
	)
}
