package proxy

import (
	"os"
	"path/filepath"
	"time"

	"pgtest-transient/internal/config"
	"pgtest-transient/internal/testutil"
)

// logIfVerbose é um wrapper para testutil.LogIfVerbose para manter compatibilidade
// dentro do pacote proxy
func logIfVerbose(format string, args ...interface{}) {
	testutil.LogIfVerbose(format, args...)
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

// resolveConfigPath resolve o caminho do arquivo de configuração
func resolveConfigPath() string {
	if envPath := os.Getenv("PGTEST_CONFIG"); envPath != "" {
		if filepath.IsAbs(envPath) {
			return envPath
		}
		projectRoot := findProjectRoot()
		if projectRoot != "" {
			return filepath.Join(projectRoot, envPath)
		}
		workDir, _ := os.Getwd()
		return filepath.Join(workDir, envPath)
	}

	projectRoot := findProjectRoot()
	if projectRoot != "" {
		return filepath.Join(projectRoot, "config", "pgtest-transient.yaml")
	}
	workDir, _ := os.Getwd()
	return filepath.Join(workDir, "config", "pgtest-transient.yaml")
}

// newPGTestFromConfig cria uma instância PGTest a partir da configuração
// Tenta carregar de:
// 1. Variável de ambiente PGTEST_CONFIG (se definida)
// 2. config/pgtest-transient.yaml (relativo ao diretório de trabalho)
// 3. Busca automática (pasta do executável ou config/)
// Se não conseguir carregar a configuração, usa valores padrão
func newPGTestFromConfig() *PGTest {
	var cfg *config.Config
	var err error
	configPath := resolveConfigPath()

	if configPath != "" {
		logIfVerbose("Trying config path: %s", configPath)
		cfg, err = config.LoadConfig(configPath)
		if err != nil {
			logIfVerbose("Warning: Failed to load config from %s: %v", configPath, err)
			// Tenta busca automática
			configPath = ""
			cfg, err = config.LoadConfig(configPath)
		}
	} else {
		// Busca automática
		cfg, err = config.LoadConfig("")
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
