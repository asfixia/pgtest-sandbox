package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"pgtest-transient/internal/testutil"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Postgres PostgresConfig `yaml:"postgres"`
	Proxy    ProxyConfig    `yaml:"proxy"`
	Logging  LoggingConfig  `yaml:"logging"`
	Test     TestConfig     `yaml:"test"`
}

type PostgresConfig struct {
	Host           string   `yaml:"host"`
	Port           int      `yaml:"port"`
	Database       string   `yaml:"database"`
	User           string   `yaml:"user"`
	Password       string   `yaml:"password"`
	SessionTimeout Duration `yaml:"session_timeout"` // Timeout de sessão PostgreSQL (idle_in_transaction_session_timeout)
}

type ProxyConfig struct {
	ListenHost        string        `yaml:"listen_host"`
	ListenPort        int           `yaml:"listen_port"`
	Timeout           time.Duration `yaml:"timeout"`
	KeepaliveInterval Duration      `yaml:"keepalive_interval"` // Intervalo de ping para manter conexão viva (ex.: em debugging)
}

type LoggingConfig struct {
	Level string `yaml:"level"`
	File  string `yaml:"file"`
}

// Duration é um tipo customizado para fazer parsing de time.Duration do YAML
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = dur
	return nil
}

type TestConfig struct {
	Schema         string   `yaml:"schema"`
	ContextTimeout Duration `yaml:"context_timeout"` // Timeout para contexto principal dos testes
	QueryTimeout   Duration `yaml:"query_timeout"`   // Timeout para execução de queries
	PingTimeout    Duration `yaml:"ping_timeout"`    // Timeout para ping
}

// LoadConfigResult contém o resultado do carregamento da configuração
type LoadConfigResult struct {
	Config     *Config
	ConfigPath string
}

// LoadConfig carrega a configuração do arquivo especificado ou busca automaticamente
// Se configPathUsed não for nil, será preenchido com o caminho do arquivo usado
func LoadConfig(configPath string) (*Config, error) {
	result, err := LoadConfigWithPath(configPath)
	if err != nil {
		return nil, err
	}
	return result.Config, nil
}

// LoadConfigWithPath carrega a configuração e retorna também o caminho do arquivo usado
func LoadConfigWithPath(configPath string) (*LoadConfigResult, error) {
	config := &Config{
		Postgres: PostgresConfig{
			Host:           "localhost",
			Port:           5432,
			Database:       "postgres",
			User:           "postgres",
			Password:       "",
			SessionTimeout: Duration{Duration: 24 * time.Hour}, // Padrão: 24 horas
		},
		Proxy: ProxyConfig{
			ListenHost:        "localhost",
			ListenPort:        5432,
			Timeout:           3600 * time.Second,
			KeepaliveInterval: Duration{Duration: 60 * time.Second},
		},
		Logging: LoggingConfig{
			Level: "info",
			File:  "",
		},
		Test: TestConfig{
			Schema:         "public",
			ContextTimeout: Duration{Duration: 10 * time.Second},
			QueryTimeout:   Duration{Duration: 5 * time.Second},
			PingTimeout:    Duration{Duration: 3 * time.Second},
		},
	}

	// Determina o caminho do arquivo de configuração
	var finalConfigPath string
	var configFileUsed string

	if configPath != "" {
		// Se fornecido explicitamente, usa o caminho fornecido
		finalConfigPath = configPath
	} else {
		// Por padrão, busca pgtest-transient.yaml na pasta do executável
		execPath, err := os.Executable()
		if err == nil {
			execDir := filepath.Dir(execPath)
			finalConfigPath = filepath.Join(execDir, "pgtest-transient.yaml")
		} else {
			workDir, _ := os.Getwd()
			finalConfigPath = filepath.Join(workDir, "config", "pgtest-transient.yaml")
		}
	}

	// Tenta carregar o arquivo de configuração
	if finalConfigPath != "" {
		data, err := os.ReadFile(finalConfigPath)
		if err == nil {
			if err := yaml.Unmarshal(data, config); err != nil {
				return nil, fmt.Errorf("failed to parse config file %s: %w", finalConfigPath, err)
			}
			// Arquivo carregado e parseado com sucesso
			configFileUsed = finalConfigPath
			testutil.LogIfVerbose("Config file loaded successfully: %s", finalConfigPath)
			testutil.LogIfVerbose("Config values - Proxy: listen_host=%s, listen_port=%d", config.Proxy.ListenHost, config.Proxy.ListenPort)
		} else {
			// Se o arquivo não existir, continua com valores padrão (não é erro)
			// Mas apenas se configPath estava vazio (busca automática)
			if configPath != "" {
				// Se foi especificado explicitamente e não existe, retorna erro
				return nil, fmt.Errorf("config file not found: %s", finalConfigPath)
			}
			// Se não encontrou o arquivo e não foi especificado explicitamente,
			// usa valores padrão e não retorna caminho de arquivo
		}
	}

	loadFromEnv(config)

	// Log dos valores finais após aplicar variáveis de ambiente
	if configFileUsed != "" {
		testutil.LogIfVerbose("Final config values after env override - Proxy: listen_host=%s, listen_port=%d",
			config.Proxy.ListenHost, config.Proxy.ListenPort)
	}

	if err := validateConfig(config); err != nil {
		return nil, err
	}

	return &LoadConfigResult{
		Config:     config,
		ConfigPath: configFileUsed,
	}, nil
}

func loadFromEnv(config *Config) {
	// Mapeamento de variáveis de ambiente para campos de configuração
	envMappings := []struct {
		envVar    string
		setter    func(string)
		converter func(string) (interface{}, error)
	}{
		// Postgres
		{"POSTGRES_HOST", func(v string) { config.Postgres.Host = v }, nil},
		{"POSTGRES_PORT", func(v string) {
			if p, err := strconv.Atoi(v); err == nil {
				config.Postgres.Port = p
			}
		}, nil},
		{"POSTGRES_DB", func(v string) { config.Postgres.Database = v }, nil},
		{"POSTGRES_USER", func(v string) { config.Postgres.User = v }, nil},
		{"POSTGRES_PASSWORD", func(v string) { config.Postgres.Password = v }, nil},
		{"POSTGRES_SESSION_TIMEOUT", func(v string) {
			if d, err := time.ParseDuration(v); err == nil {
				config.Postgres.SessionTimeout = Duration{Duration: d}
			}
		}, nil},
		// Proxy
		{"PGTEST_LISTEN_HOST", func(v string) { config.Proxy.ListenHost = v }, nil},
		{"PGTEST_LISTEN_PORT", func(v string) {
			if p, err := strconv.Atoi(v); err == nil {
				config.Proxy.ListenPort = p
			}
		}, nil},
		{"PGTEST_TIMEOUT", func(v string) {
			if d, err := time.ParseDuration(v); err == nil {
				config.Proxy.Timeout = d
			}
		}, nil},
		{"PGTEST_KEEPALIVE_INTERVAL", func(v string) {
			if d, err := time.ParseDuration(v); err == nil {
				config.Proxy.KeepaliveInterval = Duration{Duration: d}
			}
		}, nil},
		// Logging
		{"PGTEST_LOG_LEVEL", func(v string) { config.Logging.Level = v }, nil},
		{"PGTEST_LOG_FILE", func(v string) { config.Logging.File = v }, nil},
	}

	for _, mapping := range envMappings {
		if value := os.Getenv(mapping.envVar); value != "" {
			mapping.setter(value)
		}
	}
}

func validateConfig(config *Config) error {
	if config.Postgres.Host == "" {
		return fmt.Errorf("POSTGRES_HOST is required")
	}
	if config.Postgres.Port == 0 {
		return fmt.Errorf("POSTGRES_PORT is required")
	}
	if config.Postgres.Database == "" {
		return fmt.Errorf("POSTGRES_DB is required")
	}
	if config.Postgres.User == "" {
		return fmt.Errorf("POSTGRES_USER is required")
	}
	return nil
}
