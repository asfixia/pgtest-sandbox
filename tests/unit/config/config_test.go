package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"pgtest-transient/internal/config"
)

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

func TestLoadConfig_FromFile(t *testing.T) {
	// Testa carregamento de arquivo específico
	// Resolve caminho relativo à raiz do projeto
	projectRoot := findProjectRoot()
	if projectRoot == "" {
		t.Skip("Could not find project root (go.mod not found)")
	}

	configPath := filepath.Join(projectRoot, "config", "pgtest-transient.yaml")

	// Verifica se arquivo existe antes de testar
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Skipf("Config file %s does not exist, skipping test", configPath)
	}

	cfg, err := config.LoadConfig(configPath)

	if err != nil {
		t.Fatalf("config.LoadConfig(%q) error = %v, want nil", configPath, err)
	}

	if cfg == nil {
		t.Fatal("config.LoadConfig() returned nil config")
	}

	// Verifica se os valores do arquivo foram carregados
	if cfg.Postgres.Host == "" {
		t.Error("Postgres.Host should not be empty")
	}

	// Se o arquivo tem valores específicos, verifica
	if cfg.Postgres.Host != "localhost" && cfg.Postgres.Host != "pgdev" {
		t.Logf("Postgres.Host = %s (loaded from file)", cfg.Postgres.Host)
	}
}

func TestLoadConfig_AutoSearch(t *testing.T) {
	// Testa busca automática (sem especificar caminho)
	cfg, err := config.LoadConfig("")

	if err != nil {
		t.Fatalf("config.LoadConfig(\"\") error = %v, want nil", err)
	}

	if cfg == nil {
		t.Fatal("config.LoadConfig() returned nil config")
	}

	// Deve ter valores padrão ou do arquivo encontrado
	if cfg.Postgres.Host == "" {
		t.Error("Postgres.Host should not be empty")
	}
}

func TestLoadConfigInternally(t *testing.T) {
	// Testa valores padrão quando não há arquivo de config
	// Primeiro testa que arquivo inexistente retorna erro quando especificado explicitamente
	cfg, err := config.LoadConfig("nonexistent_file_that_does_not_exist_12345.yaml")

	// Deve retornar erro se arquivo não existe e foi especificado explicitamente
	if err == nil {
		t.Error("config.LoadConfig() should return error when file doesn't exist and path is specified")
	}

	// Testa com busca automática
	// NOTA: LoadConfig("") usa os.Executable() para buscar o arquivo de config,
	// então se houver um arquivo de config no diretório do executável (projeto),
	// ele será carregado. Este teste verifica o comportamento real da busca automática.
	cfg, err = config.LoadConfig("")
	if err != nil {
		t.Fatalf("config.LoadConfig(\"\") error = %v, want nil", err)
	}

	// Se um arquivo de config foi encontrado, os valores podem ser diferentes dos padrões
	// Verificamos apenas que valores essenciais não estão vazios
	if cfg.Postgres.Host == "" {
		t.Error("Postgres.Host should not be empty")
	}
	if cfg.Postgres.Port == 0 {
		t.Error("Postgres.Port should not be zero")
	}
	if cfg.Postgres.Database == "" {
		t.Error("Postgres.Database should not be empty")
	}
	if cfg.Postgres.User == "" {
		t.Error("Postgres.User should not be empty")
	}
	if cfg.Proxy.ListenPort == 0 {
		t.Error("Proxy.ListenPort should not be zero")
	}
	if cfg.Proxy.Timeout == 0 {
		t.Error("Proxy.Timeout should not be zero")
	}
	if cfg.Test.Schema == "" {
		t.Error("Test.Schema should not be empty")
	}
	if cfg.Test.ContextTimeout.Duration == 0 {
		t.Error("Test.ContextTimeout should not be zero")
	}
	if cfg.Test.QueryTimeout.Duration == 0 {
		t.Error("Test.QueryTimeout should not be zero")
	}
	if cfg.Test.PingTimeout.Duration == 0 {
		t.Error("Test.PingTimeout should not be zero")
	}

	// Log informativo sobre se valores padrão ou do arquivo foram usados
	t.Logf("Config loaded - Postgres: host=%s, port=%d, database=%s",
		cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database)
	t.Logf("Config loaded - Proxy: listen_port=%d, timeout=%v",
		cfg.Proxy.ListenPort, cfg.Proxy.Timeout)
}

func TestLoadConfig_FromEnvVar(t *testing.T) {
	// Testa se variável de ambiente PGTEST_CONFIG funciona
	originalEnv := os.Getenv("PGTEST_CONFIG")
	defer func() {
		if originalEnv != "" {
			os.Setenv("PGTEST_CONFIG", originalEnv)
		} else {
			os.Unsetenv("PGTEST_CONFIG")
		}
	}()

	// Define variável de ambiente apontando para o arquivo de config
	configPath := "config/pgtest-transient.yaml"
	os.Setenv("PGTEST_CONFIG", configPath)

	cfg, err := config.LoadConfig("")
	if err != nil {
		t.Fatalf("config.LoadConfig() with PGTEST_CONFIG error = %v", err)
	}

	if cfg == nil {
		t.Fatal("config.LoadConfig() returned nil config")
	}
}

func TestLoadConfig_EnvOverrides(t *testing.T) {
	// Testa se variáveis de ambiente sobrescrevem valores do arquivo
	originalHost := os.Getenv("POSTGRES_HOST")
	originalPort := os.Getenv("POSTGRES_PORT")
	defer func() {
		if originalHost != "" {
			os.Setenv("POSTGRES_HOST", originalHost)
		} else {
			os.Unsetenv("POSTGRES_HOST")
		}
		if originalPort != "" {
			os.Setenv("POSTGRES_PORT", originalPort)
		} else {
			os.Unsetenv("POSTGRES_PORT")
		}
	}()

	// Resolve caminho do arquivo de config
	projectRoot := findProjectRoot()
	if projectRoot == "" {
		t.Skip("Could not find project root (go.mod not found)")
	}

	configPath := filepath.Join(projectRoot, "config", "pgtest-transient.yaml")

	// Se arquivo não existe, cria um temporário para teste
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		// Usa busca automática (vai usar valores padrão)
		configPath = ""
	}

	// Define variáveis de ambiente
	os.Setenv("POSTGRES_HOST", "env_host")
	os.Setenv("POSTGRES_PORT", "9999")

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Variáveis de ambiente devem sobrescrever valores do arquivo
	if cfg.Postgres.Host != "env_host" {
		t.Errorf("Postgres.Host = %v, want env_host (from env var)", cfg.Postgres.Host)
	}
	if cfg.Postgres.Port != 9999 {
		t.Errorf("Postgres.Port = %v, want 9999 (from env var)", cfg.Postgres.Port)
	}

	// Limpa variáveis de ambiente
	os.Unsetenv("POSTGRES_HOST")
	os.Unsetenv("POSTGRES_PORT")
}

func TestLoadConfig_ValidYAML(t *testing.T) {
	// Testa se arquivo YAML válido é parseado corretamente
	projectRoot := findProjectRoot()
	if projectRoot == "" {
		t.Skip("Could not find project root (go.mod not found)")
	}

	configPath := filepath.Join(projectRoot, "config", "pgtest.yaml")

	// Verifica se arquivo existe
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Skipf("Config file %s does not exist, skipping test", configPath)
	}

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Verifica estrutura básica
	if cfg.Postgres.Host == "" {
		t.Error("Postgres.Host should not be empty")
	}
	if cfg.Proxy.ListenPort == 0 {
		t.Error("Proxy.ListenPort should not be zero")
	}
	if cfg.Test.Schema == "" {
		t.Error("Test.Schema should not be empty")
	}

	// Verifica se valores foram carregados do arquivo (não são apenas padrões)
	if cfg.Postgres.Host == "localhost" && cfg.Postgres.Port == 5432 {
		t.Log("Using default values (file may have same values as defaults)")
	}
}

func TestLoadConfig_InvalidYAML(t *testing.T) {
	// Cria arquivo YAML inválido temporário
	tempDir := t.TempDir()
	invalidYAML := filepath.Join(tempDir, "invalid.yaml")

	err := os.WriteFile(invalidYAML, []byte("invalid: yaml: content: [unclosed"), 0644)
	if err != nil {
		t.Fatalf("Failed to create invalid YAML file: %v", err)
	}

	cfg, err := config.LoadConfig(invalidYAML)

	if err == nil {
		t.Error("config.LoadConfig() should return error for invalid YAML")
	}
	if cfg != nil {
		t.Error("config.LoadConfig() should return nil config for invalid YAML")
	}
}

func TestLoadConfig_FileNotFound_ExplicitPath(t *testing.T) {
	// Testa erro quando arquivo não existe e caminho foi especificado explicitamente
	cfg, err := config.LoadConfig("nonexistent_file_12345.yaml")

	if err == nil {
		t.Error("config.LoadConfig() should return error when file doesn't exist and path is explicit")
	}
	if cfg != nil {
		t.Error("config.LoadConfig() should return nil config when file doesn't exist")
	}
}

func TestLoadConfig_ProjectRootDetection(t *testing.T) {
	// Testa se consegue encontrar arquivo relativo à raiz do projeto
	projectRoot := findProjectRoot()
	if projectRoot == "" {
		t.Skip("Could not find project root (go.mod not found)")
	}

	// Tenta carregar config/pgtest-transient.yaml usando caminho relativo
	// LoadConfig deve resolver relativo ao diretório de trabalho atual
	configPath := filepath.Join(projectRoot, "config", "pgtest-transient.yaml")

	// Verifica se arquivo existe
	if _, statErr := os.Stat(configPath); statErr == nil {
		// Usa caminho relativo (deve funcionar se executado da raiz)
		relPath := "config/pgtest-transient.yaml"
		cfg, err := config.LoadConfig(relPath)

		// Se executado da raiz, deve funcionar
		// Se executado de subdiretório, pode falhar (comportamento esperado)
		if err == nil && cfg != nil {
			t.Logf("Successfully loaded config from relative path: %s", relPath)
		} else {
			t.Logf("Relative path failed (expected if not run from root), trying absolute: %v", err)
			// Tenta com caminho absoluto
			cfg, err = config.LoadConfig(configPath)
			if err != nil {
				t.Fatalf("config.LoadConfig() with absolute path error = %v", err)
			}
			if cfg == nil {
				t.Fatal("config.LoadConfig() returned nil config")
			}
		}
	} else {
		t.Skipf("Config file %s does not exist, skipping test", configPath)
	}
}

func TestLoadConfigByCommandLine(t *testing.T) {
	// Cria um diretório temporário sem arquivo de config
	tempDir := t.TempDir()

	// Muda para o diretório temporário
	oldDir, _ := os.Getwd()
	defer os.Chdir(oldDir)
	os.Chdir(tempDir)

	cfg, err := config.LoadConfig("nonexistent.yaml")

	// Deve retornar erro se arquivo não existe e foi especificado explicitamente
	if err == nil {
		t.Error("config.LoadConfig() should return error when file doesn't exist and path is specified")
	}

	// Testa com busca automática (deve usar valores padrão)
	cfg, err = config.LoadConfig("")
	if err != nil {
		t.Fatalf("config.LoadConfig(\"\") error = %v, want nil", err)
	}

	// Verifica apenas que os valores padrão foram aplicados (não são vazios/nulos)
	if cfg == nil {
		t.Fatal("config.LoadConfig() returned nil config")
	}
	if cfg.Postgres.Host == "" {
		t.Error("Postgres.Host should not be empty")
	}
	if cfg.Postgres.Port == 0 {
		t.Error("Postgres.Port should not be zero")
	}
	if cfg.Postgres.Database == "" {
		t.Error("Postgres.Database should not be empty")
	}
	if cfg.Postgres.User == "" {
		t.Error("Postgres.User should not be empty")
	}
	if cfg.Proxy.ListenPort == 0 {
		t.Error("Proxy.ListenPort should not be zero")
	}
	if cfg.Proxy.Timeout == 0 {
		t.Error("Proxy.Timeout should not be zero")
	}
	if cfg.Test.Schema == "" {
		t.Error("Test.Schema should not be empty")
	}
}
