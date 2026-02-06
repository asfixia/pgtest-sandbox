package logger

import (
	"io"
	"log"
	"os"

	"pgtest-transient/internal/config"
)

// InitFromConfig inicializa o logger padrão a partir da configuração
// Se config for nil, usa valores padrão (INFO level, stderr)
func InitFromConfig(cfg *config.Config) error {
	var level LogLevel = INFO
	var output io.Writer = os.Stderr

	if cfg != nil && cfg.Logging.Level != "" {
		level = ParseLogLevel(cfg.Logging.Level)
	}

	if cfg != nil && cfg.Logging.File != "" {
		file, err := os.OpenFile(cfg.Logging.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return err
		}
		output = file
	}

	logger := NewLogger(level, "", log.LstdFlags)
	logger.SetOutput(output)
	SetDefaultLogger(logger)

	return nil
}
