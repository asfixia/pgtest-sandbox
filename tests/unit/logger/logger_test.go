package logger_test

import (
	"bytes"
	"strings"
	"testing"

	"pgtest-transient/pkg/logger"
)

func TestLogLevel(t *testing.T) {
	tests := []struct {
		name     string
		level    string
		expected logger.LogLevel
	}{
		{"DEBUG", "DEBUG", logger.DEBUG},
		{"INFO", "INFO", logger.INFO},
		{"WARN", "WARN", logger.WARN},
		{"WARNING", "WARNING", logger.WARN},
		{"ERROR", "ERROR", logger.ERROR},
		{"invalid", "INVALID", logger.INFO}, // Padrão
		{"lowercase", "debug", logger.DEBUG},
		{"mixed", "Info", logger.INFO},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := logger.ParseLogLevel(tt.level)
			if result != tt.expected {
				t.Errorf("ParseLogLevel(%q) = %v, want %v", tt.level, result, tt.expected)
			}
		})
	}
}

func TestLoggerLevels(t *testing.T) {
	var buf bytes.Buffer
	l := logger.NewLogger(logger.INFO, "", 0)
	l.SetOutput(&buf)

	// DEBUG não deve ser exibido (nível INFO)
	l.Debug("debug message")
	if buf.String() != "" {
		t.Errorf("DEBUG message should not be logged at INFO level")
	}

	// INFO deve ser exibido
	l.Info("info message")
	if !strings.Contains(buf.String(), "INFO") || !strings.Contains(buf.String(), "info message") {
		t.Errorf("INFO message should be logged")
	}

	buf.Reset()

	// Mudando para DEBUG
	l.SetLevel(logger.DEBUG)
	l.Debug("debug message 2")
	if !strings.Contains(buf.String(), "DEBUG") || !strings.Contains(buf.String(), "debug message 2") {
		t.Errorf("DEBUG message should be logged at DEBUG level")
	}
}

func TestLoggerOutput(t *testing.T) {
	var buf1, buf2 bytes.Buffer
	l := logger.NewLogger(logger.DEBUG, "", 0)
	l.SetOutput(&buf1)

	l.Info("message 1")
	if !strings.Contains(buf1.String(), "message 1") {
		t.Errorf("Message should be written to buf1")
	}

	l.SetOutput(&buf2)
	l.Info("message 2")
	if strings.Contains(buf2.String(), "message 1") {
		t.Errorf("buf2 should not contain message 1")
	}
	if !strings.Contains(buf2.String(), "message 2") {
		t.Errorf("buf2 should contain message 2")
	}
}

func TestGlobalLogger(t *testing.T) {
	var buf bytes.Buffer
	l := logger.NewLogger(logger.DEBUG, "", 0)
	l.SetOutput(&buf)
	logger.SetDefaultLogger(l)

	// Testa as funções estáticas globais (singleton)
	logger.Debug("global debug")
	logger.Info("global info")
	logger.Warn("global warn")
	logger.Error("global error")

	output := buf.String()
	if !strings.Contains(output, "global debug") {
		t.Errorf("Global Debug should be logged")
	}
	if !strings.Contains(output, "global info") {
		t.Errorf("Global Info should be logged")
	}
	if !strings.Contains(output, "global warn") {
		t.Errorf("Global Warn should be logged")
	}
	if !strings.Contains(output, "global error") {
		t.Errorf("Global Error should be logged")
	}
}

func TestSingletonBehavior(t *testing.T) {
	// Testa que GetDefaultLogger sempre retorna a mesma instância
	logger1 := logger.GetDefaultLogger()
	logger2 := logger.GetDefaultLogger()
	
	// Deve ser a mesma instância (mesmo ponteiro)
	if logger1 != logger2 {
		t.Errorf("GetDefaultLogger should return the same singleton instance")
	}
}

func TestSetDefaultLogger(t *testing.T) {
	var buf1, buf2 bytes.Buffer
	
	// Cria primeiro logger
	logger1 := logger.NewLogger(logger.DEBUG, "", 0)
	logger1.SetOutput(&buf1)
	logger.SetDefaultLogger(logger1)
	
	logger.Info("message 1")
	if !strings.Contains(buf1.String(), "message 1") {
		t.Errorf("First logger should receive message")
	}
	
	// Substitui o logger padrão
	logger2 := logger.NewLogger(logger.DEBUG, "", 0)
	logger2.SetOutput(&buf2)
	logger.SetDefaultLogger(logger2)
	
	logger.Info("message 2")
	if strings.Contains(buf2.String(), "message 1") {
		t.Errorf("Second logger should not contain first message")
	}
	if !strings.Contains(buf2.String(), "message 2") {
		t.Errorf("Second logger should receive new message")
	}
}

func TestStaticFunctions(t *testing.T) {
	var buf bytes.Buffer
	l := logger.NewLogger(logger.DEBUG, "", 0)
	l.SetOutput(&buf)
	logger.SetDefaultLogger(l)

	// Testa que as funções estáticas funcionam sem precisar de instância
	logger.Debug("static debug")
	logger.Info("static info")
	logger.Warn("static warn")
	logger.Error("static error")

	output := buf.String()
	expectedMessages := []string{"static debug", "static info", "static warn", "static error"}
	for _, msg := range expectedMessages {
		if !strings.Contains(output, msg) {
			t.Errorf("Static function should log: %s", msg)
		}
	}
}
