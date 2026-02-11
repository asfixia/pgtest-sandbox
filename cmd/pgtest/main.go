package main

import (
	"fmt"
	"log"
	"os"

	"pgtest-sandbox/internal/config"
	"pgtest-sandbox/internal/proxy"
	"pgtest-sandbox/internal/tray"
	"pgtest-sandbox/pkg/logger"
)

func main() {
	// Aceita o caminho do arquivo de configuração como argumento
	// Se não fornecido, usa string vazia (busca automática)
	configPath := ""
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	configResult, err := config.LoadConfigWithPath(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	config.Init()
	config.SetOnce(configResult.Config, configResult.ConfigPath)

	// Inicializa o logger a partir da configuração
	if err := logger.InitFromConfig(config.GetCfg()); err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}

	// Imprime o caminho do arquivo de configuração usado, se houver
	if configResult.ConfigPath != "" {
		logger.Info("Using config file: %s", configResult.ConfigPath)
	}

	cfg := config.GetCfg()
	server := proxy.NewServer(
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Proxy.Timeout,
		cfg.Postgres.SessionTimeout.Duration,
		cfg.Proxy.KeepaliveInterval.Duration,
		cfg.Proxy.ListenHost,
		cfg.Proxy.ListenPort,
		true, // GUI on same port at /gui
	)
	if err := server.StartError(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	guiURL := fmt.Sprintf("http://%s:%d/", cfg.Proxy.ListenHost, cfg.Proxy.ListenPort)
	log.Printf("PGTest server started on port %d", cfg.Proxy.ListenPort)
	log.Printf("GUI: %s", guiURL)

	// System tray icon blocks the main goroutine until the user clicks Quit.
	tray.Run(guiURL, func() {
		log.Println("Shutting down server...")
		if err := server.Stop(); err != nil {
			log.Printf("Error stopping server: %v", err)
		}
		log.Println("Server stopped")
	})
}
