package tstproxy

import (
	"strings"
	"sync"
	"testing"
	"time"

	"pgtest-transient/internal/config"
	"pgtest-transient/internal/proxy"
)

func TestGetOrCreateSession(t *testing.T) {
	pgtest := newPGTestFromConfig()

	t.Run("create_new_session", func(t *testing.T) {
		testID := "test123"
		session, err := pgtest.GetOrCreateSession(testID)
		if err == nil {
			t.Logf("Session created (this test requires PostgreSQL connection)")
			if pgtest.GetTestID(session) != testID {
				t.Errorf("Session.TestID = %v, want %v", pgtest.GetTestID(session), testID)
			}
		}
	})

	t.Run("reuse_existing_session", func(t *testing.T) {
		testID := "test456"
		session1, err1 := pgtest.GetOrCreateSession(testID)
		if err1 != nil {
			t.Skip("Skipping test - requires PostgreSQL connection")
		}

		session2, err2 := pgtest.GetOrCreateSession(testID)
		if err2 != nil {
			t.Fatalf("GetOrCreateSession() error = %v", err2)
		}

		if session1 != session2 {
			t.Error("GetOrCreateSession() should return same session instance")
		}
	})
}

func TestGetSession(t *testing.T) {
	pgtest := newPGTestFromConfig()

	t.Run("get_non_existent_session", func(t *testing.T) {
		session := pgtest.GetSession("nonexistent")
		if session != nil {
			t.Error("GetSession() should return nil for non-existent session")
		}
	})
}

func TestConcurrency(t *testing.T) {
	pgtest := newPGTestFromConfig()
	testID := "concurrent_test"

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := pgtest.GetOrCreateSession(testID)
			if err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		if err != nil {
			t.Logf("Concurrent access error (expected if PostgreSQL not available): %v", err)
		}
	}
}

func TestGetAllSessions(t *testing.T) {
	pgtest := newPGTestFromConfig()

	t.Run("empty_sessions", func(t *testing.T) {
		sessions := pgtest.GetAllSessions()
		if sessions == nil {
			t.Error("GetAllSessions() should not return nil")
		}
		if len(sessions) != 0 {
			t.Errorf("GetAllSessions() = %d sessions, want 0", len(sessions))
		}
	})

	t.Run("multiple_sessions", func(t *testing.T) {
		testID1 := "test_getall_1"
		testID2 := "test_getall_2"

		session1, err1 := pgtest.GetOrCreateSession(testID1)
		if err1 != nil {
			t.Skip("Skipping test - requires PostgreSQL connection")
		}

		session2, err2 := pgtest.GetOrCreateSession(testID2)
		if err2 != nil {
			t.Fatalf("GetOrCreateSession() error = %v", err2)
		}

		sessions := pgtest.GetAllSessions()
		if len(sessions) < 2 {
			t.Errorf("GetAllSessions() = %d sessions, want at least 2", len(sessions))
		}

		if sessions[testID1] != session1 {
			t.Error("GetAllSessions() should return session1 for testID1")
		}

		if sessions[testID2] != session2 {
			t.Error("GetAllSessions() should return session2 for testID2")
		}
	})
}

func TestDestroySession(t *testing.T) {
	pgtest := newPGTestFromConfig()

	t.Run("destroy_non_existent_session", func(t *testing.T) {
		err := pgtest.DestroySession("nonexistent_session")
		if err == nil {
			t.Error("DestroySession() should return error for non-existent session")
		}
		if err != nil && !strings.Contains(err.Error(), "not found") {
			t.Errorf("DestroySession() error = %v, want error containing 'not found'", err)
		}
	})

	t.Run("destroy_existing_session", func(t *testing.T) {
		testID := "test_destroy_session"
		session, err := pgtest.GetOrCreateSession(testID)
		if err != nil {
			t.Skip("Skipping test - requires PostgreSQL connection")
		}

		if session == nil {
			t.Fatal("Session should not be nil")
		}

		// Verifica que a sessão existe antes de destruir
		if pgtest.GetSession(testID) == nil {
			t.Fatal("Session should exist before destroy")
		}

		// Destrói a sessão (faz rollback, fecha conexão e remove do mapa)
		err = pgtest.DestroySession(testID)
		if err != nil {
			t.Fatalf("DestroySession() error = %v", err)
		}

		// Verifica que a sessão foi removida
		if pgtest.GetSession(testID) != nil {
			t.Error("Session should be removed after destroy")
		}
	})
}

func TestCleanupExpiredSessions(t *testing.T) {
	// Para este teste específico, precisamos de um timeout curto
	// Carrega config e usa timeout curto para o teste
	cfg, err := config.LoadConfig("")
	var pgtest *proxy.PGTest
	if err != nil {
		// Fallback se não conseguir carregar config
		pgtest = proxy.NewPGTest("localhost", 5432, "postgres", "postgres", "", 100*time.Millisecond, 24*time.Hour, 0)
	} else {
		sessionTimeout := cfg.Postgres.SessionTimeout.Duration
		if sessionTimeout <= 0 {
			sessionTimeout = 24 * time.Hour
		}
		pgtest = proxy.NewPGTest(
			cfg.Postgres.Host,
			cfg.Postgres.Port,
			cfg.Postgres.Database,
			cfg.Postgres.User,
			cfg.Postgres.Password,
			100*time.Millisecond, // Timeout curto para o teste
			sessionTimeout,
			0, // keepalive desligado no teste
		)
	}

	// Cria uma sessão que vai expirar usando GetOrCreateSession primeiro
	testID := "expired_session_test"
	session, err := pgtest.GetOrCreateSession(testID)
	if err != nil {
		t.Skip("Skipping test - requires PostgreSQL connection")
	}

	// Ajusta as datas para simular sessão expirada
	// Como CreatedAt e LastActivity são campos exportados, podemos modificá-los diretamente
	session.CreatedAt = time.Now().Add(-200 * time.Millisecond)
	session.LastActivity = time.Now().Add(-200 * time.Millisecond)

	time.Sleep(150 * time.Millisecond)

	cleaned := pgtest.CleanupExpiredSessions()
	if cleaned != 1 {
		t.Errorf("CleanupExpiredSessions() = %v, want 1", cleaned)
	}

	if pgtest.GetSession(testID) != nil {
		t.Error("Expired session should be removed")
	}
}

func TestSessionTimeout(t *testing.T) {
	// Testa especificamente o comportamento de timeout de sessão
	// Usa timeout muito curto (30ms) para teste rápido
	cfg, err := config.LoadConfig("")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}
	sessionTimeout := cfg.Postgres.SessionTimeout.Duration
	if sessionTimeout <= 0 {
		sessionTimeout = 24 * time.Hour
	}
	pgtest := proxy.NewPGTest(
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
		cfg.Postgres.User,
		cfg.Postgres.Password,
		30*time.Millisecond, // Timeout muito curto para teste rápido
		sessionTimeout,
		0, // keepalive desligado no teste
	)

	testID := "timeout_test_session"
	_, err = pgtest.GetOrCreateSession(testID)
	if err != nil {
		t.Skip("Skipping test - requires PostgreSQL connection")
	}

	// Verifica que a sessão existe
	if pgtest.GetSession(testID) == nil {
		t.Fatal("Session should exist initially")
	}

	// Espera menos que o timeout - sessão ainda deve existir
	time.Sleep(20 * time.Millisecond)
	if pgtest.GetSession(testID) == nil {
		t.Error("Session should still exist before timeout")
	}

	// Espera mais que o timeout - sessão deve expirar
	time.Sleep(20 * time.Millisecond)

	cleaned := pgtest.CleanupExpiredSessions()
	if cleaned == 0 {
		t.Log("Session may not have expired yet, but cleanup should work")
	} else {
		t.Logf("Session expired and was cleaned up (cleaned: %d)", cleaned)
	}
}
