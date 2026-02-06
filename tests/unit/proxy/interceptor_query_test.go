package tstproxy

import (
	"strings"
	"testing"

	"pgtest-transient/internal/proxy"
	"pgtest-transient/internal/testutil"
)

// assertSavepointQuery verifica se a query contém SAVEPOINT (case-insensitive) e se contém o nível esperado.
// Usa a função unificada do pacote testutil.
func assertSavepointQuery(t *testing.T, query string, expectedLevel int) {
	t.Helper()
	testutil.AssertSavepointQuery(t, query, expectedLevel)
}

// assertReleaseSavepointQuery verifica se a query contém RELEASE SAVEPOINT (case-insensitive) e se contém o nível esperado.
// Usa a função unificada do pacote testutil.
func assertReleaseSavepointQuery(t *testing.T, query string, expectedLevel int) {
	t.Helper()
	testutil.AssertReleaseSavepointQuery(t, query, expectedLevel)
}

func TestHandleBegin(t *testing.T) {
	pgtest := newPGTestFromConfig()
	session, err := pgtest.GetOrCreateSession("empty123")
	if err != nil {
		t.Fatalf("Failed to create the session for testing BEGIN")
	}

	query, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "BEGIN")
	if err != nil {
		t.Fatalf("InterceptQuery(BEGIN) error = %v", err)
	}

	if session.DB == nil {
		t.Fatalf("Error Session has no DB connection on ID: %s", pgtest.GetTestID(session))
	}

	assertSavepointQuery(t, query, 1)

	if session.DB.GetSavepointLevel() != 1 {
		t.Errorf("SavepointLevel = %v, want 1", session.DB.GetSavepointLevel())
	}

	if session.DB.SavepointLevel != 1 {
		t.Errorf("Savepoints length = %v, want 1", session.DB.SavepointLevel)
	}

	query2, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "BEGIN")
	if err != nil {
		t.Fatalf("InterceptQuery(BEGIN) error = %v", err)
	}

	assertSavepointQuery(t, query2, 2)

	if session.DB.GetSavepointLevel() != 2 {
		t.Errorf("SavepointLevel = %v, want 2", session.DB.GetSavepointLevel())
	}
}

func TestHandleCommit(t *testing.T) {
	pgtest := newPGTestFromConfig()

	session := newTestSessionWithLevel(pgtest, "test123", 2)
	if session == nil {
		t.Fatalf("Failed to create a session with the given levels")
	}
	t.Run("commit_savepoint_when_level_gt_0", func(t *testing.T) {

		query, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "COMMIT")
		if err != nil {
			t.Fatalf("InterceptQuery(COMMIT) error = %v", err)
		}

		assertReleaseSavepointQuery(t, query, 2)

		if session.DB.GetSavepointLevel() != 1 {
			t.Errorf("SavepointLevel = %v, want 1", session.DB.GetSavepointLevel())
		}

		if session.DB.SavepointLevel != 1 {
			t.Errorf("Savepoints length = %v, want 1", session.DB.SavepointLevel)
		}
	})

	t.Run("block_commit_when_level_eq_0", func(t *testing.T) {
		session, err := pgtest.GetOrCreateSession("otherId")
		if err != nil {
			t.Fatalf("Failed to get the new Session")
		}

		query, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "COMMIT")
		if err != nil {
			t.Fatalf("InterceptQuery(COMMIT) error = %v", err)
		}

		if query != DEFAULT_SELECT_ONE {
			t.Errorf("InterceptQuery(COMMIT) = %v, want %s (blocked)", query, DEFAULT_SELECT_ONE)
		}

		if session.DB.GetSavepointLevel() != 0 {
			t.Errorf("SavepointLevel = %v, want 0", session.DB.GetSavepointLevel())
		}
	})
}

func TestHandleRollback(t *testing.T) {
	pgtest := newPGTestFromConfig()

	t.Run("rollback_to_savepoint_when_level_gt_0", func(t *testing.T) {
		session := newTestSessionWithLevel(pgtest, "test123", 2)

		query, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "ROLLBACK")
		if err != nil {
			t.Fatalf("InterceptQuery(ROLLBACK) error = %v", err)
		}

		// Verifica que contém ROLLBACK TO SAVEPOINT e RELEASE SAVEPOINT com o savepoint correto
		queryUpper := strings.ToUpper(query)
		if !strings.Contains(queryUpper, "ROLLBACK") || !strings.Contains(queryUpper, "SAVEPOINT") || !strings.Contains(queryUpper, "RELEASE") {
			t.Errorf("Query should contain ROLLBACK TO SAVEPOINT and RELEASE SAVEPOINT, got: %s", query)
		}
		// Verifica que o nível esperado está contido na query
		if !strings.Contains(query, "2") {
			t.Errorf("Query should contain level 2, got: %s", query)
		}

		if session.DB.GetSavepointLevel() != 1 {
			t.Errorf("SavepointLevel = %v, want 1", session.DB.GetSavepointLevel())
		}

		if session.DB.SavepointLevel != 1 {
			t.Errorf("Savepoints length = %v, want 1", session.DB.SavepointLevel)
		}
	})

	t.Run("block_rollback_when_level_eq_0", func(t *testing.T) {
		session, err := pgtest.GetOrCreateSession("fakeId")
		if err != nil {
			t.Fatalf("Failed to get the new Session")
		}

		query, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "ROLLBACK")
		if err != nil {
			t.Fatalf("InterceptQuery(ROLLBACK) error = %v", err)
		}

		if query != DEFAULT_SELECT_ONE {
			t.Errorf("InterceptQuery(ROLLBACK) = %v, want %s (blocked)", query, DEFAULT_SELECT_ONE)
		}

		if session.DB.GetSavepointLevel() != 0 {
			t.Errorf("SavepointLevel = %v, want 0", session.DB.GetSavepointLevel())
		}
	})
}

func TestHandlePGTestCommandUnit(t *testing.T) {
	pgtest := newPGTestFromConfig()

	t.Run("invalid_command", func(t *testing.T) {
		session := newTestSession(pgtest)
		_, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "pgtest")
		if err == nil {
			t.Error("InterceptQuery(pgtest) should return error for invalid command")
		}
	})

	t.Run("unknown_action", func(t *testing.T) {
		session := newTestSession(pgtest)
		_, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "pgtest unknown")
		if err == nil {
			t.Error("InterceptQuery(pgtest unknown) should return error for unknown action")
		}
	})

	t.Run("begin_missing_test_id", func(t *testing.T) {
		// Cria sessão sem TestID para testar o caso de erro
		session := &proxy.TestSession{}
		_, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "pgtest begin")
		if err == nil {
			t.Error("InterceptQuery(pgtest begin) should return error for missing test_id")
		}
	})

	t.Run("rollback_missing_test_id", func(t *testing.T) {
		// Cria sessão sem TestID para testar o caso de erro
		session := &proxy.TestSession{}
		_, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "pgtest rollback")
		if err == nil {
			t.Error("InterceptQuery(pgtest rollback) should return error for missing test_id")
		}
	})

	t.Run("status_missing_test_id", func(t *testing.T) {
		// Cria sessão sem TestID para testar o caso de erro
		session := &proxy.TestSession{}
		_, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "pgtest status")
		if err == nil {
			t.Error("InterceptQuery(pgtest status) should return error for missing test_id")
		}
	})

	t.Run("pgtest_begin_success", func(t *testing.T) {
		testID := "test_pgtest_begin"
		// Cria sessão com o testID que será usado pelo comando
		session, err := pgtest.GetOrCreateSession(testID)
		if err != nil {
			t.Skip("Skipping test - requires PostgreSQL connection")
		}
		// Usa o testID da sessão (não passa como parâmetro)
		query, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "pgtest begin")
		if err != nil {
			t.Fatalf("InterceptQuery(pgtest begin) error = %v", err)
		}
		if query != DEFAULT_SELECT_ONE {
			t.Errorf("InterceptQuery(pgtest begin) = %v, want %s", query, DEFAULT_SELECT_ONE)
		}
		// Verifica que a sessão foi criada/reutilizada
		createdSession := pgtest.GetSession(testID)
		if createdSession == nil {
			t.Error("Session should exist after pgtest begin")
		}
	})

	t.Run("pgtest_rollback_success", func(t *testing.T) {
		testID := "test_pgtest_rollback"
		// Primeiro cria a sessão
		session, err := pgtest.GetOrCreateSession(testID)
		if err != nil {
			t.Skip("Skipping test - requires PostgreSQL connection")
		}
		// Depois faz rollback usando o testID da sessão (não passa como parâmetro)
		query, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "pgtest rollback")
		if err != nil {
			t.Fatalf("InterceptQuery(pgtest rollback) error = %v", err)
		}
		if query != FULLROLLBACK_SENTINEL {
			t.Errorf("InterceptQuery(pgtest rollback) = %v, want %s", query, FULLROLLBACK_SENTINEL)
		}
		// Verifica que a sessão ainda existe (rollback não remove a sessão, apenas reconecta)
		// A sessão é mantida, apenas a conexão é recriada
		removedSession := pgtest.GetSession(testID)
		if removedSession == nil {
			t.Error("Session should still exist after pgtest rollback (connection is recreated, not session removed)")
		}
	})

	t.Run("pgtest_status_success", func(t *testing.T) {
		testID := "test_pgtest_status"
		// Primeiro cria a sessão
		session, err := pgtest.GetOrCreateSession(testID)
		if err != nil {
			t.Skip("Skipping test - requires PostgreSQL connection")
		}
		// Depois verifica status usando o testID da sessão (não passa como parâmetro)
		query, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "pgtest status")
		if err != nil {
			t.Fatalf("InterceptQuery(pgtest status) error = %v", err)
		}
		if !contains(query, "SELECT") || !contains(query, "test_id") {
			t.Errorf("InterceptQuery(pgtest status) = %v, want SELECT query with test_id", query)
		}
	})

	t.Run("pgtest_list_success", func(t *testing.T) {
		testID1 := "test_list_1"
		testID2 := "test_list_2"
		// Cria algumas sessões
		_, err1 := pgtest.GetOrCreateSession(testID1)
		_, err2 := pgtest.GetOrCreateSession(testID2)
		if err1 != nil || err2 != nil {
			t.Skip("Skipping test - requires PostgreSQL connection")
		}
		// Depois lista (não precisa de testID, lista todas)
		session := newTestSession(pgtest)
		query, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "pgtest list")
		if err != nil {
			t.Fatalf("InterceptQuery(pgtest list) error = %v", err)
		}
		if !contains(query, "SELECT") || !contains(query, "test_id") {
			t.Errorf("InterceptQuery(pgtest list) = %v, want SELECT query with test_id", query)
		}
	})

	t.Run("pgtest_cleanup_success", func(t *testing.T) {
		session := newTestSession(pgtest)
		query, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "pgtest cleanup")
		if err != nil {
			t.Fatalf("InterceptQuery(pgtest cleanup) error = %v", err)
		}
		if !contains(query, "SELECT") || !contains(query, "cleaned") {
			t.Errorf("InterceptQuery(pgtest cleanup) = %v, want SELECT query with cleaned", query)
		}
	})
}

func TestInterceptQuery_NormalQueries(t *testing.T) {
	pgtest := newPGTestFromConfig()
	session := newTestSession(pgtest)

	t.Run("select_query_passes_through", func(t *testing.T) {
		originalQuery := "SELECT * FROM users WHERE id = 1"
		query, err := pgtest.InterceptQuery(pgtest.GetTestID(session), originalQuery)
		if err != nil {
			t.Fatalf("InterceptQuery(SELECT) error = %v", err)
		}
		if query != originalQuery {
			t.Errorf("InterceptQuery(SELECT) = %v, want %v", query, originalQuery)
		}
	})

	t.Run("insert_query_passes_through", func(t *testing.T) {
		originalQuery := "INSERT INTO users (name) VALUES ('test')"
		query, err := pgtest.InterceptQuery(pgtest.GetTestID(session), originalQuery)
		if err != nil {
			t.Fatalf("InterceptQuery(INSERT) error = %v", err)
		}
		if query != originalQuery {
			t.Errorf("InterceptQuery(INSERT) = %v, want %v", query, originalQuery)
		}
	})

	t.Run("update_query_passes_through", func(t *testing.T) {
		originalQuery := "UPDATE users SET name = 'updated' WHERE id = 1"
		query, err := pgtest.InterceptQuery(pgtest.GetTestID(session), originalQuery)
		if err != nil {
			t.Fatalf("InterceptQuery(UPDATE) error = %v", err)
		}
		if query != originalQuery {
			t.Errorf("InterceptQuery(UPDATE) = %v, want %v", query, originalQuery)
		}
	})

	t.Run("delete_query_passes_through", func(t *testing.T) {
		originalQuery := "DELETE FROM users WHERE id = 1"
		query, err := pgtest.InterceptQuery(pgtest.GetTestID(session), originalQuery)
		if err != nil {
			t.Fatalf("InterceptQuery(DELETE) error = %v", err)
		}
		if query != originalQuery {
			t.Errorf("InterceptQuery(DELETE) = %v, want %v", query, originalQuery)
		}
	})

	t.Run("query_with_whitespace_is_trimmed", func(t *testing.T) {
		originalQuery := "   SELECT * FROM users   "
		query, err := pgtest.InterceptQuery(pgtest.GetTestID(session), originalQuery)
		if err != nil {
			t.Fatalf("InterceptQuery(SELECT with whitespace) error = %v", err)
		}
		// Query deve passar através sem modificação (trimming é interno)
		if query != strings.TrimSpace(originalQuery) && query != originalQuery {
			t.Errorf("InterceptQuery(SELECT with whitespace) = %v, want trimmed or original", query)
		}
	})
}

// TestInterceptQuery_Routing is a table-driven test for InterceptQuery dispatch and ROLLBACK vs ROLLBACK TO SAVEPOINT.
// It documents when a query is passed through unchanged vs intercepted, and helps find routing bugs.
func TestInterceptQuery_Routing(t *testing.T) {
	pgtest := newPGTestFromConfig()

	tests := []struct {
		name             string
		query            string
		needSession      bool
		savepointLevel   int // used only if needSession
		expectPassThrough bool // true: output must equal query, err nil
		expectErr        bool
		outputMustContain string // if set and not pass-through, output must contain this (e.g. "SAVEPOINT", "RELEASE")
	}{
		// ---- Pass-through: no session, query returned unchanged ----
		{name: "select_passes_through", query: "SELECT 1", needSession: false, expectPassThrough: true},
		{name: "insert_passes_through", query: "INSERT INTO t VALUES (1)", needSession: false, expectPassThrough: true},
		{name: "rollback_to_savepoint_passes_through", query: "ROLLBACK TO SAVEPOINT sp1", needSession: false, expectPassThrough: true},
		{name: "rollback_to_savepoint_lowercase_passes_through", query: "rollback to savepoint sp1", needSession: false, expectPassThrough: true},
		{name: "rollback_to_savepoint_no_name_passes_through", query: "ROLLBACK TO SAVEPOINT", needSession: false, expectPassThrough: true},
		{name: "rollback_to_savepoint_whitespace_passes_through", query: "  ROLLBACK TO SAVEPOINT pgtest_v_1  ", needSession: false, expectPassThrough: true},
		{name: "select_rollback_in_string_passes_through", query: "SELECT 'ROLLBACK' FROM t", needSession: false, expectPassThrough: true},
		{name: "empty_string_passes_through", query: "", needSession: false, expectPassThrough: true},
		{name: "whitespace_only_passes_through", query: "   \t\n  ", needSession: false, expectPassThrough: true},
		// ---- Intercepted: need session ----
		{name: "plain_rollback_intercepted", query: "ROLLBACK", needSession: true, savepointLevel: 1, expectPassThrough: false, outputMustContain: "ROLLBACK TO SAVEPOINT"},
		{name: "rollback_with_semicolon_intercepted", query: "ROLLBACK;", needSession: true, savepointLevel: 1, expectPassThrough: false, outputMustContain: "SAVEPOINT"},
		{name: "rollback_whitespace_intercepted", query: "  ROLLBACK  ", needSession: true, savepointLevel: 1, expectPassThrough: false, outputMustContain: "SAVEPOINT"},
		{name: "begin_intercepted", query: "BEGIN", needSession: true, savepointLevel: 0, expectPassThrough: false, outputMustContain: "SAVEPOINT"},
		{name: "begin_transaction_intercepted_as_begin", query: "BEGIN TRANSACTION", needSession: true, savepointLevel: 0, expectPassThrough: false, outputMustContain: "SAVEPOINT"},
		{name: "commit_intercepted", query: "COMMIT", needSession: true, savepointLevel: 1, expectPassThrough: false, outputMustContain: "RELEASE SAVEPOINT"},
		{name: "commit_work_intercepted_as_commit", query: "COMMIT WORK", needSession: true, savepointLevel: 1, expectPassThrough: false, outputMustContain: "RELEASE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var testID string
			if tt.needSession {
				// Use unique testID per case so session state is isolated (no cross-test pollution)
				uniqueID := "routing_" + strings.ReplaceAll(tt.name, " ", "_")
				s := newTestSessionWithLevel(pgtest, uniqueID, tt.savepointLevel)
				if s == nil {
					t.Fatal("failed to create session for test")
				}
				testID = pgtest.GetTestID(s)
				if testID == "" {
					t.Fatal("GetTestID returned empty")
				}
			} else {
				testID = "no_session_needed"
			}

			got, err := pgtest.InterceptQuery(testID, tt.query)

			if tt.expectErr {
				if err == nil {
					t.Errorf("InterceptQuery() err = nil, want error")
				}
				return
			}
			if err != nil {
				t.Errorf("InterceptQuery() err = %v", err)
				return
			}

			if tt.expectPassThrough {
				if got != tt.query {
					t.Errorf("InterceptQuery() = %q, want pass-through %q", got, tt.query)
				}
				return
			}

			// Intercepted: output should differ from input and optionally contain expected substring
			if got == tt.query {
				t.Errorf("InterceptQuery() passed through %q unchanged, expected interception", tt.query)
			}
			if tt.outputMustContain != "" && !strings.Contains(got, tt.outputMustContain) {
				t.Errorf("InterceptQuery() = %q, want output containing %q", got, tt.outputMustContain)
			}
		})
	}
}

func TestInterceptQuery_MultipleSavepoints(t *testing.T) {
	pgtest := newPGTestFromConfig()
	session := newTestSession(pgtest)

	// Cria múltiplos savepoints aninhados
	for i := 1; i <= 5; i++ {
		query, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "BEGIN")
		if err != nil {
			t.Fatalf("InterceptQuery(BEGIN #%d) error = %v", i, err)
		}
		assertSavepointQuery(t, query, i)
		if session.DB.GetSavepointLevel() != i {
			t.Errorf("SavepointLevel after BEGIN #%d = %v, want %v", i, session.DB.GetSavepointLevel(), i)
		}
		if session.DB.SavepointLevel != i {
			t.Errorf("Savepoints length after BEGIN #%d = %v, want %v", i, session.DB.SavepointLevel, i)
		}
	}

	// Faz commit de alguns savepoints
	for i := 5; i > 2; i-- {
		query, err := pgtest.InterceptQuery(pgtest.GetTestID(session), "COMMIT")
		if err != nil {
			t.Fatalf("InterceptQuery(COMMIT at level %d) error = %v", i, err)
		}
		assertReleaseSavepointQuery(t, query, i)
		if session.DB.GetSavepointLevel() != i-1 {
			t.Errorf("SavepointLevel after COMMIT = %v, want %v", session.DB.GetSavepointLevel(), i-1)
		}
	}

	// Verifica estado final
	if session.DB.GetSavepointLevel() != 2 {
		t.Errorf("Final SavepointLevel = %v, want 2", session.DB.GetSavepointLevel())
	}
	if session.DB.SavepointLevel != 2 {
		t.Errorf("Final Savepoints length = %v, want 2", session.DB.SavepointLevel)
	}
}

func TestExecuteWithLock(t *testing.T) {
	pgtest := newPGTestFromConfig()
	testID := "test_execute_lock"
	session, err := pgtest.GetOrCreateSession(testID)
	if err != nil {
		t.Skip("Skipping test - requires PostgreSQL connection")
	}

	t.Run("execute_simple_query_with_lock", func(t *testing.T) {
		query := "SELECT 1"
		err := pgtest.ExecuteWithLock(session, query)
		if err != nil {
			t.Fatalf("ExecuteWithLock(SELECT 1) error = %v", err)
		}
	})

	t.Run("execute_insert_with_lock", func(t *testing.T) {
		query := "INSERT INTO test_table (id) VALUES (1)"
		err := pgtest.ExecuteWithLock(session, query)
		// Pode falhar se a tabela não existir, mas não deve falhar por lock
		if err != nil && !contains(err.Error(), "does not exist") && !contains(err.Error(), "relation") {
			t.Logf("ExecuteWithLock(INSERT) error = %v (may be expected if table doesn't exist)", err)
		}
	})

	t.Run("execute_update_with_lock", func(t *testing.T) {
		query := "UPDATE test_table SET id = 2 WHERE id = 1"
		err := pgtest.ExecuteWithLock(session, query)
		// Pode falhar se a tabela não existir, mas não deve falhar por lock
		if err != nil && !contains(err.Error(), "does not exist") && !contains(err.Error(), "relation") {
			t.Logf("ExecuteWithLock(UPDATE) error = %v (may be expected if table doesn't exist)", err)
		}
	})
}
