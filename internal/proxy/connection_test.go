package proxy

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestRewriteDEALLOCATEForBackend(t *testing.T) {
	p := &proxyConnection{
		preparedStatements:       make(map[string]string),
		multiStatementStatements: make(map[string]struct{}),
	}
	// Ensure other maps exist so copyPreparedStatementNames doesn't panic
	p.statementDescs = make(map[string]*pgconn.StatementDescription)
	p.portalToStatement = make(map[string]string)

	// DEALLOCATE <name>: should return one command with backend-prefixed name
	rewritten, isDEALLOCATE := p.rewriteDEALLOCATEForBackend("DEALLOCATE pdo_stmt_00000003")
	if !isDEALLOCATE || len(rewritten) != 1 {
		t.Fatalf("DEALLOCATE name: isDEALLOCATE=%v len=%d, want true, 1", isDEALLOCATE, len(rewritten))
	}
	if !strings.HasPrefix(rewritten[0], "DEALLOCATE c") || !strings.HasSuffix(rewritten[0], "pdo_stmt_00000003") {
		t.Errorf("DEALLOCATE name: got %q, want DEALLOCATE c<id>_pdo_stmt_00000003", rewritten[0])
	}

	// Not DEALLOCATE: (nil, false)
	rewritten, isDEALLOCATE = p.rewriteDEALLOCATEForBackend("SELECT 1")
	if isDEALLOCATE || rewritten != nil {
		t.Errorf("SELECT 1: got (%v, %v), want (nil, false)", rewritten, isDEALLOCATE)
	}

	// DEALLOCATE ALL with no statements: ["SELECT 1"]
	rewritten, isDEALLOCATE = p.rewriteDEALLOCATEForBackend("DEALLOCATE ALL")
	if !isDEALLOCATE || len(rewritten) != 1 || rewritten[0] != "SELECT 1" {
		t.Errorf("DEALLOCATE ALL (no stmts): got %v, %v, want [SELECT 1], true", rewritten, isDEALLOCATE)
	}

	// DEALLOCATE ALL with one prepared statement: one DEALLOCATE with backend name
	p.SetPreparedStatement("s1", "SELECT 1")
	rewritten, isDEALLOCATE = p.rewriteDEALLOCATEForBackend("DEALLOCATE ALL")
	if !isDEALLOCATE || len(rewritten) != 1 {
		t.Fatalf("DEALLOCATE ALL (one stmt): isDEALLOCATE=%v len=%d", isDEALLOCATE, len(rewritten))
	}
	if !strings.HasPrefix(rewritten[0], "DEALLOCATE c") || !strings.Contains(rewritten[0], "s1") {
		t.Errorf("DEALLOCATE ALL (one stmt): got %q", rewritten[0])
	}

	// DEALLOCATE followed by multiple spaces and ALL: rewrite works (rest is trimmed to "ALL")
	p2 := &proxyConnection{
		preparedStatements:       make(map[string]string),
		multiStatementStatements: make(map[string]struct{}),
		statementDescs:           make(map[string]*pgconn.StatementDescription),
		portalToStatement:        make(map[string]string),
	}
	p2.SetPreparedStatement("stmt_x", "SELECT 2")
	rewritten, isDEALLOCATE = p2.rewriteDEALLOCATEForBackend("DEALLOCATE    \r\n/**/ ALL")
	if !isDEALLOCATE {
		t.Fatalf("DEALLOCATE     ALL: isDEALLOCATE=%v, want true", isDEALLOCATE)
	}
	if len(rewritten) != 1 {
		t.Fatalf("DEALLOCATE     ALL: len(rewritten)=%d, want 1", len(rewritten))
	}
	if !strings.HasPrefix(rewritten[0], "DEALLOCATE c") || !strings.Contains(rewritten[0], "stmt_x") {
		t.Errorf("DEALLOCATE     ALL: got %q, want DEALLOCATE c<id>_stmt_x", rewritten[0])
	}
}

// TestDEALLOCATEOnlyAffectsOwnConnection verifies that one connection cannot deallocate
// a statement prepared on another connection. Each connection's DEALLOCATE is rewritten
// to its own backend prefix, so the second connection's DEALLOCATE targets a different
// name than the first's prepared statement and therefore does not affect it.
func TestDEALLOCATEOnlyAffectsOwnConnection(t *testing.T) {
	makeConn := func() *proxyConnection {
		return &proxyConnection{
			preparedStatements:       make(map[string]string),
			multiStatementStatements: make(map[string]struct{}),
			statementDescs:           make(map[string]*pgconn.StatementDescription),
			portalToStatement:        make(map[string]string),
		}
	}

	connA := makeConn()
	connB := makeConn()

	// Connection A prepares a statement (e.g. via extended protocol Parse).
	connA.SetPreparedStatement("pdo_stmt_00000001", "SELECT 1")

	// Connection B tries to DEALLOCATE the same client-side name.
	rewritten, isDEALLOCATE := connB.rewriteDEALLOCATEForBackend("DEALLOCATE pdo_stmt_00000001")
	if !isDEALLOCATE || len(rewritten) != 1 {
		t.Fatalf("connB DEALLOCATE: isDEALLOCATE=%v len=%d, want true, 1", isDEALLOCATE, len(rewritten))
	}

	// B's rewrite must use B's backend name, not A's.
	backendNameA := connA.backendStmtName("pdo_stmt_00000001")
	backendNameB := connB.backendStmtName("pdo_stmt_00000001")

	if backendNameA == backendNameB {
		t.Fatalf("different connections must have different backend names: both %q", backendNameA)
	}

	expectedB := "DEALLOCATE " + backendNameB
	if rewritten[0] != expectedB {
		t.Errorf("connB DEALLOCATE rewritten to %q, want %q", rewritten[0], expectedB)
	}

	// So on the backend: A's statement is backendNameA, B's DEALLOCATE targets backendNameB.
	// backendNameB does not exist (B never prepared it), so the backend would return
	// "prepared statement does not exist". A's statement remains allocated.
}

// TestDEALLOCATEALLWithSameNameOnTwoConnections: two connections prepare the same
// client-side statement name. One runs DEALLOCATE ALL (it only deallocates its own;
// it "fails" to deallocate the other's prepared statement). The other runs
// DEALLOCATE <name> and succeeds because the rewrite uses its own backend name.
func TestDEALLOCATEALLWithSameNameOnTwoConnections(t *testing.T) {
	makeConn := func() *proxyConnection {
		return &proxyConnection{
			preparedStatements:       make(map[string]string),
			multiStatementStatements: make(map[string]struct{}),
			statementDescs:           make(map[string]*pgconn.StatementDescription),
			portalToStatement:        make(map[string]string),
		}
	}

	connA := makeConn()
	connB := makeConn()

	const sameName = "pdo_stmt_00000001"
	connA.SetPreparedStatement(sameName, "SELECT 1")
	connB.SetPreparedStatement(sameName, "SELECT 1")

	backendNameA := connA.backendStmtName(sameName)
	backendNameB := connB.backendStmtName(sameName)
	if backendNameA == backendNameB {
		t.Fatalf("same client name must get different backend names: both %q", backendNameA)
	}

	// Connection A runs DEALLOCATE ALL. It must only deallocate A's statement, not B's.
	// So the rewrite must contain only A's backend name; B's statement remains on the backend.
	rewrittenA, isDEALLOCATE := connA.rewriteDEALLOCATEForBackend("DEALLOCATE ALL")
	if !isDEALLOCATE || len(rewrittenA) != 1 {
		t.Fatalf("connA DEALLOCATE ALL: isDEALLOCATE=%v len=%d, want true, 1", isDEALLOCATE, len(rewrittenA))
	}
	expectedA := "DEALLOCATE " + backendNameA
	if rewrittenA[0] != expectedA {
		t.Errorf("connA DEALLOCATE ALL: got %q, want %q", rewrittenA[0], expectedA)
	}
	// Must not contain B's backend name (so A "fails" to deallocate B's statement).
	if strings.Contains(rewrittenA[0], backendNameB) {
		t.Errorf("connA DEALLOCATE ALL must not contain connB backend name %q", backendNameB)
	}

	// Connection B runs DEALLOCATE <same name>. It must succeed by deallocating only B's.
	rewrittenB, isDEALLOCATE := connB.rewriteDEALLOCATEForBackend("DEALLOCATE " + sameName)
	if !isDEALLOCATE || len(rewrittenB) != 1 {
		t.Fatalf("connB DEALLOCATE %q: isDEALLOCATE=%v len=%d, want true, 1", sameName, isDEALLOCATE, len(rewrittenB))
	}
	expectedB := "DEALLOCATE " + backendNameB
	if rewrittenB[0] != expectedB {
		t.Errorf("connB DEALLOCATE %q: got %q, want %q", sameName, rewrittenB[0], expectedB)
	}
}
