package tstproxy

import (
	"context"
	"fmt"
	"testing"
	"time"

	"pgtest-sandbox/internal/testutil"

	_ "github.com/jackc/pgx/v5/stdlib" // Driver for database/sql
)

// TestUserTransactionRolledBackOnDisconnect verifies that user-started transactions (BEGIN)
// are rolled back when the client disconnects, while the base transaction used by pgtest
// remains active and unchanged.
func TestUserTransactionRolledBackOnDisconnect(t *testing.T) {
	testID := "test_tx_disconnect"

	db, ctx, proxyServer, cleanup := connectToProxyForTestWithServer(t, testID)
	defer cleanup()

	db.SetConnMaxLifetime(5 * time.Second)
	db.SetMaxOpenConns(1)

	tableName := fmt.Sprintf("pgtest_tx_disconnect_%d", time.Now().UnixNano())

	// Create table and insert a row *outside* any user BEGIN so it lives in the base transaction.
	testutil.CreateTableWithIdAndData(t, db, tableName)
	testutil.InsertRowWithData(t, db, tableName, "before_begin", "insert outside user transaction")

	// Start a user transaction (BEGIN -> SAVEPOINT) and insert a second row inside it.
	if _, err := db.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatalf("BEGIN in client connection failed: %v", err)
	}
	testutil.InsertRowWithData(t, db, tableName, "in_tx", "insert inside user transaction")

	// Close the client connection without COMMIT/ROLLBACK.
	// This should trigger implicit rollback of user savepoints on the server side,
	// but must NOT roll back the base transaction.
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close client DB connection: %v", err)
	}

	// Give the proxy a brief moment to process the disconnect and run the rollback defer.
	time.Sleep(200 * time.Millisecond)

	// Fetch the server-side session corresponding to this testID.
	session := proxyServer.Pgtest.GetSession(testID)
	if session == nil || session.DB == nil {
		t.Fatalf("Session for testID %q should exist and have DB", testID)
	}

	// Base transaction must still be active (we must NOT have rolled it back).
	if !session.DB.HasActiveTransaction() {
		t.Fatal("Base transaction should still be active after client disconnect")
	}

	// All user savepoints should have been rolled back.
	if level := session.DB.GetSavepointLevel(); level != 0 {
		t.Fatalf("SavepointLevel after disconnect = %d, want 0", level)
	}

	// Data inserted before BEGIN (in the base transaction) must still be visible.
	testutil.AssertRowCountWithCondition(
		t,
		session.DB.Tx(),
		tableName,
		"data = 'before_begin'",
		1,
		"row inserted before user BEGIN should remain after disconnect",
	)

	// Data inserted inside the user transaction should have been rolled back.
	testutil.AssertRowCountWithCondition(
		t,
		session.DB.Tx(),
		tableName,
		"data = 'in_tx'",
		0,
		"row inserted inside user BEGIN should be rolled back on disconnect",
	)
}

// TestOnlyOneConnectionMayHaveOpenTransaction verifies that when two connections
// use the same pgtest session (same testID), only one can have an open user transaction
// (BEGIN) at a time; the second BEGIN returns ErrOnlyOneTransactionAtATime.
func TestOnlyOneConnectionMayHaveOpenTransaction(t *testing.T) {
	testID := "test_only_one_tx"
	ctx := context.Background()

	db1, _, proxyServer, cleanup := connectToProxyForTestWithServer(t, testID)
	defer cleanup()

	cfg := getConfigForProxyTest(t)
	if cfg == nil {
		return
	}
	db2 := openDBToProxy(t, cfg, "pgtest_"+testID)
	defer db2.Close()

	// First connection: BEGIN must succeed.
	_, err1 := db1.ExecContext(ctx, "BEGIN")
	if err1 != nil {
		t.Fatalf("first BEGIN failed: %v", err1)
	}

	// Second connection: BEGIN must fail with only-one-transaction error.
	_, err2 := db2.ExecContext(ctx, "BEGIN")
	if err2 == nil {
		t.Fatal("second BEGIN should fail when another connection has an open transaction")
	}

	// Session should still have conn1 as the one with open tx; base transaction intact.
	session := proxyServer.Pgtest.GetSession(testID)
	if session == nil || session.DB == nil {
		t.Fatalf("session for testID %q should exist", testID)
	}
	if !session.DB.HasActiveTransaction() {
		t.Fatal("base transaction should still be active")
	}
	// After first connection's BEGIN, savepoint level is 1.
	if level := session.DB.GetSavepointLevel(); level != 1 {
		t.Errorf("SavepointLevel = %d, want 1", level)
	}
}

// TestDisconnectWithoutCommitTableGone connects to the proxy, starts a transaction (BEGIN),
// creates a table, then disconnects without COMMIT. It reconnects with the same testID and
// verifies that the table does not exist (implicit rollback on disconnect).
func TestDisconnectWithoutCommitTableGone(t *testing.T) {
	testID := "test_disconnect_no_commit"
	ctx := context.Background()

	db1, _, proxyServer, cleanup := connectToProxyForTestWithServer(t, testID)
	defer cleanup()

	//db1.SetConnMaxLifetime(5 * time.Second)
	//db1.SetMaxOpenConns(2)

	tableName := fmt.Sprintf("pgtest_disconnect_table_%d", time.Now().UnixNano())

	// Start a user transaction and create a table inside it.
	if _, err := db1.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}
	createSQL := fmt.Sprintf("CREATE TABLE %s (id INT)", tableName)
	if _, err := db1.ExecContext(ctx, createSQL); err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Disconnect without COMMIT (no COMMIT, no ROLLBACK).
	if err := db1.Close(); err != nil {
		t.Fatalf("Failed to close connection: %v", err)
	}

	// Reconnect with the same testID and check that the table does not exist.
	// The proxy serializes session backend use (LockBackend): db2's first query blocks
	// until the disconnect defer has finished RollbackUserSavepointsOnDisconnect.
	cfg := getConfigForProxyTest(t)
	if cfg == nil {
		return
	}
	db2 := openDBToProxy(t, cfg, "pgtest_"+testID)
	defer db2.Close()

	var exists bool
	err := db2.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)",
		tableName,
	).Scan(&exists)
	if err != nil {
		t.Fatalf("Failed to check table existence: %v", err)
	}
	if exists {
		t.Error("table should not exist after disconnecting without COMMIT (implicit rollback on disconnect)")
	}

	// Session and base transaction should still be valid.
	session := proxyServer.Pgtest.GetSession(testID)
	if session == nil || session.DB == nil {
		t.Fatalf("session for testID %q should exist after reconnect", testID)
	}
	if !session.DB.HasActiveTransaction() {
		t.Fatal("base transaction should still be active after reconnect")
	}
}

// TestFullTransactionCommitSucceeds verifies that BEGIN followed by COMMIT completes without error.
// This depends on the proxy sending ReadyForQuery('T') after BEGIN so that clients that check
// transaction state (e.g. PDO via PQtransactionStatus) before sending COMMIT will actually send COMMIT.
func TestFullTransactionCommitSucceeds(t *testing.T) {
	ctx := context.Background()
	testID := "test_full_tx_commit"

	db, _, _, cleanup := connectToProxyForTestWithServer(t, testID)
	defer cleanup()

	db.SetConnMaxLifetime(5 * time.Second)
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}
	// COMMIT must succeed; if the proxy had sent ReadyForQuery('I') after BEGIN, a client like PDO
	// would throw "There is no active transaction" before sending COMMIT.
	if _, err := db.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatalf("COMMIT after BEGIN failed: %v (proxy must send ReadyForQuery 'T' after BEGIN)", err)
	}
}
