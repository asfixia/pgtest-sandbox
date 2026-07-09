package tstproxy

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestLockStatusShowsBlockedByPgrollbackSession verifies the GUI lock inspector detects a session
// waiting on a table lock held by another pgrollback session (read-only pg_catalog query).
func TestLockStatusShowsBlockedByPgrollbackSession(t *testing.T) {
	cfg := getConfigForProxyTest(t)
	if cfg == nil {
		return
	}
	if !isPostgreSQLAvailable(t, cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database, cfg.Postgres.User, cfg.Postgres.Password) {
		t.Skipf("Skipping test - PostgreSQL is not available at %s:%d", cfg.Postgres.Host, cfg.Postgres.Port)
		return
	}

	blockerID := "lk_blk_" + strings.ReplaceAll(t.Name(), "/", "_")
	blockedID := "lk_wt_" + strings.ReplaceAll(t.Name(), "/", "_")
	advisoryLockKey := time.Now().UnixNano()

	blockerDB, ctx, server, cleanup := connectToProxyForTestWithServer(t, blockerID)
	defer cleanup()
	if blockerDB == nil || server == nil {
		return
	}

	qctx, cancel := context.WithTimeout(ctx, getOrDefault(cfg.Test.QueryTimeout.Duration, 30*time.Second))
	defer cancel()

	blockedDB := openDBToProxy(t, server.ListenHost(), server.ListenPort(), cfg, "pgrollback_"+blockedID)
	if blockedDB == nil {
		t.Fatal("failed to open blocked client connection")
	}
	blockedConn, err := blockedDB.Conn(qctx)
	if err != nil {
		t.Fatalf("blocked Conn: %v", err)
	}
	defer func() {
		blockedConn.Close()
		blockedDB.Close()
	}()

	if _, err := blockerDB.ExecContext(qctx, "SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
		t.Fatalf("blocker pg_advisory_lock: %v", err)
	}

	blockStarted := make(chan struct{})
	blockDone := make(chan struct{})
	go func() {
		close(blockStarted)
		_, _ = blockedConn.ExecContext(context.Background(), "SELECT pg_advisory_lock($1)", advisoryLockKey)
		close(blockDone)
	}()
	<-blockStarted
	time.Sleep(200 * time.Millisecond)

	deadline := time.Now().Add(8 * time.Second)
	var infoOK bool
	for time.Now().Before(deadline) {
		info, ok := server.PgRollback.SessionInfoFor(blockedID)
		if ok && info.LockStatus != nil && info.LockStatus.WaitingOnLock {
			if !info.LockStatus.BlockerIsPgrollback {
				t.Fatalf("LockStatus: blocker should be pgrollback, got app=%q", info.LockStatus.BlockerApplicationName)
			}
			if info.LockStatus.BlockerTestID != blockerID {
				t.Fatalf("LockStatus.BlockerTestID = %q, want %q", info.LockStatus.BlockerTestID, blockerID)
			}
			if info.LockStatus.LockedRelation == "" && !strings.Contains(info.LockStatus.WaitEvent, "relation") {
				t.Logf("warning: locked_relation empty (wait_event=%q)", info.LockStatus.WaitEvent)
			}
			infoOK = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !infoOK {
		t.Fatal("timed out waiting for blocked session lock status")
	}

	blockerInfo, ok := server.PgRollback.SessionInfoFor(blockerID)
	if !ok {
		t.Fatal("blocker session missing from SessionInfoFor")
	}
	if blockerInfo.LockStatus != nil && blockerInfo.LockStatus.WaitingOnLock {
		t.Fatalf("blocker session should not show waiting_on_lock: %+v", blockerInfo.LockStatus)
	}

	// Release advisory lock and disconnect blocker so the blocked goroutine can finish.
	if _, err := blockerDB.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", advisoryLockKey); err != nil {
		t.Logf("pg_advisory_unlock: %v", err)
	}
	cleanup()
	select {
	case <-blockDone:
	case <-time.After(5 * time.Second):
		t.Log("blocked ALTER did not finish after blocker disconnect (table may remain until rollback)")
	}
}
