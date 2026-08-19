package proxy

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"pgrollback/internal/proxy/gui"
)

const lockInspectorAppName = "pgrollback-lock-inspector"

// lockInspector runs read-only catalog queries on a dedicated PostgreSQL connection so lock
// checks never touch session backends or acquire locks on user tables.
type lockInspector struct {
	host     string
	port     int
	database string
	user     string
	password string

	mu               sync.Mutex
	conn             *pgx.Conn
	unavailableUntil time.Time
}

func newLockInspector(host string, port int, database, user, password string) *lockInspector {
	return &lockInspector{
		host:     host,
		port:     port,
		database: database,
		user:     user,
		password: password,
	}
}

// IsPgrollbackApplicationName reports whether application_name identifies a pgrollback backend session.
func IsPgrollbackApplicationName(app string) bool {
	if app == "" {
		return false
	}
	return strings.HasPrefix(app, "pgrollback")
}

// testIDFromPgrollbackAppName extracts the test session id from a pgrollback application_name, or "".
func testIDFromPgrollbackAppName(app string) string {
	switch {
	case strings.HasPrefix(app, "pgrollback-"):
		return strings.TrimPrefix(app, "pgrollback-")
	case strings.HasPrefix(app, "pgrollback_"):
		return strings.TrimPrefix(app, "pgrollback_")
	case app == "pgrollback_default":
		return "default"
	default:
		return ""
	}
}

func (p *PgRollback) ensureLockInspector() *lockInspector {
	p.lockInspectorOnce.Do(func() {
		p.lockInspector = newLockInspector(
			p.PostgresHost, p.PostgresPort, p.PostgresDB, p.PostgresUser, p.PostgresPass,
		)
	})
	return p.lockInspector
}

// enrichLockStatusForSession fills LockStatus on one SessionInfo with a live lookup (no-op when
// inspector unavailable) and warms the session's cache so later hot-path reads see the result.
func (p *PgRollback) enrichLockStatusForSession(info *gui.SessionInfo) {
	if info == nil {
		return
	}
	appName := getAppNameForTestID(info.TestID)
	statuses := p.ensureLockInspector().lookup(context.Background(), []string{appName})
	var result *gui.LockStatus
	if st, ok := statuses[appName]; ok {
		result = &st
	}
	info.LockStatus = result
	p.cacheLockStatus(info.TestID, result)
}

// enrichSessionsLockStatus batch-fills LockStatus for GUI session lists with one live lookup
// round trip, and warms each session's cache so later hot-path reads see the result.
func (p *PgRollback) enrichSessionsLockStatus(list []gui.SessionInfo) {
	if len(list) == 0 {
		return
	}
	appNames := make([]string, len(list))
	for i := range list {
		appNames[i] = getAppNameForTestID(list[i].TestID)
	}
	statuses := p.ensureLockInspector().lookup(context.Background(), appNames)
	for i := range list {
		var result *gui.LockStatus
		if st, ok := statuses[appNames[i]]; ok {
			result = &st
		}
		list[i].LockStatus = result
		p.cacheLockStatus(list[i].TestID, result)
	}
}

// cacheLockStatus stores the latest live lookup result on the session itself (no-op if the
// session no longer exists), so PublishSessionUpdate/PublishSnapshot never need to query Postgres.
func (p *PgRollback) cacheLockStatus(testID string, st *gui.LockStatus) {
	session := p.GetSession(testID)
	if session == nil || session.DB == nil {
		return
	}
	session.DB.Gui.SetCachedLockStatus(st)
}

func (li *lockInspector) lookup(ctx context.Context, appNames []string) map[string]gui.LockStatus {
	out := make(map[string]gui.LockStatus)
	if li == nil || len(appNames) == 0 {
		return out
	}

	unique := make([]string, 0, len(appNames))
	seen := make(map[string]struct{}, len(appNames))
	for _, app := range appNames {
		if app == "" {
			continue
		}
		if _, ok := seen[app]; ok {
			continue
		}
		seen[app] = struct{}{}
		unique = append(unique, app)
	}
	if len(unique) == 0 {
		return out
	}

	li.mu.Lock()
	defer li.mu.Unlock()

	if time.Now().Before(li.unavailableUntil) {
		return out
	}

	qctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	li.fillFromBlockingLocks(qctx, unique, out)
	li.fillFromWaitEvents(qctx, unique, out)
	return out
}

const blockingLocksSQL = `
SELECT DISTINCT ON (blocked_activity.application_name)
  blocked_activity.application_name,
  blocked_activity.wait_event_type,
  blocked_activity.wait_event,
  blocked_locks.mode,
  COALESCE(blocked_locks.relation::regclass::text, '') AS locked_relation,
  blocking_activity.pid AS blocking_pid,
  blocking_activity.application_name AS blocking_application_name,
  left(blocking_activity.query, 200) AS blocking_query
FROM pg_catalog.pg_locks blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks blocking_locks
  ON blocking_locks.locktype = blocked_locks.locktype
  AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
  AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
  AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
  AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
  AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
  AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
  AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
  AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
  AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
  AND blocking_locks.pid != blocked_locks.pid
JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.granted
  AND blocking_locks.granted
  AND blocked_activity.datname = current_database()
  AND blocked_activity.application_name = ANY($1)
ORDER BY blocked_activity.application_name, blocking_locks.granted DESC
`

// fillFromBlockingLocks runs the blocking-locks catalog query. Caller must hold li.mu.
func (li *lockInspector) fillFromBlockingLocks(ctx context.Context, appNames []string, out map[string]gui.LockStatus) {
	conn, err := li.ensureConnLocked(ctx)
	if err != nil {
		li.unavailableUntil = time.Now().Add(15 * time.Second)
		return
	}
	rows, err := conn.Query(ctx, blockingLocksSQL, appNames)
	if err != nil {
		li.invalidateConn()
		return
	}
	defer rows.Close()

	for rows.Next() {
		var app, waitType, waitEvent, mode, relation, blockerApp, blockerQuery string
		var blockerPID int32
		if err := rows.Scan(&app, &waitType, &waitEvent, &mode, &relation, &blockerPID, &blockerApp, &blockerQuery); err != nil {
			continue
		}
		st := gui.LockStatus{
			WaitingOnLock:          true,
			WaitEventType:          waitType,
			WaitEvent:              waitEvent,
			LockedRelation:         relation,
			LockMode:               mode,
			BlockerPID:             blockerPID,
			BlockerApplicationName: blockerApp,
			BlockerQuerySnippet:    strings.TrimSpace(blockerQuery),
		}
		if IsPgrollbackApplicationName(blockerApp) {
			st.BlockerIsPgrollback = true
			st.BlockerTestID = testIDFromPgrollbackAppName(blockerApp)
		}
		out[app] = st
	}
}

const waitEventsSQL = `
SELECT application_name, wait_event_type, wait_event
FROM pg_catalog.pg_stat_activity
WHERE datname = current_database()
  AND application_name = ANY($1)
  AND wait_event_type = 'Lock'
`

func (li *lockInspector) fillFromWaitEvents(ctx context.Context, appNames []string, out map[string]gui.LockStatus) {
	conn, err := li.ensureConnLocked(ctx)
	if err != nil {
		li.unavailableUntil = time.Now().Add(15 * time.Second)
		return
	}
	rows, err := conn.Query(ctx, waitEventsSQL, appNames)
	if err != nil {
		li.invalidateConn()
		return
	}
	defer rows.Close()

	for rows.Next() {
		var app, waitType, waitEvent string
		if err := rows.Scan(&app, &waitType, &waitEvent); err != nil {
			continue
		}
		if existing, ok := out[app]; ok && existing.WaitingOnLock {
			continue
		}
		out[app] = gui.LockStatus{
			WaitingOnLock: true,
			WaitEventType: waitType,
			WaitEvent:     waitEvent,
		}
	}
}

func (li *lockInspector) ensureConnLocked(ctx context.Context) (*pgx.Conn, error) {
	if li.conn != nil {
		if err := li.conn.Ping(ctx); err == nil {
			return li.conn, nil
		}
		_ = li.conn.Close(ctx)
		li.conn = nil
	}

	conn, err := li.connect(ctx)
	if err != nil {
		return nil, err
	}
	li.conn = conn
	return conn, nil
}

func (li *lockInspector) connect(ctx context.Context) (*pgx.Conn, error) {
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(li.user, li.password),
		Host:   fmt.Sprintf("%s:%d", li.host, li.port),
		Path:   li.database,
	}
	q := u.Query()
	q.Set("sslmode", "disable")
	q.Set("application_name", lockInspectorAppName)
	u.RawQuery = q.Encode()

	config, err := pgx.ParseConfig(u.String())
	if err != nil {
		return nil, err
	}
	config.ConnectTimeout = 2 * time.Second
	config.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		d := &net.Dialer{Timeout: 2 * time.Second}
		return d.DialContext(ctx, network, addr)
	}

	return pgx.ConnectConfig(ctx, config)
}

func (li *lockInspector) invalidateConn() {
	if li.conn != nil {
		_ = li.conn.Close(context.Background())
		li.conn = nil
	}
}
