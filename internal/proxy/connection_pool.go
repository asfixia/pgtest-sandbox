package proxy

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5"
)

// newConnectionForTestID cria uma nova conexão PostgreSQL para o testID.
// A conexão pertence à sessão (TestSession) que a criou; não há pool separado.
// O mesmo testID sempre usa a mesma conexão porque há apenas uma sessão por testID,
// e a sessão guarda sua DB (conn+tx) em SessionsByTestID[testID].
//
// Connection parameters are set via pgx config (not DSN string concatenation) so that
// host, user, password, database, and application_name can safely contain spaces and
// special characters without manual escaping.
func newConnectionForTestID(host string, port int, database string, user string, password string, sessionTimeout time.Duration, testID string) (*pgx.Conn, error) {
	appName := getAppNameForTestID(testID)
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, password),
		Host:   fmt.Sprintf("%s:%d", host, port),
		Path:   database,
	}
	q := u.Query()
	q.Set("sslmode", "disable")
	q.Set("application_name", appName)
	u.RawQuery = q.Encode()
	dsn := u.String()

	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	if sessionTimeout <= 0 {
		sessionTimeout = 300 * time.Second
	}
	config.ConnectTimeout = sessionTimeout
	dialer := &net.Dialer{
		KeepAlive: 30 * time.Second,
		Timeout:   30 * time.Second,
	}
	config.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return dialer.DialContext(ctx, network, addr)
	}

	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}

	timeoutMs := int64(sessionTimeout / time.Millisecond)
	_, err = conn.Exec(context.Background(), fmt.Sprintf("SET statement_timeout = '0'; SET idle_session_timeout = '0'; SET idle_in_transaction_session_timeout = %d", timeoutMs))
	if err != nil {
		conn.Close(context.Background())
		return nil, fmt.Errorf("failed to set session timeout: %w", err)
	}

	return conn, nil
}

func getAppNameForTestID(testID string) string {
	if testID == "default" {
		return "pgtest_default"
	}
	return fmt.Sprintf("pgtest-%s", testID)
}
