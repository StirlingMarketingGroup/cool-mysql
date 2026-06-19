package mysql

import (
	"testing"
	"time"

	mysqldrv "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// setKeepAlive overrides the package-level keepalive knobs for the duration of a
// test and restores them afterward, so the off-by-default global state isn't
// leaked between tests.
func setKeepAlive(t *testing.T, period time.Duration, count int) {
	t.Helper()
	origPeriod, origCount := TCPKeepAlive, TCPKeepAliveCount
	t.Cleanup(func() { TCPKeepAlive, TCPKeepAliveCount = origPeriod, origCount })
	TCPKeepAlive, TCPKeepAliveCount = period, count
}

func TestKeepAliveDialer_ConfiguredWhenOn(t *testing.T) {
	setKeepAlive(t, 2*time.Second, 4)

	d := keepAliveDialer()
	require.True(t, d.KeepAliveConfig.Enable)
	require.Equal(t, 2*time.Second, d.KeepAliveConfig.Idle)
	require.Equal(t, 2*time.Second, d.KeepAliveConfig.Interval)
	require.Equal(t, 4, d.KeepAliveConfig.Count)
}

func TestKeepAliveDialer_OffByDefault(t *testing.T) {
	setKeepAlive(t, 0, 3)

	d := keepAliveDialer()
	require.False(t, d.KeepAliveConfig.Enable)
}

func TestApplyKeepAliveToConfig(t *testing.T) {
	t.Run("tcp pool is switched onto the keepalive net", func(t *testing.T) {
		setKeepAlive(t, time.Second, 3)
		cfg := mysqldrv.NewConfig()
		cfg.Net = "tcp"
		applyKeepAliveToConfig(cfg)
		require.Equal(t, keepAliveNet, cfg.Net)
	})

	t.Run("off is a no-op", func(t *testing.T) {
		setKeepAlive(t, 0, 3)
		cfg := mysqldrv.NewConfig()
		cfg.Net = "tcp"
		applyKeepAliveToConfig(cfg)
		require.Equal(t, "tcp", cfg.Net)
	})

	t.Run("unix sockets are left untouched", func(t *testing.T) {
		setKeepAlive(t, time.Second, 3)
		cfg := mysqldrv.NewConfig()
		cfg.Net = "unix"
		applyKeepAliveToConfig(cfg)
		require.Equal(t, "unix", cfg.Net)
	})
}

// TestKeepAlive_DialsThroughCustomNet proves the registered keepalive dialer is
// wired correctly end to end: with TCPKeepAlive set, openPool rewrites the pool
// onto keepAliveNet, and a real connection still establishes and queries
// successfully through it against the in-process fake MySQL server. (OS-level
// half-open teardown timing isn't deterministically testable without dropping
// packets, so this guards the plumbing — that we connect and query through the
// custom dialer rather than break dialing.)
func TestKeepAlive_DialsThroughCustomNet(t *testing.T) {
	setKeepAlive(t, time.Second, 3)

	const q = "SELECT 'ka'"
	srv := startFakeServer(t, func(query string) (queryAction, string, time.Duration) {
		if query == q {
			return actionRows, "alive", 0
		}
		return actionOK, "", 0
	})

	db, err := NewFromDSN(srv.dsn(), srv.dsn())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	var out []string
	require.NoError(t, db.Select(&out, q, 0))
	require.Equal(t, []string{"alive"}, out)
}
