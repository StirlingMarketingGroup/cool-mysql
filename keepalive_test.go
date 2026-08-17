package mysql

import (
	"context"
	"database/sql"
	"net"
	"sync/atomic"
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

func setDialAttemptTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	orig := DialAttemptTimeout
	t.Cleanup(func() { DialAttemptTimeout = orig })
	DialAttemptTimeout = d
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

func TestApplyDialerToConfig(t *testing.T) {
	t.Run("neither knob: DialFunc stays nil", func(t *testing.T) {
		setKeepAlive(t, 0, 3)
		setDialAttemptTimeout(t, 0)
		setNetTimeouts(t, 0, 0, 0)
		cfg := mysqldrv.NewConfig()
		cfg.Net = "tcp"
		applyNetTimeoutsToConfig(cfg)
		applyDialerToConfig(cfg)
		require.Nil(t, cfg.DialFunc)
		require.Equal(t, "tcp", cfg.Net)
	})

	t.Run("keepalive only: DialFunc set, Net stays tcp", func(t *testing.T) {
		setKeepAlive(t, time.Second, 3)
		setDialAttemptTimeout(t, 0)
		setNetTimeouts(t, 0, 0, 0)
		cfg := mysqldrv.NewConfig()
		cfg.Net = "tcp"
		applyNetTimeoutsToConfig(cfg)
		applyDialerToConfig(cfg)
		require.NotNil(t, cfg.DialFunc)
		require.Equal(t, "tcp", cfg.Net)
	})

	t.Run("retry only: DialFunc set, Net stays tcp", func(t *testing.T) {
		setKeepAlive(t, 0, 3)
		setDialAttemptTimeout(t, 3*time.Second)
		setNetTimeouts(t, 0, 0, 12*time.Second)
		cfg := mysqldrv.NewConfig()
		cfg.Net = "tcp"
		applyNetTimeoutsToConfig(cfg)
		applyDialerToConfig(cfg)
		require.NotNil(t, cfg.DialFunc)
		require.Equal(t, "tcp", cfg.Net)
		require.Equal(t, 12*time.Second, cfg.Timeout)
	})

	t.Run("both knobs: DialFunc set, Net stays tcp", func(t *testing.T) {
		setKeepAlive(t, time.Second, 3)
		setDialAttemptTimeout(t, 3*time.Second)
		setNetTimeouts(t, 0, 0, 12*time.Second)
		cfg := mysqldrv.NewConfig()
		cfg.Net = "tcp"
		applyNetTimeoutsToConfig(cfg)
		applyDialerToConfig(cfg)
		require.NotNil(t, cfg.DialFunc)
		require.Equal(t, "tcp", cfg.Net)
	})

	t.Run("attempt timeout without total budget: DialFunc stays nil", func(t *testing.T) {
		setKeepAlive(t, 0, 3)
		setDialAttemptTimeout(t, 3*time.Second)
		setNetTimeouts(t, 0, 0, 0)
		cfg := mysqldrv.NewConfig()
		cfg.Net = "tcp"
		applyNetTimeoutsToConfig(cfg)
		applyDialerToConfig(cfg)
		require.Nil(t, cfg.DialFunc, "retry must not activate without a total dial budget")
		require.Equal(t, "tcp", cfg.Net)
	})

	t.Run("DSN timeout enables retry when DialTimeout is off", func(t *testing.T) {
		setKeepAlive(t, 0, 3)
		setDialAttemptTimeout(t, 3*time.Second)
		setNetTimeouts(t, 0, 0, 0)
		cfg, err := mysqldrv.ParseDSN("u:p@tcp(127.0.0.1:3306)/db?timeout=12s")
		require.NoError(t, err)
		applyNetTimeoutsToConfig(cfg)
		applyDialerToConfig(cfg)
		require.Equal(t, 12*time.Second, cfg.Timeout, "DSN timeout= is the total budget")
		require.NotNil(t, cfg.DialFunc, "a DSN timeout= must not silently disable retry")
		require.Equal(t, "tcp", cfg.Net)
	})

	t.Run("tcp4 and tcp6 get the DialFunc too", func(t *testing.T) {
		setKeepAlive(t, 0, 3)
		setDialAttemptTimeout(t, 3*time.Second)
		setNetTimeouts(t, 0, 0, 12*time.Second)
		for _, network := range []string{"tcp4", "tcp6"} {
			cfg := mysqldrv.NewConfig()
			cfg.Net = network
			applyNetTimeoutsToConfig(cfg)
			applyDialerToConfig(cfg)
			require.NotNil(t, cfg.DialFunc, network)
			require.Equal(t, network, cfg.Net)
		}
	})

	t.Run("unix sockets are left untouched", func(t *testing.T) {
		setKeepAlive(t, time.Second, 3)
		setDialAttemptTimeout(t, 3*time.Second)
		setNetTimeouts(t, 0, 0, 12*time.Second)
		cfg := mysqldrv.NewConfig()
		cfg.Net = "unix"
		applyNetTimeoutsToConfig(cfg)
		applyDialerToConfig(cfg)
		require.Nil(t, cfg.DialFunc)
		require.Equal(t, "unix", cfg.Net)
	})
}

// TestKeepAlive_DialsThroughDialFunc proves the keepalive DialFunc is wired
// correctly end to end: with TCPKeepAlive set, openPool installs cfg.DialFunc
// (cfg.Net stays "tcp"), and a real connection still establishes and queries
// successfully through it against the in-process fake MySQL server. (OS-level
// half-open teardown timing isn't deterministically testable without dropping
// packets, so this guards the plumbing — that we connect and query through the
// custom dialer rather than break dialing.)
func TestKeepAlive_DialsThroughDialFunc(t *testing.T) {
	setKeepAlive(t, time.Second, 3)

	var dials atomic.Int64
	origOpen := sqlOpenFunc
	t.Cleanup(func() { sqlOpenFunc = origOpen })
	sqlOpenFunc = func(cfg *mysqldrv.Config) (*sql.DB, error) {
		require.NotNil(t, cfg.DialFunc, "openPool must install DialFunc when TCPKeepAlive is on")
		require.Equal(t, "tcp", cfg.Net, "DialFunc must not rewrite cfg.Net")
		inner := cfg.DialFunc
		cfg.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
			dials.Add(1)
			return inner(ctx, network, addr)
		}
		return origOpen(cfg)
	}

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
	require.Greater(t, dials.Load(), int64(0), "the pool must have dialed through DialFunc")
}

// TestOpenPool_DialsThroughDialFuncWithRetry is the retry-knob counterpart:
// DialAttemptTimeout + a total budget (no keepalive) must still install a
// DialFunc that openPool actually uses to reach the fake server.
func TestOpenPool_DialsThroughDialFuncWithRetry(t *testing.T) {
	setKeepAlive(t, 0, 3)
	setDialAttemptTimeout(t, 3*time.Second)
	setNetTimeouts(t, 0, 0, 12*time.Second)

	var dials atomic.Int64
	origOpen := sqlOpenFunc
	t.Cleanup(func() { sqlOpenFunc = origOpen })
	sqlOpenFunc = func(cfg *mysqldrv.Config) (*sql.DB, error) {
		require.NotNil(t, cfg.DialFunc, "openPool must install DialFunc when dial retry is on")
		require.Equal(t, "tcp", cfg.Net)
		inner := cfg.DialFunc
		cfg.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
			dials.Add(1)
			return inner(ctx, network, addr)
		}
		return origOpen(cfg)
	}

	const q = "SELECT 'retry-dial'"
	srv := startFakeServer(t, func(query string) (queryAction, string, time.Duration) {
		if query == q {
			return actionRows, "ok", 0
		}
		return actionOK, "", 0
	})

	db, err := NewFromDSN(srv.dsn(), srv.dsn())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	var out []string
	require.NoError(t, db.Select(&out, q, 0))
	require.Equal(t, []string{"ok"}, out)
	require.Greater(t, dials.Load(), int64(0), "the pool must have dialed through DialFunc")
}
