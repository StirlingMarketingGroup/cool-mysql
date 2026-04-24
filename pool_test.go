package mysql

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	mysqldrv "github.com/go-sql-driver/mysql"
)

// The DSN value we feed the constructors in these tests. It only has to
// parse under github.com/go-sql-driver/mysql — sqlOpenFunc is swapped to
// return sqlmock pools, so the DSN is never dialed.
const testDSN = "user:pass@tcp(localhost:3306)/db"

type mockOpen struct {
	calls int
	pools []*sql.DB
	mocks []sqlmock.Sqlmock
}

// installMockOpen replaces sqlOpenFunc for the duration of a test with a
// fake that hands back sqlmock pools. The session timezone `SET` is
// issued by go-sql-driver per-conn via the injected DSN param, not by
// the library, so we don't pre-expect it here — sqlmock doesn't simulate
// the driver's conn init. The caller can push additional expectations
// onto mo.mocks[i] after the constructor runs.
func installMockOpen(t *testing.T) *mockOpen {
	t.Helper()

	original := sqlOpenFunc
	mo := &mockOpen{}

	sqlOpenFunc = func(cfg *mysqldrv.Config) (*sql.DB, error) {
		pool, m, err := sqlmock.New()
		if err != nil {
			return nil, err
		}
		mo.calls++
		mo.pools = append(mo.pools, pool)
		mo.mocks = append(mo.mocks, m)
		return pool, nil
	}

	t.Cleanup(func() {
		sqlOpenFunc = original
		for _, p := range mo.pools {
			_ = p.Close()
		}
	})

	return mo
}

func writesPool(t *testing.T, db *Database) *sql.DB {
	t.Helper()
	p, ok := db.Writes.(*sql.DB)
	require.True(t, ok, "db.Writes is not a *sql.DB")
	return p
}

func TestNewFromDSN_SameDSNSharesOnePool(t *testing.T) {
	mo := installMockOpen(t)

	db, err := NewFromDSN(testDSN, testDSN)
	require.NoError(t, err)

	require.Equal(t, 1, mo.calls, "same DSN should open exactly one pool")
	require.Same(t, writesPool(t, db), db.Reads, "Reads and Writes must share a pool")
	require.False(t, db.forceDualPool)
	require.Equal(t, testDSN, db.WritesDSN)
	require.Equal(t, testDSN, db.ReadsDSN)
}

func TestNewFromDSN_DistinctDSNsOpenTwoPools(t *testing.T) {
	mo := installMockOpen(t)

	writesDSN := testDSN
	readsDSN := "user:pass@tcp(replica:3306)/db"
	db, err := NewFromDSN(writesDSN, readsDSN)
	require.NoError(t, err)

	require.Equal(t, 2, mo.calls)
	require.NotSame(t, writesPool(t, db), db.Reads, "distinct DSNs must open distinct pools")
	require.False(t, db.forceDualPool)
	require.Equal(t, writesDSN, db.WritesDSN)
	require.Equal(t, readsDSN, db.ReadsDSN)
}

func TestNewFromDSNDualPool_OpensTwoDistinctPools(t *testing.T) {
	mo := installMockOpen(t)

	db, err := NewFromDSNDualPool(testDSN)
	require.NoError(t, err)

	require.Equal(t, 2, mo.calls, "dual-pool must open two pools even for the same DSN")
	require.NotSame(t, writesPool(t, db), db.Reads)
	require.True(t, db.forceDualPool)
	require.Equal(t, testDSN, db.WritesDSN)
	require.Equal(t, testDSN, db.ReadsDSN)
	require.NotNil(t, db.MaxInsertSize)
}

func TestReconnect_PreservesDualPool(t *testing.T) {
	mo := installMockOpen(t)

	db, err := NewFromDSNDualPool(testDSN)
	require.NoError(t, err)
	require.Equal(t, 2, mo.calls)
	// Expect the two original pools to be closed when Reconnect swaps.
	mo.mocks[0].ExpectClose()
	mo.mocks[1].ExpectClose()

	require.NoError(t, db.Reconnect())
	require.Equal(t, 4, mo.calls, "Reconnect on a dual-pool db must open two new pools")
	require.NotSame(t, writesPool(t, db), db.Reads, "Reconnect must not collapse to one pool")
	require.NoError(t, mo.mocks[0].ExpectationsWereMet())
	require.NoError(t, mo.mocks[1].ExpectationsWereMet())
}

func TestReconnect_SharedPoolStaysShared(t *testing.T) {
	mo := installMockOpen(t)

	db, err := NewFromDSN(testDSN, testDSN)
	require.NoError(t, err)
	require.Equal(t, 1, mo.calls)
	// The single shared old pool must be closed exactly once.
	mo.mocks[0].ExpectClose()

	require.NoError(t, db.Reconnect())
	require.Equal(t, 2, mo.calls, "Reconnect on a shared-pool db should only open one new pool")
	require.Same(t, writesPool(t, db), db.Reads)
	require.NoError(t, mo.mocks[0].ExpectationsWereMet())
}

func TestOpenPool_PingFailureClosesPool(t *testing.T) {
	original := sqlOpenFunc
	t.Cleanup(func() { sqlOpenFunc = original })

	var capturedMock sqlmock.Sqlmock
	pingErr := errors.New("boom")
	sqlOpenFunc = func(cfg *mysqldrv.Config) (*sql.DB, error) {
		pool, m, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		if err != nil {
			return nil, err
		}
		m.ExpectPing().WillReturnError(pingErr)
		m.ExpectClose()
		capturedMock = m
		return pool, nil
	}

	_, err := openPool(testDSN, "writes", 0)
	require.ErrorIs(t, err, pingErr)
	// ExpectClose on the mock asserts openPool closed the pool on error.
	require.NoError(t, capturedMock.ExpectationsWereMet())
}

func TestNewFromDSNDualPool_ReadsOpenFailureClosesWrites(t *testing.T) {
	original := sqlOpenFunc
	t.Cleanup(func() { sqlOpenFunc = original })

	openErr := errors.New("second open failed")
	var writesMock sqlmock.Sqlmock
	var call int
	sqlOpenFunc = func(cfg *mysqldrv.Config) (*sql.DB, error) {
		call++
		if call == 1 {
			pool, m, err := sqlmock.New()
			if err != nil {
				return nil, err
			}
			// After the reads-side open fails below, the constructor
			// must close the writes pool it already opened.
			m.ExpectClose()
			writesMock = m
			return pool, nil
		}
		return nil, openErr
	}

	_, err := NewFromDSNDualPool(testDSN)
	require.ErrorIs(t, err, openErr)
	require.NoError(t, writesMock.ExpectationsWereMet())
}

// applyTimeZoneToConfig is the per-conn hook wired via BeforeConnect (see
// openPool) and is the core of the fix for issue #152. These tests
// exercise it directly because BeforeConnect fires inside the driver's
// Connect flow, which sqlmock doesn't simulate.

func TestApplyTimeZoneToConfig_UTCLocProducesZeroOffset(t *testing.T) {
	cfg, err := mysqldrv.ParseDSN("user:pass@tcp(localhost:3306)/db?parseTime=true&loc=UTC")
	require.NoError(t, err)

	applyTimeZoneToConfig(cfg)
	require.Equal(t, "'+00:00'", cfg.Params["time_zone"])
}

func TestApplyTimeZoneToConfig_NonUTCLocUsesCurrentOffset(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	cfg, err := mysqldrv.ParseDSN("user:pass@tcp(localhost:3306)/db?parseTime=true&loc=UTC")
	require.NoError(t, err)
	cfg.Loc = loc

	applyTimeZoneToConfig(cfg)

	_, offset := time.Now().In(loc).Zone()
	expected := "'" + time.Unix(0, 0).In(time.FixedZone("", offset)).Format("-07:00") + "'"
	require.Equal(t, expected, cfg.Params["time_zone"])
}

// TestApplyTimeZoneToConfig_RecomputesPerInvocation mimics what
// BeforeConnect does across a DST transition: the same base cfg is
// cloned and mutated per conn, so simulating the two offsets on
// separate cfg copies must yield the two different offsets. This is
// the DST-safety guarantee openPool provides by wiring the hook
// per-conn rather than baking the offset into the DSN once.
func TestApplyTimeZoneToConfig_RecomputesPerInvocation(t *testing.T) {
	winter := time.FixedZone("EST", -5*60*60)
	summer := time.FixedZone("EDT", -4*60*60)

	base, err := mysqldrv.ParseDSN("user:pass@tcp(localhost:3306)/db?parseTime=true&loc=UTC")
	require.NoError(t, err)

	winterCfg := base.Clone()
	winterCfg.Loc = winter
	applyTimeZoneToConfig(winterCfg)
	require.Equal(t, "'-05:00'", winterCfg.Params["time_zone"])

	summerCfg := base.Clone()
	summerCfg.Loc = summer
	applyTimeZoneToConfig(summerCfg)
	require.Equal(t, "'-04:00'", summerCfg.Params["time_zone"])

	// The base cfg's Params map must be untouched — BeforeConnect
	// scopes mutation to the per-conn clone.
	require.Empty(t, base.Params["time_zone"])
}

func TestApplyTimeZoneToConfig_PreservesExplicitParam(t *testing.T) {
	cfg, err := mysqldrv.ParseDSN("user:pass@tcp(localhost:3306)/db?parseTime=true&loc=UTC&time_zone=%27%2B05%3A30%27")
	require.NoError(t, err)

	applyTimeZoneToConfig(cfg)
	require.Equal(t, "'+05:30'", cfg.Params["time_zone"],
		"an explicit caller-provided time_zone must be preserved verbatim")
}

func TestApplyTimeZoneToConfig_NilLocNoop(t *testing.T) {
	cfg := &mysqldrv.Config{}
	applyTimeZoneToConfig(cfg)
	require.Empty(t, cfg.Params["time_zone"])
	require.Nil(t, cfg.Params)
}
