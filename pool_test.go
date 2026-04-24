package mysql

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
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
// fake that hands back sqlmock pools. Every returned mock pre-expects the
// "SET time_zone = ?" that setSessionTimezone always emits (the go-sql
// driver's ParseDSN defaults Loc to UTC, so the call is unconditional).
// The caller can push additional expectations onto mo.mocks[i] after the
// constructor runs.
func installMockOpen(t *testing.T) *mockOpen {
	t.Helper()

	original := sqlOpenFunc
	mo := &mockOpen{}

	sqlOpenFunc = func(driver, dsn string) (*sql.DB, error) {
		if driver != "mysql" {
			return nil, errors.New("unexpected driver: " + driver)
		}
		pool, m, err := sqlmock.New()
		if err != nil {
			return nil, err
		}
		// setSessionTimezone runs unconditionally because ParseDSN
		// populates Loc with time.UTC by default.
		m.ExpectExec(`SET time_zone = \?`).
			WillReturnResult(sqlmock.NewResult(0, 0))
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

	require.NoError(t, db.Reconnect())
	require.Equal(t, 4, mo.calls, "Reconnect on a dual-pool db must open two new pools")
	require.NotSame(t, writesPool(t, db), db.Reads, "Reconnect must not collapse to one pool")
}

func TestReconnect_SharedPoolStaysShared(t *testing.T) {
	mo := installMockOpen(t)

	db, err := NewFromDSN(testDSN, testDSN)
	require.NoError(t, err)
	require.Equal(t, 1, mo.calls)

	require.NoError(t, db.Reconnect())
	require.Equal(t, 2, mo.calls, "Reconnect on a shared-pool db should only open one new pool")
	require.Same(t, writesPool(t, db), db.Reads)
}

func TestOpenPool_PingFailureClosesPool(t *testing.T) {
	original := sqlOpenFunc
	t.Cleanup(func() { sqlOpenFunc = original })

	var capturedMock sqlmock.Sqlmock
	pingErr := errors.New("boom")
	sqlOpenFunc = func(driver, dsn string) (*sql.DB, error) {
		pool, m, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		if err != nil {
			return nil, err
		}
		m.ExpectPing().WillReturnError(pingErr)
		m.ExpectClose()
		capturedMock = m
		return pool, nil
	}

	_, err := openPool(testDSN, "writes")
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
	sqlOpenFunc = func(driver, dsn string) (*sql.DB, error) {
		call++
		if call == 1 {
			pool, m, err := sqlmock.New()
			if err != nil {
				return nil, err
			}
			m.ExpectExec(`SET time_zone = \?`).
				WillReturnResult(sqlmock.NewResult(0, 0))
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
