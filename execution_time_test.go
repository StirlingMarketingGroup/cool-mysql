package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func withPackageExecTimes(t *testing.T, exec, conn time.Duration) {
	t.Helper()
	origExec := MaxExecutionTime
	origConn := MaxConnectionTime
	MaxExecutionTime = exec
	MaxConnectionTime = conn
	t.Cleanup(func() {
		MaxExecutionTime = origExec
		MaxConnectionTime = origConn
	})
}

func TestNewFromDSN_SeedsExecutionTimeFields(t *testing.T) {
	installMockOpen(t)
	withPackageExecTimes(t, 11*time.Second, 13*time.Second)

	db, err := NewFromDSN(testDSN, testDSN)
	require.NoError(t, err)
	require.Equal(t, 11*time.Second, db.MaxExecutionTime)
	require.Equal(t, 13*time.Second, db.MaxConnectionTime)
}

func TestNewFromDSN_DistinctDSN_SeedsExecutionTimeFields(t *testing.T) {
	installMockOpen(t)
	withPackageExecTimes(t, 7*time.Second, 9*time.Second)

	db, err := NewFromDSN(testDSN, "user:pass@tcp(replica:3306)/db")
	require.NoError(t, err)
	require.Equal(t, 7*time.Second, db.MaxExecutionTime)
	require.Equal(t, 9*time.Second, db.MaxConnectionTime)
}

func TestNewFromDSNDualPool_SeedsExecutionTimeFields(t *testing.T) {
	installMockOpen(t)
	withPackageExecTimes(t, 5*time.Second, 3*time.Second)

	db, err := NewFromDSNDualPool(testDSN)
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, db.MaxExecutionTime)
	require.Equal(t, 3*time.Second, db.MaxConnectionTime)
}

func TestNewFromConn_SeedsExecutionTimeFields(t *testing.T) {
	withPackageExecTimes(t, 19*time.Second, 23*time.Second)

	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = mockDB.Close() })

	mock.ExpectQuery(`^SELECT @@max_allowed_packet$`).
		WillReturnRows(sqlmock.NewRows([]string{"@@max_allowed_packet"}).AddRow(int64(4194304)))

	db, err := NewFromConn(mockDB, mockDB)
	require.NoError(t, err)
	require.Equal(t, 19*time.Second, db.MaxExecutionTime)
	require.Equal(t, 23*time.Second, db.MaxConnectionTime)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestNewFromConn_DistinctPools_SeedsExecutionTimeFields(t *testing.T) {
	withPackageExecTimes(t, 2*time.Second, 4*time.Second)

	writesDB, writesMock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = writesDB.Close() })
	readsDB, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = readsDB.Close() })

	writesMock.ExpectQuery(`^SELECT @@max_allowed_packet$`).
		WillReturnRows(sqlmock.NewRows([]string{"@@max_allowed_packet"}).AddRow(int64(4194304)))

	db, err := NewFromConn(writesDB, readsDB)
	require.NoError(t, err)
	require.Equal(t, 2*time.Second, db.MaxExecutionTime)
	require.Equal(t, 4*time.Second, db.MaxConnectionTime)
	require.NotSame(t, db.Writes, db.Reads, "distinct conns must remain distinct")
}

func TestSetMaxConnectionTime_UpdatesFieldAndPools(t *testing.T) {
	installMockOpen(t)

	db, err := NewFromDSNDualPool(testDSN)
	require.NoError(t, err)

	// Sanity: two independent pools so the setter has both branches to walk.
	require.NotSame(t, writesPool(t, db), db.Reads)

	db.SetMaxConnectionTime(42 * time.Second)
	require.Equal(t, 42*time.Second, db.MaxConnectionTime)

	db.SetMaxConnectionTime(0)
	require.Equal(t, time.Duration(0), db.MaxConnectionTime)
}

func TestSetMaxConnectionTime_NonDBWrites_NilReads(t *testing.T) {
	// Cover the branches where Writes isn't a *sql.DB (e.g. NewWriter)
	// and Reads is nil.
	db, err := NewWriter(&bytes.Buffer{})
	require.NoError(t, err)
	require.Nil(t, db.Reads)
	_, isSQLDB := db.Writes.(*sql.DB)
	require.False(t, isSQLDB)

	db.SetMaxConnectionTime(5 * time.Second)
	require.Equal(t, 5*time.Second, db.MaxConnectionTime)
}

func TestNewWriter_SeedsExecutionTimeFields(t *testing.T) {
	withPackageExecTimes(t, 6*time.Second, 8*time.Second)

	db, err := NewWriter(&bytes.Buffer{})
	require.NoError(t, err)
	require.Equal(t, 6*time.Second, db.MaxExecutionTime)
	require.Equal(t, 8*time.Second, db.MaxConnectionTime)
}

func TestNewLocalWriter_SeedsExecutionTimeFields(t *testing.T) {
	withPackageExecTimes(t, 17*time.Second, 21*time.Second)

	db, err := NewLocalWriter(t.TempDir())
	require.NoError(t, err)
	require.Equal(t, 17*time.Second, db.MaxExecutionTime)
	require.Equal(t, 21*time.Second, db.MaxConnectionTime)
}

func TestReconnect_PreservesMaxConnectionTimeOverride(t *testing.T) {
	installMockOpen(t)

	// Construct with the package-level default so fresh pools would
	// otherwise reset to it; then override per-instance and verify the
	// override survives Reconnect.
	db, err := NewFromDSN(testDSN, testDSN)
	require.NoError(t, err)

	const override = 3 * time.Second
	db.SetMaxConnectionTime(override)

	require.NoError(t, db.Reconnect())
	require.Equal(t, override, db.MaxConnectionTime, "Reconnect must not clobber the field")
	// The fresh pool had SetConnMaxLifetime re-applied; we can't observe
	// the pool's internal lifetime, but covering the branch that does
	// the re-application is what this test is here for (the exec above
	// walks the Reconnect code that touches both branches).
}

func TestReconnect_PreservesMaxConnectionTimeOverride_DualPool(t *testing.T) {
	installMockOpen(t)

	db, err := NewFromDSNDualPool(testDSN)
	require.NoError(t, err)

	const override = 9 * time.Second
	db.SetMaxConnectionTime(override)

	require.NoError(t, db.Reconnect())
	require.Equal(t, override, db.MaxConnectionTime)
	// fresh.Reads != fresh.Writes on the dual-pool path, so this also
	// exercises the branch that applies the lifetime to a distinct
	// reads pool.
	require.NotSame(t, writesPool(t, db), db.Reads)
}

func TestExec_RespectsDBMaxExecutionTime(t *testing.T) {
	// Using an unrealistically small budget so backoff.Retry stops
	// after a single retryable error instead of waiting the
	// package-level default.
	h := &failingExecHandler{
		errors: []error{
			errMockRetry, errMockRetry, errMockRetry, errMockRetry,
			errMockRetry, errMockRetry, errMockRetry, errMockRetry,
		},
	}

	db := &Database{MaxExecutionTime: time.Nanosecond}

	start := time.Now()
	_, err := db.exec(h, context.Background(), nil, true, "SELECT 1")
	elapsed := time.Since(start)

	require.Error(t, err)
	require.True(t, errors.Is(err, errMockRetry), "err = %v", err)
	require.Less(t, elapsed, time.Second, "budget not honored, elapsed=%s", elapsed)
	require.Less(t, h.calls, len(h.errors), "should not exhaust all retries with 1ns budget")
}

func TestSelect_RespectsDBMaxExecutionTime(t *testing.T) {
	// getTestDatabase gives us a *Database whose Reads/Writes are a real
	// sqlmock *sql.DB. Override MaxExecutionTime and force a retryable
	// error via sqlmock to exercise select.go's budget.
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	db.MaxExecutionTime = time.Nanosecond
	mock.ExpectQuery(`^SELECT 1$`).WillReturnError(errMockRetry)

	start := time.Now()
	var out []int
	err := db.Select(&out, "SELECT 1", 0)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.True(t, errors.Is(err, errMockRetry), "err = %v", err)
	require.Less(t, elapsed, time.Second, "budget not honored, elapsed=%s", elapsed)
}

func TestExists_RespectsDBMaxExecutionTime(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	db.MaxExecutionTime = time.Nanosecond
	mock.ExpectQuery(`^SELECT 1$`).WillReturnError(errMockRetry)

	start := time.Now()
	_, err := db.Exists("SELECT 1", 0)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.True(t, errors.Is(err, errMockRetry), "err = %v", err)
	require.Less(t, elapsed, time.Second, "budget not honored, elapsed=%s", elapsed)
}
