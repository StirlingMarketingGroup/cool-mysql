package mysql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestRetryElapsedBudget(t *testing.T) {
	db := &Database{MaxExecutionTime: 27 * time.Second}

	// No deadline → the fixed global budget (preserves non-ctx API behavior).
	require.Equal(t, 27*time.Second, db.retryElapsedBudget(context.Background()))

	// A deadline below the global cap governs.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.InDelta(t, float64(5*time.Second), float64(db.retryElapsedBudget(ctx)), float64(500*time.Millisecond))

	// A deadline above the global cap is NOT cut down to it (the #174 bug).
	ctx2, cancel2 := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel2()
	require.Greater(t, db.retryElapsedBudget(ctx2), 60*time.Second)
}

func TestRetryWithinBudget(t *testing.T) {
	db := &Database{MaxExecutionTime: 27 * time.Second}
	now := time.Now()

	// Deadline path: a ~25s attempt with ~2s left must not be retried.
	ctxTight, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.False(t, db.retryWithinBudget(ctxTight, now.Add(-25*time.Second), now.Add(-25*time.Second)))

	// Deadline path: plenty of budget left → retry is allowed.
	ctxRoomy, cancel2 := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel2()
	require.True(t, db.retryWithinBudget(ctxRoomy, now.Add(-time.Second), now.Add(-time.Second)))

	// No-deadline path falls back to MaxExecutionTime measured from start.
	require.False(t, db.retryWithinBudget(context.Background(), now.Add(-25*time.Second), now.Add(-25*time.Second)))
	require.True(t, db.retryWithinBudget(context.Background(), now.Add(-time.Second), now.Add(-time.Second)))

	// Unbounded budget (no deadline, MaxExecutionTime == 0) always retries,
	// preserving the #172/#163 connection-recovery behavior.
	unbounded := &Database{MaxExecutionTime: 0}
	require.True(t, unbounded.retryWithinBudget(context.Background(), now.Add(-100*time.Second), now.Add(-100*time.Second)))
}

// TestSelectContext_InjectsMaxExecutionTimeHint proves a ctx deadline puts a
// MAX_EXECUTION_TIME hint on the outermost SELECT.
func TestSelectContext_InjectsMaxExecutionTimeHint(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mock.ExpectQuery(`^SELECT /\*\+ MAX_EXECUTION_TIME\(\d+\) \*/ 1$`).
		WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(int64(1)))

	var out []int
	require.NoError(t, db.SelectContext(ctx, &out, "SELECT 1", 0))
	require.Equal(t, []int{1}, out)
}

// TestSelect_NoDeadlineNoHint proves the non-ctx API (and any deadline-free
// ctx) leaves the query untouched.
func TestSelect_NoDeadlineNoHint(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectQuery(`^SELECT 1$`).
		WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(int64(1)))

	var out []int
	require.NoError(t, db.Select(&out, "SELECT 1", 0))
	require.Equal(t, []int{1}, out)
}

// TestSelectContext_QueryTimeoutFailsOnceCleanly proves a server-side
// ER_QUERY_TIMEOUT (3024) — what the injected hint produces on a real server —
// surfaces once, unretried, leaving a clean MySQL error for the caller. The
// single ExpectQuery (asserted by cleanup's ExpectationsWereMet) proves there
// was no blind replay.
func TestSelectContext_QueryTimeoutFailsOnceCleanly(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mock.ExpectQuery(`MAX_EXECUTION_TIME`).
		WillReturnError(&mysql.MySQLError{Number: 3024, Message: "Query execution was interrupted, maximum statement execution time exceeded"})

	var out []int
	err := db.SelectContext(ctx, &out, "SELECT 1", 0)
	require.Error(t, err)

	var myErr *mysql.MySQLError
	require.True(t, errors.As(err, &myErr), "want a MySQLError, got %v", err)
	require.Equal(t, uint16(3024), myErr.Number)
}

// The replay-guard tests below pin MaxExecutionTime to a nanosecond (budget
// already spent by the time the first attempt errors), so retryWithinBudget
// returns false and the ErrInvalidConn paths return a permanent error instead
// of swapping the conn and re-running — the #174 doubling guard.

func TestSelect_GuardStopsReplayOnInvalidConn(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	db.MaxExecutionTime = time.Nanosecond

	mock.ExpectQuery(`^SELECT 1$`).WillReturnError(mysql.ErrInvalidConn)

	var out []int
	err := db.Select(&out, "SELECT 1", 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, mysql.ErrInvalidConn), "err = %v", err)
}

func TestSelect_GuardStopsReplayOnMidStreamInvalidConn(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	db.MaxExecutionTime = time.Nanosecond

	rows := sqlmock.NewRows([]string{"n"}).AddRow(int64(1)).RowError(0, mysql.ErrInvalidConn)
	mock.ExpectQuery(`^SELECT 1$`).WillReturnRows(rows)

	var out []int
	err := db.Select(&out, "SELECT 1", 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, mysql.ErrInvalidConn), "err = %v", err)
}

func TestExec_GuardStopsReplayOnInvalidConn(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	db.MaxExecutionTime = time.Nanosecond

	mock.ExpectExec(`^UPDATE t SET x = 1$`).WillReturnError(mysql.ErrInvalidConn)

	err := db.Exec("UPDATE t SET x = 1")
	require.Error(t, err)
	require.True(t, errors.Is(err, mysql.ErrInvalidConn), "err = %v", err)
}

func TestExists_GuardStopsReplayOnInvalidConn(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	db.MaxExecutionTime = time.Nanosecond

	mock.ExpectQuery(`^SELECT 1$`).WillReturnError(mysql.ErrInvalidConn)

	_, err := db.Exists("SELECT 1", 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, mysql.ErrInvalidConn), "err = %v", err)
}

func TestExists_GuardStopsReplayOnMidStreamInvalidConn(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	db.MaxExecutionTime = time.Nanosecond

	rows := sqlmock.NewRows([]string{"n"}).AddRow(int64(1)).RowError(0, mysql.ErrInvalidConn)
	mock.ExpectQuery(`^SELECT 1$`).WillReturnRows(rows)

	_, err := db.Exists("SELECT 1", 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, mysql.ErrInvalidConn), "err = %v", err)
}

// TestExistsContext_InjectsMaxExecutionTimeHint proves a ctx deadline puts the
// hint on the exists read too.
func TestExistsContext_InjectsMaxExecutionTimeHint(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mock.ExpectQuery(`^SELECT /\*\+ MAX_EXECUTION_TIME\(\d+\) \*/ 1$`).
		WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(int64(1)))

	got, err := db.ExistsContext(ctx, "SELECT 1", 0)
	require.NoError(t, err)
	require.True(t, got)
}
