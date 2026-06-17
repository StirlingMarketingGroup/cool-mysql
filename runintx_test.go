package mysql

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	stdMysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func withMaxAttempts(t *testing.T, n int) {
	t.Helper()
	orig := MaxAttempts
	MaxAttempts = n
	t.Cleanup(func() { MaxAttempts = orig })
}

// checkTxRetryError is the narrow gate for whole-transaction retries: a deadlock
// (1213), a lock-wait timeout (1205), or anything reporting SQLSTATE 40001 — and
// nothing else, including other retryable-in-autocommit MySQL errors.
func TestCheckTxRetryError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"deadlock 1213", &stdMysql.MySQLError{Number: 1213}, true},
		{"lock wait timeout 1205", &stdMysql.MySQLError{Number: 1205}, true},
		{"foreign vendor code, sqlstate 40001", &stdMysql.MySQLError{Number: 1637, SQLState: [5]byte{'4', '0', '0', '0', '1'}}, true},
		{"deadlock wrapped in Error", Error{Err: &stdMysql.MySQLError{Number: 1213}}, true},
		{"other mysql error (dup key)", &stdMysql.MySQLError{Number: 1062}, false},
		{"non-mysql error", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, checkTxRetryError(tt.err))
		})
	}
}

// RunInTx owns the transaction boundary: it begins, runs fn, and commits.
func TestRunInTxCommitsOnSuccess(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	var calls int
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		calls++
		return TxFromContext(ctx).Exec("insert into `t` (`a`) values (1)")
	})
	require.NoError(t, err)
	require.Equal(t, 1, calls, "fn should run exactly once on the happy path")
}

// A deadlock on the first attempt re-runs the WHOLE closure from a fresh Begin;
// the second attempt commits exactly once with no phantom rows. The retry is a
// fresh Begin/Exec/Commit, which sqlmock validates by sequence.
func TestRunInTxRetriesDeadlockThenCommits(t *testing.T) {
	withMaxAttempts(t, 3)

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnError(errTestDeadlock)
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	var calls int
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		calls++
		return TxFromContext(ctx).Exec("insert into `t` (`a`) values (1)")
	})
	require.NoError(t, err)
	require.Equal(t, 2, calls, "fn should run twice: deadlock then success")
}

// Directly exercises the "no phantom rows" acceptance criterion: attempt 1
// writes A successfully, then deadlocks on B. The whole transaction rolls back —
// A must NOT leak out as a committed autocommit write (the unsound replay #167
// removed; sqlmock would flag an extra Exec or Commit). Attempt 2 then re-runs
// the WHOLE closure (A+B) from a fresh Begin and commits exactly once.
func TestRunInTxRollsBackPreDeadlockWriteThenReplaysWholeClosure(t *testing.T) {
	withMaxAttempts(t, 3)

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	// attempt 1: A succeeds, B deadlocks → rollback (A discarded, never committed).
	mock.ExpectBegin()
	mock.ExpectExec("insert into `a`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("insert into `b`").WillReturnError(errTestDeadlock)
	mock.ExpectRollback()
	// attempt 2: the whole closure replays and commits once.
	mock.ExpectBegin()
	mock.ExpectExec("insert into `a`").WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectExec("insert into `b`").WillReturnResult(sqlmock.NewResult(3, 1))
	mock.ExpectCommit()

	var attempts int
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		attempts++
		tx := TxFromContext(ctx)
		if err := tx.Exec("insert into `a` (`x`) values (1)"); err != nil {
			return err
		}
		return tx.Exec("insert into `b` (`x`) values (2)")
	})
	require.NoError(t, err)
	require.Equal(t, 2, attempts, "the whole closure (A+B) re-runs after a mid-closure deadlock")
}

// A deadlock that never clears surfaces the 1213 after the bound, with the
// transaction rolled back and no commit.
func TestRunInTxDeadlockEveryAttemptSurfaces(t *testing.T) {
	withMaxAttempts(t, 2)

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnError(errTestDeadlock)
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnError(errTestDeadlock)
	mock.ExpectRollback()

	var calls int
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		calls++
		return TxFromContext(ctx).Exec("insert into `t` (`a`) values (1)")
	})
	require.Error(t, err)
	require.Equal(t, 2, calls, "fn should run once per attempt up to the bound")

	var mysqlErr *stdMysql.MySQLError
	require.True(t, errors.As(err, &mysqlErr) && mysqlErr.Number == 1213, "want error 1213, got %v", err)
}

// A non-deadlock error returns immediately without retry and rolls back the
// owned transaction.
func TestRunInTxNonDeadlockErrorNoRetry(t *testing.T) {
	withMaxAttempts(t, 5) // budget to spare — a non-deadlock error must still not retry

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectRollback()

	sentinel := errors.New("business rule violation")
	var calls int
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		calls++
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)
	require.Equal(t, 1, calls, "a non-deadlock error must not be retried")
}

// A RunInTx invoked with a tx already in ctx runs fn once and propagates the
// deadlock: no nested Begin (sqlmock would see a second begin) and no retry.
func TestRunInTxNestedRunsOnceNoBeginNoRetry(t *testing.T) {
	withMaxAttempts(t, 5)

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin() // the single outer tx
	mock.ExpectExec("insert into `t`").WillReturnError(errTestDeadlock)
	mock.ExpectRollback() // the caller rolls the outer tx back below

	outerTx, cancel, err := db.BeginTx()
	require.NoError(t, err)
	defer cancel()

	ctx := NewContextWithTx(context.Background(), outerTx)

	var calls int
	err = db.RunInTx(ctx, func(ctx context.Context) error {
		calls++
		return TxFromContext(ctx).Exec("insert into `t` (`a`) values (1)")
	})
	require.Error(t, err)
	require.Equal(t, 1, calls, "nested RunInTx must run fn exactly once")

	var mysqlErr *stdMysql.MySQLError
	require.True(t, errors.As(err, &mysqlErr) && mysqlErr.Number == 1213, "deadlock must propagate to the outer owner, got %v", err)

	require.NoError(t, cancel(), "the outer owner rolls back the dead transaction")
}

// Commit/rollback hooks fire once, for the final outcome. When a deadlocked
// attempt is retried away and the retry commits, only the commit hooks fire —
// the discarded attempt's rollback hooks must not fire and prematurely undo work
// that is about to be redone.
func TestRunInTxHooksFireOnceOnCommit(t *testing.T) {
	withMaxAttempts(t, 3)

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnError(errTestDeadlock)
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	var commitCount, rollbackCount int
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		tx := TxFromContext(ctx)
		tx.PostCommitHooks = append(tx.PostCommitHooks, func() error { commitCount++; return nil })
		tx.PostRollbackHooks = append(tx.PostRollbackHooks, func() { rollbackCount++ })
		return tx.Exec("insert into `t` (`a`) values (1)")
	})
	require.NoError(t, err)
	require.Equal(t, 1, commitCount, "commit hooks fire once for the committed outcome")
	require.Equal(t, 0, rollbackCount, "a retried-away attempt's rollback hooks must not fire")
}

// When every attempt deadlocks and the bound is hit, the final outcome is a
// rollback: the rollback hooks fire exactly once (not once per attempt) and the
// commit hooks never fire.
func TestRunInTxHooksFireOnceOnTerminalRollback(t *testing.T) {
	withMaxAttempts(t, 2)

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnError(errTestDeadlock)
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnError(errTestDeadlock)
	mock.ExpectRollback()

	var commitCount, rollbackCount int
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		tx := TxFromContext(ctx)
		tx.PostCommitHooks = append(tx.PostCommitHooks, func() error { commitCount++; return nil })
		tx.PostRollbackHooks = append(tx.PostRollbackHooks, func() { rollbackCount++ })
		return tx.Exec("insert into `t` (`a`) values (1)")
	})
	require.Error(t, err)
	require.Equal(t, 0, commitCount, "commit hooks never fire when the tx ultimately rolls back")
	require.Equal(t, 1, rollbackCount, "rollback hooks fire once for the final outcome, not per attempt")
}

// A PostCommitHook failure happens AFTER the SQL COMMIT already succeeded, so
// the transaction is durable. RunInTx must surface the hook error without firing
// rollback hooks (the work committed) and without retrying — even when the hook
// error wraps a deadlock code, which would otherwise re-run fn and duplicate the
// already-committed writes. sqlmock enforces this: a retry or rollback would be
// an unexpected second Begin / a Rollback that no expectation allows.
func TestRunInTxPostCommitHookFailureDoesNotRollBackOrRetry(t *testing.T) {
	withMaxAttempts(t, 5) // budget to spare — a committed tx must still never retry

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	var calls, rollbackCount int
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		calls++
		tx := TxFromContext(ctx)
		// A hook error that wraps 1213 is the trap: if RunInTx routed it through
		// the deadlock-retry path it would re-run fn on a fresh tx and double the
		// committed insert.
		tx.PostCommitHooks = append(tx.PostCommitHooks, func() error { return errTestDeadlock })
		tx.PostRollbackHooks = append(tx.PostRollbackHooks, func() { rollbackCount++ })
		return tx.Exec("insert into `t` (`a`) values (1)")
	})
	require.Error(t, err)
	require.ErrorIs(t, err, errTestDeadlock)
	require.Contains(t, err.Error(), "post commit hook failed")
	require.Equal(t, 1, calls, "a committed tx must never retry, even if a hook error wraps a deadlock")
	require.Equal(t, 0, rollbackCount, "rollback hooks must not fire for a durably committed tx")
}

// A failed Begin surfaces immediately: it is not the in-tx deadlock RunInTx
// exists to retry, so fn never runs and there is no second Begin.
func TestRunInTxBeginFailureSurfaces(t *testing.T) {
	withMaxAttempts(t, 5) // budget to spare — a Begin failure must still not retry

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	beginErr := errors.New("connection refused")
	mock.ExpectBegin().WillReturnError(beginErr)

	var calls int
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		calls++
		return nil
	})
	require.ErrorIs(t, err, beginErr)
	require.Equal(t, 0, calls, "fn must not run when Begin fails")
}

// A non-deadlock COMMIT failure surfaces without retry. The driver marks the tx
// done on a failed COMMIT, so the raw rollback returns sql.ErrTxDone — there is
// no confirmed rollback, so PostRollbackHooks must NOT fire, matching Cancel()'s
// established semantics (#139): hooks fire only on a rollback we actually
// confirmed, never after an ambiguous/failed commit.
func TestRunInTxCommitErrorSurfacesNoRetry(t *testing.T) {
	withMaxAttempts(t, 5)

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	commitErr := errors.New("commit exploded")
	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit().WillReturnError(commitErr)

	var calls, rollbackCount int
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		calls++
		tx := TxFromContext(ctx)
		tx.PostRollbackHooks = append(tx.PostRollbackHooks, func() { rollbackCount++ })
		return tx.Exec("insert into `t` (`a`) values (1)")
	})
	require.ErrorIs(t, err, commitErr)
	require.Equal(t, 1, calls, "a non-deadlock commit failure must not retry")
	require.Equal(t, 0, rollbackCount, "a failed commit is not a confirmed rollback, so rollback hooks must not fire")
}

// Tx.Commit surfaces a driver COMMIT error directly, without running
// PostCommitHooks (the SQL commit never landed).
func TestTxCommitReturnsDriverError(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	commitErr := errors.New("commit failed")
	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(commitErr)

	tx, cancel, err := db.BeginTx()
	require.NoError(t, err)
	defer cancel()

	var hookRan bool
	tx.PostCommitHooks = append(tx.PostCommitHooks, func() error { hookRan = true; return nil })

	require.ErrorIs(t, tx.Commit(), commitErr)
	require.False(t, hookRan, "PostCommitHooks must not run when the driver COMMIT fails")
}

// A tx-fatal deadlock on a SELECT inside a transaction must surface unchanged,
// not be statement-retried: the tx is dead, so a retry would run in autocommit
// and silently drop any FOR UPDATE locks the tx held. sqlmock enforces no retry —
// a second query would be an unexpected call.
func TestSelectTxDeadlockSurfacesWithoutRetry(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectQuery("select `a` from `t`").WillReturnError(errTestDeadlock)
	mock.ExpectRollback()

	tx, cancel, err := db.BeginTx()
	require.NoError(t, err)
	defer cancel()

	var out []int
	err = tx.Select(&out, "select `a` from `t`", 0)
	require.Error(t, err)
	var mysqlErr *stdMysql.MySQLError
	require.True(t, errors.As(err, &mysqlErr) && mysqlErr.Number == 1213, "want 1213, got %v", err)
}

// The same in-tx surfacing applies to Exists (a lock-wait timeout here): no
// statement-level retry, so RunInTx can restart the whole transaction.
func TestExistsTxLockWaitTimeoutSurfacesWithoutRetry(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectQuery("select 1 from `t`").WillReturnError(errTestLockWait)
	mock.ExpectRollback()

	tx, cancel, err := db.BeginTx()
	require.NoError(t, err)
	defer cancel()

	_, err = tx.Exists("select 1 from `t`", 0)
	require.Error(t, err)
	var mysqlErr *stdMysql.MySQLError
	require.True(t, errors.As(err, &mysqlErr) && mysqlErr.Number == 1205, "want 1205, got %v", err)
}

// If a deadlocked attempt is retried away and the next attempt's Begin fails, the
// discarded attempt's rollback hooks must NOT fire as the final outcome — lastTx
// is cleared before each Begin, so a Begin failure leaves no stale tx behind.
func TestRunInTxBeginFailureAfterDeadlockDropsStaleRollbackHooks(t *testing.T) {
	withMaxAttempts(t, 3)

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	beginErr := errors.New("connection refused")
	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnError(errTestDeadlock)
	mock.ExpectRollback()
	mock.ExpectBegin().WillReturnError(beginErr)

	var calls, rollbackCount int
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		calls++
		tx := TxFromContext(ctx)
		tx.PostRollbackHooks = append(tx.PostRollbackHooks, func() { rollbackCount++ })
		return tx.Exec("insert into `t` (`a`) values (1)")
	})
	require.ErrorIs(t, err, beginErr, "the terminal Begin failure should surface")
	require.Equal(t, 1, calls, "fn runs only on the first attempt; the retry can't even Begin")
	require.Equal(t, 0, rollbackCount, "the retried-away attempt's rollback hooks must not fire")
}

// A deadlock on COMMIT (not just on a statement) also retries the whole closure.
// A failed Commit marks the tx done, so the belt-and-suspenders rollback is a
// no-op that never reaches the driver — sqlmock therefore sees no rollback
// between the failed commit and the next Begin.
func TestRunInTxCommitDeadlockRetries(t *testing.T) {
	withMaxAttempts(t, 3)

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit().WillReturnError(errTestDeadlock)
	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	var calls int
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		calls++
		return TxFromContext(ctx).Exec("insert into `t` (`a`) values (1)")
	})
	require.NoError(t, err)
	require.Equal(t, 2, calls, "fn re-runs from a fresh Begin after a commit-time deadlock")
}
