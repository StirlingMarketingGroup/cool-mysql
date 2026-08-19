package mysql

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

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

func withMaxExcusedLockWaits(t *testing.T, n int) {
	t.Helper()
	orig := MaxExcusedLockWaits
	MaxExcusedLockWaits = n
	t.Cleanup(func() { MaxExcusedLockWaits = orig })
}

func withMaxExcusedDeadlocks(t *testing.T, n int) {
	t.Helper()
	orig := MaxExcusedDeadlocks
	MaxExcusedDeadlocks = n
	t.Cleanup(func() { MaxExcusedDeadlocks = orig })
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

// Tx.Values is attempt-scoped: a retried-away attempt is a different *Tx with
// its own map, so state stored on attempt 1 is gone on attempt 2 (and the
// abandoned Tx is just garbage-collected — no hook-based cleanup). Only the
// winning attempt's PostCommitHooks fire.
func TestRunInTxAttemptValuesAreAttemptScoped(t *testing.T) {
	withMaxAttempts(t, 3)

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnError(&stdMysql.MySQLError{Number: 1213})
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	const sentinelKey = "attempt-1"

	var txs []*Tx
	var commitCounts []int
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		tx := TxFromContext(ctx)
		txs = append(txs, tx)
		i := len(txs) - 1
		commitCounts = append(commitCounts, 0)

		if i == 1 {
			_, seen := tx.Values.Load(sentinelKey)
			require.False(t, seen, "attempt 2 must not see attempt 1's sentinel — Values is fresh per attempt")
		}

		tx.Values.Store(sentinelKey, "from-attempt-1")
		tx.PostCommitHooks = append(tx.PostCommitHooks, func() error {
			commitCounts[i]++
			return nil
		})
		return tx.Exec("insert into `t` (`a`) values (1)")
	})
	require.NoError(t, err)
	require.Len(t, txs, 2, "exactly 2 attempts")
	require.NotSame(t, txs[0], txs[1], "each attempt gets a distinct Tx")
	require.Equal(t, 0, commitCounts[0], "retried-away attempt's PostCommitHooks never fire")
	require.Equal(t, 1, commitCounts[1], "winning attempt's PostCommitHooks fire once")
}

// PostAbandonHooks fire for every attempt that ends without a durable commit,
// including a retried-away in-tx deadlock. The discarded attempt's abandon
// hook must run (and finish) before the replayed closure starts, so a
// per-attempt lock can be released before the next attempt re-acquires it.
// The winning attempt's abandon hook must not fire, and no rollback hooks
// fire — those stay final-outcome-only.
func TestRunInTxAbandonHooksFireOnRetriedAttempt(t *testing.T) {
	withMaxAttempts(t, 3)

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnError(errTestDeadlock)
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	var events []string
	attempt := 0
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		attempt++
		n := attempt
		events = append(events, fmt.Sprintf("enter-%d", n))
		tx := TxFromContext(ctx)
		tx.PostAbandonHooks = append(tx.PostAbandonHooks, func() {
			events = append(events, fmt.Sprintf("abandon-%d", n))
		})
		tx.PostRollbackHooks = append(tx.PostRollbackHooks, func() {
			events = append(events, fmt.Sprintf("rollback-%d", n))
		})
		return tx.Exec("insert into `t` (`a`) values (1)")
	})
	require.NoError(t, err)
	require.Equal(t, []string{"enter-1", "abandon-1", "enter-2"}, events,
		"attempt 1 abandon fires once before attempt 2 enters; attempt 2 abandon never fires; no rollback hooks")
}

// A retryable COMMIT-time 1213 ends the driver tx immediately; the raw
// rollback then reports sql.ErrTxDone, so PostRollbackHooks (confirmed-
// rollback gated) would miss this. PostAbandonHooks must still fire so
// per-attempt resources are released before the replay.
func TestRunInTxAbandonHooksFireOnRetryableCommitFailure(t *testing.T) {
	withMaxAttempts(t, 3)

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit().WillReturnError(&stdMysql.MySQLError{Number: 1213})
	// A failed Commit marks the driver tx done, so RunInTx's raw rollback
	// returns sql.ErrTxDone without reaching the driver — sqlmock therefore
	// sees no Rollback (same as TestRunInTxCommitDeadlockRetries). That is
	// exactly the case rollback-confirmation gating would miss.
	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	var events []string
	attempt := 0
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		attempt++
		n := attempt
		events = append(events, fmt.Sprintf("enter-%d", n))
		tx := TxFromContext(ctx)
		tx.PostAbandonHooks = append(tx.PostAbandonHooks, func() {
			events = append(events, fmt.Sprintf("abandon-%d", n))
		})
		tx.PostRollbackHooks = append(tx.PostRollbackHooks, func() {
			events = append(events, fmt.Sprintf("rollback-%d", n))
		})
		return tx.Exec("insert into `t` (`a`) values (1)")
	})
	require.NoError(t, err)
	require.Equal(t, []string{"enter-1", "abandon-1", "enter-2"}, events,
		"failed COMMIT still fires attempt 1 abandon before attempt 2 enters; rollback-confirmation gating would miss this")
}

// Exhausted retries: every attempt's abandon hook fires, and the final
// attempt's PostRollbackHooks fire too (both kinds for the terminal
// rollback). Earlier attempts' rollback hooks never fire.
func TestRunInTxAbandonHooksExhaustedRetries(t *testing.T) {
	withMaxAttempts(t, 2)

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnError(errTestDeadlock)
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnError(errTestDeadlock)
	mock.ExpectRollback()

	var events []string
	attempt := 0
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		attempt++
		n := attempt
		events = append(events, fmt.Sprintf("enter-%d", n))
		tx := TxFromContext(ctx)
		tx.PostAbandonHooks = append(tx.PostAbandonHooks, func() {
			events = append(events, fmt.Sprintf("abandon-%d", n))
		})
		tx.PostRollbackHooks = append(tx.PostRollbackHooks, func() {
			events = append(events, fmt.Sprintf("rollback-%d", n))
		})
		return tx.Exec("insert into `t` (`a`) values (1)")
	})
	require.Error(t, err)
	require.Equal(t, []string{"enter-1", "abandon-1", "enter-2", "abandon-2", "rollback-2"}, events,
		"each attempt abandons once; only the final attempt's rollback hooks fire")
}

// A closure may commit the tx itself through the exposed Tx.Commit. That is a
// durable commit, so the attempt defer must not treat the attempt as abandoned
// — tx.committed (set by commit()) is the gate, not RunInTx's own bookkeeping.
// RunInTx's follow-up commit then fails with sql.ErrTxDone and surfaces as an
// error, but no abandon or rollback hooks may fire on the committed tx.
func TestRunInTxClosureSelfCommitDoesNotFireAbandonHooks(t *testing.T) {
	withMaxAttempts(t, 3)

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	var abandonCount, rollbackCount, commitCount int
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		tx := TxFromContext(ctx)
		tx.PostAbandonHooks = append(tx.PostAbandonHooks, func() { abandonCount++ })
		tx.PostRollbackHooks = append(tx.PostRollbackHooks, func() { rollbackCount++ })
		tx.PostCommitHooks = append(tx.PostCommitHooks, func() error { commitCount++; return nil })
		if err := tx.Exec("insert into `t` (`a`) values (1)"); err != nil {
			return err
		}
		return tx.Commit()
	})
	require.Error(t, err, "RunInTx's own commit fails on the already-committed tx")
	require.Equal(t, 0, abandonCount, "no abandon hooks after a durable self-commit")
	require.Equal(t, 0, rollbackCount, "no rollback hooks after a durable self-commit")
	require.Equal(t, 1, commitCount, "the closure's own Commit fired commit hooks once")
}

// A closure that durably self-commits and then fails with a deadlock-coded
// error (e.g. its post-commit hook wraps one) must NOT be replayed — the
// writes are committed, and a replay would duplicate them. sqlmock proves no
// second Begin happened.
func TestRunInTxSelfCommitRetryableHookErrorNotReplayed(t *testing.T) {
	withMaxAttempts(t, 3)

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	var abandonCount, rollbackCount int
	attempts := 0
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		attempts++
		tx := TxFromContext(ctx)
		tx.PostAbandonHooks = append(tx.PostAbandonHooks, func() { abandonCount++ })
		tx.PostRollbackHooks = append(tx.PostRollbackHooks, func() { rollbackCount++ })
		tx.PostCommitHooks = append(tx.PostCommitHooks, func() error { return errTestDeadlock })
		if err := tx.Exec("insert into `t` (`a`) values (1)"); err != nil {
			return err
		}
		return tx.Commit()
	})
	require.Error(t, err)
	require.Equal(t, 1, attempts, "the committed attempt must not be replayed despite the deadlock-coded error")
	require.Equal(t, 0, abandonCount)
	require.Equal(t, 0, rollbackCount)
	require.NoError(t, mock.ExpectationsWereMet(), "exactly one Begin/Exec/Commit — no replay")
}

// Cancel inside the closure and the attempt defer are two ending paths for the
// same Tx; abandon hooks must fire exactly once (tx.abandoned), not once per
// path.
func TestRunInTxCancelInsideClosureFiresAbandonHooksOnce(t *testing.T) {
	withMaxAttempts(t, 3)

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectRollback()

	var abandonCount int
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		tx := TxFromContext(ctx)
		tx.PostAbandonHooks = append(tx.PostAbandonHooks, func() { abandonCount++ })
		if err := tx.Cancel(); err != nil {
			return err
		}
		return errors.New("giving up after cancel")
	})
	require.Error(t, err)
	require.Equal(t, 1, abandonCount, "Cancel fired the abandon hooks; the attempt defer must not fire them again")
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

// A lock-wait block far longer than MaxExecutionTime must not exhaust the
// retry budget — that blocked time is excused, up to MaxExcusedLockWaits
// attempts (#7829). Without the fix this is exactly today's prod bug: a
// single blocked attempt burns the whole elapsed-time budget and RunInTx
// never gets a second attempt.
func TestRunInTxExcusesLockWaitBlockedTimeFromBudget(t *testing.T) {
	withMaxAttempts(t, 5)
	withMaxExcusedLockWaits(t, 1)

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	db.MaxExecutionTime = 20 * time.Millisecond // far smaller than the 150ms block below

	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillDelayFor(150 * time.Millisecond).WillReturnError(errTestLockWait)
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	var calls int
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		calls++
		return TxFromContext(ctx).ExecContext(ctx, "insert into `t` (`a`) values (1)")
	})
	require.NoError(t, err)
	require.Equal(t, 2, calls, "the 150ms lock-wait block must not be charged against the 20ms budget")
}

// MaxExcusedLockWaits=1 excuses only the FIRST 1205; the second is charged in
// full and blows the tiny budget, so RunInTx must stop instead of retrying
// forever on a permanently stuck lock.
func TestRunInTxLockWaitExcuseCapStopsRunaway(t *testing.T) {
	withMaxAttempts(t, 10) // generous — the elapsed-budget gate, not MaxTries, must be what stops this
	withMaxExcusedLockWaits(t, 1)

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	db.MaxExecutionTime = 5 * time.Millisecond

	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillDelayFor(30 * time.Millisecond).WillReturnError(errTestLockWait)
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillDelayFor(30 * time.Millisecond).WillReturnError(errTestLockWait)
	mock.ExpectRollback()

	var calls int
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		calls++
		return TxFromContext(ctx).ExecContext(ctx, "insert into `t` (`a`) values (1)")
	})
	require.Error(t, err)
	require.Equal(t, 2, calls, "only the first 1205 is excused; the second is charged and exhausts the 5ms budget")

	var mysqlErr *stdMysql.MySQLError
	require.True(t, errors.As(err, &mysqlErr) && mysqlErr.Number == 1205, "want 1205, got %v", err)
}

// A deadlock's attempt time is charged in full (no blocked-time excuse), so
// MaxExecutionTime still bounds a stuck-deadlock loop — but past-budget
// deadlocks are granted MaxExcusedDeadlocks replays before the gate turns
// terminal, so the bound is budget + N re-runs, not "first over-budget 1213
// is instantly fatal". Here every attempt outruns the 5ms budget and
// deadlocks; with the grant capped at 2 the loop must stop after exactly
// 1 charged attempt + 2 excused replays.
func TestRunInTxDeadlockChargedInFullBoundedPastBudgetByExcuseCap(t *testing.T) {
	withMaxAttempts(t, 100) // generous — the elapsed-budget gate + excuse cap, not MaxTries, must be what stops this
	withMaxExcusedDeadlocks(t, 2)

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	db.MaxExecutionTime = 5 * time.Millisecond

	for range 3 {
		mock.ExpectBegin()
		mock.ExpectExec("insert into `t`").WillDelayFor(20 * time.Millisecond).WillReturnError(errTestDeadlock)
		mock.ExpectRollback()
	}

	var calls int
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		calls++
		return TxFromContext(ctx).ExecContext(ctx, "insert into `t` (`a`) values (1)")
	})
	require.Error(t, err)
	require.Equal(t, 3, calls, "over-budget deadlocks get exactly MaxExcusedDeadlocks replays past the budget, then surface")

	var mysqlErr *stdMysql.MySQLError
	require.True(t, errors.As(err, &mysqlErr) && mysqlErr.Number == 1213, "want 1213, got %v", err)
}

// The CI/prod failure shape this grant exists for: the closure's real work
// alone outruns the whole budget, then the deadlock-detector kills it
// near-instantly. Charging that work time exhausted the budget before any
// replay could run, so the very first 1213 surfaced un-retried. The grant
// must give it a replay, and the clean second attempt commits.
func TestRunInTxExcusesFirstDeadlockPastBudget(t *testing.T) {
	withMaxAttempts(t, 5)
	withMaxExcusedDeadlocks(t, 1)

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	db.MaxExecutionTime = 5 * time.Millisecond // far smaller than the 30ms of "work" below

	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillDelayFor(30 * time.Millisecond).WillReturnError(errTestDeadlock)
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	var calls int
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		calls++
		return TxFromContext(ctx).ExecContext(ctx, "insert into `t` (`a`) values (1)")
	})
	require.NoError(t, err)
	require.Equal(t, 2, calls, "a slow first attempt's deadlock must still get a replay past the exhausted budget")
}

// MaxExcusedDeadlocks<=0 restores the prior behavior: an over-budget deadlock
// is terminal immediately, with zero replays.
func TestRunInTxDeadlockPastBudgetTerminalWhenGrantDisabled(t *testing.T) {
	withMaxAttempts(t, 100)
	withMaxExcusedDeadlocks(t, 0)

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	db.MaxExecutionTime = 5 * time.Millisecond

	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillDelayFor(20 * time.Millisecond).WillReturnError(errTestDeadlock)
	mock.ExpectRollback()

	var calls int
	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		calls++
		return TxFromContext(ctx).ExecContext(ctx, "insert into `t` (`a`) values (1)")
	})
	require.Error(t, err)
	require.Equal(t, 1, calls, "with the grant disabled an over-budget deadlock must not replay")

	var mysqlErr *stdMysql.MySQLError
	require.True(t, errors.As(err, &mysqlErr) && mysqlErr.Number == 1213, "want 1213, got %v", err)
}

// A ctx deadline must cut retries short regardless of how generous
// MaxExcusedLockWaits/MaxExecutionTime are — the excuse mechanism only exists
// for the no-deadline path (#7829). sqlmock's ExecContext races the fabricated
// 200ms block against ctx.Done() and returns early when the ctx dies first.
func TestRunInTxCtxDeadlineBoundsRetriesRegardlessOfLockWaitCap(t *testing.T) {
	withMaxAttempts(t, 100)
	withMaxExcusedLockWaits(t, 100) // absurdly generous — the deadline, not the cap, must be what stops this

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	db.MaxExecutionTime = 10 * time.Second // large — proves MaxExecutionTime isn't the limiter either

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillDelayFor(200 * time.Millisecond).WillReturnError(errTestLockWait)
	mock.ExpectRollback()

	start := time.Now()
	var calls int
	err := db.RunInTx(ctx, func(ctx context.Context) error {
		calls++
		return TxFromContext(ctx).ExecContext(ctx, "insert into `t` (`a`) values (1)")
	})
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Equal(t, 1, calls, "a ctx deadline must cut the first blocked attempt short — no excusing under a deadline")
	require.Less(t, elapsed, time.Second, "must not wait out the full 200ms fabricated block, let alone retry")

	// database/sql's BeginTx(ctx) starts awaitDone, which rolls back asynchronously
	// when the ctx deadline fires. That can race RunInTx's own deferred Rollback
	// for the driver call: if awaitDone wins the done-flag CAS but has not yet
	// invoked driver.Rollback when we return, ExpectationsWereMet would spuriously
	// report an unmatched ExpectRollback. Wait briefly for the async path.
	require.Eventually(t, func() bool {
		return mock.ExpectationsWereMet() == nil
	}, time.Second, time.Millisecond)
}
