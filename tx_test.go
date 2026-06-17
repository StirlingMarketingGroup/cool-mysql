package mysql

import (
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	stdMysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestPostCommitHooks(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, cancel, err := db.BeginTx()
	require.NoError(t, err)
	defer cancel()

	var called bool
	tx.PostCommitHooks = append(tx.PostCommitHooks, func() error {
		called = true
		return nil
	})

	err = tx.Commit()
	require.NoError(t, err)
	require.True(t, called, "PostCommitHook should run after commit")
}

func TestPostCommitHookErrorStopsExecution(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, cancel, err := db.BeginTx()
	require.NoError(t, err)
	defer cancel()

	hookErr := fmt.Errorf("flush failed")
	var secondCalled bool

	tx.PostCommitHooks = append(tx.PostCommitHooks, func() error {
		return hookErr
	})
	tx.PostCommitHooks = append(tx.PostCommitHooks, func() error {
		secondCalled = true
		return nil
	})

	err = tx.Commit()
	require.Error(t, err)
	require.ErrorIs(t, err, hookErr, "Commit should return the wrapped hook error")
	require.Contains(t, err.Error(), "post commit hook failed")
	require.False(t, secondCalled, "subsequent hooks should not run after a failure")
}

func TestPostCommitHooksNotRunOnRollback(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectRollback()

	tx, _, err := db.BeginTx()
	require.NoError(t, err)

	var called bool
	tx.PostCommitHooks = append(tx.PostCommitHooks, func() error {
		called = true
		return nil
	})

	err = tx.Cancel()
	require.NoError(t, err)
	require.False(t, called, "PostCommitHook should not run on rollback")
}

func TestPostRollbackHooks(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectRollback()

	tx, _, err := db.BeginTx()
	require.NoError(t, err)

	var called bool
	tx.PostRollbackHooks = append(tx.PostRollbackHooks, func() {
		called = true
	})

	err = tx.Cancel()
	require.NoError(t, err)
	require.True(t, called, "PostRollbackHook should run after rollback")
}

func TestPostRollbackHooksNotRunOnCommit(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, cancel, err := db.BeginTx()
	require.NoError(t, err)
	defer cancel()

	var called bool
	tx.PostRollbackHooks = append(tx.PostRollbackHooks, func() {
		called = true
	})

	err = tx.Commit()
	require.NoError(t, err)
	require.False(t, called, "PostRollbackHook should not run on commit")
}

func TestPostRollbackHooksNotRunWhenAlreadyCommitted(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, cancel, err := db.BeginTx()
	require.NoError(t, err)

	var called bool
	tx.PostRollbackHooks = append(tx.PostRollbackHooks, func() {
		called = true
	})

	err = tx.Commit()
	require.NoError(t, err)

	// Cancel after commit — this is the common `defer cancel()` pattern.
	// Rollback returns sql.ErrTxDone, so hooks should NOT run.
	err = cancel()
	require.NoError(t, err)
	require.False(t, called, "PostRollbackHook should not run when tx was already committed")
}

// db.ExecResult runs on the writes pool outside any transaction; the result is
// returned to the caller.
func TestDatabaseExecResult(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(7, 1))

	res, err := db.ExecResult("insert into `t` (`a`) values (1)")
	require.NoError(t, err)
	id, err := res.LastInsertId()
	require.NoError(t, err)
	require.Equal(t, int64(7), id)
}

// tx.ExecResult runs the statement on the transaction's connection; the work is
// only durable once the caller commits.
func TestTxExecResult(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec("update `t`").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	tx, cancel, err := db.BeginTx()
	require.NoError(t, err)
	defer cancel()

	res, err := tx.ExecResult("update `t` set `a` = 1")
	require.NoError(t, err)
	n, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	require.NoError(t, tx.Commit())
}

// A deadlock (1213) inside an explicit transaction must surface to the caller
// unchanged — no inline retry and no autocommit replay (either would appear
// here as an unexpected second Exec or a `start transaction`, which sqlmock
// would reject). The caller then rolls back the dead transaction and is free to
// restart it from Begin (#167).
func TestTxExecDeadlockSurfacesForCallerRetry(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnError(errTestDeadlock)
	mock.ExpectRollback()

	tx, cancel, err := db.BeginTx()
	require.NoError(t, err)
	defer cancel() // rolls back the deadlocked transaction

	err = tx.Exec("insert into `t` (`a`) values (1)")
	require.Error(t, err)
	var mysqlErr *stdMysql.MySQLError
	require.True(t, errors.As(err, &mysqlErr) && mysqlErr.Number == 1213, "want error 1213, got %v", err)
}

func TestPostRollbackHooksMultiple(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectRollback()

	tx, _, err := db.BeginTx()
	require.NoError(t, err)

	var order []int
	tx.PostRollbackHooks = append(tx.PostRollbackHooks, func() {
		order = append(order, 1)
	})
	tx.PostRollbackHooks = append(tx.PostRollbackHooks, func() {
		order = append(order, 2)
	})
	tx.PostRollbackHooks = append(tx.PostRollbackHooks, func() {
		order = append(order, 3)
	})

	err = tx.Cancel()
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3}, order, "PostRollbackHooks should run in order")
}
