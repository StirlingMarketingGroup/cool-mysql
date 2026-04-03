package mysql

import (
	"fmt"
	"testing"

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
