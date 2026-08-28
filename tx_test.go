package mysql

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

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

func TestCancelFiresAbandonHooks(t *testing.T) {
	t.Run("uncommitted", func(t *testing.T) {
		db, mock, cleanup := getTestDatabase(t)
		defer cleanup()

		mock.ExpectBegin()
		mock.ExpectRollback()

		tx, _, err := db.BeginTx()
		require.NoError(t, err)

		var abandonCount, rollbackCount, commitCount int
		tx.PostAbandonHooks = append(tx.PostAbandonHooks, func() { abandonCount++ })
		tx.PostRollbackHooks = append(tx.PostRollbackHooks, func() { rollbackCount++ })
		tx.PostCommitHooks = append(tx.PostCommitHooks, func() error { commitCount++; return nil })

		err = tx.Cancel()
		require.NoError(t, err)
		require.Equal(t, 1, abandonCount, "Cancel on an uncommitted tx fires abandon hooks")
		require.Equal(t, 1, rollbackCount, "Cancel on an uncommitted tx fires rollback hooks")
		require.Equal(t, 0, commitCount, "Cancel must not fire commit hooks")
	})

	t.Run("twice", func(t *testing.T) {
		db, mock, cleanup := getTestDatabase(t)
		defer cleanup()

		mock.ExpectBegin()
		mock.ExpectRollback()

		tx, _, err := db.BeginTx()
		require.NoError(t, err)

		var abandonCount int
		tx.PostAbandonHooks = append(tx.PostAbandonHooks, func() { abandonCount++ })

		require.NoError(t, tx.Cancel())
		require.NoError(t, tx.Cancel())
		require.Equal(t, 1, abandonCount, "a repeated Cancel must not double-fire abandon hooks")
	})

	t.Run("after commit", func(t *testing.T) {
		db, mock, cleanup := getTestDatabase(t)
		defer cleanup()

		mock.ExpectBegin()
		mock.ExpectCommit()

		tx, cancel, err := db.BeginTx()
		require.NoError(t, err)

		var abandonCount, rollbackCount, commitCount int
		tx.PostAbandonHooks = append(tx.PostAbandonHooks, func() { abandonCount++ })
		tx.PostRollbackHooks = append(tx.PostRollbackHooks, func() { rollbackCount++ })
		tx.PostCommitHooks = append(tx.PostCommitHooks, func() error { commitCount++; return nil })

		err = tx.Commit()
		require.NoError(t, err)
		require.Equal(t, 1, commitCount, "Commit fires commit hooks once")

		err = cancel()
		require.NoError(t, err)
		require.Equal(t, 0, abandonCount, "Cancel after a successful Commit must not fire abandon hooks")
		require.Equal(t, 0, rollbackCount, "Cancel after a successful Commit must not fire rollback hooks")
		require.Equal(t, 1, commitCount, "commit hooks stay at one after the deferred Cancel")
	})
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

func TestAfterInsert_TxFiresOnCommitBeforePostCommitHooks(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectCommit()

	var order []string
	var got []InsertEvent
	db.AfterInsert = func(ev InsertEvent) {
		got = append(got, ev)
		order = append(order, "afterInsert")
	}

	tx, cancel, err := db.BeginTx()
	require.NoError(t, err)
	defer cancel()

	tx.PostCommitHooks = append(tx.PostCommitHooks, func() error {
		order = append(order, "postCommit")
		return nil
	})

	require.NoError(t, tx.Insert("insert into people", testPerson{ID: 1, Name: "A"}))
	require.NoError(t, tx.Insert("insert into people", testPerson{ID: 2, Name: "B"}))
	require.Empty(t, got, "AfterInsert must not fire until Commit")

	require.NoError(t, tx.Commit())
	require.Equal(t, []string{"afterInsert", "afterInsert", "postCommit"}, order)
	require.Len(t, got, 2)
	require.Equal(t, [][]any{{1, "A"}}, got[0].Rows)
	require.Equal(t, [][]any{{2, "B"}}, got[1].Rows)
	id, err := got[0].Result.LastInsertId()
	require.NoError(t, err)
	require.Equal(t, int64(1), id)
	id, err = got[1].Result.LastInsertId()
	require.NoError(t, err)
	require.Equal(t, int64(2), id)
}

func TestAfterInsert_TxCancelDoesNotFire(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectRollback()

	db.AfterInsert = func(InsertEvent) { t.Fatal("AfterInsert must not fire after Cancel") }

	tx, _, err := db.BeginTx()
	require.NoError(t, err)
	require.NoError(t, tx.Insert("insert into people", testPerson{ID: 1, Name: "A"}))
	require.NoError(t, tx.Cancel())
}

func TestAfterInsert_RunInTxRetriesDiscardFirstAttempt(t *testing.T) {
	withMaxAttempts(t, 3)

	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec("insert into people").WillReturnError(errTestDeadlock)
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectCommit()

	var got []InsertEvent
	db.AfterInsert = func(ev InsertEvent) { got = append(got, ev) }

	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		return TxFromContext(ctx).Insert("insert into people", testPerson{ID: 1, Name: "A"})
	})
	require.NoError(t, err)
	require.Len(t, got, 1)
	id, err := got[0].Result.LastInsertId()
	require.NoError(t, err)
	require.Equal(t, int64(2), id)
	require.Equal(t, [][]any{{1, "A"}}, got[0].Rows)
}

func TestAfterInsert_TxSetExecutorBuffersUntilCommit(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	var got []InsertEvent
	db.AfterInsert = func(ev InsertEvent) { got = append(got, ev) }

	tx, cancel, err := db.BeginTx()
	require.NoError(t, err)
	defer cancel()

	require.NoError(t, tx.I().SetExecutor(tx.Tx).Insert("insert into people", testPerson{ID: 1, Name: "A"}))
	require.Empty(t, got, "SetExecutor keeps tx lifecycle, so AfterInsert waits for Commit")
	require.NoError(t, tx.Commit())
	require.Len(t, got, 1)
	require.Equal(t, [][]any{{1, "A"}}, got[0].Rows)
}

func TestAfterInsert_TxConcurrentInsertsAreAllPublished(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	const n = 8
	mock.ExpectBegin()
	for i := 0; i < n; i++ {
		mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(int64(i+1), 1))
	}
	mock.ExpectCommit()

	var mu sync.Mutex
	var got []InsertEvent
	db.AfterInsert = func(ev InsertEvent) {
		mu.Lock()
		got = append(got, ev)
		mu.Unlock()
	}

	tx, cancel, err := db.BeginTx()
	require.NoError(t, err)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			require.NoError(t, tx.Insert("insert into people", testPerson{ID: i, Name: "A"}))
		}(i)
	}
	wg.Wait()
	require.Empty(t, got, "nothing publishes before Commit")

	require.NoError(t, tx.Commit())
	require.Len(t, got, n, "every concurrent insert's event survives the buffer")
}

func TestAfterInsert_CommitWaitsForInFlightInsert(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	var got []InsertEvent
	db.AfterInsert = func(ev InsertEvent) { got = append(got, ev) }

	tx, cancel, err := db.BeginTx()
	require.NoError(t, err)
	defer cancel()

	// Log runs inside exec after the statement landed and before the event is
	// buffered — the exact window database/sql's tx lock no longer covers.
	// Installed after Begin (which logs too) and one-shot (Commit logs too).
	inLog := make(chan struct{})
	releaseLog := make(chan struct{})
	var once sync.Once
	db.Log = func(LogDetail) {
		once.Do(func() {
			close(inLog)
			<-releaseLog
		})
	}

	insertDone := make(chan error, 1)
	go func() { insertDone <- tx.Insert("insert into people", testPerson{ID: 1, Name: "A"}) }()
	<-inLog

	commitDone := make(chan error, 1)
	go func() { commitDone <- tx.Commit() }()
	select {
	case err := <-commitDone:
		t.Fatalf("Commit finished (%v) while an insert was still buffering its event", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseLog)
	require.NoError(t, <-insertDone)
	require.NoError(t, <-commitDone)
	require.Len(t, got, 1, "the event buffered under the lock is published by that Commit")
}

func TestAfterInsert_TxInsertsPublishInExecutionOrder(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	// In-order expectations: if B's statement ran before A's landed, sqlmock
	// fails B's exec.
	mock.ExpectBegin()
	mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectCommit()

	var got []InsertEvent
	db.AfterInsert = func(ev InsertEvent) { got = append(got, ev) }

	tx, cancel, err := db.BeginTx()
	require.NoError(t, err)
	defer cancel()

	// A lands, then blocks in Log before buffering; B must wait for A's
	// event to be buffered, not slip in ahead of it.
	inLog := make(chan struct{})
	releaseLog := make(chan struct{})
	var once sync.Once
	db.Log = func(LogDetail) {
		once.Do(func() {
			close(inLog)
			<-releaseLog
		})
	}

	aDone := make(chan error, 1)
	go func() { aDone <- tx.Insert("insert into people", testPerson{ID: 1, Name: "A"}) }()
	<-inLog

	bDone := make(chan error, 1)
	go func() { bDone <- tx.Insert("insert into people", testPerson{ID: 2, Name: "B"}) }()
	select {
	case err := <-bDone:
		t.Fatalf("B finished (%v) while A had landed but not buffered", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseLog)
	require.NoError(t, <-aDone)
	require.NoError(t, <-bDone)
	require.NoError(t, tx.Commit())
	require.Len(t, got, 2)
	require.Equal(t, [][]any{{1, "A"}}, got[0].Rows)
	require.Equal(t, [][]any{{2, "B"}}, got[1].Rows)
}

func TestAfterInsert_TxByteSliceIsSnapshotted(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	mock.ExpectBegin()
	mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	var got []InsertEvent
	db.AfterInsert = func(ev InsertEvent) { got = append(got, ev) }

	tx, cancel, err := db.BeginTx()
	require.NoError(t, err)
	defer cancel()

	row := struct {
		ID   definedBytes `mysql:"id"`
		Name string       `mysql:"name"`
	}{ID: definedBytes{1, 2, 3}, Name: "A"}
	require.NoError(t, tx.Insert("insert into people", row))
	row.ID[0] = 9 // the caller reuses its buffer before Commit
	require.NoError(t, tx.Commit())
	require.Len(t, got, 1)
	require.Equal(t, definedBytes{1, 2, 3}, got[0].Rows[0][0], "the event keeps the bytes the statement sent, as the caller's type")
}

func TestAfterInsert_TxInserterOnPoolExecutorIsDurableNow(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	mock.ExpectBegin()
	mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectRollback()

	var got []InsertEvent
	db.AfterInsert = func(ev InsertEvent) { got = append(got, ev) }

	tx, _, err := db.BeginTx()
	require.NoError(t, err)

	// The chunk autocommits on the pool, so it is durable at once and the
	// tx's later fate is irrelevant to it.
	require.NoError(t, tx.I().SetExecutor(db.Writes).Insert("insert into people", testPerson{ID: 1, Name: "A"}))
	require.Len(t, got, 1, "durable on the pool: fires immediately")
	require.NoError(t, tx.Cancel())
	require.Len(t, got, 1, "and the rollback of the unrelated tx discards nothing")
}

func TestAfterInsert_TxInserterOnCustomExecutorNeverFires(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	mock.ExpectBegin()
	mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	db.AfterInsert = func(InsertEvent) {
		t.Fatal("AfterInsert must not fire for a tx Inserter on an executor it can't vouch for")
	}

	tx, cancel, err := db.BeginTx()
	require.NoError(t, err)
	defer cancel()
	require.NoError(t, tx.I().SetExecutor(passthroughExecutor{tx.Tx}).Insert("insert into people", testPerson{ID: 1, Name: "A"}))
	require.NoError(t, tx.Commit())
}

func TestAfterInsert_PublishedBeforeCommitLogPanics(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	mock.ExpectBegin()
	mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	var got []InsertEvent
	db.AfterInsert = func(ev InsertEvent) { got = append(got, ev) }

	tx, cancel, err := db.BeginTx()
	require.NoError(t, err)
	defer cancel()
	require.NoError(t, tx.Insert("insert into people", testPerson{ID: 1, Name: "A"}))

	// The driver committed before Log runs; a panicking logger must not
	// swallow the event.
	db.Log = func(detail LogDetail) {
		if detail.Query == "commit" {
			panic("commit logger")
		}
	}
	require.Panics(t, func() { _ = tx.Commit() })
	require.Len(t, got, 1)
}
