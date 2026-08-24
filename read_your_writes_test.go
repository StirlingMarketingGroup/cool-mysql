package mysql

import (
	"bytes"
	"context"
	"errors"
	"io"
	"regexp"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func expectSelectInt(mock sqlmock.Sqlmock, query string, v int64) {
	mock.ExpectQuery("^" + regexp.QuoteMeta(query) + "$").
		WillReturnRows(sqlmock.NewRows([]string{"col"}).AddRow(v))
}

func TestReadYourWrites_NoPriorWriteUsesReads(t *testing.T) {
	db, _, readsMock, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	expectSelectInt(readsMock, "SELECT 1", 1)

	var v int64
	require.NoError(t, db.Select(&v, "SELECT 1", 0))
	require.Equal(t, int64(1), v)
}

func TestReadYourWrites_ExecThenSelectUsesWrites(t *testing.T) {
	db, writesMock, _, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	writesMock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	expectSelectInt(writesMock, "SELECT 1", 1)

	require.NoError(t, db.Exec("insert into `t` (`a`) values (1)"))
	var v int64
	require.NoError(t, db.Select(&v, "SELECT 1", 0))
	require.Equal(t, int64(1), v)
}

func TestReadYourWrites_SelectAfterWindowUsesReads(t *testing.T) {
	db, writesMock, readsMock, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	db.ReadYourWritesWindow = 10 * time.Millisecond

	writesMock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	expectSelectInt(readsMock, "SELECT 1", 1)

	require.NoError(t, db.Exec("insert into `t` (`a`) values (1)"))
	time.Sleep(30 * time.Millisecond)
	var v int64
	require.NoError(t, db.Select(&v, "SELECT 1", 0))
	require.Equal(t, int64(1), v)
}

func TestReadYourWrites_RunInTxCommitPins(t *testing.T) {
	db, writesMock, _, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	writesMock.ExpectBegin()
	writesMock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	writesMock.ExpectCommit()
	expectSelectInt(writesMock, "SELECT 1", 1)

	require.NoError(t, db.RunInTx(context.Background(), func(ctx context.Context) error {
		return TxFromContext(ctx).Exec("insert into `t` (`a`) values (1)")
	}))
	var v int64
	require.NoError(t, db.Select(&v, "SELECT 1", 0))
	require.Equal(t, int64(1), v)
}

func TestReadYourWrites_RunInTxRollbackDoesNotPin(t *testing.T) {
	db, writesMock, readsMock, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	writesMock.ExpectBegin()
	writesMock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	writesMock.ExpectRollback()
	expectSelectInt(readsMock, "SELECT 1", 1)

	err := db.RunInTx(context.Background(), func(ctx context.Context) error {
		if err := TxFromContext(ctx).Exec("insert into `t` (`a`) values (1)"); err != nil {
			return err
		}
		return errors.New("abort")
	})
	require.EqualError(t, err, "abort")

	var v int64
	require.NoError(t, db.Select(&v, "SELECT 1", 0))
	require.Equal(t, int64(1), v)
}

func TestReadYourWrites_BeginReadsTxDoesNotPin(t *testing.T) {
	db, _, readsMock, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	readsMock.ExpectBegin()
	readsMock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	readsMock.ExpectCommit()
	expectSelectInt(readsMock, "SELECT 1", 1)

	tx, cancel, err := db.BeginReadsTx()
	require.NoError(t, err)
	defer cancel()
	require.NoError(t, tx.Exec("insert into `t` (`a`) values (1)"))
	require.NoError(t, tx.Commit())

	var v int64
	require.NoError(t, db.Select(&v, "SELECT 1", 0))
	require.Equal(t, int64(1), v)
}

func TestReadYourWrites_CloneSharesMarker(t *testing.T) {
	db, writesMock, _, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	writesMock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	expectSelectInt(writesMock, "SELECT 1", 1)

	require.NoError(t, db.Exec("insert into `t` (`a`) values (1)"))
	clone := db.Clone()
	var v int64
	require.NoError(t, clone.Select(&v, "SELECT 1", 0))
	require.Equal(t, int64(1), v)
}

func TestReadYourWrites_InsertReadsDoesNotPin(t *testing.T) {
	db, _, readsMock, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	const insert = "insert into`t`(`id`)values(1)"
	readsMock.ExpectExec("^" + regexp.QuoteMeta(insert) + "$").
		WillReturnResult(sqlmock.NewResult(1, 1))
	expectSelectInt(readsMock, "SELECT 1", 1)

	require.NoError(t, db.InsertReads("t", struct {
		ID int `mysql:"id"`
	}{ID: 1}))

	var v int64
	require.NoError(t, db.Select(&v, "SELECT 1", 0))
	require.Equal(t, int64(1), v)
}

func TestReadYourWrites_WindowZeroDisables(t *testing.T) {
	db, writesMock, readsMock, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	db.ReadYourWritesWindow = 0

	writesMock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	expectSelectInt(readsMock, "SELECT 1", 1)

	require.NoError(t, db.Exec("insert into `t` (`a`) values (1)"))
	var v int64
	require.NoError(t, db.Select(&v, "SELECT 1", 0))
	require.Equal(t, int64(1), v)
}

func TestReadYourWrites_ExistsAndCountUseWrites(t *testing.T) {
	db, writesMock, _, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	writesMock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	expectSelectInt(writesMock, "SELECT 1", 1)
	expectSelectInt(writesMock, "SELECT 1", 1)

	require.NoError(t, db.Exec("insert into `t` (`a`) values (1)"))

	exists, err := db.Exists("SELECT 1", 0)
	require.NoError(t, err)
	require.True(t, exists)

	n, err := db.Count("SELECT 1", 0)
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

func TestReadYourWrites_BypassesCache(t *testing.T) {
	db, writesMock, readsMock, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	db.UseCache(NewWeakCache())

	type Row struct {
		Name string
	}
	const query = "select name from t"
	cacheDuration := time.Minute

	readsMock.ExpectQuery("^" + regexp.QuoteMeta(query) + "$").
		WillReturnRows(sqlmock.NewRows([]string{"Name"}).AddRow("from-reads"))
	writesMock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	writesMock.ExpectQuery("^" + regexp.QuoteMeta(query) + "$").
		WillReturnRows(sqlmock.NewRows([]string{"Name"}).AddRow("from-writes"))

	var first []Row
	require.NoError(t, db.Select(&first, query, cacheDuration))
	require.Equal(t, []Row{{Name: "from-reads"}}, first)

	require.NoError(t, db.Exec("insert into `t` (`a`) values (1)"))

	var second []Row
	require.NoError(t, db.Select(&second, query, cacheDuration))
	require.Equal(t, []Row{{Name: "from-writes"}}, second)
}

func TestReadYourWrites_SelectRowsUsesWrites(t *testing.T) {
	db, writesMock, _, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	writesMock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	expectSelectInt(writesMock, "SELECT 1", 1)

	require.NoError(t, db.Exec("insert into `t` (`a`) values (1)"))
	rows, err := db.SelectRows("SELECT 1", 0)
	require.NoError(t, err)
	require.Len(t, rows, 1)
}

// A Writes sink that isn't a *sql.DB (NewWriter / NewLocalWriter render SQL
// instead of executing it) can't serve reads, so a marked write must still
// route to Reads rather than panic or hand out a non-pool.
func TestReadYourWrites_NonPoolWritesFallsBackToReads(t *testing.T) {
	db, _, readsMock, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	db.Writes = &writer{Writer: io.Discard}
	db.markWrite()
	expectSelectInt(readsMock, "SELECT 1", 1)

	var v int64
	require.NoError(t, db.Select(&v, "SELECT 1", 0))
	require.Equal(t, int64(1), v)
}

func withPackageReadYourWritesWindow(t *testing.T, d time.Duration) {
	t.Helper()
	orig := ReadYourWritesWindow
	ReadYourWritesWindow = d
	t.Cleanup(func() { ReadYourWritesWindow = orig })
}

// WeakCache holds values in weak.Pointer; an Insert/Exec between two Selects
// can allocate enough to collect the first Select's blob and look like a pin.
// These tests disable GC so a cache hit is actually evidence of no pin.
func disableGC(t *testing.T) {
	t.Helper()
	old := debug.SetGCPercent(-1)
	t.Cleanup(func() { debug.SetGCPercent(old) })
}

type rywCacheRow struct {
	Name string
}

const rywCacheQuery = "select name from t"

func expectRywCacheQuery(mock sqlmock.Sqlmock, name string) {
	mock.ExpectQuery("^" + regexp.QuoteMeta(rywCacheQuery) + "$").
		WillReturnRows(sqlmock.NewRows([]string{"Name"}).AddRow(name))
}

func expectInsertID(mock sqlmock.Sqlmock) {
	const insert = "insert into`t`(`id`)values(1)"
	mock.ExpectExec("^" + regexp.QuoteMeta(insert) + "$").
		WillReturnResult(sqlmock.NewResult(1, 1))
}

func insertIDRow() struct {
	ID int `mysql:"id"`
} {
	return struct {
		ID int `mysql:"id"`
	}{ID: 1}
}

func TestReadYourWrites_SharedPoolInsertReadsDoesNotInvalidateCache(t *testing.T) {
	disableGC(t)
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	require.Same(t, db.Writes, db.Reads)

	db.UseCache(NewWeakCache())
	cacheDuration := time.Minute

	expectRywCacheQuery(mock, "cached")
	expectInsertID(mock)
	expectInsertID(mock)

	var first []rywCacheRow
	require.NoError(t, db.Select(&first, rywCacheQuery, cacheDuration))
	require.Equal(t, []rywCacheRow{{Name: "cached"}}, first)

	require.NoError(t, db.InsertReads("t", insertIDRow()))
	require.NoError(t, db.InsertReadsContext(context.Background(), "t", insertIDRow()))
	require.Zero(t, db.lastWrite.Load())

	var second []rywCacheRow
	require.NoError(t, db.Select(&second, rywCacheQuery, cacheDuration))
	require.Equal(t, []rywCacheRow{{Name: "cached"}}, second)
}

func TestReadYourWrites_SharedPoolBeginReadsTxDoesNotInvalidateCache(t *testing.T) {
	disableGC(t)
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	db.UseCache(NewWeakCache())
	cacheDuration := time.Minute

	expectRywCacheQuery(mock, "cached")
	mock.ExpectBegin()
	mock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	var first []rywCacheRow
	require.NoError(t, db.Select(&first, rywCacheQuery, cacheDuration))
	require.Equal(t, []rywCacheRow{{Name: "cached"}}, first)

	tx, cancel, err := db.BeginReadsTx()
	require.NoError(t, err)
	defer cancel()
	require.NoError(t, tx.Exec("insert into `t` (`a`) values (1)"))
	require.NoError(t, tx.Commit())
	require.Zero(t, db.lastWrite.Load())

	var second []rywCacheRow
	require.NoError(t, db.Select(&second, rywCacheQuery, cacheDuration))
	require.Equal(t, []rywCacheRow{{Name: "cached"}}, second)
}

func TestReadYourWrites_SharedPoolExecInvalidatesCache(t *testing.T) {
	disableGC(t)
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	db.UseCache(NewWeakCache())
	cacheDuration := time.Minute

	expectRywCacheQuery(mock, "cached")
	mock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	expectRywCacheQuery(mock, "fresh")

	var first []rywCacheRow
	require.NoError(t, db.Select(&first, rywCacheQuery, cacheDuration))
	require.Equal(t, []rywCacheRow{{Name: "cached"}}, first)

	require.NoError(t, db.Exec("insert into `t` (`a`) values (1)"))
	require.NotZero(t, db.lastWrite.Load())

	var second []rywCacheRow
	require.NoError(t, db.Select(&second, rywCacheQuery, cacheDuration))
	require.Equal(t, []rywCacheRow{{Name: "fresh"}}, second)
}

func TestReadYourWrites_SetExecutorDoesNotSetTxWrote(t *testing.T) {
	db, writesMock, readsMock, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	writesMock.ExpectBegin()
	expectInsertID(readsMock)
	writesMock.ExpectCommit()
	expectSelectInt(readsMock, "SELECT 1", 1)

	tx, cancel, err := db.BeginTx()
	require.NoError(t, err)
	defer cancel()
	require.NoError(t, tx.I().SetExecutor(db.Reads).Insert("t", insertIDRow()))
	require.NoError(t, tx.Commit())

	var v int64
	require.NoError(t, db.Select(&v, "SELECT 1", 0))
	require.Equal(t, int64(1), v)
}

func TestReadYourWrites_RunInTxRetryThenSelectUsesWrites(t *testing.T) {
	withMaxAttempts(t, 3)

	db, writesMock, _, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	writesMock.ExpectBegin()
	writesMock.ExpectExec("insert into `t`").WillReturnError(errTestDeadlock)
	writesMock.ExpectRollback()
	writesMock.ExpectBegin()
	writesMock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	writesMock.ExpectCommit()
	expectSelectInt(writesMock, "SELECT 1", 1)

	var calls int
	require.NoError(t, db.RunInTx(context.Background(), func(ctx context.Context) error {
		calls++
		return TxFromContext(ctx).Exec("insert into `t` (`a`) values (1)")
	}))
	require.Equal(t, 2, calls)

	var v int64
	require.NoError(t, db.Select(&v, "SELECT 1", 0))
	require.Equal(t, int64(1), v)
}

func TestReadYourWrites_ConcurrentExecSelect(t *testing.T) {
	db, writesMock, readsMock, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	writesMock.MatchExpectationsInOrder(false)
	readsMock.MatchExpectationsInOrder(false)

	const n = 8
	for range n {
		writesMock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
		expectSelectInt(writesMock, "SELECT 1", 1)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := db.Exec("insert into `t` (`a`) values (1)"); err != nil {
				errCh <- err
				return
			}
			var v int64
			errCh <- db.Select(&v, "SELECT 1", 0)
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
}

func TestReadYourWrites_ReconnectKeepsMarkerOnNewWritesPool(t *testing.T) {
	mo := installMockOpen(t)

	db, err := NewFromDSNDualPool(testDSN)
	require.NoError(t, err)

	mo.mocks[0].ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	require.NoError(t, db.Exec("insert into `t` (`a`) values (1)"))

	mo.mocks[0].ExpectClose()
	mo.mocks[1].ExpectClose()
	require.NoError(t, db.Reconnect())
	require.GreaterOrEqual(t, len(mo.mocks), 3)

	expectSelectInt(mo.mocks[2], "SELECT 1", 1)
	var v int64
	require.NoError(t, db.Select(&v, "SELECT 1", 0))
	require.Equal(t, int64(1), v)
	require.NoError(t, mo.mocks[0].ExpectationsWereMet())
	require.NoError(t, mo.mocks[1].ExpectationsWereMet())
	require.NoError(t, mo.mocks[2].ExpectationsWereMet())
}

func TestReadYourWrites_ConstructorsSeedWindowAndMarker(t *testing.T) {
	const want = 42 * time.Second
	withPackageReadYourWritesWindow(t, want)

	assertSeeded := func(t *testing.T, db *Database) {
		t.Helper()
		require.Equal(t, want, db.ReadYourWritesWindow)
		require.NotNil(t, db.lastWrite)
	}

	t.Run("NewFromDSN", func(t *testing.T) {
		installMockOpen(t)
		db, err := NewFromDSN(testDSN, testDSN)
		require.NoError(t, err)
		assertSeeded(t, db)
	})

	t.Run("NewFromDSNDualPool", func(t *testing.T) {
		installMockOpen(t)
		db, err := NewFromDSNDualPool(testDSN)
		require.NoError(t, err)
		assertSeeded(t, db)
	})

	t.Run("NewFromConn", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		require.NoError(t, err)
		t.Cleanup(func() { _ = mockDB.Close() })
		mock.ExpectQuery("^SELECT @@max_allowed_packet$").
			WillReturnRows(sqlmock.NewRows([]string{"@@max_allowed_packet"}).
				AddRow(int64(4194304)))
		db, err := NewFromConn(mockDB, mockDB)
		require.NoError(t, err)
		assertSeeded(t, db)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("NewWriter", func(t *testing.T) {
		db, err := NewWriter(&bytes.Buffer{})
		require.NoError(t, err)
		assertSeeded(t, db)
	})

	t.Run("NewLocalWriter", func(t *testing.T) {
		db, err := NewLocalWriter(t.TempDir())
		require.NoError(t, err)
		assertSeeded(t, db)
	})
}

func TestReadYourWrites_SelectContextAndExistsContextUseWrites(t *testing.T) {
	db, writesMock, _, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	writesMock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	expectSelectInt(writesMock, "SELECT 1", 1)
	expectSelectInt(writesMock, "SELECT 1", 1)

	require.NoError(t, db.Exec("insert into `t` (`a`) values (1)"))

	var v int64
	require.NoError(t, db.SelectContext(context.Background(), &v, "SELECT 1", 0))
	require.Equal(t, int64(1), v)

	exists, err := db.ExistsContext(context.Background(), "SELECT 1", 0)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestReadYourWrites_NilLastWriteDoesNotPin(t *testing.T) {
	db, _, readsMock, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	db.lastWrite = nil
	expectSelectInt(readsMock, "SELECT 1", 1)

	var v int64
	require.NoError(t, db.Select(&v, "SELECT 1", 0))
	require.Equal(t, int64(1), v)
}

func TestReadYourWrites_BeginReadsTxContextDoesNotPin(t *testing.T) {
	db, _, readsMock, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	readsMock.ExpectBegin()
	readsMock.ExpectExec("insert into `t`").WillReturnResult(sqlmock.NewResult(1, 1))
	readsMock.ExpectCommit()
	expectSelectInt(readsMock, "SELECT 1", 1)

	tx, cancel, err := db.BeginReadsTxContext(context.Background())
	require.NoError(t, err)
	defer cancel()
	require.NoError(t, tx.Exec("insert into `t` (`a`) values (1)"))
	require.NoError(t, tx.Commit())

	var v int64
	require.NoError(t, db.Select(&v, "SELECT 1", 0))
	require.Equal(t, int64(1), v)
}

// A stalled markWrite (clock sampled, then preempted past a newer write's
// store) must not drag the marker backward and shorten that write's window.
func TestReadYourWrites_MarkWriteNeverMovesBackward(t *testing.T) {
	db, _, _, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	future := int64(time.Since(rywEpoch)) + int64(time.Hour)
	db.lastWrite.Store(future)
	db.markWrite()
	require.Equal(t, future, db.lastWrite.Load())
}

// A deadlock through a tx-created Inserter with a custom executor must stay
// tx-fatal (#167): SetExecutor drops provenance, not the tx lifecycle. A
// statement retry here would run in autocommit and strand phantom rows.
func TestReadYourWrites_SetExecutorKeepsTxDeadlockFatal(t *testing.T) {
	db, writesMock, _, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	writesMock.ExpectBegin()
	writesMock.ExpectExec("insert into`t`").WillReturnError(errTestDeadlock)
	writesMock.ExpectRollback()

	tx, cancel, err := db.BeginTx()
	require.NoError(t, err)
	defer cancel()

	err = tx.I().SetExecutor(tx.Tx).Insert("t", insertIDRow())
	require.ErrorIs(t, err, errTestDeadlock)
	require.NoError(t, tx.Cancel())
	// Exactly one exec attempt — a second would have shown up as an
	// unmet/unexpected expectation on the mock.
	require.NoError(t, writesMock.ExpectationsWereMet())
}
