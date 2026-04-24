package mysql

import (
	"bytes"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestDatabaseClose_SamePool(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)

	mock.ExpectQuery("^SELECT @@max_allowed_packet$").
		WillReturnRows(sqlmock.NewRows([]string{"@@max_allowed_packet"}).
			AddRow(int64(4194304)))
	mock.ExpectClose()

	db, err := NewFromConn(mockDB, mockDB)
	require.NoError(t, err)

	require.NoError(t, db.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDatabaseClose_DistinctPools(t *testing.T) {
	writesDB, writesMock, err := sqlmock.New()
	require.NoError(t, err)

	readsDB, readsMock, err := sqlmock.New()
	require.NoError(t, err)

	writesMock.ExpectQuery("^SELECT @@max_allowed_packet$").
		WillReturnRows(sqlmock.NewRows([]string{"@@max_allowed_packet"}).
			AddRow(int64(4194304)))
	writesMock.ExpectClose()
	readsMock.ExpectClose()

	db, err := NewFromConn(writesDB, readsDB)
	require.NoError(t, err)

	require.NoError(t, db.Close())
	require.NoError(t, writesMock.ExpectationsWereMet())
	require.NoError(t, readsMock.ExpectationsWereMet())
}

func TestDatabaseClose_SqlWriterHandler(t *testing.T) {
	db, err := NewLocalWriter(t.TempDir())
	require.NoError(t, err)

	// Reads is nil and Writes is an *sqlWriter — Close should be a no-op.
	require.NoError(t, db.Close())
}

func TestDatabaseClose_WriterHandler(t *testing.T) {
	db, err := NewWriter(&bytes.Buffer{})
	require.NoError(t, err)

	// Reads is nil and Writes is a *writer — Close should be a no-op.
	require.NoError(t, db.Close())
}

func TestDatabaseClose_AggregatesErrors(t *testing.T) {
	writesDB, writesMock, err := sqlmock.New()
	require.NoError(t, err)

	readsDB, readsMock, err := sqlmock.New()
	require.NoError(t, err)

	writesMock.ExpectQuery("^SELECT @@max_allowed_packet$").
		WillReturnRows(sqlmock.NewRows([]string{"@@max_allowed_packet"}).
			AddRow(int64(4194304)))

	writesCloseErr := errors.New("writes pool close failed")
	readsCloseErr := errors.New("reads pool close failed")
	writesMock.ExpectClose().WillReturnError(writesCloseErr)
	readsMock.ExpectClose().WillReturnError(readsCloseErr)

	db, err := NewFromConn(writesDB, readsDB)
	require.NoError(t, err)

	err = db.Close()
	require.Error(t, err)
	require.ErrorIs(t, err, writesCloseErr)
	require.ErrorIs(t, err, readsCloseErr)

	// The joined error must contain exactly the two close errors — any
	// extra would mean Close called something it shouldn't have.
	joined, ok := err.(interface{ Unwrap() []error })
	require.True(t, ok, "expected errors.Join result")
	require.Len(t, joined.Unwrap(), 2)
}
