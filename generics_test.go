package mysql

import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSelectSliceGeneric(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)

	mock.ExpectQuery("^SELECT @@max_allowed_packet$").
		WillReturnRows(sqlmock.NewRows([]string{"@@max_allowed_packet"}).AddRow(int64(4194304)))
	rows := sqlmock.NewRows([]string{"foo"}).AddRow("a").AddRow("b")
	mock.ExpectQuery("^SELECT foo FROM bar$").WillReturnRows(rows)

	db, err := NewFromConn(mockDB, mockDB)
	require.NoError(t, err)

	res, err := SelectSlice[string](db, "SELECT foo FROM bar", 0)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, res)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSelectOneGeneric(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)

	mock.ExpectQuery("^SELECT @@max_allowed_packet$").
		WillReturnRows(sqlmock.NewRows([]string{"@@max_allowed_packet"}).AddRow(int64(4194304)))
	mock.ExpectQuery(`^SELECT count\(\*\) FROM bar WHERE id=1$`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

	db, err := NewFromConn(mockDB, mockDB)
	require.NoError(t, err)

	val, err := SelectOne[int](db, "SELECT count(*) FROM bar WHERE id=1", 0)
	require.NoError(t, err)
	require.Equal(t, 3, val)

	require.NoError(t, mock.ExpectationsWereMet())
}
