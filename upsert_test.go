package mysql

import (
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

// test struct reused
// (use testPerson from insert_test.go maybe same package so accessible?)

func getTestDatabase(t *testing.T) (*Database, sqlmock.Sqlmock, func()) {
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)

	mock.ExpectQuery("^SELECT @@max_allowed_packet$").
		WillReturnRows(sqlmock.NewRows([]string{"@@max_allowed_packet"}).AddRow(int64(4194304)))

	db, err := NewFromConn(mockDB, mockDB)
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, mock.ExpectationsWereMet())
		mockDB.Close()
	}

	return db, mock, cleanup
}

// TestUpsertUpdateOnly verifies that when the UPDATE affects a row no INSERT is issued.
func TestUpsertUpdateOnly(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	p := testPerson{ID: 1, Name: "Alice"}

	updateQ := "update people set `name`=@@name where `id`<=>@@id"
	replaced, _, err := db.InterpolateParams(updateQ, p)
	require.NoError(t, err)

	mock.ExpectExec(regexp.QuoteMeta(replaced)).WillReturnResult(sqlmock.NewResult(0, 1))

	err = db.Upsert("people", []string{"id"}, []string{"name"}, "", p)
	require.NoError(t, err)
}

// TestUpsertUpdateWithInsertAndWhere ensures an INSERT occurs when the UPDATE affects no rows and a where clause is supplied.
func TestUpsertUpdateWithInsertAndWhere(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	p := testPerson{ID: 2, Name: "Bob"}

	updateQ := "update people set `name`=@@name where `id`<=>@@id and (deleted=0)"
	replacedUpdate, _, err := db.InterpolateParams(updateQ, p)
	require.NoError(t, err)

	insertQ := "insert into`people`(`id`,`name`)values(@@id,@@name)"
	replacedInsert, _, err := db.InterpolateParams(insertQ, p)
	require.NoError(t, err)

	mock.ExpectExec(regexp.QuoteMeta(replacedUpdate)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(replacedInsert)).WillReturnResult(sqlmock.NewResult(1, 1))

	err = db.Upsert("people", []string{"id"}, []string{"name"}, "deleted=0", p)
	require.NoError(t, err)
}

// TestUpsertExistsNoInsert checks the path where updateColumns are not supplied and an existing row is found.
func TestUpsertExistsNoInsert(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	p := testPerson{ID: 3, Name: "Carl"}

	existsQ := "select 0 from people where `id`<=>@@id"
	replacedExists, _, err := db.InterpolateParams(existsQ, p)
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"exists"}).AddRow(1)
	mock.ExpectQuery(regexp.QuoteMeta(replacedExists)).WillReturnRows(rows)

	err = db.Upsert("people", []string{"id"}, nil, "", p)
	require.NoError(t, err)
}

// TestUpsertExistsInsertWithWhere ensures an INSERT occurs when no existing row is found using the where clause.
func TestUpsertExistsInsertWithWhere(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	p := testPerson{ID: 4, Name: "Dave"}

	existsQ := "select 0 from people where `id`<=>@@id and (deleted=0)"
	replacedExists, _, err := db.InterpolateParams(existsQ, p)
	require.NoError(t, err)

	mock.ExpectQuery(regexp.QuoteMeta(replacedExists)).WillReturnRows(sqlmock.NewRows([]string{"exists"}))

	insertQ := "insert into`people`(`id`,`name`)values(@@id,@@name)"
	replacedInsert, _, err := db.InterpolateParams(insertQ, p)
	require.NoError(t, err)

	mock.ExpectExec(regexp.QuoteMeta(replacedInsert)).WillReturnResult(sqlmock.NewResult(1, 1))

	err = db.Upsert("people", []string{"id"}, nil, "deleted=0", p)
	require.NoError(t, err)
}
