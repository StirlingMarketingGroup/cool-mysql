package mysql

import (
	"encoding/hex"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

// upsert tests reuse the helper function and the testPerson struct defined in
// other test files. getTestDatabase is declared in select_test.go, so we use
// that here rather than re-defining it.

// TestUpsertUpdateOnly verifies that when the UPDATE affects a row no INSERT is issued.
func TestUpsertUpdateOnly(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	p := testPerson{ID: 1, Name: "Alice"}

	updateQ := "update `people` set`name`=@@name where`id`<=>@@id"
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

	updateQ := "update `people` set`name`=@@name where`id`<=>@@id and(deleted=0)"
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

	existsQ := "select 0 from `people` where`id`<=>@@id"
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

	existsQ := "select 0 from `people` where`id`<=>@@id and(deleted=0)"
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

// TestUpsertSliceFieldJSONEncodesOnUpdate is the regression test for issue
// #155: a struct field that's a non-[]byte slice targeting a JSON column must
// JSON-encode on the UPDATE path the same way it does on the INSERT path.
// Before the fix, the UPDATE path emitted comma-separated values and produced
// `update foos set Permissions=_utf8mb4 0x... ,_utf8mb4 0x...` — invalid SQL
// against a JSON column.
func TestUpsertSliceFieldJSONEncodesOnUpdate(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	type foo struct {
		FooID       uint
		Name        string
		Permissions []string
	}

	row := foo{FooID: 36, Name: "y", Permissions: []string{"w/c"}}

	jsonHex := hex.EncodeToString([]byte(`["w/c"]`))
	nameHex := hex.EncodeToString([]byte("y"))
	wantUpdate := "update `foos` set" +
		"`Name`=_utf8mb4 0x" + nameHex + " collate utf8mb4_unicode_ci," +
		"`Permissions`=_utf8mb4 0x" + jsonHex + " collate utf8mb4_unicode_ci " +
		"where`FooID`<=>36"

	mock.ExpectExec(regexp.QuoteMeta(wantUpdate)).WillReturnResult(sqlmock.NewResult(0, 1))

	err := db.Upsert("foos", []string{"FooID"}, []string{"Name", "Permissions"}, "", row)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestUpsertSliceFieldKeepsINClauseSemantics confirms that the JSON-encoding
// behavior is scoped to non-IN contexts: a `where` predicate using
// `IN (@@field)` against a slice field still expands to a comma-separated list.
func TestUpsertSliceFieldKeepsINClauseSemantics(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	type foo struct {
		FooID       uint
		Name        string
		Permissions []string
	}

	row := foo{FooID: 36, Name: "y", Permissions: []string{"a", "b"}}

	// Confirm via InterpolateParams that an `IN (@@Permissions)` predicate
	// still produces a comma-separated list, not a JSON array.
	check := "select 0 from `foos` where `Permissions` in (@@Permissions)"
	replaced, _, err := db.InterpolateParams(check, row)
	require.NoError(t, err)
	require.Contains(t, replaced, "in (_utf8mb4 0x"+hex.EncodeToString([]byte("a"))+
		" collate utf8mb4_unicode_ci,_utf8mb4 0x"+hex.EncodeToString([]byte("b"))+
		" collate utf8mb4_unicode_ci)")
	// And it must NOT have JSON-encoded the slice as `["a","b"]`.
	require.NotContains(t, replaced, hex.EncodeToString([]byte(`["a","b"]`)))

	// Sanity check: the UPDATE path's @@Permissions (no IN wrapper) DOES
	// JSON-encode.
	updateQ := "update `foos` set`Permissions`=@@Permissions where`FooID`<=>@@FooID"
	updateReplaced, _, err := db.InterpolateParams(updateQ, row)
	require.NoError(t, err)
	require.Contains(t, updateReplaced, hex.EncodeToString([]byte(`["a","b"]`)))

	mock.ExpectExec(regexp.QuoteMeta(updateReplaced)).WillReturnResult(sqlmock.NewResult(0, 1))
	err = db.Upsert("foos", []string{"FooID"}, []string{"Permissions"}, "", row)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestUpsertValueserFieldJSONEncodesOnUpdate is the Upsert-UPDATE-path
// counterpart to issue #161: a struct field whose type implements Valueser
// (e.g. set.Set) must JSON-encode on the UPDATE path, not expand to
// comma-separated values that would produce invalid SQL against a JSON
// column.
func TestUpsertValueserFieldJSONEncodesOnUpdate(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	type foo struct {
		FooID uint
		Name  string
		Tags  orderedValueser
	}

	row := foo{FooID: 36, Name: "y", Tags: orderedValueser{"a", "b"}}

	jsonHex := hex.EncodeToString([]byte(`["a","b"]`))
	nameHex := hex.EncodeToString([]byte("y"))
	wantUpdate := "update `foos` set" +
		"`Name`=_utf8mb4 0x" + nameHex + " collate utf8mb4_unicode_ci," +
		"`Tags`=_utf8mb4 0x" + jsonHex + " collate utf8mb4_unicode_ci " +
		"where`FooID`<=>36"

	mock.ExpectExec(regexp.QuoteMeta(wantUpdate)).WillReturnResult(sqlmock.NewResult(0, 1))

	err := db.Upsert("foos", []string{"FooID"}, []string{"Name", "Tags"}, "", row)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertDefaultZeroUsesColumnName(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	type shipment struct {
		ID        int       `mysql:"id"`
		ShippedAt time.Time `mysql:"_Shipped,defaultzero"`
	}

	row := shipment{ID: 5}

	updateQ := "update `shipments` set`_Shipped`=@@ShippedAt where`id`<=>@@ID"
	replaced, _, err := db.InterpolateParams(updateQ, row)
	require.NoError(t, err)
	require.Contains(t, replaced, "default(`_Shipped`)")

	mock.ExpectExec(regexp.QuoteMeta(replaced)).WillReturnResult(sqlmock.NewResult(0, 1))

	err = db.Upsert("shipments", []string{"id"}, []string{"_Shipped"}, "", row)
	require.NoError(t, err)
}
