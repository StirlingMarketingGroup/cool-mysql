package mysql

import (
	"bytes"
	"database/sql/driver"
	"encoding/hex"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Helper struct used across tests
type testPerson struct {
	ID   int    `mysql:"id"`
	Name string `mysql:"name"`
}

func Test_colNamesFromMap(t *testing.T) {
	m := map[string]int{"a": 1, "b": 2}
	cols := colNamesFromMap(reflect.ValueOf(m))
	sort.Strings(cols)
	require.Equal(t, []string{"a", "b"}, cols)
}

func Test_colNamesFromStruct(t *testing.T) {
	type example struct {
		A int    `mysql:"a,insertDefault"`
		B string `mysql:"b,omitempty"`
		C int    `mysql:"-"`
		D int
	}

	cols, opts, fieldMap, err := colNamesFromStruct(reflect.TypeOf(example{}))
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "D"}, cols)
	require.True(t, opts["a"].insertDefault)
	require.True(t, opts["b"].insertDefault)
	require.False(t, opts["D"].insertDefault)
	require.Equal(t, "A", fieldMap["a"])
	require.Equal(t, "b", cols[1])
}

func Test_colNamesFromQuery(t *testing.T) {
	q := "insert into foo (`a`,`b`,c) values"
	cols := colNamesFromQuery(parseQuery(q))
	require.Equal(t, []string{"a", "b", "c"}, cols)
}

func TestInsert_StructSingleRow(t *testing.T) {
	var buf bytes.Buffer
	db, err := NewWriter(&buf)
	require.NoError(t, err)

	err = db.Insert("insert into people on duplicate key update `name`=values(`name`)", testPerson{ID: 1, Name: "Alice"})
	require.NoError(t, err)

	expected := "insert into people (`id`,`name`)values(1,_utf8mb4 0x" + hex.EncodeToString([]byte("Alice")) + " collate utf8mb4_unicode_ci)on duplicate key update `name`=values(`name`);\n\n"
	require.Equal(t, expected, buf.String())
}

func TestInsert_StructSliceWithCallbacks(t *testing.T) {
	var buf bytes.Buffer
	db, err := NewWriter(&buf)
	require.NoError(t, err)

	row, chunk := 0, 0
	ins := db.I().SetAfterRowExec(func(time.Time) { row++ }).SetAfterChunkExec(func(time.Time) { chunk++ })

	err = ins.Insert("people", []testPerson{{1, "A"}, {2, "B"}})
	require.NoError(t, err)

	expected := "insert into`people`(`id`,`name`)values(1,_utf8mb4 0x" + hex.EncodeToString([]byte("A")) + " collate utf8mb4_unicode_ci),(2,_utf8mb4 0x" + hex.EncodeToString([]byte("B")) + " collate utf8mb4_unicode_ci);\n\n"
	require.Equal(t, 2, row)
	require.Equal(t, 1, chunk)
	require.Equal(t, expected, buf.String())
}

func TestInsert_OmitDefault(t *testing.T) {
	var buf bytes.Buffer
	db, err := NewWriter(&buf)
	require.NoError(t, err)

	type s struct {
		ID   int    `mysql:"id"`
		Name string `mysql:"name,omitempty"`
	}

	err = db.Insert("people", s{ID: 1})
	require.NoError(t, err)

	expected := "insert into`people`(`id`,`name`)values(1,default);\n\n"
	require.Equal(t, expected, buf.String())
}

func TestInsert_Map(t *testing.T) {
	var buf bytes.Buffer
	db, err := NewWriter(&buf)
	require.NoError(t, err)

	data := map[string]any{"id": 3, "name": "Carl"}
	err = db.Insert("insert into people (`id`,`name`)", data)
	require.NoError(t, err)

	expected := "insert into people (`id`,`name`)values(3,_utf8mb4 0x" + hex.EncodeToString([]byte("Carl")) + " collate utf8mb4_unicode_ci);\n\n"
	require.Equal(t, expected, buf.String())
}

func TestInsert_DefaultZero(t *testing.T) {
	var buf bytes.Buffer
	db, err := NewWriter(&buf)
	require.NoError(t, err)

	type s struct {
		A int `mysql:"a,defaultzero"`
	}

	err = db.Insert("t", s{})
	require.NoError(t, err)

	expected := "insert into`t`(`a`)values(default(`a`));\n\n"
	require.Equal(t, expected, buf.String())
}

func TestInsert_DefaultZero_NotZero(t *testing.T) {
	var buf bytes.Buffer
	db, err := NewWriter(&buf)
	require.NoError(t, err)

	type s struct {
		A int `mysql:"a,defaultzero"`
	}

	err = db.Insert("t", s{A: 1})
	require.NoError(t, err)

	expected := "insert into`t`(`a`)values(1);\n\n"
	require.Equal(t, expected, buf.String())
}

type zeroer struct {
	Bool bool
	Set  bool
}

func (z zeroer) IsZero() bool {
	return !z.Set
}

func (z zeroer) Value() (driver.Value, error) {
	if !z.Set {
		return nil, nil
	}
	return z.Bool, nil
}

func TestInsert_DefaultZero_Zeroer(t *testing.T) {
	var buf bytes.Buffer
	db, err := NewWriter(&buf)
	require.NoError(t, err)

	type s struct {
		A zeroer `mysql:"a,defaultzero"`
	}

	err = db.Insert("t", s{})
	require.NoError(t, err)

	expected := "insert into`t`(`a`)values(default(`a`));\n\n"
	require.Equal(t, expected, buf.String())
}

func TestInsert_DefaultZero_ZeroerSetToFalse(t *testing.T) {
	var buf bytes.Buffer
	db, err := NewWriter(&buf)
	require.NoError(t, err)

	type s struct {
		A zeroer `mysql:"a,defaultzero"`
	}

	err = db.Insert("t", s{A: zeroer{Bool: false, Set: true}})
	require.NoError(t, err)

	expected := "insert into`t`(`a`)values(0);\n\n"
	require.Equal(t, expected, buf.String())
}

func TestInsert_ErrNoColumnNames(t *testing.T) {
	var buf bytes.Buffer
	db, err := NewWriter(&buf)
	require.NoError(t, err)

	err = db.Insert("table", 1)
	require.ErrorIs(t, err, ErrNoColumnNames)
}
