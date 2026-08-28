package mysql

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"io"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
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

	cols, opts, fieldMap, err := colNamesFromStruct(reflect.TypeFor[example]())
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

	expected := "insert into`t`(`a`)values(default);\n\n"
	require.Equal(t, expected, buf.String())
}

// TestInsert_DefaultZero_TimeUsesBareDefaultKeyword pins the bare DEFAULT
// keyword for zero defaultzero values in VALUES lists. The DEFAULT(`col`)
// function form evaluates to the ZERO DATE for DEFAULT CURRENT_TIMESTAMP
// columns, which non-strict sql_mode silently inserts as '0000-00-00'.
func TestInsert_DefaultZero_TimeUsesBareDefaultKeyword(t *testing.T) {
	var buf bytes.Buffer
	db, err := NewWriter(&buf)
	require.NoError(t, err)

	type s struct {
		A int       `mysql:"a"`
		B time.Time `mysql:"b,defaultzero"`
	}

	err = db.Insert("t", s{A: 1})
	require.NoError(t, err)

	expected := "insert into`t`(`a`,`b`)values(1,default);\n\n"
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

	expected := "insert into`t`(`a`)values(default);\n\n"
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

// TestInsert_RowBufExceedsCap exercises the rowBufPoolMaxCap discard branch by
// lowering the cap so a single normal-sized row trips it. The test asserts the
// insert still produces correct output after the cap was exceeded (which
// implies the deferred discard ran instead of returning the grown buf to the
// pool).
func TestInsert_RowBufExceedsCap(t *testing.T) {
	defer func(old int) { rowBufPoolMaxCap = old }(rowBufPoolMaxCap)
	rowBufPoolMaxCap = 8

	var buf bytes.Buffer
	db, err := NewWriter(&buf)
	require.NoError(t, err)

	err = db.Insert("people", testPerson{ID: 1, Name: strings.Repeat("x", 64)})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "insert into`people`")
	require.Contains(t, buf.String(), hex.EncodeToString([]byte(strings.Repeat("x", 64))))
}

// TestInsert_InsertBufExceedsCap exercises the insertBufPoolMaxCap discard
// branch. MaxInsertSize is bumped so the chunk threshold lands above the cap,
// then the cap itself is lowered so the full statement buffer trips the
// discard.
func TestInsert_InsertBufExceedsCap(t *testing.T) {
	defer func(old int) { insertBufPoolMaxCap = old }(insertBufPoolMaxCap)
	insertBufPoolMaxCap = 16

	var buf bytes.Buffer
	db, err := NewWriter(&buf)
	require.NoError(t, err)

	err = db.Insert("people", []testPerson{{1, "A"}, {2, "B"}, {3, "C"}})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "(1,")
	require.Contains(t, buf.String(), "(2,")
	require.Contains(t, buf.String(), "(3,")
}

func TestInsert_ErrNoColumnNames(t *testing.T) {
	var buf bytes.Buffer
	db, err := NewWriter(&buf)
	require.NoError(t, err)

	err = db.Insert("table", 1)
	require.ErrorIs(t, err, ErrNoColumnNames)
}

// TestNoInsertTagOption tests that the noinsert option skips fields for inserts
// but still allows them to be used for selects with an explicit column name.
// This is a regression test for issue #133.
func TestNoInsertTagOption(t *testing.T) {
	t.Run("colNamesFromStruct skips noinsert fields", func(t *testing.T) {
		type OrderLineItem struct {
			ID             int    `mysql:"ID"`
			CustomLineItem string `mysql:"LineItem"`
			LineItemModel  string `mysql:"LineItemModel,noinsert"` // should be skipped for insert
		}

		cols, opts, fieldMap, err := colNamesFromStruct(reflect.TypeFor[OrderLineItem]())
		require.NoError(t, err)

		// Should only have ID and LineItem columns, NOT LineItemModel
		require.Equal(t, []string{"ID", "LineItem"}, cols)

		// Should have opts for ID and LineItem only
		require.Contains(t, opts, "ID")
		require.Contains(t, opts, "LineItem")
		require.NotContains(t, opts, "LineItemModel")

		// Field map should also exclude LineItemModel
		require.Equal(t, "ID", fieldMap["ID"])
		require.Equal(t, "CustomLineItem", fieldMap["LineItem"])
		require.NotContains(t, fieldMap, "LineItemModel")
	})

	t.Run("insert skips noinsert fields", func(t *testing.T) {
		var buf bytes.Buffer
		db, err := NewWriter(&buf)
		require.NoError(t, err)

		type Row struct {
			ID    int    `mysql:"id"`
			Name  string `mysql:"name"`
			Extra string `mysql:"extra,noinsert"` // should not appear in insert
		}

		err = db.Insert("test", Row{ID: 1, Name: "Alice", Extra: "ignored"})
		require.NoError(t, err)

		// The insert should NOT include the "extra" column
		require.NotContains(t, buf.String(), "extra")
		require.NotContains(t, buf.String(), "ignored")
		require.Contains(t, buf.String(), "`id`")
		require.Contains(t, buf.String(), "`name`")
	})
}

// orderedValueser is a deterministic Valueser-backed slice type used to
// exercise the Insert / Upsert single-column code paths. The prod incident in
// issue #161 used set.Set[string], which is map-backed and non-deterministic;
// an ordered slice keeps the test stable while preserving the same
// `MySQLValues() ([]driver.Value, error)` shape.
type orderedValueser []string

func (s orderedValueser) MySQLValues() ([]driver.Value, error) {
	out := make([]driver.Value, 0, len(s))
	for _, v := range s {
		out = append(out, v)
	}
	return out, nil
}

// TestInsert_ValueserMultiElementJSONEncodes is the regression test for issue
// #161. A struct field whose type implements Valueser (e.g. set.Set) targets
// a single column on INSERT — it must JSON-encode into one placeholder rather
// than expand to N comma-separated placeholders, which would mis-align row
// values against the column list and trigger MySQL 1136.
func TestInsert_ValueserMultiElementJSONEncodes(t *testing.T) {
	var buf bytes.Buffer
	db, err := NewWriter(&buf)
	require.NoError(t, err)

	type row struct {
		ID   int             `mysql:"id"`
		Tags orderedValueser `mysql:"tags"`
	}

	err = db.Insert("t", row{ID: 1, Tags: orderedValueser{"a", "b", "c"}})
	require.NoError(t, err)

	// Expected: a single placeholder containing the JSON array literal
	// `["a","b","c"]`. Before the fix this would emit three comma-separated
	// values and break alignment with the 2-column header.
	jsonHex := hex.EncodeToString([]byte(`["a","b","c"]`))
	expected := "insert into`t`(`id`,`tags`)values(1,_utf8mb4 0x" + jsonHex + " collate utf8mb4_unicode_ci);\n\n"
	require.Equal(t, expected, buf.String())
}

// TestInsert_ValueserSingleElementJSONEncodes guards against the
// single-element optimization in the existing IN-clause Valueser path bleeding
// into INSERT row values. A 1-element Valueser must still render as a
// 1-element JSON array (`["a"]`), not the bare scalar `'a'`.
func TestInsert_ValueserSingleElementJSONEncodes(t *testing.T) {
	var buf bytes.Buffer
	db, err := NewWriter(&buf)
	require.NoError(t, err)

	type row struct {
		ID   int             `mysql:"id"`
		Tags orderedValueser `mysql:"tags"`
	}

	err = db.Insert("t", row{ID: 1, Tags: orderedValueser{"a"}})
	require.NoError(t, err)

	jsonHex := hex.EncodeToString([]byte(`["a"]`))
	expected := "insert into`t`(`id`,`tags`)values(1,_utf8mb4 0x" + jsonHex + " collate utf8mb4_unicode_ci);\n\n"
	require.Equal(t, expected, buf.String())
}

// unmarshalableValueser returns a value that json.Marshal can't encode
// (channels have no JSON representation), exercising the error branch in the
// single-column Valueser path added in issue #161.
type unmarshalableValueser struct{}

func (unmarshalableValueser) MySQLValues() ([]driver.Value, error) {
	return []driver.Value{make(chan int)}, nil
}

// TestInsert_ValueserJSONMarshalError covers the error branch when
// MySQLValues returns a driver.Value that json.Marshal can't encode. The
// caller's error wrapping must surface so misconfigured user types don't
// fail silently.
func TestInsert_ValueserJSONMarshalError(t *testing.T) {
	var buf bytes.Buffer
	db, err := NewWriter(&buf)
	require.NoError(t, err)

	type row struct {
		ID   int                   `mysql:"id"`
		Tags unmarshalableValueser `mysql:"tags"`
	}

	err = db.Insert("t", row{ID: 1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "JSON-encode Valueser")
}

// TestInsert_ValueserEmptyEncodesAsJSONArray covers the zero-element case: an
// empty (non-nil) Valueser should render as the JSON literal `[]` so the
// column stays a valid JSON value.
func TestInsert_ValueserEmptyEncodesAsJSONArray(t *testing.T) {
	var buf bytes.Buffer
	db, err := NewWriter(&buf)
	require.NoError(t, err)

	type row struct {
		ID   int             `mysql:"id"`
		Tags orderedValueser `mysql:"tags"`
	}

	err = db.Insert("t", row{ID: 1, Tags: orderedValueser{}})
	require.NoError(t, err)

	jsonHex := hex.EncodeToString([]byte(`[]`))
	expected := "insert into`t`(`id`,`tags`)values(1,_utf8mb4 0x" + jsonHex + " collate utf8mb4_unicode_ci);\n\n"
	require.Equal(t, expected, buf.String())
}

func Test_parseInsertPrefix(t *testing.T) {
	tests := []struct {
		query string
		want  insertPrefix
	}{
		{query: "insert into people", want: insertPrefix{kind: insertPlain, table: "people"}},
		{query: "INSERT INTO people (`id`) values (1)", want: insertPrefix{kind: insertPlain, table: "people"}},
		{query: "insert into`people`(`id`)values(1)", want: insertPrefix{kind: insertPlain, table: "people"}},
		{query: "insert into `db`.`people`", want: insertPrefix{kind: insertPlain, table: "db.people"}},
		{query: "insert into audit . people values (1)", want: insertPrefix{kind: insertPlain, table: "audit.people"}},
		{query: "insert into audit /*c*/ . /*c*/ people", want: insertPrefix{kind: insertPlain, table: "audit.people"}},
		{query: "insert into `au``dit`.people", want: insertPrefix{kind: insertPlain, table: "au`dit.people"}},
		{query: "insert into audit$rows (`id`) values (1)", want: insertPrefix{kind: insertPlain, table: "audit$rows"}},
		{query: "insert into `audit$rows` (`id`) values (1)", want: insertPrefix{kind: insertPlain, table: "audit$rows"}},
		{query: "insert into auditéé (`id`) values (1)", want: insertPrefix{kind: insertPlain, table: "auditéé"}},
		{query: "insert into `people.archive`", want: insertPrefix{kind: insertPlain, table: "people.archive"}},
		{query: "insert into `audit/*! ignore */` (`id`) values (1)", want: insertPrefix{kind: insertPlain, table: "audit/*! ignore */"}},
		{query: "insert into --\tc\n people", want: insertPrefix{kind: insertPlain, table: "people"}},
		{query: "insert into --c\n people", want: insertPrefix{}},
		{query: "insert into --\vc\n people", want: insertPrefix{kind: insertPlain, table: "people"}},
		{query: "insert into --\x7fc\n people", want: insertPrefix{kind: insertPlain, table: "people"}},
		{query: "insert into people values (1)", want: insertPrefix{kind: insertPlain, table: "people"}},
		{query: "insert into people set `id`=1", want: insertPrefix{kind: insertPlain, table: "people"}},
		{query: "insert into people;", want: insertPrefix{kind: insertPlain, table: "people"}},
		{query: "insert people (`id`) values (1)", want: insertPrefix{kind: insertPlain, table: "people"}},
		{query: "insert ignore into people", want: insertPrefix{kind: insertIgnore, table: "people"}},
		{query: "INSERT LOW_PRIORITY IGNORE INTO people", want: insertPrefix{kind: insertIgnore, table: "people"}},
		{query: "insert ignore high_priority people", want: insertPrefix{kind: insertIgnore, table: "people"}},
		{query: "insert delayed into people", want: insertPrefix{kind: insertPlain, table: "people"}},
		{query: "/*c*/ insert /*c*/ high_priority /*c*/ into people", want: insertPrefix{kind: insertPlain, table: "people"}},
		{query: "insert -- c\n into # c\n people", want: insertPrefix{kind: insertPlain, table: "people"}},
		{query: "insert into people /*!50100 partition (p0) */ (`id`) values (1)", want: insertPrefix{kind: insertPlain, table: "people"}},
		{query: "insert /*! ignore */ into people", want: insertPrefix{kind: insertIgnore, table: "people"}},
		{query: "insert /*!40000 low_priority */ into people", want: insertPrefix{kind: insertPlain, table: "people"}},
		{query: "insert into audit /*! . */ people", want: insertPrefix{kind: insertPlain, table: "audit.people"}},
		{query: "insert into people /*! unterminated", want: insertPrefix{kind: insertPlain, table: "people"}},
		{query: "replace into people", want: insertPrefix{}},
		{query: "insert into", want: insertPrefix{}},
		{query: "insert into people.", want: insertPrefix{}},
		{query: "insert into ; people", want: insertPrefix{}},
		{query: "insert into (people)", want: insertPrefix{}},
		{query: "insert into `people", want: insertPrefix{}},
		{query: "insert into ``", want: insertPrefix{}},
		{query: "insert /* unterminated", want: insertPrefix{}},
		{query: "insert", want: insertPrefix{}},
		{query: "ignore insert into people", want: insertPrefix{}},
		{query: "", want: insertPrefix{}},
		{query: "`insert` ignore into people", want: insertPrefix{}},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			require.Equal(t, tt.want, parseInsertPrefix(tt.query))
		})
	}
}

// ptrValuerID implements driver.Valuer on the pointer only, so unwrapping it
// loses the method the consumer needs.
type ptrValuerID struct{ n int }

func (p *ptrValuerID) Value() (driver.Value, error) { return int64(p.n * 10), nil }

type definedBytes []byte

func Test_insertEventValue(t *testing.T) {
	value := func(v any) any {
		r := reflect.ValueOf(v)
		return insertEventValue(r, reflectUnwrap(r))
	}
	var nilPtr *string
	var nilMap map[string]int
	require.True(t, insertEventValue(reflect.Value{}, reflect.Value{}) == nil)
	require.True(t, value(nilPtr) == nil, "a nil pointer is NULL, not a typed nil")
	require.True(t, value(nilMap) == nil)
	require.Equal(t, 0, value(0))
	require.Equal(t, 7, value(7))

	buf := []byte{1, 2}
	snap := value(buf).([]byte)
	buf[0] = 9
	require.Equal(t, []byte{1, 2}, snap, "[]byte is snapshotted, not aliased")

	defined := definedBytes{1, 2}
	definedSnap := value(defined).(definedBytes)
	defined[0] = 9
	require.Equal(t, definedBytes{1, 2}, definedSnap, "a defined byte-slice type is snapshotted with its type kept")

	id := &ptrValuerID{n: 4}
	got, isValuer := value(id).(driver.Valuer)
	require.True(t, isValuer, "a pointer-receiver Valuer stays the Valuer the caller supplied")
	dbValue, err := got.Value()
	require.NoError(t, err)
	require.Equal(t, int64(40), dbValue)
}

func TestAfterInsert_StructSlice(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	rows := []testPerson{{1, "A"}, {2, "B"}}
	mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(10, 2))

	var got []InsertEvent
	db.AfterInsert = func(ev InsertEvent) { got = append(got, ev) }

	require.NoError(t, db.Insert("insert into people", rows))
	require.Len(t, got, 1)
	require.Equal(t, "people", got[0].Table)
	require.Equal(t, []string{"id", "name"}, got[0].Columns)
	require.Equal(t, [][]any{{1, "A"}, {2, "B"}}, got[0].Rows)
	id, err := got[0].Result.LastInsertId()
	require.NoError(t, err)
	require.Equal(t, int64(10), id)
	n, err := got[0].Result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(2), n)
}

func TestAfterInsert_FiresPerChunk(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	db.MaxInsertSize.Set(1)

	mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(11, 1))
	mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(12, 1))
	mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(13, 1))

	var got []InsertEvent
	db.AfterInsert = func(ev InsertEvent) { got = append(got, ev) }

	require.NoError(t, db.Insert("insert into people", []testPerson{{1, "A"}, {2, "B"}, {3, "C"}}))
	require.Len(t, got, 3)
	require.Equal(t, [][]any{{1, "A"}}, got[0].Rows)
	require.Equal(t, [][]any{{2, "B"}}, got[1].Rows)
	require.Equal(t, [][]any{{3, "C"}}, got[2].Rows)
	for i, wantID := range []int64{11, 12, 13} {
		id, err := got[i].Result.LastInsertId()
		require.NoError(t, err)
		require.Equal(t, wantID, id)
		require.Equal(t, []string{"id", "name"}, got[i].Columns)
		require.Equal(t, "people", got[i].Table)
	}
}

func TestAfterInsert_RowShapes(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		source    any
		wantTable string
		wantCols  []string
		check     func(t *testing.T, rows [][]any)
	}{
		{
			name:      "map",
			query:     "insert into people (`id`,`name`)",
			source:    map[string]any{"id": 3, "name": "Carl"},
			wantTable: "people",
			wantCols:  []string{"id", "name"},
			check: func(t *testing.T, rows [][]any) {
				require.Equal(t, [][]any{{3, "Carl"}}, rows)
			},
		},
		{
			name:      "map missing column is nil",
			query:     "insert into people (`id`,`name`)",
			source:    map[string]any{"id": 4},
			wantTable: "people",
			wantCols:  []string{"id", "name"},
			check: func(t *testing.T, rows [][]any) {
				require.Len(t, rows, 1)
				require.Equal(t, 4, rows[0][0])
				require.Nil(t, rows[0][1])
			},
		},
		{
			name:      "single value",
			query:     "insert into people (`id`)",
			source:    7,
			wantTable: "people",
			wantCols:  []string{"id"},
			check: func(t *testing.T, rows [][]any) {
				require.Equal(t, [][]any{{7}}, rows)
			},
		},
		{
			name:  "nil pointer field",
			query: "insert into people",
			source: struct {
				ID   int     `mysql:"id"`
				Name *string `mysql:"name"`
			}{ID: 1},
			wantTable: "people",
			wantCols:  []string{"id", "name"},
			check: func(t *testing.T, rows [][]any) {
				require.Len(t, rows, 1)
				require.Equal(t, 1, rows[0][0])
				require.True(t, rows[0][1] == nil, "NULL is an untyped nil, not (*string)(nil)")
			},
		},
		{
			name:  "defaultzero zero renders DEFAULT and carries no value",
			query: "insert into people",
			source: struct {
				ID   int    `mysql:"id,defaultzero"`
				Name string `mysql:"name"`
			}{Name: "A"},
			wantTable: "people",
			wantCols:  []string{"id", "name"},
			check: func(t *testing.T, rows [][]any) {
				require.Equal(t, [][]any{{nil, "A"}}, rows)
			},
		},
		{
			name:      "no INTO",
			query:     "insert people",
			source:    testPerson{ID: 9, Name: "Nine"},
			wantTable: "people",
			wantCols:  []string{"id", "name"},
			check: func(t *testing.T, rows [][]any) {
				require.Equal(t, [][]any{{9, "Nine"}}, rows)
			},
		},
		{
			name:      "slice row",
			query:     "insert into people (`id`,`name`)",
			source:    []any{1, "Alice"},
			wantTable: "people",
			wantCols:  []string{"id", "name"},
			check: func(t *testing.T, rows [][]any) {
				require.Equal(t, [][]any{{1, "Alice"}}, rows)
			},
		},
		{
			name:      "slice row with nil element",
			query:     "insert into people (`id`,`name`)",
			source:    []any{1, nil},
			wantTable: "people",
			wantCols:  []string{"id", "name"},
			check: func(t *testing.T, rows [][]any) {
				require.Len(t, rows, 1)
				require.Equal(t, 1, rows[0][0])
				require.Nil(t, rows[0][1])
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, cleanup := getTestDatabase(t)
			defer cleanup()
			mock.ExpectExec("insert (into )?people").WillReturnResult(sqlmock.NewResult(1, 1))

			var got []InsertEvent
			db.AfterInsert = func(ev InsertEvent) { got = append(got, ev) }

			require.NoError(t, db.Insert(tt.query, tt.source))
			require.Len(t, got, 1)
			require.Equal(t, tt.wantTable, got[0].Table)
			require.Equal(t, tt.wantCols, got[0].Columns)
			tt.check(t, got[0].Rows)
		})
	}
}

func TestAfterInsert_DoesNotFire(t *testing.T) {
	t.Run("insert ignore", func(t *testing.T) {
		db, mock, cleanup := getTestDatabase(t)
		defer cleanup()
		mock.ExpectExec("insert ignore into people").WillReturnResult(sqlmock.NewResult(1, 1))

		db.AfterInsert = func(InsertEvent) { t.Fatal("AfterInsert must not fire for INSERT IGNORE") }
		require.NoError(t, db.Insert("insert ignore into people", testPerson{ID: 1, Name: "A"}))
	})

	t.Run("insert low_priority ignore", func(t *testing.T) {
		db, mock, cleanup := getTestDatabase(t)
		defer cleanup()
		mock.ExpectExec("insert low_priority ignore into people").WillReturnResult(sqlmock.NewResult(1, 1))

		db.AfterInsert = func(InsertEvent) { t.Fatal("AfterInsert must not fire for IGNORE after a priority modifier") }
		require.NoError(t, db.Insert("insert low_priority ignore into people", testPerson{ID: 1, Name: "A"}))
	})

	t.Run("executable comment in the prefix", func(t *testing.T) {
		// MySQL executes `/*! ... */`, so the IGNORE inside it is real.
		db, mock, cleanup := getTestDatabase(t)
		defer cleanup()
		mock.ExpectExec("insert /\\*! ignore \\*/ into people").WillReturnResult(sqlmock.NewResult(1, 1))

		db.AfterInsert = func(InsertEvent) { t.Fatal("AfterInsert must not fire for an executable-comment modifier") }
		require.NoError(t, db.Insert("insert /*! ignore */ into people", testPerson{ID: 1, Name: "A"}))
	})

	t.Run("custom executor wrapper", func(t *testing.T) {
		// Not a pool and not a cool-mysql Tx: nothing this package can vouch
		// for, whatever the wrapper does underneath.
		db, mock, cleanup := getTestDatabase(t)
		defer cleanup()
		mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(1, 1))

		db.AfterInsert = func(InsertEvent) { t.Fatal("AfterInsert must not fire through an unrecognised executor") }
		require.NoError(t, db.I().SetExecutor(passthroughExecutor{db.Writes}).Insert("insert into people", testPerson{ID: 1, Name: "A"}))
	})

	t.Run("SQL renderer executor", func(t *testing.T) {
		db, err := NewWriter(io.Discard)
		require.NoError(t, err)

		db.AfterInsert = func(InsertEvent) { t.Fatal("AfterInsert must not fire through a renderer: nothing became durable") }
		require.NoError(t, db.Insert("insert into people", testPerson{ID: 1, Name: "A"}))
	})

	t.Run("replace into", func(t *testing.T) {
		db, mock, cleanup := getTestDatabase(t)
		defer cleanup()
		mock.ExpectExec("replace into people").WillReturnResult(sqlmock.NewResult(1, 1))

		db.AfterInsert = func(InsertEvent) { t.Fatal("AfterInsert must not fire for REPLACE") }
		require.NoError(t, db.Insert("replace into people", testPerson{ID: 1, Name: "A"}))
	})

	t.Run("on duplicate key update", func(t *testing.T) {
		db, mock, cleanup := getTestDatabase(t)
		defer cleanup()
		mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(1, 1))

		db.AfterInsert = func(InsertEvent) { t.Fatal("AfterInsert must not fire for ON DUPLICATE KEY UPDATE") }
		require.NoError(t, db.Insert(
			"insert into people on duplicate key update `name`=values(`name`)",
			testPerson{ID: 1, Name: "A"},
		))
	})

	t.Run("upsert update hit", func(t *testing.T) {
		db, mock, cleanup := getTestDatabase(t)
		defer cleanup()

		p := testPerson{ID: 1, Name: "Alice"}
		updateQ := "update `people` set`name`=@@name where`id`<=>@@id"
		replaced, _, err := db.InterpolateParams(updateQ, p)
		require.NoError(t, err)
		mock.ExpectExec(regexp.QuoteMeta(replaced)).WillReturnResult(sqlmock.NewResult(0, 1))

		db.AfterInsert = func(InsertEvent) { t.Fatal("AfterInsert must not fire for Upsert's UPDATE path") }
		require.NoError(t, db.Upsert("people", []string{"id"}, []string{"name"}, "", p))
	})

	t.Run("exec error", func(t *testing.T) {
		db, mock, cleanup := getTestDatabase(t)
		defer cleanup()
		mock.ExpectExec("insert into people").WillReturnError(errors.New("boom"))

		db.AfterInsert = func(InsertEvent) { t.Fatal("AfterInsert must not fire when exec errors") }
		err := db.Insert("insert into people", testPerson{ID: 1, Name: "A"})
		require.Error(t, err)
	})
}

func TestAfterInsert_NoTableNameKeepsHookOff(t *testing.T) {
	// A prefix without a table name isn't classified, so the hook stays off
	// and the statement runs exactly as it did before the hook existed (the
	// server is the one that rejects it).
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	mock.ExpectExec("insert into").WillReturnResult(sqlmock.NewResult(1, 1))

	db.AfterInsert = func(InsertEvent) { t.Fatal("AfterInsert must not fire when the table name is unparsable") }
	require.NoError(t, db.Insert("insert into", testPerson{ID: 1, Name: "A"}))
}

func TestAfterInsert_SetExecutor(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(1, 1))

	var got []InsertEvent
	db.AfterInsert = func(ev InsertEvent) { got = append(got, ev) }

	require.NoError(t, db.I().SetExecutor(db.Writes).Insert("insert into people", testPerson{ID: 1, Name: "A"}))
	require.Len(t, got, 1)
	require.Equal(t, "people", got[0].Table)
	require.Equal(t, [][]any{{1, "A"}}, got[0].Rows)
}

func TestAfterInsert_InsertDefaultZeroIsNil(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(5, 1))

	var got []InsertEvent
	db.AfterInsert = func(ev InsertEvent) { got = append(got, ev) }

	// A zero insertDefault column is sent as DEFAULT, so the event carries no
	// value for it — the same visit produces both.
	require.NoError(t, db.Insert("insert into people", struct {
		ID   int    `mysql:"id,insertDefault"`
		Name string `mysql:"name"`
	}{Name: "A"}))
	require.Len(t, got, 1)
	require.Equal(t, []string{"id", "name"}, got[0].Columns)
	require.Equal(t, [][]any{{nil, "A"}}, got[0].Rows)
}

func TestAfterInsert_FiresBeforeInserterCallbacksPanic(t *testing.T) {
	for _, tt := range []struct {
		name string
		arm  func(in *Inserter)
	}{
		{name: "AfterChunkExec", arm: func(in *Inserter) { in.SetAfterChunkExec(func(time.Time) { panic("chunk callback") }) }},
		{name: "HandleResult", arm: func(in *Inserter) { in.SetResultHandler(func(sql.Result) { panic("result callback") }) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, cleanup := getTestDatabase(t)
			defer cleanup()
			mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(1, 1))

			var got []InsertEvent
			db.AfterInsert = func(ev InsertEvent) { got = append(got, ev) }

			in := db.I()
			tt.arm(in)
			require.Panics(t, func() { _ = in.Insert("insert into people", testPerson{ID: 1, Name: "A"}) })
			require.Len(t, got, 1, "the row was durable before the callback panicked; the hook must have seen it")
		})
	}
}

func TestAfterInsert_InsertDefaultZeroerIsNil(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(1, 1))

	var got []InsertEvent
	db.AfterInsert = func(ev InsertEvent) { got = append(got, ev) }

	// Both Zeroer routes to DEFAULT — a nil pointer to a type with IsZero and
	// a zero value that reports IsZero — carry no value in the event.
	require.NoError(t, db.Insert("insert into people", struct {
		When *time.Time `mysql:"when,insertDefault"`
		At   time.Time  `mysql:"at,insertDefault"`
		Name string     `mysql:"name"`
	}{Name: "A"}))
	require.Len(t, got, 1)
	require.Equal(t, []string{"when", "at", "name"}, got[0].Columns)
	require.Equal(t, [][]any{{nil, nil, "A"}}, got[0].Rows)
}

func TestAfterInsert_OpaqueSQLTxExecutorNeverFires(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()
	mock.ExpectBegin()
	mock.ExpectExec("insert into people").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectRollback()

	// A *sql.Tx the caller commits or rolls back on their own is opaque to
	// this package: nothing can vouch for the row, so no event — Tx.I() is
	// the route to commit-time publication.
	rawTx, err := db.Writes.(*sql.DB).Begin()
	require.NoError(t, err)

	db.AfterInsert = func(InsertEvent) { t.Fatal("AfterInsert must not fire through an opaque *sql.Tx executor") }
	require.NoError(t, db.I().SetExecutor(rawTx).Insert("insert into people", testPerson{ID: 1, Name: "A"}))
	require.NoError(t, rawTx.Rollback())
}

// passthroughExecutor is a caller's own handlerWithContext wrapper.
type passthroughExecutor struct{ handlerWithContext }

func TestAfterInsert_UpsertUpdateMissInsertsAndFires(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	p := testPerson{ID: 1, Name: "Alice"}
	updateQ := "update `people` set`name`=@@name where`id`<=>@@id"
	replaced, _, err := db.InterpolateParams(updateQ, p)
	require.NoError(t, err)
	mock.ExpectExec(regexp.QuoteMeta(replaced)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("insert into`people`").WillReturnResult(sqlmock.NewResult(1, 1))

	var got []InsertEvent
	db.AfterInsert = func(ev InsertEvent) { got = append(got, ev) }

	// The row the UPDATE missed is created by the fallthrough INSERT — a
	// plain insert this session can vouch for.
	require.NoError(t, db.Upsert("people", []string{"id"}, []string{"name"}, "", p))
	require.Len(t, got, 1)
	require.Equal(t, "people", got[0].Table)
	require.Equal(t, [][]any{{1, "Alice"}}, got[0].Rows)
}
