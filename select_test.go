package mysql

import (
	"context"
	"crypto/sha3"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func getTestDatabase(t *testing.T) (*Database, sqlmock.Sqlmock, func()) {
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)

	mock.ExpectQuery("^SELECT @@max_allowed_packet$").
		WillReturnRows(sqlmock.NewRows([]string{"@@max_allowed_packet"}).
			AddRow(int64(4194304)))

	db, err := NewFromConn(mockDB, mockDB)
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, mock.ExpectationsWereMet())
		if err := mockDB.Close(); err != nil {
			t.Logf("failed to close mock DB: %v", err)
		}
	}

	return db, mock, cleanup
}

func Test_query(t *testing.T) {
	var timeVal time.Time
	var timePtr *time.Time

	var decimalVal decimal.Decimal
	var decimalPtr *decimal.Decimal

	type args struct {
		ctx           context.Context
		dest          any
		query         string
		cacheDuration time.Duration
		params        []any
	}
	tests := []struct {
		name     string
		args     args
		wantErr  bool
		wantDest any
	}{
		{
			name: "time",
			args: args{
				ctx:           context.Background(),
				dest:          &timeVal,
				query:         "SELECT cast('2020-01-01 00:00:00' as datetime)",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)),
		},
		{
			name: "time null",
			args: args{
				ctx:           context.Background(),
				dest:          &timeVal,
				query:         "SELECT null",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p(time.Time{}),
		},
		{
			name: "time ptr",
			args: args{
				ctx:           context.Background(),
				dest:          &timePtr,
				query:         "SELECT cast('2020-01-01 00:00:00' as datetime)",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p(p(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))),
		},
		{
			name: "time ptr nil",
			args: args{
				ctx:           context.Background(),
				dest:          &timePtr,
				query:         "SELECT null",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p((*time.Time)(nil)),
		},
		{
			name: "null time",
			args: args{
				ctx:           context.Background(),
				dest:          p(sql.NullTime{}),
				query:         "SELECT null",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p(sql.NullTime{}),
		},
		{
			name: "time ptr ptr",
			args: args{
				ctx:           context.Background(),
				dest:          p(&timePtr),
				query:         "SELECT cast('2020-01-01 00:00:00' as datetime)",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p(p(p(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)))),
		},
		{
			name: "time ptr ptr nil",
			args: args{
				ctx:           context.Background(),
				dest:          p(&timePtr),
				query:         "SELECT null",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p((**time.Time)(nil)),
		},
		{
			name: "struct times",
			args: args{
				ctx: context.Background(),
				dest: p(struct {
					Time1 time.Time  `mysql:"Time1"`
					Time2 *time.Time `mysql:"Time2"`
				}{}),
				query:         "SELECT cast('2020-01-01 00:00:00' as datetime)`Time1`,cast('2021-01-01 00:00:00' as datetime)`Time2`",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr: false,
			wantDest: p(struct {
				Time1 time.Time  `mysql:"Time1"`
				Time2 *time.Time `mysql:"Time2"`
			}{
				Time1: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
				Time2: p(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)),
			}),
		},
		{
			name: "ptr struct times",
			args: args{
				ctx: context.Background(),
				dest: p(&struct {
					Time1 time.Time  `mysql:"Time1"`
					Time2 *time.Time `mysql:"Time2"`
				}{}),
				query:         "SELECT cast('2020-01-01 00:00:00' as datetime)`Time1`,cast('2021-01-01 00:00:00' as datetime)`Time2`",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr: false,
			wantDest: p(&struct {
				Time1 time.Time  `mysql:"Time1"`
				Time2 *time.Time `mysql:"Time2"`
			}{
				Time1: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
				Time2: p(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)),
			}),
		},
		{
			name: "struct times w/ nil",
			args: args{
				ctx: context.Background(),
				dest: p(struct {
					Time1 time.Time  `mysql:"Time1"`
					Time2 *time.Time `mysql:"Time2"`
				}{}),
				query:         "SELECT null`Time1`,null`Time2`",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr: false,
			wantDest: p(struct {
				Time1 time.Time  `mysql:"Time1"`
				Time2 *time.Time `mysql:"Time2"`
			}{
				Time1: time.Time{},
				Time2: nil,
			}),
		},
		{
			name: "string",
			args: args{
				ctx:           context.Background(),
				dest:          p("yeet"),
				query:         "SELECT 'hello'",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p("hello"),
		},
		{
			name: "string",
			args: args{
				ctx:           context.Background(),
				dest:          p("yeet"),
				query:         "SELECT 'hello'",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p("hello"),
		},
		{
			name: "map rows",
			args: args{
				ctx:           context.Background(),
				dest:          &MapRows{},
				query:         "select 'a' `One`, 'b' `Two` union select 'c' `One`, 'd' `Two`",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr: false,
			wantDest: p(MapRows{
				{
					"One": []byte("a"),
					"Two": []byte("b"),
				},
				{
					"One": []byte("c"),
					"Two": []byte("d"),
				},
			}),
		},
		{
			name: "slice rows",
			args: args{
				ctx:           context.Background(),
				dest:          &SliceRows{},
				query:         "select 'a' `One`, 'b' `Two` union select 'c' `One`, 'd' `Two`",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr: false,
			wantDest: p(SliceRows{
				{
					[]byte("a"),
					[]byte("b"),
				},
				{
					[]byte("c"),
					[]byte("d"),
				},
			}),
		},
		{
			name: "decimal",
			args: args{
				ctx:           context.Background(),
				dest:          &decimalVal,
				query:         "SELECT '1.23'",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p(decimal.RequireFromString("1.23")),
		},
		{
			name: "null decimal to value",
			args: args{
				ctx:           context.Background(),
				dest:          &decimalVal,
				query:         "SELECT null",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p(decimal.Decimal{}),
		},
		{
			name: "decimal ptr",
			args: args{
				ctx:           context.Background(),
				dest:          &decimalPtr,
				query:         "SELECT '1.23'",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p(p(decimal.RequireFromString("1.23"))),
		},
		{
			name: "null decimal to ptr",
			args: args{
				ctx:           context.Background(),
				dest:          &decimalPtr,
				query:         "SELECT null",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p((*decimal.Decimal)(nil)),
		},
		{
			name: "strings slice",
			args: args{
				ctx:           context.Background(),
				dest:          &[]string{},
				query:         "select * from json_table('[ {\"hello\": \"world\"},{\"hello\": null},{\"hello\": \"bar\"} ]', '$[*]' COLUMNS( hello varchar(255) PATH '$.hello' ERROR ON ERROR )) a;",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p([]string{"world", "", "bar"}),
		},
		{
			name: "strings ptrs slice",
			args: args{
				ctx:           context.Background(),
				dest:          &[]*string{},
				query:         "select * from json_table('[ {\"hello\": \"world\"},{\"hello\": null},{\"hello\": \"bar\"} ]', '$[*]' COLUMNS( hello varchar(255) PATH '$.hello' ERROR ON ERROR )) a;",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p([]*string{p("world"), nil, p("bar")}),
		},
		{
			name: "json array",
			args: args{
				ctx:           context.Background(),
				dest:          &struct{ Strings []string }{},
				query:         "select json_array('world',null,'bar') `Strings`",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: &struct{ Strings []string }{Strings: []string{"world", "", "bar"}},
		},
		{
			name: "json array slice",
			args: args{
				ctx:           context.Background(),
				dest:          &[]struct{ Strings []string }{},
				query:         "select json_array('world',null,'bar') `Strings`",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: &[]struct{ Strings []string }{{Strings: []string{"world", "", "bar"}}},
		},
		{
			name: "json array ptr slice",
			args: args{
				ctx:           context.Background(),
				dest:          &[]struct{ Strings *[]string }{},
				query:         "select json_array('world',null,'bar') `Strings`",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: &[]struct{ Strings *[]string }{{Strings: &[]string{"world", "", "bar"}}},
		},
		{
			name: "date",
			args: args{
				ctx:           context.Background(),
				dest:          &civil.Date{},
				query:         "SELECT date('2024-09-02')",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p(civil.DateOf(time.Date(2024, 9, 2, 0, 0, 0, 0, time.UTC))),
		},
		{
			name: "date nil",
			args: args{
				ctx:           context.Background(),
				dest:          &civil.Date{},
				query:         "SELECT date(null)",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p(civil.Date{}),
		},
		{
			name: "slice of struct ptrs",
			args: args{
				ctx:           context.Background(),
				dest:          &[]*struct{ Strings *[]string }{},
				query:         "select json_array('world',null,'bar') `Strings`",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: &[]*struct{ Strings *[]string }{{Strings: &[]string{"world", "", "bar"}}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db, mock, cleanup := getTestDatabase(t)
			defer cleanup()

			var rows *sqlmock.Rows
			switch tt.name {
			case "time", "time ptr", "time ptr ptr":
				rows = sqlmock.NewRows([]string{"col"}).AddRow(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
			case "time null", "time ptr nil", "null time", "time ptr ptr nil", "null decimal to value", "null decimal to ptr", "date nil":
				rows = sqlmock.NewRows([]string{"col"}).AddRow(nil)
			case "struct times", "ptr struct times":
				rows = sqlmock.NewRows([]string{"Time1", "Time2"}).
					AddRow(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC))
			case "struct times w/ nil":
				rows = sqlmock.NewRows([]string{"Time1", "Time2"}).AddRow(nil, nil)
			case "string":
				rows = sqlmock.NewRows([]string{"col"}).AddRow("hello")
			case "map rows", "slice rows":
				rows = sqlmock.NewRows([]string{"One", "Two"}).
					AddRow([]byte("a"), []byte("b")).
					AddRow([]byte("c"), []byte("d"))
			case "decimal", "decimal ptr":
				rows = sqlmock.NewRows([]string{"col"}).AddRow("1.23")
			case "strings slice", "strings ptrs slice":
				rows = sqlmock.NewRows([]string{"hello"}).
					AddRow("world").
					AddRow(nil).
					AddRow("bar")
			case "json array", "json array slice", "json array ptr slice", "slice of struct ptrs":
				rows = sqlmock.NewRows([]string{"Strings"}).
					AddRow([]byte(`["world",null,"bar"]`))
			case "date":
				rows = sqlmock.NewRows([]string{"col"}).AddRow(time.Date(2024, 9, 2, 0, 0, 0, 0, time.UTC))
			default:
				rows = sqlmock.NewRows([]string{"col"}).AddRow(nil)
			}

			mock.ExpectQuery(regexp.QuoteMeta(tt.args.query)).WillReturnRows(rows)

			if err := db.query(db.Writes, tt.args.ctx, tt.args.dest, tt.args.query, tt.args.cacheDuration, tt.args.params...); (err != nil) != tt.wantErr {
				t.Errorf("query() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !reflect.DeepEqual(tt.args.dest, tt.wantDest) {
				t.Errorf("query() dest = %v, wantDest %v", tt.args.dest, tt.wantDest)
			}
		})
	}
}

func Test_isMultiValueElement(t *testing.T) {
	type args struct {
		t reflect.Type
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "slice",
			args: args{
				t: reflect.TypeOf([]int{}),
			},
			want: true,
		},
		{
			name: "time pointer",
			args: args{
				t: reflect.TypeOf(&time.Time{}),
			},
			want: false,
		},
		{
			name: "decimal.Decimal",
			args: args{
				t: reflect.TypeOf(decimal.Decimal{}),
			},
			want: false,
		},
		{
			name: "maprow",
			args: args{
				t: reflect.TypeOf(MapRow{}),
			},
			want: true,
		},
		{
			name: "slicerow",
			args: args{
				t: reflect.TypeOf(SliceRow{}),
			},
			want: true,
		},
		{
			name: "slicerow ptr",
			args: args{
				t: reflect.TypeOf(&SliceRow{}),
			},
			want: true,
		},
		{
			name: "string",
			args: args{
				t: reflect.TypeOf(""),
			},
			want: false,
		},
		{
			name: "int",
			args: args{
				t: reflect.TypeOf(0),
			},
			want: false,
		},
		{
			name: "misc struct",
			args: args{
				t: reflect.TypeOf(struct {
					A int
				}{}),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isMultiValueElement(tt.args.t); got != tt.want {
				t.Errorf("isMultiValueElement() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSelectRetriesAndCloses(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		if err := mockDB.Close(); err != nil {
			t.Logf("failed to close mock DB: %v", err)
		}
	}()

	// 1) Expect the max_allowed_packet lookup
	mock.ExpectQuery("^SELECT @@max_allowed_packet$").
		WillReturnRows(sqlmock.NewRows([]string{"@@max_allowed_packet"}).
			AddRow(int64(4194304)))

	// 2) Simulate first SELECT foo FROM bar failing
	mock.ExpectQuery("^SELECT foo FROM bar$").
		WillReturnError(errMockRetry)
	// 3) Then simulate it succeeding with 2 rows
	rows := sqlmock.NewRows([]string{"foo"}).
		AddRow("a").
		AddRow("b")
	mock.ExpectQuery("^SELECT foo FROM bar$").
		WillReturnRows(rows)

	db, err := NewFromConn(mockDB, mockDB)
	require.NoError(t, err)

	err = db.Select(func(scanDest any) {
		// no-op row processor
	}, "SELECT foo FROM bar", 0)
	require.NoError(t, err)

	// verify we closed the failed-attempt rows and met all expectations
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSelectChannelUnexportedField(t *testing.T) {
	db, _, cleanup := getTestDatabase(t)
	defer cleanup()

	type row struct {
		Foo string
		bar int //nolint:unused
	}
	ch := make(chan row)
	err := db.Select(ch, "SELECT foo, bar FROM table_name", 0)
	require.ErrorIs(t, err, ErrUnexportedField)
}

// TestSelectJSONUnmarshalErrorWithPointerElement tests that JSON unmarshal errors
// are handled correctly when the destination is a slice of struct pointers.
// This is a regression test for issue #130: panic when el.Type().FieldByIndex()
// is called on a pointer type.
func TestSelectJSONUnmarshalErrorWithPointerElement(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	type OrderLineItem struct {
		ID    int      `mysql:"ID"`
		Items []string `mysql:"Items"` // JSON field
	}

	// Return invalid JSON for the Items column to trigger unmarshal error
	rows := sqlmock.NewRows([]string{"ID", "Items"}).
		AddRow(1, []byte(`{invalid json}`))

	mock.ExpectQuery("SELECT ID, Items FROM orders").WillReturnRows(rows)

	// Use slice of struct pointers - this triggers the bug
	var dest []*OrderLineItem
	err := db.Select(&dest, "SELECT ID, Items FROM orders", 0)

	// Should get an error about JSON unmarshal, not a panic
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to unmarshal json into struct field")
	require.Contains(t, err.Error(), "Items")
}

// TestSelectNoInsertFieldMapping tests that fields with noinsert option
// still participate in SELECT column mapping with their explicit tag name.
// This is a regression test for issue #133.
func TestSelectNoInsertFieldMapping(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	// This struct demonstrates the conflict scenario from issue #133:
	// - CustomLineItem maps to column "LineItem" for both insert and select
	// - LineItemModel maps to column "LineItemModel" for select only (skipped for insert)
	// Without noinsert, if LineItemModel used mysql:"-", its struct name "LineItemModel"
	// wouldn't conflict. But with a different field having mysql:"LineItem", we need
	// noinsert to map to a different column name without being inserted.
	type OrderRow struct {
		ID             int    `mysql:"ID"`
		CustomLineItem string `mysql:"LineItem"`               // maps to "LineItem" column
		LineItemModel  string `mysql:"LineItemModel,noinsert"` // maps to "LineItemModel" for select, skipped for insert
	}

	// Return data for both columns
	rows := sqlmock.NewRows([]string{"ID", "LineItem", "LineItemModel"}).
		AddRow(1, "item-value", "model-value")

	mock.ExpectQuery("SELECT ID, LineItem, LineItemModel FROM orders").WillReturnRows(rows)

	var dest []OrderRow
	err := db.Select(&dest, "SELECT ID, LineItem, LineItemModel FROM orders", 0)
	require.NoError(t, err)

	require.Len(t, dest, 1)
	require.Equal(t, 1, dest[0].ID)
	require.Equal(t, "item-value", dest[0].CustomLineItem) // LineItem column -> CustomLineItem field
	require.Equal(t, "model-value", dest[0].LineItemModel) // LineItemModel column -> LineItemModel field
}

// TestSelectReacquiresConnOnErrInvalidConn verifies that when QueryContext
// returns mysql.ErrInvalidConn on a dedicated conn checked out from the
// pool, select.go releases the dead conn and grabs a fresh one before
// retrying. Without that swap the retry would hit the same dead conn and
// burn the entire retry budget.
func TestSelectReacquiresConnOnErrInvalidConn(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectQuery("^SELECT 1$").WillReturnError(mysql.ErrInvalidConn)
	mock.ExpectQuery("^SELECT 1$").
		WillReturnRows(sqlmock.NewRows([]string{"col"}).AddRow(int64(1)))

	var v int64
	require.NoError(t, db.Select(&v, "SELECT 1", 0))
	require.Equal(t, int64(1), v)
}

// getDualPoolTestDatabase builds a Database whose Reads and Writes point
// at independent sqlmock pools so tests can exercise the writes-specific
// branch of the conn-reacquire logic (c == db.Writes.(*sql.DB)).
func getDualPoolTestDatabase(t *testing.T) (*Database, sqlmock.Sqlmock, sqlmock.Sqlmock, func()) {
	writesDB, writesMock, err := sqlmock.New()
	require.NoError(t, err)
	readsDB, readsMock, err := sqlmock.New()
	require.NoError(t, err)

	writesMock.ExpectQuery("^SELECT @@max_allowed_packet$").
		WillReturnRows(sqlmock.NewRows([]string{"@@max_allowed_packet"}).
			AddRow(int64(4194304)))

	db, err := NewFromConn(writesDB, readsDB)
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, writesMock.ExpectationsWereMet())
		require.NoError(t, readsMock.ExpectationsWereMet())
		if err := writesDB.Close(); err != nil {
			t.Logf("failed to close writes mock: %v", err)
		}
		if err := readsDB.Close(); err != nil {
			t.Logf("failed to close reads mock: %v", err)
		}
	}
	return db, writesMock, readsMock, cleanup
}

// TestSelectWritesReacquiresConnOnErrInvalidConn covers the writes branch
// of getFreshConn — c matches db.Writes.(*sql.DB) but not db.Reads.
func TestSelectWritesReacquiresConnOnErrInvalidConn(t *testing.T) {
	db, writesMock, _, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	writesMock.ExpectQuery("^SELECT 2$").WillReturnError(mysql.ErrInvalidConn)
	writesMock.ExpectQuery("^SELECT 2$").
		WillReturnRows(sqlmock.NewRows([]string{"col"}).AddRow(int64(2)))

	var v int64
	require.NoError(t, db.SelectWrites(&v, "SELECT 2", 0))
	require.Equal(t, int64(2), v)
}

// TestSelectUnrelatedPoolReacquiresOnErrInvalidConn covers the fallback
// branch where the passed conn is a *sql.DB that matches neither db.Reads
// nor db.Writes — getFreshConn keeps using that pool directly.
func TestSelectUnrelatedPoolReacquiresOnErrInvalidConn(t *testing.T) {
	db, _, _, cleanup := getDualPoolTestDatabase(t)
	defer cleanup()

	thirdDB, thirdMock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, thirdMock.ExpectationsWereMet())
		if err := thirdDB.Close(); err != nil {
			t.Logf("failed to close third mock: %v", err)
		}
	}()

	thirdMock.ExpectQuery("^SELECT 3$").WillReturnError(mysql.ErrInvalidConn)
	thirdMock.ExpectQuery("^SELECT 3$").
		WillReturnRows(sqlmock.NewRows([]string{"col"}).AddRow(int64(3)))

	var v int64
	require.NoError(t, db.query(thirdDB, context.Background(), &v, "SELECT 3", 0))
	require.Equal(t, int64(3), v)
}

// errInvalidConnCounter counts QueryContext calls and always returns
// mysql.ErrInvalidConn — stands in for a *sql.Tx whose conn has died.
type errInvalidConnCounter struct {
	calls int
}

func (h *errInvalidConnCounter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	panic("unexpected ExecContext call in errInvalidConnCounter")
}

func (h *errInvalidConnCounter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	h.calls++
	return nil, mysql.ErrInvalidConn
}

// TestSelectErrInvalidConnFailsFastWithoutFreshConn verifies that when
// conn is not a *sql.DB (e.g. a *sql.Tx), an ErrInvalidConn is treated as
// permanent. Without that fail-fast the backoff loop would spin on the
// same dead conn until the whole retry budget was burned.
func TestSelectErrInvalidConnFailsFastWithoutFreshConn(t *testing.T) {
	h := &errInvalidConnCounter{}
	db := &Database{Logger: DefaultLogger(), testMx: new(sync.Mutex)}

	var out []int
	err := db.query(h, context.Background(), &out, "SELECT 1", 0)
	require.Error(t, err)
	require.ErrorIs(t, err, mysql.ErrInvalidConn)
	require.Equal(t, 1, h.calls, "expected exactly one attempt, got %d", h.calls)
}

// TestSelectPropagatesTestError verifies that when db.Test() fails during
// the ErrInvalidConn recovery path (pool Ping fails and Reconnect also
// fails), that error surfaces to the caller instead of being swallowed.
func TestSelectPropagatesTestError(t *testing.T) {
	originalMax := MaxAttempts
	MaxAttempts = 1
	t.Cleanup(func() { MaxAttempts = originalMax })

	mockDB, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	defer func() {
		if err := mockDB.Close(); err != nil {
			t.Logf("failed to close mock: %v", err)
		}
	}()

	mock.ExpectQuery("^SELECT @@max_allowed_packet$").
		WillReturnRows(sqlmock.NewRows([]string{"@@max_allowed_packet"}).
			AddRow(int64(4194304)))

	db, err := NewFromConn(mockDB, mockDB)
	require.NoError(t, err)

	// First QueryContext fails with ErrInvalidConn — drives us into the
	// Test() recovery branch. MonitorPingsOption+no ExpectPing makes the
	// subsequent db.Test() Ping fail, and since WritesDSN/ReadsDSN are
	// empty strings (NewFromConn doesn't know them), Reconnect also
	// fails — so Test() returns an error.
	mock.ExpectQuery("^SELECT 9$").WillReturnError(mysql.ErrInvalidConn)

	var v int64
	err = db.Select(&v, "SELECT 9", 0)
	require.Error(t, err)
}

// TestSelectFailsWhenInitialConnAcquireFails verifies the initial
// getFreshConn failure path — if the pool is closed before the query, we
// surface the acquire error instead of entering the retry loop.
func TestSelectFailsWhenInitialConnAcquireFails(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)

	mock.ExpectQuery("^SELECT @@max_allowed_packet$").
		WillReturnRows(sqlmock.NewRows([]string{"@@max_allowed_packet"}).
			AddRow(int64(4194304)))

	db, err := NewFromConn(mockDB, mockDB)
	require.NoError(t, err)

	// sqlmock complains about unexpected Close, but we intentionally
	// close here to force the pool into a state where Conn(ctx) fails.
	_ = mockDB.Close()

	var v int64
	err = db.Select(&v, "SELECT 1", 0)
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to get connection")
}

// TestSelectRetriesMidStreamConnDrop verifies that a buffered (slice) dest
// whose connection drops *mid-stream* — after rows have started flowing — is
// retried and succeeds within the existing retry budget. Before the fix the
// retry only wrapped query establishment, so a drop during scanning surfaced
// straight to the caller. The partial first-attempt row must be discarded so
// the result reflects only the successful re-run.
func TestSelectRetriesMidStreamConnDrop(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	// First attempt: one good row, then the connection dies on the 2nd row.
	mock.ExpectQuery("^SELECT n$").WillReturnRows(
		sqlmock.NewRows([]string{"n"}).
			AddRow(int64(1)).
			AddRow(int64(2)).
			RowError(1, mysql.ErrInvalidConn),
	)
	// Retry on a fresh conn returns a clean result set.
	mock.ExpectQuery("^SELECT n$").WillReturnRows(
		sqlmock.NewRows([]string{"n"}).
			AddRow(int64(10)).
			AddRow(int64(20)),
	)

	var ns []int64
	require.NoError(t, db.Select(&ns, "SELECT n", 0))
	require.Equal(t, []int64{10, 20}, ns)
}

// TestSelectRetriesMidStreamPreservesExistingSlice verifies that a mid-stream
// retry discards only the rows collected on the failed attempt, not the
// elements the caller had already accumulated into the slice. Select appends
// onto the passed-in slice, so the reset must truncate back to the caller's
// pre-query length rather than emptying it.
func TestSelectRetriesMidStreamPreservesExistingSlice(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectQuery("^SELECT n$").WillReturnRows(
		sqlmock.NewRows([]string{"n"}).
			AddRow(int64(1)).
			AddRow(int64(2)).
			RowError(1, mysql.ErrInvalidConn),
	)
	mock.ExpectQuery("^SELECT n$").WillReturnRows(
		sqlmock.NewRows([]string{"n"}).
			AddRow(int64(10)).
			AddRow(int64(20)),
	)

	ns := []int64{99}
	require.NoError(t, db.Select(&ns, "SELECT n", 0))
	require.Equal(t, []int64{99, 10, 20}, ns)
}

// TestSelectRetriesMidStreamSingleDest covers the single-value (*T) buffered
// dest: a mid-stream drop discards the partial result and the re-run wins.
func TestSelectRetriesMidStreamSingleDest(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectQuery("^SELECT n$").WillReturnRows(
		sqlmock.NewRows([]string{"n"}).
			AddRow(int64(1)).
			RowError(0, mysql.ErrInvalidConn),
	)
	mock.ExpectQuery("^SELECT n$").WillReturnRows(
		sqlmock.NewRows([]string{"n"}).AddRow(int64(42)),
	)

	var v int64
	require.NoError(t, db.Select(&v, "SELECT n", 0))
	require.Equal(t, int64(42), v)
}

// TestSelectDoesNotRetryMidStreamNonConnError verifies a non-transient
// mid-stream error (anything that isn't ErrInvalidConn / ErrBadConn) is not
// retried and surfaces immediately. Only one query is issued.
func TestSelectDoesNotRetryMidStreamNonConnError(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	boom := errors.New("mid-stream type error")
	mock.ExpectQuery("^SELECT n$").WillReturnRows(
		sqlmock.NewRows([]string{"n"}).
			AddRow(int64(1)).
			AddRow(int64(2)).
			RowError(1, boom),
	)

	var ns []int64
	err := db.Select(&ns, "SELECT n", 0)
	require.Error(t, err)
	require.ErrorIs(t, err, boom)
}

// TestSelectChannelDestDoesNotRetryMidStream verifies channel dests keep
// establishment-only retry semantics: rows are emitted to the caller as they
// are scanned, so a mid-stream drop can't be re-run and surfaces as-is. Only
// one query is issued and the already-streamed row stays visible.
func TestSelectChannelDestDoesNotRetryMidStream(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectQuery("^SELECT n$").WillReturnRows(
		sqlmock.NewRows([]string{"n"}).
			AddRow(int64(1)).
			AddRow(int64(2)).
			RowError(1, mysql.ErrInvalidConn),
	)

	ch := make(chan int64, 10)
	err := db.Select(ch, "SELECT n", 0)
	close(ch)

	require.Error(t, err)
	require.ErrorIs(t, err, mysql.ErrInvalidConn)

	var got []int64
	for v := range ch {
		got = append(got, v)
	}
	require.Equal(t, []int64{1}, got)
}

// TestExistsRetriesMidStreamConnDrop verifies Exists re-runs the query when
// the connection drops during the single-row read phase.
func TestExistsRetriesMidStreamConnDrop(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectQuery("^SELECT 1$").WillReturnRows(
		sqlmock.NewRows([]string{"1"}).
			AddRow(int64(1)).
			RowError(0, mysql.ErrInvalidConn),
	)
	mock.ExpectQuery("^SELECT 1$").WillReturnRows(
		sqlmock.NewRows([]string{"1"}).AddRow(int64(1)),
	)

	exists, err := db.Exists("SELECT 1", 0)
	require.NoError(t, err)
	require.True(t, exists)
}

// TestExistsRetriesEstablishmentConnDrop guards against a silent false
// negative: when QueryContext fails with ErrInvalidConn on a healthy pool,
// exists must re-run the query (the next QueryContext draws a fresh pooled
// conn) rather than reporting a dead conn as "no rows". Returning db.Test()
// directly would return (false, nil) and stop the retry.
func TestExistsRetriesEstablishmentConnDrop(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectQuery("^SELECT 1$").WillReturnError(mysql.ErrInvalidConn)
	mock.ExpectQuery("^SELECT 1$").WillReturnRows(
		sqlmock.NewRows([]string{"1"}).AddRow(int64(1)),
	)

	exists, err := db.Exists("SELECT 1", 0)
	require.NoError(t, err)
	require.True(t, exists)
}

// TestExistsTxFailsFastOnEstablishmentConnDrop verifies that an ErrInvalidConn
// inside a transaction is not retried — the tx is bound to its dead conn and
// can't be resumed on a fresh one, so it must surface immediately.
func TestExistsTxFailsFastOnEstablishmentConnDrop(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectQuery("^SELECT 1$").WillReturnError(mysql.ErrInvalidConn)
	mock.ExpectRollback()

	tx, _, err := db.BeginTx()
	require.NoError(t, err)

	_, err = tx.Exists("SELECT 1", 0)
	require.Error(t, err)
	require.ErrorIs(t, err, mysql.ErrInvalidConn)

	require.NoError(t, tx.Cancel())
}

// TestExistsTxFailsFastOnMidStreamConnDrop covers the same tx fail-fast for a
// connection that drops during the single-row read rather than at
// establishment.
func TestExistsTxFailsFastOnMidStreamConnDrop(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectQuery("^SELECT 1$").WillReturnRows(
		sqlmock.NewRows([]string{"1"}).
			AddRow(int64(1)).
			RowError(0, mysql.ErrInvalidConn),
	)
	mock.ExpectRollback()

	tx, _, err := db.BeginTx()
	require.NoError(t, err)

	_, err = tx.Exists("SELECT 1", 0)
	require.Error(t, err)
	require.ErrorIs(t, err, mysql.ErrInvalidConn)

	require.NoError(t, tx.Cancel())
}

// selectCacheKey computes the cache key the same way Database.query does for a
// multi-row (slice) select whose element type is elemType.
func selectCacheKey(elemType reflect.Type, replacedQuery string, cacheDuration time.Duration) string {
	key := "cool-mysql:" + elemType.String() + ":" + replacedQuery + ":" + strconv.FormatInt(int64(cacheDuration), 10)
	h := sha3.Sum224([]byte(key))
	return hex.EncodeToString(h[:])
}

// TestSelectUndecodableCacheTreatedAsMiss_Garbage plants non-msgpack bytes under
// the select cache key and asserts Select falls through to MySQL, succeeds, and
// overwrites the bad entry with a clean blob.
func TestSelectUndecodableCacheTreatedAsMiss_Garbage(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	cache := NewWeakCache()
	db.UseCache(cache)

	type Row struct {
		Name string
	}

	const query = "select name from t"
	cacheDuration := time.Minute

	replacedQuery, _, err := db.InterpolateParams(query)
	require.NoError(t, err)
	require.Equal(t, query, replacedQuery, "param-free query should pass through unchanged")

	cacheKey := selectCacheKey(reflect.TypeOf(Row{}), replacedQuery, cacheDuration)
	require.NoError(t, cache.Set(context.Background(), cacheKey, []byte("not msgpack"), cacheDuration))

	mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnRows(
		sqlmock.NewRows([]string{"Name"}).AddRow("alice"),
	)

	var dest []Row
	err = db.Select(&dest, query, cacheDuration)
	require.NoError(t, err)
	require.Equal(t, []Row{{Name: "alice"}}, dest)

	// Bad entry must have been overwritten with a clean msgpack blob.
	b, err := cache.Get(context.Background(), cacheKey)
	require.NoError(t, err)
	var check []Row
	require.NoError(t, msgpack.Unmarshal(b, &check))
	require.Equal(t, dest, check)
}

// TestSelectUndecodableCacheTreatedAsMiss_ShapeChange reproduces the prod panic
// shape from SMG#8686: a msgpack blob written for a struct with *time.Time is
// read into a same-named type with time.Time. Before the fix, msgpack's
// nilAwareDecoder panics with 'reflect: call of reflect.Value.IsNil on struct
// Value'. After the fix, Select must treat it as a cache miss and succeed from DB.
func TestSelectUndecodableCacheTreatedAsMiss_ShapeChange(t *testing.T) {
	// Old shape: Created is a pointer (nil in the cached blob).
	type Row struct {
		Name    string
		Created *time.Time
	}
	oldBlob, err := msgpack.Marshal([]Row{{Name: "old", Created: nil}})
	require.NoError(t, err)

	// Nested scope so the new-shape type can reuse the name "Row" — the cache
	// key uses t.String() which for a local named type includes the package
	// path and function name, so old vs new type names need not collide; we
	// still name both Row for fidelity to the deploy-time shape flip.
	t.Run("new-shape-reader", func(t *testing.T) {
		type Row struct {
			Name    string
			Created time.Time
		}

		// Confirm the raw decode panics with the prod IsNil message (documents
		// the failure mode even after the library fix swallows it).
		func() {
			defer func() {
				r := recover()
				require.NotNil(t, r, "expected msgpack decode of pointer-nil into value field to panic")
				t.Logf("reproduced decode panic mode: %v", r)
				require.Contains(t, fmt.Sprint(r), "reflect: call of reflect.Value.IsNil on struct Value")
			}()
			var probe []Row
			_ = msgpack.Unmarshal(oldBlob, &probe)
			t.Fatal("expected panic from msgpack.Unmarshal of old-shape blob into new-shape dest")
		}()

		db, mock, cleanup := getTestDatabase(t)
		defer cleanup()

		cache := NewWeakCache()
		db.UseCache(cache)

		const query = "select name, created from t"
		cacheDuration := time.Minute

		replacedQuery, _, err := db.InterpolateParams(query)
		require.NoError(t, err)
		require.Equal(t, query, replacedQuery)

		cacheKey := selectCacheKey(reflect.TypeOf(Row{}), replacedQuery, cacheDuration)
		require.NoError(t, cache.Set(context.Background(), cacheKey, oldBlob, cacheDuration))

		wantTime := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
		mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnRows(
			sqlmock.NewRows([]string{"Name", "Created"}).AddRow("alice", wantTime),
		)

		var dest []Row
		// Must not panic; decode failure is a cache miss → DB path.
		require.NotPanics(t, func() {
			err = db.Select(&dest, query, cacheDuration)
		})
		require.NoError(t, err)
		require.Len(t, dest, 1)
		require.Equal(t, "alice", dest[0].Name)
		require.True(t, dest[0].Created.Equal(wantTime), "Created = %v, want %v", dest[0].Created, wantTime)
	})
}

// TestSelectCacheHitSlice populates the cache via a first Select, then serves a
// second identical Select entirely from cache (no second SQL expectation).
func TestSelectCacheHitSlice(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	db.UseCache(NewWeakCache())

	type Row struct {
		Name string
	}

	const query = "select name from t"
	cacheDuration := time.Minute

	mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnRows(
		sqlmock.NewRows([]string{"Name"}).AddRow("alice"),
	)

	var first []Row
	require.NoError(t, db.Select(&first, query, cacheDuration))
	require.Equal(t, []Row{{Name: "alice"}}, first)

	// Second call must hit cache — no additional sqlmock expectation.
	var second []Row
	require.NoError(t, db.Select(&second, query, cacheDuration))
	require.Equal(t, first, second)
}

// TestSelectCacheHitSingleRow serves a single-row (*Row) dest from a cache
// entry populated by a prior slice select (same cache key element type).
func TestSelectCacheHitSingleRow(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	db.UseCache(NewWeakCache())

	type Row struct {
		Name string
	}

	const query = "select name from t"
	cacheDuration := time.Minute

	mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnRows(
		sqlmock.NewRows([]string{"Name"}).AddRow("bob"),
	)

	var slice []Row
	require.NoError(t, db.Select(&slice, query, cacheDuration))
	require.Equal(t, []Row{{Name: "bob"}}, slice)

	// Single-row dest, same query+TTL → same key → cache hit, !multiRow break.
	var row Row
	require.NoError(t, db.Select(&row, query, cacheDuration))
	require.Equal(t, Row{Name: "bob"}, row)
}

// TestSelectCacheHitSingleRowEmpty plants an empty msgpack slice under the
// single-dest cache key; Select must return sql.ErrNoRows from the hit path
// without querying MySQL.
func TestSelectCacheHitSingleRowEmpty(t *testing.T) {
	db, _, cleanup := getTestDatabase(t)
	defer cleanup()

	cache := NewWeakCache()
	db.UseCache(cache)

	type Row struct {
		Name string
	}

	const query = "select name from t"
	cacheDuration := time.Minute

	replacedQuery, _, err := db.InterpolateParams(query)
	require.NoError(t, err)

	cacheKey := selectCacheKey(reflect.TypeOf(Row{}), replacedQuery, cacheDuration)
	emptyBlob, err := msgpack.Marshal([]Row{})
	require.NoError(t, err)
	require.NoError(t, cache.Set(context.Background(), cacheKey, emptyBlob, cacheDuration))

	// No sqlmock expectation: served entirely from cache.
	// cleanup's ExpectationsWereMet proves zero queries were issued.
	var row Row
	err = db.Select(&row, query, cacheDuration)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

// TestSelectCacheHitSendElementError covers the cache-hit sendElement error
// path: an unbuffered channel with a cancelled context cannot accept the send.
func TestSelectCacheHitSendElementError(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	db.UseCache(NewWeakCache())

	type Row struct {
		Name string
	}

	const query = "select name from t"
	cacheDuration := time.Minute

	mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnRows(
		sqlmock.NewRows([]string{"Name"}).AddRow("carol"),
	)

	var slice []Row
	require.NoError(t, db.Select(&slice, query, cacheDuration))
	require.Equal(t, []Row{{Name: "carol"}}, slice)

	ch := make(chan Row) // unbuffered; nothing reading
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := db.SelectContext(ctx, ch, query, cacheDuration)
	require.ErrorIs(t, err, context.Canceled)
}

// flakyLockerCache wraps WeakCache with a Locker that fails once, then returns
// an unlock func that always errors (to exercise the unlock-warn path).
type flakyLockerCache struct {
	*WeakCache
	lockAttempts int
}

func (c *flakyLockerCache) Lock(ctx context.Context, key string) (func() error, error) {
	c.lockAttempts++
	if c.lockAttempts == 1 {
		return nil, fmt.Errorf("lock contended")
	}
	return func() error { return fmt.Errorf("unlock failed") }, nil
}

// TestSelectCacheMissLockerRetryAndUnlockWarn covers Lock acquisition, the
// lock-failure sleep+retry branch, and the deferred unlock-error warn on a
// cold-cache Select that then succeeds from MySQL.
func TestSelectCacheMissLockerRetryAndUnlockWarn(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	cache := &flakyLockerCache{WeakCache: NewWeakCache()}
	db.UseCache(cache)

	type Row struct {
		Name string
	}

	const query = "select name from t"
	cacheDuration := time.Minute

	mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnRows(
		sqlmock.NewRows([]string{"Name"}).AddRow("dave"),
	)

	var dest []Row
	require.NoError(t, db.Select(&dest, query, cacheDuration))
	require.Equal(t, []Row{{Name: "dave"}}, dest)
	require.Equal(t, 2, cache.lockAttempts, "first Lock fails, second succeeds")

	// Cache must hold a decodable blob after the successful DB path.
	replacedQuery, _, err := db.InterpolateParams(query)
	require.NoError(t, err)
	cacheKey := selectCacheKey(reflect.TypeOf(Row{}), replacedQuery, cacheDuration)
	b, err := cache.Get(context.Background(), cacheKey)
	require.NoError(t, err)
	var check []Row
	require.NoError(t, msgpack.Unmarshal(b, &check))
	require.Equal(t, dest, check)
}

// erroringGetCache wraps WeakCache with a Get that always fails with a
// non-miss error.
type erroringGetCache struct {
	*WeakCache
}

func (c *erroringGetCache) Get(ctx context.Context, key string) ([]byte, error) {
	return nil, fmt.Errorf("redis down")
}

// TestSelectCacheGetErrorReturned covers the non-miss cache Get error path:
// with no HandleCacheError set, the select fails with the wrapped error.
func TestSelectCacheGetErrorReturned(t *testing.T) {
	db, _, cleanup := getTestDatabase(t)
	defer cleanup()

	db.UseCache(&erroringGetCache{WeakCache: NewWeakCache()})

	type Row struct {
		Name string
	}

	var dest []Row
	err := db.Select(&dest, "select name from t", time.Minute)
	require.ErrorContains(t, err, "failed to get data from cache")
	require.ErrorContains(t, err, "redis down")
}

// TestSelectCacheGetErrorSwallowed covers HandleCacheError swallowing a cache
// Get error so the select proceeds to MySQL without the stampede lock.
func TestSelectCacheGetErrorSwallowed(t *testing.T) {
	db, mock, cleanup := getTestDatabase(t)
	defer cleanup()

	db.UseCache(&erroringGetCache{WeakCache: NewWeakCache()})
	var handled error
	db.HandleCacheError = func(err error) error {
		handled = err
		return nil
	}

	type Row struct {
		Name string
	}

	const query = "select name from t"
	mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnRows(
		sqlmock.NewRows([]string{"Name"}).AddRow("erin"),
	)

	var dest []Row
	require.NoError(t, db.Select(&dest, query, time.Minute))
	require.Equal(t, []Row{{Name: "erin"}}, dest)
	require.ErrorContains(t, handled, "redis down")
}
