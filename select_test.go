package mysql

import (
	"context"
	"database/sql"
	"reflect"
	"regexp"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
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
	require.Equal(t, "item-value", dest[0].CustomLineItem)  // LineItem column -> CustomLineItem field
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
