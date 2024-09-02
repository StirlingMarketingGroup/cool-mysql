package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"github.com/shopspring/decimal"
)

func getTestDatabase() *Database {
	tz, err := time.LoadLocation(os.Getenv("TZ"))
	if err != nil {
		panic(fmt.Sprintf("failed to get current tz: %v", err))
	}

	db, err := New(
		getenv("MYSQL_TEST_USER", "root"),
		getenv("MYSQL_TEST_PASS", ""),
		getenv("MYSQL_TEST_DBNAME", "test"),
		getenv("MYSQL_TEST_HOST", "127.0.0.1"),
		int(getenvInt64("MYSQL_TEST_PORT", 3306)),

		getenv("MYSQL_TEST_USER", "root"),
		getenv("MYSQL_TEST_PASS", ""),
		getenv("MYSQL_TEST_DBNAME", "test"),
		getenv("MYSQL_TEST_HOST", "127.0.0.1"),
		int(getenvInt64("MYSQL_TEST_PORT", 3306)),
		"utf8_unicode_ci", tz,
	)
	if err != nil {
		panic(fmt.Sprintf("failed to create database: %v", err))
	}

	return db
}

func Test_query(t *testing.T) {
	db := getTestDatabase()

	var timeVal time.Time
	var timePtr *time.Time

	var decimalVal decimal.Decimal
	var decimalPtr *decimal.Decimal

	type args struct {
		db            *Database
		conn          handlerWithContext
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
				db:            db,
				conn:          db.Writes,
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
				db:            db,
				conn:          db.Writes,
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
				db:            db,
				conn:          db.Writes,
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
				db:            db,
				conn:          db.Writes,
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
				db:            db,
				conn:          db.Writes,
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
				db:            db,
				conn:          db.Writes,
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
				db:            db,
				conn:          db.Writes,
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
				db:   db,
				conn: db.Writes,
				ctx:  context.Background(),
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
				db:   db,
				conn: db.Writes,
				ctx:  context.Background(),
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
				db:   db,
				conn: db.Writes,
				ctx:  context.Background(),
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
				db:            db,
				conn:          db.Writes,
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
				db:            db,
				conn:          db.Writes,
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
				db:            db,
				conn:          db.Writes,
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
				db:            db,
				conn:          db.Writes,
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
				db:            db,
				conn:          db.Writes,
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
				db:            db,
				conn:          db.Writes,
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
				db:            db,
				conn:          db.Writes,
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
				db:            db,
				conn:          db.Writes,
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
				db:            db,
				conn:          db.Writes,
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
				db:            db,
				conn:          db.Writes,
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
				db:            db,
				conn:          db.Writes,
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
				db:            db,
				conn:          db.Writes,
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
				db:            db,
				conn:          db.Writes,
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
				db:            db,
				conn:          db.Writes,
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
				db:            db,
				conn:          db.Writes,
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
				db:            db,
				conn:          db.Writes,
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
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.args.db.query(tt.args.conn, tt.args.ctx, tt.args.dest, tt.args.query, tt.args.cacheDuration, tt.args.params...); (err != nil) != tt.wantErr {
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
