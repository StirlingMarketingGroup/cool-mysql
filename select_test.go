package mysql

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func getTestDatabase(t *testing.T) *Database {
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
	db := getTestDatabase(t)

	type args struct {
		db            *Database
		conn          commander
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
				dest:          p(time.Time{}),
				query:         "SELECT cast('2020-01-01 00:00:00' as datetime)",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)),
		},
		{
			name: "decimal",
			args: args{
				db:            db,
				conn:          db.Writes,
				ctx:           context.Background(),
				dest:          p(decimal.Decimal{}),
				query:         "SELECT '1.23'",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p(decimal.RequireFromString("1.23")),
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
