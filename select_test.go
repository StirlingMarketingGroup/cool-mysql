package mysql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/civil"
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
	db.AddScannerFuncs(func(dest *civil.Date, src any) error {
		switch v := src.(type) {
		case []byte:
			if v == nil {
				*dest = civil.Date{}
				return nil
			}
			var err error
			*dest, err = civil.ParseDate(string(v))
			return err
		case string:
			var err error
			*dest, err = civil.ParseDate(v)
			return err
		case time.Time:
			*dest = civil.DateOf(v)
			return nil
		case nil:
			*dest = civil.Date{}
			return nil
		default:
			return fmt.Errorf("invalid type to scan into civil.Date: %T", src)
		}
	})

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
			name: "decimal ptr",
			args: args{
				db:            db,
				conn:          db.Writes,
				ctx:           context.Background(),
				dest:          p(&decimal.Decimal{}),
				query:         "SELECT '1.23'",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p(p(decimal.RequireFromString("1.23"))),
		},
		{
			name: "decimal in struct",
			args: args{
				db:            db,
				conn:          db.Writes,
				ctx:           context.Background(),
				dest:          p(struct{ Decimal decimal.Decimal }{}),
				query:         "SELECT '1.23'`Decimal`",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p(struct{ Decimal decimal.Decimal }{decimal.RequireFromString("1.23")}),
		},
		{
			name: "decimal ptr in struct",
			args: args{
				db:            db,
				conn:          db.Writes,
				ctx:           context.Background(),
				dest:          p(struct{ Decimal *decimal.Decimal }{}),
				query:         "SELECT '1.23'`Decimal`",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p(struct{ Decimal *decimal.Decimal }{p(decimal.RequireFromString("1.23"))}),
		},
		{
			name: "civil date",
			args: args{
				db:            db,
				conn:          db.Writes,
				ctx:           context.Background(),
				dest:          p(civil.Date{}),
				query:         "SELECT '2020-01-01'",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p(civil.Date{Year: 2020, Month: 1, Day: 1}),
		},
		{
			name: "civil date ptr",
			args: args{
				db:            db,
				conn:          db.Writes,
				ctx:           context.Background(),
				dest:          p(&civil.Date{}),
				query:         "SELECT '2020-01-01'",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p(&civil.Date{Year: 2020, Month: 1, Day: 1}),
		},
		{
			name: "civil date from nil",
			args: args{
				db:            db,
				conn:          db.Writes,
				ctx:           context.Background(),
				dest:          p(civil.Date{}),
				query:         "SELECT null",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p(civil.Date{}),
		},
		{
			name: "civil date in struct",
			args: args{
				db:            db,
				conn:          db.Writes,
				ctx:           context.Background(),
				dest:          p(struct{ Date civil.Date }{}),
				query:         "SELECT '2020-01-01' `Date`",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p(struct{ Date civil.Date }{civil.Date{Year: 2020, Month: 1, Day: 1}}),
		},
		{
			name: "civil date ptr in struct",
			args: args{
				db:            db,
				conn:          db.Writes,
				ctx:           context.Background(),
				dest:          p(struct{ Date *civil.Date }{}),
				query:         "SELECT '2020-01-01' `Date`",
				cacheDuration: 0,
				params:        nil,
			},
			wantErr:  false,
			wantDest: p(struct{ Date *civil.Date }{&civil.Date{Year: 2020, Month: 1, Day: 1}}),
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
		t            reflect.Type
		scannerFuncs map[reflect.Type]reflect.Value
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
		{
			name: "civil date w/o scanner func",
			args: args{
				t: reflect.TypeOf(civil.Date{}),
			},
			want: true,
		},
		{
			name: "civil date w/ scanner func",
			args: args{
				t: reflect.TypeOf(civil.Date{}),
				scannerFuncs: map[reflect.Type]reflect.Value{
					reflect.TypeOf((*civil.Date)(nil)): reflect.ValueOf(func(d *civil.Date) (driver.Value, error) {
						return nil, nil
					}),
				},
			},
			want: false,
		},
		{
			name: "civil date ptr w/o scanner func",
			args: args{
				t: reflect.TypeOf(&civil.Date{}),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isMultiValueElement(tt.args.t, tt.args.scannerFuncs); got != tt.want {
				t.Errorf("isMultiValueElement() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getElementTypeFromDest(t *testing.T) {
	type args struct {
		destRef      reflect.Value
		scannerFuncs map[reflect.Type]reflect.Value
	}
	tests := []struct {
		name         string
		args         args
		wantT        reflect.Type
		wantMultiRow bool
	}{
		{
			name: "slice",
			args: args{
				destRef: reflect.ValueOf(&[]int{}),
			},
			wantT:        reflect.TypeOf(int(0)),
			wantMultiRow: true,
		},
		{
			name: "slice of slices",
			args: args{
				destRef: reflect.ValueOf(&[][]int{}),
			},
			wantT:        reflect.TypeOf([]int{}),
			wantMultiRow: true,
		},
		{
			name: "int",
			args: args{
				destRef: reflect.ValueOf(p(int(0))),
			},
			wantT:        reflect.TypeOf(int(0)),
			wantMultiRow: false,
		},
		{
			name: "time",
			args: args{
				destRef: reflect.ValueOf(p(time.Time{})),
			},
			wantT:        reflect.TypeOf(time.Time{}),
			wantMultiRow: false,
		},
		{
			name: "decimal",
			args: args{
				destRef: reflect.ValueOf(p(decimal.Decimal{})),
			},
			wantT:        reflect.TypeOf(decimal.Decimal{}),
			wantMultiRow: false,
		},
		{
			name: "chan",
			args: args{
				destRef: reflect.ValueOf(p(make(chan int))),
			},
			wantT:        reflect.TypeOf(int(0)),
			wantMultiRow: true,
		},
		{
			name: "civil date",
			args: args{
				destRef: reflect.ValueOf(p(civil.Date{})),
				scannerFuncs: map[reflect.Type]reflect.Value{
					reflect.TypeOf((*civil.Date)(nil)): reflect.ValueOf(func(d *civil.Date, src any) error {
						return nil
					}),
				},
			},
			wantT:        reflect.TypeOf(civil.Date{}),
			wantMultiRow: false,
		},
		{
			name: "civil date w/o scanner func",
			args: args{
				destRef: reflect.ValueOf(p(civil.Date{})),
			},
			wantT:        reflect.TypeOf(civil.Date{}),
			wantMultiRow: false,
		},
		{
			name: "civil date ptr w/o scanner func",
			args: args{
				destRef: reflect.ValueOf(p(&civil.Date{})),
			},
			wantT:        reflect.TypeOf(&civil.Date{}),
			wantMultiRow: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotT, gotMultiRow := getElementTypeFromDest(tt.args.destRef, tt.args.scannerFuncs)
			if !reflect.DeepEqual(gotT, tt.wantT) {
				t.Errorf("getElementTypeFromDest() gotT = %v, want %v", gotT, tt.wantT)
			}
			if gotMultiRow != tt.wantMultiRow {
				t.Errorf("getElementTypeFromDest() gotMultiRow = %v, want %v", gotMultiRow, tt.wantMultiRow)
			}
		})
	}
}
