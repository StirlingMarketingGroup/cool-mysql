package mysql

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"github.com/shopspring/decimal"
)

func Test_convertAssignRows(t *testing.T) {
	type args struct {
		dest         any
		src          any
		scannerFuncs map[reflect.Type]reflect.Value
	}

	civilDateScannerFuncs := map[reflect.Type]reflect.Value{
		reflect.TypeOf((*civil.Date)(nil)): reflect.ValueOf(func(dest *civil.Date, src any) error {
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
		}),
	}

	tests := []struct {
		name    string
		args    args
		want    any
		wantErr bool
	}{
		{
			name: "string",
			args: args{
				dest: new(string),
				src:  "test",
			},
			want: "test",
		},
		{
			name: "time",
			args: args{
				dest: new(time.Time),
				src:  time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			want: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "decimal",
			args: args{
				dest: new(decimal.Decimal),
				src:  "1.23",
			},
			want: decimal.NewFromFloat(1.23),
		},
		{
			name: "civil date w/o scanner func",
			args: args{
				dest: new(civil.Date),
				src:  "2020-01-01",
			},
			wantErr: true,
		},
		{
			name: "civil date w/ scanner func",
			args: args{
				dest:         new(civil.Date),
				src:          "2020-01-01",
				scannerFuncs: civilDateScannerFuncs,
			},
			want: civil.Date{Year: 2020, Month: 1, Day: 1},
		},
		{
			name: "civil date w/ scanner func, invalid date string",
			args: args{
				dest:         new(civil.Date),
				src:          "2020-01-01 00:00:00",
				scannerFuncs: civilDateScannerFuncs,
			},
			wantErr: true,
		},
		{
			name: "civil date w/ scanner func, from bytes",
			args: args{
				dest:         new(civil.Date),
				src:          []byte("2020-01-01 00:00:00"),
				scannerFuncs: civilDateScannerFuncs,
			},
			wantErr: true,
		},
		{
			name: "civil date w/ scanner func, from time",
			args: args{
				dest:         new(civil.Date),
				src:          time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
				scannerFuncs: civilDateScannerFuncs,
			},
			want: civil.Date{Year: 2020, Month: 1, Day: 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := convertAssignRows(tt.args.dest, tt.args.src, tt.args.scannerFuncs); (err != nil) != tt.wantErr {
				t.Errorf("convertAssignRows() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.want != nil && !reflect.DeepEqual(reflect.ValueOf(tt.args.dest).Elem().Interface(), tt.want) {
				t.Errorf("convertAssignRows() got = %v, want %v", tt.args.dest, tt.want)
			}
		})
	}
}
