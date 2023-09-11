package mysql

import (
	"database/sql/driver"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"github.com/shopspring/decimal"
)

func Test_isMultiColumn(t *testing.T) {
	type args struct {
		t           reflect.Type
		valuerFuncs map[reflect.Type]reflect.Value
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "string",
			args: args{
				t:           reflect.TypeOf(""),
				valuerFuncs: nil,
			},
			want: false,
		},
		{
			name: "strings",
			args: args{
				t:           reflect.TypeOf([]string{}),
				valuerFuncs: nil,
			},
			want: true,
		},
		{
			name: "bytes",
			args: args{
				t:           reflect.TypeOf([]byte{}),
				valuerFuncs: nil,
			},
			want: false,
		},
		{
			name: "time",
			args: args{
				t:           reflect.TypeOf(time.Time{}),
				valuerFuncs: nil,
			},
			want: false,
		},
		{
			name: "misc struct",
			args: args{
				t:           reflect.TypeOf(struct{}{}),
				valuerFuncs: nil,
			},
			want: true,
		},
		{
			name: "civil date",
			args: args{
				t: reflect.TypeOf(civil.Date{}),
				valuerFuncs: map[reflect.Type]reflect.Value{
					reflect.TypeOf(civil.Date{}): reflect.ValueOf(func(d civil.Date) (driver.Value, error) {
						return nil, nil
					}),
				},
			},
		},
		{
			name: "civil date without the valuer func",
			args: args{
				t: reflect.TypeOf(civil.Date{}),
			},
			want: true,
		},
		{
			name: "decimal",
			args: args{
				t: reflect.TypeOf(decimal.Decimal{}),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isMultiColumn(tt.args.t, tt.args.valuerFuncs); got != tt.want {
				t.Errorf("isMultiColumn() = %v, want %v", got, tt.want)
			}
		})
	}
}
