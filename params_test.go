package mysql

import (
	"reflect"
	"testing"
	"time"
)

func Test_normalizeParams(t *testing.T) {
	type args struct {
		params []Params
	}
	tests := []struct {
		name string
		args args
		want Params
	}{{
		name: "normalize params",
		args: args{
			params: []Params{
				{"Hello": "World", "Foo": "Bar", "hey": "There"},
				{"foo": "World II"},
			},
		},
		want: Params{"hello": "World", "foo": "World II", "hey": "There"},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeParams(tt.args.params...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("normalizeParams() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_convertToParams(t *testing.T) {
	type args struct {
		firstParamName string
		v              any
	}
	tests := []struct {
		name string
		args args
		want Params
	}{
		{
			name: "string",
			args: args{firstParamName: "foo", v: "bar"},
			want: Params{"foo": "bar"},
		},
		{
			name: "strings",
			args: args{firstParamName: "foo", v: []string{"bar", "yeet"}},
			want: Params{"foo": []string{"bar", "yeet"}},
		},
		{
			name: "params",
			args: args{firstParamName: "swick", v: Params{"foo": "bar"}},
			want: Params{"foo": "bar"},
		},
		{
			name: "struct",
			args: args{firstParamName: "swick", v: struct {
				Hello string
				World string `mysql:"-"`
				foo   string
				Bar   string `mysql:"test,omitempty"`
			}{"swick", "yeets", "blazeit", "w00t"}},
			want: Params{"Hello": "swick", "World": "yeets", "test": "w00t"},
		},
		{
			name: "map",
			args: args{firstParamName: "swick", v: map[int]any{1: "hello", 4: "world"}},
			want: Params{"1": "hello", "4": "world"},
		},
		{
			name: "null",
			args: args{firstParamName: "swick", v: nil},
			want: Params{"swick": nil},
		},
		{
			name: "time",
			args: args{firstParamName: "swick", v: time.Time{}},
			want: Params{"swick": time.Time{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := convertToParams(tt.args.firstParamName, tt.args.v); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("convertToParams() = %v, want %v", got, tt.want)
			}
		})
	}
}
