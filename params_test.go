package mysql

import (
	"encoding/hex"
	"reflect"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func Test_normalizeParams(t *testing.T) {
	type args struct {
		params []Params
	}
	tests := []struct {
		name string
		args args
		want Params
	}{
		{
			name: "normalize params",
			args: args{
				params: []Params{
					{"Hello": "World", "Foo": "Bar", "hey": "There"},
					{"foo": "World II"},
				},
			},
			want: Params{"hello": "World", "foo": "World II", "hey": "There"},
		},
		{
			name: "empty",
			args: args{
				params: []Params{},
			},
			want: nil,
		},
	}
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
			want: Params{"Hello": "swick", "World": "yeets", "Bar": "w00t"},
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
		{
			name: "slice",
			args: args{firstParamName: "foo", v: []any{1, 2, 3}},
			want: Params{"foo": []any{1, 2, 3}},
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

func TestInterpolateParams(t *testing.T) {
	type args struct {
		query  string
		params []any
	}
	tests := []struct {
		name                 string
		args                 args
		wantReplacedQuery    string
		wantNormalizedParams Params
		wantErr              bool
	}{
		{
			name: "simple",
			args: args{
				query:  "SELECT * FROM `test` WHERE `foo` = @@1 AND `bar` = @@2",
				params: []any{Params{"1": "hello", "2": "world"}},
			},
			wantReplacedQuery:    "SELECT * FROM `test` WHERE `foo` = _utf8mb4 0x68656c6c6f collate utf8mb4_unicode_ci AND `bar` = _utf8mb4 0x776f726c64 collate utf8mb4_unicode_ci",
			wantNormalizedParams: normalizeParams(Params{"1": "hello", "2": "world"}),
		},
		{
			name: "slice of strings",
			args: args{
				query:  "SELECT * FROM `test` WHERE `foo` IN (@@1)",
				params: []any{[]string{"hello", "world"}},
			},
			wantReplacedQuery:    "SELECT * FROM `test` WHERE `foo` IN (_utf8mb4 0x68656c6c6f collate utf8mb4_unicode_ci,_utf8mb4 0x776f726c64 collate utf8mb4_unicode_ci)",
			wantNormalizedParams: normalizeParams(Params{"1": []string{"hello", "world"}}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotReplacedQuery, gotNormalizedParams, err := InterpolateParams(tt.args.query, tt.args.params...)
			if (err != nil) != tt.wantErr {
				t.Errorf("InterpolateParams() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotReplacedQuery != tt.wantReplacedQuery {
				t.Errorf("InterpolateParams() gotReplacedQuery = %v, want %v", gotReplacedQuery, tt.wantReplacedQuery)
			}
			if !reflect.DeepEqual(gotNormalizedParams, tt.wantNormalizedParams) {
				t.Errorf("InterpolateParams() gotNormalizedParams = %v, want %v", gotNormalizedParams, tt.wantNormalizedParams)
			}
		})
	}
}

func Test_parseQuery(t *testing.T) {
	type args struct {
		query string
	}
	tests := []struct {
		name string
		args args
		want []queryToken
	}{
		{
			name: "double back tick",
			args: args{query: "`hello``world` 'don\\'t test me'"},
			want: []queryToken{
				{string: "`hello``world`", pos: 0, end: 13, kind: queryTokenKindString},
				{string: " ", pos: 14, end: 14, kind: queryTokenKindMisc},
				{string: "'don\\'t test me'", pos: 15, end: 30, kind: queryTokenKindString},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseQuery(tt.args.query); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseName(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "simple",
			args: args{s: "foo"},
			want: "foo",
		},
		{
			name: "back tick",
			args: args{s: "`foo`"},
			want: "foo",
		},
		{
			name: "double back tick",
			args: args{s: "`foo``bar`"},
			want: "foo`bar",
		},
		{
			name: "backslash back tick",
			args: args{s: "`foo\\``"},
			want: "foo`",
		},
		{
			name: "single letter",
			args: args{s: "f"},
			want: "f",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseName(tt.args.s); got != tt.want {
				t.Errorf("parseName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_marshal(t *testing.T) {
	type args struct {
		x     any
		depth int
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "func",
			args: args{
				x: func() {},
			},
			wantErr: true,
		},
		{
			name: "chan",
			args: args{
				x: make(chan int),
			},
			wantErr: true,
		},
		{
			name: "map",
			args: args{
				x: map[string]int{
					"foo": 1,
					"bar": 2,
				},
			},
			want: []byte("_utf8mb4 0x" + hex.EncodeToString([]byte(`{"bar":2,"foo":1}`)) + " collate utf8mb4_unicode_ci"),
		},
		{
			name: "struct",
			args: args{
				x: struct {
					Foo int
					Bar int
				}{
					Foo: 1,
					Bar: 2,
				},
			},
			want: []byte("_utf8mb4 0x" + hex.EncodeToString([]byte(`{"Foo":1,"Bar":2}`)) + " collate utf8mb4_unicode_ci"),
		},
		{
			name: "slice of ints",
			args: args{
				x: []int{1, 2, 3},
			},
			want: []byte("1,2,3"),
		},
		{
			name: "decimal.Decimal",
			args: args{
				x: decimal.NewFromFloat(1.23),
			},
			want: []byte("_utf8mb4 0x" + hex.EncodeToString([]byte(`1.23`)) + " collate utf8mb4_unicode_ci"),
		},
		{
			name: "decimal.Decimal ptr",
			args: args{
				x: p(decimal.NewFromFloat(1.23)),
			},
			want: []byte("_utf8mb4 0x" + hex.EncodeToString([]byte(`1.23`)) + " collate utf8mb4_unicode_ci"),
		},
		{
			name: "decimal.Decimal ptr zero",
			args: args{
				x: p(decimal.Zero),
			},
			want: []byte("_utf8mb4 0x" + hex.EncodeToString([]byte(`0`)) + " collate utf8mb4_unicode_ci"),
		},
		{
			name: "decimal.Decimal zero",
			args: args{
				x: decimal.Zero,
			},
			want: []byte("_utf8mb4 0x" + hex.EncodeToString([]byte(`0`)) + " collate utf8mb4_unicode_ci"),
		},
		{
			name: "decimal.Decimal nil ptr",
			args: args{
				x: (*decimal.Decimal)(nil),
			},
			want: []byte("null"),
		},
		{
			name: "untyped nil",
			args: args{
				x: nil,
			},
			want: []byte("null"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := marshal(tt.args.x, tt.args.depth)
			if (err != nil) != tt.wantErr {
				t.Errorf("marshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("marshal() = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}
