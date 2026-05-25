package mysql

import (
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"github.com/shopspring/decimal"
)

func Test_normalizeParams(t *testing.T) {
	type args struct {
		caseSensitive bool
		params        []Params
	}
	tests := []struct {
		name string
		args args
		want Params
	}{
		{
			name: "normalize params",
			args: args{
				caseSensitive: false,
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
				caseSensitive: false,
				params:        []Params{},
			},
			want: nil,
		},
		{
			name: "case sensitive",
			args: args{
				caseSensitive: true,
				params: []Params{
					{"Hello": "World", "Foo": "Bar", "hey": "There"},
					{"foo": "World II"},
				},
			},
			want: Params{"Hello": "World", "Foo": "Bar", "hey": "There", "foo": "World II"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := mergeParams(tt.args.caseSensitive, tt.args.params, nil); !reflect.DeepEqual(got, tt.want) {
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
			if got, _ := convertToParams(tt.args.firstParamName, tt.args.v); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("convertToParams() = %v, want %v", got, tt.want)
			}
		})
	}
}

type testDefaultStruct struct {
	Hello string `mysql:"hello,defaultzero"`
	World string `mysql:"world,omitempty"`
}

func TestInterpolateParams(t *testing.T) {
	simpleNormalizedParams, _ := mergeParams(false, []Params{{"1": "hello", "2": "world"}}, nil)
	sliceOfStringsNormalizedParams, _ := mergeParams(false, []Params{{"1": []string{"hello", "world"}}}, nil)

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
			wantNormalizedParams: simpleNormalizedParams,
		},
		{
			name: "slice of strings",
			args: args{
				query:  "SELECT * FROM `test` WHERE `foo` IN (@@1)",
				params: []any{[]string{"hello", "world"}},
			},
			wantReplacedQuery:    "SELECT * FROM `test` WHERE `foo` IN (_utf8mb4 0x68656c6c6f collate utf8mb4_unicode_ci,_utf8mb4 0x776f726c64 collate utf8mb4_unicode_ci)",
			wantNormalizedParams: sliceOfStringsNormalizedParams,
		},
		{
			name: "template error",
			args: args{
				query:  "SELECT * FROM `test` WHERE `foo` = {{.foo}",
				params: []any{Params{"foo": "bar"}},
			},
			wantErr: true,
		},
		{
			name: "template error",
			args: args{
				query:  "SELECT * FROM `test` WHERE `foo` = {{.foo}}{{end}}",
				params: []any{Params{"foo": "bar"}},
			},
			wantErr: true,
		},
		{
			name: "struct defaultzero w/ value",
			args: args{
				query:  "SELECT * FROM `test` WHERE `hello` = @@hello",
				params: []any{testDefaultStruct{Hello: "hello"}},
			},
			wantReplacedQuery:    "SELECT * FROM `test` WHERE `hello` = _utf8mb4 0x68656c6c6f collate utf8mb4_unicode_ci",
			wantNormalizedParams: Params{"hello": "hello"},
		},
		{
			name: "struct defaultzero w/ empty",
			args: args{
				query:  "SELECT * FROM `test` WHERE `hello` = @@hello",
				params: []any{testDefaultStruct{}},
			},
			wantReplacedQuery:    "SELECT * FROM `test` WHERE `hello` = default(`hello`)",
			wantNormalizedParams: Params{"hello": ""},
		},
		{
			name: "struct defaultzero omitempty w/ empty",
			args: args{
				query:  "SELECT * FROM `test` WHERE `hello` = @@world",
				params: []any{testDefaultStruct{}},
			},
			wantReplacedQuery:    "SELECT * FROM `test` WHERE `hello` = ''",
			wantNormalizedParams: Params{"world": ""},
		},
		{
			name: "param touching alias",
			args: args{
				query:  "SELECT @@foo`bar`",
				params: []any{Params{"foo": 1}},
			},
			wantReplacedQuery:    "SELECT 1 `bar`",
			wantNormalizedParams: Params{"foo": 1},
		},
		{
			name: "slice in SET clause json encodes",
			args: args{
				query:  "UPDATE `t` SET `col` = @@list",
				params: []any{Params{"list": []string{"a", "b"}}},
			},
			wantReplacedQuery:    "UPDATE `t` SET `col` = _utf8mb4 0x" + hex.EncodeToString([]byte(`["a","b"]`)) + " collate utf8mb4_unicode_ci",
			wantNormalizedParams: Params{"list": []string{"a", "b"}},
		},
		{
			name: "slice in NOT IN clause comma separates",
			args: args{
				query:  "SELECT * FROM `t` WHERE `c` NOT IN (@@list)",
				params: []any{Params{"list": []string{"a", "b"}}},
			},
			wantReplacedQuery:    "SELECT * FROM `t` WHERE `c` NOT IN (_utf8mb4 0x" + hex.EncodeToString([]byte("a")) + " collate utf8mb4_unicode_ci,_utf8mb4 0x" + hex.EncodeToString([]byte("b")) + " collate utf8mb4_unicode_ci)",
			wantNormalizedParams: Params{"list": []string{"a", "b"}},
		},
		{
			name: "slice in IN clause with neighbor parens comma separates",
			args: args{
				query:  "SELECT * FROM `t` WHERE `c` IN (1, (2+3), @@list)",
				params: []any{Params{"list": []int{4, 5}}},
			},
			wantReplacedQuery:    "SELECT * FROM `t` WHERE `c` IN (1, (2+3), 4,5)",
			wantNormalizedParams: Params{"list": []int{4, 5}},
		},
		// Issue #159: slice params inside json_array(...) were getting
		// JSON-encoded after #155, so `json_array(@@ids)` rendered as
		// `json_array("[1,2,3]")` = `["[1,2,3]"]` instead of `[1,2,3]`. The
		// downstream `json_overlaps(col, json_array(@@ids))` would never match.
		{
			name: "slice in json_array comma separates",
			args: args{
				query:  "select json_array(@@ids)",
				params: []any{Params{"ids": []int{2459149}}},
			},
			wantReplacedQuery:    "select json_array(2459149)",
			wantNormalizedParams: Params{"ids": []int{2459149}},
		},
		{
			name: "slice in json_overlaps→json_array nested comma separates",
			args: args{
				query:  "select json_overlaps(`c`, json_array(@@ids))",
				params: []any{Params{"ids": []int{1, 2, 3}}},
			},
			wantReplacedQuery:    "select json_overlaps(`c`, json_array(1,2,3))",
			wantNormalizedParams: Params{"ids": []int{1, 2, 3}},
		},
		{
			name: "string slice in concat comma separates",
			args: args{
				query:  "select concat(@@parts)",
				params: []any{Params{"parts": []string{"a", "b", "c"}}},
			},
			wantReplacedQuery: "select concat(" +
				"_utf8mb4 0x" + hex.EncodeToString([]byte("a")) + " collate utf8mb4_unicode_ci," +
				"_utf8mb4 0x" + hex.EncodeToString([]byte("b")) + " collate utf8mb4_unicode_ci," +
				"_utf8mb4 0x" + hex.EncodeToString([]byte("c")) + " collate utf8mb4_unicode_ci)",
			wantNormalizedParams: Params{"parts": []string{"a", "b", "c"}},
		},
		{
			name: "scalar in json_array unchanged",
			args: args{
				query:  "select json_array(@@id)",
				params: []any{Params{"id": 2459149}},
			},
			wantReplacedQuery:    "select json_array(2459149)",
			wantNormalizedParams: Params{"id": 2459149},
		},
		// User-defined functions stay JSON-encoded — we can't know the arg
		// shape, and that matches the v0.0.26 fix for any caller wrapping a
		// slice column-value in a custom function.
		{
			name: "slice in unknown function json encodes",
			args: args{
				query:  "select MY_FUNC(@@ids)",
				params: []any{Params{"ids": []int{1, 2}}},
			},
			wantReplacedQuery:    "select MY_FUNC(_utf8mb4 0x" + hex.EncodeToString([]byte(`[1,2]`)) + " collate utf8mb4_unicode_ci)",
			wantNormalizedParams: Params{"ids": []int{1, 2}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotReplacedQuery, gotNormalizedParams, err := InterpolateParams(tt.args.query, nil, nil, tt.args.params...)
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

func Test_paramInCommaSeparatedList(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{"WHERE a IN (@@x)", true},
		{"WHERE a in (@@x)", true},
		{"WHERE a NOT IN (@@x)", true},
		{"WHERE a IN(@@x)", true},
		{"WHERE a IN (1, 2, @@x)", true},
		{"WHERE a IN (1, (2+3), @@x)", true},
		{"WHERE a IN (SELECT b FROM t WHERE c IN (@@x))", true},
		{"SET col = @@x", false},
		{"VALUES (@@x)", false},
		{"@@x", false},
		{"WHERE (a = @@x)", false},
		{"MY_FUNC(@@x)", false},
		// Param inside an IN's subquery is NOT in the IN's value list — the
		// SELECT at depth 0 walking back tells us we're in subquery scope.
		{"WHERE a IN (SELECT b FROM t WHERE c = @@x)", false},
		// SELECT at nested depth (e.g. the subquery is itself one of several
		// IN-list elements) doesn't disqualify a sibling param.
		{"WHERE a IN ((SELECT b FROM t), @@x)", true},
		// Wrapping paren has nothing meaningful before it.
		{"(@@x)", false},
		{" (@@x)", false},
		// Inline SQL comments between IN and ( must not break detection.
		{"WHERE a IN /* ids */ (@@x)", true},
		{"WHERE a IN -- ids\n (@@x)", true},
		{"WHERE a IN -- ids\r (@@x)", true},
		{"WHERE a IN # ids\n (@@x)", true},
		{"WHERE a IN # ids\r (@@x)", true},
		// MySQL requires `--` to be followed by whitespace to be a comment;
		// otherwise it's arithmetic. We must still see and replace later params.
		{"col--@@x", false},
		// Known variadic SQL functions expand slice params comma-separated.
		// json_array is the main case from issue #159 — json_overlaps was
		// silently filtering out the user's own slice rows because the inner
		// json_array(@@slice) was JSON-encoding the slice.
		{"SELECT json_array(@@x)", true},
		{"select JSON_ARRAY(@@x)", true},
		{"SELECT json_array(1, @@x, 3)", true},
		{"WHERE json_overlaps(col, json_array(@@x))", true},
		{"SELECT concat(@@x)", true},
		{"SELECT concat('a', @@x, 'b')", true},
		{"SELECT concat_ws('-', @@x)", true},
		{"SELECT coalesce(@@x, 'd')", true},
		{"SELECT greatest(@@x)", true},
		{"SELECT least(1, @@x)", true},
		{"SELECT json_object('k', @@x)", true},
		{"SELECT field(col, @@x)", true},
		{"SELECT elt(1, @@x)", true},
		{"SELECT make_set(7, @@x)", true},
		{"SELECT interval(@@x, 1, 2, 3)", true},
		// JSON_EXTRACT takes a JSON doc and a path — not in the allowlist, so
		// slice @@x JSON-encodes (the right thing if someone passes a slice
		// they want treated as a JSON array document).
		{"SELECT json_extract(@@x, '$.foo')", false},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			tokens := parseQuery(tt.query)
			var idx int = -1
			for i, tok := range tokens {
				if tok.kind == queryTokenKindParam {
					idx = i
					break
				}
			}
			if idx < 0 {
				t.Fatalf("no param token found in %q", tt.query)
			}
			if got := paramInCommaSeparatedList(tokens, idx); got != tt.want {
				t.Errorf("paramInCommaSeparatedList(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}

type singleValueser struct{ v int }

func (s singleValueser) MySQLValues() ([]driver.Value, error) {
	return []driver.Value{s.v}, nil
}

// Test_InterpolateParams_ValueserScalar guards against Valueser returning a
// single-element []driver.Value getting JSON-encoded (as `[7]`) instead of
// expanded as the scalar `7`.
func Test_InterpolateParams_ValueserScalar(t *testing.T) {
	got, _, err := InterpolateParams("SET col = @@v", nil, nil, Params{"v": singleValueser{v: 7}})
	if err != nil {
		t.Fatal(err)
	}
	want := "SET col = 7"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// Test_InterpolateParams_DoubleDashArithmetic confirms `--` only triggers a
// SQL line comment when followed by whitespace; otherwise it's arithmetic
// (`col--@@x` is `col - (-@@x)`) and the param must still be interpolated.
func Test_InterpolateParams_DoubleDashArithmetic(t *testing.T) {
	got, _, err := InterpolateParams("SELECT col--@@x", nil, nil, Params{"x": 3})
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT col--3"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// Test_parseQuery_UnterminatedBlockComment ensures the tokenizer doesn't panic
// on `/*` at end-of-input — it should consume the rest as a misc token and
// let the DB reject the malformed query.
func Test_parseQuery_UnterminatedBlockComment(t *testing.T) {
	for _, q := range []string{"/*", "SELECT 1 /*", "SELECT 1 /*a*"} {
		t.Run(q, func(t *testing.T) {
			_ = parseQuery(q)
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
		x           any
		opt         marshalOpt
		fieldName   string
		valuerFuncs map[reflect.Type]reflect.Value
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
			want: []byte("1.23"),
		},
		{
			name: "decimal.Decimal ptr",
			args: args{
				x: p(decimal.NewFromFloat(1.23)),
			},
			want: []byte("1.23"),
		},
		{
			name: "decimal.Decimal ptr zero",
			args: args{
				x: p(decimal.Zero),
			},
			want: []byte("0"),
		},
		{
			name: "decimal.Decimal zero",
			args: args{
				x: decimal.Zero,
			},
			want: []byte("0"),
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
		{
			name: "slice of ints w wrap",
			args: args{
				x:   []int{1, 2, 3},
				opt: marshalOptWrapSliceWithParens,
			},
			want: []byte("(1,2,3)"),
		},
		{
			name: "slice of ints w json",
			args: args{
				x:   []int{1, 2, 3},
				opt: marshalOptJSONSlice,
			},
			want: []byte("_utf8mb4 0x" + hex.EncodeToString([]byte("[1,2,3]")) + " collate utf8mb4_unicode_ci"),
		},
		{
			name: "time",
			args: args{
				x: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			want: []byte("'2020-01-01 00:00:00.000000'"),
		},
		{
			name: "civil date",
			args: args{
				x: civil.Date{Year: 2020, Month: 1, Day: 1},
				valuerFuncs: map[reflect.Type]reflect.Value{
					reflect.TypeOf(civil.Date{}): reflect.ValueOf(func(d civil.Date) (driver.Value, error) {
						return d.In(time.UTC), nil
					}),
				},
			},
			want: []byte("'2020-01-01 00:00:00.000000'"),
		},
		{
			name: "civil date ptr",
			args: args{
				x: p(civil.Date{Year: 2020, Month: 1, Day: 1}),
				valuerFuncs: map[reflect.Type]reflect.Value{
					reflect.TypeOf((*civil.Date)(nil)): reflect.ValueOf(func(d *civil.Date) (driver.Value, error) {
						return d.In(time.UTC), nil
					}),
				},
			},
			want: []byte("'2020-01-01 00:00:00.000000'"),
		},
		{
			name: "civil date nil ptr",
			args: args{
				x: (*civil.Date)(nil),
				valuerFuncs: map[reflect.Type]reflect.Value{
					reflect.TypeOf((*civil.Date)(nil)): reflect.ValueOf(func(d *civil.Date) (driver.Value, error) {
						if d == nil {
							return nil, nil
						}
						return d.In(time.UTC), nil
					}),
				},
			},
			want: []byte("null"),
		},
		{
			name: "civil date with ptr func",
			args: args{
				x: civil.Date{Year: 2020, Month: 1, Day: 1},
				valuerFuncs: map[reflect.Type]reflect.Value{
					reflect.TypeOf((*civil.Date)(nil)): reflect.ValueOf(func(d *civil.Date) (driver.Value, error) {
						if d == nil {
							return nil, nil
						}
						return d.String(), nil
					}),
				},
			},
			want: []byte("_utf8mb4 0x" + hex.EncodeToString([]byte(`2020-01-01`)) + " collate utf8mb4_unicode_ci"),
		},
		{
			name: "civil date ptr with value func",
			args: args{
				x: &civil.Date{Year: 2020, Month: 1, Day: 1},
				valuerFuncs: map[reflect.Type]reflect.Value{
					reflect.TypeOf(civil.Date{}): reflect.ValueOf(func(d civil.Date) (driver.Value, error) {
						return d.String(), nil
					}),
				},
			},
			want: []byte("_utf8mb4 0x" + hex.EncodeToString([]byte(`2020-01-01`)) + " collate utf8mb4_unicode_ci"),
		},
		{
			name: "nil civil date ptr with value func",
			args: args{
				x: (*civil.Date)(nil),
				valuerFuncs: map[reflect.Type]reflect.Value{
					reflect.TypeOf(civil.Date{}): reflect.ValueOf(func(d civil.Date) (driver.Value, error) {
						return d.String(), nil
					}),
				},
			},
			want: []byte("null"),
		},
		{name: "bool true", args: args{x: true}, want: []byte("1")},
		{name: "bool false", args: args{x: false}, want: []byte("0")},
		{name: "empty string", args: args{x: ""}, want: []byte("''")},
		{name: "nil []byte", args: args{x: []byte(nil)}, want: []byte("null")},
		{name: "empty []byte", args: args{x: []byte{}}, want: []byte("''")},
		{name: "populated []byte", args: args{x: []byte{0xde, 0xad}}, want: []byte("0xdead")},
		{name: "int", args: args{x: int(-12)}, want: []byte("-12")},
		{name: "int8", args: args{x: int8(-8)}, want: []byte("-8")},
		{name: "int16", args: args{x: int16(-16)}, want: []byte("-16")},
		{name: "int32", args: args{x: int32(-32)}, want: []byte("-32")},
		{name: "int64", args: args{x: int64(-64)}, want: []byte("-64")},
		{name: "uint", args: args{x: uint(12)}, want: []byte("12")},
		{name: "uint8", args: args{x: uint8(8)}, want: []byte("8")},
		{name: "uint16", args: args{x: uint16(16)}, want: []byte("16")},
		{name: "uint32", args: args{x: uint32(32)}, want: []byte("32")},
		{name: "uint64", args: args{x: uint64(64)}, want: []byte("64")},
		{name: "float32", args: args{x: float32(1.5)}, want: []byte(strconv.FormatFloat(float64(float32(1.5)), 'E', -1, 64))},
		{name: "float64", args: args{x: float64(2.5)}, want: []byte(strconv.FormatFloat(2.5, 'E', -1, 64))},
		{name: "complex64", args: args{x: complex64(complex(1, 2))}, want: []byte(strconv.FormatComplex(complex128(complex(float32(1), float32(2))), 'E', -1, 64))},
		{name: "complex128", args: args{x: complex(3.0, 4.0)}, want: []byte(strconv.FormatComplex(complex(3.0, 4.0), 'E', -1, 64))},
		{name: "time zero", args: args{x: time.Time{}}, want: []byte("null")},
		{name: "civil date zero", args: args{x: civil.Date{}}, want: []byte("null")},
		{name: "civil date non-zero", args: args{x: civil.Date{Year: 2020, Month: 1, Day: 1}}, want: []byte("'2020-01-01'")},
		{name: "nil json.RawMessage", args: args{x: json.RawMessage(nil)}, want: []byte("null")},
		{name: "empty json.RawMessage", args: args{x: json.RawMessage{}}, want: []byte("''")},
		{name: "populated json.RawMessage", args: args{x: json.RawMessage(`{"a":1}`)}, want: []byte("_utf8mb4 0x" + hex.EncodeToString([]byte(`{"a":1}`)) + " collate utf8mb4_unicode_ci")},
		{name: "Raw", args: args{x: Raw("NOW()")}, want: []byte("NOW()")},
		{
			name: "defaultzero with fieldName",
			args: args{
				x:         0,
				opt:       marshalOptDefaultZero,
				fieldName: "col",
			},
			want: []byte("default(`col`)"),
		},
		{
			name: "defaultzero without fieldName",
			args: args{
				x:   0,
				opt: marshalOptDefaultZero,
			},
			want: []byte("default"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := marshal(tt.args.x, tt.args.opt, tt.args.fieldName, tt.args.valuerFuncs, time.UTC)
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

func Test_execTemplate(t *testing.T) {
	type args struct {
		q      string
		params Params
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "no template",
			args: args{
				q:      "SELECT * FROM `test`",
				params: nil,
			},
			want:    "SELECT * FROM `test`",
			wantErr: false,
		},
		{
			name: "template",
			args: args{
				q:      "SELECT * FROM `test` WHERE `foo` = {{.foo}}",
				params: Params{"foo": "bar"},
			},
			want:    "SELECT * FROM `test` WHERE `foo` = bar",
			wantErr: false,
		},
		{
			name: "marshal",
			args: args{
				q:      "SELECT * FROM `test` WHERE `foo` = {{marshal .foo}}",
				params: Params{"foo": "bar"},
			},
			want:    "SELECT * FROM `test` WHERE `foo` = _utf8mb4 0x626172 collate utf8mb4_unicode_ci",
			wantErr: false,
		},
		{
			name: "template error",
			args: args{
				q:      "SELECT * FROM `test` WHERE `foo` = {{.foo}",
				params: Params{"foo": "bar"},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "case sensitive",
			args: args{
				q:      "SELECT * FROM `test` WHERE `foo` = {{.Foo}}",
				params: Params{"foo": "bar"},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "err on missing key",
			args: args{
				q:      "SELECT * FROM `test` WHERE `foo` = {{.foo}}",
				params: Params{"bar": "bar"},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "upper case param err",
			args: args{
				q:      "SELECT * FROM `test` WHERE `foo` = {{.FOO}}",
				params: Params{"foo": "bar"},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "upper case param",
			args: args{
				q:      "SELECT * FROM `test` WHERE `foo` = {{.FOO}}",
				params: Params{"FOO": "bar"},
			},
			want:    "SELECT * FROM `test` WHERE `foo` = bar",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := execTemplate(tt.args.q, tt.args.params, nil, nil, time.UTC)
			if (err != nil) != tt.wantErr {
				t.Errorf("execTemplate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("execTemplate() = %v, want %v", got, tt.want)
			}
		})
	}
}
