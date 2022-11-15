package mysql

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// Params are a map of paramterer names to values
// use in the query like `select @@Name`
type Params map[string]any

var stringsBuilderPool = sync.Pool{
	New: func() any {
		return new(strings.Builder)
	},
}

var MaxTime = time.Unix((1<<31)-1, 999999999)

var BuiltInParams = Params{
	"MaxTime": MaxTime,
}

func normalizeParams(params ...Params) Params {
	if len(params) == 0 {
		return nil
	}

	normalizedParams := make(Params)
	for _, p := range params {
		for k, v := range p {
			normalizedParams[strings.ToLower(k)] = v
		}
	}

	return normalizedParams
}

// InlineParams replaces the `@@` parameters in a query
// with their values from the map(s)
// Takes multiple "sets" of params for convenience, so we don't
// have to specify params if there aren't any, but each param will
// override the values of the previous. If there are 2 maps given,
// both with the key "ID", the last one will be used
func InlineParams(query string, params ...any) (replacedQuery string, normalizedParams Params) {
	if !strings.Contains(query, "@@") {
		return query, nil
	}

	queryTokens := parseQuery(query)
	if len(queryTokens) == 0 {
		return query, nil
	}

	var firstParamName string
	for _, t := range parseQuery(query) {
		if t.kind == queryTokenKindRaw {
			continue
		}

		firstParamName = t.string[2:]
		break
	}

	convertedParams := make([]Params, 0, len(params))
	for _, p := range params {
		convertedParams = append(convertedParams, convertToParams(firstParamName, p))
	}

	allParams := append(make([]Params, 0, len(params)+2), Params{"now": time.Now()}, BuiltInParams)
	allParams = append(allParams, convertedParams...)

	normalizedParams = normalizeParams(allParams...)

	if len(normalizedParams) == 0 {
		return query, nil
	}

	usedParams := make(map[string]struct{})

	s := stringsBuilderPool.Get().(*strings.Builder)
	defer stringsBuilderPool.Put(s)
	s.Reset()

	for _, t := range queryTokens {
		switch t.kind {
		case queryTokenKindRaw:
			s.WriteString(t.string)
		case queryTokenKindParam:
			k := strings.ToLower(t.string[2:])
			if v, ok := normalizedParams[k]; ok {
				WriteEncoded(s, v, true)

				usedParams[k] = struct{}{}
				break
			}

			s.WriteString(t.string)
		default:
			panic(fmt.Errorf("unknown query token kind of %q", t.kind))
		}
	}

	for k := range normalizedParams {
		if _, ok := usedParams[k]; !ok {
			delete(normalizedParams, k)
		}
	}

	return s.String(), normalizedParams
}

type queryToken struct {
	string
	pos  int
	end  int
	kind queryTokenKind
}

type queryTokenKind int

const (
	queryTokenKindParam queryTokenKind = iota
	queryTokenKindRaw
)

func parseQuery(query string) []queryToken {
	i := 0
	start := 0
	l := len(query)

	next := func() {
		i++
	}

	nextN := func(c int) {
		i += c
	}

	prev := func() {
		i--
	}

	consumeUntilEsc := func(b byte) {
	loop:
		for i < l {
			switch query[i] {
			case b:
				if i+1 < l && query[i+1] == b {
					nextN(2)
				} else {
					break loop
				}
			case '\\':
				nextN(2)
			default:
				next()
			}
		}
		prev()
		if i >= l {
			i = l - 1
		}
	}

	consumeAllAlphaNum := func() {
	loop:
		for i < l {
			switch b := query[i]; {
			case 'A' <= b && b <= 'Z', 'a' <= b && b <= 'z', '0' <= b && b <= '9':
				next()
			default:
				prev()
				break loop
			}
		}
		if i >= l {
			i = l - 1
		}
	}

	queryTokens := make([]queryToken, 0)

	pushToken := func(kind queryTokenKind) {
		if len(query[start:i]) == 0 {
			return
		}

		queryTokens = append(queryTokens, queryToken{
			string: query[start:i],
			pos:    start,
			end:    i,
			kind:   kind,
		})
	}

	for i < l {
		switch b := query[i]; b {
		case '\'', '"', '`':
			next()
			consumeUntilEsc(b)
			next()
		case '@':
			if i+2 < l && query[i+1] == '@' {
				pushToken(queryTokenKindRaw)
				start = i

				nextN(2)
				consumeAllAlphaNum()
				next()

				pushToken(queryTokenKindParam)
				start = i
			}
		}
		next()
	}
	if start < l {
		pushToken(queryTokenKindRaw)
	}

	return queryTokens
}

// Encoder is a type with it's own cool mysql
// encode method for safe replacing
type Encoder interface {
	CoolMySQLEncode(Builder)
}

// Builder is a writeable buffer that the encoding will write to
type Builder interface {
	Write(p []byte) (n int, err error)
	WriteByte(c byte) error
	WriteString(s string) (int, error)
	Grow(n int)
}

type nestedValue struct {
	x any
}

// WriteEncoded takes a string builder and any value and writes it
// safely to the query, encoding values that could have escaping issues.
// Strings and []byte are hex encoded so as to make extra sure nothing
// bad is let through
func WriteEncoded(s Builder, x any, possiblyNull bool) {
	nested, _ := x.(*nestedValue)
	if nested != nil {
		x = nested.x
	}

	if possiblyNull && isNil(x) {
		s.WriteString("null")
		return
	}

	switch v := x.(type) {
	case Encoder:
		v.CoolMySQLEncode(s)
		return
	case bool:
		if v {
			s.WriteByte('1')
		} else {
			s.WriteByte('0')
		}
		return
	case string:
		if len(v) != 0 {
			s.WriteString("_utf8mb4 0x")
			hex.NewEncoder(s).Write([]byte(v))
			s.WriteString(" collate utf8mb4_unicode_ci")
		} else {
			s.WriteString("''")
		}
		return
	case []byte:
		if len(v) != 0 {
			s.WriteString("0x")
			hex.NewEncoder(s).Write([]byte(v))
		} else {
			s.WriteString("''")
		}
		return
	case int:
		s.WriteString(strconv.FormatInt(int64(v), 10))
		return
	case int8:
		s.WriteString(strconv.FormatInt(int64(v), 10))
		return
	case int16:
		s.WriteString(strconv.FormatInt(int64(v), 10))
		return
	case int32:
		s.WriteString(strconv.FormatInt(int64(v), 10))
		return
	case int64:
		s.WriteString(strconv.FormatInt(v, 10))
		return
	case uint:
		s.WriteString(strconv.FormatUint(uint64(v), 10))
		return
	case uint8:
		s.WriteString(strconv.FormatUint(uint64(v), 10))
		return
	case uint16:
		s.WriteString(strconv.FormatUint(uint64(v), 10))
		return
	case uint32:
		s.WriteString(strconv.FormatUint(uint64(v), 10))
		return
	case uint64:
		s.WriteString(strconv.FormatUint(uint64(v), 10))
		return
	case complex64:
		s.WriteString(strconv.FormatComplex(complex128(v), 'E', -1, 64))
		return
	case complex128:
		s.WriteString(strconv.FormatComplex(complex128(v), 'E', -1, 64))
		return
	case float32:
		s.WriteString(strconv.FormatFloat(float64(v), 'E', -1, 64))
		return
	case float64:
		s.WriteString(strconv.FormatFloat(float64(v), 'E', -1, 64))
		return
	case decimal.Decimal:
		s.WriteString(v.String())
		return
	case time.Time:
		s.WriteString("convert_tz('")
		s.WriteString(v.UTC().Format("2006-01-02 15:04:05.000000"))
		s.WriteString("','UTC',@@session.time_zone)")
		return
	case uuid.UUID:
		s.WriteByte('\'')
		s.WriteString(v.String())
		s.WriteByte('\'')
		return
	case json.RawMessage:
		if len(v) != 0 {
			s.WriteString("_utf8mb4 0x")
			hex.NewEncoder(s).Write(v)
			s.WriteString(" collate utf8mb4_unicode_ci")
		} else {
			s.WriteString("''")
		}
		return
	}

	// check the reflect kind, since we want to
	// deal with underlying value types if they didn't
	// explicitly set a way to be encoded
	ref := reflect.ValueOf(x)
	kind := ref.Kind()
	switch kind {
	case reflect.Ptr:
		WriteEncoded(s, ref.Elem().Interface(), true)
		return
	case reflect.Bool:
		v := ref.Bool()
		if v {
			s.WriteByte('1')
		} else {
			s.WriteByte('0')
		}
		return
	case reflect.String:
		v := ref.String()
		if len(v) != 0 {
			s.WriteString("_utf8mb4 0x")
			hex.NewEncoder(s).Write([]byte(v))
			s.WriteString(" collate utf8mb4_unicode_ci")
		} else {
			s.WriteString("''")
		}
		return
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		s.WriteString(strconv.FormatInt(ref.Int(), 10))
		return
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		s.WriteString(strconv.FormatUint(ref.Uint(), 10))
		return
	case reflect.Complex64, reflect.Complex128:
		s.WriteString(strconv.FormatComplex(ref.Complex(), 'E', -1, 64))
		return
	case reflect.Float32, reflect.Float64:
		s.WriteString(strconv.FormatFloat(ref.Float(), 'E', -1, 64))
		return
	case reflect.Slice:
		switch subKind := ref.Type().Elem().Kind(); subKind {
		case reflect.Uint8:
			v := ref.Bytes()
			if len(v) != 0 {
				s.WriteString("0x")
				hex.NewEncoder(s).Write(v)
			} else {
				s.WriteString("''")
			}
			return
		}
	}

	if kind == reflect.Slice || kind == reflect.Map {
		if nested != nil {
			s.WriteByte('(')
		}

		refLen := ref.Len()
		if refLen == 0 {
			s.WriteString("null")
		}
		for i := 0; i < refLen; i++ {
			if i != 0 {
				s.WriteByte(',')
			}

			WriteEncoded(s, &nestedValue{ref.Index(i).Interface()}, true)
		}

		if nested != nil {
			s.WriteByte(')')
		}

		return
	}

	panic(fmt.Errorf("not sure how to interpret %q of type %T", x, x))
}

var encoderType = reflect.TypeOf((*Encoder)(nil)).Elem()
var paramsType = reflect.TypeOf((*Params)(nil)).Elem()

func convertToParams(firstParamName string, v any) Params {
	r := reflect.ValueOf(v)

	if !r.IsValid() {
		return Params{firstParamName: v}
	}

	rv := reflect.Indirect(r)
	t := r.Type()
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	if isSingleParam(t) || isNil(v) {
		return Params{firstParamName: v}
	}

	if t == paramsType {
		return rv.Interface().(Params)
	}

	switch k := t.Kind(); k {
	case reflect.Struct:
		p := make(Params)

		structFieldIndexes := StructFieldIndexes(t)
		for _, i := range structFieldIndexes {
			f := t.FieldByIndex(i)

			if !f.IsExported() {
				continue
			}

			p[f.Name] = rv.FieldByIndex(i).Interface()
		}

		return p
	case reflect.Map:
		p := make(Params)
		for _, k := range rv.MapKeys() {
			p[fmt.Sprint(k.Interface())] = rv.MapIndex(k).Interface()
		}

		return p
	}

	return nil
}

func isSingleParam(t reflect.Type) bool {
	if reflect.New(t).Type().Implements(encoderType) || t == timeType {
		return true
	}

	switch k := t.Kind(); k {
	case reflect.Map, reflect.Struct:
		return false
	}

	return true
}
