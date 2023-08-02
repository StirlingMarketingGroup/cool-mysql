package mysql

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"
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

// InterpolateParams replaces the `@@` parameters in a query
// with their values from the map(s)
// Takes multiple "sets" of params for convenience, so we don't
// have to specify params if there aren't any, but each param will
// override the values of the previous. If there are 2 maps given,
// both with the key "ID", the last one will be used
func InterpolateParams(query string, params ...any) (replacedQuery string, normalizedParams Params, err error) {
	return interpolateParams(query, marshalOptNone, params...)
}

func interpolateParams(query string, opts marshalOpt, params ...any) (replacedQuery string, normalizedParams Params, err error) {
	if strings.Contains(query, "{{") {
		convertedParams := make([]Params, 0, len(params))
		for _, p := range params {
			convertedParams = append(convertedParams, convertToParams("param", p))
		}

		query, err = execTemplate(query, convertToParams("params", normalizeParams(convertedParams...)))
		if err != nil {
			return "", nil, err
		}
	}

	if !strings.Contains(query, "@@") {
		return query, nil, nil
	}

	queryTokens := parseQuery(query)
	if len(queryTokens) == 0 {
		return query, nil, nil
	}

	var firstParamName string
	for _, t := range queryTokens {
		if t.kind != queryTokenKindParam {
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
		return query, nil, nil
	}

	usedParams := make(map[string]struct{})

	s := stringsBuilderPool.Get().(*strings.Builder)
	defer stringsBuilderPool.Put(s)
	s.Reset()

	for _, t := range queryTokens {
		switch t.kind {
		case queryTokenKindParam:
			k := strings.ToLower(t.string[2:])
			if v, ok := normalizedParams[k]; ok {
				b, err := marshal(v, 0)
				if err != nil {
					return "", nil, err
				}

				s.Write(b)

				usedParams[k] = struct{}{}
				break
			}

			s.WriteString(t.string)
		default:
			s.WriteString(t.string)
		}
	}

	for k := range normalizedParams {
		if _, ok := usedParams[k]; !ok {
			delete(normalizedParams, k)
		}
	}

	return s.String(), normalizedParams, nil
}

type queryToken struct {
	string
	pos  int
	end  int
	kind queryTokenKind
}

type queryTokenKind int

const (
	queryTokenKindParam queryTokenKind = iota + 1
	queryTokenKindParen
	queryTokenKindString
	queryTokenKindWord
	queryTokenKindVar
	queryTokenKindComma
	queryTokenKindMisc
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
	}

	isWordChar := func(b byte) bool {
		return 'A' <= b && b <= 'Z' || 'a' <= b && b <= 'z' || '0' <= b && b <= '9' || b == '_'
	}

	consumeAllWordChars := func() {
	loop:
		for i < l {
			switch b := query[i]; {
			case isWordChar(b):
				next()
			default:
				break loop
			}
		}
		prev()
	}

	isParamChar := func(b byte) bool {
		return isWordChar(b) || b == '-' || b == '.'
	}

	consumeAllParamChars := func() {
	loop:
		for i < l {
			switch b := query[i]; {
			case isParamChar(b):
				next()
			default:
				break loop
			}
		}
		prev()
	}

	queryTokens := make([]queryToken, 0)

	pushToken := func(kind queryTokenKind) {
		if len(query[start:i+1]) == 0 {
			return
		}

		queryTokens = append(queryTokens, queryToken{
			string: query[start : i+1],
			pos:    start,
			end:    i,
			kind:   kind,
		})
	}

	for i < l {
		start = i
		switch b := query[i]; true {
		case b == '\'', b == '"', b == '`':
			next()
			consumeUntilEsc(b)
			next()

			pushToken(queryTokenKindString)
		case b == '(', b == ')':
			pushToken(queryTokenKindParen)
		case b == ',':
			pushToken(queryTokenKindComma)
		case b == '@':
			if i+2 < l && query[i+1] == '@' && isParamChar(query[i+2]) {
				nextN(2)
				consumeAllParamChars()

				pushToken(queryTokenKindParam)
			} else {
				next()
				consumeAllWordChars()

				pushToken(queryTokenKindVar)
			}
		case isWordChar(b):
			consumeAllWordChars()

			pushToken(queryTokenKindWord)
		default:
			pushToken(queryTokenKindMisc)
		}
		next()
	}

	return queryTokens
}

func Marshal(x any) ([]byte, error) {
	return marshal(x, 0)
}

type marshalOpt uint

const (
	marshalOptNone marshalOpt = 1 << iota
	marshalOptWrapSliceWithParens
	marshalOptJSONSlice
)

// marshal returns the interpolated param, encoding values that could have escaping issues.
// Strings and []byte are hex encoded so as to make extra sure nothing
// bad is let through
func marshal(x any, opts marshalOpt) ([]byte, error) {
	switch v := x.(type) {
	case bool:
		if !v {
			return []byte("0"), nil

		}
		return []byte("1"), nil
	case string:
		if len(v) == 0 {
			return []byte("''"), nil
		}
		return []byte(fmt.Sprintf("_utf8mb4 0x%x collate utf8mb4_unicode_ci", v)), nil
	case []byte:
		if v == nil {
			return []byte("null"), nil
		}
		if len(v) == 0 {
			return []byte("''"), nil
		}
		return []byte(fmt.Sprintf("0x%x", v)), nil
	case int:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case int8:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case int16:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case int32:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case int64:
		return []byte(strconv.FormatInt(v, 10)), nil
	case uint:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case uint8:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case uint16:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case uint32:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case uint64:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case complex64:
		return []byte(strconv.FormatComplex(complex128(v), 'E', -1, 64)), nil
	case complex128:
		return []byte(strconv.FormatComplex(complex128(v), 'E', -1, 64)), nil
	case float32:
		return []byte(strconv.FormatFloat(float64(v), 'E', -1, 64)), nil
	case float64:
		return []byte(strconv.FormatFloat(float64(v), 'E', -1, 64)), nil
	case time.Time:
		if v.IsZero() {
			return []byte("null"), nil
		}
		return []byte(fmt.Sprintf("convert_tz('%s','UTC',@@session.time_zone)", v.UTC().Format("2006-01-02 15:04:05.000000"))), nil
	case json.RawMessage:
		if v == nil {
			return []byte("null"), nil
		}
		if len(v) == 0 {
			return []byte("''"), nil
		}
		return []byte(fmt.Sprintf("_utf8mb4 0x%x collate utf8mb4_unicode_ci", v)), nil
	case Raw:
		return []byte(v), nil
	}

	v := reflect.ValueOf(x)
	if v.IsValid() && (v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface) {
		if v := v.Elem(); v.IsValid() {
			return marshal(v.Interface(), opts)
		}
	}

	// check the reflect kind, since we want to
	// deal with underlying value types if they didn't
	// explicitly set a way to be encoded
	v = reflectUnwrap(v)

	if !v.IsValid() {
		return []byte("null"), nil
	}

	// pv needs to always be a pointer to a value
	// a pointer to something can call pointer methods or value methods,
	// but a value can only call value methods
	pv := v
	if v.Kind() != reflect.Ptr {
		pv = reflect.New(v.Type())
		pv.Elem().Set(v)
	}

	if v, ok := pv.Interface().(driver.Valuer); ok {
		if pv.IsNil() {
			// but, if the pointer is nil and we try to call a value method, we get a dereference panic
			// so we need to check if the element type of the pointer has the method
			// if it does have the method, then we can't call it, because we're nil
			if _, ok := pv.Type().Elem().MethodByName("Value"); ok {
				return []byte("null"), nil
			}
		}

		v, err := v.Value()
		if err != nil {
			return nil, fmt.Errorf("cool-mysql: failed to call Value on driver.Valuer: %w", err)
		}
		return marshal(v, opts)
	}

	if vs, ok := pv.Interface().(Valueser); ok {
		if pv.IsNil() {
			if _, ok := pv.Type().Elem().MethodByName("Value"); ok {
				return []byte("null"), nil
			}
		}

		vs, err := vs.MySQLValues()
		if err != nil {
			return nil, fmt.Errorf("cool-mysql: failed to call MySQLValues on mysql.MySQLValues: %w", err)
		}
		return marshal(vs, opts)
	}

	if isNil(x) {
		return []byte("null"), nil
	}

	k := v.Kind()
	switch k {
	case reflect.Bool:
		return marshal(v.Bool(), opts)
	case reflect.String:
		return marshal(v.String(), opts)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return marshal(v.Int(), opts)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return marshal(v.Uint(), opts)
	case reflect.Complex64, reflect.Complex128:
		return marshal(v.Complex(), opts)
	case reflect.Float32, reflect.Float64:
		return marshal(v.Float(), opts)
	case reflect.Struct, reflect.Map:
		j, err := json.Marshal(x)
		if err != nil {
			return nil, fmt.Errorf("cool-mysql: failed to marshal struct to json: %w", err)
		}

		return marshal(json.RawMessage(j), opts)
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return marshal(v.Bytes(), opts)
		}

		if opts&marshalOptJSONSlice != 0 {
			j, err := json.Marshal(x)
			if err != nil {
				return nil, fmt.Errorf("cool-mysql: failed to marshal slice to json: %w", err)
			}

			return marshal(json.RawMessage(j), opts)
		}

		buf := new(bytes.Buffer)

		if opts&marshalOptWrapSliceWithParens != 0 {
			buf.WriteByte('(')
		}

		refLen := v.Len()
		if refLen == 0 {
			buf.WriteString("null")
		}
		for i := 0; i < refLen; i++ {
			if i != 0 {
				buf.WriteByte(',')
			}

			b, err := marshal(v.Index(i).Interface(), opts|marshalOptWrapSliceWithParens)
			if err != nil {
				return nil, err
			}
			buf.Write(b)
		}

		if opts&marshalOptWrapSliceWithParens != 0 {
			buf.WriteByte(')')
		}

		return buf.Bytes(), nil
	}

	return nil, fmt.Errorf("cool-mysql: not sure how to interpret %q of type %T", x, x)
}

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
		structFieldIndexes := StructFieldIndexes(t)

		p := make(Params, len(structFieldIndexes))

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
	case reflect.Slice, reflect.Array:
		l := rv.Len()
		p := make(Params, l)
		for i := 0; i < l; i++ {
			p[strconv.Itoa(i)] = rv.Index(i).Interface()
		}

		return p
	}

	return nil
}

func isSingleParam(t reflect.Type) bool {
	if t.Implements(valuerType) || t == timeType {
		return true
	}

	switch k := t.Kind(); k {
	case reflect.Map, reflect.Struct:
		return false
	default:
		return true
	}
}

// string replacer for double backticks and escaped backticks
var backtickReplacer = strings.NewReplacer("``", "`", "\\`", "`")

// removes surrounding backticks and unescapes interior ones
func parseName(s string) string {
	if len(s) < 2 {
		return s
	}

	if s[0] == '`' && s[len(s)-1] == '`' {
		s = s[1 : len(s)-1]
	}

	return backtickReplacer.Replace(s)
}

func execTemplate(q string, params Params) (string, error) {
	if !strings.Contains(q, "{{") {
		return q, nil
	}

	tmpl, err := template.New("query").Funcs(template.FuncMap{
		"marshal": func(x any) (string, error) {
			b, err := marshal(x, 0)
			if err != nil {
				return "", err
			}

			return string(b), nil
		},
	}).Parse(q)
	if err != nil {
		return "", fmt.Errorf("cool-mysql: failed to parse query template: %w", err)
	}

	s := stringsBuilderPool.Get().(*strings.Builder)
	defer stringsBuilderPool.Put(s)
	s.Reset()

	err = tmpl.Execute(s, params)
	if err != nil {
		return "", fmt.Errorf("cool-mysql: failed to execute query template: %w", err)
	}

	return s.String(), nil
}
