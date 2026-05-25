package mysql

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"cloud.google.com/go/civil"
	"github.com/fatih/structtag"
	"github.com/shopspring/decimal"
)

// Params are a map of parameter names to values
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

func mergeParams(caseSensitive bool, params []Params, paramMetas []map[string]paramMeta) (Params, map[string]paramMeta) {
	if len(params) == 0 {
		return nil, nil
	}

	mergeParams := make(Params)
	for _, p := range params {
		for k, v := range p {
			if !caseSensitive {
				mergeParams[strings.ToLower(k)] = v
			} else {
				mergeParams[k] = v
			}
		}
	}

	mergedParamMetas := make(map[string]paramMeta)
	for _, pm := range paramMetas {
		for k, v := range pm {
			if !caseSensitive {
				mergedParamMetas[strings.ToLower(k)] = v
			} else {
				mergedParamMetas[k] = v
			}
		}
	}

	return mergeParams, mergedParamMetas
}

// InterpolateParams replaces the `@@` parameters in a query
// with their values from the map(s)
// Takes multiple "sets" of params for convenience, so we don't
// have to specify params if there aren't any, but each param will
// override the values of the previous. If there are 2 maps given,
// both with the key "ID", the last one will be used.
//
// time.Time params are formatted as naive datetime literals in time.UTC.
// To format times in a different location, route through a *Database
// (which carries Loc from the DSN) via db.InterpolateParams.
func InterpolateParams(query string, tmplFuncs template.FuncMap, valuerFuncs map[reflect.Type]reflect.Value, params ...any) (replacedQuery string, normalizedParams Params, err error) {
	return interpolateParams(query, tmplFuncs, valuerFuncs, time.UTC, params...)
}

func interpolateParams(query string, tmplFuncs template.FuncMap, valuerFuncs map[reflect.Type]reflect.Value, loc *time.Location, params ...any) (replacedQuery string, mergedParams Params, err error) {
	if strings.Contains(query, "{{") {
		convertedParams := make([]Params, 0, len(params))
		for _, p := range params {
			cp, _ := convertToParams("param", p)
			convertedParams = append(convertedParams, cp)
		}

		mp, _ := mergeParams(true, convertedParams, nil)
		cp, _ := convertToParams("params", mp)
		query, err = execTemplate(query, cp, tmplFuncs, valuerFuncs, loc)
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
	paramMetas := make([]map[string]paramMeta, 0, len(params))
	for _, p := range params {
		cp, pm := convertToParams(firstParamName, p)
		convertedParams = append(convertedParams, cp)
		paramMetas = append(paramMetas, pm)
	}

	allParams := append(make([]Params, 0, len(params)+2), Params{"now": time.Now()}, BuiltInParams)
	allParams = append(allParams, convertedParams...)

	var mergedParamMetas map[string]paramMeta
	mergedParams, mergedParamMetas = mergeParams(false, allParams, paramMetas)

	if len(mergedParams) == 0 {
		return query, nil, nil
	}

	usedParams := make(map[string]struct{})

	s := stringsBuilderPool.Get().(*strings.Builder)
	defer stringsBuilderPool.Put(s)
	s.Reset()

	for i, t := range queryTokens {
		switch t.kind {
		case queryTokenKindParam:
			name := t.string[2:]
			k := strings.ToLower(name)
			if v, ok := mergedParams[k]; ok {
				var (
					opts      marshalOpt
					fieldName = name
				)
				if mergedParamMetas != nil {
					if meta, ok := mergedParamMetas[k]; ok {
						if meta.defaultZero {
							opts |= marshalOptDefaultZero
						}
						if len(meta.columnName) != 0 {
							fieldName = meta.columnName
						}
					}
				}
				// Slices used as a column value JSON-encode so they round-trip
				// through a JSON column (SET col=@@list, VALUES(@@list)).
				// Slices wrapped by an `IN (...)` value list or a known
				// variadic function (json_array, concat, etc.) expand
				// comma-separated instead. See commaSeparatedArgFuncs.
				if !paramInCommaSeparatedList(queryTokens, i) {
					opts |= marshalOptJSONSlice
				}
				b, err := marshal(v, opts, fieldName, valuerFuncs, loc)
				if err != nil {
					return "", nil, err
				}

				s.Write(b)

				if next := i + 1; next < len(queryTokens) {
					nt := queryTokens[next]
					if nt.pos == t.end+1 {
						switch nt.kind {
						case queryTokenKindString, queryTokenKindWord, queryTokenKindVar, queryTokenKindParam:
							s.WriteByte(' ')
						}
					}
				}

				usedParams[k] = struct{}{}
				break
			}

			s.WriteString(t.string)
		default:
			s.WriteString(t.string)
		}
	}

	for k := range mergedParams {
		if _, ok := usedParams[k]; !ok {
			delete(mergedParams, k)
		}
	}

	return s.String(), mergedParams, nil
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
		switch b := query[i]; {
		case b == '\'', b == '"', b == '`':
			next()
			consumeUntilEsc(b)
			next()

			pushToken(queryTokenKindString)
		case b == '/' && i+1 < l && query[i+1] == '*':
			nextN(2)
			for i+1 < l && (query[i] != '*' || query[i+1] != '/') {
				next()
			}
			if i+1 < l {
				next()
			} else if i >= l {
				// Unterminated `/*` at EOF — clamp so the misc token's slice
				// stays in bounds. The trailing next() advances to l.
				i = l - 1
			}
			pushToken(queryTokenKindMisc)
		case b == '-' && i+2 < l && query[i+1] == '-' && (query[i+2] == ' ' || query[i+2] == '\t' || query[i+2] == '\n' || query[i+2] == '\r'):
			for i < l && query[i] != '\n' && query[i] != '\r' {
				next()
			}
			prev()
			pushToken(queryTokenKindMisc)
		case b == '#':
			for i < l && query[i] != '\n' && query[i] != '\r' {
				next()
			}
			prev()
			pushToken(queryTokenKindMisc)
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

// commaSeparatedArgFuncs lists the SQL keywords/functions whose argument list
// is a flat, comma-separated value list rather than a single column value.
// When a slice @@param is wrapped by one of these, the param expands as
// `1,2,3`; outside them it JSON-encodes as `[1,2,3]` so it round-trips
// through JSON columns (SET col=@@list, INSERT ... VALUES(@@list)).
//
// User-defined functions are conservatively excluded — without knowing the
// argument shape, JSON encoding matches the v0.0.26 Upsert UPDATE-path fix
// (#155) for any caller that wraps a slice in a custom function call.
var commaSeparatedArgFuncs = map[string]struct{}{
	"in":          {},
	"json_array":  {},
	"json_object": {},
	"concat":      {},
	"concat_ws":   {},
	"coalesce":    {},
	"greatest":    {},
	"least":       {},
	"field":       {},
	"elt":         {},
	"make_set":    {},
	"interval":    {},
}

// paramInCommaSeparatedList reports whether the param token at
// queryTokens[paramIdx] sits inside an `IN (...)` / `NOT IN (...)` value list
// or a known variadic SQL function. Used to decide whether a slice value
// expands comma-separated (correct for value lists and variadic functions) or
// JSON-encodes (correct for column assignments like SET col=@@list).
//
// The walk tracks paren depth so nested parens (`IN (a, (b,c), @@x)` or
// `json_overlaps(col, json_array(@@x))`) don't pick up an inner `(`. A
// `SELECT` keyword at our own depth (before the wrapping paren) means the
// param lives in a subquery expression — e.g. `IN (SELECT c FROM t WHERE
// c=@@x)` — not in the value list, so we return false.
//
// The identifier immediately before the wrapping `(` is looked up in
// commaSeparatedArgFuncs (case-insensitively). Anything not on the list
// (`VALUES`, user-defined functions, bare grouping parens) falls through
// to JSON encoding.
func paramInCommaSeparatedList(queryTokens []queryToken, paramIdx int) bool {
	depth := 0
	for j := paramIdx - 1; j >= 0; j-- {
		t := queryTokens[j]
		if depth == 0 && t.kind == queryTokenKindWord && strings.EqualFold(t.string, "select") {
			return false
		}
		if t.kind != queryTokenKindParen {
			continue
		}
		if t.string == ")" {
			depth++
			continue
		}
		if depth > 0 {
			depth--
			continue
		}
		for k := j - 1; k >= 0; k-- {
			tt := queryTokens[k]
			if tt.kind == queryTokenKindMisc {
				continue
			}
			if tt.kind != queryTokenKindWord {
				return false
			}
			_, ok := commaSeparatedArgFuncs[strings.ToLower(tt.string)]
			return ok
		}
		return false
	}
	return false
}

// Marshal formats x as a MySQL literal. time.Time values are formatted as
// naive datetime literals in time.UTC. To format times in a different
// location, route through a *Database (which carries Loc from the DSN).
func Marshal(x any, valuerFuncs map[reflect.Type]reflect.Value) ([]byte, error) {
	return marshal(x, 0, "", valuerFuncs, time.UTC)
}

type marshalOpt uint

const (
	marshalOptNone marshalOpt = 1 << iota
	marshalOptWrapSliceWithParens
	marshalOptJSONSlice
	marshalOptDefaultZero
)

// marshal keeps the original allocating API so callers outside the insert
// hot path (Marshal, interpolateParams) don't change. The actual work lives
// in marshalAppend.
//
// loc controls how time.Time values are formatted: the value is converted
// In(loc) and emitted as a naive datetime literal. Pass time.UTC if no
// specific location is associated with the caller.
func marshal(x any, opts marshalOpt, fieldName string, valuerFuncs map[reflect.Type]reflect.Value, loc *time.Location) ([]byte, error) {
	return marshalAppend(nil, x, opts, fieldName, valuerFuncs, loc)
}

const hexChars = "0123456789abcdef"

// appendHex appends src's lowercase hex representation to dst without any
// intermediate allocation. Replaces fmt.Appendf(nil, "%x", src) on the
// INSERT hot path, which was ~19% of row-build allocations.
func appendHex(dst, src []byte) []byte {
	// Grow once so the inner loop doesn't repeatedly trip append's resize.
	dst = slices.Grow(dst, len(src)*2)
	for _, b := range src {
		dst = append(dst, hexChars[b>>4], hexChars[b&0x0f])
	}
	return dst
}

// appendHexString is the string-input variant — skips the []byte(string) copy
// fmt.Appendf would do internally.
func appendHexString(dst []byte, src string) []byte {
	dst = slices.Grow(dst, len(src)*2)
	for i := 0; i < len(src); i++ {
		b := src[i]
		dst = append(dst, hexChars[b>>4], hexChars[b&0x0f])
	}
	return dst
}

// marshalAppend is the append-style variant of marshal. It appends the
// interpolated param into dst and returns the extended buffer. Passing
// dst=nil preserves the legacy "return a fresh slice" behavior.
//
// Strings and []byte are hex encoded to guarantee no escaping issues can
// sneak through. The encoding matches marshal byte-for-byte.
//
// loc controls how time.Time values are formatted: the value is converted
// In(loc) and emitted as a naive datetime literal so the round-trip
// through go-sql-driver's DST-aware Loc preserves the original instant.
// time.UTC is used when loc is nil.
func marshalAppend(dst []byte, x any, opts marshalOpt, fieldName string, valuerFuncs map[reflect.Type]reflect.Value, loc *time.Location) ([]byte, error) {
	if (opts&marshalOptDefaultZero) != 0 && isZero(x) {
		if len(fieldName) != 0 {
			dst = append(dst, "default(`"...)
			dst = append(dst, fieldName...)
			dst = append(dst, "`)"...)
			return dst, nil
		}
		return append(dst, "default"...), nil
	}

	// The decision whether to render a default value is scoped to the top-level
	// value associated with a struct field carrying the `defaultzero` option.
	// Once we have evaluated that value (and found that it should not be
	// rendered as DEFAULT) the flag must be cleared before we recurse further.
	//
	// Keeping the flag for nested marshal calls results in incorrect behavior
	// for user-defined types that implement driver.Valuer. For example, with a
	// bool-like Valuer that returns `false`, the recursive marshal call would
	// see the zero value of `false` and incorrectly output `DEFAULT`. Clearing
	// the flag here ensures the decision is made exactly once.
	opts &^= marshalOptDefaultZero

	v := reflect.ValueOf(x)
	if valuerFuncs != nil && v.IsValid() {
		pv := v
		if v.IsValid() && v.Kind() != reflect.Pointer {
			pv = reflect.New(v.Type())
			pv.Elem().Set(v)
		}

		fn, ok := valuerFuncs[pv.Type()]
		arg := pv
		if !ok {
			fn, ok = valuerFuncs[reflectUnwrapType(pv.Type())]
			arg = reflectUnwrap(pv)
			if arg.Kind() == reflect.Pointer && arg.IsNil() && fn.Type().In(0).Kind() != reflect.Pointer {
				return append(dst, "null"...), nil
			}
		}
		if ok {
			returns := fn.Call([]reflect.Value{arg})
			if err := returns[1].Interface(); err != nil {
				return nil, fmt.Errorf("cool-mysql: failed to call valuer func: %w", err.(error))
			}

			return marshalAppend(dst, returns[0].Interface(), opts, fieldName, valuerFuncs, loc)
		}
	}

	switch v := x.(type) {
	case bool:
		if !v {
			return append(dst, '0'), nil
		}
		return append(dst, '1'), nil
	case string:
		if len(v) == 0 {
			return append(dst, "''"...), nil
		}
		dst = append(dst, "_utf8mb4 0x"...)
		dst = appendHexString(dst, v)
		dst = append(dst, " collate utf8mb4_unicode_ci"...)
		return dst, nil
	case []byte:
		if v == nil {
			return append(dst, "null"...), nil
		}
		if len(v) == 0 {
			return append(dst, "''"...), nil
		}
		dst = append(dst, "0x"...)
		dst = appendHex(dst, v)
		return dst, nil
	case int:
		return strconv.AppendInt(dst, int64(v), 10), nil
	case int8:
		return strconv.AppendInt(dst, int64(v), 10), nil
	case int16:
		return strconv.AppendInt(dst, int64(v), 10), nil
	case int32:
		return strconv.AppendInt(dst, int64(v), 10), nil
	case int64:
		return strconv.AppendInt(dst, v, 10), nil
	case uint:
		return strconv.AppendUint(dst, uint64(v), 10), nil
	case uint8:
		return strconv.AppendUint(dst, uint64(v), 10), nil
	case uint16:
		return strconv.AppendUint(dst, uint64(v), 10), nil
	case uint32:
		return strconv.AppendUint(dst, uint64(v), 10), nil
	case uint64:
		return strconv.AppendUint(dst, uint64(v), 10), nil
	case complex64:
		return append(dst, strconv.FormatComplex(complex128(v), 'E', -1, 64)...), nil
	case complex128:
		return append(dst, strconv.FormatComplex(complex128(v), 'E', -1, 64)...), nil
	case float32:
		return strconv.AppendFloat(dst, float64(v), 'E', -1, 64), nil
	case float64:
		return strconv.AppendFloat(dst, v, 'E', -1, 64), nil
	case time.Time:
		if v.IsZero() {
			return append(dst, "null"...), nil
		}
		// Emit a naive datetime literal formatted in loc. The driver re-parses
		// naive DATETIME values through the same Loc on the read side, so the
		// instant round-trips exactly — including across DST boundaries that
		// a fixed @@session.time_zone offset would slip on. See #157.
		if loc == nil {
			loc = time.UTC
		}
		dst = append(dst, '\'')
		dst = v.In(loc).AppendFormat(dst, "2006-01-02 15:04:05.000000")
		dst = append(dst, '\'')
		return dst, nil
	case civil.Date:
		if v.IsZero() {
			return append(dst, "null"...), nil
		}
		dst = append(dst, '\'')
		dst = append(dst, v.String()...)
		dst = append(dst, '\'')
		return dst, nil
	case decimal.Decimal:
		return append(dst, v.String()...), nil
	case json.RawMessage:
		if v == nil {
			return append(dst, "null"...), nil
		}
		if len(v) == 0 {
			return append(dst, "''"...), nil
		}
		dst = append(dst, "_utf8mb4 0x"...)
		dst = appendHex(dst, v)
		dst = append(dst, " collate utf8mb4_unicode_ci"...)
		return dst, nil
	case Raw:
		return append(dst, v...), nil
	}

	v = reflect.ValueOf(x)
	if v.IsValid() && (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) {
		if v := v.Elem(); v.IsValid() {
			return marshalAppend(dst, v.Interface(), opts, fieldName, valuerFuncs, loc)
		}
	}

	// check the reflect kind, since we want to
	// deal with underlying value types if they didn't
	// explicitly set a way to be encoded
	v = reflectUnwrap(v)

	if !v.IsValid() {
		return append(dst, "null"...), nil
	}

	// pv needs to always be a pointer to a value
	// a pointer to something can call pointer methods or value methods,
	// but a value can only call value methods
	pv := v
	if v.Kind() != reflect.Pointer {
		pv = reflect.New(v.Type())
		pv.Elem().Set(v)
	}

	if valuerFuncs != nil {
		fn, ok := valuerFuncs[pv.Type()]
		pv := pv
		if !ok {
			fn, ok = valuerFuncs[reflectUnwrapType(pv.Type())]
			pv = reflectUnwrap(pv)
			if pv.Kind() == reflect.Pointer && pv.IsNil() && fn.Type().In(0).Kind() != reflect.Pointer {
				return append(dst, "null"...), nil
			}
		}
		if ok {
			returns := fn.Call([]reflect.Value{pv})
			if err := returns[1].Interface(); err != nil {
				return nil, fmt.Errorf("cool-mysql: failed to call valuer func: %w", err.(error))
			}
			return marshalAppend(dst, returns[0].Interface(), opts, fieldName, valuerFuncs, loc)
		}
	}

	if v, ok := pv.Interface().(driver.Valuer); ok {
		if pv.IsNil() {
			// but, if the pointer is nil and we try to call a value method, we get a dereference panic
			// so we need to check if the element type of the pointer has the method
			// if it does have the method, then we can't call it, because we're nil
			if _, ok := pv.Type().Elem().MethodByName("Value"); ok {
				return append(dst, "null"...), nil
			}
		}

		v, err := v.Value()
		if err != nil {
			return nil, fmt.Errorf("cool-mysql: failed to call Value on driver.Valuer: %w", err)
		}
		return marshalAppend(dst, v, opts, fieldName, valuerFuncs, loc)
	}

	if vs, ok := pv.Interface().(Valueser); ok {
		if pv.IsNil() {
			if _, ok := pv.Type().Elem().MethodByName("Value"); ok {
				return append(dst, "null"...), nil
			}
		}

		vs, err := vs.MySQLValues()
		if err != nil {
			return nil, fmt.Errorf("cool-mysql: failed to call MySQLValues on mysql.MySQLValues: %w", err)
		}
		// Valueser intentionally returns a []driver.Value to be expanded
		// comma-separated; suppress marshalOptJSONSlice so a 1-element return
		// like `[]driver.Value{int(s)}` renders as `s`, not `[s]`.
		return marshalAppend(dst, vs, opts&^marshalOptJSONSlice, fieldName, valuerFuncs, loc)
	}

	if isNil(x) {
		return append(dst, "null"...), nil
	}

	k := v.Kind()
	switch k {
	case reflect.Bool:
		return marshalAppend(dst, v.Bool(), opts, fieldName, valuerFuncs, loc)
	case reflect.String:
		return marshalAppend(dst, v.String(), opts, fieldName, valuerFuncs, loc)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return marshalAppend(dst, v.Int(), opts, fieldName, valuerFuncs, loc)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return marshalAppend(dst, v.Uint(), opts, fieldName, valuerFuncs, loc)
	case reflect.Complex64, reflect.Complex128:
		return marshalAppend(dst, v.Complex(), opts, fieldName, valuerFuncs, loc)
	case reflect.Float32, reflect.Float64:
		return marshalAppend(dst, v.Float(), opts, fieldName, valuerFuncs, loc)
	case reflect.Struct, reflect.Map:
		j, err := json.Marshal(x)
		if err != nil {
			return nil, fmt.Errorf("cool-mysql: failed to marshal struct to json: %w", err)
		}

		return marshalAppend(dst, json.RawMessage(j), opts, fieldName, valuerFuncs, loc)
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return marshalAppend(dst, v.Bytes(), opts, fieldName, valuerFuncs, loc)
		}

		if opts&marshalOptJSONSlice != 0 {
			j, err := json.Marshal(x)
			if err != nil {
				return nil, fmt.Errorf("cool-mysql: failed to marshal slice to json: %w", err)
			}

			return marshalAppend(dst, json.RawMessage(j), opts, fieldName, valuerFuncs, loc)
		}

		if opts&marshalOptWrapSliceWithParens != 0 {
			dst = append(dst, '(')
		}

		refLen := v.Len()
		if refLen == 0 {
			dst = append(dst, "null"...)
		}
		for i := range refLen {
			if i != 0 {
				dst = append(dst, ',')
			}

			var err error
			dst, err = marshalAppend(dst, v.Index(i).Interface(), opts|marshalOptWrapSliceWithParens, fieldName, valuerFuncs, loc)
			if err != nil {
				return nil, err
			}
		}

		if opts&marshalOptWrapSliceWithParens != 0 {
			dst = append(dst, ')')
		}

		return dst, nil
	}

	return nil, fmt.Errorf("cool-mysql: not sure how to interpret %q of type %T", x, x)
}

type paramMeta struct {
	defaultZero bool
	columnName  string
}

func convertToParams(firstParamName string, v any) (Params, map[string]paramMeta) {
	r := reflect.ValueOf(v)

	if !r.IsValid() {
		return Params{firstParamName: v}, nil
	}

	rv := reflect.Indirect(r)
	t := r.Type()
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	if isSingleParam(t) || isNil(v) {
		return Params{firstParamName: v}, nil
	}

	if t == paramsType {
		return rv.Interface().(Params), nil
	}

	switch k := t.Kind(); k {
	case reflect.Struct:
		structFieldIndexes := StructFieldIndexes(t)

		p := make(Params, len(structFieldIndexes))
		meta := make(map[string]paramMeta, len(structFieldIndexes))

		for _, i := range structFieldIndexes {
			f := t.FieldByIndex(i)

			if !f.IsExported() {
				continue
			}

			p[f.Name] = rv.FieldByIndex(i).Interface()

			t, _ := structtag.Parse(string(f.Tag))
			if t, _ := t.Get("mysql"); t != nil {
				columnName := f.Name
				if len(t.Name) != 0 {
					if decoded, err := decodeHex(t.Name); err == nil {
						columnName = decoded
					} else {
						columnName = t.Name
					}
				}

				meta[f.Name] = paramMeta{
					defaultZero: t.HasOption("defaultzero"),
					columnName:  strings.ReplaceAll(columnName, "`", "``"),
				}
			}
		}

		return p, meta
	case reflect.Map:
		p := make(Params)
		for _, k := range rv.MapKeys() {
			p[fmt.Sprint(k.Interface())] = rv.MapIndex(k).Interface()
		}

		return p, nil
	case reflect.Slice, reflect.Array:
		l := rv.Len()
		p := make(Params, l)
		for i := range l {
			p[strconv.Itoa(i)] = rv.Index(i).Interface()
		}

		return p, nil
	}

	return nil, nil
}

func isSingleParam(t reflect.Type) bool {
	if t.Implements(valuerType) || t == timeType || t == civilDateType {
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

func execTemplate(q string, params Params, additionalTmplFuncs template.FuncMap, valuerFuncs map[reflect.Type]reflect.Value, loc *time.Location) (string, error) {
	if !strings.Contains(q, "{{") {
		return q, nil
	}

	tmplFuncs := template.FuncMap{
		"marshal": func(x any) (string, error) {
			b, err := marshal(x, 0, "", valuerFuncs, loc)
			if err != nil {
				return "", err
			}

			return string(b), nil
		},
	}

	tmpl, err := template.New("query").Funcs(tmplFuncs).Funcs(additionalTmplFuncs).Option("missingkey=error").Parse(q)
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
