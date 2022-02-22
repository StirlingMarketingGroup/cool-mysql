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

	"github.com/shopspring/decimal"
)

// Params are a map of paramterer names to values
// use in the query like `select @@Name`
type Params map[string]interface{}

var stringsBuilderPool = sync.Pool{
	New: func() interface{} {
		return new(strings.Builder)
	},
}

// ReplaceParams replaces the `@@` parameters in a query
// with their values from the map(s)
// Takes multiple "sets" of params for convenience, so we don't
// have to specify params if there aren't any, but each param will
// override the values of the previous. If there are 2 maps given,
// both with the key "ID", the last one will be used
func ReplaceParams(query string, params ...Params) (replacedQuery string, mergedParams Params) {
	if len(params) == 0 {
		return query, nil
	}

	for i, p := range params {
		if i == 0 {
			continue
		}

		for k, v := range p {
			params[0][k] = v
		}
	}

	if len(params[0]) == 0 {
		return query, nil
	}

	i := 0
	start := 0
	l := len(query)

	s := stringsBuilderPool.Get().(*strings.Builder)
	defer stringsBuilderPool.Put(s)
	s.Reset()

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

	for i < l {
		switch b := query[i]; b {
		case '\'', '"', '`':
			next()
			consumeUntilEsc(b)
			next()
		case '@':
			if i+2 < l && query[i+1] == '@' {
				s.WriteString(query[start:i])

				nextN(2)
				start = i
				consumeAllAlphaNum()
				k := query[start : i+1]
				v, ok := params[0][k]
				if ok {
					WriteEncoded(s, v, true)
				} else {
					s.WriteString("@@")
					s.WriteString(k)
				}
				next()
				start = i

			}
		}
		next()
	}
	if start < l {
		s.WriteString(query[start:])
	}

	return s.String(), params[0]
}

// Encodable is a type with it's own cool mysql
// encode method for safe replacing
type Encodable interface {
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
	x interface{}
}

// WriteEncoded takes a string builder and any value and writes it
// safely to the query, encoding values that could have escaping issues.
// Strings and []byte are hex encoded so as to make extra sure nothing
// bad is let through
func WriteEncoded(s Builder, x interface{}, possiblyNull bool) {
	nested, _ := x.(*nestedValue)
	if nested != nil {
		x = nested.x
	}

	if possiblyNull && isNil(x) {
		s.WriteString("null")
		return
	}

	switch v := x.(type) {
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
	case Encodable:
		v.CoolMySQLEncode(s)
		return
	case decimal.Decimal:
		s.WriteString(v.String())
		return
	case time.Time:
		s.WriteByte('\'')
		s.WriteString(v.Format("2006-01-02 15:04:05.000000"))
		s.WriteByte('\'')
		return
	case json.RawMessage:
		if len(v) != 0 {
			s.WriteString("_utf8mb4 0x")
			hex.NewEncoder(s).Write([]byte(v))
			s.WriteString(" collate utf8mb4_unicode_ci")
		} else {
			s.WriteString("''")
		}
		return
	}

	// check the reflect kind, since we want to
	// deal with underyling value types if they didn't
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
