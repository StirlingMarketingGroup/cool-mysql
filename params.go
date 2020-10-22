package mysql

import (
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
)

// Params are a map of paramterer names to values
// use in the query like `select @@Name`
type Params map[string]interface{}

var replaceParamsPool = sync.Pool{
	New: func() interface{} {
		return new(strings.Builder)
	},
}

// ReplaceParams replaces the `@@` parameters in a query
// with their values from the map(s)
// Takes multiple "sets" of params for convienece, so we don't
// have to specify params if there aren't any, but each param will
// override the values of the previous. If there are 2 maps given,
// both with the key "ID", the last one will be used
func ReplaceParams(query string, params ...Params) string {
	if len(params) == 0 {
		return query
	}

	for i, p := range params {
		if i == 0 {
			continue
		}

		for k, v := range p {
			params[0][k] = v
		}
	}

	i := 0
	start := 0
	l := len(query)

	s := replaceParamsPool.Get().(*strings.Builder)
	defer replaceParamsPool.Put(s)
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
					WriteEncoded(s, v)
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

	return s.String()
}

// WriteEncoded takes a string builder and any value and writes it
// safely to the query, encoding values that could have escaping issues.
// Strings and []byte are hex encoded so as to make extra sure nothing
// bad is let through
func WriteEncoded(w *strings.Builder, x interface{}) {
	if isNil(x) {
		w.WriteString("null")
		return
	}

	h := hex.NewEncoder(w)

	// first handle literals, and changing types to their underlying types
	switch v := x.(type) {
	case Literal:
		w.WriteString(string(v))
		return
	case JSON:
		WriteEncoded(w, string(v))
		return
	case bool:
		if v {
			w.WriteByte('1')
		} else {
			w.WriteByte('0')
		}
		return
	case string:
		if len(v) != 0 {
			w.WriteString("_utf8mb4 0x")
			h.Write([]byte(v))
			w.WriteString(" collate utf8mb4_unicode_ci")
		} else {
			w.WriteString("''")
		}
		return
	case []byte:
		if len(v) != 0 {
			w.WriteString("0x")
			h.Write([]byte(v))
		} else {
			w.WriteString("''")
		}
		return
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		complex64, complex128:
		fmt.Fprintf(w, "%v", v)
		return
	}

	panic(fmt.Errorf("not sure how to interpret %q of type %T", x, x))
}
