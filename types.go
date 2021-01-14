package mysql

import "github.com/pkg/errors"

// Literal is a literal MySQL string,
// not to be encoded or escaped in any way
type Literal string

// CoolMySQLEncode writes the literal to the query writer
func (v Literal) CoolMySQLEncode(s Builder) {
	s.WriteString(string(v))
}

// JSON gets treated as bytes in Go
// but as a string with character encoding
// in MySQL
type JSON []byte

// CoolMySQLEncode writes the literal to the query writer
func (v JSON) CoolMySQLEncode(s Builder) {
	WriteEncoded(s, string(v), false)
}

// String is a string that's safe to scan null values into
type String string

// Scan implements sql.Scanner for String
func (s *String) Scan(src interface{}) error {
	if src == nil {
		return nil
	}

	switch v := src.(type) {
	case []byte:
		*s = String(v)
		return nil
	case string:
		*s = String(v)
		return nil
	default:
		return errors.New("incompatible type for string")
	}
}
