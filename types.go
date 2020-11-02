package mysql

import (
	"strings"
)

// Literal is a literal MySQL string,
// not to be encoded or escaped in any way
type Literal string

// CoolMySQLEncode writes the literal to the query writer
func (v Literal) CoolMySQLEncode(s *strings.Builder) {
	s.WriteString(string(v))
}

// JSON gets treated as bytes in Go
// but as a string with character encoding
// in MySQL
type JSON []byte

// CoolMySQLEncode writes the literal to the query writer
func (v JSON) CoolMySQLEncode(s *strings.Builder) {
	WriteEncoded(s, string(v), false)
}
