package mysql

// RawMySQL is a literal MySQL string,
// not to be encoded or escaped in any way
type RawMySQL string

// CoolMySQLEncode writes the literal to the query writer
func (v RawMySQL) CoolMySQLEncode(s Builder) {
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
