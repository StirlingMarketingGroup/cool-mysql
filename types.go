package mysql

// RawMySQL is a literal MySQL string,
// not to be encoded or escaped in any way
type RawMySQL string

// CoolMySQLEncode writes the literal to the query writer
func (v RawMySQL) CoolMySQLEncode(s Builder) {
	s.WriteString(string(v))
}
