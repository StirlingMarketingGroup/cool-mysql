package mysql

// Raw is a literal MySQL string,
// not to be encoded or escaped in any way
type Raw string

// MarshalMySQL writes the literal to the query writer
func (v Raw) MarshalMySQL() ([]byte, error) {
	return []byte(v), nil
}
