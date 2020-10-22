package mysql

// Literal is a literal MySQL string,
// not to be encoded or escaped in any way
type Literal string

// JSON gets treated as bytes in Go
// but as a string with character encoding
// in MySQL
type JSON []byte
