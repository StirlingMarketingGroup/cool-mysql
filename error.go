package mysql

import (
	"fmt"

	"github.com/davecgh/go-spew/spew"
)

// Error contains the error and query details
type Error struct {
	Err error

	OriginalQuery string
	ReplacedQuery string
	Params        Params
}

// QueryErrorLoggingLength is the size of the query
// characters that are logged when an error occurs
var QueryErrorLoggingLength = 1 << 11 // 2kB

func (v Error) Error() string {
	if len(v.ReplacedQuery) > QueryErrorLoggingLength {
		half := QueryErrorLoggingLength >> 1
		v.ReplacedQuery = v.ReplacedQuery[:half] + fmt.Sprintf("\n/* %d characters hidden */\n", len(v.ReplacedQuery)-QueryErrorLoggingLength) + v.ReplacedQuery[len(v.ReplacedQuery)-half:]
	}
	return fmt.Sprintf("%s\n\nquery len:\n%d\n\nquery:\n%s\n\nparams:\n%s", v.Err.Error(), len(v.ReplacedQuery), v.ReplacedQuery, spew.Sdump(v.Params))
}
