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

func (v Error) Error() string {
	return fmt.Sprintf("%s\n\nquery:\n%s\n\nparams:\n%s", v.Err.Error(), v.ReplacedQuery, spew.Sdump(v.Params))
}
