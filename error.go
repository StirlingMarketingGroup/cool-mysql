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
	return fmt.Sprintf("%s\n\nquery len:\n%d\n\nquery:\n%s\n\nparams:\n%s", v.Err.Error(), len(v.ReplacedQuery), v.ReplacedQuery, spew.Sdump(v.Params))
}
