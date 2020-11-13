package mysql

import (
	"github.com/davecgh/go-spew/spew"
	"github.com/pkg/errors"
)

// Error contains the error and query details
type Error struct {
	Err error

	OriginalQuery string
	ReplacedQuery string
	Params        Params
}

func (v Error) Error() string {
	return v.Err.Error()
}

// DebugError returns an error including the replaced query
// and the params given
func (v Error) DebugError() error {
	return errors.Errorf("%s\n\nquery:\n%s\n\nparams:\n%s", v.Err.Error(), v.ReplacedQuery, spew.Sdump(v.Params))
}
