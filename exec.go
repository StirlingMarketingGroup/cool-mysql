package mysql

import (
	"fmt"
	"os"
	"time"
)

// Exec executes a query and nothing more
func (db *Database) Exec(query string, params ...Params) error {
	replacedQuery, mergedParams := ReplaceParams(query, params...)
	if db.die {
		fmt.Println(replacedQuery)
		os.Exit(0)
	}

	start := time.Now()
	_, err := db.Writes.Exec(replacedQuery)
	db.callLog(replacedQuery, mergedParams, time.Since(start))

	if err != nil {
		return Error{
			Err:           err,
			OriginalQuery: query,
			ReplacedQuery: replacedQuery,
			Params:        mergedParams,
		}
	}

	return nil
}
