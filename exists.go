package mysql

import (
	"fmt"
	"os"
	"time"
)

// exists efficiently checks if there are any rows in the given query
func exists(db *Database, conn commander, query string, cache time.Duration, params ...Params) (bool, error) {
	replacedQuery, mergedParams := ReplaceParams(query, params...)
	if db.die {
		fmt.Println(replacedQuery)
		os.Exit(0)
	}

	start := time.Now()
	rows, err := db.Reads.Query(replacedQuery)
	db.callLog(replacedQuery, mergedParams, time.Since(start), false)

	if err != nil {
		return false, Error{
			Err:           err,
			OriginalQuery: query,
			ReplacedQuery: replacedQuery,
			Params:        mergedParams,
		}
	}

	defer rows.Close()
	for rows.Next() {
		return true, nil
	}

	return false, nil
}
