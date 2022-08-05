package mysql

import (
	"fmt"
	"os"
	"time"
)

// Count efficiently checks the number of rows a query returns
func (db *Database) Count(query string, cache time.Duration, params ...Params) (int, error) {
	replacedQuery, mergedParams := ReplaceParams(query, params...)
	if db.die {
		fmt.Println(replacedQuery)
		os.Exit(0)
	}

	start := time.Now()
	rows, err := db.Reads.Query(string(replacedQuery))
	db.callLog(string(replacedQuery), mergedParams, time.Since(start), false)

	if err != nil {
		return 0, Error{
			Err:           err,
			OriginalQuery: query,
			ReplacedQuery: string(replacedQuery),
			Params:        mergedParams,
		}
	}

	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	return count, nil
}
