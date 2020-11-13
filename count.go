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

	rows, err := db.Reads.Query(replacedQuery)
	if err != nil {
		return 0, Error{
			Err:           err,
			OriginalQuery: query,
			ReplacedQuery: replacedQuery,
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
