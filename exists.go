package mysql

import (
	"fmt"
	"os"
	"time"
)

// Exists efficiently checks if there are any rows in the given query
func (db *Database) Exists(query string, cache time.Duration, params ...Params) (bool, error) {
	replacedQuery, mergedParams := ReplaceParams(query, params...)
	if db.die {
		fmt.Println(replacedQuery)
		os.Exit(0)
	}

	rows, err := db.Reads.Query(replacedQuery)
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
