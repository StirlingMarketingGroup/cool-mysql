package mysql

import (
	"fmt"
	"os"
	"time"
)

// Count efficiently checks the number of rows a query returns
func (db *Database) Count(query string, cache time.Duration, params ...any) (int, error) {
	replacedQuery, normalizedParams, err := db.InterpolateParams(query, params...)
	if err != nil {
		return 0, fmt.Errorf("failed to interpolate params: %w", err)
	}

	if db.die {
		fmt.Println(replacedQuery)
		os.Exit(0)
	}

	start := time.Now()
	rows, err := db.Reads.Query(replacedQuery)
	db.callLog(LogDetail{
		Query:    replacedQuery,
		Params:   normalizedParams,
		Duration: time.Since(start),
		Attempt:  1,
		Error:    err,
	})

	if err != nil {
		return 0, Error{
			Err:           err,
			OriginalQuery: query,
			ReplacedQuery: replacedQuery,
			Params:        normalizedParams,
		}
	}

	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	return count, nil
}
