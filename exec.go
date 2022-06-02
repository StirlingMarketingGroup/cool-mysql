package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-sql-driver/mysql"
)

type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// exec executes a query and nothing more
func (db *Database) exec(ex Executor, ctx context.Context, query string, params ...Params) (sql.Result, error) {
	replacedQuery, mergedParams := ReplaceParams(query, params...)
	if db.die {
		fmt.Println(replacedQuery)
		os.Exit(0)
	}

	start := time.Now()
	var res sql.Result

	var b = backoff.NewExponentialBackOff()
	b.MaxElapsedTime = MaxExecutionTime
	err := backoff.Retry(func() error {
		var err error
		res, err = ex.ExecContext(ctx, replacedQuery)
		if err != nil {
			if checkRetryError(err) {
				return err
			} else if errors.Is(err, mysql.ErrInvalidConn) {
				if err := db.Test(); err != nil {
					return err
				}
				return err
			} else {
				return backoff.Permanent(err)
			}
		}

		return nil
	}, backoff.WithContext(b, ctx))

	db.callLog(replacedQuery, mergedParams, time.Since(start), false)

	if err != nil {
		return nil, Error{
			Err:           err,
			OriginalQuery: query,
			ReplacedQuery: replacedQuery,
			Params:        mergedParams,
		}
	}

	return res, nil
}
