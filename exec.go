package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-sql-driver/mysql"
)

// exec executes a query and nothing more
func (db *Database) exec(conn commander, ctx context.Context, query string, opts marshalOpt, params ...any) (sql.Result, error) {
	replacedQuery, normalizedParams, err := db.interpolateParams(query, opts, params...)
	if err != nil {
		return nil, fmt.Errorf("failed to interpolate params: %w", err)
	}

	if db.die {
		fmt.Println(replacedQuery)
		j, _ := json.MarshalIndent(normalizedParams, "", "  ")
		fmt.Println(string(j))
		os.Exit(0)
	}

	start := time.Now()
	var res sql.Result

	var b = backoff.NewExponentialBackOff()
	b.MaxElapsedTime = MaxExecutionTime
	var attempt int
	var rowsAffected int64
	err = backoff.Retry(func() error {
		attempt++
		var err error
		res, err = conn.ExecContext(ctx, replacedQuery)
		if res != nil {
			rowsAffected, _ = res.RowsAffected()
		}
		tx, _ := conn.(*sql.Tx)
		db.callLog(LogDetail{
			Query:        replacedQuery,
			Params:       normalizedParams,
			Duration:     time.Since(start),
			RowsAffected: rowsAffected,
			Tx:           tx,
			Attempt:      attempt,
			Error:        err,
		})
		if err != nil {
			if checkRetryError(err) {
				return err
			} else if errors.Is(err, mysql.ErrInvalidConn) {
				return db.Test()
			} else {
				return backoff.Permanent(err)
			}
		}

		return nil
	}, backoff.WithContext(b, ctx))

	if err != nil {
		return nil, Error{
			Err:           err,
			OriginalQuery: query,
			ReplacedQuery: replacedQuery,
			Params:        normalizedParams,
		}
	}

	return res, nil
}
