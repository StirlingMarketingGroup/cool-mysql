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
func (db *Database) exec(conn commander, ctx context.Context, tx *Tx, retries bool, query string, opts marshalOpt, params ...any) (sql.Result, error) {
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
	exec := func() error {
		attempt++
		var err error
		res, err = conn.ExecContext(ctx, replacedQuery)
		if res != nil {
			rowsAffected, _ = res.RowsAffected()
		}
		realTx, _ := conn.(*sql.Tx)
		db.callLog(LogDetail{
			Query:        replacedQuery,
			Params:       normalizedParams,
			Duration:     time.Since(start),
			RowsAffected: rowsAffected,
			Tx:           realTx,
			Attempt:      attempt,
			Error:        err,
		})
		if err != nil {
			if tx != nil && checkDeadlockError(err) {
				// deadlock occurred, which means *every* query in this transaction
				// was rolled back, so we need to run them all again
				tx.updates.RLock()
				defer tx.updates.RUnlock()

				for _, q := range tx.updates.queries {
					_, err := db.exec(conn, ctx, nil, false, q, marshalOptNone)
					if err != nil {
						return err
					}
				}

				return err
			}

			if checkRetryError(err) {
				return err
			} else if errors.Is(err, mysql.ErrInvalidConn) {
				return db.Test()
			} else {
				return backoff.Permanent(err)
			}
		}

		return nil
	}

	if retries {
		err = backoff.Retry(exec, backoff.WithContext(b, ctx))
	} else {
		err = exec()
	}

	if err != nil {
		return nil, Error{
			Err:           err,
			OriginalQuery: query,
			ReplacedQuery: replacedQuery,
			Params:        normalizedParams,
		}
	}

	if tx != nil {
		tx.updates.Lock()
		defer tx.updates.Unlock()
		tx.updates.queries = append(tx.updates.queries, replacedQuery)
	}

	return res, nil
}
