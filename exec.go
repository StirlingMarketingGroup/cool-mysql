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
// newQuery is true if this is a new query, false if it's a replay of a query in a transaction
func (db *Database) exec(conn commander, ctx context.Context, tx *Tx, newQuery bool, query string, opts marshalOpt, params ...any) (sql.Result, error) {
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
			var handleDeadlock func(err error) error
			handleDeadlock = func(err error) error {
				if tx == nil || !checkDeadlockError(err) {
					return nil
				}

				// if this is a tx replay query already, we don't want it running all of the queries back
				// again, so we just return the error immediately to left the top level retry loop handle it
				if !newQuery {
					return backoff.Permanent(err)
				}

				// deadlock occurred, which means *every* query in this transaction
				// was rolled back, so we need to run them all again
				tx.updates.RLock()
				defer tx.updates.RUnlock()

				for _, q := range tx.updates.queries {
					_, err := db.exec(conn, ctx, nil, false, q, marshalOptNone)
					if err := handleDeadlock(err); err != nil {
						return err
					}
					if err != nil {
						return err
					}
				}

				// return the original deadlock error to resume regular functionality of the retry loop
				return err
			}
			if err := handleDeadlock(err); err != nil {
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

	var b = backoff.NewExponentialBackOff()
	b.MaxElapsedTime = MaxExecutionTime
	err = backoff.Retry(exec, backoff.WithContext(b, ctx))
	if err != nil {
		return nil, Error{
			Err:           err,
			OriginalQuery: query,
			ReplacedQuery: replacedQuery,
			Params:        normalizedParams,
		}
	}

	if tx != nil && newQuery {
		tx.updates.Lock()
		defer tx.updates.Unlock()
		tx.updates.queries = append(tx.updates.queries, replacedQuery)
	}

	return res, nil
}
