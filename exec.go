package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/go-sql-driver/mysql"
)

// exec executes a query and nothing more
func (db *Database) exec(conn handlerWithContext, ctx context.Context, tx *Tx, query string, params ...any) (sql.Result, error) {
	replacedQuery, normalizedParams, err := db.interpolateParams(query, params...)
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

	b := backoff.NewExponentialBackOff()
	var attempt int
	var rowsAffected int64
	operation := func() (sql.Result, error) {
		attempt++
		res, err := conn.ExecContext(ctx, replacedQuery)
		if res != nil {
			rowsAffected, _ = res.RowsAffected()
		} else {
			rowsAffected = 0
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
			// Within an explicit transaction a deadlock (1213) — and a lock-wait
			// timeout (1205) when innodb_rollback_on_timeout is ON — rolls back
			// AND ends the whole transaction on the session, leaving the
			// connection in autocommit mode. We cannot transparently retry:
			// re-running the failing statement — or replaying the transaction's
			// earlier writes — would execute in autocommit and commit piecewise,
			// outside the caller's transaction and beyond the reach of its
			// eventual COMMIT/ROLLBACK, stranding phantom rows. The only way to
			// preserve atomicity is to restart the whole transaction from Begin,
			// so surface these tx-fatal errors instead of retrying them (#167);
			// db.RunInTx automates that restart.
			if tx != nil && checkTxRetryError(err) {
				return nil, backoff.Permanent(err)
			}

			if checkRetryError(err) {
				return nil, err
			}
			if errors.Is(err, mysql.ErrInvalidConn) {
				return nil, db.Test()
			}
			return nil, backoff.Permanent(err)
		}

		return res, nil
	}

	options := []backoff.RetryOption{
		backoff.WithBackOff(b),
		backoff.WithMaxElapsedTime(db.MaxExecutionTime),
	}
	if MaxAttempts > 0 {
		options = append(options, backoff.WithMaxTries(uint(MaxAttempts)))
	}

	res, err = backoff.Retry(ctx, operation, options...)
	if err != nil {
		return nil, Error{
			Err:           unwrapBackoffPermanent(err),
			OriginalQuery: query,
			ReplacedQuery: replacedQuery,
			Params:        normalizedParams,
		}
	}

	return res, nil
}
