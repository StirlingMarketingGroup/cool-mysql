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
// newQuery is true if this is a new query, false if it's a replay of a query in a transaction
func (db *Database) exec(conn handlerWithContext, ctx context.Context, tx *Tx, newQuery bool, query string, params ...any) (sql.Result, error) {
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
			// A deadlock (1213) rolls back the whole transaction AND ends it on
			// the session, so the connection is back in autocommit mode and
			// anything run on it from here commits piecewise.
			if tx != nil && checkDeadlockError(err) {
				// A replayed statement (newQuery=false) that deadlocks must not
				// be retried inline — that retry would run in autocommit. Surface
				// it so the top-level reconstruction below re-opens the tx and
				// replays from the top.
				if !newQuery {
					return nil, backoff.Permanent(err)
				}

				// Reconstruct the rolled-back transaction before the failing
				// statement is retried: re-open it and replay every recorded
				// query, so the replay, the retry, and the rest of the caller's
				// work ride a real transaction again — otherwise each commits
				// piecewise in autocommit and the caller's eventual
				// COMMIT/ROLLBACK is a no-op, stranding partial state beyond the
				// reach of a rollback.
				//
				// Replaying is only sound when a retry of the failing statement
				// follows; once the retry budget is spent the replay would commit
				// the recorded queries individually outside any transaction, so
				// surface the deadlock as-is instead (#165). A deadlock during the
				// replay ends the session's tx again, so re-open and restart;
				// each restart counts against that same budget, and
				// MaxExecutionTime bounds the loop when attempts are uncapped.
				for restart := 0; ; restart++ {
					if MaxAttempts > 0 && attempt+restart >= MaxAttempts {
						return nil, backoff.Permanent(err)
					}
					if db.MaxExecutionTime > 0 && time.Since(start) >= db.MaxExecutionTime {
						return nil, backoff.Permanent(err)
					}

					startTxStart := time.Now()
					_, startTxErr := conn.ExecContext(ctx, "start transaction")
					startTx, _ := conn.(*sql.Tx)
					db.callLog(LogDetail{
						Query:    "start transaction",
						Duration: time.Since(startTxStart),
						Tx:       startTx,
						Attempt:  1,
						Error:    startTxErr,
					})
					if startTxErr != nil {
						return nil, backoff.Permanent(startTxErr)
					}

					replayErr := db.replayTx(conn, ctx, tx)
					if replayErr == nil {
						break
					}
					// only a replay deadlock is recoverable by re-opening and
					// replaying again; any other failure is terminal.
					if !checkDeadlockError(replayErr) {
						return nil, backoff.Permanent(replayErr)
					}
				}

				// resume the outer retry loop so the failing statement runs
				// again inside the reconstructed transaction
				return nil, err
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

	if tx != nil && newQuery {
		tx.updates.Lock()
		defer tx.updates.Unlock()
		tx.updates.queries = append(tx.updates.queries, replacedQuery)
	}

	return res, nil
}

// replayTx replays every recorded statement of tx on conn, which must already
// have a fresh transaction open. Replays pass newQuery=false so they are not
// re-recorded and a deadlock on one surfaces (rather than being retried inline
// in autocommit) for the caller to recover by re-opening and replaying again.
func (db *Database) replayTx(conn handlerWithContext, ctx context.Context, tx *Tx) error {
	tx.updates.RLock()
	defer tx.updates.RUnlock()

	for _, q := range tx.updates.queries {
		if _, err := db.exec(conn, ctx, tx, false, q); err != nil {
			return err
		}
	}

	return nil
}
