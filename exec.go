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
func (db *Database) exec(conn handlerWithContext, ctx context.Context, tx *Tx, role poolRole, query string, params ...any) (sql.Result, error) {
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
		attemptStart := time.Now()
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
			// An error checkTxRetryError classifies needs a fresh transaction:
			// re-running the failing statement, or the transaction's earlier
			// writes, on this session would fail again or commit piecewise in
			// autocommit, stranding phantom rows. Surface it unchanged so the
			// caller or db.RunInTx restarts the whole unit of work from Begin.
			if tx != nil && checkTxRetryError(err) {
				return nil, backoff.Permanent(err)
			}

			if checkRetryError(err) {
				return nil, err
			}
			if errors.Is(err, mysql.ErrInvalidConn) {
				// A *sql.Tx is bound to its (now dead) conn and can't be
				// resumed on a fresh one, so fail fast. Otherwise reconnect
				// if the pool is down and return the error so backoff
				// re-runs — the next ExecContext draws a healthy pooled
				// conn. Returning db.Test() directly would report (nil, nil)
				// as success when the pool is healthy, silently turning a
				// failed write into a no-op "success" (mirrors exists.go).
				if tx != nil {
					return nil, backoff.Permanent(err)
				}
				// Don't follow a near-budget attempt with another full-length
				// one (the ReadTimeout doubling in #174).
				if !db.retryWithinBudget(ctx, start, attemptStart) {
					return nil, backoff.Permanent(err)
				}
				if testErr := db.Test(); testErr != nil {
					return nil, testErr
				}
				return nil, err
			}
			return nil, backoff.Permanent(err)
		}

		return res, nil
	}

	options := []backoff.RetryOption{
		backoff.WithBackOff(b),
		// Pass the budget unconditionally: backoff defaults an omitted
		// MaxElapsedTime to 15m, whereas WithMaxElapsedTime(0) means uncapped (#174).
		backoff.WithMaxElapsedTime(db.retryElapsedBudget(ctx)),
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

	// poolUnknown (SetExecutor) never marks: the statement's effects may not
	// have landed where tx/db think their writes go.
	switch {
	case role == poolUnknown:
	case tx != nil:
		tx.wrote.Store(true)
	case role == poolWriter:
		db.markWrite()
	}

	return res, nil
}
