package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
)

// ExecContextResult executes a query and nothing more
func (db *Database) ExecContextResult(ctx context.Context, query string, params ...Params) (sql.Result, error) {
	replacedQuery, mergedParams := ReplaceParams(query, params...)
	if db.die {
		fmt.Println(replacedQuery)
		os.Exit(0)
	}

	start := time.Now()
	var res sql.Result

	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = BackoffDefaultMaxElapsedTime
	err := backoff.Retry(func() error {
		var err error
		res, err = db.Writes.ExecContext(ctx, replacedQuery)
		if err != nil {
			if checkRetryError(err) {
				return err
			} else {
				return backoff.Permanent(err)
			}
		}

		return nil
	}, backoff.WithContext(b, ctx))

	db.callLog(replacedQuery, mergedParams, time.Since(start))

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

// ExecContext executes a query and nothing more
func (db *Database) ExecContext(ctx context.Context, query string, params ...Params) error {
	_, err := db.ExecContextResult(ctx, query, params...)
	return err
}

// ExecResult executes a query and nothing more
func (db *Database) ExecResult(query string, params ...Params) (sql.Result, error) {
	return db.ExecContextResult(context.Background(), query, params...)
}

// Exec executes a query and nothing more
func (db *Database) Exec(query string, params ...Params) error {
	_, err := db.ExecContextResult(context.Background(), query, params...)
	return err
}

// ExecContextResult executes a query and nothing more
func (tx *Tx) ExecContextResult(ctx context.Context, query string, params ...Params) (sql.Result, error) {
	replacedQuery, mergedParams := ReplaceParams(query, params...)
	if tx.Database.die {
		fmt.Println(replacedQuery)
		os.Exit(0)
	}

	if tx.Database.DisableForeignKeyChecks {
		_, err := tx.Tx.ExecContext(ctx, "set`FOREIGN_KEY_CHECKS`=0")
		if err != nil {
			return nil, errors.Wrapf(err, "failed to disable foreign key checks")
		}
	}

	start := time.Now()
	var res sql.Result

	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = BackoffDefaultMaxElapsedTime
	err := backoff.Retry(func() error {
		var err error
		res, err = tx.Tx.ExecContext(ctx, replacedQuery)
		if err != nil {
			if checkRetryError(err) {
				return err
			} else {
				return backoff.Permanent(err)
			}
		}

		return nil
	}, backoff.WithContext(b, ctx))

	tx.Database.callLog(replacedQuery, mergedParams, time.Since(start))

	if tx.Database.DisableForeignKeyChecks {
		_, err := tx.Tx.ExecContext(ctx, "set`FOREIGN_KEY_CHECKS`=1")
		if err != nil {
			return nil, errors.Wrapf(err, "failed to re-enable foreign key checks")
		}
	}

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

// ExecContext executes a query and nothing more
func (tx *Tx) ExecContext(ctx context.Context, query string, params ...Params) error {
	_, err := tx.ExecContextResult(ctx, query, params...)
	return err
}

// ExecResult executes a query and nothing more
func (tx *Tx) ExecResult(query string, params ...Params) (sql.Result, error) {
	return tx.ExecContextResult(context.Background(), query, params...)
}

// Exec executes a query and nothing more
func (tx *Tx) Exec(query string, params ...Params) error {
	_, err := tx.ExecContextResult(context.Background(), query, params...)
	return err
}
