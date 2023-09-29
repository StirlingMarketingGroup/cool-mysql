package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-sql-driver/mysql"
)

// Tx is a cool MySQL transaction
type Tx struct {
	db *Database

	Tx   *sql.Tx
	Time time.Time

	updates *struct {
		sync.RWMutex
		queries []string
	}
}

type txCancelFunc func() error

func (db *Database) beginTx(conn *sql.DB, ctx context.Context) (*Tx, txCancelFunc, error) {
	start := time.Now()

	t, err := conn.BeginTx(ctx, nil)
	tx := &Tx{
		db:   db,
		Tx:   t,
		Time: time.Now(),

		updates: &struct {
			sync.RWMutex
			queries []string
		}{queries: make([]string, 0)},
	}

	db.callLog(LogDetail{
		Query:    "start transaction",
		Duration: time.Since(start),
		Tx:       tx.Tx,
		Attempt:  1,
		Error:    err,
	})
	if err != nil {
		return nil, tx.Cancel, err
	}

	return tx, tx.Cancel, nil
}

// BeginTx begins and returns a new transaction on the writes connection
func (db *Database) BeginTx() (tx *Tx, cancel func() error, err error) {
	return db.beginTx(db.Writes, context.Background())
}

// BeginTxContext begins and returns a new transaction on the writes connection
func (db *Database) BeginTxContext(ctx context.Context) (tx *Tx, cancel func() error, err error) {
	return db.beginTx(db.Writes, ctx)
}

// BeginReadsTx begins and returns a new transaction on the writes connection
func (db *Database) BeginReadsTx() (tx *Tx, cancel func() error, err error) {
	return db.beginTx(db.Reads, context.Background())
}

// BeginReadsTxContext begins and returns a new transaction on the reads connection
func (db *Database) BeginReadsTxContext(ctx context.Context) (tx *Tx, cancel func() error, err error) {
	return db.beginTx(db.Reads, ctx)
}

// Commit commits the transaction
func (tx *Tx) Commit() error {
	if tx.db.die {
		fmt.Println("commit")
		os.Exit(0)
	}

	start := time.Now()

	var attempt int
	exec := func() error {
		attempt++
		err := tx.Tx.Commit()
		tx.db.callLog(LogDetail{
			Query:    "commit",
			Duration: time.Since(start),
			Tx:       tx.Tx,
			Attempt:  attempt,
			Error:    err,
		})
		if err != nil {
			var handleDeadlock func(err error) error
			handleDeadlock = func(err error) error {
				if !checkDeadlockError(err) {
					return nil
				}

				// deadlock occurred, which means *every* query in this transaction
				// was rolled back, so we need to run them all again
				tx.updates.RLock()
				defer tx.updates.RUnlock()

				for _, q := range tx.updates.queries {
					_, err := tx.db.exec(tx.Tx, context.Background(), nil, false, q, marshalOptNone)
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
				return tx.db.Test()
			} else {
				return backoff.Permanent(err)
			}
		}

		return nil
	}

	var b = backoff.NewExponentialBackOff()
	b.MaxElapsedTime = MaxExecutionTime
	err := backoff.Retry(exec, backoff.WithContext(b, context.Background()))
	if err != nil {
		return Error{
			Err:           err,
			OriginalQuery: "commit",
			ReplacedQuery: "commit",
		}
	}

	return nil
}

// Cancel the transaction
// this should be deferred after creating new tx every time
func (tx *Tx) Cancel() error {
	if tx.Tx == nil {
		return nil
	}

	start := time.Now()
	err := tx.Tx.Rollback()
	tx.db.callLog(LogDetail{
		Query:    "rollback",
		Duration: time.Since(start),
		Tx:       tx.Tx,
		Attempt:  1,
		Error:    err,
	})

	return err
}

func (tx *Tx) DefaultInsertOptions() *Inserter {
	return &Inserter{
		db:   tx.db,
		conn: tx.Tx,
		tx:   tx,
	}
}

func (tx *Tx) I() *Inserter {
	return tx.DefaultInsertOptions()
}

func (tx *Tx) Insert(insert string, source any) error {
	return tx.I().Insert(insert, source)
}

func (tx *Tx) InsertContext(ctx context.Context, insert string, source any) error {
	return tx.I().InsertContext(ctx, insert, source)
}

// ExecContextResult executes a query and nothing more
func (tx *Tx) ExecContextResult(ctx context.Context, query string, params ...any) (sql.Result, error) {
	return tx.db.exec(tx.Tx, ctx, tx, true, query, marshalOptNone, params...)
}

// ExecContext executes a query and nothing more
func (tx *Tx) ExecContext(ctx context.Context, query string, params ...any) error {
	_, err := tx.ExecContextResult(ctx, query, params...)
	return err
}

// ExecResult executes a query and nothing more
func (tx *Tx) ExecResult(query string, params ...any) (sql.Result, error) {
	return tx.ExecContextResult(context.Background(), query, params...)
}

// Exec executes a query and nothing more
func (tx *Tx) Exec(query string, params ...any) error {
	_, err := tx.ExecContextResult(context.Background(), query, params...)
	return err
}

func (tx *Tx) Select(dest any, q string, cache time.Duration, params ...any) error {
	return tx.db.query(tx.Tx, context.Background(), dest, q, cache, params...)
}

func (tx *Tx) SelectContext(ctx context.Context, dest any, q string, cache time.Duration, params ...any) error {
	return tx.db.query(tx.Tx, ctx, dest, q, cache, params...)
}

func (tx *Tx) SelectJSON(dest any, query string, cache time.Duration, params ...any) error {
	return tx.SelectJSONContext(context.Background(), dest, query, cache, params...)
}

func (tx *Tx) SelectJSONContext(ctx context.Context, dest any, query string, cache time.Duration, params ...any) error {
	var j []byte
	err := tx.SelectContext(ctx, &j, query, cache, params...)
	if err != nil {
		return err
	}

	err = json.Unmarshal(j, dest)
	if err != nil {
		return err
	}

	return nil
}

// Exists efficiently checks if there are any rows in the given query using the `Reads` connection
func (tx *Tx) Exists(query string, cache time.Duration, params ...any) (bool, error) {
	return tx.db.exists(tx.Tx, context.Background(), query, cache, marshalOptNone, params...)
}

// ExistsContext efficiently checks if there are any rows in the given query using the `Reads` connection
func (tx *Tx) ExistsContext(ctx context.Context, query string, cache time.Duration, params ...any) (bool, error) {
	return tx.db.exists(tx.Tx, ctx, query, cache, marshalOptNone, params...)
}

func (tx *Tx) Upsert(insert string, uniqueColumns, updateColumns []string, where string, source any) error {
	return tx.I().Upsert(insert, uniqueColumns, updateColumns, where, source)
}

func (tx *Tx) UpsertContext(ctx context.Context, insert string, uniqueColumns, updateColumns []string, where string, source any) error {
	return tx.I().UpsertContext(ctx, insert, uniqueColumns, updateColumns, where, source)
}
