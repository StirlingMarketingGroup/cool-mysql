package mysql

import (
	"context"
	"database/sql"
	"time"
)

// Tx is a cool MySQL transaction
type Tx struct {
	db *Database

	Tx *sql.Tx
}

type txCancelFunc func() error

func beginTx(db *Database, conn *sql.DB, ctx context.Context) (*Tx, txCancelFunc, error) {
	start := time.Now()
	t, err := conn.BeginTx(ctx, nil)
	db.callLog("start transaction", nil, time.Since(start), false)
	if err != nil {
		return nil, t.Rollback, err
	}

	return &Tx{
		db: db,
		Tx: t,
	}, t.Rollback, nil
}

// BeginTx begins and returns a new transaction on the writes connection
func (db *Database) BeginTx() (tx *Tx, cancel func() error, err error) {
	return beginTx(db, db.Writes, context.Background())
}

// BeginTxContext begins and returns a new transaction on the writes connection
func (db *Database) BeginTxContext(ctx context.Context) (tx *Tx, cancel func() error, err error) {
	return beginTx(db, db.Writes, ctx)
}

// BeginReadsTx begins and returns a new transaction on the writes connection
func (db *Database) BeginReadsTx() (tx *Tx, cancel func() error, err error) {
	return beginTx(db, db.Reads, context.Background())
}

// BeginReadsTxContext begins and returns a new transaction on the reads connection
func (db *Database) BeginReadsTxContext(ctx context.Context) (tx *Tx, cancel func() error, err error) {
	return beginTx(db, db.Reads, ctx)
}

// Commit commits the transaction
func (tx *Tx) Commit() error {
	start := time.Now()
	err := tx.Tx.Commit()
	tx.db.callLog("commit", nil, time.Since(start), false)

	return err
}

// Cancel the transaction
// this should be deferred after creating new tx every time
func (tx *Tx) Cancel() error {
	return tx.Tx.Rollback()
}

func (tx *Tx) DefaultInsertOptions() *Inserter {
	return &Inserter{
		db:   tx.db,
		conn: tx.Tx,
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

func (tx *Tx) InsertUniquely(insertQuery string, uniqueColumns []string, active string, args interface{}) error {
	return tx.I().InsertUniquely(insertQuery, uniqueColumns, active, args)
}

// ExecContextResult executes a query and nothing more
func (tx *Tx) ExecContextResult(ctx context.Context, query string, params ...Params) (sql.Result, error) {
	return tx.db.exec(tx.Tx, ctx, query, params...)
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

func (tx *Tx) Select(dest any, q string, cache time.Duration, params ...Params) error {
	return query(tx.db, tx.Tx, context.Background(), dest, q, cache, params...)
}

func (tx *Tx) SelectContext(ctx context.Context, dest any, q string, cache time.Duration, params ...Params) error {
	return query(tx.db, tx.Tx, ctx, dest, q, cache, params...)
}

// Exists efficiently checks if there are any rows in the given query using the `Reads` connection
func (tx *Tx) Exists(query string, cache time.Duration, params ...Params) (bool, error) {
	return exists(tx.db, tx.Tx, query, cache, params...)
}
