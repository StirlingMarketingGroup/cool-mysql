package mysql

import (
	"context"
	"database/sql"
	"time"
)

// Tx is a cool MySQL transaction
type Tx struct {
	db *Database

	Tx   *sql.Tx
	Time time.Time
}

type txCancelFunc func() error

func (db *Database) beginTx(conn *sql.DB, ctx context.Context) (*Tx, txCancelFunc, error) {
	start := time.Now()

	t, err := conn.BeginTx(ctx, nil)
	tx := &Tx{
		db:   db,
		Tx:   t,
		Time: time.Now(),
	}

	db.callLog("start transaction", nil, time.Since(start), false)
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
	start := time.Now()
	err := tx.Tx.Commit()
	tx.db.callLog("commit", nil, time.Since(start), false)

	return err
}

// Cancel the transaction
// this should be deferred after creating new tx every time
func (tx *Tx) Cancel() error {
	if tx.Tx == nil {
		return nil
	}

	start := time.Now()
	err := tx.Tx.Rollback()
	tx.db.callLog("rollback", nil, time.Since(start), false)

	return err
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

// ExecContextResult executes a query and nothing more
func (tx *Tx) ExecContextResult(ctx context.Context, query string, params ...any) (sql.Result, error) {
	return tx.db.exec(tx.Tx, ctx, query, marshalOptNone, params...)
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
