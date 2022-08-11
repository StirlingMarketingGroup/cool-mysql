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

// BeginTx begins and returns a new transaction on the writes connection
func (db *Database) BeginTx(ctx context.Context) (tx *Tx, cancel func() error, err error) {
	start := time.Now()
	t, err := db.Writes.BeginTx(ctx, nil)
	db.callLog("start transaction", nil, time.Since(start), false)
	if err != nil {
		return nil, t.Rollback, err
	}

	return &Tx{
		db: db,
		Tx: t,
	}, t.Rollback, nil
}

// BeginReadsTx begins and returns a new transaction on the reads connection
func (db *Database) BeginReadsTx(ctx context.Context) (tx *Tx, cancel func() error, err error) {
	start := time.Now()
	t, err := db.Reads.BeginTx(ctx, nil)
	db.callLog("start transaction", nil, time.Since(start), false)
	if err != nil {
		return nil, t.Rollback, err
	}

	return &Tx{
		db: db,
		Tx: t,
	}, t.Rollback, nil
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

func (tx *Tx) DefaultSelectOptions() *Selector {
	return &Selector{
		db:   tx.db,
		tx:   tx,
		conn: tx.Tx,
	}
}

func (tx *Tx) S() *Selector {
	return tx.DefaultSelectOptions()
}

func (tx *Tx) Select(dest any, q string, cache time.Duration, params ...Params) error {
	return tx.S().Select(dest, q, cache, params...)
}

func (tx *Tx) SelectContext(ctx context.Context, dest any, q string, cache time.Duration, params ...Params) error {
	return tx.S().SelectContext(ctx, dest, q, cache, params...)
}

func (tx *Tx) SelectWrites(dest any, q string, cache time.Duration, params ...Params) error {
	return tx.S().SelectWrites(dest, q, cache, params...)
}

func (tx *Tx) SelectWritesContext(ctx context.Context, dest any, q string, cache time.Duration, params ...Params) error {
	return tx.S().SelectWritesContext(ctx, dest, q, cache, params...)
}

func (tx *Tx) SelectJSON(dest interface{}, query string, cache time.Duration, params ...Params) error {
	return tx.S().SelectJSONContext(context.Background(), dest, query, cache, params...)
}

func (tx *Tx) DefaultInsertOptions() *Inserter {
	return &Inserter{
		db:   tx.db,
		tx:   tx,
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

func (tx *Tx) InsertUniquely(insert string, uniqueColumns []string, active string, source any) error {
	return tx.I().InsertUniquely(insert, uniqueColumns, active, source)
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
