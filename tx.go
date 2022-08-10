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
	db.callLog("start transaction", nil, time.Since(start), true)
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
	db.callLog("start transaction", nil, time.Since(start), true)
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
	tx.db.callLog("commit", nil, time.Since(start), true)

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
