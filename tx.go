package mysql

import (
	"context"
	"database/sql"
)

// Tx is a cool MySQL transaction
type Tx struct {
	db *Database

	Tx *sql.Tx
}

// BeginTx begins and returns a new transaction on the writes connection
func (db *Database) BeginTx(ctx context.Context) (*Tx, error) {
	tx, err := db.Writes.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &Tx{
		db: db,
		Tx: tx,
	}, nil
}

// BeginReadsTx begins and returns a new transaction on the reads connection
func (db *Database) BeginReadsTx(ctx context.Context) (*Tx, error) {
	tx, err := db.Writes.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &Tx{
		db: db,
		Tx: tx,
	}, nil
}

// Commit commits the transaction
func (tx *Tx) Commit() error {
	return tx.Tx.Commit()
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
