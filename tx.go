package mysql

import (
	"context"
	"database/sql"
	"time"
)

// Tx is a cool MySQL transaction
type Tx struct {
	*sql.Tx

	Database *Database
}

// BeginWritesTx begins and returns a new transaction on the writes connection
func (db *Database) BeginWritesTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := db.Writes.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &Tx{
		Tx:       tx,
		Database: db,
	}, nil
}

// BeginReadsTx begins and returns a new transaction on the reads connection
func (db *Database) BeginReadsTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := db.Reads.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &Tx{
		Tx:       tx,
		Database: db,
	}, nil
}

func (tx *Tx) Select(dest interface{}, query string, cache time.Time, args ...Params) error {
	return _select(context.Background(), tx.Database, tx, dest, query, cache, args...)
}

func (tx *Tx) SelectContext(ctx context.Context, dest interface{}, query string, cache time.Time, args ...Params) error {
	return _select(ctx, tx.Database, tx, dest, query, cache, args...)
}

func (tx *Tx) Exec(query string, args ...Params) (sql.Result, error) {
	return exec(context.Background(), tx.Database, tx.Tx, query, args...)
}

func (tx *Tx) ExecContext(ctx context.Context, query string, args ...Params) (sql.Result, error) {
	return exec(ctx, tx.Database, tx.Tx, query, args...)
}
