package mysql

import (
	"context"
	"database/sql"
)

// Tx is a cool MySQL transaction
type Tx struct {
	Database *Database

	Tx *sql.Tx
}

// BeginWritesTx begins and returns a new transaction on the writes conneciton
func (db *Database) BeginWritesTx(ctx context.Context) (*Tx, error) {
	tx, err := db.Writes.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &Tx{
		Database: db,
		Tx:       tx,
	}, nil
}

// Commit commits the transaction
func (tx *Tx) Commit() error {
	return tx.Tx.Commit()
}
