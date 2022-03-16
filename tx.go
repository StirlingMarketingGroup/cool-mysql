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

// BeginWritesTx begins and returns a new transaction on the writes conneciton
func (db *Database) BeginWritesTx(ctx context.Context) (*Tx, error) {
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

func (tx *Tx) Insert(insert string, source any) error {
	return tx.db.I().insert(tx.Tx, context.Background(), insert, source)
}

func (tx *Tx) InsertContext(ctx context.Context, insert string, source any) error {
	return tx.db.I().insert(tx.Tx, ctx, insert, source)
}
