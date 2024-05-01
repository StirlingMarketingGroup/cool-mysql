package mysql

import (
	"context"
	"fmt"
)

type key int

var dbKey = key(0)

// NewContext returns a new context.Context with the given *Database
func NewContext(ctx context.Context, db *Database) context.Context {
	return context.WithValue(ctx, dbKey, db)
}

// NewContextWithFunc returns a new context.Context with the given func () *Database
// This can be useful if you only want to initialize the database when it is actually needed.
// Combine with sync.OnceValue to ensure the database is only initialized once.
//
// Example:
//
//	ctx := NewContextWithFunc(context.Background(), sync.OnceValue(func() *Database {
//		db, err := NewDatabase()
//		if err != nil {
//			panic(err)
//		}
//		return db
//	}))
func NewContextWithFunc(ctx context.Context, f func() *Database) context.Context {
	return context.WithValue(ctx, dbKey, f)
}

// FromContext returns a *Database from a context.Context
// or nil if none is present.
func FromContext(ctx context.Context) *Database {
	if f, ok := ctx.Value(dbKey).(func() *Database); ok {
		return f()
	}

	db, _ := ctx.Value(dbKey).(*Database)
	return db
}

var txKey = key(1)

// NewContextWithTx returns a new context.Context with the given *Tx
func NewContextWithTx(ctx context.Context, tx *Tx) context.Context {
	return context.WithValue(ctx, txKey, tx)
}

// TxFromContext returns a *Tx from a context.Context
// or nil if none is present.
func TxFromContext(ctx context.Context) *Tx {
	tx, _ := ctx.Value(txKey).(*Tx)
	return tx
}

// GetOrCreateTxFromContext returns a *Tx from a context.Context
// or creates a new one if none is present.
// It also returns a `commit` func and `cancel` func.
// Both funcs will be noop if the tx is not created in this function.
// `cancel` should be deferred directly after calling this function to
// ensure the tx is rolled back if an error occurs.
//
// Example:
//
//	tx, commit, cancel, err := GetOrCreateTxFromContext(ctx)
//	defer cancel()
//	if err != nil {
//	    return fmt.Errorf("failed to get or create tx: %w", err)
//	}
//	ctx = NewContextWithTx(ctx, tx) // if you want to pass tx to other functions
//
//	// do something with tx
//
//	if err := commit(); err != nil {
//	    return fmt.Errorf("failed to commit tx: %w", err)
//	}
func GetOrCreateTxFromContext(ctx context.Context) (tx *Tx, commit, cancel func() error, err error) {
	tx = TxFromContext(ctx)
	if tx == nil {
		db := FromContext(ctx)

		tx, cancel, err = db.BeginTx()
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to begin tx: %w", err)
		}

		return tx, tx.Commit, cancel, nil
	}
	return tx, func() error { return nil }, func() error { return nil }, nil
}
