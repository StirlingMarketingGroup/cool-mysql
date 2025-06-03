package mysql

import (
	"context"
	"time"
)

// SelectSliceContext executes a query and returns a typed slice using the Reads connection.
func SelectSliceContext[T any](db *Database, ctx context.Context, q string, cache time.Duration, params ...any) ([]T, error) {
	var dest []T
	err := db.query(db.Reads, ctx, &dest, q, cache, params...)
	if err != nil {
		return nil, err
	}
	return dest, nil
}

// SelectSlice executes a query and returns a typed slice using the Reads connection.
func SelectSlice[T any](db *Database, q string, cache time.Duration, params ...any) ([]T, error) {
	return SelectSliceContext[T](db, context.Background(), q, cache, params...)
}

// SelectOneContext executes a query and returns a single typed value using the Reads connection.
func SelectOneContext[T any](db *Database, ctx context.Context, q string, cache time.Duration, params ...any) (T, error) {
	var dest T
	err := db.query(db.Reads, ctx, &dest, q, cache, params...)
	if err != nil {
		var zero T
		return zero, err
	}
	return dest, nil
}

// SelectOne executes a query and returns a single typed value using the Reads connection.
func SelectOne[T any](db *Database, q string, cache time.Duration, params ...any) (T, error) {
	return SelectOneContext[T](db, context.Background(), q, cache, params...)
}

// Tx helpers

// TxSelectSliceContext executes a query on the transaction and returns a typed slice.
func TxSelectSliceContext[T any](tx *Tx, ctx context.Context, q string, cache time.Duration, params ...any) ([]T, error) {
	var dest []T
	err := tx.db.query(tx.Tx, ctx, &dest, q, cache, params...)
	if err != nil {
		return nil, err
	}
	return dest, nil
}

// TxSelectSlice executes a query on the transaction and returns a typed slice.
func TxSelectSlice[T any](tx *Tx, q string, cache time.Duration, params ...any) ([]T, error) {
	return TxSelectSliceContext[T](tx, context.Background(), q, cache, params...)
}

// TxSelectOneContext executes a query on the transaction and returns a single typed value.
func TxSelectOneContext[T any](tx *Tx, ctx context.Context, q string, cache time.Duration, params ...any) (T, error) {
	var dest T
	err := tx.db.query(tx.Tx, ctx, &dest, q, cache, params...)
	if err != nil {
		var zero T
		return zero, err
	}
	return dest, nil
}

// TxSelectOne executes a query on the transaction and returns a single typed value.
func TxSelectOne[T any](tx *Tx, q string, cache time.Duration, params ...any) (T, error) {
	return TxSelectOneContext[T](tx, context.Background(), q, cache, params...)
}
