package mysql

import (
	"context"
	"database/sql"
	"time"
)

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

func _select(ctx context.Context, db *Database, src queryer, dest interface{}, query string, cache time.Time, args ...Params) error {
	return nil
}
