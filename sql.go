package mysql

import (
	"context"
	"database/sql"
	"time"
)

type handlerWithContext interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type Handler interface {
	Insert(insert string, source any) error
	InsertContext(ctx context.Context, insert string, source any) error

	ExecContextResult(ctx context.Context, query string, params ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, params ...any) error
	ExecResult(query string, params ...any) (sql.Result, error)
	Exec(query string, params ...any) error

	Select(dest any, q string, cache time.Duration, params ...any) error
	SelectRows(q string, cache time.Duration, params ...any) (Rows, error)
	SelectContext(ctx context.Context, dest any, q string, cache time.Duration, params ...any) error
	SelectJSON(dest any, query string, cache time.Duration, params ...any) error
	SelectJSONContext(ctx context.Context, dest any, query string, cache time.Duration, params ...any) error

	Exists(query string, cache time.Duration, params ...any) (bool, error)
	ExistsContext(ctx context.Context, query string, cache time.Duration, params ...any) (bool, error)
	Upsert(insert string, uniqueColumns, updateColumns []string, where string, source any) error
	UpsertContext(ctx context.Context, insert string, uniqueColumns, updateColumns []string, where string, source any) error
}

var _ Handler = &Database{}
var _ Handler = &Tx{}
