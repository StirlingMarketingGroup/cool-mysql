package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"
)

type writer struct {
	io.Writer
}

var _ handlerWithContext = &writer{}

func (w *writer) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	_, err := fmt.Fprintf(w.Writer, "%s;\n\n", strings.TrimRight(strings.TrimSpace(query), ";"))
	if err != nil {
		return nil, fmt.Errorf("failed to write to writer: %w", err)
	}

	return nil, nil
}

func (w *writer) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	panic("not implemented")
}
