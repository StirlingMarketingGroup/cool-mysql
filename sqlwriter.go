package mysql

import (
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
)

type sqlWriter struct {
	path  string
	index *synct[int]
}

var _ handlerWithContext = &sqlWriter{}

func (w *sqlWriter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if err := os.MkdirAll(w.path, os.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to create directory %q: %w", w.path, err)
	}

	w.index.mx.Lock()
	w.index.v++
	w.index.mx.Unlock()

	name := fmt.Sprintf("%010d.sql.gz", w.index.Get())
	name = filepath.Join(w.path, name)
	f, err := os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if f != nil {
		defer f.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to open file %q: %w", w.path, err)
	}

	gz := gzip.NewWriter(f)
	defer gz.Close()

	if _, err := gz.Write([]byte(query)); err != nil {
		return nil, fmt.Errorf("failed to write to file %q: %w", w.path, err)
	}

	return nil, nil
}

func (w *sqlWriter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	panic("not implemented")
}
