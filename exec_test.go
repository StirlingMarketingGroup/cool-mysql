package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

type stubResult struct{}

func (stubResult) LastInsertId() (int64, error) { return 0, nil }
func (stubResult) RowsAffected() (int64, error) { return 0, nil }

type failingExecHandler struct {
	errors []error
	calls  int
}

func (h *failingExecHandler) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	h.calls++
	if h.calls <= len(h.errors) {
		return nil, h.errors[h.calls-1]
	}
	return stubResult{}, nil
}

func (h *failingExecHandler) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	panic("unexpected QueryContext call in failingExecHandler")
}

func TestExecRespectsMaxAttempts(t *testing.T) {
	originalMax := MaxAttempts
	MaxAttempts = 3
	t.Cleanup(func() { MaxAttempts = originalMax })

	h := &failingExecHandler{
		errors: []error{errMockRetry, errMockRetry, errMockRetry, errMockRetry},
	}

	db := &Database{}
	_, err := db.exec(h, context.Background(), nil, true, "SELECT 1")
	if err == nil {
		t.Fatalf("expected error after retries exhausted")
	}
	if !errors.Is(err, errMockRetry) {
		t.Fatalf("expected errMockRetry, got %v", err)
	}
	if h.calls != MaxAttempts {
		t.Fatalf("expected %d attempts, got %d", MaxAttempts, h.calls)
	}
}
