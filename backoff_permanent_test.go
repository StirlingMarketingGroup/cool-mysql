package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/cenkalti/backoff/v5"
)

// failingQueryHandler returns the configured error on QueryContext and panics
// on ExecContext, mirroring the failingExecHandler pattern but for the
// read-path APIs (exists, select).
type failingQueryHandler struct {
	err error
}

func (h *failingQueryHandler) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	panic("unexpected ExecContext call in failingQueryHandler")
}

func (h *failingQueryHandler) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return nil, h.err
}

// errPermanentProbe is a sentinel distinct from any error cool-mysql treats as
// retryable or as an invalid-connection signal, so it's guaranteed to hit the
// backoff.Permanent branch in exec/exists/select.
var errPermanentProbe = errors.New("permanent probe error")

func assertNoPermanentWrapper(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var perm *backoff.PermanentError
	if errors.As(err, &perm) {
		t.Fatalf("returned error chain still contains *backoff.PermanentError: %#v", err)
	}
	if !errors.Is(err, errPermanentProbe) {
		t.Fatalf("returned error should unwrap to errPermanentProbe, got %v", err)
	}
}

func TestExecDoesNotLeakPermanentError(t *testing.T) {
	h := &failingExecHandler{errors: []error{errPermanentProbe}}

	db := &Database{}
	_, err := db.exec(h, context.Background(), nil, true, "SELECT 1")
	assertNoPermanentWrapper(t, err)
}

func TestExistsDoesNotLeakPermanentError(t *testing.T) {
	h := &failingQueryHandler{err: errPermanentProbe}

	db := &Database{Logger: DefaultLogger()}
	_, err := db.exists(h, context.Background(), "SELECT 1", 0)
	assertNoPermanentWrapper(t, err)
}

func TestSelectDoesNotLeakPermanentError(t *testing.T) {
	h := &failingQueryHandler{err: errPermanentProbe}

	db := &Database{Logger: DefaultLogger()}
	var out []int
	err := db.query(h, context.Background(), &out, "SELECT 1", 0)
	assertNoPermanentWrapper(t, err)
}
