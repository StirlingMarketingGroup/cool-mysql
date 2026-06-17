package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/cenkalti/backoff/v5"
	stdMysql "github.com/go-sql-driver/mysql"
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
	_, err := db.exec(h, context.Background(), nil, "SELECT 1")
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

// recordingExecHandler records every query it receives and fails with the
// scripted error (by call index) when one is set.
type recordingExecHandler struct {
	queries []string
	errors  []error
}

func (h *recordingExecHandler) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	h.queries = append(h.queries, query)
	if i := len(h.queries) - 1; i < len(h.errors) && h.errors[i] != nil {
		return nil, h.errors[i]
	}
	return stubResult{}, nil
}

func (h *recordingExecHandler) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	panic("unexpected QueryContext call in recordingExecHandler")
}

var errTestDeadlock = &stdMysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock; try restarting transaction"}

// Within an explicit transaction a deadlock (1213) rolls back AND ends the
// whole transaction on the session, leaving the connection in autocommit mode.
// cool-mysql must not transparently retry it: re-running the failing statement
// — or replaying the transaction's earlier writes — would execute in autocommit
// and commit piecewise outside the caller's transaction, beyond the reach of
// its eventual COMMIT/ROLLBACK, leaving phantom rows. The deadlock must surface
// unchanged so the caller can restart the whole transaction from Begin (#167).
func TestExecTxDeadlockSurfacesWithoutRetry(t *testing.T) {
	originalMax := MaxAttempts
	MaxAttempts = 5 // budget to spare — the deadlock must still not be retried
	t.Cleanup(func() { MaxAttempts = originalMax })

	h := &recordingExecHandler{errors: []error{errTestDeadlock}}

	db := &Database{}
	_, err := db.exec(h, context.Background(), &Tx{}, "insert into `t` (`a`) values (2)")
	if err == nil {
		t.Fatal("expected the deadlock error to surface")
	}

	var mysqlErr *stdMysql.MySQLError
	if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1213 {
		t.Fatalf("expected error 1213, got %v", err)
	}
	// The backoff.Permanent wrapper used to stop the retry loop must never
	// leak — it would hijack a caller's own backoff.Retry around the tx.
	var perm *backoff.PermanentError
	if errors.As(err, &perm) {
		t.Fatalf("returned error chain still contains *backoff.PermanentError: %#v", err)
	}
	if len(h.queries) != 1 {
		t.Fatalf("expected the single failing statement with no retry or replay, got %q", h.queries)
	}
}

// Outside a transaction a deadlock is safely retryable: the statement ran in
// autocommit, so re-running just that statement is sound. The tx-only guard
// must not suppress that retry.
func TestExecAutocommitDeadlockStillRetries(t *testing.T) {
	originalMax := MaxAttempts
	MaxAttempts = 3
	t.Cleanup(func() { MaxAttempts = originalMax })

	// deadlock once, then succeed on retry.
	h := &recordingExecHandler{errors: []error{errTestDeadlock}}

	db := &Database{}
	_, err := db.exec(h, context.Background(), nil, "insert into `t` (`a`) values (2)")
	if err != nil {
		t.Fatalf("expected the autocommit retry to recover, got %v", err)
	}
	if len(h.queries) != 2 {
		t.Fatalf("expected the statement to run twice (deadlock then retry), got %q", h.queries)
	}
}
