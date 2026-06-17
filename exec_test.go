package mysql

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"

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

func newDeadlockTestTx() *Tx {
	return &Tx{
		updates: &struct {
			sync.RWMutex
			queries []string
		}{queries: []string{"update `t` set `a` = 1"}},
	}
}

var errTestDeadlock = &stdMysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock; try restarting transaction"}

// A 1213 rolls back the whole transaction AND ends it on the session, so
// anything executed on the connection afterwards runs in autocommit mode.
// When no retry will follow, replaying the recorded tx queries would commit
// each of them individually outside any transaction — beyond the reach of
// the caller's eventual rollback. The replay must be skipped.
func TestExecDeadlockReplaySkippedWhenNoRetryBudget(t *testing.T) {
	originalMax := MaxAttempts
	MaxAttempts = 1
	t.Cleanup(func() { MaxAttempts = originalMax })

	h := &recordingExecHandler{errors: []error{errTestDeadlock}}

	db := &Database{}
	_, err := db.exec(h, context.Background(), newDeadlockTestTx(), true, "insert into `t` (`a`) values (2)")
	if err == nil {
		t.Fatal("expected the deadlock error to surface")
	}
	var mysqlErr *stdMysql.MySQLError
	if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1213 {
		t.Fatalf("expected error 1213, got %v", err)
	}
	if len(h.queries) != 1 {
		t.Fatalf("expected only the original statement (no replays) with no retry budget, got %q", h.queries)
	}
}

// With retry budget left the replay must still run, but only after a fresh
// `start transaction` re-opens the session the deadlock ended — otherwise the
// replay and the retried statement would commit piecewise in autocommit mode
// and the caller's eventual COMMIT/ROLLBACK would be meaningless. The retry of
// the original statement then follows so the transaction resumes where it left
// off.
func TestExecDeadlockReplayRunsWithRetryBudget(t *testing.T) {
	originalMax := MaxAttempts
	MaxAttempts = 3
	t.Cleanup(func() { MaxAttempts = originalMax })

	h := &recordingExecHandler{errors: []error{errTestDeadlock}}

	db := &Database{}
	_, err := db.exec(h, context.Background(), newDeadlockTestTx(), true, "insert into `t` (`a`) values (2)")
	if err != nil {
		t.Fatalf("expected the replay + retry to recover, got %v", err)
	}
	want := []string{
		"insert into `t` (`a`) values (2)",
		"start transaction",
		"update `t` set `a` = 1",
		"insert into `t` (`a`) values (2)",
	}
	if len(h.queries) != len(want) {
		t.Fatalf("expected queries %q, got %q", want, h.queries)
	}
	for i := range want {
		if h.queries[i] != want[i] {
			t.Fatalf("expected queries %q, got %q", want, h.queries)
		}
	}
}

// A deadlock raised by a replayed statement ends the re-opened transaction on
// the session again, so it must not be retried inline (that retry would run in
// autocommit). Instead the whole reconstruction restarts: a second `start
// transaction` re-opens the tx before the recorded queries are replayed again.
func TestExecDeadlockReplayReopensWhenReplayDeadlocks(t *testing.T) {
	originalMax := MaxAttempts
	MaxAttempts = 3
	t.Cleanup(func() { MaxAttempts = originalMax })

	// call 0: original insert deadlocks; call 1: first `start transaction`;
	// call 2: replayed update deadlocks; the reconstruction restarts.
	h := &recordingExecHandler{errors: []error{errTestDeadlock, nil, errTestDeadlock}}

	db := &Database{}
	_, err := db.exec(h, context.Background(), newDeadlockTestTx(), true, "insert into `t` (`a`) values (2)")
	if err != nil {
		t.Fatalf("expected the replay restart + retry to recover, got %v", err)
	}
	want := []string{
		"insert into `t` (`a`) values (2)",
		"start transaction",
		"update `t` set `a` = 1", // replay deadlocks
		"start transaction",      // reconstruction restarts before replaying again
		"update `t` set `a` = 1", // replay succeeds
		"insert into `t` (`a`) values (2)",
	}
	if len(h.queries) != len(want) {
		t.Fatalf("expected queries %q, got %q", want, h.queries)
	}
	for i := range want {
		if h.queries[i] != want[i] {
			t.Fatalf("expected queries %q, got %q", want, h.queries)
		}
	}
}

// If re-opening the transaction fails, the replay must not run — committing the
// recorded queries in autocommit mode is exactly what the re-open prevents — so
// the start-transaction error surfaces and no recorded query is replayed.
func TestExecDeadlockReplayAbortsWhenReopenFails(t *testing.T) {
	originalMax := MaxAttempts
	MaxAttempts = 3
	t.Cleanup(func() { MaxAttempts = originalMax })

	reopenErr := errors.New("connection is dead")
	// call 0 is the original statement (deadlock); call 1 is `start transaction`.
	h := &recordingExecHandler{errors: []error{errTestDeadlock, reopenErr}}

	db := &Database{}
	_, err := db.exec(h, context.Background(), newDeadlockTestTx(), true, "insert into `t` (`a`) values (2)")
	if err == nil {
		t.Fatal("expected the re-open failure to surface")
	}
	if !errors.Is(err, reopenErr) {
		t.Fatalf("expected the start-transaction error, got %v", err)
	}
	want := []string{
		"insert into `t` (`a`) values (2)",
		"start transaction",
	}
	if len(h.queries) != len(want) {
		t.Fatalf("expected no replay after a failed re-open, got %q", h.queries)
	}
	for i := range want {
		if h.queries[i] != want[i] {
			t.Fatalf("expected queries %q, got %q", want, h.queries)
		}
	}
}
