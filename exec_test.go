package mysql

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
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
	_, err := db.exec(h, context.Background(), nil, poolWriter, "SELECT 1")
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

var errTestLockWait = &stdMysql.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded; try restarting transaction"}

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
	_, err := db.exec(h, context.Background(), &Tx{}, poolWriter, "insert into `t` (`a`) values (2)")
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

// A lock-wait timeout (1205) inside a transaction is treated like a deadlock:
// under innodb_rollback_on_timeout it ends the whole transaction, so a
// statement-level retry would run in autocommit and strand phantom rows. It must
// surface unchanged for a whole-transaction restart (via RunInTx), not be
// retried in place.
func TestExecTxLockWaitTimeoutSurfacesWithoutRetry(t *testing.T) {
	originalMax := MaxAttempts
	MaxAttempts = 5 // budget to spare — it must still not be retried inside the tx
	t.Cleanup(func() { MaxAttempts = originalMax })

	h := &recordingExecHandler{errors: []error{errTestLockWait}}

	db := &Database{}
	_, err := db.exec(h, context.Background(), &Tx{}, poolWriter, "insert into `t` (`a`) values (2)")
	if err == nil {
		t.Fatal("expected the lock-wait timeout to surface")
	}

	var mysqlErr *stdMysql.MySQLError
	if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1205 {
		t.Fatalf("expected error 1205, got %v", err)
	}
	if len(h.queries) != 1 {
		t.Fatalf("expected the single failing statement with no in-tx retry, got %q", h.queries)
	}
}

// Outside a transaction a lock-wait timeout is safely retryable in place — the
// statement ran in autocommit. The tx-only guard must not suppress that retry.
func TestExecAutocommitLockWaitTimeoutStillRetries(t *testing.T) {
	originalMax := MaxAttempts
	MaxAttempts = 3
	t.Cleanup(func() { MaxAttempts = originalMax })

	h := &recordingExecHandler{errors: []error{errTestLockWait}}

	db := &Database{}
	_, err := db.exec(h, context.Background(), nil, poolWriter, "insert into `t` (`a`) values (2)")
	if err != nil {
		t.Fatalf("expected the autocommit retry to recover, got %v", err)
	}
	if len(h.queries) != 2 {
		t.Fatalf("expected the statement to run twice (timeout then retry), got %q", h.queries)
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
	_, err := db.exec(h, context.Background(), nil, poolWriter, "insert into `t` (`a`) values (2)")
	if err != nil {
		t.Fatalf("expected the autocommit retry to recover, got %v", err)
	}
	if len(h.queries) != 2 {
		t.Fatalf("expected the statement to run twice (deadlock then retry), got %q", h.queries)
	}
}

// Inside an explicit transaction a dead connection (mysql.ErrInvalidConn —
// which ReadTimeout now makes reachable on the write path, #172) cannot be
// resumed on a fresh conn, since the *sql.Tx is bound to it. exec must fail
// fast and NOT retry — the caller must restart the whole transaction. The
// backoff.Permanent wrapper used to stop the loop must not leak.
func TestExecTxInvalidConnFailsFastWithoutRetry(t *testing.T) {
	originalMax := MaxAttempts
	MaxAttempts = 5 // budget to spare — it must still not be retried inside the tx
	t.Cleanup(func() { MaxAttempts = originalMax })

	h := &recordingExecHandler{errors: []error{stdMysql.ErrInvalidConn}}

	db := &Database{}
	_, err := db.exec(h, context.Background(), &Tx{}, poolWriter, "insert into `t` (`a`) values (2)")
	if err == nil {
		t.Fatal("expected ErrInvalidConn to surface inside a tx")
	}
	if !errors.Is(err, stdMysql.ErrInvalidConn) {
		t.Fatalf("expected ErrInvalidConn, got %v", err)
	}
	var perm *backoff.PermanentError
	if errors.As(err, &perm) {
		t.Fatalf("returned error chain still contains *backoff.PermanentError: %#v", err)
	}
	if len(h.queries) != 1 {
		t.Fatalf("a tx-bound dead conn must not be retried, got %q", h.queries)
	}
}

// Outside a transaction, a dead connection (ErrInvalidConn) must reconnect
// and retry on a fresh pooled conn rather than report (nil, nil) as success
// (the bug #172 made reachable on the write pool). When the pool itself is
// down so db.Test()/Reconnect fails, that error surfaces and the statement
// is never falsely reported as having succeeded.
func TestExecInvalidConnPoolDownSurfacesError(t *testing.T) {
	originalMax := MaxAttempts
	MaxAttempts = 2
	t.Cleanup(func() { MaxAttempts = originalMax })

	// A closed pool whose Ping fails, so db.Test() attempts a Reconnect...
	closedPool, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	closedPool.Close()

	// ...and Reconnect fails because sqlOpenFunc can't build a new pool, so
	// db.Test() returns a non-nil error.
	origOpen := sqlOpenFunc
	t.Cleanup(func() { sqlOpenFunc = origOpen })
	sqlOpenFunc = func(*stdMysql.Config) (*sql.DB, error) {
		return nil, errors.New("dial refused")
	}

	h := &recordingExecHandler{errors: []error{stdMysql.ErrInvalidConn, stdMysql.ErrInvalidConn}}
	db := &Database{
		testMx:    new(sync.Mutex),
		Writes:    closedPool,
		WritesDSN: testDSN,
		ReadsDSN:  testDSN,
		Logger:    DefaultLogger(),
	}

	_, err = db.exec(h, context.Background(), nil, poolWriter, "insert into `t` (`a`) values (2)")
	if err == nil {
		t.Fatal("expected the failed Test()/Reconnect error to surface, not a phantom success")
	}
	if len(h.queries) != MaxAttempts {
		t.Fatalf("statement must re-run each attempt (never falsely succeed) and stay bounded by MaxAttempts; got %d calls", len(h.queries))
	}
}
