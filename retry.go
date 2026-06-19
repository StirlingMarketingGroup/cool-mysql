package mysql

import (
	"context"
	"time"
)

// retryElapsedBudget returns the WithMaxElapsedTime value for a query's retry
// loop. When ctx carries a deadline, that deadline governs the budget: a caller
// whose deadline is larger than the Lambda-oriented MaxExecutionTime default is
// no longer cut short at ~27s, and one whose deadline is smaller is bounded by
// it. backoff.Retry also stops at ctx cancellation, but a fixed
// WithMaxElapsedTime(MaxExecutionTime) would otherwise impose the global cap
// regardless of the caller's real budget. Without a deadline the fixed
// db.MaxExecutionTime applies, preserving the historical behavior of the
// non-context APIs (which pass context.Background()). A non-positive result
// means "no elapsed-time cap" — but only because callers pass it to
// backoff.WithMaxElapsedTime *unconditionally*: WithMaxElapsedTime(0) is
// uncapped, whereas omitting the option lets backoff apply its 15m default.
// See #174.
func (db *Database) retryElapsedBudget(ctx context.Context) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		return time.Until(deadline)
	}
	return db.MaxExecutionTime
}

// retryWithinBudget reports whether another attempt should be started after a
// connection-level failure (ErrInvalidConn / ErrBadConn). A swap-and-retry only
// helps if there is enough budget left for a fresh attempt to plausibly finish;
// estimating the next attempt's cost from the one that just failed (attemptStart
// → now), it returns false once the remaining budget — the ctx deadline if set,
// otherwise db.MaxExecutionTime measured from start — is smaller than that
// estimate. This stops a first attempt that already consumed most of the budget
// (e.g. a socket ReadTimeout trip at ~25s on a healthy-but-slow query) from
// being followed by a second full-length attempt that overshoots to ~2x the
// budget (#174). An unbounded budget (no deadline and MaxExecutionTime == 0)
// always allows the retry, preserving the connection-recovery behavior from
// #172/#163.
func (db *Database) retryWithinBudget(ctx context.Context, start, attemptStart time.Time) bool {
	lastAttempt := time.Since(attemptStart)
	if deadline, ok := ctx.Deadline(); ok {
		return time.Until(deadline) >= lastAttempt
	}
	if db.MaxExecutionTime > 0 {
		return db.MaxExecutionTime-time.Since(start) >= lastAttempt
	}
	return true
}
