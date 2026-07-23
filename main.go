package mysql

import (
	"time"
)

// MaxExecutionTime is the total time we would like our queries to be able to execute.
// Since we are using 30 second limited AWS Lambda functions, we'll default this time to
// 90% of 30 seconds (27 seconds), with the goal of letting our process clean up and correctly
// log any failed queries
var MaxExecutionTime = time.Duration(getenvInt64("COOL_MAX_EXECUTION_TIME_TIME", int64(float64(30)*.9))) * time.Second

var MaxConnectionTime = MaxExecutionTime

// MaxAttempts caps total attempts per query, including the first try (<=0 disables the cap).
var MaxAttempts = getenvInt("COOL_MAX_ATTEMPTS", 0)

// MaxExcusedLockWaits caps how many attempts within a single RunInTx call may
// have their innodb_lock_wait_timeout (1205) blocked wall-clock time excused
// from the elapsed-time retry budget (db.MaxExecutionTime / db.retryElapsedBudget).
// That blocked time is not retry effort — it's the cost of correctly waiting
// out someone else's transaction — so charging it against the same budget that
// bounds real retry attempts made a genuine 1205 whose lock-wait timeout
// exceeds MaxExecutionTime structurally un-retriable (#7829). Without a cap, a
// permanently stuck lock would let RunInTx retry forever when no ctx deadline
// and no MaxAttempts cap are in play; capping the number of EXCUSED attempts
// (not a separate elapsed-time ceiling) keeps the worst case bounded and
// simple: at most MaxExcusedLockWaits full lock-wait-timeout blocks before a
// 1205 is charged normally like any other error. <=0 disables excusing
// entirely (every 1205 is charged in full, matching pre-#7829 behavior).
//
// Only applies when RunInTx's ctx carries no deadline — a caller-supplied
// deadline is an absolute ceiling and is never excused (see RunInTx in tx.go).
var MaxExcusedLockWaits = getenvInt("COOL_MAX_EXCUSED_LOCK_WAITS", 3)

// MaxExcusedDeadlocks caps how many deadlock (1213 / SQLSTATE 40001, not 1205)
// replays a single RunInTx call may be granted AFTER its elapsed-time retry
// budget is exhausted. A deadlock's attempt time is genuine work and stays
// charged in full — but a closure slow enough that one attempt outruns the
// budget (heavy unit of work under load) would otherwise surface its first
// 1213 with zero replays, making the deadlock structurally un-retriable: the
// same failure shape MaxExcusedLockWaits fixed for 1205s (#7829), reached
// through work time instead of blocked time. Capping the excused replays (not
// adding a second elapsed-time ceiling) keeps a genuinely stuck deadlock loop
// bounded: at most MaxExcusedDeadlocks past-budget re-runs before the 1213
// surfaces. <=0 disables the grant (past-budget deadlocks are terminal
// immediately, matching prior behavior). Under-budget deadlocks are unaffected
// — they retry as always without consuming this allowance.
//
// Only applies when RunInTx's ctx carries no deadline — a caller-supplied
// deadline is an absolute ceiling and is never excused (see RunInTx in tx.go).
var MaxExcusedDeadlocks = getenvInt("COOL_MAX_EXCUSED_DEADLOCKS", 3)

var RedisLockRetryDelay = time.Duration(getenvFloat("COOL_REDIS_LOCK_RETRY_DELAY", .020)) * time.Second

// ReadTimeout, WriteTimeout, and DialTimeout are go-sql-driver socket
// timeouts (in whole seconds) applied to every pool opened by New /
// NewFromDSN / NewFromDSNDualPool and re-applied by Reconnect. They are set
// on the parsed DSN config in openPool only where the DSN itself left the
// field at zero, so an explicit *non-zero* readTimeout= / writeTimeout= /
// timeout= in the DSN always wins. (An explicit zero in the DSN is
// indistinguishable from an omitted value once parsed, so a non-zero
// package default still applies over it — set the field to your intended
// value rather than to zero to opt a pool out.) All default to 0 (off),
// preserving the historical behavior of no socket deadlines. See #172.
//
// ReadTimeout is the load-bearing one for half-open detection. Without it, a
// pooled connection that has gone half-open — the peer vanished with no
// FIN/RST — blocks the next query's packet read until the *caller's* context
// deadline fires, and cool-mysql's ErrInvalidConn retry never triggers because
// a silent read never produces a connection error to retry on. go-sql-driver
// resets the read deadline before every socket read, so a healthy query that
// *streams* steadily is unaffected no matter how long it runs; a connection
// that goes fully silent for the whole ReadTimeout trips it, surfacing
// mysql.ErrInvalidConn, which select.go / exec.go / exists.go catch and recover
// from by swapping to a fresh connection.
//
// The catch (#174): a healthy but *non-streaming* read (heavy
// json_arrayagg / GROUP BY / filesort — the server sends no packets until the
// whole result is computed) also looks fully silent, so a ReadTimeout set as a
// tight whole-query cap will cut it even though nothing is wrong. ReadTimeout is
// therefore no longer the recommended liveness mechanism. Prefer instead:
// TCPKeepAlive for half-open detection (the network-layer counterpart that does
// not cut healthy long reads), plus the caller's ctx deadline and the injected
// MAX_EXECUTION_TIME hint (a clean server-side ER_QUERY_TIMEOUT) to bound query
// duration. If you do set ReadTimeout tight anyway, the #174 replay guard
// prevents the doubling, but it can still cut a healthy long non-streaming read.
var ReadTimeout = time.Duration(getenvInt64("COOL_READ_TIMEOUT", 0)) * time.Second

// WriteTimeout bounds the socket write side symmetrically with ReadTimeout.
var WriteTimeout = time.Duration(getenvInt64("COOL_WRITE_TIMEOUT", 0)) * time.Second

// DialTimeout bounds new-connection establishment (go-sql-driver's dial
// Timeout). Zero relies on the OS / caller context deadline instead.
var DialTimeout = time.Duration(getenvInt64("COOL_DIAL_TIMEOUT", 0)) * time.Second

// TCPKeepAlive, when > 0, switches the TCP pools cool-mysql opens onto a
// keepalive-tuned dialer (see applyKeepAliveToConfig). It is the recommended
// half-open-detection mechanism — the network-layer counterpart to ReadTimeout
// that does NOT double as a whole-query read cap, so it catches a peer that
// vanished (no FIN/RST) without cutting a healthy long-running non-streaming
// read the way a tight ReadTimeout does (#172/#174).
//
// When set, the OS sends keepalive probes after the connection has been idle
// for TCPKeepAlive, repeats them every TCPKeepAlive, and tears the connection
// down after TCPKeepAliveCount unanswered probes — so a dead peer surfaces as a
// connection error (driver.ErrInvalidConn) for the existing swap-and-retry path
// in roughly TCPKeepAlive*(1+TCPKeepAliveCount). A healthy peer answers the
// probes at the OS layer even while a slow query computes, so a long query is
// unaffected. Off by default (0) preserves Go's ~15s keepalive default. TCP
// only — unix-socket pools are left untouched. Env COOL_TCP_KEEPALIVE (whole
// seconds). Like the socket timeouts, NewFromConn can't apply this (its pools
// are pre-built).
var TCPKeepAlive = time.Duration(getenvInt64("COOL_TCP_KEEPALIVE", 0)) * time.Second

// TCPKeepAliveCount is the number of unanswered keepalive probes the OS sends
// before tearing a connection down. Only used when TCPKeepAlive > 0. Some
// platforms ignore the count and apply their own; the idle/interval still tune
// detection there. Env COOL_TCP_KEEPALIVE_COUNT.
var TCPKeepAliveCount = getenvInt("COOL_TCP_KEEPALIVE_COUNT", 3)
