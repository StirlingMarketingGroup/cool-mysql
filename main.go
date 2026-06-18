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
// ReadTimeout is the load-bearing one. Without it, a pooled connection that
// has gone half-open — the peer vanished with no FIN/RST — blocks the next
// query's packet read until the *caller's* context deadline fires, and
// cool-mysql's ErrInvalidConn retry never triggers because a silent read
// never produces a connection error to retry on. go-sql-driver resets the
// read deadline before every socket read, so a healthy query that streams
// steadily is unaffected no matter how long it runs; only a connection that
// goes fully silent for the whole ReadTimeout trips it. When it does, the
// driver surfaces mysql.ErrInvalidConn, which select.go / exec.go /
// exists.go already catch and recover from by swapping to a fresh
// connection. In a deadline-bounded environment (Lambda/API Gateway), set
// ReadTimeout just under the request budget for a near-zero-risk win — the
// only queries it can cut are ones already doomed by the deadline, except
// now they retry on a fresh connection instead of hard-failing.
var ReadTimeout = time.Duration(getenvInt64("COOL_READ_TIMEOUT", 0)) * time.Second

// WriteTimeout bounds the socket write side symmetrically with ReadTimeout.
var WriteTimeout = time.Duration(getenvInt64("COOL_WRITE_TIMEOUT", 0)) * time.Second

// DialTimeout bounds new-connection establishment (go-sql-driver's dial
// Timeout). Zero relies on the OS / caller context deadline instead.
var DialTimeout = time.Duration(getenvInt64("COOL_DIAL_TIMEOUT", 0)) * time.Second
