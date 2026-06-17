package mysql

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cenkalti/backoff/v5"
	stdMysql "github.com/go-sql-driver/mysql"
)

// unwrapBackoffPermanent strips any *backoff.PermanentError wrapper from the
// error chain. backoff.Permanent is used internally to signal "don't retry" to
// backoff.Retry; it must never leak into the public error surface because it
// would hijack any outer backoff.Retry loop a caller builds around us.
func unwrapBackoffPermanent(err error) error {
	var p *backoff.PermanentError
	if errors.As(err, &p) {
		return p.Err
	}
	return err
}

// Error contains the error and query details
type Error struct {
	Err error

	OriginalQuery string
	ReplacedQuery string
	Params        any
}

// QueryErrorLoggingLength is the size of the query
// characters that are logged when an error occurs
var QueryErrorLoggingLength = getenvInt("COOL_MYSQL_MAX_QUERY_LOG_LENGTH", 1<<12) // 4kB

func (v Error) Error() string {
	if QueryErrorLoggingLength > 0 && len(v.ReplacedQuery) > QueryErrorLoggingLength {
		half := QueryErrorLoggingLength >> 1
		v.ReplacedQuery = v.ReplacedQuery[:half] + fmt.Sprintf("\n/* %d characters hidden */\n", len(v.ReplacedQuery)-QueryErrorLoggingLength) + v.ReplacedQuery[len(v.ReplacedQuery)-half:]
	}
	j, _ := json.MarshalIndent(v.Params, "", "  ")
	return fmt.Sprintf("%s\n\nquery len:\n%d\n\nquery:\n%s\n\nparams:\n%s", v.Err.Error(), len(v.ReplacedQuery), v.ReplacedQuery, j)
}

func (v Error) Unwrap() error {
	return v.Err
}

var errMockRetry = errors.New("mock retry error")

func checkRetryError(err error) (ok bool) {
	if errors.Is(err, errMockRetry) {
		return true
	}

	var mysqlErr *stdMysql.MySQLError
	if errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case 1213, 1205, 2006, 2003, 1047, 1452, 1317, 1146, 1305, 1105:
			return true
		default:
			return false
		}
	}
	return false
}

// checkTxRetryError reports whether err is a transaction-fatal MySQL error that
// can only be recovered by restarting the whole transaction from a fresh Begin:
// a deadlock (1213, SQLSTATE 40001) or a lock-wait timeout (1205). Inside an
// explicit transaction these can end the transaction on the session — a deadlock
// always, a lock-wait timeout when innodb_rollback_on_timeout is ON — leaving
// the connection in autocommit mode, so replaying individual statements would
// commit piecewise outside the caller's transaction and strand phantom rows (the
// unsound path removed in #167). exec/query/exists therefore surface these
// unchanged when in a tx instead of retrying them statement-by-statement, and
// db.RunInTx restarts the whole closure on them. It is intentionally narrower
// than checkRetryError, whose broader set is safe to re-run in place in
// autocommit.
func checkTxRetryError(err error) (ok bool) {
	var mysqlErr *stdMysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	switch mysqlErr.Number {
	case 1213, 1205:
		return true
	}
	// Galera/PXC and some proxies surface a deadlock under SQLSTATE 40001 with a
	// different vendor code; honor the standard serialization-failure state too.
	return mysqlErr.SQLState == [5]byte{'4', '0', '0', '0', '1'}
}

func Wrap(err error, originalQuery, replaceQuery string, params any) Error {
	return Error{
		Err:           err,
		OriginalQuery: originalQuery,
		ReplacedQuery: replaceQuery,
		Params:        params,
	}
}
