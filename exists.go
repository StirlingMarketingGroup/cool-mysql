package mysql

import (
	"context"
	"crypto/sha3"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/go-sql-driver/mysql"
)

// exists efficiently checks if there are any rows in the given query
func (db *Database) exists(conn handlerWithContext, ctx context.Context, query string, cacheDuration time.Duration, params ...any) (exists bool, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	replacedQuery, normalizedParams, err := db.interpolateParams(query, params...)
	if err != nil {
		return false, fmt.Errorf("failed to interpolate params: %w", err)
	}

	if db.die {
		fmt.Println(replacedQuery)
		cancel()
		os.Exit(0)
	}

	defer func() {
		if err != nil {
			err = Error{
				Err:           err,
				OriginalQuery: query,
				ReplacedQuery: replacedQuery,
				Params:        normalizedParams,
			}
		}
	}()

	var cacheKey string

	if cacheDuration > 0 {
		key := new(strings.Builder)
		key.WriteString("cool-mysql:exists:")
		key.WriteString(replacedQuery)
		key.WriteByte(':')
		key.WriteString(strconv.FormatInt(int64(cacheDuration), 10))

		h := sha3.Sum224([]byte(key.String()))
		cacheKey = hex.EncodeToString(h[:])

		start := time.Now()

	CHECK_CACHE:
		var buf []byte
		buf, err = db.cache.Get(ctx, cacheKey)
		if errors.Is(err, ErrCacheMiss) {
			// cache miss!

			var unlockFn func() error
			if db.locker != nil {
				unlockFn, err = db.locker.Lock(ctx, cacheKey+":mutex")
				if err != nil {
					time.Sleep(RedisLockRetryDelay)
					goto CHECK_CACHE
				}
			}

			defer func() {
				if unlockFn != nil {
					if err := unlockFn(); err != nil {
						db.Logger.Warn("failed to unlock cache mutex", "err", err)
					}
				}
			}()
		} else if err != nil {
			err = fmt.Errorf("failed to get data from cache: %w", err)
			if db.HandleCacheError != nil {
				err = db.HandleCacheError(err)
			}
			if err != nil {
				return exists, err
			}
		} else {
			exists = len(buf) > 0 && buf[0] == 1
			tx, _ := conn.(*sql.Tx)
			db.callLog(LogDetail{
				Query:    replacedQuery,
				Params:   normalizedParams,
				Duration: time.Since(start),
				CacheHit: true,
				Tx:       tx,
				Attempt:  1,
			})
			return exists, err
		}
	}

	start := time.Now()

	b := backoff.NewExponentialBackOff()
	var attempt int
	operation := func() (bool, error) {
		attempt++
		rows, err := conn.QueryContext(ctx, replacedQuery)
		tx, _ := conn.(*sql.Tx)
		db.callLog(LogDetail{
			Query:    replacedQuery,
			Params:   normalizedParams,
			Duration: time.Since(start),
			Tx:       tx,
			Attempt:  attempt,
			Error:    err,
		})
		if err != nil {
			if checkRetryError(err) {
				return false, err
			}
			if errors.Is(err, mysql.ErrInvalidConn) {
				// A *sql.Tx is bound to its (now dead) conn and can't be
				// resumed on a fresh one, so fail fast. Otherwise reconnect
				// if the pool is down and return the error so backoff
				// re-runs — the next QueryContext draws a healthy pooled
				// conn. Returning db.Test() directly would report (false,
				// nil) as success when the pool is healthy, silently turning
				// a dead conn into a "no rows" answer.
				if tx != nil {
					return false, backoff.Permanent(err)
				}
				if testErr := db.Test(); testErr != nil {
					return false, testErr
				}
				return false, err
			}
			return false, backoff.Permanent(err)
		}

		// The row-streaming phase (rows.Next / rows.Err) runs inside the
		// retry so a connection that drops mid-stream is recoverable. exists
		// reads a single boolean that the caller can't observe until we
		// return, so re-running the whole query is always safe here.
		got := rows.Next()
		scanErr := rows.Err()
		if closeErr := rows.Close(); closeErr != nil {
			db.Logger.Warn("failed to close rows", "err", closeErr)
		}
		if scanErr != nil {
			if errors.Is(scanErr, mysql.ErrInvalidConn) || errors.Is(scanErr, driver.ErrBadConn) {
				// A *sql.Tx can't be resumed on a fresh conn, so fail fast.
				if tx != nil {
					return false, backoff.Permanent(scanErr)
				}
				// Reconnect if the pool is down, then return the error so
				// backoff re-runs the whole query (exists hasn't surfaced a
				// result yet, so a re-run is always safe).
				if testErr := db.Test(); testErr != nil {
					return false, testErr
				}
				return false, scanErr
			}
			return false, backoff.Permanent(scanErr)
		}

		return got, nil
	}

	options := []backoff.RetryOption{
		backoff.WithBackOff(b),
		backoff.WithMaxElapsedTime(db.MaxExecutionTime),
	}
	if MaxAttempts > 0 {
		options = append(options, backoff.WithMaxTries(uint(MaxAttempts)))
	}

	exists, err = backoff.Retry(ctx, operation, options...)
	if err != nil {
		return exists, unwrapBackoffPermanent(err)
	}

	if len(cacheKey) != 0 {
		var val byte
		if exists {
			val = 1
		}
		err = db.cache.Set(ctx, cacheKey, []byte{val}, cacheDuration)
		if err != nil {
			err = fmt.Errorf("failed to set cache: %w", err)
			if db.HandleCacheError != nil {
				if handleErr := db.HandleCacheError(err); handleErr != nil {
					db.Logger.Warn("failed to handle cache error", "err", handleErr)
				}
			}
		}
	}

	return exists, err
}
