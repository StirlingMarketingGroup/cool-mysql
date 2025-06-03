package mysql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/sha3"
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
		exists, err = db.redis.Get(ctx, cacheKey).Bool()
		if errors.Is(err, redis.Nil) {
			// cache miss!

			// grab a lock so we can update the cache
			mutex := db.rs.NewMutex(cacheKey+":mutex", redsync.WithTries(1))

			if err = mutex.Lock(); err != nil {
				// if we couldn't get the lock, then just check the cache again
				time.Sleep(db.redisLockRetryDelay)
				goto CHECK_CACHE
			}

			unlock := func() {
				if mutex != nil && len(mutex.Value()) != 0 {
					if _, err = mutex.Unlock(); err != nil {
						db.Logger.Warn(fmt.Sprintf("failed to unlock redis mutex: %v", err))
					}
				}
			}

			defer unlock()
		} else if err != nil {
			err = fmt.Errorf("failed to get data from redis: %w", err)
			if db.HandleRedisError != nil {
				err = db.HandleRedisError(err)
			}
			if err != nil {
				return
			}
		} else {
			tx, _ := conn.(*sql.Tx)
			db.callLog(LogDetail{
				Query:    replacedQuery,
				Params:   normalizedParams,
				Duration: time.Since(start),
				CacheHit: true,
				Tx:       tx,
				Attempt:  1,
			})
			return
		}
	}

	var rows *sql.Rows
	defer func() {
		if rows != nil {
			rows.Close()
		}
	}()

	start := time.Now()

	var b = backoff.NewExponentialBackOff()
	b.MaxElapsedTime = db.maxExecutionTime
	var attempt int
	err = backoff.Retry(func() error {
		attempt++
		var err error
		rows, err = conn.QueryContext(ctx, replacedQuery)
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
				return err
			} else if errors.Is(err, mysql.ErrInvalidConn) {
				return db.Test()
			} else {
				return backoff.Permanent(err)
			}
		}

		return nil
	}, backoff.WithContext(b, ctx))
	if err != nil {
		return
	}

	exists = rows.Next()
	if err = rows.Err(); err != nil {
		return
	}

	if len(cacheKey) != 0 {
		err = db.redis.Set(ctx, cacheKey, exists, cacheDuration).Err()
		if err != nil {
			err = fmt.Errorf("failed to set redis cache: %w", err)
			if db.HandleRedisError != nil {
				db.HandleRedisError(err)
			}
		}
	}

	return
}
