package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-sql-driver/mysql"
)

// exists efficiently checks if there are any rows in the given query
func exists(db *Database, conn commander, ctx context.Context, query string, cacheDuration time.Duration, params ...any) (exists bool, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	replacedQuery, normalizedParams, err := InterpolateParams(query, params...)
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

		cacheKey = key.String()

		start := time.Now()

	CHECK_CACHE:
		exists, err = db.redis.Get(ctx, cacheKey).Bool()
		if errors.Is(err, redis.Nil) {
			// cache miss!

			// grab a lock so we can update the cache
			mutex := db.rs.NewMutex(cacheKey+":mutex", redsync.WithTries(1))

			if err = mutex.Lock(); err != nil {
				// if we couldn't get the lock, then just check the cache again
				time.Sleep(RedisLockRetryDelay)
				goto CHECK_CACHE
			}

			unlock := func() {
				if mutex != nil && len(mutex.Value()) != 0 {
					if _, err = mutex.Unlock(); err != nil {
						db.Logger.Error(fmt.Sprintf("failed to unlock redis mutex: %v", err))
					}
				}
			}

			defer unlock()
		} else if err != nil {
			err = fmt.Errorf("failed to get data from redis: %w", err)
			ok := false
			if db.HandleRedisError != nil {
				ok = db.HandleRedisError(err)
			}
			if !ok {
				return
			}
		} else {
			db.callLog(replacedQuery, normalizedParams, time.Since(start), true)
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
	b.MaxElapsedTime = MaxExecutionTime
	err = backoff.Retry(func() error {
		var err error
		rows, err = conn.QueryContext(ctx, replacedQuery)
		if err != nil {
			if checkRetryError(err) {
				return err
			} else if errors.Is(err, mysql.ErrInvalidConn) {
				if err := db.Test(); err != nil {
					return err
				}
				return err
			} else {
				return backoff.Permanent(err)
			}
		}

		return nil
	}, backoff.WithContext(b, ctx))
	db.callLog(replacedQuery, normalizedParams, time.Since(start), false)
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
