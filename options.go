package mysql

import "time"

// Option configures a Database.
type Option func(*Database)

// WithMaxExecutionTime sets the maximum backoff elapsed time for queries.
func WithMaxExecutionTime(d time.Duration) Option {
	return func(db *Database) { db.maxExecutionTime = d }
}

// WithConnectionLifetime sets the maximum lifetime for connections.
func WithConnectionLifetime(d time.Duration) Option {
	return func(db *Database) { db.maxConnectionTime = d }
}

// WithRedisLockRetryDelay sets the delay between lock retries when caching.
func WithRedisLockRetryDelay(d time.Duration) Option {
	return func(db *Database) { db.redisLockRetryDelay = d }
}

// WithDefaultCacheDuration sets a default cache duration for Select/Exists helpers.
func WithDefaultCacheDuration(d time.Duration) Option {
	return func(db *Database) { db.defaultCacheDuration = d }
}
