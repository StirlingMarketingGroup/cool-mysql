package mysql

import (
	"context"
	"errors"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
)

// RedisCache implements Cache and Locker using go-redis and redsync.
type RedisCache struct {
	Client redis.UniversalClient
	rs     *redsync.Redsync
}

// NewRedisCache creates a RedisCache from a universal client.
func NewRedisCache(client redis.UniversalClient) *RedisCache {
	return &RedisCache{
		Client: client,
		rs:     redsync.New(goredis.NewPool(client)),
	}
}

func (r *RedisCache) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := r.Client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrCacheMiss
	}
	return b, err
}

func (r *RedisCache) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	return r.Client.Set(ctx, key, val, ttl).Err()
}

func (r *RedisCache) Lock(ctx context.Context, key string) (func() error, error) {
	m := r.rs.NewMutex(key, redsync.WithTries(1))
	if err := m.LockContext(ctx); err != nil {
		return nil, err
	}
	return func() error {
		_, err := m.Unlock()
		return err
	}, nil
}

var _ Cache = (*RedisCache)(nil)
var _ Locker = (*RedisCache)(nil)
