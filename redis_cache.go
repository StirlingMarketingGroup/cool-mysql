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
	b, _, err := r.GetWithTTL(ctx, key)
	return b, err
}

func (r *RedisCache) GetWithTTL(ctx context.Context, key string) ([]byte, time.Duration, error) {
	var get *redis.StringCmd
	var pttl *redis.DurationCmd
	if _, err := r.Client.Pipelined(ctx, func(p redis.Pipeliner) error {
		get = p.Get(ctx, key)
		pttl = p.PTTL(ctx, key)
		return nil
	}); err != nil && !errors.Is(err, redis.Nil) {
		return nil, 0, err
	}

	b, err := get.Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, 0, ErrCacheMiss
	}
	if err != nil {
		return nil, 0, err
	}

	ttl, err := pttl.Result()
	if err != nil {
		return nil, 0, err
	}
	// PTTL reports a negative duration for a key that is gone (-2) or has no
	// expiry (-1); neither is a lifetime worth propagating.
	if ttl < 0 {
		ttl = 0
	}

	return b, ttl, nil
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
var _ TTLCache = (*RedisCache)(nil)
