package mysql

import (
	"context"
	"errors"
	"time"
)

// ErrCacheMiss is returned by Cache implementations when a key is not found.
var ErrCacheMiss = errors.New("cache miss")

// Cache defines basic get/set operations for query caching.
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
}

// Locker provides optional distributed locking for cache population.
type Locker interface {
	Lock(ctx context.Context, key string) (func() error, error)
}

// MultiCache composes multiple caches. Reads check each cache in order and
// populate earlier caches on a hit. Writes fan out to all caches.
type MultiCache struct {
	caches []Cache
}

// NewMultiCache creates a MultiCache from the provided caches.
func NewMultiCache(caches ...Cache) *MultiCache { return &MultiCache{caches: caches} }

func (m *MultiCache) Get(ctx context.Context, key string) ([]byte, error) {
	var lastMiss error
	for i, c := range m.caches {
		b, err := c.Get(ctx, key)
		if err == nil {
			for j := 0; j < i; j++ {
				_ = m.caches[j].Set(ctx, key, b, 0)
			}
			return b, nil
		}
		if !errors.Is(err, ErrCacheMiss) {
			return nil, err
		}
		lastMiss = err
	}
	if lastMiss == nil {
		lastMiss = ErrCacheMiss
	}
	return nil, lastMiss
}

func (m *MultiCache) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	var lastErr error
	for _, c := range m.caches {
		if err := c.Set(ctx, key, val, ttl); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

var _ Cache = (*MultiCache)(nil)
