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

// TTLCache is an optional interface a Cache may implement to report how much
// lifetime a cached entry has left. MultiCache uses it so a hit in a later tier
// can be back-populated into earlier tiers with the entry's remaining lifetime
// instead of forever.
type TTLCache interface {
	Cache

	// GetWithTTL returns the value along with its remaining lifetime. A ttl of
	// zero or less means the cache cannot report one for that entry.
	GetWithTTL(ctx context.Context, key string) ([]byte, time.Duration, error)
}

// MultiCache composes multiple caches. Reads check each cache in order and
// back-populate earlier caches on a hit with the source entry's remaining TTL
// (skipped when the source can't report one). Writes fan out to all caches.
type MultiCache struct {
	caches []Cache
}

// NewMultiCache creates a MultiCache from the provided caches.
func NewMultiCache(caches ...Cache) *MultiCache { return &MultiCache{caches: caches} }

func (m *MultiCache) Get(ctx context.Context, key string) ([]byte, error) {
	b, _, err := m.GetWithTTL(ctx, key)
	return b, err
}

func (m *MultiCache) GetWithTTL(ctx context.Context, key string) ([]byte, time.Duration, error) {
	var lastMiss error
	for i, c := range m.caches {
		b, ttl, err := cacheGetWithTTL(ctx, c, key)
		if err == nil {
			// Back-populating with an unknown lifetime would store the entry
			// forever in the earlier tier, serving it long past the TTL the
			// caller asked for, so only propagate a lifetime the source reports.
			if ttl > 0 {
				for j := range i {
					_ = m.caches[j].Set(ctx, key, b, ttl)
				}
			}
			return b, ttl, nil
		}
		if !errors.Is(err, ErrCacheMiss) {
			return nil, 0, err
		}
		lastMiss = err
	}
	if lastMiss == nil {
		lastMiss = ErrCacheMiss
	}
	return nil, 0, lastMiss
}

// cacheGetWithTTL reads from c, reporting the entry's remaining lifetime when c
// can supply one.
func cacheGetWithTTL(ctx context.Context, c Cache, key string) ([]byte, time.Duration, error) {
	if t, ok := c.(TTLCache); ok {
		return t.GetWithTTL(ctx, key)
	}
	b, err := c.Get(ctx, key)
	return b, 0, err
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
var _ TTLCache = (*MultiCache)(nil)
