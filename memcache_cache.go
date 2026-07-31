package mysql

import (
	"context"
	"math"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

// MemcacheCache implements Cache using a memcached client.
type MemcacheCache struct {
	Client *memcache.Client
}

func NewMemcacheCache(client *memcache.Client) *MemcacheCache {
	return &MemcacheCache{Client: client}
}

func (m *MemcacheCache) Get(ctx context.Context, key string) ([]byte, error) {
	it, err := m.Client.Get(key)
	if err == memcache.ErrCacheMiss {
		return nil, ErrCacheMiss
	}
	if err != nil {
		return nil, err
	}
	return it.Value, nil
}

func (m *MemcacheCache) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	var exp int32
	if ttl > 0 {
		// memcached treats 0 as no expiry; int32(ttl.Seconds()) would turn any
		// sub-second TTL into an immortal entry, so round up.
		exp = int32(math.Ceil(ttl.Seconds()))
	}
	return m.Client.Set(&memcache.Item{Key: key, Value: val, Expiration: exp})
}

var _ Cache = (*MemcacheCache)(nil)
