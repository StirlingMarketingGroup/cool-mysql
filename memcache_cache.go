package mysql

import (
	"context"
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
	return m.Client.Set(&memcache.Item{Key: key, Value: val, Expiration: int32(ttl.Seconds())})
}

var _ Cache = (*MemcacheCache)(nil)
