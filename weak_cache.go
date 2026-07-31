//go:build go1.24

package mysql

import (
	"context"
	"sync"
	"time"

	"weak"
)

type weakEntry struct {
	p       weak.Pointer[[]byte]
	expires time.Time
}

// WeakCache stores values in memory using weak pointers so the garbage
// collector may reclaim them under pressure.
type WeakCache struct {
	mu     sync.Mutex
	values map[string]*weakEntry
}

func NewWeakCache() *WeakCache { return &WeakCache{values: make(map[string]*weakEntry)} }

func (w *WeakCache) Get(ctx context.Context, key string) ([]byte, error) {
	b, _, err := w.GetWithTTL(ctx, key)
	return b, err
}

func (w *WeakCache) GetWithTTL(ctx context.Context, key string) ([]byte, time.Duration, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	e, ok := w.values[key]
	if !ok {
		return nil, 0, ErrCacheMiss
	}
	if time.Now().After(e.expires) {
		delete(w.values, key)
		return nil, 0, ErrCacheMiss
	}
	if b := e.p.Value(); b != nil {
		out := make([]byte, len(*b))
		copy(out, *b)
		return out, time.Until(e.expires), nil
	}
	delete(w.values, key)
	return nil, 0, ErrCacheMiss
}

func (w *WeakCache) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	// An entry with no expiry can outlive every TTL the caller declared, and
	// there is no query cache for which that is correct.
	if ttl <= 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	buf := make([]byte, len(val))
	copy(buf, val)
	w.values[key] = &weakEntry{p: weak.Make(&buf), expires: time.Now().Add(ttl)}
	return nil
}

var _ Cache = (*WeakCache)(nil)
var _ TTLCache = (*WeakCache)(nil)
