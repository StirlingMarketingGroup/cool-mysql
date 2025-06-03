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
	w.mu.Lock()
	defer w.mu.Unlock()
	e, ok := w.values[key]
	if !ok {
		return nil, ErrCacheMiss
	}
	if !e.expires.IsZero() && time.Now().After(e.expires) {
		delete(w.values, key)
		return nil, ErrCacheMiss
	}
	if b := e.p.Value(); b != nil {
		out := make([]byte, len(*b))
		copy(out, *b)
		return out, nil
	}
	delete(w.values, key)
	return nil, ErrCacheMiss
}

func (w *WeakCache) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	buf := make([]byte, len(val))
	copy(buf, val)
	expires := time.Time{}
	if ttl > 0 {
		expires = time.Now().Add(ttl)
	}
	w.values[key] = &weakEntry{p: weak.Make(&buf), expires: expires}
	return nil
}

var _ Cache = (*WeakCache)(nil)
