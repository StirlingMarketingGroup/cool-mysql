package mysql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWeakCache(t *testing.T) {
	c := NewWeakCache()
	if _, err := c.Get(context.Background(), "nope"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("expected miss")
	}
	if err := c.Set(context.Background(), "k", []byte("v"), time.Minute); err != nil {
		t.Fatal(err)
	}
	b, err := c.Get(context.Background(), "k")
	if err != nil || string(b) != "v" {
		t.Fatalf("unexpected result %v %v", b, err)
	}
}

func TestMultiCache(t *testing.T) {
	wc := NewWeakCache()
	mc := NewWeakCache()
	m := NewMultiCache(wc, mc)
	_ = m.Set(context.Background(), "a", []byte("b"), time.Minute)
	b, err := wc.Get(context.Background(), "a")
	if err != nil || string(b) != "b" {
		t.Fatalf("weak cache not set")
	}
	// clear wc to force miss
	wc.values = map[string]*weakEntry{}
	b, err = m.Get(context.Background(), "a")
	if err != nil || string(b) != "b" {
		t.Fatalf("get failed %v %v", b, err)
	}
	if _, err = wc.Get(context.Background(), "a"); err != nil {
		t.Fatalf("should populate first cache")
	}
}

func TestMultiCacheBackPopulateCarriesTTL(t *testing.T) {
	wc0 := NewWeakCache()
	wc1 := NewWeakCache()
	m := NewMultiCache(wc0, wc1)
	const ttl = 50 * time.Millisecond
	if err := m.Set(context.Background(), "a", []byte("b"), ttl); err != nil {
		t.Fatal(err)
	}
	// Force a miss on tier 0 so Get back-populates from tier 1.
	wc0.values = map[string]*weakEntry{}
	before := time.Now()
	b, err := m.Get(context.Background(), "a")
	if err != nil || string(b) != "b" {
		t.Fatalf("get failed %v %v", b, err)
	}
	e, ok := wc0.values["a"]
	if !ok {
		t.Fatalf("tier 0 should hold the back-populated key")
	}
	if e.expires.IsZero() {
		t.Fatalf("back-populated entry must have a non-zero expiry")
	}
	// Remaining lifetime must not exceed the original TTL from now.
	if e.expires.After(before.Add(ttl + 10*time.Millisecond)) {
		t.Fatalf("expiry %v is beyond original TTL from now", e.expires)
	}
	// Wait past the TTL; both tiers must miss (not live forever).
	time.Sleep(ttl + 20*time.Millisecond)
	if _, err := wc0.Get(context.Background(), "a"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("tier 0 should have expired, got %v", err)
	}
	if _, err := wc1.Get(context.Background(), "a"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("tier 1 should have expired, got %v", err)
	}
}

// noTTLCache is a Cache that never reports a remaining TTL (no GetWithTTL).
type noTTLCache struct {
	vals map[string][]byte
}

func (n *noTTLCache) Get(ctx context.Context, key string) ([]byte, error) {
	v, ok := n.vals[key]
	if !ok {
		return nil, ErrCacheMiss
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

func (n *noTTLCache) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	if n.vals == nil {
		n.vals = make(map[string][]byte)
	}
	buf := make([]byte, len(val))
	copy(buf, val)
	n.vals[key] = buf
	return nil
}

func TestMultiCacheSkipsBackPopulateWithoutTTL(t *testing.T) {
	wc := NewWeakCache()
	src := &noTTLCache{vals: map[string][]byte{"a": []byte("b")}}
	m := NewMultiCache(wc, src)
	b, err := m.Get(context.Background(), "a")
	if err != nil || string(b) != "b" {
		t.Fatalf("get failed %v %v", b, err)
	}
	if _, err := wc.Get(context.Background(), "a"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("WeakCache must still miss — no immortal back-populate without TTL, got %v", err)
	}
}

// lockRecordingCache is a Cache+Locker that records Lock calls.
type lockRecordingCache struct {
	noTTLCache
	lockKeys []string
	unlocked int
}

func (l *lockRecordingCache) Lock(ctx context.Context, key string) (func() error, error) {
	l.lockKeys = append(l.lockKeys, key)
	return func() error {
		l.unlocked++
		return nil
	}, nil
}

func TestMultiCacheLockDelegatesToLockingTier(t *testing.T) {
	locking := &lockRecordingCache{}
	m := NewMultiCache(NewWeakCache(), locking)
	unlock, err := m.Lock(context.Background(), "k:mutex")
	if err != nil {
		t.Fatal(err)
	}
	if len(locking.lockKeys) != 1 || locking.lockKeys[0] != "k:mutex" {
		t.Fatalf("lock not delegated: %v", locking.lockKeys)
	}
	if err := unlock(); err != nil {
		t.Fatal(err)
	}
	if locking.unlocked != 1 {
		t.Fatalf("unlock not delegated: %d", locking.unlocked)
	}
}

func TestMultiCacheLockWithoutLockingTierIsNoop(t *testing.T) {
	m := NewMultiCache(NewWeakCache(), &noTTLCache{})
	unlock, err := m.Lock(context.Background(), "k:mutex")
	if err != nil {
		t.Fatal(err)
	}
	if err := unlock(); err != nil {
		t.Fatal(err)
	}
}

// TestUseCacheMultiCacheSetsLocker proves the original bug can't return: a
// MultiCache wrapping a locking tier must register as the Database's locker.
func TestUseCacheMultiCacheSetsLocker(t *testing.T) {
	db := &Database{}
	db.UseCache(NewMultiCache(NewWeakCache(), &lockRecordingCache{}))
	if db.locker == nil {
		t.Fatal("UseCache(MultiCache with locking tier) must set db.locker")
	}
}

func TestWeakCacheSetWithoutTTL(t *testing.T) {
	c := NewWeakCache()
	if err := c.Set(context.Background(), "k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(context.Background(), "k"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("expected miss after Set with ttl=0, got %v", err)
	}
}
