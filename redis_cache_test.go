package mysql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRedisCache(t *testing.T) (*RedisCache, *miniredis.Miniredis) {
	t.Helper()
	s := miniredis.RunT(t)
	return NewRedisCache(redis.NewClient(&redis.Options{Addr: s.Addr()})), s
}

func TestRedisCacheGetWithTTL(t *testing.T) {
	c, s := newTestRedisCache(t)
	ctx := context.Background()
	const ttl = time.Minute
	if err := c.Set(ctx, "k", []byte("v"), ttl); err != nil {
		t.Fatal(err)
	}
	b, got, err := c.GetWithTTL(ctx, "k")
	if err != nil {
		t.Fatalf("GetWithTTL: %v", err)
	}
	if string(b) != "v" {
		t.Fatalf("value = %q, want %q", b, "v")
	}
	if got <= 0 || got > ttl {
		t.Fatalf("ttl = %v, want in (0, %v]", got, ttl)
	}
	s.FastForward(ttl)
	if _, err := c.Get(ctx, "k"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("after FastForward: Get = %v, want ErrCacheMiss", err)
	}
}

func TestRedisCacheGetWithTTLMiss(t *testing.T) {
	c, _ := newTestRedisCache(t)
	b, ttl, err := c.GetWithTTL(context.Background(), "absent")
	if !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("err = %v, want ErrCacheMiss", err)
	}
	if b != nil {
		t.Fatalf("value = %v, want nil", b)
	}
	if ttl != 0 {
		t.Fatalf("ttl = %v, want 0", ttl)
	}
}

func TestRedisCacheGetWithTTLNoExpiry(t *testing.T) {
	c, s := newTestRedisCache(t)
	ctx := context.Background()
	// Key with no expiry: PTTL returns -1; GetWithTTL must report 0, not a
	// negative duration.
	if err := s.Set("k", "v"); err != nil {
		t.Fatal(err)
	}
	b, ttl, err := c.GetWithTTL(ctx, "k")
	if err != nil {
		t.Fatalf("GetWithTTL: %v", err)
	}
	if string(b) != "v" {
		t.Fatalf("value = %q, want %q", b, "v")
	}
	if ttl != 0 {
		t.Fatalf("ttl = %v, want 0 (PTTL -1 must not leak)", ttl)
	}
}

func TestRedisCacheGet(t *testing.T) {
	c, _ := newTestRedisCache(t)
	ctx := context.Background()
	if err := c.Set(ctx, "k", []byte("v"), time.Minute); err != nil {
		t.Fatal(err)
	}
	b, err := c.Get(ctx, "k")
	if err != nil || string(b) != "v" {
		t.Fatalf("Get live key: %v %v", b, err)
	}
	if _, err := c.Get(ctx, "absent"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("Get absent: %v, want ErrCacheMiss", err)
	}
}

func TestMultiCacheBackPopulateFromRedis(t *testing.T) {
	redisCache, _ := newTestRedisCache(t)
	wc := NewWeakCache()
	m := NewMultiCache(wc, redisCache)
	ctx := context.Background()
	const ttl = 2 * time.Second
	if err := redisCache.Set(ctx, "k", []byte("v"), ttl); err != nil {
		t.Fatal(err)
	}
	before := time.Now()
	b, err := m.Get(ctx, "k")
	if err != nil || string(b) != "v" {
		t.Fatalf("MultiCache.Get: %v %v", b, err)
	}
	e, ok := wc.values["k"]
	if !ok {
		t.Fatalf("WeakCache tier should hold the back-populated key")
	}
	if e.expires.IsZero() {
		t.Fatalf("back-populated entry must have a non-zero expiry")
	}
	// Remaining lifetime must not exceed the source TTL from now (+ small slack).
	if e.expires.After(before.Add(ttl + 50*time.Millisecond)) {
		t.Fatalf("expiry %v is beyond original TTL from now", e.expires)
	}
	// miniredis FastForward does not move WeakCache's wall clock — use a real sleep.
	time.Sleep(ttl + 100*time.Millisecond)
	if _, err := wc.Get(ctx, "k"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("WeakCache should have expired after ~%v, got %v", ttl, err)
	}
}
