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
