package cache

import (
	"testing"
	"time"
)

func TestMemoryCacheTouchUpdatesLastAccess(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{})
	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	mustSet(t, cache, "key", []byte("value"), time.Minute)
	entry := cache.shardFor("key").entries["key"]
	initialLastAccess := entry.lastAccess.Load()

	now = now.Add(30 * time.Second)
	touched, err := cache.Touch("key", time.Minute)
	if err != nil || !touched {
		t.Fatalf("Touch() = %v, %v; want true, nil", touched, err)
	}
	if got := entry.lastAccess.Load(); got <= initialLastAccess {
		t.Fatalf("lastAccess = %d, want greater than %d", got, initialLastAccess)
	}
	if got := entry.lastAccess.Load(); got != cache.accessSeq.Load() {
		t.Fatalf("lastAccess = %d, access sequence = %d; want equal", got, cache.accessSeq.Load())
	}
}

func TestMemoryCacheForeverReplacesExpiringEntry(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{})
	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	mustSet(t, cache, "key", []byte("old"), time.Minute)
	now = now.Add(30 * time.Second)
	mustForever(t, cache, "key", []byte("new"))

	now = now.Add(24 * time.Hour)
	got, err := cache.Get("key")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(got) != "new" {
		t.Fatalf("Get() = %q, want new", got)
	}
}

func TestMemoryCacheSetReplacesForeverEntryWithExpiration(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{})
	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	mustForever(t, cache, "key", []byte("old"))
	mustSet(t, cache, "key", []byte("new"), time.Minute)

	now = now.Add(time.Minute)
	got, err := cache.Get("key")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got != nil {
		t.Fatalf("Get() = %q, want nil after replacement TTL expires", got)
	}
}
