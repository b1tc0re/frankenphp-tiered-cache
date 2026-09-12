package cache

import (
	"testing"
	"time"
)

func TestMemoryCacheGetMissDoesNotReadClock(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{})
	cache.now = func() time.Time {
		t.Fatal("Get() read clock for a missing key")
		return time.Time{}
	}

	value, err := cache.Get("missing")
	if err != nil || value != nil {
		t.Fatalf("Get(missing) = %q, %v; want nil, nil", value, err)
	}
}

func TestMemoryCacheTouchUsesCurrentTimeForNewDeadline(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{})
	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	mustSet(t, cache, "key", []byte("value"), time.Minute)
	now = time.Unix(150, 0)

	touched, err := cache.Touch("key", time.Minute)
	if err != nil || !touched {
		t.Fatalf("Touch() = %v, %v; want true, nil", touched, err)
	}

	shard := cache.shardFor("key")
	shard.mu.RLock()
	expiresAt := shard.entries["key"].expiresAt
	shard.mu.RUnlock()

	want := time.Unix(210, 0)
	if !expiresAt.Equal(want) {
		t.Fatalf("expiresAt = %v, want %v", expiresAt, want)
	}
}
