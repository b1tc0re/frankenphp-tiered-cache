package cache

import (
	"testing"
	"time"
)

func TestMemoryCacheAccessSequenceTracksSuccessfulAccesses(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{})
	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	mustForever(t, cache, "key", []byte("value"))
	if got := cache.accessSeq.Load(); got != 1 {
		t.Fatalf("access sequence after store = %d, want 1", got)
	}
	if got := memoryEntryAccessSequence(cache, "key"); got != 1 {
		t.Fatalf("lastAccess after store = %d, want 1", got)
	}

	// Moving wall-clock time backwards must not affect local LRU ordering.
	now = time.Unix(1, 0)
	got, err := cache.Get("key")
	if err != nil || got == nil {
		t.Fatalf("Get() = %q, %v; want hit", got, err)
	}
	if seq := cache.accessSeq.Load(); seq != 2 {
		t.Fatalf("access sequence after Get() = %d, want 2", seq)
	}
	if seq := memoryEntryAccessSequence(cache, "key"); seq != 2 {
		t.Fatalf("lastAccess after Get() = %d, want 2", seq)
	}

	touched, err := cache.Touch("key", time.Minute)
	if err != nil || !touched {
		t.Fatalf("Touch() = %v, %v; want true, nil", touched, err)
	}
	if seq := cache.accessSeq.Load(); seq != 3 {
		t.Fatalf("access sequence after Touch() = %d, want 3", seq)
	}
	if seq := memoryEntryAccessSequence(cache, "key"); seq != 3 {
		t.Fatalf("lastAccess after Touch() = %d, want 3", seq)
	}

	if _, err := cache.Get("missing"); err != nil {
		t.Fatalf("Get(missing) error = %v", err)
	}
	if seq := cache.accessSeq.Load(); seq != 3 {
		t.Fatalf("access sequence after miss = %d, want 3", seq)
	}
}

func memoryEntryAccessSequence(cache *MemoryCache, key string) uint64 {
	shard := cache.shardFor(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry := shard.entries[key]
	if entry == nil {
		return 0
	}

	return entry.lastAccess.Load()
}
