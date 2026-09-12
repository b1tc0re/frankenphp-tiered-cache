package cache

import "testing"

func TestMemoryCacheStaleEvictionCandidateDoesNotRemoveReplacement(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{
		MaxMemoryBytes:   1024,
		MaxItemSizeBytes: 1024,
	})

	mustForever(t, cache, "key", []byte("old"))

	shard := cache.shardFor("key")
	shard.mu.RLock()
	original := shard.entries["key"]
	shard.mu.RUnlock()

	candidate := &evictionCandidate{
		shard:      shard,
		key:        "key",
		entry:      original,
		lastAccess: original.lastAccess.Load(),
	}

	mustForever(t, cache, "key", []byte("new"))
	before := cache.current.Load()

	if cache.tryEvictCandidate(candidate) {
		t.Fatal("tryEvictCandidate() = true for stale candidate; want false")
	}
	if got := cache.current.Load(); got != before {
		t.Fatalf("current bytes = %d after stale eviction attempt, want %d", got, before)
	}

	got, err := cache.Get("key")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(got) != "new" {
		t.Fatalf("Get() = %q, want replacement value", got)
	}
}

func TestMemoryCacheEvictionCandidateReportsActualDeletion(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{
		MaxMemoryBytes:   1024,
		MaxItemSizeBytes: 1024,
	})

	mustForever(t, cache, "key", []byte("value"))

	shard := cache.shardFor("key")
	shard.mu.RLock()
	entry := shard.entries["key"]
	shard.mu.RUnlock()

	candidate := &evictionCandidate{
		shard:      shard,
		key:        "key",
		entry:      entry,
		lastAccess: entry.lastAccess.Load(),
	}
	before := cache.current.Load()

	if !cache.tryEvictCandidate(candidate) {
		t.Fatal("tryEvictCandidate() = false for current candidate; want true")
	}
	if got := cache.current.Load(); got != before-entry.cost {
		t.Fatalf("current bytes = %d after eviction, want %d", got, before-entry.cost)
	}

	got, err := cache.Get("key")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got != nil {
		t.Fatalf("Get() = %q after eviction, want nil", got)
	}
}
