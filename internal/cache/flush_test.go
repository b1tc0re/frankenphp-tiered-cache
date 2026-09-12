package cache

import "testing"

func TestMemoryCacheFlushReplacesShardMap(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{})
	stopMemoryCacheMaintenanceForTest(cache)

	mustForever(t, cache, "key", []byte("value"))
	shard := cache.shardFor("key")
	oldEntries := shard.entries

	flushed, err := cache.Flush()
	if err != nil || !flushed {
		t.Fatalf("Flush() = %v, %v; want true, nil", flushed, err)
	}

	oldEntries["retained"] = &memoryEntry{}

	shard.mu.RLock()
	_, reusedOldMap := shard.entries["retained"]
	entryCount := len(shard.entries)
	shard.mu.RUnlock()

	if reusedOldMap {
		t.Fatal("Flush() reused the old shard map")
	}
	if entryCount != 0 {
		t.Fatalf("new shard map contains %d entries, want 0", entryCount)
	}
	if got := cache.current.Load(); got != 0 {
		t.Fatalf("current bytes = %d after Flush(), want 0", got)
	}
}
