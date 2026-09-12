package cache

import (
	"testing"
	"time"
)

func TestMemoryCacheBackgroundCleanupAdvancesAcrossShardBatches(t *testing.T) {
	cache, err := newMemoryCache(MemoryConfig{}, defaultShardCount, defaultLRUSamples)
	if err != nil {
		t.Fatalf("NewMemoryCache() error = %v", err)
	}
	stopMemoryCacheMaintenanceForTest(cache)
	t.Cleanup(func() {
		if err := cache.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	keys := make([]string, len(cache.shards))
	for shardIndex := range cache.shards {
		key := keyForMemoryShard(t, cache, shardIndex, "batch")
		keys[shardIndex] = key
		mustSet(t, cache, key, []byte("value"), time.Second)
	}

	now = now.Add(2 * time.Second)
	nextShard := 0

	for batch := 0; batch < defaultShardCount/backgroundCleanupShardsPerTick; batch++ {
		nextShard = cache.purgeExpiredBackgroundBatch(now, nextShard)

		wantNext := ((batch + 1) * backgroundCleanupShardsPerTick) % len(cache.shards)
		if nextShard != wantNext {
			t.Fatalf("next shard after batch %d = %d, want %d", batch+1, nextShard, wantNext)
		}

		cleanedThrough := (batch + 1) * backgroundCleanupShardsPerTick
		for shardIndex, key := range keys {
			present := memoryShardHasKey(&cache.shards[shardIndex], key)
			wantPresent := shardIndex >= cleanedThrough
			if present != wantPresent {
				t.Fatalf(
					"entry in shard %d after batch %d present = %v, want %v",
					shardIndex,
					batch+1,
					present,
					wantPresent,
				)
			}
		}
	}

	if nextShard != 0 {
		t.Fatalf("next shard after full round = %d, want 0", nextShard)
	}
	if got := cache.current.Load(); got != 0 {
		t.Fatalf("current bytes after full round = %d, want 0", got)
	}
}

func TestMemoryCacheBackgroundCleanupScansSmallCacheOncePerTick(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{})
	stopMemoryCacheMaintenanceForTest(cache)

	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	for shardIndex := range cache.shards {
		key := keyForMemoryShard(t, cache, shardIndex, "small-batch")
		mustSet(t, cache, key, []byte("value"), time.Second)
	}

	now = now.Add(2 * time.Second)
	nextShard := cache.purgeExpiredBackgroundBatch(now, 0)

	if nextShard != 0 {
		t.Fatalf("next shard = %d, want 0", nextShard)
	}
	if got := cache.current.Load(); got != 0 {
		t.Fatalf("current bytes = %d, want 0", got)
	}
}
