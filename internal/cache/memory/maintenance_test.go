package memory

import (
	"fmt"
	"testing"
	"time"
)

func TestMemoryCachePurgeExpiredShardOnlyCleansTargetShard(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})

	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	expiredShard0 := keyForMemoryShard(t, cache, 0, "expired-0")
	liveShard0 := keyForMemoryShard(t, cache, 0, "live-0")
	expiredShard1 := keyForMemoryShard(t, cache, 1, "expired-1")

	mustSet(t, cache, expiredShard0, []byte("expired-0"), time.Second)
	mustForever(t, cache, liveShard0, []byte("live"))
	mustSet(t, cache, expiredShard1, []byte("expired-1"), time.Second)

	shard0ExpiredCost := itemCost(expiredShard0, []byte("expired-0"))
	shard1ExpiredCost := itemCost(expiredShard1, []byte("expired-1"))
	before := cache.current.Load()

	now = now.Add(2 * time.Second)
	cache.purgeExpiredShard(&cache.shards[0], now)

	if memoryShardHasKey(&cache.shards[0], expiredShard0) {
		t.Fatal("expired entry in cleaned shard was not removed")
	}
	if !memoryShardHasKey(&cache.shards[0], liveShard0) {
		t.Fatal("live entry in cleaned shard was removed")
	}
	if !memoryShardHasKey(&cache.shards[1], expiredShard1) {
		t.Fatal("expired entry in another shard was removed")
	}
	if got := cache.current.Load(); got != before-shard0ExpiredCost {
		t.Fatalf("current bytes = %d after first shard cleanup, want %d", got, before-shard0ExpiredCost)
	}

	cache.purgeExpiredShard(&cache.shards[1], now)
	if memoryShardHasKey(&cache.shards[1], expiredShard1) {
		t.Fatal("expired entry in second cleaned shard was not removed")
	}
	if got := cache.current.Load(); got != before-shard0ExpiredCost-shard1ExpiredCost {
		t.Fatalf("current bytes = %d after second shard cleanup, want %d", got, before-shard0ExpiredCost-shard1ExpiredCost)
	}
}

func TestMemoryCachePurgeExpiredSampleIsBounded(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})

	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	const entries = 5
	keys := make([]string, 0, entries)
	for i := 0; i < entries; i++ {
		key := keyForMemoryShard(t, cache, 0, fmt.Sprintf("sample-%d", i))
		keys = append(keys, key)
		mustSet(t, cache, key, []byte("value"), time.Second)
	}

	before := cache.current.Load()
	now = now.Add(2 * time.Second)
	cache.purgeExpiredSample(&cache.shards[0], now, 2)

	remaining := 0
	remainingCost := int64(0)
	for _, key := range keys {
		if memoryShardHasKey(&cache.shards[0], key) {
			remaining++
			remainingCost += itemCost(key, []byte("value"))
		}
	}

	if remaining != entries-2 {
		t.Fatalf("remaining entries = %d, want %d", remaining, entries-2)
	}
	if got := cache.current.Load(); got != remainingCost {
		t.Fatalf("current bytes = %d after bounded cleanup, want %d (before=%d)", got, remainingCost, before)
	}
}

func TestMemoryCacheBackgroundCleanupAdvancesAcrossShardBatches(t *testing.T) {
	cache, err := newMemoryCache(Config{}, defaultShardCount, defaultLRUSamples)
	if err != nil {
		t.Fatalf("New() error = %v", err)
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
				t.Fatalf("entry in shard %d after batch %d present = %v, want %v", shardIndex, batch+1, present, wantPresent)
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
	cache := newTestMemoryCache(t, Config{})
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

func TestNewCloseStopsRunningMaintenance(t *testing.T) {
	cache, err := New(Config{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := cache.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	select {
	case <-cache.maintenanceDone:
	default:
		t.Fatal("maintenance goroutine is still running after Close()")
	}

	if err := cache.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func stopMemoryCacheMaintenanceForTest(cache *MemoryCache) {
	cache.closeOnce.Do(func() {
		close(cache.maintenanceStop)
		<-cache.maintenanceDone
	})
}

func keyForMemoryShard(t *testing.T, cache *MemoryCache, shardIndex int, prefix string) string {
	t.Helper()
	if shardIndex < 0 || shardIndex >= len(cache.shards) {
		t.Fatalf("shard index %d out of range", shardIndex)
	}

	target := &cache.shards[shardIndex]
	for i := 0; i < 100_000; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		if cache.shardFor(key) == target {
			return key
		}
	}

	t.Fatalf("could not find key for shard %d", shardIndex)
	return ""
}

func memoryShardHasKey(shard *memoryShard, key string) bool {
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	_, ok := shard.entries[key]
	return ok
}
