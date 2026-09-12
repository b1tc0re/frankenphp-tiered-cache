package cache

import (
	"fmt"
	"testing"
	"time"
)

func TestMemoryCachePurgeExpiredShardOnlyCleansTargetShard(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{})
	stopMemoryCacheMaintenanceForTest(cache)

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
