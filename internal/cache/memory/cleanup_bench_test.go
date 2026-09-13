package memory

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

const cleanupExpiredBenchBatchSize = 256

func BenchmarkMemoryCacheBackgroundCleanupScan(b *testing.B) {
	cache := newCleanupBenchCache(b)
	populateCleanupBenchShard(b, cache, 0, 2048)
	shard := &cache.shards[0]
	now := time.Now()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.purgeExpiredSample(shard, now, backgroundCleanupSample)
	}
}

func BenchmarkMemoryCacheBackgroundCleanupExpired(b *testing.B) {
	cases := []struct {
		name         string
		expiredCount int
	}{
		{name: "live", expiredCount: 0},
		{name: "all-expired", expiredCount: backgroundCleanupSample},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			cache := &MemoryCache{}
			shards := make([]memoryShard, cleanupExpiredBenchBatchSize)
			now := time.Now()
			cache.current.Store(resetCleanupExpiredBenchShards(shards, now, tc.expiredCount))

			b.ReportAllocs()
			b.ResetTimer()

			completed := 0
			for completed < b.N {
				batch := cleanupExpiredBenchBatchSize
				if remaining := b.N - completed; remaining < batch {
					batch = remaining
				}

				for i := 0; i < batch; i++ {
					cache.purgeExpiredSample(&shards[i], now, backgroundCleanupSample)
				}
				completed += batch

				if completed < b.N {
					b.StopTimer()
					cache.current.Store(resetCleanupExpiredBenchShards(shards, now, tc.expiredCount))
					b.StartTimer()
				}
			}
		})
	}
}

func BenchmarkMemoryCacheGetCleanupContention(b *testing.B) {
	benchmarkMemoryCacheCleanupContention(b, false)
}

func BenchmarkMemoryCacheSetCleanupContention(b *testing.B) {
	benchmarkMemoryCacheCleanupContention(b, true)
}

func benchmarkMemoryCacheCleanupContention(b *testing.B, set bool) {
	b.Helper()

	cases := []struct {
		name         string
		expiredCount int
		cleanup      bool
	}{
		{name: "baseline"},
		{name: "live-scan", cleanup: true},
		{name: "all-expired", expiredCount: backgroundCleanupSample, cleanup: true},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			cache := newCleanupBenchCache(b)
			hotKey := keyForCleanupBenchShard(b, cache, 0, "hot")
			value := []byte("value")
			stored, err := cache.Forever(hotKey, value)
			if err != nil || !stored {
				b.Fatalf("Forever(%q) = %v, %v; want true, nil", hotKey, stored, err)
			}

			var stop chan struct{}
			var wg sync.WaitGroup
			if tc.cleanup {
				now := time.Now()
				expiredEntries := populateCleanupContentionEntries(cache, 0, now, tc.expiredCount)
				stop = make(chan struct{})
				wg.Add(1)
				go func() {
					defer wg.Done()
					shard := &cache.shards[0]
					for {
						select {
						case <-stop:
							return
						default:
							cache.purgeExpiredSample(shard, now, backgroundCleanupSample)
							restoreCleanupContentionEntries(cache, shard, expiredEntries)
						}
					}
				}()
			}

			b.ReportAllocs()
			b.ResetTimer()
			if set {
				for i := 0; i < b.N; i++ {
					stored, err := cache.Forever(hotKey, value)
					if err != nil || !stored {
						b.Fatalf("Forever(%q) = %v, %v; want true, nil", hotKey, stored, err)
					}
				}
			} else {
				for i := 0; i < b.N; i++ {
					got, _, err := cache.Get(hotKey)
					if err != nil || got == nil {
						b.Fatalf("Get(%q) = %q, %v; want hit", hotKey, got, err)
					}
				}
			}
			b.StopTimer()

			if stop != nil {
				close(stop)
				wg.Wait()
			}
		})
	}
}

func newCleanupBenchCache(b *testing.B) *MemoryCache {
	b.Helper()

	cache, err := New(Config{
		MaxMemoryBytes:   128 << 20,
		MaxItemSizeBytes: 4 << 20,
	})
	if err != nil {
		b.Fatalf("New() error = %v", err)
	}
	stopMemoryCacheMaintenanceForTest(cache)
	b.Cleanup(func() {
		if err := cache.Close(); err != nil {
			b.Errorf("Close() error = %v", err)
		}
	})

	return cache
}

func populateCleanupBenchShard(b *testing.B, cache *MemoryCache, shardIndex, count int) {
	b.Helper()

	for i := 0; i < count; i++ {
		key := keyForCleanupBenchShard(b, cache, shardIndex, fmt.Sprintf("entry-%d", i))
		stored, err := cache.Forever(key, []byte("value"))
		if err != nil || !stored {
			b.Fatalf("Forever(%q) = %v, %v; want true, nil", key, stored, err)
		}
	}
}

func resetCleanupExpiredBenchShards(shards []memoryShard, now time.Time, expiredCount int) int64 {
	var totalCost int64

	for i := range shards {
		entries := make(map[string]*memoryEntry, backgroundCleanupSample)
		for entryIndex := 0; entryIndex < backgroundCleanupSample; entryIndex++ {
			key := fmt.Sprintf("expired-bench:%d", entryIndex)
			expiresAt := time.Time{}
			if entryIndex < expiredCount {
				expiresAt = now.Add(-time.Second)
			}
			entry := &memoryEntry{
				value:     []byte("value"),
				expiresAt: expiresAt,
				cost:      itemCost(key, []byte("value")),
			}
			entries[key] = entry
			totalCost += entry.cost
		}
		shards[i].entries = entries
	}

	return totalCost
}

func populateCleanupContentionEntries(cache *MemoryCache, shardIndex int, now time.Time, expiredCount int) map[string]*memoryEntry {
	shard := &cache.shards[shardIndex]
	expiredEntries := make(map[string]*memoryEntry, expiredCount)

	shard.mu.Lock()
	for i := 0; i < backgroundCleanupSample; i++ {
		key := fmt.Sprintf("cleanup-contention:%d", i)
		expiresAt := time.Time{}
		if i < expiredCount {
			expiresAt = now.Add(-time.Second)
		}
		entry := &memoryEntry{
			value:     []byte("value"),
			expiresAt: expiresAt,
			cost:      itemCost(key, []byte("value")),
		}
		shard.entries[key] = entry
		cache.current.Add(entry.cost)
		if i < expiredCount {
			expiredEntries[key] = entry
		}
	}
	shard.mu.Unlock()

	return expiredEntries
}

func restoreCleanupContentionEntries(cache *MemoryCache, shard *memoryShard, entries map[string]*memoryEntry) {
	if len(entries) == 0 {
		return
	}

	shard.mu.Lock()
	for key, entry := range entries {
		if _, ok := shard.entries[key]; ok {
			continue
		}
		shard.entries[key] = entry
		cache.current.Add(entry.cost)
	}
	shard.mu.Unlock()
}

func keyForCleanupBenchShard(b *testing.B, cache *MemoryCache, shardIndex int, prefix string) string {
	b.Helper()

	if shardIndex < 0 || shardIndex >= len(cache.shards) {
		b.Fatalf("shard index %d out of range", shardIndex)
	}

	target := &cache.shards[shardIndex]
	for i := 0; i < 100_000; i++ {
		key := fmt.Sprintf("cleanup-bench:%s:%d", prefix, i)
		if cache.shardFor(key) == target {
			return key
		}
	}

	b.Fatalf("could not find key for shard %d", shardIndex)
	return ""
}
