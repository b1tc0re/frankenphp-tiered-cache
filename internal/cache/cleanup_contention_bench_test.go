package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

var backgroundCleanupSampleSizes = [...]int{16, 32, 64, 128, 256}

func BenchmarkMemoryCacheBackgroundCleanupScan(b *testing.B) {
	for _, sampleSize := range backgroundCleanupSampleSizes {
		sampleSize := sampleSize
		b.Run(fmt.Sprintf("sample=%d", sampleSize), func(b *testing.B) {
			cache := newCleanupBenchCache(b)
			populateCleanupBenchShard(b, cache, 0, 2048)
			shard := &cache.shards[0]
			now := time.Now()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cache.purgeExpiredSample(shard, now, sampleSize)
			}
		})
	}
}

func BenchmarkMemoryCacheSetWithBackgroundCleanup(b *testing.B) {
	cases := []struct {
		name       string
		sampleSize int
	}{
		{name: "baseline", sampleSize: 0},
		{name: "sample=16", sampleSize: 16},
		{name: "sample=64", sampleSize: 64},
		{name: "sample=256", sampleSize: 256},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			cache := newCleanupBenchCache(b)
			populateCleanupBenchShard(b, cache, 0, 2048)

			setKey := keyForCleanupBenchShard(b, cache, 0, "set")
			value := []byte("value")
			stored, err := cache.Forever(setKey, value)
			if err != nil || !stored {
				b.Fatalf("Forever(%q) = %v, %v; want true, nil", setKey, stored, err)
			}

			var stop chan struct{}
			var wg sync.WaitGroup
			if tc.sampleSize > 0 {
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
							cache.purgeExpiredSample(shard, time.Now(), tc.sampleSize)
						}
					}
				}()
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				stored, err := cache.Forever(setKey, value)
				if err != nil || !stored {
					b.Fatalf("Forever(%q) = %v, %v; want true, nil", setKey, stored, err)
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

	cache, err := NewMemoryCache(MemoryConfig{
		MaxMemoryBytes:   128 << 20,
		MaxItemSizeBytes: 4 << 20,
	})
	if err != nil {
		b.Fatalf("NewMemoryCache() error = %v", err)
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
