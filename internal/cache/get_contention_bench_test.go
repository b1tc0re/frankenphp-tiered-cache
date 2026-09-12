package cache

import (
	"fmt"
	"sync"
	"testing"
)

var memoryCacheGetWorkerCounts = [...]int{1, 2, 4, 8, 16, 32, 64}

func BenchmarkMemoryCacheGetContention(b *testing.B) {
	for _, workers := range memoryCacheGetWorkerCounts {
		workers := workers

		b.Run(fmt.Sprintf("same-key/workers=%d/tracked", workers), func(b *testing.B) {
			benchmarkMemoryCacheGet(b, workers, false, true)
		})
		b.Run(fmt.Sprintf("same-key/workers=%d/baseline", workers), func(b *testing.B) {
			benchmarkMemoryCacheGet(b, workers, false, false)
		})
		b.Run(fmt.Sprintf("distinct-keys/workers=%d/tracked", workers), func(b *testing.B) {
			benchmarkMemoryCacheGet(b, workers, true, true)
		})
		b.Run(fmt.Sprintf("distinct-keys/workers=%d/baseline", workers), func(b *testing.B) {
			benchmarkMemoryCacheGet(b, workers, true, false)
		})
	}
}

func benchmarkMemoryCacheGet(b *testing.B, workers int, distinctKeys, tracked bool) {
	b.Helper()

	cache, err := NewMemoryCache(MemoryConfig{})
	if err != nil {
		b.Fatalf("NewMemoryCache() error = %v", err)
	}
	defer func() {
		if err := cache.Close(); err != nil {
			b.Errorf("Close() error = %v", err)
		}
	}()

	keys := make([]string, workers)
	if distinctKeys {
		for i := range keys {
			keys[i] = fmt.Sprintf("bench:get:%d", i)
			stored, err := cache.Forever(keys[i], []byte("value"))
			if err != nil || !stored {
				b.Fatalf("Forever(%q) = %v, %v; want true, nil", keys[i], stored, err)
			}
		}
	} else {
		for i := range keys {
			keys[i] = "bench:get"
		}
		stored, err := cache.Forever(keys[0], []byte("value"))
		if err != nil || !stored {
			b.Fatalf("Forever(%q) = %v, %v; want true, nil", keys[0], stored, err)
		}
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(workers)

	baseIterations := b.N / workers
	remainder := b.N % workers

	for worker := 0; worker < workers; worker++ {
		worker := worker
		iterations := baseIterations
		if worker < remainder {
			iterations++
		}

		go func() {
			defer wg.Done()
			<-start

			key := keys[worker]
			for i := 0; i < iterations; i++ {
				var value []byte
				var err error
				if tracked {
					value, err = cache.Get(key)
				} else {
					value, err = memoryCacheGetWithoutAccessTracking(cache, key)
				}
				if err != nil || value == nil {
					b.Errorf("Get(%q) = %q, %v; want hit", key, value, err)
					return
				}
			}
		}()
	}

	b.ReportAllocs()
	b.ResetTimer()
	close(start)
	wg.Wait()
	b.StopTimer()
}

// memoryCacheGetWithoutAccessTracking mirrors MemoryCache.Get but intentionally
// omits recordAccess. It exists only as a benchmark baseline for measuring the
// cost of coarse LRU access tracking.
func memoryCacheGetWithoutAccessTracking(cache *MemoryCache, key string) ([]byte, error) {
	shard := cache.shardFor(key)
	now := cache.now().UnixNano()

	shard.mu.RLock()
	entry, ok := shard.entries[key]
	if !ok {
		shard.mu.RUnlock()
		return nil, nil
	}
	if entry.expiresAt > 0 && entry.expiresAt <= now {
		shard.mu.RUnlock()
		cache.deleteExpired(shard, key, entry, now)
		return nil, nil
	}

	value := entry.value
	shard.mu.RUnlock()

	return value, nil
}
