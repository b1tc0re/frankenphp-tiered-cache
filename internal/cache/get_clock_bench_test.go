package cache

import (
	"testing"
	"time"
)

func BenchmarkMemoryCacheGetClockPlacement(b *testing.B) {
	cache, err := NewMemoryCache(MemoryConfig{})
	if err != nil {
		b.Fatalf("NewMemoryCache() error = %v", err)
	}
	defer func() {
		if err := cache.Close(); err != nil {
			b.Errorf("Close() error = %v", err)
		}
	}()

	stored, err := cache.Set("bench:get:clock", []byte("value"), time.Hour)
	if err != nil || !stored {
		b.Fatalf("Set() = %v, %v; want true, nil", stored, err)
	}

	b.Run("before-lock", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			value, err := memoryCacheGetClockBeforeLock(cache, "bench:get:clock")
			if err != nil || value == nil {
				b.Fatalf("Get() = %q, %v; want hit", value, err)
			}
		}
	})

	b.Run("after-lock", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			value, err := cache.Get("bench:get:clock")
			if err != nil || value == nil {
				b.Fatalf("Get() = %q, %v; want hit", value, err)
			}
		}
	})
}

func memoryCacheGetClockBeforeLock(cache *MemoryCache, key string) ([]byte, error) {
	shard := cache.shardFor(key)
	now := cache.now()

	shard.mu.RLock()
	entry, ok := shard.entries[key]
	if !ok {
		shard.mu.RUnlock()
		return nil, nil
	}
	if isExpired(entry.expiresAt, now) {
		shard.mu.RUnlock()
		cache.deleteExpired(shard, key, entry, now)
		return nil, nil
	}

	cache.recordAccess(entry)
	value := entry.value
	shard.mu.RUnlock()

	return value, nil
}
