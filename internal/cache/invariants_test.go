package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMemoryCacheAccountingMatchesStoredEntriesAfterConcurrentAccess(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{
		MaxMemoryBytes:   1 << 20,
		MaxItemSizeBytes: 16 << 10,
	})

	var wg sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				key := fmt.Sprintf("accounting-%d-%d", worker, i%32)
				value := []byte(fmt.Sprintf("value-%d-%d", worker, i))

				if stored, err := cache.Set(key, value, time.Minute); err != nil || !stored {
					t.Errorf("Set() = %v, %v", stored, err)
					return
				}
				if i%7 == 0 {
					if _, err := cache.Forget(key); err != nil {
						t.Errorf("Forget() error = %v", err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()

	var accounted int64
	for i := range cache.shards {
		shard := &cache.shards[i]
		shard.mu.RLock()
		for _, entry := range shard.entries {
			accounted += entry.cost
		}
		shard.mu.RUnlock()
	}

	if got := cache.current.Load(); got != accounted {
		t.Fatalf("current bytes = %d, stored entry cost = %d", got, accounted)
	}
}

func TestMemoryConfigRejectsNegativeLimits(t *testing.T) {
	if _, err := (MemoryConfig{MaxMemoryBytes: -1}).normalized(); err == nil {
		t.Fatal("negative max memory was accepted")
	}
	if _, err := (MemoryConfig{MaxItemSizeBytes: -1}).normalized(); err == nil {
		t.Fatal("negative max item size was accepted")
	}
}

func TestMemoryConfigRejectsItemLimitAboveMemoryLimit(t *testing.T) {
	if _, err := (MemoryConfig{
		MaxMemoryBytes:   1024,
		MaxItemSizeBytes: 2048,
	}).normalized(); err == nil {
		t.Fatal("max item size above max memory was accepted")
	}
}

func TestMemoryConfigClampsDefaultItemLimitToMemoryLimit(t *testing.T) {
	cfg, err := (MemoryConfig{MaxMemoryBytes: 1024}).normalized()
	if err != nil {
		t.Fatalf("normalized() error = %v", err)
	}
	if cfg.MaxItemSizeBytes != 1024 {
		t.Fatalf("max item size = %d, want 1024", cfg.MaxItemSizeBytes)
	}
}
