package cache

import (
	"sync"
	"testing"
	"time"
)

func TestMemoryCacheLRUClockTracksSuccessfulAccesses(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{})
	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	mustForever(t, cache, "key", []byte("value"))
	if got := memoryEntryAccessClock(cache, "key"); got != cache.lruClock.Load() {
		t.Fatalf("lastAccess = %d, lru clock = %d; want equal", got, cache.lruClock.Load())
	}

	clock := cache.lruClock.Add(1)

	// Moving wall-clock time backwards must not affect local LRU ordering.
	now = time.Unix(1, 0)
	got, err := cache.Get("key")
	if err != nil || got == nil {
		t.Fatalf("Get() = %q, %v; want hit", got, err)
	}
	if got := memoryEntryAccessClock(cache, "key"); got != clock {
		t.Fatalf("lastAccess after Get() = %d, want %d", got, clock)
	}
	if got := cache.lruClock.Load(); got != clock {
		t.Fatalf("Get() changed lru clock to %d, want %d", got, clock)
	}

	if _, err := cache.Get("missing"); err != nil {
		t.Fatalf("Get(missing) error = %v", err)
	}
	if got := cache.lruClock.Load(); got != clock {
		t.Fatalf("miss changed lru clock to %d, want %d", got, clock)
	}
}

func TestStoreMaxAccessClockDoesNotRegressWithConcurrentUpdates(t *testing.T) {
	entry := &memoryEntry{}

	lowReady := make(chan struct{})
	releaseLow := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		close(lowReady)
		<-releaseLow
		storeMaxAccessClock(entry, 1)
	}()

	go func() {
		defer wg.Done()
		<-lowReady
		storeMaxAccessClock(entry, 2)
		close(releaseLow)
	}()

	wg.Wait()

	if got := entry.lastAccess.Load(); got != 2 {
		t.Fatalf("lastAccess = %d, want 2", got)
	}
}

func TestMemoryCacheConcurrentGetsUseCurrentLRUClock(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{})
	mustForever(t, cache, "key", []byte("value"))
	clock := cache.lruClock.Add(1)

	const (
		workers    = 32
		iterations = 1000
	)

	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				got, err := cache.Get("key")
				if err != nil || got == nil {
					t.Errorf("Get() = %q, %v; want hit", got, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	if got := memoryEntryAccessClock(cache, "key"); got != clock {
		t.Fatalf("lastAccess = %d, want %d", got, clock)
	}
	if got := cache.lruClock.Load(); got != clock {
		t.Fatalf("concurrent Get() changed lru clock to %d, want %d", got, clock)
	}
}

func TestMemoryCacheCloseStopsMaintenance(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{})

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

func memoryEntryAccessClock(cache *MemoryCache, key string) uint64 {
	shard := cache.shardFor(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry := shard.entries[key]
	if entry == nil {
		return 0
	}

	return entry.lastAccess.Load()
}
