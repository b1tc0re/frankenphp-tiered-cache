package memory

import (
	"testing"
	"time"
)

type recordingObserver struct {
	events []EvictionEvent
}

func (o *recordingObserver) OnEviction(event EvictionEvent) {
	o.events = append(o.events, event)
}

type lockCheckingObserver struct {
	cache         *MemoryCache
	lockAvailable bool
}

func (o *lockCheckingObserver) OnEviction(EvictionEvent) {
	shard := &o.cache.shards[0]
	if shard.mu.TryLock() {
		o.lockAvailable = true
		shard.mu.Unlock()
	}
}

func TestObserverReportsLivePressureEviction(t *testing.T) {
	observer := &recordingObserver{}
	cache := newObserverTestCache(t, Config{
		MaxMemoryBytes:   100,
		MaxItemSizeBytes: 100,
		Observer:         observer,
	}, 1)

	mustObserverForever(t, cache, "a", make([]byte, 60))
	mustObserverForever(t, cache, "b", make([]byte, 60))

	if len(observer.events) != 1 {
		t.Fatalf("observer events = %d, want 1", len(observer.events))
	}
	if got, want := observer.events[0].Bytes, int64(61); got != want {
		t.Fatalf("evicted bytes = %d, want %d", got, want)
	}
}

func TestObserverDoesNotReportExpiredEntryRemoval(t *testing.T) {
	observer := &recordingObserver{}
	cache := newObserverTestCache(t, Config{
		MaxMemoryBytes:   100,
		MaxItemSizeBytes: 100,
		Observer:         observer,
	}, 1)
	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	stored, err := cache.Set("a", make([]byte, 60), time.Second)
	if err != nil || !stored {
		t.Fatalf("Set() = %v, %v; want true, nil", stored, err)
	}
	now = now.Add(2 * time.Second)
	mustObserverForever(t, cache, "b", make([]byte, 60))

	if len(observer.events) != 0 {
		t.Fatalf("observer events = %d, want 0", len(observer.events))
	}
}

func TestObserverIgnoresExplicitRemovalAndFlush(t *testing.T) {
	observer := &recordingObserver{}
	cache := newObserverTestCache(t, Config{Observer: observer}, 1)

	mustObserverForever(t, cache, "a", []byte("value"))
	if removed, err := cache.Forget("a"); err != nil || !removed {
		t.Fatalf("Forget() = %v, %v; want true, nil", removed, err)
	}
	mustObserverForever(t, cache, "b", []byte("value"))
	if flushed, err := cache.Flush(); err != nil || !flushed {
		t.Fatalf("Flush() = %v, %v; want true, nil", flushed, err)
	}

	if len(observer.events) != 0 {
		t.Fatalf("observer events = %d, want 0", len(observer.events))
	}
}

func TestObserverRunsAfterShardUnlock(t *testing.T) {
	observer := &lockCheckingObserver{}
	cache := newObserverTestCache(t, Config{
		MaxMemoryBytes:   100,
		MaxItemSizeBytes: 100,
		Observer:         observer,
	}, 1)
	observer.cache = cache

	mustObserverForever(t, cache, "a", make([]byte, 60))
	mustObserverForever(t, cache, "b", make([]byte, 60))

	if !observer.lockAvailable {
		t.Fatal("observer was called while the eviction shard lock was held")
	}
}

func newObserverTestCache(t *testing.T, config Config, shards int) *MemoryCache {
	t.Helper()
	cache, err := newMemoryCache(config, shards, 1)
	if err != nil {
		t.Fatalf("newMemoryCache() error = %v", err)
	}
	cache.closeOnce.Do(func() {
		close(cache.maintenanceStop)
		<-cache.maintenanceDone
	})
	t.Cleanup(func() { _ = cache.Close() })
	return cache
}

func mustObserverForever(t *testing.T, cache *MemoryCache, key string, value []byte) {
	t.Helper()
	stored, err := cache.Forever(key, value)
	if err != nil || !stored {
		t.Fatalf("Forever(%q) = %v, %v; want true, nil", key, stored, err)
	}
}
