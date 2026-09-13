package memory

import (
	"testing"
	"time"
)

type recordingObserver struct {
	summaries []EvictionSummary
}

func (o *recordingObserver) OnEviction(summary EvictionSummary) {
	o.summaries = append(o.summaries, summary)
}

type lockCheckingObserver struct {
	cache               *MemoryCache
	shardLockAvailable  bool
	evictionMuAvailable bool
}

func (o *lockCheckingObserver) OnEviction(EvictionSummary) {
	shard := &o.cache.shards[0]
	if shard.mu.TryLock() {
		o.shardLockAvailable = true
		shard.mu.Unlock()
	}
	if o.cache.evictionMu.TryLock() {
		o.evictionMuAvailable = true
		o.cache.evictionMu.Unlock()
	}
}

type reentrantFlushObserver struct {
	cache *MemoryCache
}

func (o *reentrantFlushObserver) OnEviction(EvictionSummary) {
	_, _ = o.cache.Flush()
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

	if len(observer.summaries) != 1 {
		t.Fatalf("observer summaries = %d, want 1", len(observer.summaries))
	}
	if got, want := observer.summaries[0].Entries, uint64(1); got != want {
		t.Fatalf("evicted entries = %d, want %d", got, want)
	}
	if got, want := observer.summaries[0].Bytes, int64(61); got != want {
		t.Fatalf("evicted bytes = %d, want %d", got, want)
	}
}

func TestObserverAggregatesLivePressureEvictions(t *testing.T) {
	observer := &recordingObserver{}
	cache := newObserverTestCache(t, Config{
		MaxMemoryBytes:   100,
		MaxItemSizeBytes: 100,
		Observer:         observer,
	}, 1)

	mustObserverForever(t, cache, "a", make([]byte, 30))
	mustObserverForever(t, cache, "b", make([]byte, 30))
	mustObserverForever(t, cache, "c", make([]byte, 30))
	mustObserverForever(t, cache, "d", make([]byte, 89))

	if len(observer.summaries) != 1 {
		t.Fatalf("observer summaries = %d, want 1", len(observer.summaries))
	}
	if got, want := observer.summaries[0].Entries, uint64(3); got != want {
		t.Fatalf("evicted entries = %d, want %d", got, want)
	}
	if got, want := observer.summaries[0].Bytes, int64(93); got != want {
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

	if len(observer.summaries) != 0 {
		t.Fatalf("observer summaries = %d, want 0", len(observer.summaries))
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

	if len(observer.summaries) != 0 {
		t.Fatalf("observer summaries = %d, want 0", len(observer.summaries))
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

	if !observer.shardLockAvailable {
		t.Fatal("observer was called while the eviction shard lock was held")
	}
	if !observer.evictionMuAvailable {
		t.Fatal("observer was called while evictionMu was held")
	}
}

func TestObserverCanReenterFlushWithoutDeadlock(t *testing.T) {
	observer := &reentrantFlushObserver{}
	cache := newObserverTestCache(t, Config{
		MaxMemoryBytes:   100,
		MaxItemSizeBytes: 100,
		Observer:         observer,
	}, 1)
	observer.cache = cache

	mustObserverForever(t, cache, "a", make([]byte, 60))

	done := make(chan error, 1)
	go func() {
		_, err := cache.Forever("b", make([]byte, 60))
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Forever() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("observer reentrant Flush() deadlocked")
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
