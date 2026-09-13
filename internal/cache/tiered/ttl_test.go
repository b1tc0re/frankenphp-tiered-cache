package tiered

import (
	"testing"
	"time"
)

func TestTieredCacheAsyncWriteUsesRemainingTTL(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.setStarted = make(chan struct{}, 1)
	l2.releaseSet = make(chan struct{})
	cache := newTestTieredCache(t, Config{WriteQueueCapacity: 2}, l1, l2)

	now := time.Unix(1_700_000_000, 0)
	cache.now = func() time.Time { return now }

	if ok, err := cache.Set("blocker", []byte("blocker"), time.Minute); err != nil || !ok {
		t.Fatalf("blocker Set() = (%t, %v), want (true, nil)", ok, err)
	}
	select {
	case <-l2.setStarted:
	case <-time.After(time.Second):
		t.Fatal("L2 worker did not start blocker write")
	}

	if ok, err := cache.Set("key", []byte("value"), time.Minute); err != nil || !ok {
		t.Fatalf("Set() = (%t, %v), want (true, nil)", ok, err)
	}

	now = now.Add(10 * time.Second)
	close(l2.releaseSet)

	if err := cache.pending.wait("key"); err != nil {
		t.Fatalf("pending write error = %v", err)
	}

	value, ttl, err := l2.Get("key")
	if err != nil {
		t.Fatalf("L2 Get() error = %v", err)
	}
	if string(value) != "value" {
		t.Fatalf("L2 value = %q, want value", value)
	}
	if ttl != 50*time.Second {
		t.Fatalf("L2 TTL = %v, want %v", ttl, 50*time.Second)
	}
}

func TestTieredCacheExpiredQueuedWriteRemovesStaleL2Value(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.values["key"] = []byte("stale")
	l2.ttls["key"] = time.Minute
	l2.setStarted = make(chan struct{}, 1)
	l2.releaseSet = make(chan struct{})
	cache := newTestTieredCache(t, Config{WriteQueueCapacity: 2}, l1, l2)

	now := time.Unix(1_700_000_000, 0)
	cache.now = func() time.Time { return now }

	if ok, err := cache.Set("blocker", []byte("blocker"), time.Minute); err != nil || !ok {
		t.Fatalf("blocker Set() = (%t, %v), want (true, nil)", ok, err)
	}
	select {
	case <-l2.setStarted:
	case <-time.After(time.Second):
		t.Fatal("L2 worker did not start blocker write")
	}

	if ok, err := cache.Set("key", []byte("fresh"), 5*time.Second); err != nil || !ok {
		t.Fatalf("Set() = (%t, %v), want (true, nil)", ok, err)
	}

	now = now.Add(10 * time.Second)
	close(l2.releaseSet)

	if err := cache.pending.wait("key"); err != nil {
		t.Fatalf("pending write error = %v", err)
	}

	value, ttl, err := l2.Get("key")
	if err != nil {
		t.Fatalf("L2 Get() error = %v", err)
	}
	if value != nil || ttl != 0 {
		t.Fatalf("L2 Get() after expired queued write = (%q, %v), want (nil, 0)", value, ttl)
	}
}
