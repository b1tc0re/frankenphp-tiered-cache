package cache

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMemoryCacheSetGetZeroCopy(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{})
	value := []byte("hello")

	if err := cache.Set("key", value, time.Minute); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	got, ok, err := cache.Get("key")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatal("Get() missed stored key")
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("Get() = %q, want %q", got, value)
	}
	if len(value) > 0 && &got[0] != &value[0] {
		t.Fatal("Get() returned a copied payload; want zero-copy storage")
	}
}

func TestMemoryCacheExpiration(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{})
	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	if err := cache.Set("key", []byte("value"), time.Minute); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	now = now.Add(time.Minute)

	if _, ok, err := cache.Get("key"); err != nil {
		t.Fatalf("Get() error = %v", err)
	} else if ok {
		t.Fatal("Get() hit an expired key")
	}
	if cache.current.Load() != 0 {
		t.Fatalf("current bytes = %d, want 0 after expiration cleanup", cache.current.Load())
	}
}

func TestMemoryCacheForever(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{})
	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	if err := cache.Forever("key", []byte("value")); err != nil {
		t.Fatalf("Forever() error = %v", err)
	}

	now = now.Add(100 * 365 * 24 * time.Hour)

	got, ok, err := cache.Get("key")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok || string(got) != "value" {
		t.Fatalf("Get() = %q, %v; want value, true", got, ok)
	}
}

func TestMemoryCacheForget(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{})

	if err := cache.Forever("key", []byte("value")); err != nil {
		t.Fatalf("Forever() error = %v", err)
	}
	if err := cache.Forget("key"); err != nil {
		t.Fatalf("Forget() error = %v", err)
	}

	if _, ok, err := cache.Get("key"); err != nil {
		t.Fatalf("Get() error = %v", err)
	} else if ok {
		t.Fatal("Get() hit forgotten key")
	}
	if cache.current.Load() != 0 {
		t.Fatalf("current bytes = %d, want 0", cache.current.Load())
	}
}

func TestMemoryCacheTouch(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{})
	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	if err := cache.Set("key", []byte("value"), time.Minute); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	now = now.Add(30 * time.Second)
	if err := cache.Touch("key", 2*time.Minute); err != nil {
		t.Fatalf("Touch() error = %v", err)
	}

	now = now.Add(90 * time.Second)
	if _, ok, err := cache.Get("key"); err != nil {
		t.Fatalf("Get() error = %v", err)
	} else if !ok {
		t.Fatal("Get() missed key after Touch() extended its TTL")
	}

	now = now.Add(31 * time.Second)
	if _, ok, err := cache.Get("key"); err != nil {
		t.Fatalf("Get() error = %v", err)
	} else if ok {
		t.Fatal("Get() hit key after touched TTL expired")
	}
}

func TestMemoryCacheRejectsInvalidTTL(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{})

	if err := cache.Set("key", []byte("value"), 0); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("Set() error = %v, want ErrInvalidTTL", err)
	}
	if err := cache.Touch("key", -time.Second); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("Touch() error = %v, want ErrInvalidTTL", err)
	}
}

func TestMemoryCacheRejectsOversizedItem(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{
		MaxMemoryBytes:   1024,
		MaxItemSizeBytes: 256,
	})

	value := make([]byte, 257)
	if err := cache.Forever("key", value); !errors.Is(err, ErrItemTooLarge) {
		t.Fatalf("Forever() error = %v, want ErrItemTooLarge", err)
	}
}

func TestMemoryCacheAccountsRetainedCapacity(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{
		MaxMemoryBytes:   2048,
		MaxItemSizeBytes: 1024,
	})

	backing := make([]byte, 1024)
	value := backing[:1]

	if err := cache.Forever("key", value); err != nil {
		t.Fatalf("Forever() error = %v", err)
	}

	want := int64(len("key")+cap(value)) + entryOverheadBytes
	if got := cache.current.Load(); got != want {
		t.Fatalf("current bytes = %d, want %d", got, want)
	}
}

func TestMemoryCacheEvictsToStayWithinBudget(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{
		MaxMemoryBytes:   1024,
		MaxItemSizeBytes: 512,
	})

	now := time.Unix(100, 0)
	cache.now = func() time.Time {
		now = now.Add(time.Nanosecond)
		return now
	}

	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("key-%d", i)
		if err := cache.Forever(key, make([]byte, 200)); err != nil {
			t.Fatalf("Forever(%q) error = %v", key, err)
		}
		if got := cache.current.Load(); got > cache.maxMemory {
			t.Fatalf("current bytes = %d, exceeds max %d", got, cache.maxMemory)
		}
	}

	if _, ok, err := cache.Get("key-7"); err != nil {
		t.Fatalf("Get() error = %v", err)
	} else if !ok {
		t.Fatal("newly inserted key was unexpectedly evicted")
	}
}

func TestMemoryCachePurgesExpiredBeforeLiveEntries(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{
		MaxMemoryBytes:   700,
		MaxItemSizeBytes: 300,
	})

	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	if err := cache.Set("expired", make([]byte, 200), time.Second); err != nil {
		t.Fatalf("Set(expired) error = %v", err)
	}
	if err := cache.Forever("live", make([]byte, 200)); err != nil {
		t.Fatalf("Forever(live) error = %v", err)
	}

	now = now.Add(2 * time.Second)
	if err := cache.Forever("new", make([]byte, 200)); err != nil {
		t.Fatalf("Forever(new) error = %v", err)
	}

	if _, ok, _ := cache.Get("expired"); ok {
		t.Fatal("expired key survived pressure cleanup")
	}
	if _, ok, _ := cache.Get("live"); !ok {
		t.Fatal("live key was evicted while expired space was available")
	}
	if _, ok, _ := cache.Get("new"); !ok {
		t.Fatal("new key missing")
	}
}

func TestMemoryCacheReplacementAccounting(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{
		MaxMemoryBytes:   2048,
		MaxItemSizeBytes: 1024,
	})

	if err := cache.Forever("key", make([]byte, 100)); err != nil {
		t.Fatalf("Forever() error = %v", err)
	}
	small := cache.current.Load()

	if err := cache.Forever("key", make([]byte, 500)); err != nil {
		t.Fatalf("Forever() grow error = %v", err)
	}
	large := cache.current.Load()
	if large <= small {
		t.Fatalf("current bytes did not grow: small=%d large=%d", small, large)
	}

	if err := cache.Forever("key", make([]byte, 50)); err != nil {
		t.Fatalf("Forever() shrink error = %v", err)
	}
	shrunk := cache.current.Load()
	if shrunk >= large {
		t.Fatalf("current bytes did not shrink: large=%d shrunk=%d", large, shrunk)
	}
}

func TestMemoryCacheConcurrentAccess(t *testing.T) {
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
				key := fmt.Sprintf("worker-%d-%d", worker, i%32)
				value := []byte(fmt.Sprintf("value-%d-%d", worker, i))
				if err := cache.Set(key, value, time.Minute); err != nil {
					t.Errorf("Set() error = %v", err)
					return
				}
				if _, _, err := cache.Get(key); err != nil {
					t.Errorf("Get() error = %v", err)
					return
				}
				if i%10 == 0 {
					if err := cache.Touch(key, time.Minute); err != nil {
						t.Errorf("Touch() error = %v", err)
						return
					}
				}
				if i%17 == 0 {
					if err := cache.Forget(key); err != nil {
						t.Errorf("Forget() error = %v", err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()

	if got := cache.current.Load(); got < 0 || got > cache.maxMemory {
		t.Fatalf("current bytes = %d, want 0..%d", got, cache.maxMemory)
	}
}

func TestMemoryConfigDefaults(t *testing.T) {
	cache := newTestMemoryCache(t, MemoryConfig{})

	if cache.maxMemory != DefaultMaxMemoryBytes {
		t.Fatalf("max memory = %d, want %d", cache.maxMemory, DefaultMaxMemoryBytes)
	}
	if cache.maxItemSize != DefaultMaxItemSizeBytes {
		t.Fatalf("max item size = %d, want %d", cache.maxItemSize, DefaultMaxItemSizeBytes)
	}
}

func newTestMemoryCache(t *testing.T, config MemoryConfig) *MemoryCache {
	t.Helper()

	cache, err := newMemoryCache(config, 8, 5)
	if err != nil {
		t.Fatalf("NewMemoryCache() error = %v", err)
	}

	return cache
}
