package memory

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	cachecontract "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache"
)

func TestMemoryCacheGetMissReturnsNil(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})

	got, _, err := cache.Get("missing")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got != nil {
		t.Fatalf("Get() = %v, want nil", got)
	}
}

func TestMemoryCacheSetGetZeroCopy(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})
	value := []byte("hello")

	stored, err := cache.Set("key", value, time.Minute)
	if err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if !stored {
		t.Fatal("Set() = false, want true")
	}

	got, ttl, err := cache.Get("key")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("Get() = %q, want %q", got, value)
	}
	if ttl <= 0 || ttl > time.Minute {
		t.Fatalf("Get() TTL = %v, want between 0 and %v", ttl, time.Minute)
	}
	if len(value) > 0 && &got[0] != &value[0] {
		t.Fatal("Get() returned a copied payload; want zero-copy storage")
	}
}

func TestMemoryCacheAddIsAtomicAndDoesNotOverwrite(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})
	value := []byte("value")

	if stored, err := cache.Add("key", value, time.Minute); err != nil || !stored {
		t.Fatalf("Add(missing) = (%t, %v), want (true, nil)", stored, err)
	}
	if stored, err := cache.Add("key", []byte("new"), 2*time.Minute); err != nil || stored {
		t.Fatalf("Add(existing) = (%t, %v), want (false, nil)", stored, err)
	}
	if got, ttl, err := cache.Get("key"); err != nil || string(got) != "value" || ttl <= 0 || ttl > time.Minute {
		t.Fatalf("Get() after rejected Add() = (%q, %v, %v), want original value and TTL", got, ttl, err)
	}
}

func TestMemoryCacheAddTreatsExpiredKeyAsMissing(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})
	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	if stored, err := cache.Add("key", []byte("old"), time.Second); err != nil || !stored {
		t.Fatalf("first Add() = (%t, %v), want (true, nil)", stored, err)
	}
	now = now.Add(time.Second)
	if stored, err := cache.Add("key", []byte("new"), time.Minute); err != nil || !stored {
		t.Fatalf("Add(expired) = (%t, %v), want (true, nil)", stored, err)
	}
	if got, _, err := cache.Get("key"); err != nil || string(got) != "new" {
		t.Fatalf("Get() after Add(expired) = (%q, %v), want new value", got, err)
	}
}

func TestMemoryCacheConcurrentAddOneWinner(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})
	const workers = 32

	var wg sync.WaitGroup
	results := make(chan bool, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stored, err := cache.Add("key", []byte("value"), time.Minute)
			if err != nil {
				t.Errorf("Add() error = %v", err)
			}
			results <- stored
		}()
	}
	wg.Wait()
	close(results)

	winners := 0
	for stored := range results {
		if stored {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("successful Add() calls = %d, want 1", winners)
	}
}

func TestMemoryCacheSetManyValidatesBeforeMutating(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})
	values := map[string][]byte{
		"first":   []byte("value"),
		"invalid": nil,
	}

	stored, err := cache.SetMany(values, time.Minute)
	if stored || !errors.Is(err, cachecontract.ErrNilValue) {
		t.Fatalf("SetMany() = (%t, %v), want false and ErrNilValue", stored, err)
	}
	if value, _, getErr := cache.Get("first"); getErr != nil || value != nil {
		t.Fatalf("SetMany() partially stored values: (%q, %v)", value, getErr)
	}
}

func TestMemoryCacheSetManyStoresAllValues(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})
	values := map[string][]byte{
		"first":  []byte("one"),
		"second": []byte("two"),
		"third":  []byte("three"),
	}

	stored, err := cache.SetMany(values, time.Minute)
	if !stored || err != nil {
		t.Fatalf("SetMany() = (%t, %v), want (true, nil)", stored, err)
	}
	for key, want := range values {
		value, ttl, getErr := cache.Get(key)
		if getErr != nil || string(value) != string(want) || ttl <= 0 {
			t.Errorf("Get(%q) = (%q, %v, %v), want value with TTL", key, value, ttl, getErr)
		}
	}
}

func TestMemoryCacheSetManyEmptyBatch(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})

	stored, err := cache.SetMany(nil, time.Minute)
	if stored || err != nil {
		t.Fatalf("SetMany(empty) = (%t, %v), want (false, nil)", stored, err)
	}
}

func TestMemoryCacheGetManyReturnsHitsAndOmitsMisses(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})
	cache.now = func() time.Time { return time.Unix(100, 0) }
	if _, err := cache.Set("temporary", []byte("value"), time.Minute); err != nil {
		t.Fatalf("Set(temporary) error = %v", err)
	}
	if _, err := cache.Forever("forever", []byte("permanent")); err != nil {
		t.Fatalf("Forever(forever) error = %v", err)
	}

	result, err := cache.GetMany([]string{"temporary", "missing", "forever"})
	if err != nil {
		t.Fatalf("GetMany() error = %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("GetMany() returned %d items, want 2", len(result))
	}
	if item, ok := result["temporary"]; !ok || string(item.Value) != "value" || item.TTL <= 0 {
		t.Fatalf("temporary item = (%q, %v, %t), want value with TTL", item.Value, item.TTL, ok)
	}
	if item, ok := result["forever"]; !ok || string(item.Value) != "permanent" || item.TTL != 0 {
		t.Fatalf("forever item = (%q, %v, %t), want permanent with TTL 0", item.Value, item.TTL, ok)
	}
	if _, ok := result["missing"]; ok {
		t.Fatal("GetMany() included missing key")
	}
}

func TestMemoryCacheGetManyEmptyInputReturnsEmptyResult(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})

	result, err := cache.GetMany(nil)
	if err != nil || result == nil || len(result) != 0 {
		t.Fatalf("GetMany(empty) = (%v, %v), want non-nil empty result", result, err)
	}
}

func TestMemoryCacheAllowsEmptyNonNilValue(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})
	value := make([]byte, 0)

	stored, err := cache.Set("key", value, time.Minute)
	if err != nil || !stored {
		t.Fatalf("Set() = %v, %v; want true, nil", stored, err)
	}

	got, _, err := cache.Get("key")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got == nil {
		t.Fatal("Get() returned nil for stored empty payload")
	}
	if len(got) != 0 {
		t.Fatalf("len(Get()) = %d, want 0", len(got))
	}
}

func TestMemoryCacheExpiration(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})
	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	mustSet(t, cache, "key", []byte("value"), time.Minute)
	now = now.Add(time.Minute)

	got, _, err := cache.Get("key")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got != nil {
		t.Fatalf("Get() = %q, want nil after expiration", got)
	}
	if cache.current.Load() != 0 {
		t.Fatalf("current bytes = %d, want 0", cache.current.Load())
	}
}

func TestMemoryCacheForever(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})
	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	mustForever(t, cache, "key", []byte("value"))
	now = now.Add(100 * 365 * 24 * time.Hour)

	got, ttl, err := cache.Get("key")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(got) != "value" {
		t.Fatalf("Get() = %q, want value", got)
	}
	if ttl != 0 {
		t.Fatalf("Get() TTL = %v, want 0 for Forever value", ttl)
	}
}

func TestMemoryCacheForgetReturnsStatus(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})

	removed, err := cache.Forget("missing")
	if err != nil || removed {
		t.Fatalf("Forget(missing) = %v, %v; want false, nil", removed, err)
	}

	mustForever(t, cache, "key", []byte("value"))
	removed, err = cache.Forget("key")
	if err != nil || !removed {
		t.Fatalf("Forget(existing) = %v, %v; want true, nil", removed, err)
	}

	removed, err = cache.Forget("key")
	if err != nil || removed {
		t.Fatalf("Forget(removed) = %v, %v; want false, nil", removed, err)
	}
}

func TestMemoryCacheForgetExpiredReturnsFalse(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})
	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	mustSet(t, cache, "key", []byte("value"), time.Second)
	now = now.Add(2 * time.Second)

	removed, err := cache.Forget("key")
	if err != nil || removed {
		t.Fatalf("Forget(expired) = %v, %v; want false, nil", removed, err)
	}
	if cache.current.Load() != 0 {
		t.Fatalf("current bytes = %d, want 0", cache.current.Load())
	}
}

func TestMemoryCacheTouchReturnsStatus(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})
	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	touched, err := cache.Touch("missing", time.Minute)
	if err != nil || touched {
		t.Fatalf("Touch(missing) = %v, %v; want false, nil", touched, err)
	}

	mustSet(t, cache, "key", []byte("value"), time.Minute)
	now = now.Add(30 * time.Second)

	touched, err = cache.Touch("key", 2*time.Minute)
	if err != nil || !touched {
		t.Fatalf("Touch(existing) = %v, %v; want true, nil", touched, err)
	}

	now = now.Add(90 * time.Second)
	got, _, err := cache.Get("key")
	if err != nil || got == nil {
		t.Fatalf("Get() after Touch() = %q, %v; want hit", got, err)
	}

	now = now.Add(31 * time.Second)
	got, _, err = cache.Get("key")
	if err != nil || got != nil {
		t.Fatalf("Get() after touched TTL = %q, %v; want miss", got, err)
	}
}

func TestMemoryCacheTouchExpiredReturnsFalse(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})
	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	mustSet(t, cache, "key", []byte("value"), time.Second)
	now = now.Add(2 * time.Second)

	touched, err := cache.Touch("key", time.Minute)
	if err != nil || touched {
		t.Fatalf("Touch(expired) = %v, %v; want false, nil", touched, err)
	}
}

func TestMemoryCacheFlush(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})
	mustForever(t, cache, "a", []byte("1"))
	mustForever(t, cache, "b", []byte("2"))

	flushed, err := cache.Flush()
	if err != nil || !flushed {
		t.Fatalf("Flush() = %v, %v; want true, nil", flushed, err)
	}
	if cache.current.Load() != 0 {
		t.Fatalf("current bytes = %d, want 0", cache.current.Load())
	}

	for _, key := range []string{"a", "b"} {
		got, _, err := cache.Get(key)
		if err != nil || got != nil {
			t.Fatalf("Get(%q) = %q, %v; want nil, nil", key, got, err)
		}
	}

	flushed, err = cache.Flush()
	if err != nil || !flushed {
		t.Fatalf("Flush(empty) = %v, %v; want true, nil", flushed, err)
	}
}

func TestMemoryCacheRejectsInvalidTTL(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})

	if stored, err := cache.Set("key", []byte("value"), 0); stored || !errors.Is(err, cachecontract.ErrInvalidTTL) {
		t.Fatalf("Set() = %v, %v; want false, ErrInvalidTTL", stored, err)
	}
	if touched, err := cache.Touch("key", -time.Second); touched || !errors.Is(err, cachecontract.ErrInvalidTTL) {
		t.Fatalf("Touch() = %v, %v; want false, ErrInvalidTTL", touched, err)
	}
	if stored, err := cache.Add("key", []byte("value"), 0); stored || !errors.Is(err, cachecontract.ErrInvalidTTL) {
		t.Fatalf("Add() = %v, %v; want false, ErrInvalidTTL", stored, err)
	}
}

func TestMemoryCacheRejectsNilValue(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})

	if stored, err := cache.Set("key", nil, time.Minute); stored || !errors.Is(err, cachecontract.ErrNilValue) {
		t.Fatalf("Set(nil) = %v, %v; want false, ErrNilValue", stored, err)
	}
	if stored, err := cache.Forever("key", nil); stored || !errors.Is(err, cachecontract.ErrNilValue) {
		t.Fatalf("Forever(nil) = %v, %v; want false, ErrNilValue", stored, err)
	}
	if stored, err := cache.Add("key", nil, time.Minute); stored || !errors.Is(err, cachecontract.ErrNilValue) {
		t.Fatalf("Add(nil) = %v, %v; want false, ErrNilValue", stored, err)
	}
}

func TestMemoryCacheRejectsOversizedItem(t *testing.T) {
	cache := newTestMemoryCache(t, Config{
		MaxMemoryBytes:   1024,
		MaxItemSizeBytes: 256,
	})

	stored, err := cache.Forever("key", make([]byte, 254))
	if stored || !errors.Is(err, cachecontract.ErrItemTooLarge) {
		t.Fatalf("Forever(oversized) = %v, %v; want false, ErrItemTooLarge", stored, err)
	}
	stored, err = cache.Add("key", make([]byte, 254), time.Minute)
	if stored || !errors.Is(err, cachecontract.ErrItemTooLarge) {
		t.Fatalf("Add(oversized) = %v, %v; want false, ErrItemTooLarge", stored, err)
	}
}

func TestMemoryCacheAllowsItemAtExactSizeLimit(t *testing.T) {
	cache := newTestMemoryCache(t, Config{
		MaxMemoryBytes:   1024,
		MaxItemSizeBytes: 256,
	})

	stored, err := cache.Forever("key", make([]byte, 253))
	if err != nil || !stored {
		t.Fatalf("Forever(exact limit) = %v, %v; want true, nil", stored, err)
	}
}

func TestMemoryCacheAccountsRetainedCapacity(t *testing.T) {
	cache := newTestMemoryCache(t, Config{
		MaxMemoryBytes:   2048,
		MaxItemSizeBytes: 2048,
	})

	backing := make([]byte, 1024)
	value := backing[:1]
	mustForever(t, cache, "key", value)

	want := int64(len("key") + cap(value))
	if got := cache.current.Load(); got != want {
		t.Fatalf("current bytes = %d, want %d", got, want)
	}
}

func TestMemoryCacheEvictsToStayWithinBudget(t *testing.T) {
	cache := newTestMemoryCache(t, Config{
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
		mustForever(t, cache, key, make([]byte, 200))
		if got := cache.current.Load(); got > cache.maxMemory {
			t.Fatalf("current bytes = %d, exceeds max %d", got, cache.maxMemory)
		}
	}

	got, _, err := cache.Get("key-7")
	if err != nil || got == nil {
		t.Fatalf("Get(key-7) = %q, %v; want hit", got, err)
	}
}

func TestMemoryCachePurgesExpiredBeforeLiveEntries(t *testing.T) {
	cache := newTestMemoryCache(t, Config{
		MaxMemoryBytes:   700,
		MaxItemSizeBytes: 300,
	})

	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	mustSet(t, cache, "expired", make([]byte, 200), time.Second)
	mustForever(t, cache, "live", make([]byte, 200))

	now = now.Add(2 * time.Second)
	mustForever(t, cache, "new", make([]byte, 200))

	if got, _, _ := cache.Get("expired"); got != nil {
		t.Fatal("expired key survived pressure cleanup")
	}
	if got, _, _ := cache.Get("live"); got == nil {
		t.Fatal("live key was evicted while expired space was available")
	}
	if got, _, _ := cache.Get("new"); got == nil {
		t.Fatal("new key missing")
	}
}

func TestMemoryCacheReplacementAccounting(t *testing.T) {
	cache := newTestMemoryCache(t, Config{
		MaxMemoryBytes:   2048,
		MaxItemSizeBytes: 1024,
	})

	mustForever(t, cache, "key", make([]byte, 100))
	small := cache.current.Load()

	mustForever(t, cache, "key", make([]byte, 500))
	large := cache.current.Load()
	if large <= small {
		t.Fatalf("current bytes did not grow: small=%d large=%d", small, large)
	}

	mustForever(t, cache, "key", make([]byte, 50))
	shrunk := cache.current.Load()
	if shrunk >= large {
		t.Fatalf("current bytes did not shrink: large=%d shrunk=%d", large, shrunk)
	}
}

func TestMemoryCacheConcurrentAccess(t *testing.T) {
	cache := newTestMemoryCache(t, Config{
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

				if stored, err := cache.Set(key, value, time.Minute); err != nil || !stored {
					t.Errorf("Set() = %v, %v", stored, err)
					return
				}
				if _, _, err := cache.Get(key); err != nil {
					t.Errorf("Get() error = %v", err)
					return
				}
				if i%10 == 0 {
					if _, err := cache.Touch(key, time.Minute); err != nil {
						t.Errorf("Touch() error = %v", err)
						return
					}
				}
				if i%17 == 0 {
					if _, err := cache.Forget(key); err != nil {
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

func TestConfigDefaults(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})

	if cache.maxMemory != DefaultMaxMemoryBytes {
		t.Fatalf("max memory = %d, want %d", cache.maxMemory, DefaultMaxMemoryBytes)
	}
	if cache.maxItemSize != DefaultMaxItemSizeBytes {
		t.Fatalf("max item size = %d, want %d", cache.maxItemSize, DefaultMaxItemSizeBytes)
	}
}

func mustSet(t *testing.T, cache *MemoryCache, key string, value []byte, ttl time.Duration) {
	t.Helper()
	stored, err := cache.Set(key, value, ttl)
	if err != nil || !stored {
		t.Fatalf("Set() = %v, %v; want true, nil", stored, err)
	}
}

func mustForever(t *testing.T, cache *MemoryCache, key string, value []byte) {
	t.Helper()
	stored, err := cache.Forever(key, value)
	if err != nil || !stored {
		t.Fatalf("Forever() = %v, %v; want true, nil", stored, err)
	}
}

func newTestMemoryCache(t *testing.T, config Config) *MemoryCache {
	t.Helper()

	cache, err := newMemoryCache(config, 8, 5)
	if err != nil {
		t.Fatalf("newMemoryCache() error = %v", err)
	}
	stopMemoryCacheMaintenanceForTest(cache)
	t.Cleanup(func() {
		if err := cache.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	return cache
}
