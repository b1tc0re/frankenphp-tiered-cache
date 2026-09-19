package memory

import (
	"errors"
	"testing"
	"time"

	cachecontract "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache"
)

func TestMemoryCacheCountersPreserveTTL(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})
	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	mustSet(t, cache, "counter", []byte("10"), time.Minute)
	now = now.Add(30 * time.Second)

	if got, err := cache.Increment("counter", 2); err != nil || got != 12 {
		t.Fatalf("Increment() = (%d, %v), want (12, nil)", got, err)
	}
	if got, ttl, err := cache.Get("counter"); err != nil || string(got) != "12" || ttl != 30*time.Second {
		t.Fatalf("Get() = (%q, %v, %v), want (12, 30s, nil)", got, ttl, err)
	}

	if got, err := cache.Decrement("counter", 5); err != nil || got != 7 {
		t.Fatalf("Decrement() = (%d, %v), want (7, nil)", got, err)
	}
}

func TestMemoryCacheMissingCounterIsPersistent(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})

	if got, err := cache.Increment("counter", 2); err != nil || got != 2 {
		t.Fatalf("Increment(missing) = (%d, %v), want (2, nil)", got, err)
	}
	if got, ttl, err := cache.Get("counter"); err != nil || string(got) != "2" || ttl != 0 {
		t.Fatalf("Get() = (%q, %v, %v), want (2, 0, nil)", got, ttl, err)
	}
}

func TestMemoryCacheCounterRejectsInvalidValueAndOverflow(t *testing.T) {
	cache := newTestMemoryCache(t, Config{})
	mustForever(t, cache, "invalid", []byte("abc"))

	if _, err := cache.Increment("invalid", 1); err == nil {
		t.Fatal("Increment(invalid) error = nil, want an error")
	}

	mustForever(t, cache, "overflow", []byte("9223372036854775807"))
	if _, err := cache.Increment("overflow", 1); !errors.Is(err, cachecontract.ErrCounterOverflow) {
		t.Fatalf("Increment(overflow) error = %v, want ErrCounterOverflow", err)
	}
}
