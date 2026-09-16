package tiered

import (
	"testing"
	"time"
)

func TestTieredCacheSetPassesRemainingTTLToL1(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.setDelay = 50 * time.Millisecond
	cache := newTestTieredCache(t, Config{}, l1, l2)

	const ttl = time.Second
	if ok, err := cache.Set("key", []byte("value"), ttl); err != nil || !ok {
		t.Fatalf("Set() = (%t, %v), want (true, nil)", ok, err)
	}

	if value, l2TTL, err := l2.Get("key"); err != nil || string(value) != "value" || l2TTL != ttl {
		t.Fatalf("L2 = (%q, %v, %v), want (value, %v, nil)", value, l2TTL, err, ttl)
	}
	value, l1TTL, err := l1.Get("key")
	if err != nil || string(value) != "value" {
		t.Fatalf("L1 = (%q, %v, %v), want (value, positive TTL, nil)", value, l1TTL, err)
	}
	if l1TTL <= 0 || l1TTL >= ttl-25*time.Millisecond {
		t.Fatalf("L1 TTL = %v, want a value reduced by the L2 delay from %v", l1TTL, ttl)
	}
}

func TestTieredCacheTouchPassesRemainingTTLToL1(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	cache := newTestTieredCache(t, Config{}, l1, l2)

	if ok, err := cache.Forever("key", []byte("value")); err != nil || !ok {
		t.Fatalf("Forever() = (%t, %v), want (true, nil)", ok, err)
	}
	l2.touchDelay = 50 * time.Millisecond
	const ttl = time.Second
	if ok, err := cache.Touch("key", ttl); err != nil || !ok {
		t.Fatalf("Touch() = (%t, %v), want (true, nil)", ok, err)
	}

	if _, l2TTL, err := l2.Get("key"); err != nil || l2TTL != ttl {
		t.Fatalf("L2 TTL = %v, %v; want %v, nil", l2TTL, err, ttl)
	}
	_, l1TTL, err := l1.Get("key")
	if err != nil {
		t.Fatalf("L1 Get() error = %v", err)
	}
	if l1TTL <= 0 || l1TTL >= ttl-25*time.Millisecond {
		t.Fatalf("L1 TTL = %v, want a value reduced by the L2 delay from %v", l1TTL, ttl)
	}
}

func TestTieredCacheGetPassesRemainingTTLToL1(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.put("key", []byte("value"), time.Second)
	l2.getDelay = 50 * time.Millisecond
	cache := newTestTieredCache(t, Config{}, l1, l2)

	value, ttl, err := cache.Get("key")
	if err != nil || string(value) != "value" {
		t.Fatalf("Get() = (%q, %v, %v), want (value, positive TTL, nil)", value, ttl, err)
	}
	if ttl <= 0 || ttl >= time.Second-25*time.Millisecond {
		t.Fatalf("Get() TTL = %v, want a value reduced by the L2 delay", ttl)
	}
	_, l1TTL, err := l1.Get("key")
	if err != nil {
		t.Fatalf("L1 Get() error = %v", err)
	}
	if l1TTL != ttl {
		t.Fatalf("L1 TTL = %v, Get() TTL = %v; want equal remaining TTLs", l1TTL, ttl)
	}
}

func TestTieredCacheSetRemovesExpiredL1WhenL2IsTooSlow(t *testing.T) {
	l1 := newFakeCache()
	l1.put("key", []byte("old"), time.Minute)
	l2 := newFakeCache()
	l2.setDelay = 50 * time.Millisecond
	cache := newTestTieredCache(t, Config{}, l1, l2)

	if ok, err := cache.Set("key", []byte("new"), 10*time.Millisecond); err != nil || !ok {
		t.Fatalf("Set() = (%t, %v), want (true, nil)", ok, err)
	}
	if value, _, err := l1.Get("key"); err != nil || value != nil {
		t.Fatalf("L1 after expired Set() = (%q, %v), want (nil, nil)", value, err)
	}
}

func TestTieredCacheTouchRemovesExpiredL1WhenL2IsTooSlow(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	cache := newTestTieredCache(t, Config{}, l1, l2)

	if ok, err := cache.Forever("key", []byte("value")); err != nil || !ok {
		t.Fatalf("Forever() = (%t, %v), want (true, nil)", ok, err)
	}
	l2.touchDelay = 50 * time.Millisecond
	if ok, err := cache.Touch("key", 10*time.Millisecond); err != nil || !ok {
		t.Fatalf("Touch() = (%t, %v), want (true, nil)", ok, err)
	}
	if value, _, err := l1.Get("key"); err != nil || value != nil {
		t.Fatalf("L1 after expired Touch() = (%q, %v), want (nil, nil)", value, err)
	}
}
