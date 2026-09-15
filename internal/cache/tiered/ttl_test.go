package tiered

import (
	"testing"
	"time"
)

func TestTieredCacheSetKeepsTTLInBothLevels(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	cache := newTestTieredCache(t, Config{}, l1, l2)

	if ok, err := cache.Set("key", []byte("value"), 30*time.Second); err != nil || !ok {
		t.Fatalf("Set() = (%t, %v), want (true, nil)", ok, err)
	}
	for name, backend := range map[string]*fakeCache{"L1": l1, "L2": l2} {
		value, ttl, err := backend.Get("key")
		if err != nil || string(value) != "value" || ttl != 30*time.Second {
			t.Errorf("%s = (%q, %v, %v), want (value, 30s, nil)", name, value, ttl, err)
		}
	}
}

func TestTieredCacheTouchChangesTTLInBothLevels(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	cache := newTestTieredCache(t, Config{}, l1, l2)

	if ok, err := cache.Forever("key", []byte("value")); err != nil || !ok {
		t.Fatalf("Forever() = (%t, %v), want (true, nil)", ok, err)
	}
	if ok, err := cache.Touch("key", 45*time.Second); err != nil || !ok {
		t.Fatalf("Touch() = (%t, %v), want (true, nil)", ok, err)
	}
	for name, backend := range map[string]*fakeCache{"L1": l1, "L2": l2} {
		_, ttl, err := backend.Get("key")
		if err != nil || ttl != 45*time.Second {
			t.Errorf("%s TTL = %v, %v; want %v, nil", name, ttl, err, 45*time.Second)
		}
	}
}
