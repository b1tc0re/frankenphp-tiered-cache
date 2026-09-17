package tiered

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/b1tc0re/frankenphp-tiered-cache/internal/cache/memory"
	redisbackend "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache/redis"
)

func TestTieredCacheRedisPubSubIntegration(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("set REDIS_ADDR to run the Redis integration test")
	}

	prefix := fmt.Sprintf("frankenphp-tiered-integration:%d:", time.Now().UnixNano())
	redisConfig := redisbackend.Config{Addr: addr, KeyPrefix: prefix}

	l1A, err := memory.New(memory.Config{})
	if err != nil {
		t.Fatalf("MemoryCache A: %v", err)
	}
	l1B, err := memory.New(memory.Config{})
	if err != nil {
		_ = l1A.Close()
		t.Fatalf("MemoryCache B: %v", err)
	}
	l2A, err := redisbackend.New(redisConfig)
	if err != nil {
		_ = l1A.Close()
		_ = l1B.Close()
		t.Fatalf("RedisCache A: %v", err)
	}
	l2B, err := redisbackend.New(redisConfig)
	if err != nil {
		_ = l2A.Close()
		_ = l1A.Close()
		_ = l1B.Close()
		t.Fatalf("RedisCache B: %v", err)
	}
	busA, err := redisbackend.NewInvalidationBus(redisConfig)
	if err != nil {
		_ = l2A.Close()
		_ = l2B.Close()
		_ = l1A.Close()
		_ = l1B.Close()
		t.Fatalf("invalidation bus A: %v", err)
	}
	busB, err := redisbackend.NewInvalidationBus(redisConfig)
	if err != nil {
		_ = busA.Close()
		_ = l2A.Close()
		_ = l2B.Close()
		_ = l1A.Close()
		_ = l1B.Close()
		t.Fatalf("invalidation bus B: %v", err)
	}

	config := Config{RecoveryInterval: 25 * time.Millisecond}
	cacheA, err := NewWithInvalidation(config, l1A, l2A, busA)
	if err != nil {
		_ = busB.Close()
		_ = busA.Close()
		_ = l2A.Close()
		_ = l2B.Close()
		_ = l1A.Close()
		_ = l1B.Close()
		t.Fatalf("TieredCache A: %v", err)
	}
	cacheB, err := NewWithInvalidation(config, l1B, l2B, busB)
	if err != nil {
		_ = cacheA.Close()
		t.Fatalf("TieredCache B: %v", err)
	}
	t.Cleanup(func() {
		_ = cacheA.Close()
		_ = cacheB.Close()
	})

	waitForRedisIntegrationCondition(t, func() bool {
		return cacheA.invalidationReady.Load() && cacheB.invalidationReady.Load()
	})

	if ok, err := cacheA.Set("key", []byte("v1"), time.Minute); err != nil || !ok {
		t.Fatalf("initial Set() = (%t, %v), want (true, nil)", ok, err)
	}
	waitForRedisIntegrationCondition(t, func() bool {
		value, _, err := l2A.Get("key")
		return err == nil && string(value) == "v1"
	})
	warmCacheFromL2(t, cacheA, "v1")
	warmCacheFromL2(t, cacheB, "v1")

	batchValues := map[string][]byte{
		"batch-first":  []byte("batch-v1-first"),
		"batch-second": []byte("batch-v1-second"),
		"batch-third":  []byte("batch-v1-third"),
	}
	if stored, err := cacheA.SetMany(batchValues, time.Minute); err != nil || !stored {
		t.Fatalf("initial SetMany() = (%t, %v), want (true, nil)", stored, err)
	}
	for key, want := range batchValues {
		waitForRedisIntegrationCondition(t, func() bool {
			value, _, err := l2A.Get(key)
			return err == nil && string(value) == string(want)
		})
		warmCacheFromL2Key(t, cacheB, key, string(want))
	}

	updatedBatchValues := map[string][]byte{
		"batch-first":  []byte("batch-v2-first"),
		"batch-second": []byte("batch-v2-second"),
		"batch-third":  []byte("batch-v2-third"),
	}
	if stored, err := cacheA.SetMany(updatedBatchValues, time.Minute); err != nil || !stored {
		t.Fatalf("updated SetMany() = (%t, %v), want (true, nil)", stored, err)
	}
	for key, want := range updatedBatchValues {
		waitForRedisIntegrationCondition(t, func() bool {
			value, _, err := l2A.Get(key)
			return err == nil && string(value) == string(want)
		})
		waitForRedisIntegrationCondition(t, func() bool {
			value, _, err := l1B.Get(key)
			return err == nil && value == nil
		})
		if value, _, err := cacheB.Get(key); err != nil || string(value) != string(want) {
			t.Fatalf("peer Get(%q) = (%q, %v), want (%s, nil)", key, value, err, want)
		}
	}

	if removed, err := cacheA.Forget("key"); err != nil || !removed {
		t.Fatalf("Forget() = (%t, %v), want (true, nil)", removed, err)
	}
	waitForRedisIntegrationCondition(t, func() bool {
		value, _, err := l1B.Get("key")
		return err == nil && value == nil
	})

	if ok, err := cacheA.Set("key", []byte("v1"), time.Minute); err != nil || !ok {
		t.Fatalf("second Set() = (%t, %v), want (true, nil)", ok, err)
	}
	waitForRedisIntegrationCondition(t, func() bool {
		value, _, err := l2A.Get("key")
		return err == nil && string(value) == "v1"
	})
	warmCacheFromL2(t, cacheB, "v1")

	if ok, err := cacheA.Set("key", []byte("v2"), time.Minute); err != nil || !ok {
		t.Fatalf("updated Set() = (%t, %v), want (true, nil)", ok, err)
	}
	waitForRedisIntegrationCondition(t, func() bool {
		value, _, err := l2A.Get("key")
		return err == nil && string(value) == "v2"
	})
	waitForRedisIntegrationCondition(t, func() bool {
		value, _, err := l1B.Get("key")
		return err == nil && value == nil
	})
	if value, _, err := cacheB.Get("key"); err != nil || string(value) != "v2" {
		t.Fatalf("peer Get() = (%q, %v), want (v2, nil)", value, err)
	}

	warmCacheFromL2(t, cacheB, "v2")
	if ok, err := cacheA.Flush(); err != nil || !ok {
		t.Fatalf("Flush() = (%t, %v), want (true, nil)", ok, err)
	}
	waitForRedisIntegrationCondition(t, func() bool {
		value, _, err := l1B.Get("key")
		return err == nil && value == nil
	})
}

func warmCacheFromL2(t *testing.T, cache *TieredCache, want string) {
	t.Helper()
	warmCacheFromL2Key(t, cache, "key", want)
}

func warmCacheFromL2Key(t *testing.T, cache *TieredCache, key, want string) {
	t.Helper()
	value, _, err := cache.Get(key)
	if err != nil {
		t.Fatalf("warm Get(%q) error = %v", key, err)
	}
	if string(value) != want {
		t.Fatalf("warm Get(%q) value = %q, want %q", key, value, want)
	}
}

func waitForRedisIntegrationCondition(t *testing.T, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("Redis integration condition was not satisfied")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
