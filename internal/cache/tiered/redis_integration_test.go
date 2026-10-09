package tiered

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/b1tc0re/frankenphp-tiered-cache/internal/cache/memory"
	redisbackend "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache/redis"
	"github.com/b1tc0re/frankenphp-tiered-cache/internal/observability"
	goredis "github.com/redis/go-redis/v9"
)

func TestTieredCacheRedisPubSubIntegration(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("set REDIS_ADDR to run the Redis integration test")
	}

	prefix := fmt.Sprintf("frankenphp-tiered-integration:%d:", time.Now().UnixNano())
	redisConfig := redisbackend.Config{Addr: addr, KeyPrefix: prefix}
	cacheA, cacheB, _, l1B := newRedisIntegrationCachePair(t, redisConfig)

	waitForRedisIntegrationCondition(t, func() bool {
		return cacheA.invalidationReady.Load() && cacheB.invalidationReady.Load()
	})

	if ok, err := cacheA.Set("key", []byte("v1"), time.Minute); err != nil || !ok {
		t.Fatalf("initial Set() = (%t, %v), want (true, nil)", ok, err)
	}
	waitForRedisIntegrationCondition(t, func() bool {
		value, _, err := cacheA.l2.Get("key")
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
			value, _, err := cacheA.l2.Get(key)
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
			value, _, err := cacheA.l2.Get(key)
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

	for key := range updatedBatchValues {
		if _, err := l1B.Forget(key); err != nil {
			t.Fatalf("clear L1 B for %q: %v", key, err)
		}
	}
	for key, want := range updatedBatchValues {
		value, _, err := cacheA.l2.Get(key)
		if err != nil || string(value) != string(want) {
			t.Fatalf("direct L2 Get(%q) = (%q, %v), want (%s, nil)", key, value, err, want)
		}
	}
	batchResult, err := cacheB.GetMany([]string{"batch-first", "batch-second", "batch-third", "batch-missing"})
	if err != nil {
		t.Fatalf("peer GetMany() error = %v", err)
	}
	if len(batchResult) != len(updatedBatchValues) {
		t.Fatalf("peer GetMany() returned %d items, want %d: %#v", len(batchResult), len(updatedBatchValues), batchResult)
	}
	for key, want := range updatedBatchValues {
		item, ok := batchResult[key]
		if !ok || string(item.Value) != string(want) || item.TTL <= 0 {
			t.Fatalf("peer GetMany(%q) = (%q, %v, %t), want value with TTL", key, item.Value, item.TTL, ok)
		}
	}
	if _, ok := batchResult["batch-missing"]; ok {
		t.Fatal("peer GetMany() returned missing key")
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
		value, _, err := cacheA.l2.Get("key")
		return err == nil && string(value) == "v1"
	})
	warmCacheFromL2(t, cacheB, "v1")

	if ok, err := cacheA.Set("key", []byte("v2"), time.Minute); err != nil || !ok {
		t.Fatalf("updated Set() = (%t, %v), want (true, nil)", ok, err)
	}
	waitForRedisIntegrationCondition(t, func() bool {
		value, _, err := cacheA.l2.Get("key")
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

func TestTieredCacheExternalRedisFlushIntegration(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("set REDIS_ADDR to a disposable Redis instance")
	}

	// FLUSHDB is intentionally destructive; Taskfile.yml starts a disposable
	// Redis instance for integration tests.
	prefix := fmt.Sprintf("frankenphp-tiered-external-flush:%d:", time.Now().UnixNano())
	redisConfig := redisbackend.Config{Addr: addr, KeyPrefix: prefix}
	cacheA, cacheB, l1A, l1B := newRedisIntegrationCachePair(t, redisConfig)

	waitForRedisIntegrationCondition(t, func() bool {
		return cacheA.invalidationReady.Load() && cacheB.invalidationReady.Load()
	})

	const key = "external-flush-key"
	externalClient := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { _ = externalClient.Close() })
	for flushNumber := 1; flushNumber <= 2; flushNumber++ {
		want := fmt.Sprintf("cached-before-flush-%d", flushNumber)
		if ok, err := cacheA.Set(key, []byte(want), time.Minute); err != nil || !ok {
			t.Fatalf("Set() before flush %d = (%t, %v), want (true, nil)", flushNumber, ok, err)
		}
		if value, _, err := cacheB.Get(key); err != nil || string(value) != want {
			t.Fatalf("peer Get() before flush %d = (%q, %v), want %q", flushNumber, value, err, want)
		}
		for name, l1 := range map[string]*memory.MemoryCache{"A": l1A, "B": l1B} {
			value, _, err := l1.Get(key)
			if err != nil || string(value) != want {
				t.Fatalf("L1 %s before external flush %d = (%q, %v), want %q", name, flushNumber, value, err, want)
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := externalClient.FlushDB(ctx).Err()
		cancel()
		if err != nil {
			t.Fatalf("external Redis FlushDB() %d error = %v", flushNumber, err)
		}

		waitForRedisIntegrationCondition(t, func() bool {
			valueA, _, errA := l1A.Get(key)
			valueB, _, errB := l1B.Get(key)
			return errA == nil && valueA == nil && errB == nil && valueB == nil
		})
		for name, cache := range map[string]*TieredCache{"A": cacheA, "B": cacheB} {
			value, _, err := cache.Get(key)
			if err != nil || value != nil {
				t.Fatalf("TieredCache %s Get() after external flush %d = (%q, %v), want miss", name, flushNumber, value, err)
			}
		}
	}
}

func TestTieredCacheTrackingCheckTimeoutIntegration(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("set REDIS_ADDR to run the Redis integration test")
	}

	metrics := new(observability.MetricsState)
	prefix := fmt.Sprintf("frankenphp-tiered-tracking-timeout:%d:", time.Now().UnixNano())
	redisConfig := redisbackend.Config{Addr: addr, KeyPrefix: prefix}
	tieredConfig := Config{RecoveryInterval: 25 * time.Millisecond, Metrics: metrics}
	cacheA, cacheB, l1A, l1B := newRedisIntegrationCachePairWithConfig(t, redisConfig, tieredConfig)

	waitForRedisIntegrationCondition(t, func() bool {
		return cacheA.invalidationReady.Load() && cacheB.invalidationReady.Load()
	})

	const key = "tracking-timeout-key"
	const warmValue = "cached-before-redis-pause"
	const redisPauseDuration = 5 * time.Second
	keyInvalidationCount := func() uint64 {
		return metrics.Snapshot().Invalidation[observability.InvalidationReceived][observability.InvalidationKey][observability.InvalidationSuccess]
	}
	keyInvalidationsBeforeWarm := keyInvalidationCount()
	if ok, err := cacheA.Set(key, []byte(warmValue), time.Minute); err != nil || !ok {
		t.Fatalf("Set() before Redis pause = (%t, %v), want (true, nil)", ok, err)
	}
	if !waitForRedisIntegrationConditionWithin(3*time.Second, func() bool {
		return keyInvalidationCount() > keyInvalidationsBeforeWarm
	}) {
		t.Fatal("peer did not receive the warm-up invalidation")
	}
	if value, _, err := cacheB.Get(key); err != nil || string(value) != warmValue {
		t.Fatalf("peer Get() before Redis pause = (%q, %v), want %q", value, err, warmValue)
	}
	for name, l1 := range map[string]*memory.MemoryCache{"A": l1A, "B": l1B} {
		value, _, err := l1.Get(key)
		if err != nil || string(value) != warmValue {
			t.Fatalf("L1 %s before Redis pause = (%q, %v), want %q", name, value, err, warmValue)
		}
	}

	externalClient := goredis.NewClient(&goredis.Options{
		Addr:                  addr,
		ContextTimeoutEnabled: true,
	})
	unpauseRedis := func() error {
		// CLIENT PAUSE ALL also queues CLIENT UNPAUSE until the pause expires.
		ctx, cancel := context.WithTimeout(context.Background(), redisPauseDuration+2*time.Second)
		defer cancel()
		return externalClient.Do(ctx, "CLIENT", "UNPAUSE").Err()
	}
	t.Cleanup(func() {
		if err := unpauseRedis(); err != nil {
			t.Errorf("CLIENT UNPAUSE during cleanup: %v", err)
		}
		if err := externalClient.Close(); err != nil {
			t.Errorf("close independent Redis client: %v", err)
		}
	})

	receiveErrorsBeforePause := metrics.Snapshot().SubscriberErrors[observability.SubscriberErrorReceive]
	pauseCtx, cancelPause := context.WithTimeout(context.Background(), 2*time.Second)
	pauseErr := externalClient.Do(pauseCtx, "CLIENT", "PAUSE", redisPauseDuration.Milliseconds(), "ALL").Err()
	cancelPause()
	if pauseErr != nil {
		t.Fatalf("CLIENT PAUSE 5000 ALL: %v", pauseErr)
	}
	pauseStarted := time.Now()

	const trackingCheckLimit = 2 * time.Second
	const schedulerTolerance = time.Second
	receiveErrorWait := trackingCheckLimit + schedulerTolerance
	// This counter is updated as soon as Subscription.Receive returns the
	// tracking probe error, before degradation and recovery start.
	errorReceived := waitForRedisIntegrationConditionWithin(receiveErrorWait, func() bool {
		return metrics.Snapshot().SubscriberErrors[observability.SubscriberErrorReceive] > receiveErrorsBeforePause
	})
	if !errorReceived {
		t.Fatalf("tracking check error was not received within %s while Redis was paused", receiveErrorWait)
	}
	checkErrorElapsed := time.Since(pauseStarted)
	if minElapsed := trackingCheckLimit - 500*time.Millisecond; checkErrorElapsed < minElapsed || checkErrorElapsed > receiveErrorWait {
		t.Fatalf("tracking check error arrived after %s, want between %s and %s", checkErrorElapsed, minElapsed, receiveErrorWait)
	}

	const degradedTransitionWait = time.Second
	if !waitForRedisIntegrationConditionWithin(degradedTransitionWait, func() bool {
		valueA, _, errA := l1A.Get(key)
		valueB, _, errB := l1B.Get(key)
		return healthState(cacheA.healthState.Load()) == degraded &&
			healthState(cacheB.healthState.Load()) == degraded &&
			errA == nil && valueA == nil && errB == nil && valueB == nil
	}) {
		t.Fatalf("TieredCache instances did not degrade and clear L1 within %s", degradedTransitionWait)
	}

	if err := unpauseRedis(); err != nil {
		t.Fatalf("CLIENT UNPAUSE after tracking check: %v", err)
	}

	const recoveryWait = 10 * time.Second
	if !waitForRedisIntegrationConditionWithin(recoveryWait, func() bool {
		return cacheA.invalidationReady.Load() &&
			cacheB.invalidationReady.Load() &&
			healthState(cacheA.healthState.Load()) == healthy &&
			healthState(cacheB.healthState.Load()) == healthy
	}) {
		t.Fatalf("TieredCache subscriptions did not recover within %s", recoveryWait)
	}

	for name, cache := range map[string]*TieredCache{"A": cacheA, "B": cacheB} {
		value, _, err := cache.Get(key)
		if err != nil || string(value) != warmValue {
			t.Fatalf("TieredCache %s Get() after recovery = (%q, %v), want %q", name, value, err, warmValue)
		}
	}
	const recoveredValue = "cache-after-redis-pause"
	if ok, err := cacheA.Set(key, []byte(recoveredValue), time.Minute); err != nil || !ok {
		t.Fatalf("Set() after recovery = (%t, %v), want (true, nil)", ok, err)
	}
	waitForRedisIntegrationCondition(t, func() bool {
		value, _, err := l1B.Get(key)
		return err == nil && value == nil
	})
	if value, _, err := cacheB.Get(key); err != nil || string(value) != recoveredValue {
		t.Fatalf("peer Get() after subscription recovery = (%q, %v), want %q", value, err, recoveredValue)
	}
}

func newRedisIntegrationCachePair(
	t *testing.T,
	redisConfig redisbackend.Config,
) (*TieredCache, *TieredCache, *memory.MemoryCache, *memory.MemoryCache) {
	return newRedisIntegrationCachePairWithConfig(t, redisConfig, Config{RecoveryInterval: 25 * time.Millisecond})
}

func newRedisIntegrationCachePairWithConfig(
	t *testing.T,
	redisConfig redisbackend.Config,
	tieredConfig Config,
) (*TieredCache, *TieredCache, *memory.MemoryCache, *memory.MemoryCache) {
	t.Helper()

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

	cacheA, err := NewWithInvalidation(tieredConfig, l1A, l2A, busA)
	if err != nil {
		_ = busB.Close()
		_ = busA.Close()
		_ = l2A.Close()
		_ = l2B.Close()
		_ = l1A.Close()
		_ = l1B.Close()
		t.Fatalf("TieredCache A: %v", err)
	}
	cacheB, err := NewWithInvalidation(tieredConfig, l1B, l2B, busB)
	if err != nil {
		_ = cacheA.Close()
		t.Fatalf("TieredCache B: %v", err)
	}
	t.Cleanup(func() {
		_ = cacheA.Close()
		_ = cacheB.Close()
	})

	return cacheA, cacheB, l1A, l1B
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
	if !waitForRedisIntegrationConditionWithin(3*time.Second, condition) {
		t.Fatal("Redis integration condition was not satisfied")
	}
}

func waitForRedisIntegrationConditionWithin(timeout time.Duration, condition func() bool) bool {
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
	return true
}
