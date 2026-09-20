package tiered

import (
	"errors"
	"testing"
	"time"

	cacheinvalidation "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache/invalidation"
	"github.com/b1tc0re/frankenphp-tiered-cache/internal/observability"
)

func TestTieredMetricsDoesNotCountL1HitWhenHealthCheckRejectsResult(t *testing.T) {
	metrics := new(observability.MetricsState)
	l1 := newFakeCache()
	l1.put("key", []byte("value"), time.Minute)
	cache := newTestTieredCache(t, Config{Metrics: metrics}, l1, newFakeCache())
	l1.getHook = func() {
		cache.transitionToDegraded(errors.New("subscriber unavailable"))
	}

	if _, _, err := cache.Get("key"); !errors.Is(err, ErrL2Unavailable) {
		t.Fatalf("Get() error = %v, want ErrL2Unavailable", err)
	}

	snapshot := metrics.Snapshot()
	if snapshot.Lookup[observability.LookupL1Hit] != 0 {
		t.Fatalf("L1 hits = %d, want 0 for rejected result", snapshot.Lookup[observability.LookupL1Hit])
	}
}

func TestTieredMetricsDoNotCountGetManyRetryIntermediates(t *testing.T) {
	metrics := new(observability.MetricsState)
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.put("key", []byte("old"), time.Minute)
	l2.getManyStarted = make(chan struct{}, 1)
	l2.releaseGetMany = make(chan struct{})
	cache := newTestTieredCache(t, Config{Metrics: metrics}, l1, l2)

	resultCh := make(chan error, 1)
	go func() {
		_, err := cache.GetMany([]string{"key"})
		resultCh <- err
	}()
	select {
	case <-l2.getManyStarted:
	case <-time.After(time.Second):
		t.Fatal("GetMany() did not start L2 read")
	}

	if _, err := cache.Set("key", []byte("new"), time.Minute); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	close(l2.releaseGetMany)
	if err := <-resultCh; err != nil {
		t.Fatalf("GetMany() error = %v", err)
	}

	snapshot := metrics.Snapshot()
	if snapshot.Lookup[observability.LookupL1Hit] != 1 || snapshot.Lookup[observability.LookupL2Hit] != 0 || snapshot.Lookup[observability.LookupMiss] != 0 {
		t.Fatalf("lookup metrics = %#v, want one final L1 hit", snapshot.Lookup)
	}
	if snapshot.L1Misses != 0 {
		t.Fatalf("L1 misses = %d, want 0 for retry intermediate", snapshot.L1Misses)
	}
}

func TestTieredMetricsCountExpiredL2ValueAsMiss(t *testing.T) {
	metrics := new(observability.MetricsState)
	l2 := newFakeCache()
	l2.put("key", []byte("value"), 10*time.Millisecond)
	l2.getDelay = 50 * time.Millisecond
	cache := newTestTieredCache(t, Config{Metrics: metrics}, newFakeCache(), l2)

	value, _, err := cache.Get("key")
	if err != nil || value != nil {
		t.Fatalf("Get() = (%q, %v), want miss", value, err)
	}

	snapshot := metrics.Snapshot()
	if snapshot.Lookup[observability.LookupMiss] != 1 || snapshot.Lookup[observability.LookupL2Hit] != 0 {
		t.Fatalf("lookup metrics = %#v, want one miss", snapshot.Lookup)
	}
}

func TestTieredMetricsAggregateDuplicateGetManyKeys(t *testing.T) {
	metrics := new(observability.MetricsState)
	l1 := newFakeCache()
	l1.put("warm", []byte("l1"), time.Minute)
	l2 := newFakeCache()
	l2.put("cold", []byte("l2"), time.Minute)
	cache := newTestTieredCache(t, Config{Metrics: metrics}, l1, l2)

	result, err := cache.GetMany([]string{"warm", "warm", "cold", "cold", "missing", "missing"})
	if err != nil {
		t.Fatalf("GetMany() error = %v", err)
	}
	if len(result) != 2 || string(result["warm"].Value) != "l1" || string(result["cold"].Value) != "l2" {
		t.Fatalf("GetMany() = %#v, want warm and cold values", result)
	}

	snapshot := metrics.Snapshot()
	if snapshot.Lookup[observability.LookupL1Hit] != 2 || snapshot.Lookup[observability.LookupL2Hit] != 2 || snapshot.Lookup[observability.LookupMiss] != 2 {
		t.Fatalf("lookup metrics = %#v, want 2 hits per result class", snapshot.Lookup)
	}
	if snapshot.L1Misses != 4 {
		t.Fatalf("L1 misses = %d, want 4", snapshot.L1Misses)
	}
}

func TestTieredMetricsInvalidationErrorClearsReadyGauge(t *testing.T) {
	metrics := new(observability.MetricsState)
	bus := newFakeInvalidationBus()
	newTestTieredCacheWithInvalidationConfig(t, Config{
		Metrics:          metrics,
		RecoveryInterval: time.Hour,
	}, newFakeCache(), newFakeCache(), bus)
	waitForInvalidationCondition(t, func() bool { return metrics.Snapshot().InvalidationReady })

	bus.mu.Lock()
	for subscriber := range bus.subscribers {
		subscriber.events <- cacheinvalidation.Event{}
	}
	bus.mu.Unlock()
	waitForInvalidationCondition(t, func() bool { return !metrics.Snapshot().InvalidationReady })
	if got := metrics.Snapshot().SubscriberErrors[observability.SubscriberErrorApply]; got != 1 {
		t.Fatalf("subscriber apply errors = %d, want 1", got)
	}
}

func TestTieredMetricsRecordSelfInvalidationAsIgnored(t *testing.T) {
	metrics := new(observability.MetricsState)
	cache := newTestTieredCache(t, Config{Metrics: metrics}, newFakeCache(), newFakeCache())
	cache.invalidationOrigin = "local-origin"

	err := cache.applyInvalidation(cacheinvalidation.Event{
		Version: cacheinvalidation.ProtocolVersion,
		Type:    cacheinvalidation.EventTypeInvalidate,
		Key:     "key",
		Origin:  "local-origin",
	})
	if err != nil {
		t.Fatalf("applyInvalidation() error = %v", err)
	}

	snapshot := metrics.Snapshot()
	if snapshot.Invalidation[observability.InvalidationReceived][observability.InvalidationKey][observability.InvalidationIgnoredSelf] != 1 {
		t.Fatalf("self invalidations = %#v, want ignored_self=1", snapshot.Invalidation)
	}
	if snapshot.Invalidation[observability.InvalidationReceived][observability.InvalidationKey][observability.InvalidationSuccess] != 0 {
		t.Fatalf("self invalidation was counted as success: %#v", snapshot.Invalidation)
	}
}

func TestTieredMetricsRecordFlushPostCommitError(t *testing.T) {
	metrics := new(observability.MetricsState)
	l1 := newFakeCache()
	l1.flushErr = errors.New("L1 flush failed")
	cache := newTestTieredCache(t, Config{Metrics: metrics}, l1, newFakeCache())

	if ok, err := cache.Flush(); ok || err == nil {
		t.Fatalf("Flush() = (%t, %v), want post-commit error", ok, err)
	}
	if got := metrics.Snapshot().PostCommitErrors[observability.OperationFlush]; got != 1 {
		t.Fatalf("flush post-commit errors = %d, want 1", got)
	}
}

func TestTieredMetricsRecordL1FallbackResults(t *testing.T) {
	metrics := new(observability.MetricsState)
	l1 := newFakeCache()
	cache := newTestTieredCache(t, Config{Metrics: metrics}, l1, newFakeCache())

	cache.mutationMu.Lock()
	if err := cache.reconcileL1MutationErrorLocked(errors.New("L1 mutation failed")); err == nil {
		t.Fatal("successful fallback returned nil error")
	}
	cache.mutationMu.Unlock()

	l1.mu.Lock()
	l1.flushErr = errors.New("L1 flush failed")
	l1.mu.Unlock()
	cache.mutationMu.Lock()
	if err := cache.reconcileL1MutationErrorLocked(errors.New("L1 mutation failed")); err == nil {
		t.Fatal("failed fallback returned nil error")
	}
	cache.mutationMu.Unlock()

	snapshot := metrics.Snapshot()
	want := [observability.L1FlushFallbackResultCount]uint64{1, 1}
	if snapshot.L1FlushFallbacks != want {
		t.Fatalf("fallback metrics = %#v, want %#v", snapshot.L1FlushFallbacks, want)
	}
}

func TestTieredMetricsRecordRecoveryResult(t *testing.T) {
	metrics := new(observability.MetricsState)
	l2 := newFakeCache()
	l2.getErr = errors.New("Redis unavailable")
	cache := newTestTieredCache(t, Config{
		Metrics:          metrics,
		RecoveryInterval: time.Millisecond,
	}, newFakeCache(), l2)

	if _, _, err := cache.Get("key"); err == nil {
		t.Fatal("Get() error = nil, want degraded error")
	}
	waitForTieredCondition(t, func() bool { return metrics.Snapshot().RecoveryFailures > 0 })
	l2.mu.Lock()
	l2.getErr = nil
	l2.mu.Unlock()
	waitForTieredCondition(t, func() bool {
		snapshot := metrics.Snapshot()
		return snapshot.RecoveryAttempts > 0 && snapshot.RecoverySuccesses > 0 && !snapshot.Degraded
	})
}
