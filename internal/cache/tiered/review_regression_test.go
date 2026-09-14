package tiered

import (
	"errors"
	"testing"
	"time"
)

func TestTieredCacheWithInvalidationIsReadyOnReturn(t *testing.T) {
	bus := newFakeInvalidationBus()
	cache, err := NewWithInvalidation(
		Config{RecoveryInterval: 5 * time.Millisecond},
		newFakeCache(),
		newFakeCache(),
		bus,
	)
	if err != nil {
		t.Fatalf("NewWithInvalidation() error = %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	if !cache.invalidationReady.Load() {
		t.Fatal("NewWithInvalidation() returned before initial subscription was ready")
	}
	if _, _, err := cache.Get("missing"); err != nil {
		t.Fatalf("Get() immediately after construction error = %v", err)
	}
}

func TestTieredCacheWarmGetDoesNotWaitForMutationMutex(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l1.values["key"] = []byte("value")
	l1.ttls["key"] = time.Minute
	cache := newTestTieredCache(t, Config{}, l1, l2)

	cache.mutationMu.Lock()
	defer cache.mutationMu.Unlock()

	done := make(chan error, 1)
	go func() {
		value, _, err := cache.Get("key")
		if err == nil && string(value) != "value" {
			err = errors.New("unexpected cached value")
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("warm Get() error = %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("warm Get() waited for global mutation mutex")
	}
}

func TestTieredCacheFailedFlushStillInvalidatesPeerL1(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1A := newFakeCache()
	l1B := newFakeCache()
	l2 := newFakeCache()
	l2.values["key"] = []byte("value")
	l2.ttls["key"] = time.Minute

	config := Config{RecoveryInterval: 5 * time.Millisecond}
	cacheA := newTestTieredCacheWithInvalidationConfig(t, config, l1A, l2, bus)
	cacheB := newTestTieredCacheWithInvalidationConfig(t, config, l1B, l2, bus)
	if _, _, err := cacheB.Get("key"); err != nil {
		t.Fatalf("warm peer Get() error = %v", err)
	}

	wantErr := errors.New("flush failed")
	l2.setFlushError(wantErr)
	if ok, err := cacheA.Flush(); ok || !errors.Is(err, wantErr) {
		t.Fatalf("Flush() = (%t, %v), want (false, %v)", ok, err, wantErr)
	}

	waitForInvalidationCondition(t, func() bool {
		value, _, err := l1B.Get("key")
		return err == nil && value == nil
	})
	if got := l2.flushCount(); got != 1 {
		t.Fatalf("L2 Flush() calls = %d, want 1", got)
	}
}
