package tiered

import (
	"errors"
	"testing"
	"time"
)

func TestTieredCacheGetWaitsForPendingWritesAfterL1Miss(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.values["key"] = []byte("old")
	l2.ttls["key"] = time.Minute
	l2.setStarted = make(chan struct{}, 1)
	l2.releaseSet = make(chan struct{})

	cache := newTestTieredCache(t, Config{WriteQueueCapacity: 2}, l1, l2)

	if ok, err := cache.Set("key", []byte("v2"), time.Minute); err != nil || !ok {
		t.Fatalf("first Set() = (%t, %v), want (true, nil)", ok, err)
	}
	select {
	case <-l2.setStarted:
	case <-time.After(time.Second):
		t.Fatal("L2 worker did not start first write")
	}

	if ok, err := cache.Set("key", []byte("v3"), time.Minute); err != nil || !ok {
		t.Fatalf("second Set() = (%t, %v), want (true, nil)", ok, err)
	}

	if removed, err := l1.Forget("key"); err != nil || !removed {
		t.Fatalf("simulated L1 eviction = (%t, %v), want (true, nil)", removed, err)
	}

	type getResult struct {
		value []byte
		ttl   time.Duration
		err   error
	}
	getDone := make(chan getResult, 1)
	go func() {
		value, ttl, err := cache.Get("key")
		getDone <- getResult{value: value, ttl: ttl, err: err}
	}()

	select {
	case result := <-getDone:
		t.Fatalf("Get() returned before pending writes completed: %+v", result)
	case <-time.After(20 * time.Millisecond):
	}
	if got := l2.getCount(); got != 0 {
		t.Fatalf("L2 Get() calls while write pending = %d, want 0", got)
	}

	close(l2.releaseSet)

	select {
	case result := <-getDone:
		if result.err != nil {
			t.Fatalf("Get() error = %v", result.err)
		}
		if string(result.value) != "v3" {
			t.Fatalf("Get() value = %q, want v3", result.value)
		}
	case <-time.After(time.Second):
		t.Fatal("Get() did not finish after pending writes completed")
	}
}

func TestTieredCachePendingWriteFailureDoesNotExposeStaleL2(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.values["key"] = []byte("old")
	l2.ttls["key"] = time.Minute
	wantErr := errors.New("redis unavailable")
	l2.setErr = wantErr

	errorsReported := make(chan error, 1)
	cache := newTestTieredCache(t, Config{
		OnWriteError: func(_ string, err error) {
			errorsReported <- err
		},
	}, l1, l2)

	if ok, err := cache.Set("key", []byte("new"), time.Minute); err != nil || !ok {
		t.Fatalf("Set() = (%t, %v), want (true, nil)", ok, err)
	}

	select {
	case err := <-errorsReported:
		if !errors.Is(err, wantErr) {
			t.Fatalf("reported error = %v, want %v", err, wantErr)
		}
	case <-time.After(time.Second):
		t.Fatal("async write error was not reported")
	}

	if value, _, err := l1.Get("key"); err != nil || value != nil {
		t.Fatalf("L1 value after degraded transition = (%q, %v), want (nil, nil)", value, err)
	}

	value, ttl, err := cache.Get("key")
	if value != nil || ttl != 0 || !errors.Is(err, wantErr) {
		t.Fatalf("Get() = (%q, %v, %v), want (nil, 0, %v)", value, ttl, err, wantErr)
	}
	if got := l2.getCount(); got != 0 {
		t.Fatalf("L2 Get() calls after failed pending write = %d, want 0", got)
	}
}

func TestTieredCacheRecoversAfterL2BecomesAvailable(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	wantErr := errors.New("redis unavailable")
	l2.setErrors(wantErr, wantErr)

	errorsReported := make(chan error, 1)
	cache := newTestTieredCache(t, Config{
		RecoveryInterval: 10 * time.Millisecond,
		OnWriteError: func(_ string, err error) {
			errorsReported <- err
		},
	}, l1, l2)

	if ok, err := cache.Set("key", []byte("value"), time.Minute); err != nil || !ok {
		t.Fatalf("Set() = (%t, %v), want (true, nil)", ok, err)
	}
	select {
	case err := <-errorsReported:
		if !errors.Is(err, wantErr) {
			t.Fatalf("reported error = %v, want %v", err, wantErr)
		}
	case <-time.After(time.Second):
		t.Fatal("async write error was not reported")
	}

	l2.setErrors(nil, nil)
	deadline := time.Now().Add(time.Second)
	for {
		ok, err := cache.Set("recovered", []byte("value"), time.Minute)
		if ok && err == nil {
			break
		}
		if !errors.Is(err, ErrL2Unavailable) {
			t.Fatalf("recovery Set() = (%t, %v), want temporary ErrL2Unavailable", ok, err)
		}
		if time.Now().After(deadline) {
			t.Fatal("TieredCache did not recover after L2 became available")
		}
		time.Sleep(time.Millisecond)
	}

	value, ttl, err := l1.Get("recovered")
	if err != nil {
		t.Fatalf("recovered L1 Get() error = %v", err)
	}
	if string(value) != "value" || ttl != time.Minute {
		t.Fatalf("recovered L1 = (%q, %v), want (value, %v)", value, ttl, time.Minute)
	}
}
