package tiered

import (
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestTieredCacheGetChecksL1BeforeL2(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l1.values["key"] = []byte("l1")
	l1.ttls["key"] = time.Minute
	l2.values["key"] = []byte("l2")
	l2.ttls["key"] = 2 * time.Minute

	cache := newTestTieredCache(t, Config{}, l1, l2)

	value, ttl, err := cache.Get("key")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(value) != "l1" || ttl != time.Minute {
		t.Fatalf("Get() = (%q, %v), want (l1, %v)", value, ttl, time.Minute)
	}
	if l2.getCount() != 0 {
		t.Fatalf("L2 Get() calls = %d, want 0", l2.getCount())
	}
}

func TestTieredCacheGetWarmsL1WithRemainingTTL(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.values["key"] = []byte("value")
	l2.ttls["key"] = 42 * time.Second

	cache := newTestTieredCache(t, Config{}, l1, l2)

	value, ttl, err := cache.Get("key")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(value) != "value" || ttl != 42*time.Second {
		t.Fatalf("Get() = (%q, %v), want (value, %v)", value, ttl, 42*time.Second)
	}

	warmedValue, warmedTTL, err := l1.Get("key")
	if err != nil {
		t.Fatalf("warmed L1 Get() error = %v", err)
	}
	if string(warmedValue) != "value" || warmedTTL != 42*time.Second {
		t.Fatalf("warmed L1 = (%q, %v), want (value, %v)", warmedValue, warmedTTL, 42*time.Second)
	}
}

func TestTieredCacheGetWarmsL1ForeverValue(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.values["key"] = []byte("value")
	l2.ttls["key"] = 0

	cache := newTestTieredCache(t, Config{}, l1, l2)

	if _, _, err := cache.Get("key"); err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	_, ttl, err := l1.Get("key")
	if err != nil {
		t.Fatalf("warmed L1 Get() error = %v", err)
	}
	if ttl != 0 {
		t.Fatalf("warmed L1 TTL = %v, want 0", ttl)
	}
}

func TestTieredCacheDoesNotWarmL1AfterConcurrentFlush(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.values["key"] = []byte("stale")
	l2.ttls["key"] = time.Minute

	l2Read := make(chan struct{})
	allowL2Return := make(chan struct{})
	l2.getHook = func() {
		close(l2Read)
		<-allowL2Return
	}

	cache := newTestTieredCache(t, Config{}, l1, l2)

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
	case <-l2Read:
	case <-time.After(time.Second):
		t.Fatal("L2 Get() did not start")
	}

	flushDone := make(chan struct {
		ok  bool
		err error
	}, 1)
	go func() {
		ok, err := cache.Flush()
		flushDone <- struct {
			ok  bool
			err error
		}{ok: ok, err: err}
	}()

	select {
	case result := <-flushDone:
		if result.err != nil || !result.ok {
			t.Fatalf("Flush() = (%t, %v), want (true, nil)", result.ok, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("Flush() did not finish")
	}
	close(allowL2Return)

	select {
	case result := <-getDone:
		if result.err != nil {
			t.Fatalf("Get() error = %v", result.err)
		}
		if result.value != nil || result.ttl != 0 {
			t.Fatalf("Get() after Flush() = (%q, %v), want (nil, 0)", result.value, result.ttl)
		}
	case <-time.After(time.Second):
		t.Fatal("Get() did not finish")
	}
}

func TestTieredCacheSetDoesNotWaitForL2(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.setStarted = make(chan struct{}, 1)
	l2.releaseSet = make(chan struct{})
	cache := newTestTieredCache(t, Config{WriteQueueCapacity: 1}, l1, l2)

	start := time.Now()
	ok, err := cache.Set("key", []byte("value"), time.Minute)
	if err != nil || !ok {
		t.Fatalf("Set() = (%t, %v), want (true, nil)", ok, err)
	}
	if elapsed := time.Since(start); elapsed >= 50*time.Millisecond {
		t.Fatalf("Set() took %v while L2 was blocked", elapsed)
	}

	select {
	case <-l2.setStarted:
	case <-time.After(time.Second):
		t.Fatal("L2 worker did not receive Set()")
	}
	close(l2.releaseSet)
	if err := cache.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestTieredCacheSetWaitsForQueueSpaceUntilTimeout(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.values["second"] = []byte("old")
	l2.ttls["second"] = time.Minute
	l2.setStarted = make(chan struct{}, 1)
	l2.releaseSet = make(chan struct{})
	cache := newTestTieredCache(t, Config{
		WriteQueueCapacity:    1,
		WriteQueueWaitTimeout: 20 * time.Millisecond,
		RecoveryInterval:      10 * time.Millisecond,
	}, l1, l2)

	if ok, err := cache.Set("first", []byte("1"), time.Minute); err != nil || !ok {
		t.Fatalf("first Set() = (%t, %v), want (true, nil)", ok, err)
	}
	select {
	case <-l2.setStarted:
	case <-time.After(time.Second):
		t.Fatal("L2 worker did not receive first Set()")
	}

	ok, err := cache.Set("second", []byte("2"), time.Minute)
	if ok || !errors.Is(err, ErrL2Unavailable) || !errors.Is(err, ErrWriteQueueTimeout) {
		t.Fatalf("second Set() = (%t, %v), want (false, ErrL2Unavailable wrapping ErrWriteQueueTimeout)", ok, err)
	}
	if value, _, _ := l1.Get("second"); value != nil {
		t.Fatalf("L1 second value = %q, want nil after degraded transition", value)
	}

	close(l2.releaseSet)
	deadline := time.Now().Add(time.Second)
	for {
		value, _, err := l2.Get("second")
		if err != nil {
			t.Fatalf("L2 Get(second) = %v", err)
		}
		if value == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("queue-timeout dirty key was not removed during recovery")
		}
		time.Sleep(time.Millisecond)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestTieredCacheForgetWaitsForQueuedWrites(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.setStarted = make(chan struct{}, 1)
	l2.releaseSet = make(chan struct{})
	cache := newTestTieredCache(t, Config{WriteQueueCapacity: 1}, l1, l2)

	if ok, err := cache.Set("key", []byte("value"), time.Minute); err != nil || !ok {
		t.Fatalf("Set() = (%t, %v), want (true, nil)", ok, err)
	}
	select {
	case <-l2.setStarted:
	case <-time.After(time.Second):
		t.Fatal("L2 worker did not receive Set()")
	}

	done := make(chan struct{})
	go func() {
		_, _ = cache.Forget("key")
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Forget() returned before queued L2 write completed")
	case <-time.After(20 * time.Millisecond):
	}

	close(l2.releaseSet)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Forget() did not finish after queued write completed")
	}

	if value, _, _ := l1.Get("key"); value != nil {
		t.Fatalf("L1 key = %q after Forget(), want nil", value)
	}
	if value, _, _ := l2.Get("key"); value != nil {
		t.Fatalf("L2 key = %q after Forget(), want nil", value)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestTieredCacheAsyncWriteErrorIsReported(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	wantErr := errors.New("redis unavailable")
	l2.setErr = wantErr
	errorsReported := make(chan error, 1)
	cache := newTestTieredCache(t, Config{
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
	if err := cache.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestTieredCacheSynchronousOperationsUseBothLevels(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l1.values["key"] = []byte("value")
	l2.values["key"] = []byte("value")
	cache := newTestTieredCache(t, Config{}, l1, l2)

	touched, err := cache.Touch("key", time.Minute)
	if err != nil || !touched {
		t.Fatalf("Touch() = (%t, %v), want (true, nil)", touched, err)
	}
	if _, ttl, _ := l1.Get("key"); ttl != time.Minute {
		t.Fatalf("L1 TTL after Touch() = %v, want %v", ttl, time.Minute)
	}
	if _, ttl, _ := l2.Get("key"); ttl != time.Minute {
		t.Fatalf("L2 TTL after Touch() = %v, want %v", ttl, time.Minute)
	}

	flushed, err := cache.Flush()
	if err != nil || !flushed {
		t.Fatalf("Flush() = (%t, %v), want (true, nil)", flushed, err)
	}
	if value, _, _ := l1.Get("key"); value != nil {
		t.Fatalf("L1 key = %q after Flush(), want nil", value)
	}
	if value, _, _ := l2.Get("key"); value != nil {
		t.Fatalf("L2 key = %q after Flush(), want nil", value)
	}
}

func TestTieredCacheFlushFailureIsNotRetriedDuringRecovery(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l1.values["key"] = []byte("value")
	l2.values["key"] = []byte("value")
	wantErr := errors.New("redis flush failed")
	l2.setFlushError(wantErr)

	cache := newTestTieredCache(t, Config{RecoveryInterval: 10 * time.Millisecond}, l1, l2)

	flushed, err := cache.Flush()
	if flushed || !errors.Is(err, ErrL2Unavailable) || !errors.Is(err, wantErr) {
		t.Fatalf("Flush() = (%t, %v), want (false, L2 unavailable wrapping %v)", flushed, err, wantErr)
	}
	if value, _, _ := l1.Get("key"); value != nil {
		t.Fatalf("L1 key after failed Flush() = %q, want nil", value)
	}

	deadline := time.Now().Add(time.Second)
	for {
		ok, err := cache.Set("after-recovery", []byte("value"), time.Minute)
		if ok && err == nil {
			break
		}
		if !errors.Is(err, ErrL2Unavailable) {
			t.Fatalf("Set() during recovery = (%t, %v), want temporary L2 unavailable", ok, err)
		}
		if time.Now().After(deadline) {
			t.Fatal("TieredCache did not recover after failed Flush()")
		}
		time.Sleep(time.Millisecond)
	}

	if got := l2.flushCount(); got != 1 {
		t.Fatalf("L2 Flush() calls = %d, want 1", got)
	}
	if value, _, err := l2.Get("key"); err != nil || string(value) != "value" {
		t.Fatalf("L2 key after failed Flush() = (%q, %v), want (value, nil)", value, err)
	}
}

func TestTieredCacheCloseIsIdempotent(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	cache := newTestTieredCache(t, Config{}, l1, l2)

	if err := cache.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if l1.closeCount() != 1 || l2.closeCount() != 1 {
		t.Fatalf("Close() counts = (%d, %d), want (1, 1)", l1.closeCount(), l2.closeCount())
	}
}

func TestTieredCacheOperationsAfterCloseReturnErrClosed(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	cache := newTestTieredCache(t, Config{}, l1, l2)

	if err := cache.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "Get",
			call: func() error {
				_, _, err := cache.Get("key")
				return err
			},
		},
		{
			name: "Set",
			call: func() error {
				_, err := cache.Set("key", []byte("value"), time.Minute)
				return err
			},
		},
		{
			name: "Forever",
			call: func() error {
				_, err := cache.Forever("key", []byte("value"))
				return err
			},
		},
		{
			name: "Forget",
			call: func() error {
				_, err := cache.Forget("key")
				return err
			},
		},
		{
			name: "Touch",
			call: func() error {
				_, err := cache.Touch("key", time.Minute)
				return err
			},
		},
		{
			name: "Flush",
			call: func() error {
				_, err := cache.Flush()
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); !errors.Is(err, ErrClosed) {
				t.Fatalf("error = %v, want ErrClosed", err)
			}
		})
	}
	if got := l1.getCount(); got != 0 {
		t.Fatalf("L1 Get() calls after Close() = %d, want 0", got)
	}
	if got := l2.getCount(); got != 0 {
		t.Fatalf("L2 Get() calls after Close() = %d, want 0", got)
	}
}

func TestTieredCacheCloseWaitsForActiveGet(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l1.values["key"] = []byte("value")
	l1.ttls["key"] = time.Minute
	getStarted := make(chan struct{})
	releaseGet := make(chan struct{})
	l1.getHook = func() {
		close(getStarted)
		<-releaseGet
	}

	cache := newTestTieredCache(t, Config{}, l1, l2)
	getDone := make(chan struct {
		value []byte
		ttl   time.Duration
		err   error
	}, 1)
	go func() {
		value, ttl, err := cache.Get("key")
		getDone <- struct {
			value []byte
			ttl   time.Duration
			err   error
		}{value: value, ttl: ttl, err: err}
	}()

	select {
	case <-getStarted:
	case <-time.After(time.Second):
		t.Fatal("Get() did not reach L1")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- cache.Close()
	}()
	select {
	case err := <-closeDone:
		t.Fatalf("Close() returned before active Get(): %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseGet)
	select {
	case result := <-getDone:
		if string(result.value) != "value" || result.ttl != time.Minute || result.err != nil {
			t.Fatalf("Get() = (%q, %v, %v), want (value, %v, nil)", result.value, result.ttl, result.err, time.Minute)
		}
	case <-time.After(time.Second):
		t.Fatal("Get() did not finish")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close() did not finish after Get()")
	}
}

func TestTieredCacheEnterHealthySerializesStateCleanup(t *testing.T) {
	cache := &TieredCache{}
	oldErr := errors.New("old L2 error")
	oldFlushDone := make(chan struct{})
	cache.healthState.Store(uint32(degraded))
	cache.stateErr = oldErr
	cache.degradedFlushDone = oldFlushDone

	cache.stateErrMu.Lock()
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		cache.enterHealthy()
		close(done)
	}()
	<-started

	for i := 0; i < 1000; i++ {
		if healthState(cache.healthState.Load()) != degraded {
			t.Fatal("enterHealthy changed health before acquiring stateErrMu")
		}
		runtime.Gosched()
	}
	cache.stateErrMu.Unlock()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("enterHealthy did not finish")
	}
	if healthState(cache.healthState.Load()) != healthy {
		t.Fatal("health state = degraded, want healthy")
	}
	cache.stateErrMu.RLock()
	degradedFlushDone := cache.degradedFlushDone
	stateErr := cache.stateErr
	cache.stateErrMu.RUnlock()
	if stateErr != nil || degradedFlushDone != nil {
		t.Fatalf("state after enterHealthy = (err %v, flush %v), want (nil, nil)", stateErr, degradedFlushDone)
	}
}

func newTestTieredCache(t *testing.T, config Config, l1, l2 *fakeCache) *TieredCache {
	t.Helper()

	cache, err := New(config, l1, l2)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		_ = cache.Close()
	})
	return cache
}

type fakeCache struct {
	mu sync.Mutex

	values map[string][]byte
	ttls   map[string]time.Duration

	getCalls    int
	getHook     func()
	getErr      error
	setErr      error
	forgetErr   error
	touchErr    error
	flushErr    error
	forgetCalls int
	flushCalls  int

	setStarted chan struct{}
	releaseSet chan struct{}

	closeCalls int
}

func newFakeCache() *fakeCache {
	return &fakeCache{
		values: make(map[string][]byte),
		ttls:   make(map[string]time.Duration),
	}
}

func (f *fakeCache) Get(key string) ([]byte, time.Duration, error) {
	f.mu.Lock()
	f.getCalls++
	getErr := f.getErr
	if getErr != nil {
		f.mu.Unlock()
		return nil, 0, getErr
	}
	value, ok := f.values[key]
	if !ok {
		f.mu.Unlock()
		return nil, 0, nil
	}
	valueCopy := append([]byte(nil), value...)
	ttl := f.ttls[key]
	getHook := f.getHook
	f.mu.Unlock()

	if getHook != nil {
		getHook()
	}
	return valueCopy, ttl, nil
}

func (f *fakeCache) Set(key string, keyValue []byte, ttl time.Duration) (bool, error) {
	return f.set(key, keyValue, ttl, false)
}

func (f *fakeCache) Forever(key string, keyValue []byte) (bool, error) {
	return f.set(key, keyValue, 0, true)
}

func (f *fakeCache) setErrors(getErr, setErr error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getErr = getErr
	f.setErr = setErr
}

func (f *fakeCache) set(key string, keyValue []byte, ttl time.Duration, forever bool) (bool, error) {
	f.mu.Lock()
	err := f.setErr
	started := f.setStarted
	release := f.releaseSet
	f.mu.Unlock()

	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if release != nil {
		<-release
	}
	if err != nil {
		return false, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.values[key] = append([]byte(nil), keyValue...)
	if forever {
		f.ttls[key] = 0
	} else {
		f.ttls[key] = ttl
	}
	return true, nil
}

func (f *fakeCache) Forget(key string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forgetCalls++
	if f.forgetErr != nil {
		return false, f.forgetErr
	}
	if _, ok := f.values[key]; !ok {
		return false, nil
	}
	delete(f.values, key)
	delete(f.ttls, key)
	return true, nil
}

func (f *fakeCache) Touch(key string, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.touchErr != nil {
		return false, f.touchErr
	}
	if _, ok := f.values[key]; !ok {
		return false, nil
	}
	f.ttls[key] = ttl
	return true, nil
}

func (f *fakeCache) Flush() (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.flushCalls++
	if f.flushErr != nil {
		return false, f.flushErr
	}
	f.values = make(map[string][]byte)
	f.ttls = make(map[string]time.Duration)
	return true, nil
}

func (f *fakeCache) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls++
	return nil
}

func (f *fakeCache) getCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getCalls
}

func (f *fakeCache) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closeCalls
}

func (f *fakeCache) setForgetError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forgetErr = err
}

func (f *fakeCache) setFlushError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.flushErr = err
}

func (f *fakeCache) forgetCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.forgetCalls
}

func (f *fakeCache) flushCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.flushCalls
}
