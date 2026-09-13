package tiered

import (
	"errors"
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
	l2.setStarted = make(chan struct{}, 1)
	l2.releaseSet = make(chan struct{})
	cache := newTestTieredCache(t, Config{
		WriteQueueCapacity:    1,
		WriteQueueWaitTimeout: 20 * time.Millisecond,
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

	getCalls int
	getHook  func()
	getErr   error
	setErr   error

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
	if _, ok := f.values[key]; !ok {
		return false, nil
	}
	f.ttls[key] = ttl
	return true, nil
}

func (f *fakeCache) Flush() (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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
