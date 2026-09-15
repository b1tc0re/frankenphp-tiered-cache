package tiered

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestTieredCacheGetChecksL1BeforeL2(t *testing.T) {
	l1 := newFakeCache()
	l1.put("key", []byte("l1"), time.Minute)
	l2 := newFakeCache()
	l2.getErr = errors.New("L2 must not be called")
	cache := newTestTieredCache(t, Config{}, l1, l2)

	value, ttl, err := cache.Get("key")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(value) != "l1" || ttl != time.Minute {
		t.Fatalf("Get() = (%q, %v), want (l1, %v)", value, ttl, time.Minute)
	}
	if got := l2.getCount(); got != 0 {
		t.Fatalf("L2 Get() calls = %d, want 0", got)
	}
}

func TestTieredCacheGetWarmsL1WithRemainingTTL(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.put("key", []byte("value"), 42*time.Second)
	cache := newTestTieredCache(t, Config{}, l1, l2)

	value, ttl, err := cache.Get("key")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(value) != "value" || ttl != 42*time.Second {
		t.Fatalf("Get() = (%q, %v), want (value, %v)", value, ttl, 42*time.Second)
	}
	_, l1TTL, err := l1.Get("key")
	if err != nil {
		t.Fatalf("L1 Get() error = %v", err)
	}
	if l1TTL != 42*time.Second {
		t.Fatalf("warmed L1 TTL = %v, want %v", l1TTL, 42*time.Second)
	}
}

func TestTieredCacheGetWarmsL1ForeverValue(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.put("key", []byte("value"), 0)
	cache := newTestTieredCache(t, Config{}, l1, l2)

	if value, ttl, err := cache.Get("key"); err != nil || string(value) != "value" || ttl != 0 {
		t.Fatalf("Get() = (%q, %v, %v), want (value, 0, nil)", value, ttl, err)
	}
	_, l1TTL, err := l1.Get("key")
	if err != nil {
		t.Fatalf("L1 Get() error = %v", err)
	}
	if l1TTL != 0 {
		t.Fatalf("warmed L1 TTL = %v, want 0", l1TTL)
	}
}

func TestTieredCacheGetDoesNotWarmStaleL2AfterLocalMutation(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.put("key", []byte("old"), time.Minute)
	l2.getStarted = make(chan struct{}, 1)
	l2.releaseGet = make(chan struct{})
	cache := newTestTieredCache(t, Config{}, l1, l2)

	result := make(chan struct {
		value []byte
		err   error
	}, 1)
	go func() {
		value, _, err := cache.Get("key")
		result <- struct {
			value []byte
			err   error
		}{value: value, err: err}
	}()

	select {
	case <-l2.getStarted:
	case <-time.After(time.Second):
		t.Fatal("L2 Get() did not start")
	}
	if ok, err := cache.Set("key", []byte("new"), time.Minute); err != nil || !ok {
		t.Fatalf("Set() = (%t, %v), want (true, nil)", ok, err)
	}
	close(l2.releaseGet)

	select {
	case got := <-result:
		if got.err != nil || string(got.value) != "new" {
			t.Fatalf("Get() = (%q, %v), want (new, nil)", got.value, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("Get() did not finish")
	}
	if value, _, err := l1.Get("key"); err != nil || string(value) != "new" {
		t.Fatalf("L1 after race = (%q, %v), want (new, nil)", value, err)
	}
}

func TestTieredCacheSetIsSynchronousAndWritesRedisBeforeL1(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.setStarted = make(chan struct{}, 1)
	l2.releaseSet = make(chan struct{})
	var orderMu sync.Mutex
	var order []string
	l2.setHook = func() {
		orderMu.Lock()
		order = append(order, "redis")
		orderMu.Unlock()
	}
	l1.setHook = func() {
		orderMu.Lock()
		order = append(order, "l1")
		orderMu.Unlock()
	}
	cache := newTestTieredCache(t, Config{}, l1, l2)

	result := make(chan struct {
		ok  bool
		err error
	}, 1)
	go func() {
		ok, err := cache.Set("key", []byte("value"), time.Minute)
		result <- struct {
			ok  bool
			err error
		}{ok: ok, err: err}
	}()

	select {
	case <-l2.setStarted:
	case <-time.After(time.Second):
		t.Fatal("Redis Set() did not start")
	}
	select {
	case got := <-result:
		t.Fatalf("Set() returned before Redis completed: (%t, %v)", got.ok, got.err)
	case <-time.After(20 * time.Millisecond):
	}
	close(l2.releaseSet)

	select {
	case got := <-result:
		if got.err != nil || !got.ok {
			t.Fatalf("Set() = (%t, %v), want (true, nil)", got.ok, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("Set() did not finish")
	}
	orderMu.Lock()
	defer orderMu.Unlock()
	if got, want := order, []string{"redis", "l1"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("mutation order = %v, want %v", got, want)
	}
}

func TestTieredCacheRedisSetErrorDoesNotChangeL1(t *testing.T) {
	l1 := newFakeCache()
	l1.put("key", []byte("old"), time.Minute)
	l2 := newFakeCache()
	wantErr := errors.New("Redis unavailable")
	l2.setErr = wantErr
	cache := newTestTieredCache(t, Config{}, l1, l2)

	ok, err := cache.Set("key", []byte("new"), time.Minute)
	if ok || !errors.Is(err, ErrL2Unavailable) || !errors.Is(err, wantErr) {
		t.Fatalf("Set() = (%t, %v), want (false, unavailable wrapping Redis error)", ok, err)
	}
	value, _, getErr := l1.Get("key")
	if getErr != nil || string(value) != "old" {
		t.Fatalf("L1 after Redis failure = (%q, %v), want (old, nil)", value, getErr)
	}
	if got := l1.setCount(); got != 0 {
		t.Fatalf("L1 Set() calls = %d, want 0", got)
	}
}

func TestTieredCacheRedisFailureRecoveryClearsL1BeforeHealthy(t *testing.T) {
	l1 := newFakeCache()
	l1.put("key", []byte("old"), time.Minute)
	l2 := newFakeCache()
	l2.setErr = errors.New("Redis unavailable")
	cache := newTestTieredCache(t, Config{RecoveryInterval: 5 * time.Millisecond}, l1, l2)

	if ok, err := cache.Set("key", []byte("new"), time.Minute); ok || err == nil {
		t.Fatalf("Set() = (%t, %v), want false and error", ok, err)
	}
	l2.mu.Lock()
	l2.setErr = nil
	l2.mu.Unlock()
	waitForTieredCondition(t, func() bool {
		return cache.unavailableError() == nil
	})
	if value, _, err := l1.Get("key"); err != nil || value != nil {
		t.Fatalf("L1 after recovery = (%q, %v), want (nil, nil)", value, err)
	}
}

func TestTieredCacheCommittedRedisValueSurvivesL1Failure(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	wantErr := errors.New("L1 unavailable")
	l1.setErr = wantErr
	cache := newTestTieredCache(t, Config{}, l1, l2)

	ok, err := cache.Set("key", []byte("value"), time.Minute)
	if !ok || !errors.Is(err, wantErr) {
		t.Fatalf("Set() = (%t, %v), want (true, L1 error)", ok, err)
	}
	value, _, getErr := l2.Get("key")
	if getErr != nil || string(value) != "value" {
		t.Fatalf("L2 after L1 failure = (%q, %v), want (value, nil)", value, getErr)
	}
}

func TestTieredCacheSynchronousMutationsUseBothLevels(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	cache := newTestTieredCache(t, Config{}, l1, l2)

	if ok, err := cache.Forever("key", []byte("value")); err != nil || !ok {
		t.Fatalf("Forever() = (%t, %v), want (true, nil)", ok, err)
	}
	if ok, err := cache.Touch("key", time.Minute); err != nil || !ok {
		t.Fatalf("Touch() = (%t, %v), want (true, nil)", ok, err)
	}
	if _, ttl, err := l2.Get("key"); err != nil || ttl != time.Minute {
		t.Fatalf("L2 TTL after Touch() = %v, %v; want %v, nil", ttl, err, time.Minute)
	}
	if removed, err := cache.Forget("key"); err != nil || !removed {
		t.Fatalf("Forget() = (%t, %v), want (true, nil)", removed, err)
	}
	if value, _, err := l1.Get("key"); err != nil || value != nil {
		t.Fatalf("L1 after Forget() = (%q, %v), want (nil, nil)", value, err)
	}
	if value, _, err := l2.Get("key"); err != nil || value != nil {
		t.Fatalf("L2 after Forget() = (%q, %v), want (nil, nil)", value, err)
	}
}

func TestTieredCacheCloseOperationsReturnErrClosed(t *testing.T) {
	cache := newTestTieredCache(t, Config{}, newFakeCache(), newFakeCache())
	if err := cache.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if _, _, err := cache.Get("key"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get() error = %v, want ErrClosed", err)
	}
	if ok, err := cache.Set("key", []byte("value"), time.Minute); ok || !errors.Is(err, ErrClosed) {
		t.Fatalf("Set() = (%t, %v), want (false, ErrClosed)", ok, err)
	}
	if ok, err := cache.Forever("key", []byte("value")); ok || !errors.Is(err, ErrClosed) {
		t.Fatalf("Forever() = (%t, %v), want (false, ErrClosed)", ok, err)
	}
	if ok, err := cache.Forget("key"); ok || !errors.Is(err, ErrClosed) {
		t.Fatalf("Forget() = (%t, %v), want (false, ErrClosed)", ok, err)
	}
	if ok, err := cache.Touch("key", time.Minute); ok || !errors.Is(err, ErrClosed) {
		t.Fatalf("Touch() = (%t, %v), want (false, ErrClosed)", ok, err)
	}
	if ok, err := cache.Flush(); ok || !errors.Is(err, ErrClosed) {
		t.Fatalf("Flush() = (%t, %v), want (false, ErrClosed)", ok, err)
	}
}

func TestTieredCacheRejectsInvalidConfigAndBackends(t *testing.T) {
	if _, err := New(Config{RecoveryInterval: -time.Second}, newFakeCache(), newFakeCache()); err == nil {
		t.Fatal("New() error = nil, want invalid recovery interval error")
	}
	if _, err := New(Config{}, nil, newFakeCache()); err == nil {
		t.Fatal("New() error = nil, want nil L1 error")
	}
	if _, err := New(Config{}, newFakeCache(), nil); err == nil {
		t.Fatal("New() error = nil, want nil L2 error")
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

func waitForTieredCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition was not satisfied")
		}
		time.Sleep(time.Millisecond)
	}
}

type fakeCache struct {
	mu sync.Mutex

	values map[string][]byte
	ttls   map[string]time.Duration

	getErr    error
	setErr    error
	forgetErr error
	touchErr  error
	flushErr  error
	closeErr  error

	getStarted  chan struct{}
	releaseGet  chan struct{}
	setStarted  chan struct{}
	releaseSet  chan struct{}
	setHook     func()
	getCalls    int
	setCalls    int
	forgetCalls int
	flushCalls  int
	closeCalls  int
}

func newFakeCache() *fakeCache {
	return &fakeCache{
		values: make(map[string][]byte),
		ttls:   make(map[string]time.Duration),
	}
}

func (f *fakeCache) put(key string, value []byte, ttl time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.values[key] = append([]byte(nil), value...)
	f.ttls[key] = ttl
}

func (f *fakeCache) Get(key string) ([]byte, time.Duration, error) {
	f.mu.Lock()
	f.getCalls++
	err := f.getErr
	value := append([]byte(nil), f.values[key]...)
	ttl := f.ttls[key]
	started := f.getStarted
	release := f.releaseGet
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
		return nil, 0, err
	}
	if value == nil {
		return nil, 0, nil
	}
	return value, ttl, nil
}

func (f *fakeCache) Set(key string, value []byte, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	f.setCalls++
	err := f.setErr
	started := f.setStarted
	release := f.releaseSet
	hook := f.setHook
	f.mu.Unlock()

	if hook != nil {
		hook()
	}
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
	f.put(key, value, ttl)
	return true, nil
}

func (f *fakeCache) Forever(key string, value []byte) (bool, error) {
	return f.Set(key, value, 0)
}

func (f *fakeCache) Forget(key string) (bool, error) {
	f.mu.Lock()
	f.forgetCalls++
	err := f.forgetErr
	if err != nil {
		f.mu.Unlock()
		return false, err
	}
	_, exists := f.values[key]
	delete(f.values, key)
	delete(f.ttls, key)
	f.mu.Unlock()
	return exists, nil
}

func (f *fakeCache) Touch(key string, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.touchErr != nil {
		return false, f.touchErr
	}
	if _, exists := f.values[key]; !exists {
		return false, nil
	}
	f.ttls[key] = ttl
	return true, nil
}

func (f *fakeCache) Flush() (bool, error) {
	f.mu.Lock()
	f.flushCalls++
	defer f.mu.Unlock()
	if f.flushErr != nil {
		return false, f.flushErr
	}
	f.values = make(map[string][]byte)
	f.ttls = make(map[string]time.Duration)
	return true, nil
}

func (f *fakeCache) Close() error {
	f.mu.Lock()
	f.closeCalls++
	err := f.closeErr
	f.mu.Unlock()
	return err
}

func (f *fakeCache) getCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getCalls
}

func (f *fakeCache) setCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.setCalls
}

func (f *fakeCache) flushCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.flushCalls
}
