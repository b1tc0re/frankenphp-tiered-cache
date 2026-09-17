package tiered

import (
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	cachecontract "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache"
)

const (
	maxInt64 = int64(^uint64(0) >> 1)
	minInt64 = -maxInt64 - 1
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
	l2.put("key", []byte("value"), time.Second)
	l2.getDelay = 50 * time.Millisecond
	cache := newTestTieredCache(t, Config{}, l1, l2)

	value, ttl, err := cache.Get("key")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(value) != "value" || ttl <= 0 || ttl >= time.Second-25*time.Millisecond {
		t.Fatalf("Get() = (%q, %v), want (value, reduced positive TTL)", value, ttl)
	}
	_, l1TTL, err := l1.Get("key")
	if err != nil {
		t.Fatalf("L1 Get() error = %v", err)
	}
	if l1TTL != ttl {
		t.Fatalf("warmed L1 TTL = %v, Get() TTL = %v; want equal remaining TTLs", l1TTL, ttl)
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

func TestTieredCacheAddUpdatesBothLevelsAfterRedisCommit(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	cache := newTestTieredCache(t, Config{}, l1, l2)

	added, err := cache.Add("key", []byte("value"), time.Minute)
	if err != nil || !added {
		t.Fatalf("Add() = (%t, %v), want (true, nil)", added, err)
	}
	if value, ttl, err := l2.Get("key"); err != nil || string(value) != "value" || ttl != time.Minute {
		t.Fatalf("L2 after Add() = (%q, %v, %v), want (value, %v, nil)", value, ttl, err, time.Minute)
	}
	if value, ttl, err := l1.Get("key"); err != nil || string(value) != "value" || ttl <= 0 || ttl > time.Minute {
		t.Fatalf("L1 after Add() = (%q, %v, %v), want value with remaining TTL", value, ttl, err)
	}
}

func TestTieredCacheRejectedAddDoesNotChangeL1OrMutationVersion(t *testing.T) {
	l1 := newFakeCache()
	l1.put("key", []byte("old"), time.Minute)
	l2 := newFakeCache()
	l2.put("key", []byte("old"), time.Minute)
	cache := newTestTieredCache(t, Config{}, l1, l2)
	version := cache.mutationVersion.Load()

	added, err := cache.Add("key", []byte("new"), 2*time.Minute)
	if err != nil || added {
		t.Fatalf("Add(existing) = (%t, %v), want (false, nil)", added, err)
	}
	if got := cache.mutationVersion.Load(); got != version {
		t.Fatalf("mutationVersion after rejected Add() = %d, want %d", got, version)
	}
	if got := l1.setCount(); got != 0 {
		t.Fatalf("L1 Set() calls after rejected Add() = %d, want 0", got)
	}
	if value, _, err := l1.Get("key"); err != nil || string(value) != "old" {
		t.Fatalf("L1 after rejected Add() = (%q, %v), want old value", value, err)
	}
}

func TestTieredCacheAddUsesRemainingTTLForL1(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.addDelay = 50 * time.Millisecond
	cache := newTestTieredCache(t, Config{}, l1, l2)

	const ttl = time.Second
	added, err := cache.Add("key", []byte("value"), ttl)
	if err != nil || !added {
		t.Fatalf("Add() = (%t, %v), want (true, nil)", added, err)
	}
	_, l1TTL, err := l1.Get("key")
	if err != nil {
		t.Fatalf("L1 Get() error = %v", err)
	}
	if l1TTL <= 0 || l1TTL >= ttl-25*time.Millisecond {
		t.Fatalf("L1 TTL after delayed Add() = %v, want reduced positive TTL", l1TTL)
	}
}

func TestTieredCacheAddRedisFailureDegradesWithoutChangingL1(t *testing.T) {
	l1 := newFakeCache()
	l1.put("key", []byte("old"), time.Minute)
	l2 := newFakeCache()
	wantErr := errors.New("Redis unavailable")
	l2.addErr = wantErr
	cache := newTestTieredCache(t, Config{}, l1, l2)

	added, err := cache.Add("key", []byte("new"), time.Minute)
	if added || !errors.Is(err, ErrL2Unavailable) || !errors.Is(err, wantErr) {
		t.Fatalf("Add() = (%t, %v), want false and unavailable Redis error", added, err)
	}
	if value, _, getErr := l1.Get("key"); getErr != nil || string(value) != "old" {
		t.Fatalf("L1 after Redis Add() failure = (%q, %v), want old value", value, getErr)
	}
}

func TestTieredCacheAddReturnsCommittedResultOnL1Failure(t *testing.T) {
	l1 := newFakeCache()
	l1.setErr = errors.New("L1 unavailable")
	l2 := newFakeCache()
	cache := newTestTieredCache(t, Config{}, l1, l2)

	added, err := cache.Add("key", []byte("value"), time.Minute)
	if !added || !errors.Is(err, ErrPostCommit) || !errors.Is(err, l1.setErr) {
		t.Fatalf("Add() = (%t, %v), want true and post-commit L1 error", added, err)
	}
	if value, _, getErr := l2.Get("key"); getErr != nil || string(value) != "value" {
		t.Fatalf("L2 after L1 Add() failure = (%q, %v), want value", value, getErr)
	}
}

func TestTieredCacheSetManyUpdatesBothLevelsAndVersionOnce(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	cache := newTestTieredCache(t, Config{}, l1, l2)
	values := map[string][]byte{
		"first":  []byte("one"),
		"second": []byte("two"),
		"third":  []byte("three"),
	}
	version := cache.mutationVersion.Load()

	stored, err := cache.SetMany(values, time.Minute)
	if !stored || err != nil {
		t.Fatalf("SetMany() = (%t, %v), want (true, nil)", stored, err)
	}
	if got := cache.mutationVersion.Load(); got != version+1 {
		t.Fatalf("mutationVersion after SetMany() = %d, want %d", got, version+1)
	}
	if l2.setManyCalls != 1 || l2.setCalls != 0 {
		t.Fatalf("L2 calls = (SetMany=%d, Set=%d), want (1, 0)", l2.setManyCalls, l2.setCalls)
	}
	for key, want := range values {
		for name, backend := range map[string]*fakeCache{"L1": l1, "L2": l2} {
			value, ttl, getErr := backend.Get(key)
			if getErr != nil || string(value) != string(want) || ttl <= 0 {
				t.Errorf("%s Get(%q) = (%q, %v, %v), want value with TTL", name, key, value, ttl, getErr)
			}
		}
	}
}

func TestTieredCacheSetManyUsesRemainingTTLForL1(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.setManyDelay = 50 * time.Millisecond
	cache := newTestTieredCache(t, Config{}, l1, l2)

	const ttl = time.Second
	stored, err := cache.SetMany(map[string][]byte{"key": []byte("value")}, ttl)
	if !stored || err != nil {
		t.Fatalf("SetMany() = (%t, %v), want (true, nil)", stored, err)
	}
	_, l1TTL, err := l1.Get("key")
	if err != nil {
		t.Fatalf("L1 Get() error = %v", err)
	}
	if l1TTL <= 0 || l1TTL >= ttl-25*time.Millisecond {
		t.Fatalf("L1 TTL after delayed SetMany() = %v, want reduced positive TTL", l1TTL)
	}
}

func TestTieredCacheSetManyRedisFailureDoesNotChangeL1(t *testing.T) {
	l1 := newFakeCache()
	l1.put("key", []byte("old"), time.Minute)
	l2 := newFakeCache()
	wantErr := errors.New("Redis unavailable")
	l2.setManyErr = wantErr
	cache := newTestTieredCache(t, Config{}, l1, l2)

	stored, err := cache.SetMany(map[string][]byte{"key": []byte("new")}, time.Minute)
	if stored || !errors.Is(err, ErrL2Unavailable) || !errors.Is(err, wantErr) {
		t.Fatalf("SetMany() = (%t, %v), want false and unavailable Redis error", stored, err)
	}
	if value, _, getErr := l1.Get("key"); getErr != nil || string(value) != "old" {
		t.Fatalf("L1 after Redis SetMany() failure = (%q, %v), want old value", value, getErr)
	}
}

func TestTieredCacheSetManyReturnsCommittedResultOnL1Failure(t *testing.T) {
	l1 := newFakeCache()
	l1.setErr = errors.New("L1 unavailable")
	l2 := newFakeCache()
	cache := newTestTieredCache(t, Config{}, l1, l2)

	stored, err := cache.SetMany(map[string][]byte{
		"first":  []byte("one"),
		"second": []byte("two"),
	}, time.Minute)
	if !stored || !errors.Is(err, ErrPostCommit) || !errors.Is(err, l1.setErr) {
		t.Fatalf("SetMany() = (%t, %v), want true and post-commit L1 error", stored, err)
	}
	for key, want := range map[string]string{"first": "one", "second": "two"} {
		value, _, getErr := l2.Get(key)
		if getErr != nil || string(value) != want {
			t.Errorf("L2 Get(%q) = (%q, %v), want committed value", key, value, getErr)
		}
	}
}

func TestTieredCacheSetManyEmptyBatchDoesNotTouchL2(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	cache := newTestTieredCache(t, Config{}, l1, l2)
	version := cache.mutationVersion.Load()

	stored, err := cache.SetMany(nil, time.Minute)
	if stored || err != nil {
		t.Fatalf("SetMany(empty) = (%t, %v), want (false, nil)", stored, err)
	}
	if l2.setManyCalls != 0 {
		t.Fatalf("L2 SetMany() calls for empty batch = %d, want 0", l2.setManyCalls)
	}
	if got := cache.mutationVersion.Load(); got != version {
		t.Fatalf("mutationVersion after empty SetMany() = %d, want %d", got, version)
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

func TestTieredCacheL1CleanupFailureDegradesAfterCommittedMutation(t *testing.T) {
	l1 := newFakeCache()
	l1.put("key", []byte("old"), time.Minute)
	l1.setErr = errors.New("L1 unavailable")
	l1.flushErr = errors.New("L1 flush unavailable")
	l2 := newFakeCache()
	l2.put("key", []byte("old"), time.Minute)
	cache := newTestTieredCache(t, Config{}, l1, l2)

	ok, err := cache.Set("key", []byte("new"), time.Minute)
	if !ok || !errors.Is(err, ErrL2Unavailable) || !errors.Is(err, l1.setErr) || !errors.Is(err, l1.flushErr) {
		t.Fatalf("Set() = (%t, %v), want committed value with degraded error", ok, err)
	}
	if healthErr := cache.unavailableError(); healthErr == nil {
		t.Fatal("cache health after failed L1 cleanup = healthy, want degraded")
	}
	if value, _, getErr := l1.Get("key"); getErr != nil || string(value) != "old" {
		t.Fatalf("L1 after failed cleanup = (%q, %v), want stale old value retained but inaccessible through TieredCache", value, getErr)
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

func TestTieredCacheIncrementAndDecrementUseRedisAndForgetL1(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l1.put("counter", []byte("10"), time.Minute)
	l2.put("counter", []byte("10"), time.Minute)
	cache := newTestTieredCache(t, Config{}, l1, l2)

	if got, err := cache.Increment("counter", 2); err != nil || got != 12 {
		t.Fatalf("Increment() = (%d, %v), want (12, nil)", got, err)
	}
	if value, _, err := l1.Get("counter"); err != nil || value != nil {
		t.Fatalf("L1 after Increment() = (%q, %v), want (nil, nil)", value, err)
	}
	if value, ttl, err := l2.Get("counter"); err != nil || string(value) != "12" || ttl != time.Minute {
		t.Fatalf("L2 after Increment() = (%q, %v, %v), want (12, %v, nil)", value, ttl, err, time.Minute)
	}

	if got, err := cache.Decrement("counter", 5); err != nil || got != 7 {
		t.Fatalf("Decrement() = (%d, %v), want (7, nil)", got, err)
	}
	if value, _, err := l1.Get("counter"); err != nil || value != nil {
		t.Fatalf("L1 after Decrement() = (%q, %v), want (nil, nil)", value, err)
	}
}

func TestTieredCacheCounterReturnsCommittedValueWhenL1InvalidationFails(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l1.put("counter", []byte("10"), time.Minute)
	l2.put("counter", []byte("10"), time.Minute)
	wantErr := errors.New("L1 unavailable")
	l1.forgetErr = wantErr
	cache := newTestTieredCache(t, Config{}, l1, l2)

	got, err := cache.Increment("counter", 1)
	if got != 11 || !errors.Is(err, ErrPostCommit) || !errors.Is(err, wantErr) {
		t.Fatalf("Increment() = (%d, %v), want committed value 11 and post-commit L1 error", got, err)
	}
	if value, _, getErr := l2.Get("counter"); getErr != nil || string(value) != "11" {
		t.Fatalf("L2 after failed L1 invalidation = (%q, %v), want (11, nil)", value, getErr)
	}
}

func TestTieredCacheCommittedZeroCounterIsPostCommit(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l1.put("counter", []byte("1"), time.Minute)
	l2.put("counter", []byte("1"), time.Minute)
	wantErr := errors.New("L1 unavailable")
	l1.forgetErr = wantErr
	cache := newTestTieredCache(t, Config{}, l1, l2)

	got, err := cache.Decrement("counter", 1)
	if got != 0 || !errors.Is(err, ErrPostCommit) || !errors.Is(err, wantErr) {
		t.Fatalf("Decrement() = (%d, %v), want committed zero and post-commit L1 error", got, err)
	}
	if value, _, getErr := l2.Get("counter"); getErr != nil || string(value) != "0" {
		t.Fatalf("L2 after Decrement() = (%q, %v), want (0, nil)", value, getErr)
	}
}

func TestTieredCacheFlushFailureDegradesAfterCommittedRedisFlush(t *testing.T) {
	l1 := newFakeCache()
	l1.put("key", []byte("stale"), time.Minute)
	wantErr := errors.New("L1 flush unavailable")
	l1.flushErr = wantErr
	l2 := newFakeCache()
	l2.put("key", []byte("value"), time.Minute)
	cache := newTestTieredCache(t, Config{}, l1, l2)

	flushed, err := cache.Flush()
	if flushed || !errors.Is(err, wantErr) || !errors.Is(err, ErrL2Unavailable) {
		t.Fatalf("Flush() = (%t, %v), want false and degraded L1 error", flushed, err)
	}
	if healthErr := cache.unavailableError(); healthErr == nil {
		t.Fatal("cache health after failed L1 flush = healthy, want degraded")
	}
	if value, _, getErr := l1.Get("key"); getErr != nil || string(value) != "stale" {
		t.Fatalf("L1 after failed Flush() = (%q, %v), want stale value retained but inaccessible through TieredCache", value, getErr)
	}
}

func TestTieredCacheRedisCommandErrorDoesNotDegrade(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	l1.put("counter", []byte("abc"), time.Minute)
	l2.incrErr = cachecontract.ErrRedisCommand
	cache := newTestTieredCache(t, Config{}, l1, l2)

	if got, err := cache.Increment("counter", 1); got != 0 || !errors.Is(err, cachecontract.ErrRedisCommand) {
		t.Fatalf("Increment() = (%d, %v), want (0, ErrRedisCommand)", got, err)
	}
	if err := cache.unavailableError(); err != nil {
		t.Fatalf("cache health after Redis command error = %v, want healthy", err)
	}
	if value, _, err := l1.Get("counter"); err != nil || string(value) != "abc" {
		t.Fatalf("L1 after Redis command error = (%q, %v), want (abc, nil)", value, err)
	}
}

func TestTieredCacheCounterTransportErrorDegrades(t *testing.T) {
	l1 := newFakeCache()
	l2 := newFakeCache()
	wantErr := errors.New("Redis unavailable")
	l2.incrErr = wantErr
	cache := newTestTieredCache(t, Config{}, l1, l2)

	if got, err := cache.Increment("counter", 1); got != 0 || !errors.Is(err, ErrL2Unavailable) || !errors.Is(err, wantErr) {
		t.Fatalf("Increment() = (%d, %v), want unavailable wrapping transport error", got, err)
	}
	if err := cache.unavailableError(); err == nil {
		t.Fatal("cache health after transport error = healthy, want degraded")
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
	if got, err := cache.Increment("key", 1); got != 0 || !errors.Is(err, ErrClosed) {
		t.Fatalf("Increment() = (%d, %v), want (0, ErrClosed)", got, err)
	}
	if got, err := cache.Decrement("key", 1); got != 0 || !errors.Is(err, ErrClosed) {
		t.Fatalf("Decrement() = (%d, %v), want (0, ErrClosed)", got, err)
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

	getErr       error
	setErr       error
	addErr       error
	setManyErr   error
	incrErr      error
	decrErr      error
	forgetErr    error
	touchErr     error
	flushErr     error
	closeErr     error
	getDelay     time.Duration
	setDelay     time.Duration
	addDelay     time.Duration
	setManyDelay time.Duration
	touchDelay   time.Duration

	getStarted   chan struct{}
	releaseGet   chan struct{}
	setStarted   chan struct{}
	releaseSet   chan struct{}
	setHook      func()
	getCalls     int
	setCalls     int
	setManyCalls int
	forgetCalls  int
	flushCalls   int
	closeCalls   int
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
	delay := f.getDelay
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
	if delay > 0 {
		time.Sleep(delay)
	}
	if err != nil {
		return nil, 0, err
	}
	if value == nil {
		return nil, 0, nil
	}
	return value, ttl, nil
}

func (f *fakeCache) Add(key string, value []byte, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.addDelay > 0 {
		time.Sleep(f.addDelay)
	}
	if f.addErr != nil {
		return false, f.addErr
	}
	if _, exists := f.values[key]; exists {
		return false, nil
	}
	f.values[key] = append([]byte(nil), value...)
	f.ttls[key] = ttl
	return true, nil
}

func (f *fakeCache) Set(key string, value []byte, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	f.setCalls++
	err := f.setErr
	started := f.setStarted
	release := f.releaseSet
	hook := f.setHook
	delay := f.setDelay
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
	if delay > 0 {
		time.Sleep(delay)
	}
	if err != nil {
		return false, err
	}
	f.put(key, value, ttl)
	return true, nil
}

func (f *fakeCache) SetMany(values map[string][]byte, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	f.setManyCalls++
	err := f.setManyErr
	delay := f.setManyDelay
	f.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}
	if err != nil {
		return false, err
	}
	for key, value := range values {
		f.put(key, value, ttl)
	}
	return true, nil
}

func (f *fakeCache) Forever(key string, value []byte) (bool, error) {
	return f.Set(key, value, 0)
}

func (f *fakeCache) Increment(key string, value int64) (int64, error) {
	return f.changeCounter(key, value, false)
}

func (f *fakeCache) Decrement(key string, value int64) (int64, error) {
	return f.changeCounter(key, value, true)
}

func (f *fakeCache) changeCounter(key string, value int64, decrement bool) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if decrement {
		if f.decrErr != nil {
			return 0, f.decrErr
		}
		if value == minInt64 {
			return 0, errors.New("counter overflow")
		}
		value = -value
	} else if f.incrErr != nil {
		return 0, f.incrErr
	}

	var current int64
	if raw, ok := f.values[key]; ok {
		parsed, err := strconv.ParseInt(string(raw), 10, 64)
		if err != nil {
			return 0, err
		}
		current = parsed
	}
	if (value > 0 && current > maxInt64-value) || (value < 0 && current < minInt64-value) {
		return 0, errors.New("counter overflow")
	}
	current += value
	f.values[key] = []byte(strconv.FormatInt(current, 10))
	return current, nil
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
	err := f.touchErr
	_, exists := f.values[key]
	delay := f.touchDelay
	f.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()
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
