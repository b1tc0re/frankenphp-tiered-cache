package tiered

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	cacheinvalidation "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache/invalidation"
)

func TestTieredCacheSetInvalidatesWarmPeer(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1A, l1B, l2 := newFakeCache(), newFakeCache(), newFakeCache()
	l2.put("key", []byte("old"), time.Minute)
	cacheA := newTestTieredCacheWithInvalidation(t, l1A, l2, bus)
	cacheB := newTestTieredCacheWithInvalidation(t, l1B, l2, bus)
	waitForInvalidationCondition(t, func() bool {
		return cacheA.invalidationReady.Load() && cacheB.invalidationReady.Load()
	})
	warmCache(t, cacheA, "old")
	warmCache(t, cacheB, "old")

	if ok, err := cacheA.Set("key", []byte("new"), time.Minute); err != nil || !ok {
		t.Fatalf("Set() = (%t, %v), want (true, nil)", ok, err)
	}
	waitForInvalidationCondition(t, func() bool {
		value, _, err := l1B.Get("key")
		return err == nil && value == nil
	})
	if value, _, err := cacheB.Get("key"); err != nil || string(value) != "new" {
		t.Fatalf("peer Get() = (%q, %v), want (new, nil)", value, err)
	}
}

func TestTieredCacheSenderIgnoresOwnInvalidation(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1A, l1B, l2 := newFakeCache(), newFakeCache(), newFakeCache()
	cacheA := newTestTieredCacheWithInvalidation(t, l1A, l2, bus)
	cacheB := newTestTieredCacheWithInvalidation(t, l1B, l2, bus)
	waitForInvalidationCondition(t, func() bool {
		return cacheA.invalidationReady.Load() && cacheB.invalidationReady.Load()
	})

	if ok, err := cacheA.Set("key", []byte("value"), time.Minute); err != nil || !ok {
		t.Fatalf("Set() = (%t, %v), want (true, nil)", ok, err)
	}
	value, _, err := l1A.Get("key")
	if err != nil || string(value) != "value" {
		t.Fatalf("sender L1 = (%q, %v), want (value, nil)", value, err)
	}
	waitForInvalidationCondition(t, func() bool {
		value, _, err := l1B.Get("key")
		return err == nil && value == nil
	})
}

func TestTieredCacheDelayedInvalidationOnlyForgetsL1(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.put("key", []byte("new"), time.Minute)
	cache := newTestTieredCacheWithInvalidation(t, l1, l2, bus)
	waitForInvalidationCondition(t, func() bool { return cache.invalidationReady.Load() })
	warmCache(t, cache, "new")

	bus.delayOrigin = "remote-pod"
	bus.delayStarted = make(chan struct{}, 1)
	bus.delayRelease = make(chan struct{})
	if err := bus.Publish(context.Background(), cacheinvalidation.Event{
		Version: cacheinvalidation.ProtocolVersion,
		Type:    cacheinvalidation.EventTypeInvalidate,
		Key:     "key",
		Origin:  "remote-pod",
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	select {
	case <-bus.delayStarted:
	case <-time.After(time.Second):
		t.Fatal("invalidation was not delayed")
	}
	if value, _, err := l2.Get("key"); err != nil || string(value) != "new" {
		t.Fatalf("L2 before delivery = (%q, %v), want (new, nil)", value, err)
	}
	close(bus.delayRelease)
	waitForInvalidationCondition(t, func() bool {
		value, _, err := l1.Get("key")
		return err == nil && value == nil
	})
	if value, _, err := l2.Get("key"); err != nil || string(value) != "new" {
		t.Fatalf("L2 after delivery = (%q, %v), want (new, nil)", value, err)
	}
}

func TestTieredCacheRemoteInvalidationWaitsForLocalMutation(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.setStarted = make(chan struct{}, 1)
	l2.releaseSet = make(chan struct{})
	cache := newTestTieredCacheWithInvalidation(t, l1, l2, bus)
	waitForInvalidationCondition(t, func() bool { return cache.invalidationReady.Load() })

	result := make(chan error, 1)
	go func() {
		_, err := cache.Set("key", []byte("value"), time.Minute)
		result <- err
	}()
	select {
	case <-l2.setStarted:
	case <-time.After(time.Second):
		t.Fatal("Redis Set() did not start")
	}
	if err := bus.Publish(context.Background(), cacheinvalidation.Event{
		Version: cacheinvalidation.ProtocolVersion,
		Type:    cacheinvalidation.EventTypeInvalidate,
		Key:     "key",
		Origin:  "remote-pod",
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	close(l2.releaseSet)
	if err := <-result; err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	waitForInvalidationCondition(t, func() bool {
		value, _, err := l1.Get("key")
		return err == nil && value == nil
	})
}

func TestTieredCacheConcurrentSetsLeaveOnlyCommittedRedisValue(t *testing.T) {
	l2 := newFakeCache()
	cacheA := newTestTieredCache(t, Config{}, newFakeCache(), l2)
	cacheB := newTestTieredCache(t, Config{}, newFakeCache(), l2)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i, cache := range []*TieredCache{cacheA, cacheB} {
		cache := cache
		value := []byte("v1")
		if i == 1 {
			value = []byte("v2")
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := cache.Set("key", value, time.Minute)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Set() error = %v", err)
		}
	}
	value, _, err := l2.Get("key")
	if err != nil || (string(value) != "v1" && string(value) != "v2") {
		t.Fatalf("L2 after concurrent Set() = (%q, %v), want one of v1/v2", value, err)
	}
}

func TestTieredCacheInvalidationOriginsAreUnique(t *testing.T) {
	bus := newFakeInvalidationBus()
	cacheA := newTestTieredCacheWithInvalidation(t, newFakeCache(), newFakeCache(), bus)
	cacheB := newTestTieredCacheWithInvalidation(t, newFakeCache(), newFakeCache(), bus)
	waitForInvalidationCondition(t, func() bool {
		return cacheA.invalidationReady.Load() && cacheB.invalidationReady.Load()
	})

	if cacheA.invalidationOrigin == "" || cacheB.invalidationOrigin == "" {
		t.Fatal("invalidation origin must be generated for every cache")
	}
	if cacheA.invalidationOrigin == cacheB.invalidationOrigin {
		t.Fatalf("invalidation origins are equal: %q", cacheA.invalidationOrigin)
	}
}

func TestTieredCacheForgetInvalidatesPeer(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1A, l1B, l2 := newFakeCache(), newFakeCache(), newFakeCache()
	l2.put("key", []byte("value"), time.Minute)
	cacheA := newTestTieredCacheWithInvalidation(t, l1A, l2, bus)
	cacheB := newTestTieredCacheWithInvalidation(t, l1B, l2, bus)
	waitForInvalidationCondition(t, func() bool { return cacheA.invalidationReady.Load() && cacheB.invalidationReady.Load() })
	warmCache(t, cacheB, "value")

	if removed, err := cacheA.Forget("key"); err != nil || !removed {
		t.Fatalf("Forget() = (%t, %v), want (true, nil)", removed, err)
	}
	waitForInvalidationCondition(t, func() bool {
		value, _, err := l1B.Get("key")
		return err == nil && value == nil
	})
}

func TestTieredCacheTouchInvalidatesPeer(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1A, l1B, l2 := newFakeCache(), newFakeCache(), newFakeCache()
	l2.put("key", []byte("value"), time.Minute)
	cacheA := newTestTieredCacheWithInvalidation(t, l1A, l2, bus)
	cacheB := newTestTieredCacheWithInvalidation(t, l1B, l2, bus)
	waitForInvalidationCondition(t, func() bool { return cacheA.invalidationReady.Load() && cacheB.invalidationReady.Load() })
	warmCache(t, cacheB, "value")

	if touched, err := cacheA.Touch("key", 2*time.Minute); err != nil || !touched {
		t.Fatalf("Touch() = (%t, %v), want (true, nil)", touched, err)
	}
	waitForInvalidationCondition(t, func() bool {
		value, _, err := l1B.Get("key")
		return err == nil && value == nil
	})
}

func TestTieredCacheFlushInvalidatesPeer(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1A, l1B, l2 := newFakeCache(), newFakeCache(), newFakeCache()
	l2.put("key", []byte("value"), time.Minute)
	cacheA := newTestTieredCacheWithInvalidation(t, l1A, l2, bus)
	cacheB := newTestTieredCacheWithInvalidation(t, l1B, l2, bus)
	waitForInvalidationCondition(t, func() bool { return cacheA.invalidationReady.Load() && cacheB.invalidationReady.Load() })
	warmCache(t, cacheB, "value")

	if flushed, err := cacheA.Flush(); err != nil || !flushed {
		t.Fatalf("Flush() = (%t, %v), want (true, nil)", flushed, err)
	}
	waitForInvalidationCondition(t, func() bool {
		value, _, err := l1B.Get("key")
		return err == nil && value == nil
	})
	if value, _, err := l2.Get("key"); err != nil || value != nil {
		t.Fatalf("L2 after Flush() = (%q, %v), want (nil, nil)", value, err)
	}
}

func TestTieredCachePublishFailureKeepsRedisAndRecoversPendingInvalidation(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1A, l1B, l2 := newFakeCache(), newFakeCache(), newFakeCache()
	l2.put("key", []byte("old"), time.Minute)
	config := Config{RecoveryInterval: 5 * time.Millisecond}
	cacheA := newTestTieredCacheWithInvalidationConfig(t, config, l1A, l2, bus)
	cacheB := newTestTieredCacheWithInvalidationConfig(t, config, l1B, l2, bus)
	waitForInvalidationCondition(t, func() bool { return cacheA.invalidationReady.Load() && cacheB.invalidationReady.Load() })
	warmCache(t, cacheB, "old")

	wantErr := errors.New("Pub/Sub unavailable")
	bus.setPublishError(wantErr)
	ok, err := cacheA.Set("key", []byte("new"), time.Minute)
	if !ok || !errors.Is(err, ErrL2Unavailable) || !errors.Is(err, wantErr) {
		t.Fatalf("Set() = (%t, %v), want committed true and degraded error", ok, err)
	}
	value, _, getErr := l2.Get("key")
	if getErr != nil || string(value) != "new" {
		t.Fatalf("L2 after publish failure = (%q, %v), want (new, nil)", value, getErr)
	}
	if !cacheA.pendingInvalidation("key") {
		t.Fatal("pending invalidation was not retained")
	}

	bus.setPublishError(nil)
	waitForInvalidationCondition(t, func() bool {
		value, _, err := l1B.Get("key")
		return err == nil && value == nil
	})
	waitForInvalidationCondition(t, func() bool { return cacheA.unavailableError() == nil })
	if cacheA.pendingInvalidation("key") {
		t.Fatal("pending invalidation was not cleared after recovery")
	}
}

func TestTieredCacheCounterReturnsCommittedValueOnPublishFailure(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1A, l1B, l2 := newFakeCache(), newFakeCache(), newFakeCache()
	l2.put("counter", []byte("10"), time.Minute)
	config := Config{RecoveryInterval: 5 * time.Millisecond}
	cacheA := newTestTieredCacheWithInvalidationConfig(t, config, l1A, l2, bus)
	cacheB := newTestTieredCacheWithInvalidationConfig(t, config, l1B, l2, bus)
	waitForInvalidationCondition(t, func() bool { return cacheA.invalidationReady.Load() && cacheB.invalidationReady.Load() })
	if value, _, err := cacheB.Get("counter"); err != nil || string(value) != "10" {
		t.Fatalf("peer warm Get() = (%q, %v), want (10, nil)", value, err)
	}

	wantErr := errors.New("Pub/Sub unavailable")
	bus.setPublishError(wantErr)
	got, err := cacheA.Increment("counter", 1)
	if got != 11 || !errors.Is(err, ErrPostCommit) || !errors.Is(err, ErrL2Unavailable) || !errors.Is(err, wantErr) {
		t.Fatalf("Increment() = (%d, %v), want committed value 11 and post-commit publish error", got, err)
	}
	if !cacheA.pendingInvalidation("counter") {
		t.Fatal("pending counter invalidation was not retained")
	}

	bus.setPublishError(nil)
	waitForInvalidationCondition(t, func() bool {
		value, _, err := l1B.Get("counter")
		return err == nil && value == nil
	})
	waitForInvalidationCondition(t, func() bool { return cacheA.unavailableError() == nil })
}

func TestTieredCacheAddPublishFailureKeepsCommittedValueAndRecovers(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1A, l1B, l2 := newFakeCache(), newFakeCache(), newFakeCache()
	l1B.put("key", []byte("stale"), time.Minute)
	config := Config{RecoveryInterval: 5 * time.Millisecond}
	cacheA := newTestTieredCacheWithInvalidationConfig(t, config, l1A, l2, bus)
	cacheB := newTestTieredCacheWithInvalidationConfig(t, config, l1B, l2, bus)
	waitForInvalidationCondition(t, func() bool { return cacheA.invalidationReady.Load() && cacheB.invalidationReady.Load() })

	wantErr := errors.New("Pub/Sub unavailable")
	bus.setPublishError(wantErr)
	added, err := cacheA.Add("key", []byte("value"), time.Minute)
	if !added || !errors.Is(err, ErrPostCommit) || !errors.Is(err, ErrL2Unavailable) || !errors.Is(err, wantErr) {
		t.Fatalf("Add() = (%t, %v), want committed true and post-commit publish error", added, err)
	}
	if value, _, getErr := l2.Get("key"); getErr != nil || string(value) != "value" {
		t.Fatalf("L2 after publish failure = (%q, %v), want value", value, getErr)
	}
	if !cacheA.pendingInvalidation("key") {
		t.Fatal("pending Add invalidation was not retained")
	}

	bus.setPublishError(nil)
	waitForInvalidationCondition(t, func() bool {
		value, _, err := l1B.Get("key")
		return err == nil && value == nil
	})
	waitForInvalidationCondition(t, func() bool { return cacheA.unavailableError() == nil })
}

func TestTieredCacheRejectedAddDoesNotPublishInvalidation(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1A, l1B, l2 := newFakeCache(), newFakeCache(), newFakeCache()
	l2.put("key", []byte("old"), time.Minute)
	cacheA := newTestTieredCacheWithInvalidation(t, l1A, l2, bus)
	cacheB := newTestTieredCacheWithInvalidation(t, l1B, l2, bus)
	waitForInvalidationCondition(t, func() bool { return cacheA.invalidationReady.Load() && cacheB.invalidationReady.Load() })
	warmCache(t, cacheA, "old")
	warmCache(t, cacheB, "old")
	version := cacheA.mutationVersion.Load()

	added, err := cacheA.Add("key", []byte("new"), time.Minute)
	if err != nil || added {
		t.Fatalf("Add(existing) = (%t, %v), want (false, nil)", added, err)
	}
	if got := cacheA.mutationVersion.Load(); got != version {
		t.Fatalf("mutationVersion after rejected Add() = %d, want %d", got, version)
	}
	if got := bus.publishCount(); got != 0 {
		t.Fatalf("Pub/Sub publishes after rejected Add() = %d, want 0", got)
	}
	if value, _, getErr := l1B.Get("key"); getErr != nil || string(value) != "old" {
		t.Fatalf("peer L1 after rejected Add() = (%q, %v), want old value", value, getErr)
	}
}

func TestTieredCachePublishFailureForFlushDoesNotRepeatRedisFlush(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1A, l1B, l2 := newFakeCache(), newFakeCache(), newFakeCache()
	l2.put("key", []byte("value"), time.Minute)
	config := Config{RecoveryInterval: 5 * time.Millisecond}
	cacheA := newTestTieredCacheWithInvalidationConfig(t, config, l1A, l2, bus)
	cacheB := newTestTieredCacheWithInvalidationConfig(t, config, l1B, l2, bus)
	waitForInvalidationCondition(t, func() bool { return cacheA.invalidationReady.Load() && cacheB.invalidationReady.Load() })
	warmCache(t, cacheB, "value")

	bus.setPublishError(errors.New("Pub/Sub unavailable"))
	if flushed, err := cacheA.Flush(); flushed || err == nil {
		t.Fatalf("Flush() = (%t, %v), want false and error", flushed, err)
	}
	if got := l2.flushCount(); got != 1 {
		t.Fatalf("L2 Flush() calls before recovery = %d, want 1", got)
	}
	bus.setPublishError(nil)
	waitForInvalidationCondition(t, func() bool {
		value, _, err := l1B.Get("key")
		return err == nil && value == nil
	})
	waitForInvalidationCondition(t, func() bool { return cacheA.unavailableError() == nil })
	if got := l2.flushCount(); got != 1 {
		t.Fatalf("L2 Flush() calls after recovery = %d, want 1", got)
	}
}

func TestTieredCacheReconnectFlushesLocalL1(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1, l2 := newFakeCache(), newFakeCache()
	l2.put("key", []byte("value"), time.Minute)
	cache := newTestTieredCacheWithInvalidationConfig(t, Config{RecoveryInterval: 5 * time.Millisecond}, l1, l2, bus)
	waitForInvalidationCondition(t, func() bool { return cache.invalidationReady.Load() })
	warmCache(t, cache, "value")

	bus.disconnect(errors.New("subscriber connection lost"))
	waitForInvalidationCondition(t, func() bool {
		value, _, err := l1.Get("key")
		return err == nil && value == nil
	})
	if _, _, err := cache.Get("key"); !errors.Is(err, ErrL2Unavailable) {
		t.Fatalf("Get() during reconnect = %v, want ErrL2Unavailable", err)
	}
	waitForInvalidationCondition(t, func() bool { return cache.unavailableError() == nil })
}

func (c *TieredCache) pendingInvalidation(key string) bool {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	_, ok := c.pendingInvalidations[key]
	return ok
}

func warmCache(t *testing.T, cache *TieredCache, want string) {
	t.Helper()
	value, _, err := cache.Get("key")
	if err != nil || string(value) != want {
		t.Fatalf("warm Get() = (%q, %v), want (%s, nil)", value, err, want)
	}
}

func newTestTieredCacheWithInvalidation(t *testing.T, l1, l2 *fakeCache, bus *fakeInvalidationBus) *TieredCache {
	return newTestTieredCacheWithInvalidationConfig(t, Config{RecoveryInterval: 5 * time.Millisecond}, l1, l2, bus)
}

func newTestTieredCacheWithInvalidationConfig(t *testing.T, config Config, l1, l2 *fakeCache, bus *fakeInvalidationBus) *TieredCache {
	t.Helper()
	cache, err := NewWithInvalidation(config, l1, l2, bus)
	if err != nil {
		t.Fatalf("NewWithInvalidation() error = %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	return cache
}

func waitForInvalidationCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition was not satisfied")
		}
		time.Sleep(time.Millisecond)
	}
}

type fakeInvalidationBus struct {
	mu           sync.Mutex
	subscribers  map[*fakeInvalidationSubscription]struct{}
	publishErr   error
	subscribeErr error
	delayOrigin  string
	delayStarted chan struct{}
	delayRelease chan struct{}
	publishCalls int
	closed       bool
}

func newFakeInvalidationBus() *fakeInvalidationBus {
	return &fakeInvalidationBus{subscribers: make(map[*fakeInvalidationSubscription]struct{})}
}

func (b *fakeInvalidationBus) Publish(ctx context.Context, event cacheinvalidation.Event) error {
	if err := event.Validate(); err != nil {
		return err
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return errors.New("fake invalidation bus: closed")
	}
	b.publishCalls++
	if b.publishErr != nil {
		err := b.publishErr
		b.mu.Unlock()
		return err
	}
	subscribers := make([]*fakeInvalidationSubscription, 0, len(b.subscribers))
	for subscriber := range b.subscribers {
		subscribers = append(subscribers, subscriber)
	}
	delayOrigin := b.delayOrigin
	delayStarted := b.delayStarted
	delayRelease := b.delayRelease
	b.mu.Unlock()

	for _, subscriber := range subscribers {
		if event.Origin == delayOrigin && delayRelease != nil {
			if delayStarted != nil {
				select {
				case delayStarted <- struct{}{}:
				default:
				}
			}
			go deliverDelayedInvalidation(subscriber, event, delayRelease)
			continue
		}
		select {
		case subscriber.events <- event:
		case <-subscriber.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func deliverDelayedInvalidation(subscriber *fakeInvalidationSubscription, event cacheinvalidation.Event, release <-chan struct{}) {
	select {
	case <-release:
		select {
		case subscriber.events <- event:
		case <-subscriber.done:
		}
	case <-subscriber.done:
	}
}

func (b *fakeInvalidationBus) Subscribe(ctx context.Context) (cacheinvalidation.Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b.closed {
		return nil, errors.New("fake invalidation bus: closed")
	}
	if b.subscribeErr != nil {
		return nil, b.subscribeErr
	}
	subscriber := &fakeInvalidationSubscription{
		bus: b, events: make(chan cacheinvalidation.Event, 16), errors: make(chan error, 1), done: make(chan struct{}),
	}
	b.subscribers[subscriber] = struct{}{}
	return subscriber, nil
}

func (b *fakeInvalidationBus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	subscribers := make([]*fakeInvalidationSubscription, 0, len(b.subscribers))
	for subscriber := range b.subscribers {
		subscribers = append(subscribers, subscriber)
	}
	b.mu.Unlock()
	for _, subscriber := range subscribers {
		_ = subscriber.Close()
	}
	return nil
}

func (b *fakeInvalidationBus) setPublishError(err error) {
	b.mu.Lock()
	b.publishErr = err
	b.mu.Unlock()
}

func (b *fakeInvalidationBus) publishCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.publishCalls
}

func (b *fakeInvalidationBus) disconnect(err error) {
	b.mu.Lock()
	subscribers := make([]*fakeInvalidationSubscription, 0, len(b.subscribers))
	for subscriber := range b.subscribers {
		subscribers = append(subscribers, subscriber)
	}
	b.mu.Unlock()
	for _, subscriber := range subscribers {
		select {
		case subscriber.errors <- err:
		default:
		}
	}
}

type fakeInvalidationSubscription struct {
	bus    *fakeInvalidationBus
	events chan cacheinvalidation.Event
	errors chan error
	done   chan struct{}
	once   sync.Once
}

func (s *fakeInvalidationSubscription) Receive(ctx context.Context) (cacheinvalidation.Event, error) {
	select {
	case event := <-s.events:
		return event, nil
	case err := <-s.errors:
		return cacheinvalidation.Event{}, err
	case <-s.done:
		return cacheinvalidation.Event{}, errors.New("fake invalidation subscription: closed")
	case <-ctx.Done():
		return cacheinvalidation.Event{}, ctx.Err()
	}
}

func (s *fakeInvalidationSubscription) Close() error {
	s.once.Do(func() {
		close(s.done)
		s.bus.mu.Lock()
		delete(s.bus.subscribers, s)
		s.bus.mu.Unlock()
	})
	return nil
}
