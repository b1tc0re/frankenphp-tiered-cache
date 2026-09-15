package tiered

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	cacheinvalidation "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache/invalidation"
)

func TestTieredCacheCrossPodInvalidatesLocalL1(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1A := newFakeCache()
	l1B := newFakeCache()
	l2 := newFakeCache()
	l2.values["key"] = []byte("v1")
	l2.ttls["key"] = time.Minute

	cacheA := newTestTieredCacheWithInvalidation(t, l1A, l2, bus)
	cacheB := newTestTieredCacheWithInvalidation(t, l1B, l2, bus)
	waitForInvalidationCondition(t, func() bool {
		return cacheA.invalidationReady.Load() && cacheB.invalidationReady.Load()
	})

	for name, cache := range map[string]*TieredCache{"A": cacheA, "B": cacheB} {
		t.Run("warm "+name, func(t *testing.T) {
			value, _, err := cache.Get("key")
			if err != nil {
				t.Fatalf("Get() error = %v", err)
			}
			if string(value) != "v1" {
				t.Fatalf("Get() value = %q, want v1", value)
			}
		})
	}

	if removed, err := cacheA.Forget("key"); err != nil || !removed {
		t.Fatalf("Forget() = (%t, %v), want (true, nil)", removed, err)
	}

	waitForInvalidationCondition(t, func() bool {
		value, _, err := l1B.Get("key")
		return err == nil && value == nil
	})
	if value, _, err := cacheB.Get("key"); err != nil || value != nil {
		t.Fatalf("peer Get() = (%q, %v), want (nil, nil)", value, err)
	}
}

func TestTieredCacheRemoteInvalidationCancelsInFlightWrite(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1A := newFakeCache()
	l1B := newFakeCache()
	l2 := newFakeCache()
	l2.setStarted = make(chan struct{}, 1)
	l2.releaseSet = make(chan struct{})

	cacheA := newTestTieredCacheWithInvalidation(t, l1A, l2, bus)
	cacheB := newTestTieredCacheWithInvalidation(t, l1B, l2, bus)
	waitForInvalidationCondition(t, func() bool {
		return cacheA.invalidationReady.Load() && cacheB.invalidationReady.Load()
	})

	if ok, err := cacheA.Set("key", []byte("v1"), time.Minute); err != nil || !ok {
		t.Fatalf("cache A Set() = (%t, %v), want (true, nil)", ok, err)
	}
	select {
	case <-l2.setStarted:
	case <-time.After(time.Second):
		t.Fatal("cache A L2 worker did not start write")
	}

	if _, err := cacheB.Forget("key"); err != nil {
		t.Fatalf("cache B Forget() error = %v", err)
	}
	waitForInvalidationCondition(t, func() bool {
		value, _, err := l1A.Get("key")
		return err == nil && value == nil
	})

	close(l2.releaseSet)
	cacheA.queue.waitEmpty()
	if value, _, err := l2.Get("key"); err != nil || value != nil {
		t.Fatalf("stale L2 value after invalidated write = (%q, %v), want (nil, nil)", value, err)
	}
}

func TestTieredCacheRemoteInvalidationAfterWriteCompletionDoesNotLeaveStaleL2(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1A := newFakeCache()
	l1B := newFakeCache()
	l2 := newFakeCache()
	l2.values["key"] = []byte("old")
	l2.ttls["key"] = time.Minute
	l2.setStarted = make(chan struct{}, 1)
	l2.releaseSet = make(chan struct{})

	cacheA := newTestTieredCacheWithInvalidation(t, l1A, l2, bus)
	cacheB := newTestTieredCacheWithInvalidation(t, l1B, l2, bus)
	waitForInvalidationCondition(t, func() bool {
		return cacheA.invalidationReady.Load() && cacheB.invalidationReady.Load()
	})

	delayedDeliveryStarted := make(chan struct{}, 1)
	delayedDeliveryRelease := make(chan struct{})
	bus.delayOrigin = cacheB.invalidationOrigin
	bus.delayStarted = delayedDeliveryStarted
	bus.delayRelease = delayedDeliveryRelease

	if ok, err := cacheA.Set("key", []byte("v1"), time.Minute); err != nil || !ok {
		t.Fatalf("cache A Set() = (%t, %v), want (true, nil)", ok, err)
	}
	select {
	case <-l2.setStarted:
	case <-time.After(time.Second):
		t.Fatal("cache A L2 worker did not start write")
	}

	if _, err := cacheB.Forget("key"); err != nil {
		t.Fatalf("cache B Forget() error = %v", err)
	}
	select {
	case <-delayedDeliveryStarted:
	case <-time.After(time.Second):
		t.Fatal("cache B invalidation was not delayed")
	}

	close(l2.releaseSet)
	cacheA.queue.waitEmpty()
	if value, _, err := l2.Get("key"); err != nil || value != nil {
		t.Fatalf("L2 before delayed invalidation = (%q, %v), want (nil, nil)", value, err)
	}

	close(delayedDeliveryRelease)
	waitForInvalidationCondition(t, func() bool {
		value, _, err := l1A.Get("key")
		return err == nil && value == nil
	})
	if value, _, err := l2.Get("key"); err != nil || value != nil {
		t.Fatalf("L2 after delayed invalidation = (%q, %v), want (nil, nil)", value, err)
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

func TestTieredCacheRecoveryDeletesL2BeforePublishingInvalidation(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.values["key"] = []byte("stale")
	l2.ttls["key"] = time.Minute
	cache := newTestTieredCacheWithInvalidation(t, l1, l2, bus)
	waitForInvalidationCondition(t, func() bool {
		return cache.invalidationReady.Load()
	})

	token, err := l2.ReserveFence("key")
	if err != nil {
		t.Fatalf("ReserveFence() error = %v", err)
	}
	cache.markDirtyWithFence("key", token)
	var valueAtPublish []byte
	bus.publishHook = func(cacheinvalidation.Event) {
		valueAtPublish, _, _ = l2.Get("key")
	}

	if !cache.recoverDirtyKeys() {
		t.Fatal("recoverDirtyKeys() = false, want true")
	}
	if valueAtPublish != nil {
		t.Fatalf("L2 value at invalidation publish = %q, want nil", valueAtPublish)
	}
	if value, _, err := l2.Get("key"); err != nil || value != nil {
		t.Fatalf("L2 value after recovery = (%q, %v), want (nil, nil)", value, err)
	}
}

func TestTieredCacheReleasesFenceAfterL1MutationFailure(t *testing.T) {
	tests := []struct {
		name string
		set  func(*fakeCache, error)
		call func(*TieredCache) (bool, error)
	}{
		{
			name: "set",
			set:  func(l1 *fakeCache, err error) { l1.setErrors(nil, err) },
			call: func(cache *TieredCache) (bool, error) {
				return cache.Set("key", []byte("value"), time.Minute)
			},
		},
		{
			name: "forget",
			set:  func(l1 *fakeCache, err error) { l1.setForgetError(err) },
			call: func(cache *TieredCache) (bool, error) {
				return cache.Forget("key")
			},
		},
		{
			name: "touch",
			set:  func(l1 *fakeCache, err error) { l1.setTouchError(err) },
			call: func(cache *TieredCache) (bool, error) {
				return cache.Touch("key", time.Minute)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bus := newFakeInvalidationBus()
			l1 := newFakeCache()
			l2 := newFakeCache()
			cache := newTestTieredCacheWithInvalidation(t, l1, l2, bus)
			waitForInvalidationCondition(t, func() bool {
				return cache.invalidationReady.Load()
			})

			wantErr := errors.New("L1 unavailable")
			test.set(l1, wantErr)
			if _, err := test.call(cache); !errors.Is(err, wantErr) {
				t.Fatalf("operation error = %v, want %v", err, wantErr)
			}

			l2.mu.Lock()
			_, hasFence := l2.fenceTokens["key"]
			l2.mu.Unlock()
			if hasFence {
				t.Fatal("L2 fence remains after failed L1 mutation")
			}
		})
	}
}

func TestTieredCacheRecoveryDoesNotDeleteNewerL2Value(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1 := newFakeCache()
	l2 := newFakeCache()
	cache := newTestTieredCacheWithInvalidation(t, l1, l2, bus)
	waitForInvalidationCondition(t, func() bool {
		return cache.invalidationReady.Load()
	})

	failedToken, err := l2.ReserveFence("key")
	if err != nil {
		t.Fatalf("ReserveFence(failed operation) error = %v", err)
	}
	cache.markDirtyWithFence("key", failedToken)

	newerToken, err := l2.ReserveFence("key")
	if err != nil {
		t.Fatalf("ReserveFence(newer operation) error = %v", err)
	}
	if ok, err := l2.SetWithFence("key", []byte("new"), time.Minute, newerToken); err != nil || !ok {
		t.Fatalf("newer SetWithFence() = (%t, %v), want (true, nil)", ok, err)
	}

	if !cache.recoverDirtyKeys() {
		t.Fatal("recoverDirtyKeys() = false, want true")
	}
	if value, _, err := l2.Get("key"); err != nil || string(value) != "new" {
		t.Fatalf("L2 value after recovery = (%q, %v), want (new, nil)", value, err)
	}
}

func TestTieredCacheWriterRecoveryDoesNotDeleteNewerL2Value(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1 := newFakeCache()
	l2 := newFakeCache()
	wantErr := errors.New("redis unavailable")
	l2.setErrors(nil, wantErr)
	errorsReported := make(chan error, 1)
	cache := newTestTieredCacheWithInvalidationConfig(t, Config{
		RecoveryInterval: time.Hour,
		OnWriteError: func(_ string, err error) {
			errorsReported <- err
		},
	}, l1, l2, bus)
	waitForInvalidationCondition(t, func() bool {
		return cache.invalidationReady.Load()
	})

	if ok, err := cache.Set("key", []byte("old"), time.Minute); err != nil || !ok {
		t.Fatalf("Set() = (%t, %v), want (true, nil)", ok, err)
	}
	select {
	case err := <-errorsReported:
		if !errors.Is(err, wantErr) {
			t.Fatalf("reported error = %v, want %v", err, wantErr)
		}
	case <-time.After(time.Second):
		t.Fatal("failed writer did not report an error")
	}

	l2.setErrors(nil, nil)
	newerToken, err := l2.ReserveFence("key")
	if err != nil {
		t.Fatalf("ReserveFence(newer operation) error = %v", err)
	}
	if ok, err := l2.SetWithFence("key", []byte("new"), time.Minute, newerToken); err != nil || !ok {
		t.Fatalf("newer SetWithFence() = (%t, %v), want (true, nil)", ok, err)
	}

	if !cache.recoverDirtyKeys() {
		t.Fatal("recoverDirtyKeys() = false, want true")
	}
	if value, _, err := l2.Get("key"); err != nil || string(value) != "new" {
		t.Fatalf("L2 value after writer recovery = (%q, %v), want (new, nil)", value, err)
	}
}

func TestTieredCachePublishFailureIsRecoveredAndInvalidatesPeers(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1A := newFakeCache()
	l1B := newFakeCache()
	l2 := newFakeCache()
	l2.values["key"] = []byte("old")
	l2.ttls["key"] = time.Minute

	config := Config{RecoveryInterval: 5 * time.Millisecond}
	cacheA := newTestTieredCacheWithInvalidationConfig(t, config, l1A, l2, bus)
	cacheB := newTestTieredCacheWithInvalidationConfig(t, config, l1B, l2, bus)
	waitForInvalidationCondition(t, func() bool {
		return cacheA.invalidationReady.Load() && cacheB.invalidationReady.Load()
	})
	if _, _, err := cacheB.Get("key"); err != nil {
		t.Fatalf("peer warm Get() error = %v", err)
	}

	wantErr := errors.New("pubsub unavailable")
	bus.setPublishError(wantErr)
	if ok, err := cacheA.Set("key", []byte("new"), time.Minute); err != nil || !ok {
		t.Fatalf("Set() = (%t, %v), want (true, nil)", ok, err)
	}
	waitForInvalidationCondition(t, func() bool {
		return cacheA.unavailableError() != nil
	})
	if value, _, err := l1A.Get("key"); err != nil || value != nil {
		t.Fatalf("local L1 after publish failure = (%q, %v), want (nil, nil)", value, err)
	}

	bus.setPublishError(nil)
	waitForInvalidationCondition(t, func() bool {
		value, _, err := l1B.Get("key")
		return err == nil && value == nil
	})
	waitForInvalidationCondition(t, func() bool {
		return cacheA.unavailableError() == nil
	})

	if value, _, err := l2.Get("key"); err != nil || string(value) != "new" {
		t.Fatalf("L2 value after publish-only recovery = (%q, %v), want (new, nil)", value, err)
	}
}

func TestTieredCacheFlushPublishFailureRetriesOnlyInvalidation(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1A := newFakeCache()
	l1B := newFakeCache()
	l2 := newFakeCache()
	l2.values["key"] = []byte("value")
	l2.ttls["key"] = time.Minute

	config := Config{RecoveryInterval: 5 * time.Millisecond}
	cacheA := newTestTieredCacheWithInvalidationConfig(t, config, l1A, l2, bus)
	cacheB := newTestTieredCacheWithInvalidationConfig(t, config, l1B, l2, bus)
	waitForInvalidationCondition(t, func() bool {
		return cacheA.invalidationReady.Load() && cacheB.invalidationReady.Load()
	})
	if _, _, err := cacheB.Get("key"); err != nil {
		t.Fatalf("peer warm Get() error = %v", err)
	}

	bus.setPublishError(errors.New("pubsub unavailable"))
	if ok, err := cacheA.Flush(); ok || err == nil {
		t.Fatalf("Flush() = (%t, %v), want (false, error)", ok, err)
	}
	if value, _, err := l2.Get("key"); err != nil || value != nil {
		t.Fatalf("L2 after Flush() = (%q, %v), want (nil, nil)", value, err)
	}
	if got := l2.flushCount(); got != 1 {
		t.Fatalf("L2 Flush() calls = %d, want 1 before recovery", got)
	}

	bus.setPublishError(nil)
	waitForInvalidationCondition(t, func() bool {
		value, _, err := l1B.Get("key")
		return err == nil && value == nil
	})
	waitForInvalidationCondition(t, func() bool {
		return cacheA.unavailableError() == nil
	})
	if got := l2.flushCount(); got != 1 {
		t.Fatalf("L2 Flush() calls = %d, want 1 after recovery", got)
	}
}

func TestTieredCacheReconnectFlushesLocalL1BeforeRecovery(t *testing.T) {
	bus := newFakeInvalidationBus()
	l1 := newFakeCache()
	l2 := newFakeCache()
	l2.values["key"] = []byte("value")
	l2.ttls["key"] = time.Minute

	cache := newTestTieredCacheWithInvalidationConfig(t, Config{RecoveryInterval: 5 * time.Millisecond}, l1, l2, bus)
	waitForInvalidationCondition(t, func() bool {
		return cache.invalidationReady.Load()
	})
	if _, _, err := cache.Get("key"); err != nil {
		t.Fatalf("warm Get() error = %v", err)
	}

	subscribeErr := errors.New("subscriber connection lost")
	bus.setSubscribeError(subscribeErr)
	bus.disconnect(subscribeErr)
	waitForInvalidationCondition(t, func() bool {
		value, _, err := l1.Get("key")
		return err == nil && value == nil
	})
	if _, _, err := cache.Get("key"); !errors.Is(err, ErrL2Unavailable) {
		t.Fatalf("Get() during subscriber outage = %v, want ErrL2Unavailable", err)
	}

	bus.setSubscribeError(nil)
	waitForInvalidationCondition(t, func() bool {
		return cache.unavailableError() == nil
	})
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
	t.Cleanup(func() {
		_ = cache.Close()
	})
	return cache
}

func waitForInvalidationCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
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
	publishHook  func(cacheinvalidation.Event)
	delayOrigin  string
	delayStarted chan<- struct{}
	delayRelease <-chan struct{}
	closed       bool
}

func newFakeInvalidationBus() *fakeInvalidationBus {
	return &fakeInvalidationBus{
		subscribers: make(map[*fakeInvalidationSubscription]struct{}),
	}
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
	if b.publishErr != nil {
		err := b.publishErr
		b.mu.Unlock()
		return err
	}
	subscribers := make([]*fakeInvalidationSubscription, 0, len(b.subscribers))
	for subscriber := range b.subscribers {
		subscribers = append(subscribers, subscriber)
	}
	publishHook := b.publishHook
	delayOrigin := b.delayOrigin
	delayStarted := b.delayStarted
	delayRelease := b.delayRelease
	b.mu.Unlock()
	if publishHook != nil {
		publishHook(event)
	}

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
		bus:    b,
		events: make(chan cacheinvalidation.Event, 16),
		errors: make(chan error, 1),
		done:   make(chan struct{}),
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

func (b *fakeInvalidationBus) setSubscribeError(err error) {
	b.mu.Lock()
	b.subscribeErr = err
	b.mu.Unlock()
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
