package tiered

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	cachecontract "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache"
	cacheinvalidation "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache/invalidation"
)

const recoveryProbeKey = "\x00frankenphp-tiered-cache/recovery-probe"

const invalidationOriginSize = 16

func newInvalidationOrigin() (string, error) {
	origin := make([]byte, invalidationOriginSize)
	if _, err := cryptorand.Read(origin); err != nil {
		return "", fmt.Errorf("tiered cache: generate invalidation origin: %w", err)
	}

	return hex.EncodeToString(origin), nil
}

type healthState uint32

const (
	healthy healthState = iota
	degraded
)

var errL1MutationNotStored = errors.New("tiered cache: L1 mutation was not stored")

// TieredCache uses Redis as the authoritative store and MemoryCache as a
// local read-through cache. Mutations change Redis first, then update L1 and
// invalidate peers before returning.
type TieredCache struct {
	l1 cachecontract.Cache
	l2 cachecontract.Cache

	recoveryInterval time.Duration

	healthState atomic.Uint32
	stateErrMu  sync.RWMutex
	stateErr    error
	healthEpoch uint64

	recoveryWake chan struct{}
	recoveryStop chan struct{}
	recoveryDone chan struct{}
	recoveryMu   sync.Mutex

	// Pending entries contain only Pub/Sub messages that must be retried.
	// They never represent Redis data and recovery never deletes cache keys.
	pendingMu            sync.Mutex
	pendingInvalidations map[string]struct{}
	pendingFlush         bool

	mutationMu      sync.Mutex
	mutationVersion atomic.Uint64

	invalidationBus    cacheinvalidation.Bus
	invalidationOrigin string
	invalidationReady  atomic.Bool
	invalidationCancel context.CancelFunc
	invalidationDone   chan struct{}

	lifecycleMu sync.RWMutex
	closeOnce   sync.Once
	closeErr    error
	closed      bool
}

var _ cachecontract.Cache = (*TieredCache)(nil)

func New(config Config, l1, l2 cachecontract.Cache) (*TieredCache, error) {
	return newTieredCache(config, l1, l2, nil)
}

// NewWithInvalidation composes L1 and L2 with a cross-instance invalidation
// bus. Events contain no values; receivers only forget their local L1 entry.
func NewWithInvalidation(config Config, l1, l2 cachecontract.Cache, bus cacheinvalidation.Bus) (*TieredCache, error) {
	return newTieredCache(config, l1, l2, bus)
}

func newTieredCache(config Config, l1, l2 cachecontract.Cache, bus cacheinvalidation.Bus) (*TieredCache, error) {
	if l1 == nil {
		return nil, fmt.Errorf("tiered cache: L1 cache must not be nil")
	}
	if l2 == nil {
		return nil, fmt.Errorf("tiered cache: L2 cache must not be nil")
	}

	config, err := config.normalized()
	if err != nil {
		return nil, err
	}

	var origin string
	if bus != nil {
		origin, err = newInvalidationOrigin()
		if err != nil {
			return nil, err
		}
	}

	c := &TieredCache{
		l1:                   l1,
		l2:                   l2,
		recoveryInterval:     config.RecoveryInterval,
		recoveryWake:         make(chan struct{}, 1),
		recoveryStop:         make(chan struct{}),
		recoveryDone:         make(chan struct{}),
		pendingInvalidations: make(map[string]struct{}),
		invalidationBus:      bus,
		invalidationOrigin:   origin,
		invalidationDone:     make(chan struct{}),
	}
	if bus == nil {
		c.invalidationReady.Store(true)
		close(c.invalidationDone)
	} else {
		ctx, cancel := context.WithCancel(context.Background())
		c.invalidationCancel = cancel
		go c.runInvalidationSubscriber(ctx)
	}
	go c.runRecovery()

	return c, nil
}

func (c *TieredCache) unavailableError() error {
	c.stateErrMu.RLock()
	state := healthState(c.healthState.Load())
	err := c.stateErr
	c.stateErrMu.RUnlock()

	if state != degraded && (c.invalidationBus == nil || c.invalidationReady.Load()) {
		return nil
	}
	if err == nil {
		return ErrL2Unavailable
	}
	return fmt.Errorf("%w: %w", ErrL2Unavailable, err)
}

func (c *TieredCache) transitionToDegraded(cause error) (bool, uint64) {
	if cause == nil {
		cause = ErrL2Unavailable
	}

	c.stateErrMu.Lock()
	defer c.stateErrMu.Unlock()

	if healthState(c.healthState.Load()) == degraded {
		return false, c.healthEpoch
	}

	c.healthState.Store(uint32(degraded))
	c.healthEpoch++
	c.stateErr = cause

	select {
	case c.recoveryWake <- struct{}{}:
	default:
	}
	return true, c.healthEpoch
}

// degradeLocked is called while mutationMu is held. That ordering prevents a
// local mutation and an optional fail-closed L1 flush from interleaving.
func (c *TieredCache) degradeLocked(cause error, flushL1 bool) {
	_, _ = c.transitionToDegraded(cause)
	if flushL1 {
		c.flushL1Locked()
	}
}

func (c *TieredCache) degradeAndFlush(cause error) {
	_, _ = c.transitionToDegraded(cause)

	c.recoveryMu.Lock()
	defer c.recoveryMu.Unlock()
	c.mutationMu.Lock()
	c.flushL1Locked()
	c.mutationMu.Unlock()
}

func (c *TieredCache) flushL1Locked() {
	if c.closed {
		return
	}

	_, flushErr := c.l1.Flush()
	if flushErr != nil {
		c.stateErrMu.Lock()
		c.stateErr = errors.Join(c.stateErr, flushErr)
		c.stateErrMu.Unlock()
	}
}

func (c *TieredCache) enterHealthy(expectedEpoch uint64) bool {
	c.stateErrMu.Lock()
	defer c.stateErrMu.Unlock()

	if healthState(c.healthState.Load()) != degraded || c.healthEpoch != expectedEpoch {
		return false
	}
	if c.invalidationBus != nil && !c.invalidationReady.Load() {
		return false
	}

	c.healthState.Store(uint32(healthy))
	c.stateErr = nil
	return true
}

func (c *TieredCache) publishKeyInvalidation(key string) error {
	if c.invalidationBus == nil {
		return nil
	}

	return c.invalidationBus.Publish(context.Background(), cacheinvalidation.Event{
		Version: cacheinvalidation.ProtocolVersion,
		Type:    cacheinvalidation.EventTypeInvalidate,
		Key:     key,
		Origin:  c.invalidationOrigin,
	})
}

func (c *TieredCache) publishFlushInvalidation() error {
	if c.invalidationBus == nil {
		return nil
	}

	return c.invalidationBus.Publish(context.Background(), cacheinvalidation.Event{
		Version: cacheinvalidation.ProtocolVersion,
		Type:    cacheinvalidation.EventTypeFlush,
		Origin:  c.invalidationOrigin,
	})
}

func (c *TieredCache) markPendingInvalidation(key string) {
	if c.invalidationBus == nil {
		return
	}

	c.pendingMu.Lock()
	if !c.pendingFlush {
		c.pendingInvalidations[key] = struct{}{}
	}
	c.pendingMu.Unlock()
}

func (c *TieredCache) markPendingFlush() {
	if c.invalidationBus == nil {
		return
	}

	c.pendingMu.Lock()
	c.pendingFlush = true
	clear(c.pendingInvalidations)
	c.pendingMu.Unlock()
}

func (c *TieredCache) recoverPendingInvalidations() bool {
	if c.invalidationBus == nil {
		return true
	}

	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()

	if c.pendingFlush {
		if err := c.publishFlushInvalidation(); err != nil {
			return false
		}
		c.pendingFlush = false
		return true
	}

	for key := range c.pendingInvalidations {
		if err := c.publishKeyInvalidation(key); err != nil {
			return false
		}
		delete(c.pendingInvalidations, key)
	}
	return true
}

func (c *TieredCache) tryRecovery() {
	if healthState(c.healthState.Load()) != degraded {
		return
	}
	if c.invalidationBus != nil && !c.invalidationReady.Load() {
		return
	}

	if _, _, err := c.l2.Get(recoveryProbeKey); err != nil {
		return
	}
	if !c.recoverPendingInvalidations() {
		return
	}

	c.recoveryMu.Lock()
	defer c.recoveryMu.Unlock()
	if healthState(c.healthState.Load()) != degraded {
		return
	}
	if c.invalidationBus != nil && !c.invalidationReady.Load() {
		return
	}
	c.stateErrMu.RLock()
	expectedEpoch := c.healthEpoch
	c.stateErrMu.RUnlock()

	// A Redis outage may have happened before a local L1 mutation was
	// attempted. Keep that mutation untouched while degraded, then clear L1
	// immediately before making the cache healthy again.
	c.mutationMu.Lock()
	if c.closed {
		c.mutationMu.Unlock()
		return
	}
	_, flushErr := c.l1.Flush()
	c.mutationMu.Unlock()
	if flushErr != nil {
		return
	}
	c.enterHealthy(expectedEpoch)
}

func (c *TieredCache) runRecovery() {
	defer close(c.recoveryDone)

	ticker := time.NewTicker(c.recoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.tryRecovery()
		case <-c.recoveryWake:
			// The ticker defines the recovery cadence. The wake-up channel
			// prevents a future implementation from sleeping forever while
			// degraded, but an error must not be retried immediately.
		case <-c.recoveryStop:
			return
		}
	}
}

func waitForRetry(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *TieredCache) runInvalidationSubscriber(ctx context.Context) {
	defer close(c.invalidationDone)

	for {
		if ctx.Err() != nil || c.isClosed() {
			return
		}
		c.invalidationReady.Store(false)

		subscription, err := c.invalidationBus.Subscribe(ctx)
		if err != nil {
			if ctx.Err() != nil || c.isClosed() {
				return
			}
			c.degradeAndFlush(err)
			if !waitForRetry(ctx, c.recoveryInterval) {
				return
			}
			continue
		}

		if err := c.flushLocalL1(); err != nil {
			_ = subscription.Close()
			if ctx.Err() != nil || c.isClosed() {
				return
			}
			c.degradeAndFlush(err)
			if !waitForRetry(ctx, c.recoveryInterval) {
				return
			}
			continue
		}
		c.invalidationReady.Store(true)

		for {
			event, receiveErr := subscription.Receive(ctx)
			if receiveErr != nil {
				_ = subscription.Close()
				c.invalidationReady.Store(false)
				if ctx.Err() != nil || c.isClosed() {
					return
				}
				c.degradeAndFlush(receiveErr)
				break
			}

			if err := c.applyInvalidation(event); err != nil {
				_ = subscription.Close()
				c.invalidationReady.Store(false)
				if ctx.Err() != nil || c.isClosed() {
					return
				}
				c.degradeAndFlush(err)
				break
			}
		}

		if !waitForRetry(ctx, c.recoveryInterval) {
			return
		}
	}
}

func (c *TieredCache) isClosed() bool {
	c.mutationMu.Lock()
	closed := c.closed
	c.mutationMu.Unlock()
	return closed
}

func (c *TieredCache) flushLocalL1() error {
	c.recoveryMu.Lock()
	defer c.recoveryMu.Unlock()
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()

	if c.closed {
		return ErrClosed
	}
	c.mutationVersion.Add(1)
	_, err := c.l1.Flush()
	return err
}

func (c *TieredCache) applyInvalidation(event cacheinvalidation.Event) error {
	if err := event.Validate(); err != nil {
		return err
	}
	if event.Origin == c.invalidationOrigin {
		return nil
	}

	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	if c.closed {
		return ErrClosed
	}

	c.mutationVersion.Add(1)
	if event.Type == cacheinvalidation.EventTypeFlush {
		_, err := c.l1.Flush()
		return err
	}
	_, err := c.l1.Forget(event.Key)
	return err
}

func (c *TieredCache) Get(key string) ([]byte, time.Duration, error) {
	c.lifecycleMu.RLock()
	defer c.lifecycleMu.RUnlock()

	for {
		c.mutationMu.Lock()
		if c.closed {
			c.mutationMu.Unlock()
			return nil, 0, ErrClosed
		}
		if err := c.unavailableError(); err != nil {
			c.mutationMu.Unlock()
			return nil, 0, err
		}
		c.mutationMu.Unlock()

		value, ttl, err := c.l1.Get(key)
		if err != nil {
			return nil, 0, err
		}
		if value != nil {
			c.mutationMu.Lock()
			if c.closed {
				c.mutationMu.Unlock()
				return nil, 0, ErrClosed
			}
			if err := c.unavailableError(); err != nil {
				c.mutationMu.Unlock()
				return nil, 0, err
			}
			c.mutationMu.Unlock()
			return value, ttl, nil
		}

		version := c.mutationVersion.Load()
		started := time.Now()
		l2Value, l2TTL, l2Err := c.l2.Get(key)

		c.mutationMu.Lock()
		if c.closed {
			c.mutationMu.Unlock()
			return nil, 0, ErrClosed
		}
		if version != c.mutationVersion.Load() {
			c.mutationMu.Unlock()
			continue
		}
		if err := c.unavailableError(); err != nil {
			c.mutationMu.Unlock()
			return nil, 0, err
		}
		if l2Err != nil {
			c.degradeLocked(l2Err, true)
			err := c.unavailableError()
			c.mutationMu.Unlock()
			return nil, 0, err
		}

		current, currentTTL, currentErr := c.l1.Get(key)
		if currentErr != nil {
			c.mutationMu.Unlock()
			return nil, 0, currentErr
		}
		if current != nil {
			c.mutationMu.Unlock()
			return current, currentTTL, nil
		}
		if l2Value == nil {
			c.mutationMu.Unlock()
			return nil, 0, nil
		}

		if l2TTL > 0 {
			l2TTL -= time.Since(started)
			if l2TTL <= 0 {
				c.mutationMu.Unlock()
				return nil, 0, nil
			}
			_, _ = c.l1.Set(key, l2Value, l2TTL)
		} else {
			_, _ = c.l1.Forever(key, l2Value)
		}
		c.mutationMu.Unlock()
		return l2Value, l2TTL, nil
	}
}

func (c *TieredCache) Set(key string, value []byte, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, cachecontract.ErrInvalidTTL
	}
	if value == nil {
		return false, cachecontract.ErrNilValue
	}
	return c.set(key, value, ttl, false)
}

func (c *TieredCache) Forever(key string, value []byte) (bool, error) {
	if value == nil {
		return false, cachecontract.ErrNilValue
	}
	return c.set(key, value, 0, true)
}

// Increment atomically changes the authoritative Redis counter and
// invalidates the local copy. The returned value remains valid even when a
// post-commit L1 or Pub/Sub step reports an error; callers must not retry the
// operation solely because such an error was returned.
func (c *TieredCache) Increment(key string, value int64) (int64, error) {
	return c.changeCounter(key, value, false)
}

// Decrement atomically changes the authoritative Redis counter and
// invalidates the local copy.
func (c *TieredCache) Decrement(key string, value int64) (int64, error) {
	return c.changeCounter(key, value, true)
}

func (c *TieredCache) changeCounter(key string, value int64, decrement bool) (int64, error) {
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()

	if c.closed {
		return 0, ErrClosed
	}
	if err := c.unavailableError(); err != nil {
		return 0, err
	}

	var result int64
	var l2Err error
	if decrement {
		result, l2Err = c.l2.Decrement(key, value)
	} else {
		result, l2Err = c.l2.Increment(key, value)
	}
	if l2Err != nil {
		if errors.Is(l2Err, cachecontract.ErrRedisCommand) {
			return 0, l2Err
		}
		c.degradeLocked(l2Err, false)
		return 0, c.unavailableError()
	}

	// Redis has committed. The result must be returned with any later error so
	// callers do not repeat a mutation that already changed the counter.
	c.mutationVersion.Add(1)
	_, l1Err := c.l1.Forget(key)
	if l1Err != nil {
		_, flushErr := c.l1.Flush()
		l1Err = errors.Join(l1Err, flushErr)
	}

	publishErr := c.publishKeyInvalidation(key)
	if publishErr != nil {
		c.markPendingInvalidation(key)
		c.degradeLocked(publishErr, true)
	}

	return result, errors.Join(l1Err, c.unavailableErrorIf(publishErr))
}

func (c *TieredCache) set(key string, value []byte, ttl time.Duration, forever bool) (bool, error) {
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()

	if c.closed {
		return false, ErrClosed
	}
	if err := c.unavailableError(); err != nil {
		return false, err
	}

	var deadline time.Time
	if !forever {
		deadline = time.Now().Add(ttl)
	}

	var stored bool
	var l2Err error
	if forever {
		stored, l2Err = c.l2.Forever(key, value)
	} else {
		stored, l2Err = c.l2.Set(key, value, ttl)
	}
	if l2Err != nil {
		c.degradeLocked(l2Err, false)
		return false, c.unavailableError()
	}
	if !stored {
		return false, nil
	}

	// Redis has committed. From this point on, errors must not make callers
	// believe that the Redis mutation can safely be repeated as a fallback.
	c.mutationVersion.Add(1)
	var l1Stored bool
	var l1Err error
	if forever {
		l1Stored, l1Err = c.l1.Forever(key, value)
	} else {
		remaining := time.Until(deadline)
		if remaining > 0 {
			l1Stored, l1Err = c.l1.Set(key, value, remaining)
		} else {
			_, l1Err = c.l1.Forget(key)
			l1Stored = true
		}
	}
	if l1Err == nil && !l1Stored {
		l1Err = errL1MutationNotStored
	}
	if l1Err != nil {
		_, flushErr := c.l1.Flush()
		l1Err = errors.Join(l1Err, flushErr)
	}

	publishErr := c.publishKeyInvalidation(key)
	if publishErr != nil {
		c.markPendingInvalidation(key)
		c.degradeLocked(publishErr, true)
	}
	if l1Err != nil || publishErr != nil {
		return true, errors.Join(l1Err, c.unavailableError())
	}
	return true, nil
}

func (c *TieredCache) Forget(key string) (bool, error) {
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()

	if c.closed {
		return false, ErrClosed
	}
	if err := c.unavailableError(); err != nil {
		return false, err
	}

	l2Removed, l2Err := c.l2.Forget(key)
	if l2Err != nil {
		c.degradeLocked(l2Err, false)
		return false, c.unavailableError()
	}
	c.mutationVersion.Add(1)
	l1Removed, l1Err := c.l1.Forget(key)
	if l1Err != nil {
		_, flushErr := c.l1.Flush()
		l1Err = errors.Join(l1Err, flushErr)
	}
	publishErr := c.publishKeyInvalidation(key)
	if publishErr != nil {
		c.markPendingInvalidation(key)
		c.degradeLocked(publishErr, true)
	}
	return l1Removed || l2Removed, errors.Join(l1Err, c.unavailableErrorIf(publishErr))
}

func (c *TieredCache) Touch(key string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, cachecontract.ErrInvalidTTL
	}

	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()

	if c.closed {
		return false, ErrClosed
	}
	if err := c.unavailableError(); err != nil {
		return false, err
	}

	deadline := time.Now().Add(ttl)
	l2Touched, l2Err := c.l2.Touch(key, ttl)
	if l2Err != nil {
		c.degradeLocked(l2Err, true)
		return false, c.unavailableError()
	}
	c.mutationVersion.Add(1)
	var l1Touched bool
	var l1Err error
	if l2Touched {
		remaining := time.Until(deadline)
		if remaining > 0 {
			l1Touched, l1Err = c.l1.Touch(key, remaining)
		} else {
			_, l1Err = c.l1.Forget(key)
			l1Touched = true
		}
	} else {
		l1Touched, l1Err = c.l1.Forget(key)
	}
	if l1Err != nil {
		_, flushErr := c.l1.Flush()
		l1Err = errors.Join(l1Err, flushErr)
	}
	publishErr := c.publishKeyInvalidation(key)
	if publishErr != nil {
		c.markPendingInvalidation(key)
		c.degradeLocked(publishErr, true)
	}
	return l2Touched || l1Touched, errors.Join(l1Err, c.unavailableErrorIf(publishErr))
}

func (c *TieredCache) Flush() (bool, error) {
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()

	if c.closed {
		return false, ErrClosed
	}
	if err := c.unavailableError(); err != nil {
		return false, err
	}

	l2Flushed, l2Err := c.l2.Flush()
	if l2Err != nil {
		c.degradeLocked(l2Err, false)
		return false, c.unavailableError()
	}
	c.mutationVersion.Add(1)
	l1Flushed, l1Err := c.l1.Flush()
	publishErr := c.publishFlushInvalidation()
	if publishErr != nil {
		c.markPendingFlush()
		c.degradeLocked(publishErr, true)
	}
	return l1Flushed && l2Flushed && l1Err == nil && publishErr == nil, errors.Join(l1Err, c.unavailableErrorIf(publishErr))
}

func (c *TieredCache) unavailableErrorIf(operationErr error) error {
	if operationErr == nil {
		return nil
	}
	return c.unavailableError()
}

func (c *TieredCache) Close() error {
	c.closeOnce.Do(func() {
		c.lifecycleMu.Lock()
		defer c.lifecycleMu.Unlock()

		c.mutationMu.Lock()
		c.closed = true
		c.mutationVersion.Add(1)
		close(c.recoveryStop)
		if c.invalidationCancel != nil {
			c.invalidationCancel()
		}
		c.mutationMu.Unlock()

		<-c.recoveryDone
		<-c.invalidationDone

		var invalidationErr error
		if c.invalidationBus != nil {
			invalidationErr = c.invalidationBus.Close()
		}
		c.closeErr = errors.Join(invalidationErr, c.l2.Close(), c.l1.Close())
	})

	return c.closeErr
}
