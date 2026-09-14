package tiered

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	cachecontract "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache"
	cacheinvalidation "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache/invalidation"
)

// TieredCache composes a process-local L1 cache with an authoritative L2.
//
// Set and Forever update L1 synchronously and enqueue the L2 mutation for the
// single FIFO writer. Forget, Touch, and Flush are synchronous and wait for
// earlier queued writes before mutating either backend.
type TieredCache struct {
	l1       cachecontract.Cache
	l2       cachecontract.Cache
	fencedL2 cachecontract.FencedCache

	queue                 *writeQueue
	pending               *pendingWrites
	workerDone            chan struct{}
	writeQueueWaitTimeout time.Duration
	recoveryInterval      time.Duration
	onWriteError          func(key string, err error)
	now                   func() time.Time

	healthState       atomic.Uint32
	stateErrMu        sync.RWMutex
	stateErr          error
	degradedFlushDone chan struct{}
	recoveryWake      chan struct{}
	recoveryStop      chan struct{}
	recoveryDone      chan struct{}

	dirtyMu        sync.Mutex
	dirtyKeys      map[string]struct{}
	pendingFlushMu sync.Mutex
	pendingFlush   bool

	mutationMu      sync.Mutex
	lifecycleMu     sync.RWMutex
	mutationVersion atomic.Uint64
	flushGeneration atomic.Uint64
	generationMu    sync.Mutex
	keyGenerations  map[string]keyGenerationState

	invalidationBus    cacheinvalidation.Bus
	invalidationOrigin string
	invalidationReady  atomic.Bool
	invalidationCancel context.CancelFunc
	invalidationDone   chan struct{}

	closeOnce sync.Once
	closeErr  error
	closed    bool
}

var _ cachecontract.Cache = (*TieredCache)(nil)

func New(config Config, l1, l2 cachecontract.Cache) (*TieredCache, error) {
	return newTieredCache(config, l1, l2, nil)
}

// NewWithInvalidation composes L1 and L2 with a cross-instance invalidation
// bus. L2 must support fencing so delayed writes cannot recreate a value after
// a newer cross-instance mutation.
//
// The initial subscription is established before this function returns. A
// successful constructor therefore returns a cache that can be used
// immediately without a separate readiness phase.
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

	var fencedL2 cachecontract.FencedCache
	if bus != nil {
		var ok bool
		fencedL2, ok = l2.(cachecontract.FencedCache)
		if !ok {
			return nil, fmt.Errorf("tiered cache: L2 cache must support fencing when invalidation is enabled")
		}
	}

	var invalidationOrigin string
	if bus != nil {
		invalidationOrigin, err = newInvalidationOrigin()
		if err != nil {
			return nil, err
		}
	}

	cache := &TieredCache{
		l1:                    l1,
		l2:                    l2,
		fencedL2:              fencedL2,
		queue:                 newWriteQueue(config.WriteQueueCapacity, config.WriteQueueMaxBytes),
		pending:               newPendingWrites(),
		workerDone:            make(chan struct{}),
		writeQueueWaitTimeout: config.WriteQueueWaitTimeout,
		recoveryInterval:      config.RecoveryInterval,
		onWriteError:          config.OnWriteError,
		now:                   time.Now,
		recoveryWake:          make(chan struct{}, 1),
		recoveryStop:          make(chan struct{}),
		recoveryDone:          make(chan struct{}),
		dirtyKeys:             make(map[string]struct{}),
		keyGenerations:        make(map[string]keyGenerationState),
		invalidationBus:       bus,
		invalidationOrigin:    invalidationOrigin,
		invalidationDone:      make(chan struct{}),
	}

	if bus == nil {
		cache.invalidationReady.Store(true)
		close(cache.invalidationDone)
	} else {
		invalidationContext, cancel := context.WithCancel(context.Background())
		subscription, subscribeErr := bus.Subscribe(invalidationContext)
		if subscribeErr != nil {
			cancel()
			return nil, fmt.Errorf("tiered cache: subscribe invalidation bus: %w", subscribeErr)
		}
		cache.invalidationCancel = cancel
		cache.invalidationReady.Store(true)
		go cache.runInvalidationSubscriber(invalidationContext, subscription)
	}

	go cache.runWriter()
	go cache.runRecovery()

	return cache, nil
}

func (c *TieredCache) Get(key string) ([]byte, time.Duration, error) {
	c.lifecycleMu.RLock()
	defer c.lifecycleMu.RUnlock()

	// Close holds lifecycleMu exclusively while setting closed and shutting down
	// the backends, so the hot read path does not need mutationMu just to check
	// lifecycle state.
	if c.closed {
		return nil, 0, ErrClosed
	}

	for {
		if err := c.unavailableError(); err != nil {
			return nil, 0, err
		}

		value, ttl, err := c.l1.Get(key)
		if err != nil || value != nil {
			if unavailable := c.unavailableError(); unavailable != nil {
				return nil, 0, unavailable
			}
			return value, ttl, err
		}

		version := c.mutationVersion.Load()
		_ = c.pending.wait(key)
		if unavailable := c.unavailableError(); unavailable != nil {
			return nil, 0, unavailable
		}
		if c.mutationVersion.Load() != version {
			continue
		}

		value, ttl, l2Err := c.l2.Get(key)

		c.mutationMu.Lock()
		if c.mutationVersion.Load() != version {
			c.mutationMu.Unlock()
			continue
		}
		if unavailable := c.unavailableError(); unavailable != nil {
			c.mutationMu.Unlock()
			return nil, 0, unavailable
		}

		pending, _ := c.pending.status(key)
		if pending {
			c.mutationMu.Unlock()
			_ = c.pending.wait(key)
			if unavailable := c.unavailableError(); unavailable != nil {
				return nil, 0, unavailable
			}
			continue
		}

		currentValue, currentTTL, currentErr := c.l1.Get(key)
		if currentErr != nil {
			c.mutationMu.Unlock()
			return nil, 0, currentErr
		}
		if currentValue != nil {
			c.mutationMu.Unlock()
			return currentValue, currentTTL, nil
		}

		if l2Err != nil {
			newlyDegraded, flushDone := c.transitionToDegraded(l2Err)
			if newlyDegraded {
				c.flushL1Locked(flushDone)
			}
			c.mutationMu.Unlock()
			return nil, 0, c.unavailableError()
		}
		if value == nil {
			c.mutationMu.Unlock()
			return nil, 0, nil
		}

		if ttl > 0 {
			_, _ = c.l1.Set(key, value, ttl)
		} else {
			_, _ = c.l1.Forever(key, value)
		}
		c.mutationMu.Unlock()

		return value, ttl, nil
	}
}

func (c *TieredCache) Set(key string, value []byte, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, cachecontract.ErrInvalidTTL
	}
	if value == nil {
		return false, cachecontract.ErrNilValue
	}

	return c.enqueueWrite(writeOperation{
		key:       key,
		value:     value,
		ttl:       ttl,
		expiresAt: c.now().Add(ttl),
	})
}

func (c *TieredCache) Forever(key string, value []byte) (bool, error) {
	if value == nil {
		return false, cachecontract.ErrNilValue
	}

	return c.enqueueWrite(writeOperation{key: key, value: value, forever: true})
}

func (c *TieredCache) Forget(key string) (bool, error) {
	c.mutationMu.Lock()
	if c.closed {
		c.mutationMu.Unlock()
		return false, ErrClosed
	}
	if err := c.unavailableError(); err != nil {
		c.mutationMu.Unlock()
		return false, err
	}
	c.mutationVersion.Add(1)

	c.queue.waitEmpty()
	if err := c.unavailableError(); err != nil {
		c.mutationMu.Unlock()
		return false, err
	}

	l1Removed, l1Err := c.l1.Forget(key)
	var l2Removed bool
	var l2Err error
	if c.fencedL2 != nil {
		l2Removed, l2Err = c.fencedL2.ForgetWithFence(key)
	} else {
		l2Removed, l2Err = c.l2.Forget(key)
	}

	var publishErr error
	if l2Err == nil {
		publishErr = c.publishKeyInvalidation(key)
	}
	backendErr := errors.Join(l1Err, l2Err, publishErr)
	if backendErr != nil {
		if l2Err != nil || publishErr != nil {
			c.markDirty(key)
		}
		newlyDegraded, flushDone := c.transitionToDegraded(backendErr)
		if newlyDegraded {
			c.flushL1Locked(flushDone)
		}
		c.mutationMu.Unlock()
		return l1Removed || l2Removed, c.unavailableError()
	}

	c.pending.clear(key)
	c.mutationMu.Unlock()
	return l1Removed || l2Removed, nil
}

func (c *TieredCache) Touch(key string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, cachecontract.ErrInvalidTTL
	}

	c.mutationMu.Lock()
	if c.closed {
		c.mutationMu.Unlock()
		return false, ErrClosed
	}
	if err := c.unavailableError(); err != nil {
		c.mutationMu.Unlock()
		return false, err
	}
	c.mutationVersion.Add(1)

	c.queue.waitEmpty()
	if err := c.unavailableError(); err != nil {
		c.mutationMu.Unlock()
		return false, err
	}

	l1Touched, l1Err := c.l1.Touch(key, ttl)
	var l2Touched bool
	var l2Err error
	if c.fencedL2 != nil {
		l2Touched, l2Err = c.fencedL2.TouchWithFence(key, ttl)
	} else {
		l2Touched, l2Err = c.l2.Touch(key, ttl)
	}

	var publishErr error
	if l2Err == nil {
		publishErr = c.publishKeyInvalidation(key)
	}
	backendErr := errors.Join(l1Err, l2Err, publishErr)
	if backendErr != nil {
		if l2Err != nil || publishErr != nil {
			c.markDirty(key)
		}
		newlyDegraded, flushDone := c.transitionToDegraded(backendErr)
		if newlyDegraded {
			c.flushL1Locked(flushDone)
		}
		c.mutationMu.Unlock()
		return l1Touched || l2Touched, c.unavailableError()
	}

	c.mutationMu.Unlock()
	return l1Touched || l2Touched, nil
}

func (c *TieredCache) Flush() (bool, error) {
	c.mutationMu.Lock()
	if c.closed {
		c.mutationMu.Unlock()
		return false, ErrClosed
	}
	if err := c.unavailableError(); err != nil {
		c.mutationMu.Unlock()
		return false, err
	}
	c.mutationVersion.Add(1)

	c.queue.waitEmpty()
	if err := c.unavailableError(); err != nil {
		c.mutationMu.Unlock()
		return false, err
	}

	l1Flushed, l1Err := c.l1.Flush()
	l2Flushed, l2Err := c.l2.Flush()

	// Flush may have partially mutated L2 even when it returns an error (for
	// example after one SCAN/DEL batch). Always invalidate peers after the L2
	// attempt; retrying only this notification is safe and never repeats Flush.
	publishErr := c.publishFlushInvalidation()
	if publishErr != nil {
		c.markPendingFlush()
	}

	backendErr := errors.Join(l1Err, l2Err, publishErr)
	if backendErr != nil {
		newlyDegraded, flushDone := c.transitionToDegraded(backendErr)
		if newlyDegraded {
			c.flushL1Locked(flushDone)
		}
		c.mutationMu.Unlock()
		return false, c.unavailableError()
	}

	c.pending.clearAll()
	c.mutationMu.Unlock()
	return l1Flushed && l2Flushed, nil
}

func (c *TieredCache) Close() error {
	c.closeOnce.Do(func() {
		c.lifecycleMu.Lock()
		defer c.lifecycleMu.Unlock()

		c.mutationMu.Lock()
		c.closed = true
		c.mutationVersion.Add(1)
		c.queue.close()
		close(c.recoveryStop)
		if c.invalidationCancel != nil {
			c.invalidationCancel()
		}
		c.mutationMu.Unlock()

		<-c.workerDone
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
