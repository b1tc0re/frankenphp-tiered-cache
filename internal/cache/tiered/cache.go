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

type writeOperation struct {
	key             string
	value           []byte
	ttl             time.Duration
	expiresAt       time.Time
	forever         bool
	keyGeneration   uint64
	flushGeneration uint64
}

type keyGenerationState struct {
	generation uint64
	pending    int
}

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

type writeQueue struct {
	mu       sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond
	empty    *sync.Cond

	items []writeOperation
	head  int
	tail  int
	size  int

	pendingBytes int64
	pending      int
	maxBytes     int64
	closed       bool
}

func newWriteQueue(capacity int, maxBytes int64) *writeQueue {
	queue := &writeQueue{
		items:    make([]writeOperation, capacity),
		maxBytes: maxBytes,
	}
	queue.notEmpty = sync.NewCond(&queue.mu)
	queue.notFull = sync.NewCond(&queue.mu)
	queue.empty = sync.NewCond(&queue.mu)
	return queue
}

func (q *writeQueue) enqueue(operation writeOperation, timeout time.Duration) error {
	operationBytes := int64(len(operation.value))
	if operationBytes > q.maxBytes {
		return ErrWriteQueueValueTooLarge
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrClosed
	}

	deadline := time.Now().Add(timeout)
	for q.pending == len(q.items) || q.pendingBytes+operationBytes > q.maxBytes {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return ErrWriteQueueTimeout
		}

		timer := time.AfterFunc(remaining, func() {
			q.mu.Lock()
			q.notFull.Broadcast()
			q.mu.Unlock()
		})
		q.notFull.Wait()
		timer.Stop()

		if q.closed {
			return ErrClosed
		}
	}

	q.items[q.tail] = operation
	q.tail = (q.tail + 1) % len(q.items)
	q.size++
	q.pending++
	q.pendingBytes += operationBytes
	q.notEmpty.Signal()
	return nil
}

func (q *writeQueue) dequeue() (writeOperation, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.size == 0 && !q.closed {
		q.notEmpty.Wait()
	}
	if q.size == 0 {
		return writeOperation{}, false
	}

	operation := q.items[q.head]
	q.items[q.head] = writeOperation{}
	q.head = (q.head + 1) % len(q.items)
	q.size--
	return operation, true
}

func (q *writeQueue) complete(operation writeOperation) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.pending--
	q.pendingBytes -= int64(len(operation.value))
	q.notFull.Broadcast()
	if q.pending == 0 {
		q.empty.Broadcast()
	}
}

func (q *writeQueue) waitEmpty() {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.pending > 0 {
		q.empty.Wait()
	}
}

func (q *writeQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.closed = true
	q.notEmpty.Broadcast()
	q.notFull.Broadcast()
	q.empty.Broadcast()
}

// TieredCache composes an L1 cache with an L2 cache.
//
// L2 writes from Set and Forever are processed by one FIFO worker. The worker
// keeps operation ordering deterministic; synchronous invalidation operations
// wait for all earlier queued writes before touching either backend. When an
// L2 key mutation fails, its key remains dirty until recovery removes it.
type TieredCache struct {
	l1 cachecontract.Cache
	l2 cachecontract.Cache

	queue                 *writeQueue
	pending               *pendingWrites
	workerDone            chan struct{}
	writeQueueWaitTimeout time.Duration
	recoveryInterval      time.Duration
	onWriteError          func(key string, err error)
	now                   func() time.Time

	healthState        atomic.Uint32
	stateErrMu         sync.RWMutex
	stateErr           error
	degradedFlushDone  chan struct{}
	recoveryWake       chan struct{}
	recoveryStop       chan struct{}
	recoveryDone       chan struct{}
	dirtyMu            sync.Mutex
	dirtyKeys          map[string]struct{}
	pendingFlushMu     sync.Mutex
	pendingFlush       bool
	mutationMu         sync.Mutex
	lifecycleMu        sync.RWMutex
	mutationVersion    atomic.Uint64
	flushGeneration    atomic.Uint64
	generationMu       sync.Mutex
	keyGenerations     map[string]keyGenerationState
	invalidationBus    cacheinvalidation.Bus
	invalidationOrigin string
	invalidationReady  atomic.Bool
	invalidationCancel context.CancelFunc
	invalidationDone   chan struct{}
	closeOnce          sync.Once
	closeErr           error
	closed             bool
}

var _ cachecontract.Cache = (*TieredCache)(nil)

func New(config Config, l1, l2 cachecontract.Cache) (*TieredCache, error) {
	return newTieredCache(config, l1, l2, nil)
}

// NewWithInvalidation composes L1 and L2 with a cross-instance invalidation
// bus. Received events are applied to this cache's L1 only.
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
		cache.invalidationCancel = cancel
		go cache.runInvalidationSubscriber(invalidationContext)
	}
	go cache.runWriter()
	go cache.runRecovery()

	return cache, nil
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

func (c *TieredCache) degradeAndFlush(cause error) {
	newlyDegraded, flushDone := c.transitionToDegraded(cause)
	if !newlyDegraded {
		return
	}

	c.mutationMu.Lock()
	c.flushL1Locked(flushDone)
	c.mutationMu.Unlock()
}

func (c *TieredCache) transitionToDegraded(cause error) (bool, chan struct{}) {
	if cause == nil {
		cause = ErrL2Unavailable
	}

	c.stateErrMu.Lock()
	if !c.healthState.CompareAndSwap(uint32(healthy), uint32(degraded)) {
		c.stateErrMu.Unlock()
		return false, nil
	}

	flushDone := make(chan struct{})
	c.stateErr = cause
	c.degradedFlushDone = flushDone
	c.stateErrMu.Unlock()

	select {
	case c.recoveryWake <- struct{}{}:
	default:
	}
	return true, flushDone
}

func (c *TieredCache) flushL1Locked(flushDone chan struct{}) {
	defer close(flushDone)

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

func (c *TieredCache) enterHealthy() {
	c.stateErrMu.Lock()
	defer c.stateErrMu.Unlock()

	if !c.healthState.CompareAndSwap(uint32(degraded), uint32(healthy)) {
		return
	}

	c.stateErr = nil
	c.degradedFlushDone = nil
}

func (c *TieredCache) waitForL1Flush() {
	c.stateErrMu.RLock()
	flushDone := c.degradedFlushDone
	c.stateErrMu.RUnlock()
	if flushDone != nil {
		<-flushDone
	}
}

func (c *TieredCache) markDirty(key string) {
	// A key is retained only until the next successful recovery cleanup.
	c.dirtyMu.Lock()
	c.dirtyKeys[key] = struct{}{}
	c.dirtyMu.Unlock()
}

// beginWriteGeneration registers an async write before it enters the queue.
// The pending count keeps a generation entry alive while a worker may still
// compare against it, without retaining every key invalidated over the cache
// lifetime.
func (c *TieredCache) beginWriteGeneration(key string) uint64 {
	c.generationMu.Lock()
	defer c.generationMu.Unlock()

	state := c.keyGenerations[key]
	state.pending++
	c.keyGenerations[key] = state
	return state.generation
}

func (c *TieredCache) endWriteGeneration(key string) {
	c.generationMu.Lock()
	defer c.generationMu.Unlock()

	state, ok := c.keyGenerations[key]
	if !ok {
		return
	}
	state.pending--
	if state.pending <= 0 {
		delete(c.keyGenerations, key)
		return
	}
	c.keyGenerations[key] = state
}

func (c *TieredCache) bumpKeyGeneration(key string) {
	c.generationMu.Lock()
	defer c.generationMu.Unlock()

	state, ok := c.keyGenerations[key]
	if !ok {
		// There is no in-flight write to invalidate. Avoid retaining a
		// tombstone for a key that may never be used again.
		return
	}
	state.generation++
	c.keyGenerations[key] = state
}

func (c *TieredCache) keyGeneration(key string) uint64 {
	c.generationMu.Lock()
	defer c.generationMu.Unlock()

	return c.keyGenerations[key].generation
}

func (c *TieredCache) writeOperationStale(operation writeOperation) bool {
	if c.flushGeneration.Load() != operation.flushGeneration {
		return true
	}
	return c.keyGeneration(operation.key) != operation.keyGeneration
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

func (c *TieredCache) markPendingFlush() {
	c.pendingFlushMu.Lock()
	c.pendingFlush = true
	c.pendingFlushMu.Unlock()
}

func (c *TieredCache) recoverPendingFlush() bool {
	if c.invalidationBus == nil {
		return true
	}

	c.pendingFlushMu.Lock()
	defer c.pendingFlushMu.Unlock()
	if !c.pendingFlush {
		return true
	}
	if err := c.publishFlushInvalidation(); err != nil {
		return false
	}
	c.pendingFlush = false
	return true
}

func (c *TieredCache) recoverDirtyKeys() bool {
	c.dirtyMu.Lock()
	defer c.dirtyMu.Unlock()

	for key := range c.dirtyKeys {
		if _, err := c.l2.Forget(key); err != nil {
			return false
		}
		if err := c.publishKeyInvalidation(key); err != nil {
			return false
		}
		delete(c.dirtyKeys, key)
	}
	return true
}

func (c *TieredCache) runRecovery() {
	defer close(c.recoveryDone)

	for {
		select {
		case <-c.recoveryWake:
		case <-c.recoveryStop:
			return
		}

		ticker := time.NewTicker(c.recoveryInterval)
		for healthState(c.healthState.Load()) == degraded {
			select {
			case <-ticker.C:
				c.queue.waitEmpty()
				if healthState(c.healthState.Load()) != degraded {
					continue
				}
				c.waitForL1Flush()
				if healthState(c.healthState.Load()) != degraded {
					continue
				}
				if c.invalidationBus != nil && !c.invalidationReady.Load() {
					continue
				}

				if _, _, err := c.l2.Get(recoveryProbeKey); err == nil && c.recoverPendingFlush() && c.recoverDirtyKeys() {
					c.enterHealthy()
				}
			case <-c.recoveryStop:
				ticker.Stop()
				return
			}
		}
		ticker.Stop()
	}
}

func (c *TieredCache) runInvalidationSubscriber(ctx context.Context) {
	defer close(c.invalidationDone)

	firstSubscription := true
	for {
		if ctx.Err() != nil {
			return
		}

		subscription, err := c.invalidationBus.Subscribe(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.invalidationReady.Store(false)
			c.degradeAndFlush(err)
			if !waitForRetry(ctx, c.recoveryInterval) {
				return
			}
			continue
		}

		if !firstSubscription {
			c.invalidationReady.Store(false)
			if err := c.flushLocalL1(); err != nil {
				_ = subscription.Close()
				c.degradeAndFlush(err)
				if !waitForRetry(ctx, c.recoveryInterval) {
					return
				}
				continue
			}
		}
		firstSubscription = false
		c.invalidationReady.Store(true)

		for {
			event, receiveErr := subscription.Receive(ctx)
			if receiveErr != nil {
				_ = subscription.Close()
				if ctx.Err() != nil {
					return
				}
				c.invalidationReady.Store(false)
				c.degradeAndFlush(receiveErr)
				break
			}

			if event.Origin == c.invalidationOrigin {
				continue
			}
			if err := c.applyInvalidation(event); err != nil {
				c.degradeAndFlush(err)
			}
		}

		if !waitForRetry(ctx, c.recoveryInterval) {
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

func (c *TieredCache) flushLocalL1() error {
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	if c.closed {
		return nil
	}

	c.mutationVersion.Add(1)
	_, err := c.l1.Flush()
	return err
}

func (c *TieredCache) applyInvalidation(event cacheinvalidation.Event) error {
	if err := event.Validate(); err != nil {
		return err
	}

	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	if c.closed {
		return nil
	}

	c.mutationVersion.Add(1)
	switch event.Type {
	case cacheinvalidation.EventTypeInvalidate:
		c.bumpKeyGeneration(event.Key)
		_, err := c.l1.Forget(event.Key)
		return err
	case cacheinvalidation.EventTypeFlush:
		c.flushGeneration.Add(1)
		_, err := c.l1.Flush()
		return err
	default:
		return event.Validate()
	}
}

func (c *TieredCache) Get(key string) ([]byte, time.Duration, error) {
	c.lifecycleMu.RLock()
	defer c.lifecycleMu.RUnlock()

	c.mutationMu.Lock()
	if c.closed {
		c.mutationMu.Unlock()
		return nil, 0, ErrClosed
	}
	c.mutationMu.Unlock()

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
	l2Removed, l2Err := c.l2.Forget(key)
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
	if l2Err == nil {
		c.pending.clear(key)
	}
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
	l2Touched, l2Err := c.l2.Touch(key, ttl)
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
	backendErr := errors.Join(l1Err, l2Err)
	if backendErr != nil {
		newlyDegraded, flushDone := c.transitionToDegraded(backendErr)
		if newlyDegraded {
			c.flushL1Locked(flushDone)
		}
		c.mutationMu.Unlock()
		return false, c.unavailableError()
	}
	if err := c.publishFlushInvalidation(); err != nil {
		c.markPendingFlush()
		newlyDegraded, flushDone := c.transitionToDegraded(err)
		if newlyDegraded {
			c.flushL1Locked(flushDone)
		}
		c.mutationMu.Unlock()
		return false, c.unavailableError()
	}
	if l2Err == nil {
		c.pending.clearAll()
	}
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

func (c *TieredCache) enqueueWrite(operation writeOperation) (bool, error) {
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
	if int64(len(operation.value)) > c.queue.maxBytes {
		c.mutationMu.Unlock()
		return false, ErrWriteQueueValueTooLarge
	}

	var stored bool
	var err error
	if operation.forever {
		stored, err = c.l1.Forever(operation.key, operation.value)
	} else {
		stored, err = c.l1.Set(operation.key, operation.value, operation.ttl)
	}
	if err != nil || !stored {
		c.mutationMu.Unlock()
		return stored, err
	}

	operation.keyGeneration = c.beginWriteGeneration(operation.key)
	operation.flushGeneration = c.flushGeneration.Load()
	c.pending.add(operation.key)
	if err := c.queue.enqueue(operation, c.writeQueueWaitTimeout); err != nil {
		c.pending.cancel(operation.key)
		c.endWriteGeneration(operation.key)
		if errors.Is(err, ErrWriteQueueTimeout) {
			c.markDirty(operation.key)
			newlyDegraded, flushDone := c.transitionToDegraded(err)
			if newlyDegraded {
				c.flushL1Locked(flushDone)
			}
			c.mutationMu.Unlock()
			return false, c.unavailableError()
		}
		c.mutationMu.Unlock()
		return false, err
	}
	c.mutationMu.Unlock()
	return true, nil
}

func (c *TieredCache) runWriter() {
	defer close(c.workerDone)

	for {
		operation, ok := c.queue.dequeue()
		if !ok {
			return
		}

		stale := c.writeOperationStale(operation)
		var err error
		if !stale {
			if operation.forever {
				_, err = c.l2.Forever(operation.key, operation.value)
			} else {
				remainingTTL := operation.expiresAt.Sub(c.now())
				if remainingTTL <= 0 {
					_, err = c.l2.Forget(operation.key)
				} else {
					_, err = c.l2.Set(operation.key, operation.value, remainingTTL)
				}
			}
		}
		if err == nil && !stale {
			if c.writeOperationStale(operation) {
				_, err = c.l2.Forget(operation.key)
				if err == nil {
					// The generation change already invalidated peers, but
					// publish again after cleanup so a concurrent newer write
					// cannot remain warm in another pod's L1.
					err = c.publishKeyInvalidation(operation.key)
				}
			} else {
				err = c.publishKeyInvalidation(operation.key)
			}
		}
		newlyDegraded := false
		var flushDone chan struct{}
		if err != nil {
			c.markDirty(operation.key)
			newlyDegraded, flushDone = c.transitionToDegraded(err)
		}
		c.pending.complete(operation.key)
		c.endWriteGeneration(operation.key)
		c.queue.complete(operation)
		if newlyDegraded {
			c.mutationMu.Lock()
			c.flushL1Locked(flushDone)
			c.mutationMu.Unlock()
		}
		if err != nil {
			if c.onWriteError != nil {
				c.onWriteError(operation.key, err)
			}
		}
	}
}
