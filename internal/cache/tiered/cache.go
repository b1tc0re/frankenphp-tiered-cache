package tiered

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	cachecontract "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache"
)

type writeOperation struct {
	key       string
	value     []byte
	ttl       time.Duration
	expiresAt time.Time
	forever   bool
}

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
// wait for all earlier queued writes before touching either backend.
type TieredCache struct {
	l1 cachecontract.Cache
	l2 cachecontract.Cache

	queue                 *writeQueue
	pending               *pendingWrites
	workerDone            chan struct{}
	writeQueueWaitTimeout time.Duration
	onWriteError          func(key string, err error)
	now                   func() time.Time

	mutationMu      sync.Mutex
	mutationVersion atomic.Uint64
	closeOnce       sync.Once
	closeErr        error
	closed          bool
}

var _ cachecontract.Cache = (*TieredCache)(nil)

func New(config Config, l1, l2 cachecontract.Cache) (*TieredCache, error) {
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

	cache := &TieredCache{
		l1:                    l1,
		l2:                    l2,
		queue:                 newWriteQueue(config.WriteQueueCapacity, config.WriteQueueMaxBytes),
		pending:               newPendingWrites(),
		workerDone:            make(chan struct{}),
		writeQueueWaitTimeout: config.WriteQueueWaitTimeout,
		onWriteError:          config.OnWriteError,
		now:                   time.Now,
	}
	go cache.runWriter()

	return cache, nil
}

func (c *TieredCache) Get(key string) ([]byte, time.Duration, error) {
	for {
		value, ttl, err := c.l1.Get(key)
		if err != nil || value != nil {
			return value, ttl, err
		}

		version := c.mutationVersion.Load()
		if err := c.pending.wait(key); err != nil {
			return nil, 0, err
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

		pending, pendingErr := c.pending.status(key)
		if pending || pendingErr != nil {
			c.mutationMu.Unlock()
			if pending {
				if err := c.pending.wait(key); err != nil {
					return nil, 0, err
				}
				continue
			}
			return nil, 0, pendingErr
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
			c.mutationMu.Unlock()
			return nil, 0, l2Err
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
	defer c.mutationMu.Unlock()
	if c.closed {
		return false, ErrClosed
	}
	c.mutationVersion.Add(1)

	c.queue.waitEmpty()
	l1Removed, l1Err := c.l1.Forget(key)
	l2Removed, l2Err := c.l2.Forget(key)
	if l2Err == nil {
		c.pending.clear(key)
	}
	return l1Removed || l2Removed, errors.Join(l1Err, l2Err)
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
	c.mutationVersion.Add(1)

	c.queue.waitEmpty()
	l1Touched, l1Err := c.l1.Touch(key, ttl)
	l2Touched, l2Err := c.l2.Touch(key, ttl)
	return l1Touched || l2Touched, errors.Join(l1Err, l2Err)
}

func (c *TieredCache) Flush() (bool, error) {
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	if c.closed {
		return false, ErrClosed
	}
	c.mutationVersion.Add(1)

	c.queue.waitEmpty()
	l1Flushed, l1Err := c.l1.Flush()
	l2Flushed, l2Err := c.l2.Flush()
	if l2Err == nil {
		c.pending.clearAll()
	}
	return l1Flushed && l2Flushed, errors.Join(l1Err, l2Err)
}

func (c *TieredCache) Close() error {
	c.closeOnce.Do(func() {
		c.mutationMu.Lock()
		c.closed = true
		c.mutationVersion.Add(1)
		c.queue.close()
		c.mutationMu.Unlock()

		<-c.workerDone
		c.closeErr = errors.Join(c.l2.Close(), c.l1.Close())
	})

	return c.closeErr
}

func (c *TieredCache) enqueueWrite(operation writeOperation) (bool, error) {
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	if c.closed {
		return false, ErrClosed
	}
	c.mutationVersion.Add(1)
	if int64(len(operation.value)) > c.queue.maxBytes {
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
		return stored, err
	}

	c.pending.add(operation.key)
	if err := c.queue.enqueue(operation, c.writeQueueWaitTimeout); err != nil {
		c.pending.cancel(operation.key)
		return false, err
	}
	return true, nil
}

func (c *TieredCache) runWriter() {
	defer close(c.workerDone)

	for {
		operation, ok := c.queue.dequeue()
		if !ok {
			return
		}

		var err error
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
		c.pending.complete(operation.key, err)
		c.queue.complete(operation)
		if err != nil && c.onWriteError != nil {
			c.onWriteError(operation.key, err)
		}
	}
}
