package tiered

import (
	"errors"
	"fmt"
	"time"

	cachecontract "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache"
)

func (c *TieredCache) Set(key string, value []byte, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, cachecontract.ErrInvalidTTL
	}
	if value == nil {
		return false, cachecontract.ErrNilValue
	}
	return c.set(key, value, ttl, false)
}

// Add stores value only when the key is absent in the authoritative L2.
// Redis performs the check and mutation atomically; L1 is updated only after
// that mutation has committed.
func (c *TieredCache) Add(key string, value []byte, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, cachecontract.ErrInvalidTTL
	}
	if value == nil {
		return false, cachecontract.ErrNilValue
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
	added, l2Err := c.l2.Add(key, value, ttl)
	if l2Err != nil {
		c.degradeLocked(l2Err, false)
		return false, c.unavailableError()
	}
	if !added {
		return false, nil
	}

	// Redis has committed. Errors from the remaining local/coherence steps must
	// not make callers repeat Add and accidentally change the result later.
	c.mutationVersion.Add(1)
	remaining := time.Until(deadline)
	var l1Stored bool
	var l1Err error
	if remaining > 0 {
		l1Stored, l1Err = c.l1.Set(key, value, remaining)
	} else {
		_, l1Err = c.l1.Forget(key)
		l1Stored = true
	}
	if l1Err == nil && !l1Stored {
		l1Err = errL1MutationNotStored
	}
	l1Err = c.reconcileL1MutationErrorLocked(l1Err)

	publishErr := c.publishKeyInvalidation(key)
	if publishErr != nil {
		c.markPendingInvalidation(key)
		c.degradeLocked(publishErr, true)
	}
	if l1Err != nil || publishErr != nil {
		postCommitErr := errors.Join(l1Err, c.unavailableErrorIf(errors.Join(l1Err, publishErr)))
		return true, fmt.Errorf("%w: %w", ErrPostCommit, postCommitErr)
	}
	return true, nil
}

// SetMany commits the complete batch in Redis before warming L1. Redis is the
// authoritative store, so local cache and Pub/Sub failures are reported as
// post-commit errors and never make callers repeat the batch mutation.
func (c *TieredCache) SetMany(values map[string][]byte, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, cachecontract.ErrInvalidTTL
	}
	if len(values) == 0 {
		return false, nil
	}
	for _, value := range values {
		if value == nil {
			return false, cachecontract.ErrNilValue
		}
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
	stored, l2Err := c.l2.SetMany(values, ttl)
	if l2Err != nil {
		c.degradeLocked(l2Err, false)
		return false, c.unavailableError()
	}
	if !stored {
		return false, nil
	}

	// One Redis transaction committed the whole batch. From this point on,
	// callers must retain the successful result even when local coherence work
	// reports an error.
	c.mutationVersion.Add(1)
	remaining := time.Until(deadline)
	var l1Err error
	if remaining > 0 {
		for key, value := range values {
			l1Stored, err := c.l1.Set(key, value, remaining)
			if err == nil && !l1Stored {
				err = errL1MutationNotStored
			}
			l1Err = errors.Join(l1Err, err)
		}
	} else {
		for key := range values {
			_, err := c.l1.Forget(key)
			l1Err = errors.Join(l1Err, err)
		}
	}
	l1Err = c.reconcileL1MutationErrorLocked(l1Err)

	var publishErr error
	for key := range values {
		publishErr = c.publishKeyInvalidation(key)
		if publishErr != nil {
			for pendingKey := range values {
				c.markPendingInvalidation(pendingKey)
			}
			c.degradeLocked(publishErr, true)
			break
		}
	}

	if l1Err != nil || publishErr != nil {
		postCommitErr := errors.Join(l1Err, c.unavailableErrorIf(errors.Join(l1Err, publishErr)))
		return true, fmt.Errorf("%w: %w", ErrPostCommit, postCommitErr)
	}
	return true, nil
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
	l1Err = c.reconcileL1MutationErrorLocked(l1Err)

	publishErr := c.publishKeyInvalidation(key)
	if publishErr != nil {
		c.markPendingInvalidation(key)
		c.degradeLocked(publishErr, true)
	}

	postCommitErr := errors.Join(l1Err, c.unavailableErrorIf(errors.Join(l1Err, publishErr)))
	if postCommitErr != nil {
		return result, fmt.Errorf("%w: %w", ErrPostCommit, postCommitErr)
	}
	return result, nil
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
	l1Err = c.reconcileL1MutationErrorLocked(l1Err)

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
	l1Err = c.reconcileL1MutationErrorLocked(l1Err)
	publishErr := c.publishKeyInvalidation(key)
	if publishErr != nil {
		c.markPendingInvalidation(key)
		c.degradeLocked(publishErr, true)
	}
	return l1Removed || l2Removed, errors.Join(l1Err, c.unavailableErrorIf(errors.Join(l1Err, publishErr)))
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
		l1Err = c.reconcileL1MutationErrorLocked(l1Err)
	}
	publishErr := c.publishKeyInvalidation(key)
	if publishErr != nil {
		c.markPendingInvalidation(key)
		c.degradeLocked(publishErr, true)
	}
	return l2Touched || l1Touched, errors.Join(l1Err, c.unavailableErrorIf(errors.Join(l1Err, publishErr)))
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
	l1Flushed, l1Err := flushL1(c.l1)
	if l1Err != nil {
		c.degradeLocked(l1Err, false)
	}
	publishErr := c.publishFlushInvalidation()
	if publishErr != nil {
		c.markPendingFlush()
		c.degradeLocked(publishErr, true)
	}
	return l1Flushed && l2Flushed && l1Err == nil && publishErr == nil, errors.Join(l1Err, c.unavailableErrorIf(errors.Join(l1Err, publishErr)))
}
