package tiered

import (
	"errors"
	"time"

	cachecontract "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache"
)

type writeOperation struct {
	key             string
	value           []byte
	ttl             time.Duration
	expiresAt       time.Time
	forever         bool
	fenceToken      cachecontract.FenceToken
	keyGeneration   uint64
	flushGeneration uint64
}

type keyGenerationState struct {
	generation uint64
	pending    int
}

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
		// Keep generation state bounded to keys with in-flight writes.
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

	if writeOperationBytes(operation) > c.queue.maxBytes {
		c.mutationMu.Unlock()
		return false, ErrWriteQueueValueTooLarge
	}

	var err error
	if c.fencedL2 != nil {
		// The fence must be reserved before the local L1 mutation. Reversing this
		// order reopens the cross-pod race where a concurrent Forget can happen
		// before this write becomes visible to Redis-side ordering.
		operation.fenceToken, err = c.fencedL2.ReserveFence(operation.key)
		if err != nil {
			newlyDegraded, flushDone := c.transitionToDegraded(err)
			if newlyDegraded {
				c.flushL1Locked(flushDone)
			}
			c.mutationMu.Unlock()
			return false, c.unavailableError()
		}
	}

	var stored bool
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
			if c.fencedL2 != nil {
				var stored bool
				if operation.forever {
					stored, err = c.fencedL2.ForeverWithFence(operation.key, operation.value, operation.fenceToken)
				} else {
					remainingTTL := operation.expiresAt.Sub(c.now())
					if remainingTTL <= 0 {
						stored, err = c.fencedL2.ForgetIfFence(operation.key, operation.fenceToken)
					} else {
						stored, err = c.fencedL2.SetWithFence(operation.key, operation.value, remainingTTL, operation.fenceToken)
					}
				}
				if err == nil && !stored {
					stale = true
				}
			} else if operation.forever {
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
				removed := true
				if c.fencedL2 != nil {
					removed, err = c.fencedL2.ForgetIfFence(operation.key, operation.fenceToken)
				} else {
					_, err = c.l2.Forget(operation.key)
				}
				if err == nil && removed {
					// The generation change already invalidated peers. Publishing
					// again after cleanup also clears a peer warmed by a concurrent
					// newer write.
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

		// Wake pending readers only after the health transition is visible. A
		// reader must never fall through to stale L2 after this write failed.
		c.pending.complete(operation.key)
		c.endWriteGeneration(operation.key)
		c.queue.complete(operation)

		if newlyDegraded {
			c.mutationMu.Lock()
			c.flushL1Locked(flushDone)
			c.mutationMu.Unlock()
		}
		if err != nil && c.onWriteError != nil {
			c.onWriteError(operation.key, err)
		}
	}
}
