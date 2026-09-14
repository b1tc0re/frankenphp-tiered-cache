package tiered

import (
	"errors"
	"fmt"
	"time"
)

const recoveryProbeKey = "\x00frankenphp-tiered-cache/recovery-probe"

type healthState uint32

const (
	healthy healthState = iota
	degraded
)

func (c *TieredCache) unavailableError() error {
	// Healthy reads are the dominant path. Avoid stateErrMu unless there is an
	// actual degraded/not-ready condition to explain.
	if healthState(c.healthState.Load()) != degraded &&
		(c.invalidationBus == nil || c.invalidationReady.Load()) {
		return nil
	}

	c.stateErrMu.RLock()
	err := c.stateErr
	c.stateErrMu.RUnlock()
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
	c.dirtyMu.Lock()
	c.dirtyKeys[key] = struct{}{}
	c.dirtyMu.Unlock()
}

func (c *TieredCache) markPendingFlush() {
	if c.invalidationBus == nil {
		return
	}
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
		var err error
		if c.fencedL2 != nil {
			_, err = c.fencedL2.ForgetWithFence(key)
		} else {
			_, err = c.l2.Forget(key)
		}
		if err != nil {
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

				if _, _, err := c.l2.Get(recoveryProbeKey); err == nil &&
					c.recoverPendingFlush() && c.recoverDirtyKeys() {
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
