package tiered

import (
	"errors"
	"fmt"

	cachecontract "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache"
)

type healthState uint32

const (
	healthy healthState = iota
	degraded
)

var errL1MutationNotStored = errors.New("tiered cache: L1 mutation was not stored")
var errL1FlushNotCompleted = errors.New("tiered cache: L1 flush was not completed")

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
	c.metrics.SetDegraded(true)

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

	_, flushErr := flushL1(c.l1)
	if flushErr != nil {
		c.stateErrMu.Lock()
		c.stateErr = errors.Join(c.stateErr, flushErr)
		c.stateErrMu.Unlock()
	}
}

func flushL1(l1 cachecontract.Cache) (bool, error) {
	flushed, err := l1.Flush()
	if err == nil && !flushed {
		err = errL1FlushNotCompleted
	}
	return flushed, err
}

// reconcileL1MutationErrorLocked retries the local invalidation by flushing
// L1. If that safety fallback also fails, the local cache can no longer be
// trusted and the TieredCache must remain degraded.
func (c *TieredCache) reconcileL1MutationErrorLocked(l1Err error) error {
	if l1Err == nil {
		return nil
	}
	c.metrics.ObserveL1MutationError()

	if _, flushErr := flushL1(c.l1); flushErr == nil {
		return l1Err
	} else {
		c.metrics.ObserveL1FlushFallback()
		combinedErr := errors.Join(l1Err, flushErr)
		c.degradeLocked(combinedErr, false)
		return combinedErr
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
	c.metrics.SetDegraded(false)
	return true
}

func (c *TieredCache) unavailableErrorIf(operationErr error) error {
	if operationErr == nil {
		return nil
	}
	return c.unavailableError()
}
