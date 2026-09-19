package tiered

import "time"

const recoveryProbeKey = "\x00frankenphp-tiered-cache/recovery-probe"

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
	_, flushErr := flushL1(c.l1)
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
