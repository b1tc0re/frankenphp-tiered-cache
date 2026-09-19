package tiered

import "errors"

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
