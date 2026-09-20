package tiered

import (
	"errors"
	"time"

	cachecontract "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache"
	"github.com/b1tc0re/frankenphp-tiered-cache/internal/observability"
)

func (c *TieredCache) observeL2(operation observability.Operation, started time.Time, err error) {
	if c.metrics == nil {
		return
	}
	class := observability.L2ErrorNone
	if err != nil {
		errorClass := observability.L2ErrorTransport
		if errors.Is(err, cachecontract.ErrRedisCommand) {
			errorClass = observability.L2ErrorCommand
		}
		class = errorClass
	}
	c.metrics.ObserveL2(operation, time.Since(started), class)
}

func (c *TieredCache) observeBatch(operation observability.Operation, items int) {
	if items > 0 {
		c.metrics.ObserveBatch(operation, uint64(items))
	}
}

func (c *TieredCache) observePostCommitError(operation observability.Operation, err error) {
	if c.metrics != nil && err != nil {
		c.metrics.ObservePostCommitError(operation)
	}
}
