package tiered

import (
	"time"

	cachecontract "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache"
	"github.com/b1tc0re/frankenphp-tiered-cache/internal/observability"
)

func (c *TieredCache) Get(key string) ([]byte, time.Duration, error) {
	c.lifecycleMu.RLock()
	defer c.lifecycleMu.RUnlock()

	for {
		c.mutationMu.Lock()
		if c.closed {
			c.mutationMu.Unlock()
			return nil, 0, ErrClosed
		}
		if err := c.unavailableError(); err != nil {
			c.mutationMu.Unlock()
			return nil, 0, err
		}
		c.mutationMu.Unlock()

		value, ttl, err := c.l1.Get(key)
		if err != nil {
			return nil, 0, err
		}
		if value != nil {
			c.metrics.ObserveLookup(observability.LookupL1Hit)
			c.mutationMu.Lock()
			if c.closed {
				c.mutationMu.Unlock()
				return nil, 0, ErrClosed
			}
			if err := c.unavailableError(); err != nil {
				c.mutationMu.Unlock()
				return nil, 0, err
			}
			c.mutationMu.Unlock()
			return value, ttl, nil
		}
		c.metrics.ObserveL1Miss()

		version := c.mutationVersion.Load()
		started := time.Now()
		l2Value, l2TTL, l2Err := c.l2.Get(key)
		c.observeL2(observability.OperationGet, started, l2Err)

		c.mutationMu.Lock()
		if c.closed {
			c.mutationMu.Unlock()
			return nil, 0, ErrClosed
		}
		if version != c.mutationVersion.Load() {
			c.mutationMu.Unlock()
			continue
		}
		if err := c.unavailableError(); err != nil {
			c.mutationMu.Unlock()
			return nil, 0, err
		}
		if l2Err != nil {
			c.degradeLocked(l2Err, true)
			err := c.unavailableError()
			c.mutationMu.Unlock()
			return nil, 0, err
		}

		current, currentTTL, currentErr := c.l1.Get(key)
		if currentErr != nil {
			c.mutationMu.Unlock()
			return nil, 0, currentErr
		}
		if current != nil {
			c.metrics.ObserveLookup(observability.LookupL1Hit)
			c.mutationMu.Unlock()
			return current, currentTTL, nil
		}
		if l2Value == nil {
			c.metrics.ObserveLookup(observability.LookupMiss)
			c.mutationMu.Unlock()
			return nil, 0, nil
		}
		c.metrics.ObserveLookup(observability.LookupL2Hit)

		if l2TTL > 0 {
			l2TTL -= time.Since(started)
			if l2TTL <= 0 {
				c.mutationMu.Unlock()
				return nil, 0, nil
			}
			_, _ = c.l1.Set(key, l2Value, l2TTL)
		} else {
			_, _ = c.l1.Forever(key, l2Value)
		}
		c.mutationMu.Unlock()
		return l2Value, l2TTL, nil
	}
}

// GetMany reads L1 first and performs one L2 batch read for the remaining
// keys. The mutation version protects the L2 response from being warmed into
// L1 after a concurrent local or remote mutation.
func (c *TieredCache) GetMany(keys []string) (map[string]cachecontract.Item, error) {
	if len(keys) == 0 {
		return make(map[string]cachecontract.Item), nil
	}

	c.lifecycleMu.RLock()
	defer c.lifecycleMu.RUnlock()

	for {
		c.mutationMu.Lock()
		if c.closed {
			c.mutationMu.Unlock()
			return nil, ErrClosed
		}
		if err := c.unavailableError(); err != nil {
			c.mutationMu.Unlock()
			return nil, err
		}
		version := c.mutationVersion.Load()
		c.mutationMu.Unlock()

		results := make(map[string]cachecontract.Item, len(keys))
		misses := make([]string, 0, len(keys))
		seenMisses := make(map[string]struct{}, len(keys))
		for _, key := range keys {
			value, ttl, err := c.l1.Get(key)
			if err != nil {
				return nil, err
			}
			if value != nil {
				c.metrics.ObserveLookup(observability.LookupL1Hit)
				results[key] = cachecontract.Item{Value: value, TTL: ttl}
				continue
			}
			c.metrics.ObserveL1Miss()
			if _, ok := seenMisses[key]; !ok {
				seenMisses[key] = struct{}{}
				misses = append(misses, key)
			}
		}

		if len(misses) == 0 {
			c.mutationMu.Lock()
			if c.closed {
				c.mutationMu.Unlock()
				return nil, ErrClosed
			}
			if version != c.mutationVersion.Load() {
				c.mutationMu.Unlock()
				continue
			}
			if err := c.unavailableError(); err != nil {
				c.mutationMu.Unlock()
				return nil, err
			}
			c.mutationMu.Unlock()
			return results, nil
		}

		started := time.Now()
		l2Results, l2Err := c.l2.GetMany(misses)
		elapsed := time.Since(started)
		c.observeL2(observability.OperationGetMany, started, l2Err)
		c.observeBatch(observability.OperationGetMany, len(misses))

		c.mutationMu.Lock()
		if c.closed {
			c.mutationMu.Unlock()
			return nil, ErrClosed
		}
		if version != c.mutationVersion.Load() {
			c.mutationMu.Unlock()
			continue
		}
		if err := c.unavailableError(); err != nil {
			c.mutationMu.Unlock()
			return nil, err
		}
		if l2Err != nil {
			c.degradeLocked(l2Err, true)
			err := c.unavailableError()
			c.mutationMu.Unlock()
			return nil, err
		}

		for _, key := range misses {
			current, currentTTL, currentErr := c.l1.Get(key)
			if currentErr != nil {
				c.mutationMu.Unlock()
				return nil, currentErr
			}
			if current != nil {
				c.metrics.ObserveLookup(observability.LookupL1Hit)
				results[key] = cachecontract.Item{Value: current, TTL: currentTTL}
				continue
			}

			item, found := l2Results[key]
			if !found {
				c.metrics.ObserveLookup(observability.LookupMiss)
				continue
			}
			c.metrics.ObserveLookup(observability.LookupL2Hit)

			ttl := item.TTL
			if ttl > 0 {
				ttl -= elapsed
				if ttl <= 0 {
					continue
				}
				_, _ = c.l1.Set(key, item.Value, ttl)
			} else {
				_, _ = c.l1.Forever(key, item.Value)
			}
			results[key] = cachecontract.Item{Value: item.Value, TTL: ttl}
		}
		c.mutationMu.Unlock()
		return results, nil
	}
}
