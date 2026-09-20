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
			c.metrics.ObserveLookup(observability.LookupL1Hit)
			return value, ttl, nil
		}

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
			c.metrics.ObserveL1Miss()
			c.metrics.ObserveLookup(observability.LookupL1Hit)
			c.mutationMu.Unlock()
			return current, currentTTL, nil
		}
		if l2Value == nil {
			c.metrics.ObserveL1Miss()
			c.metrics.ObserveLookup(observability.LookupMiss)
			c.mutationMu.Unlock()
			return nil, 0, nil
		}
		if l2TTL > 0 {
			l2TTL -= time.Since(started)
			if l2TTL <= 0 {
				c.metrics.ObserveL1Miss()
				c.metrics.ObserveLookup(observability.LookupMiss)
				c.mutationMu.Unlock()
				return nil, 0, nil
			}
			_, _ = c.l1.Set(key, l2Value, l2TTL)
		} else {
			_, _ = c.l1.Forever(key, l2Value)
		}
		c.metrics.ObserveL1Miss()
		c.metrics.ObserveLookup(observability.LookupL2Hit)
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
		missCounts := make(map[string]uint64, len(keys))
		var initialL1HitCount uint64
		var initialL1MissCount uint64
		for _, key := range keys {
			value, ttl, err := c.l1.Get(key)
			if err != nil {
				return nil, err
			}
			if value != nil {
				initialL1HitCount++
				results[key] = cachecontract.Item{Value: value, TTL: ttl}
				continue
			}
			initialL1MissCount++
			missCounts[key]++
			if missCounts[key] == 1 {
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
			c.metrics.ObserveLookups(observability.LookupL1Hit, initialL1HitCount)
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

		finalL1HitCount := initialL1HitCount
		var finalL2HitCount uint64
		var finalMissCount uint64

		for _, key := range misses {
			missCount := missCounts[key]
			current, currentTTL, currentErr := c.l1.Get(key)
			if currentErr != nil {
				c.mutationMu.Unlock()
				return nil, currentErr
			}
			if current != nil {
				finalL1HitCount += missCount
				results[key] = cachecontract.Item{Value: current, TTL: currentTTL}
				continue
			}

			item, found := l2Results[key]
			if !found {
				finalMissCount += missCount
				continue
			}

			ttl := item.TTL
			if ttl > 0 {
				ttl -= elapsed
				if ttl <= 0 {
					finalMissCount += missCount
					continue
				}
				_, _ = c.l1.Set(key, item.Value, ttl)
			} else {
				_, _ = c.l1.Forever(key, item.Value)
			}
			finalL2HitCount += missCount
			results[key] = cachecontract.Item{Value: item.Value, TTL: ttl}
		}
		c.metrics.ObserveL1Misses(initialL1MissCount)
		c.metrics.ObserveLookups(observability.LookupL1Hit, finalL1HitCount)
		c.metrics.ObserveLookups(observability.LookupL2Hit, finalL2HitCount)
		c.metrics.ObserveLookups(observability.LookupMiss, finalMissCount)
		c.mutationMu.Unlock()
		return results, nil
	}
}
