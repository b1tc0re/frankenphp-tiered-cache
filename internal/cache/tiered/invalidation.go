package tiered

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	cacheinvalidation "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache/invalidation"
	"github.com/b1tc0re/frankenphp-tiered-cache/internal/observability"
)

const invalidationOriginSize = 16

func newInvalidationOrigin() (string, error) {
	origin := make([]byte, invalidationOriginSize)
	if _, err := cryptorand.Read(origin); err != nil {
		return "", fmt.Errorf("tiered cache: generate invalidation origin: %w", err)
	}

	return hex.EncodeToString(origin), nil
}

func (c *TieredCache) publishKeyInvalidation(key string) error {
	if c.invalidationBus == nil {
		return nil
	}

	err := c.invalidationBus.Publish(context.Background(), cacheinvalidation.Event{
		Version: cacheinvalidation.ProtocolVersion,
		Type:    cacheinvalidation.EventTypeInvalidate,
		Key:     key,
		Origin:  c.invalidationOrigin,
	})
	result := observability.InvalidationSuccess
	if err != nil {
		result = observability.InvalidationError
	}
	c.metrics.ObserveInvalidation(observability.InvalidationPublished, observability.InvalidationKey, result)
	return err
}

func (c *TieredCache) publishFlushInvalidation() error {
	if c.invalidationBus == nil {
		return nil
	}

	err := c.invalidationBus.Publish(context.Background(), cacheinvalidation.Event{
		Version: cacheinvalidation.ProtocolVersion,
		Type:    cacheinvalidation.EventTypeFlush,
		Origin:  c.invalidationOrigin,
	})
	result := observability.InvalidationSuccess
	if err != nil {
		result = observability.InvalidationError
	}
	c.metrics.ObserveInvalidation(observability.InvalidationPublished, observability.InvalidationFlush, result)
	return err
}

func (c *TieredCache) markPendingInvalidation(key string) {
	if c.invalidationBus == nil {
		return
	}

	c.pendingMu.Lock()
	if !c.pendingFlush {
		c.pendingInvalidations[key] = struct{}{}
		c.metrics.SetPendingInvalidations(uint64(len(c.pendingInvalidations)))
	}
	c.pendingMu.Unlock()
}

func (c *TieredCache) markPendingFlush() {
	if c.invalidationBus == nil {
		return
	}

	c.pendingMu.Lock()
	c.pendingFlush = true
	clear(c.pendingInvalidations)
	c.metrics.SetPendingInvalidations(0)
	c.metrics.SetPendingFlush(true)
	c.pendingMu.Unlock()
}

func (c *TieredCache) recoverPendingInvalidations() bool {
	if c.invalidationBus == nil {
		return true
	}

	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()

	if c.pendingFlush {
		if err := c.publishFlushInvalidation(); err != nil {
			return false
		}
		c.pendingFlush = false
		c.metrics.SetPendingFlush(false)
		return true
	}

	for key := range c.pendingInvalidations {
		if err := c.publishKeyInvalidation(key); err != nil {
			return false
		}
		delete(c.pendingInvalidations, key)
		c.metrics.SetPendingInvalidations(uint64(len(c.pendingInvalidations)))
	}
	return true
}

func waitForRetry(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *TieredCache) runInvalidationSubscriber(ctx context.Context) {
	defer close(c.invalidationDone)

	for {
		if ctx.Err() != nil || c.isClosed() {
			return
		}
		c.invalidationReady.Store(false)
		c.metrics.SetInvalidationReady(false)

		subscription, err := c.invalidationBus.Subscribe(ctx)
		if err != nil {
			if ctx.Err() != nil || c.isClosed() {
				return
			}
			c.degradeAndFlush(err)
			if !waitForRetry(ctx, c.recoveryInterval) {
				return
			}
			continue
		}

		if err := c.flushLocalL1(); err != nil {
			_ = subscription.Close()
			if ctx.Err() != nil || c.isClosed() {
				return
			}
			c.degradeAndFlush(err)
			if !waitForRetry(ctx, c.recoveryInterval) {
				return
			}
			continue
		}
		c.invalidationReady.Store(true)
		c.metrics.SetInvalidationReady(true)

		for {
			event, receiveErr := subscription.Receive(ctx)
			if receiveErr != nil {
				_ = subscription.Close()
				c.invalidationReady.Store(false)
				c.metrics.SetInvalidationReady(false)
				if ctx.Err() != nil || c.isClosed() {
					return
				}
				c.degradeAndFlush(receiveErr)
				break
			}

			if err := c.applyInvalidation(event); err != nil {
				_ = subscription.Close()
				c.invalidationReady.Store(false)
				c.metrics.SetInvalidationReady(false)
				if ctx.Err() != nil || c.isClosed() {
					return
				}
				c.degradeAndFlush(err)
				break
			}
		}

		if !waitForRetry(ctx, c.recoveryInterval) {
			return
		}
	}
}

func (c *TieredCache) isClosed() bool {
	c.mutationMu.Lock()
	closed := c.closed
	c.mutationMu.Unlock()
	return closed
}

func (c *TieredCache) flushLocalL1() error {
	c.recoveryMu.Lock()
	defer c.recoveryMu.Unlock()
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()

	if c.closed {
		return ErrClosed
	}
	c.mutationVersion.Add(1)
	_, err := flushL1(c.l1)
	return err
}

func (c *TieredCache) applyInvalidation(event cacheinvalidation.Event) error {
	if err := event.Validate(); err != nil {
		return err
	}
	typ := observability.InvalidationKey
	if event.Type == cacheinvalidation.EventTypeFlush {
		typ = observability.InvalidationFlush
	}
	if event.Origin == c.invalidationOrigin {
		c.metrics.ObserveInvalidation(observability.InvalidationReceived, typ, observability.InvalidationSuccess)
		return nil
	}

	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	if c.closed {
		return ErrClosed
	}

	c.mutationVersion.Add(1)
	if event.Type == cacheinvalidation.EventTypeFlush {
		_, err := flushL1(c.l1)
		result := observability.InvalidationSuccess
		if err != nil {
			result = observability.InvalidationError
		}
		c.metrics.ObserveInvalidation(observability.InvalidationReceived, typ, result)
		return err
	}
	_, err := c.l1.Forget(event.Key)
	result := observability.InvalidationSuccess
	if err != nil {
		result = observability.InvalidationError
	}
	c.metrics.ObserveInvalidation(observability.InvalidationReceived, typ, result)
	return err
}
