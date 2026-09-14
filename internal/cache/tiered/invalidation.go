package tiered

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	cacheinvalidation "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache/invalidation"
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

	return c.invalidationBus.Publish(context.Background(), cacheinvalidation.Event{
		Version: cacheinvalidation.ProtocolVersion,
		Type:    cacheinvalidation.EventTypeInvalidate,
		Key:     key,
		Origin:  c.invalidationOrigin,
	})
}

func (c *TieredCache) publishFlushInvalidation() error {
	if c.invalidationBus == nil {
		return nil
	}

	return c.invalidationBus.Publish(context.Background(), cacheinvalidation.Event{
		Version: cacheinvalidation.ProtocolVersion,
		Type:    cacheinvalidation.EventTypeFlush,
		Origin:  c.invalidationOrigin,
	})
}

func (c *TieredCache) runInvalidationSubscriber(
	ctx context.Context,
	subscription cacheinvalidation.Subscription,
) {
	defer close(c.invalidationDone)

	current := subscription
	for {
		for {
			event, receiveErr := current.Receive(ctx)
			if receiveErr != nil {
				_ = current.Close()
				if ctx.Err() != nil {
					return
				}
				c.invalidationReady.Store(false)
				c.degradeAndFlush(receiveErr)
				break
			}

			if event.Origin == c.invalidationOrigin {
				continue
			}
			if err := c.applyInvalidation(event); err != nil {
				c.degradeAndFlush(err)
			}
		}

		if !waitForRetry(ctx, c.recoveryInterval) {
			return
		}

		for {
			next, err := c.invalidationBus.Subscribe(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				c.invalidationReady.Store(false)
				c.degradeAndFlush(err)
				if !waitForRetry(ctx, c.recoveryInterval) {
					return
				}
				continue
			}

			// Pub/Sub is at-most-once. Anything published while this process was
			// disconnected may have been missed, so discard the complete local L1
			// before declaring the subscription ready again.
			c.invalidationReady.Store(false)
			if err := c.flushLocalL1(); err != nil {
				_ = next.Close()
				c.degradeAndFlush(err)
				if !waitForRetry(ctx, c.recoveryInterval) {
					return
				}
				continue
			}

			c.invalidationReady.Store(true)
			current = next
			break
		}
	}
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

func (c *TieredCache) flushLocalL1() error {
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	if c.closed {
		return nil
	}

	c.mutationVersion.Add(1)
	_, err := c.l1.Flush()
	return err
}

func (c *TieredCache) applyInvalidation(event cacheinvalidation.Event) error {
	if err := event.Validate(); err != nil {
		return err
	}

	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	if c.closed {
		return nil
	}

	c.mutationVersion.Add(1)
	switch event.Type {
	case cacheinvalidation.EventTypeInvalidate:
		c.bumpKeyGeneration(event.Key)
		_, err := c.l1.Forget(event.Key)
		return err
	case cacheinvalidation.EventTypeFlush:
		c.flushGeneration.Add(1)
		_, err := c.l1.Flush()
		return err
	default:
		return event.Validate()
	}
}
