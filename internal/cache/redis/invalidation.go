package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	cacheinvalidation "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache/invalidation"
	goredis "github.com/redis/go-redis/v9"
)

const invalidationChannelSuffix = "__invalidation"

// InvalidationBus transports cache invalidation events through Redis Pub/Sub.
// It uses a separate Redis client from RedisCache so a blocked subscriber
// connection cannot consume the backend's regular connection resources.
type InvalidationBus struct {
	client  *goredis.Client
	channel string

	closeOnce sync.Once
	closeErr  error
}

var _ cacheinvalidation.Bus = (*InvalidationBus)(nil)

func NewInvalidationBus(config Config) (*InvalidationBus, error) {
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}

	return &InvalidationBus{
		client: goredis.NewClient(&goredis.Options{
			Addr:        config.Addr,
			Username:    config.Username,
			Password:    config.Password,
			DB:          config.DB,
			DialTimeout: config.DialTimeout,
			// A Pub/Sub subscriber must wait for messages indefinitely. The
			// Receive context, not an idle read timeout, controls shutdown.
			ReadTimeout:  0,
			WriteTimeout: config.WriteTimeout,
		}),
		channel: invalidationChannelForDB(config.KeyPrefix, config.DB),
	}, nil
}

func (b *InvalidationBus) Publish(ctx context.Context, event cacheinvalidation.Event) error {
	if err := event.Validate(); err != nil {
		return err
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("cache invalidation: encode event: %w", err)
	}

	return b.client.Publish(ctx, b.channel, payload).Err()
}

func (b *InvalidationBus) Subscribe(ctx context.Context) (cacheinvalidation.Subscription, error) {
	pubsub := b.client.Subscribe(ctx, b.channel)
	subscription := &redisInvalidationSubscription{
		pubsub: pubsub,
		stop:   make(chan struct{}),
	}
	go subscription.watchContext(ctx)

	if _, err := pubsub.Receive(ctx); err != nil {
		_ = subscription.Close()
		return nil, err
	}

	return subscription, nil
}

func (b *InvalidationBus) Close() error {
	b.closeOnce.Do(func() {
		b.closeErr = b.client.Close()
	})

	return b.closeErr
}

type redisInvalidationSubscription struct {
	pubsub *goredis.PubSub
	stop   chan struct{}

	closeOnce sync.Once
	closeErr  error
}

func (s *redisInvalidationSubscription) watchContext(ctx context.Context) {
	select {
	case <-ctx.Done():
		_ = s.Close()
	case <-s.stop:
	}
}

func (s *redisInvalidationSubscription) Receive(ctx context.Context) (cacheinvalidation.Event, error) {
	message, err := s.pubsub.ReceiveMessage(ctx)
	if err != nil {
		return cacheinvalidation.Event{}, err
	}

	var event cacheinvalidation.Event
	if err := json.Unmarshal([]byte(message.Payload), &event); err != nil {
		return cacheinvalidation.Event{}, fmt.Errorf("cache invalidation: decode event: %w", err)
	}
	if err := event.Validate(); err != nil {
		return cacheinvalidation.Event{}, err
	}

	return event, nil
}

func (s *redisInvalidationSubscription) Close() error {
	s.closeOnce.Do(func() {
		close(s.stop)
		s.closeErr = s.pubsub.Close()
	})

	return s.closeErr
}

func invalidationChannel(prefix string) string {
	return prefix + invalidationChannelSuffix
}

// Redis Pub/Sub channels are server-wide rather than scoped to a selected DB.
// Include DB explicitly so caches with the same key prefix in different Redis
// databases do not invalidate each other's process-local L1 entries.
func invalidationChannelForDB(prefix string, db int) string {
	return fmt.Sprintf("%s:db:%d", invalidationChannel(prefix), db)
}
