package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	cacheinvalidation "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache/invalidation"
	goredis "github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/push"
)

const (
	invalidationChannelSuffix       = "__invalidation"
	invalidationTrackingKeySuffix   = "\x00frankenphp-tiered-cache:l1-generation"
	invalidationTrackingMarkerValue = "1"
	invalidationTrackingProbeEvery  = 250 * time.Millisecond
	invalidationTrackingProbeLimit  = 2 * time.Second
	invalidationTrackingBufferSize  = 256
)

type invalidationResult struct {
	event cacheinvalidation.Event
	err   error
}

// InvalidationBus transports application invalidations through Redis Pub/Sub
// and full-cache resets through server-assisted tracking of one shared marker.
// The marker is stable for a database and key prefix, so process restarts do
// not create additional Redis keys.
type InvalidationBus struct {
	client      *goredis.Client
	channel     string
	trackingKey string

	trackingMu  sync.RWMutex
	trackingSub *redisInvalidationSubscription

	closeOnce sync.Once
	closeErr  error
}

var _ cacheinvalidation.Bus = (*InvalidationBus)(nil)
var _ push.NotificationHandler = (*InvalidationBus)(nil)

func NewInvalidationBus(config Config) (*InvalidationBus, error) {
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}

	client := goredis.NewClient(&goredis.Options{
		Addr:        config.Addr,
		Username:    config.Username,
		Password:    config.Password,
		DB:          config.DB,
		DialTimeout: config.DialTimeout,
		// RESP3 delivers CLIENT TRACKING invalidations as push notifications.
		Protocol: 3,
		// Pub/Sub waits while idle; Receive uses its context for cancellation.
		ReadTimeout:  0,
		WriteTimeout: config.WriteTimeout,
	})

	bus := &InvalidationBus{
		client:      client,
		channel:     invalidationChannel(config.KeyPrefix),
		trackingKey: config.KeyPrefix + invalidationTrackingKeySuffix,
	}
	if err := client.RegisterPushNotificationHandler("invalidate", bus, true); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("cache invalidation: register Redis tracking handler: %w", err)
	}

	return bus, nil
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
	subscriptionCtx, cancel := context.WithCancel(ctx)
	pubsub := b.client.Subscribe(subscriptionCtx, b.channel)
	if _, err := pubsub.Receive(subscriptionCtx); err != nil {
		cancel()
		_ = pubsub.Close()
		return nil, err
	}

	subscription := &redisInvalidationSubscription{
		bus:              b,
		pubsub:           pubsub,
		ctx:              subscriptionCtx,
		cancel:           cancel,
		pubsubMessages:   make(chan invalidationResult, 1),
		trackingMessages: make(chan invalidationResult, invalidationTrackingBufferSize),
		trackingOverflow: make(chan struct{}, 1),
		trackingErrors:   make(chan error, 1),
		refreshMarker:    make(chan struct{}, 1),
	}

	b.trackingMu.Lock()
	if b.trackingSub != nil {
		b.trackingMu.Unlock()
		cancel()
		_ = pubsub.Close()
		return nil, fmt.Errorf("cache invalidation: Redis tracking subscription already active")
	}
	b.trackingSub = subscription
	b.trackingMu.Unlock()

	trackingConn := b.client.Conn()
	subscription.trackingConn = trackingConn
	clientID, err := trackingConn.ClientID(subscriptionCtx).Result()
	if err != nil {
		_ = subscription.Close()
		return nil, fmt.Errorf("cache invalidation: identify Redis tracking connection: %w", err)
	}
	subscription.trackingClientID = clientID
	if err := subscription.enableTracking(subscriptionCtx); err != nil {
		_ = subscription.Close()
		return nil, fmt.Errorf("cache invalidation: enable Redis key tracking: %w", err)
	}
	if err := subscription.ensureTrackingMarker(subscriptionCtx); err != nil {
		_ = subscription.Close()
		return nil, fmt.Errorf("cache invalidation: initialize Redis tracking marker: %w", err)
	}

	subscription.wg.Add(2)
	go subscription.receivePubSub()
	go subscription.monitorTracking()

	return subscription, nil
}

// HandlePushNotification receives Redis RESP3 client-tracking invalidations.
// A null key list means Redis flushed the selected database or all databases.
func (b *InvalidationBus) HandlePushNotification(
	_ context.Context,
	_ push.NotificationHandlerContext,
	notification []interface{},
) error {
	b.trackingMu.RLock()
	subscription := b.trackingSub
	b.trackingMu.RUnlock()
	if subscription == nil {
		return nil
	}

	events, refreshMarker, err := decodeTrackingInvalidations(notification, b.trackingKey)
	if err != nil {
		subscription.enqueueTracking(invalidationResult{err: err})
		return nil
	}
	for _, event := range events {
		subscription.enqueueTracking(invalidationResult{event: event})
	}
	if refreshMarker {
		select {
		case subscription.refreshMarker <- struct{}{}:
		default:
		}
	}

	return nil
}

func decodeTrackingInvalidations(
	notification []interface{},
	trackingKey string,
) ([]cacheinvalidation.Event, bool, error) {
	if len(notification) != 2 {
		return nil, false, fmt.Errorf("cache invalidation: malformed Redis tracking notification")
	}
	name, ok := notification[0].(string)
	if !ok || name != "invalidate" {
		return nil, false, fmt.Errorf("cache invalidation: unexpected Redis push notification")
	}

	if notification[1] == nil {
		return []cacheinvalidation.Event{redisFlushEvent()}, true, nil
	}

	var keys []interface{}
	switch value := notification[1].(type) {
	case []interface{}:
		keys = value
	case []string:
		keys = make([]interface{}, len(value))
		for i := range value {
			keys[i] = value[i]
		}
	default:
		return nil, false, fmt.Errorf("cache invalidation: malformed Redis tracking key list %T", notification[1])
	}

	var markerChanged bool
	for _, value := range keys {
		key, ok := redisTrackingKey(value)
		if !ok {
			return nil, false, fmt.Errorf("cache invalidation: malformed Redis tracking key %T", value)
		}
		markerChanged = markerChanged || key == trackingKey
	}

	if markerChanged {
		return []cacheinvalidation.Event{redisFlushEvent()}, true, nil
	}
	return nil, false, nil
}

func redisTrackingKey(value interface{}) (string, bool) {
	switch key := value.(type) {
	case string:
		return key, true
	case []byte:
		return string(key), true
	default:
		return "", false
	}
}

func redisFlushEvent() cacheinvalidation.Event {
	return cacheinvalidation.Event{
		Version: cacheinvalidation.ProtocolVersion,
		Type:    cacheinvalidation.EventTypeFlush,
	}
}

func (b *InvalidationBus) Close() error {
	b.closeOnce.Do(func() {
		b.closeErr = b.client.Close()
	})

	return b.closeErr
}

type redisInvalidationSubscription struct {
	bus              *InvalidationBus
	pubsub           *goredis.PubSub
	trackingConn     *goredis.Conn
	trackingClientID int64
	ctx              context.Context
	cancel           context.CancelFunc
	pubsubMessages   chan invalidationResult
	trackingMessages chan invalidationResult
	trackingOverflow chan struct{}
	trackingErrors   chan error
	refreshMarker    chan struct{}

	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

func (s *redisInvalidationSubscription) enableTracking(ctx context.Context) error {
	return s.trackingConn.ClientTracking(ctx, true, nil).Err()
}

func (s *redisInvalidationSubscription) ensureTrackingMarker(ctx context.Context) error {
	// All instances share this key. SETNX avoids per-process keys; GET registers
	// the control key with CLIENT TRACKING on this dedicated connection.
	if _, err := s.trackingConn.SetNX(ctx, s.bus.trackingKey, invalidationTrackingMarkerValue, 0).Result(); err != nil {
		return err
	}
	if _, err := s.trackingConn.Get(ctx, s.bus.trackingKey).Result(); err != nil {
		return err
	}
	return nil
}

func (s *redisInvalidationSubscription) refreshTrackingMarker() error {
	ctx, cancel := context.WithTimeout(s.ctx, invalidationTrackingProbeLimit)
	defer cancel()
	return s.ensureTrackingMarker(ctx)
}

func (s *redisInvalidationSubscription) receivePubSub() {
	defer s.wg.Done()
	for {
		message, err := s.pubsub.ReceiveMessage(s.ctx)
		if err != nil {
			if s.ctx.Err() == nil {
				s.sendPubSub(invalidationResult{err: fmt.Errorf("cache invalidation: receive Redis Pub/Sub event: %w", err)})
			}
			return
		}

		var event cacheinvalidation.Event
		if err := json.Unmarshal([]byte(message.Payload), &event); err != nil {
			s.sendPubSub(invalidationResult{err: fmt.Errorf("cache invalidation: decode event: %w", err)})
			return
		}
		if err := event.Validate(); err != nil {
			s.sendPubSub(invalidationResult{err: err})
			return
		}
		if !s.sendPubSub(invalidationResult{event: event}) {
			return
		}
	}
}

func (s *redisInvalidationSubscription) monitorTracking() {
	defer s.wg.Done()
	ticker := time.NewTicker(invalidationTrackingProbeEvery)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.refreshMarker:
			if err := s.refreshTrackingMarker(); err != nil {
				s.sendTrackingError(fmt.Errorf("cache invalidation: restore Redis tracking marker: %w", err))
				return
			}
		case <-ticker.C:
			probeCtx, cancel := context.WithTimeout(s.ctx, invalidationTrackingProbeLimit)
			clientID, err := s.trackingConn.ClientID(probeCtx).Result()
			cancel()
			if err != nil {
				if s.ctx.Err() == nil {
					s.sendTrackingError(fmt.Errorf("cache invalidation: check Redis tracking connection: %w", err))
				}
				return
			}
			if clientID == s.trackingClientID {
				select {
				case <-s.refreshMarker:
					if err := s.refreshTrackingMarker(); err != nil {
						s.sendTrackingError(fmt.Errorf("cache invalidation: restore Redis tracking marker: %w", err))
						return
					}
				default:
				}
				continue
			}
			s.sendTrackingError(fmt.Errorf("cache invalidation: Redis tracking connection changed from %d to %d", s.trackingClientID, clientID))
			return
		}
	}
}

func (s *redisInvalidationSubscription) sendPubSub(result invalidationResult) bool {
	select {
	case s.pubsubMessages <- result:
		return true
	case <-s.ctx.Done():
		return false
	}
}

func (s *redisInvalidationSubscription) enqueueTracking(result invalidationResult) {
	select {
	case s.trackingMessages <- result:
	default:
		// Losing individual key events is safe only if the whole local L1 is
		// discarded, so coalesce a full event buffer into a flush signal.
		select {
		case s.trackingOverflow <- struct{}{}:
		default:
		}
	}
}

func (s *redisInvalidationSubscription) sendTrackingError(err error) {
	select {
	case s.trackingErrors <- err:
	default:
	}
}

func (s *redisInvalidationSubscription) Receive(ctx context.Context) (cacheinvalidation.Event, error) {
	select {
	case err := <-s.trackingErrors:
		return cacheinvalidation.Event{}, err
	default:
	}

	select {
	case result := <-s.pubsubMessages:
		return result.event, result.err
	case result := <-s.trackingMessages:
		return result.event, result.err
	case <-s.trackingOverflow:
		return redisFlushEvent(), nil
	case err := <-s.trackingErrors:
		return cacheinvalidation.Event{}, err
	case <-ctx.Done():
		return cacheinvalidation.Event{}, ctx.Err()
	case <-s.ctx.Done():
		return cacheinvalidation.Event{}, s.ctx.Err()
	}
}

func (s *redisInvalidationSubscription) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		if s.pubsub != nil {
			s.closeErr = errors.Join(s.closeErr, s.pubsub.Close())
		}
		s.wg.Wait()
		if s.trackingConn != nil {
			s.closeErr = errors.Join(s.closeErr, s.trackingConn.Close())
		}
		s.bus.trackingMu.Lock()
		if s.bus.trackingSub == s {
			s.bus.trackingSub = nil
		}
		s.bus.trackingMu.Unlock()
	})

	return s.closeErr
}

func invalidationChannel(prefix string) string {
	return prefix + invalidationChannelSuffix
}
