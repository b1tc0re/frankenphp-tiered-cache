package invalidation

import (
	"context"
	"fmt"
)

const ProtocolVersion = 1

type EventType string

const (
	EventTypeInvalidate EventType = "invalidate"
	EventTypeFlush      EventType = "flush"
)

// Event is a cache invalidation message exchanged between cache instances.
//
// The origin is used by a publisher to ignore its own message. Key
// invalidation is deliberately idempotent, so the protocol does not need
// ordering or delivery acknowledgements.
type Event struct {
	Version int       `json:"v"`
	Type    EventType `json:"op"`
	Key     string    `json:"key"`
	Origin  string    `json:"origin,omitempty"`
}

func (e Event) Validate() error {
	if e.Version != ProtocolVersion {
		return fmt.Errorf("cache invalidation: unsupported protocol version %d", e.Version)
	}

	switch e.Type {
	case EventTypeInvalidate:
	case EventTypeFlush:
		if e.Key != "" {
			return fmt.Errorf("cache invalidation: key must be empty for %q event", e.Type)
		}
	default:
		return fmt.Errorf("cache invalidation: unsupported event type %q", e.Type)
	}

	return nil
}

// Subscription is a reconnectable stream of invalidation events.
type Subscription interface {
	Receive(context.Context) (Event, error)
	Close() error
}

// Bus transports invalidation events. The bus owns no cache state; the
// composition layer applies received events to its local L1 only.
type Bus interface {
	Publish(context.Context, Event) error
	Subscribe(context.Context) (Subscription, error)
	Close() error
}
