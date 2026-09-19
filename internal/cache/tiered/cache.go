package tiered

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	cachecontract "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache"
	cacheinvalidation "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache/invalidation"
)

// TieredCache uses Redis as the authoritative store and MemoryCache as a
// local read-through cache. Mutations change Redis first, then update L1 and
// invalidate peers before returning.
type TieredCache struct {
	l1 cachecontract.Cache
	l2 cachecontract.Cache

	recoveryInterval time.Duration

	healthState atomic.Uint32
	stateErrMu  sync.RWMutex
	stateErr    error
	healthEpoch uint64

	recoveryWake chan struct{}
	recoveryStop chan struct{}
	recoveryDone chan struct{}
	recoveryMu   sync.Mutex

	// Pending entries contain only Pub/Sub messages that must be retried.
	// They never represent Redis data and recovery never deletes cache keys.
	pendingMu            sync.Mutex
	pendingInvalidations map[string]struct{}
	pendingFlush         bool

	mutationMu      sync.Mutex
	mutationVersion atomic.Uint64

	invalidationBus    cacheinvalidation.Bus
	invalidationOrigin string
	invalidationReady  atomic.Bool
	invalidationCancel context.CancelFunc
	invalidationDone   chan struct{}

	lifecycleMu sync.RWMutex
	closeOnce   sync.Once
	closeErr    error
	closed      bool
}

var _ cachecontract.Cache = (*TieredCache)(nil)

func New(config Config, l1, l2 cachecontract.Cache) (*TieredCache, error) {
	return newTieredCache(config, l1, l2, nil)
}

// NewWithInvalidation composes L1 and L2 with a cross-instance invalidation
// bus. Events contain no values; receivers only forget their local L1 entry.
func NewWithInvalidation(config Config, l1, l2 cachecontract.Cache, bus cacheinvalidation.Bus) (*TieredCache, error) {
	return newTieredCache(config, l1, l2, bus)
}

func newTieredCache(config Config, l1, l2 cachecontract.Cache, bus cacheinvalidation.Bus) (*TieredCache, error) {
	if l1 == nil {
		return nil, fmt.Errorf("tiered cache: L1 cache must not be nil")
	}
	if l2 == nil {
		return nil, fmt.Errorf("tiered cache: L2 cache must not be nil")
	}

	config, err := config.normalized()
	if err != nil {
		return nil, err
	}

	var origin string
	if bus != nil {
		origin, err = newInvalidationOrigin()
		if err != nil {
			return nil, err
		}
	}

	c := &TieredCache{
		l1:                   l1,
		l2:                   l2,
		recoveryInterval:     config.RecoveryInterval,
		recoveryWake:         make(chan struct{}, 1),
		recoveryStop:         make(chan struct{}),
		recoveryDone:         make(chan struct{}),
		pendingInvalidations: make(map[string]struct{}),
		invalidationBus:      bus,
		invalidationOrigin:   origin,
		invalidationDone:     make(chan struct{}),
	}
	if bus == nil {
		c.invalidationReady.Store(true)
		close(c.invalidationDone)
	} else {
		ctx, cancel := context.WithCancel(context.Background())
		c.invalidationCancel = cancel
		go c.runInvalidationSubscriber(ctx)
	}
	go c.runRecovery()

	return c, nil
}
