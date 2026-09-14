package cache

import (
	"errors"
	"time"
)

var (
	ErrInvalidTTL   = errors.New("cache: ttl must be greater than zero")
	ErrNilValue     = errors.New("cache: nil value is not allowed")
	ErrItemTooLarge = errors.New("cache: item exceeds maximum size")
)

// Cache defines the shared semantics used by all cache backends.
//
// Implementations may retain values without copying them. Callers must treat a
// value as read-only after a successful Set or Forever call. Bytes returned by
// Get are also read-only and must not be modified.
type Cache interface {
	// Get returns a cached value and its remaining TTL.
	//
	// A nil value with a nil error means cache miss. A zero TTL means that the
	// value does not expire.
	Get(key string) ([]byte, time.Duration, error)
	Set(key string, value []byte, ttl time.Duration) (bool, error)
	Forever(key string, value []byte) (bool, error)
	Forget(key string) (bool, error)
	Touch(key string, ttl time.Duration) (bool, error)
	Flush() (bool, error)
	Close() error
}

// FenceToken identifies the version of a key mutation reserved in L2.
//
// A token is opaque to the composition layer. A fenced backend must reject a
// queued write when another instance has advanced the key's fence in the
// meantime.
type FenceToken string

// FencedCache extends Cache with atomic L2 fencing operations used by
// TieredCache when cross-instance invalidation is enabled.
type FencedCache interface {
	Cache

	ReserveFence(key string) (FenceToken, error)
	SetWithFence(key string, value []byte, ttl time.Duration, token FenceToken) (bool, error)
	ForeverWithFence(key string, value []byte, token FenceToken) (bool, error)
	ForgetWithFence(key string) (bool, error)
	ForgetIfFence(key string, token FenceToken) (bool, error)
	TouchWithFence(key string, ttl time.Duration) (bool, error)
}
