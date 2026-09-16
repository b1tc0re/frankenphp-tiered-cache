package cache

import (
	"errors"
	"time"
)

var (
	ErrInvalidTTL        = errors.New("cache: ttl must be greater than zero")
	ErrNilValue          = errors.New("cache: nil value is not allowed")
	ErrItemTooLarge      = errors.New("cache: item exceeds maximum size")
	ErrInsufficientMemory = errors.New("cache: insufficient memory")
	ErrCounterOverflow   = errors.New("cache: counter overflow")
)

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
	Increment(key string, value int64) (int64, error)
	Decrement(key string, value int64) (int64, error)
	Close() error
}
