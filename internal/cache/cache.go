package cache

import (
	"errors"
	"time"
)

var (
	ErrInvalidTTL   = errors.New("cache: ttl must be greater than zero")
	ErrItemTooLarge = errors.New("cache: item exceeds maximum size")
	ErrCacheFull    = errors.New("cache: unable to free enough memory")
)

type Cache interface {
	Get(key string) ([]byte, bool, error)
	Set(key string, value []byte, ttl time.Duration) error
	Forever(key string, value []byte) error
	Forget(key string) error
	Touch(key string, ttl time.Duration) error
}
