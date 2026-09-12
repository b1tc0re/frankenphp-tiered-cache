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

type Cache interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte, ttl time.Duration) (bool, error)
	Forever(key string, value []byte) (bool, error)
	Forget(key string) (bool, error)
	Touch(key string, ttl time.Duration) (bool, error)
	Flush() (bool, error)
	Close() error
}
