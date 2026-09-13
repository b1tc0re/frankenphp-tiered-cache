package tiered

import (
	"errors"
	"fmt"
	"time"
)

const (
	DefaultWriteQueueCapacity          = 1024
	DefaultWriteQueueMaxBytes    int64 = 64 << 20
	DefaultWriteQueueWaitTimeout       = 100 * time.Millisecond
	DefaultRecoveryInterval            = 5 * time.Second
)

var (
	ErrClosed                  = errors.New("tiered cache: cache is closed")
	ErrL2Unavailable           = errors.New("tiered cache: L2 is unavailable")
	ErrWriteQueueTimeout       = errors.New("tiered cache: write queue wait timeout")
	ErrWriteQueueValueTooLarge = errors.New("tiered cache: value exceeds write queue byte limit")
)

// Config contains settings owned by TieredCache.
//
// Backend configuration belongs to the backend constructors and is not part
// of this type.
type Config struct {
	WriteQueueCapacity    int
	WriteQueueMaxBytes    int64
	WriteQueueWaitTimeout time.Duration
	RecoveryInterval      time.Duration
	OnWriteError          func(key string, err error)
}

func (c Config) normalized() (Config, error) {
	if c.WriteQueueCapacity == 0 {
		c.WriteQueueCapacity = DefaultWriteQueueCapacity
	}
	if c.WriteQueueMaxBytes == 0 {
		c.WriteQueueMaxBytes = DefaultWriteQueueMaxBytes
	}
	if c.WriteQueueWaitTimeout == 0 {
		c.WriteQueueWaitTimeout = DefaultWriteQueueWaitTimeout
	}
	if c.RecoveryInterval == 0 {
		c.RecoveryInterval = DefaultRecoveryInterval
	}
	if c.WriteQueueCapacity < 0 {
		return Config{}, fmt.Errorf("tiered cache: write queue capacity must not be negative")
	}
	if c.WriteQueueMaxBytes < 0 {
		return Config{}, fmt.Errorf("tiered cache: write queue max bytes must not be negative")
	}
	if c.WriteQueueWaitTimeout < 0 {
		return Config{}, fmt.Errorf("tiered cache: write queue wait timeout must not be negative")
	}
	if c.RecoveryInterval < 0 {
		return Config{}, fmt.Errorf("tiered cache: recovery interval must not be negative")
	}

	return c, nil
}
