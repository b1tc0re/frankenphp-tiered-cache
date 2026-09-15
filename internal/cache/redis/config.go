package redis

import (
	"fmt"
	"time"
)

const (
	DefaultAddr      = "127.0.0.1:6379"
	DefaultKeyPrefix = "franken_cache:"
	// DefaultFenceLease bounds the lifetime of an unfinished reservation.
	DefaultFenceLease = 30 * time.Second
)

const flushScanCount int64 = 100

// Config contains settings owned by RedisCache.
//
// It intentionally does not contain settings for any other cache backend.
type Config struct {
	Addr      string
	Username  string
	Password  string
	DB        int
	KeyPrefix string
	// FenceLease must exceed the maximum expected queue and Redis operation time.
	FenceLease   time.Duration
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func (c Config) normalized() (Config, error) {
	if c.Addr == "" {
		c.Addr = DefaultAddr
	}
	if c.KeyPrefix == "" {
		c.KeyPrefix = DefaultKeyPrefix
	}
	if c.FenceLease == 0 {
		c.FenceLease = DefaultFenceLease
	}
	if c.DB < 0 {
		return Config{}, fmt.Errorf("redis: db must not be negative")
	}
	if c.DialTimeout < 0 {
		return Config{}, fmt.Errorf("redis: dial timeout must not be negative")
	}
	if c.ReadTimeout < 0 {
		return Config{}, fmt.Errorf("redis: read timeout must not be negative")
	}
	if c.WriteTimeout < 0 {
		return Config{}, fmt.Errorf("redis: write timeout must not be negative")
	}
	if c.FenceLease < 0 {
		return Config{}, fmt.Errorf("redis: fence lease must not be negative")
	}

	return c, nil
}
