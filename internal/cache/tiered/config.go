package tiered

import (
	"errors"
	"fmt"
	"time"
)

const (
	DefaultRecoveryInterval = 5 * time.Second
)

var (
	ErrClosed        = errors.New("tiered cache: cache is closed")
	ErrL2Unavailable = errors.New("tiered cache: L2 is unavailable")
)

// Config contains settings owned by TieredCache.
//
// Backend configuration belongs to the backend constructors and is not part
// of this type.
type Config struct {
	RecoveryInterval time.Duration
}

func (c Config) normalized() (Config, error) {
	if c.RecoveryInterval == 0 {
		c.RecoveryInterval = DefaultRecoveryInterval
	}
	if c.RecoveryInterval < 0 {
		return Config{}, fmt.Errorf("tiered cache: recovery interval must not be negative")
	}

	return c, nil
}
