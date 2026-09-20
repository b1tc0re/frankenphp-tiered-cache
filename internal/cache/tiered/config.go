package tiered

import (
	"errors"
	"fmt"
	"time"

	"github.com/b1tc0re/frankenphp-tiered-cache/internal/observability"
)

const (
	DefaultRecoveryInterval = 5 * time.Second
)

var (
	ErrClosed        = errors.New("tiered cache: cache is closed")
	ErrL2Unavailable = errors.New("tiered cache: L2 is unavailable")
	ErrPostCommit    = errors.New("tiered cache: mutation already committed")
)

// Config contains settings owned by TieredCache.
//
// Backend configuration belongs to the backend constructors and is not part
// of this type.
type Config struct {
	RecoveryInterval time.Duration
	Metrics          *observability.MetricsState
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
