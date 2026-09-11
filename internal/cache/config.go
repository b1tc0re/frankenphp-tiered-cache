package cache

import "fmt"

const (
	DefaultMaxMemoryBytes   int64 = 64 << 20
	DefaultMaxItemSizeBytes int64 = 4 << 20

	defaultShardCount = 64
	defaultLRUSamples = 5
	evictionTargetPct = 95
)

type MemoryConfig struct {
	MaxMemoryBytes   int64
	MaxItemSizeBytes int64
}

func (c MemoryConfig) normalized() (MemoryConfig, error) {
	if c.MaxMemoryBytes < 0 {
		return MemoryConfig{}, fmt.Errorf("cache: max memory must not be negative")
	}
	if c.MaxItemSizeBytes < 0 {
		return MemoryConfig{}, fmt.Errorf("cache: max item size must not be negative")
	}

	if c.MaxMemoryBytes == 0 {
		c.MaxMemoryBytes = DefaultMaxMemoryBytes
	}

	explicitItemLimit := c.MaxItemSizeBytes != 0
	if c.MaxItemSizeBytes == 0 {
		c.MaxItemSizeBytes = DefaultMaxItemSizeBytes
		if c.MaxItemSizeBytes > c.MaxMemoryBytes {
			c.MaxItemSizeBytes = c.MaxMemoryBytes
		}
	}

	if explicitItemLimit && c.MaxItemSizeBytes > c.MaxMemoryBytes {
		return MemoryConfig{}, fmt.Errorf("cache: max item size must not exceed max memory")
	}

	return c, nil
}
