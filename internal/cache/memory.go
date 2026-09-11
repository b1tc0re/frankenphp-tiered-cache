package cache

import (
	"hash/maphash"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

const entryOverheadBytes int64 = 64

type memoryEntry struct {
	value      []byte
	expiresAt  int64
	lastAccess atomic.Int64
	cost       int64
}

type memoryShard struct {
	mu      sync.RWMutex
	entries map[string]*memoryEntry
}

type MemoryCache struct {
	shards      []memoryShard
	hashSeed    maphash.Seed
	maxMemory   int64
	maxItemSize int64
	current     atomic.Int64
	evictionMu  sync.Mutex
	lruSamples  int
	now         func() time.Time
}

var _ Cache = (*MemoryCache)(nil)

func NewMemoryCache(config MemoryConfig) (*MemoryCache, error) {
	return newMemoryCache(config, defaultShardCount, defaultLRUSamples)
}

func newMemoryCache(config MemoryConfig, shardCount, lruSamples int) (*MemoryCache, error) {
	cfg, err := config.normalized()
	if err != nil {
		return nil, err
	}
	if shardCount <= 0 {
		shardCount = defaultShardCount
	}
	if lruSamples <= 0 {
		lruSamples = defaultLRUSamples
	}

	shards := make([]memoryShard, shardCount)
	for i := range shards {
		shards[i].entries = make(map[string]*memoryEntry)
	}

	return &MemoryCache{
		shards:      shards,
		hashSeed:    maphash.MakeSeed(),
		maxMemory:   cfg.MaxMemoryBytes,
		maxItemSize: cfg.MaxItemSizeBytes,
		lruSamples:  lruSamples,
		now:         time.Now,
	}, nil
}

// Get returns the cached value without copying it. The returned bytes are
// read-only and must not be modified by the caller.
func (c *MemoryCache) Get(key string) ([]byte, bool, error) {
	shard := c.shardFor(key)
	now := c.now().UnixNano()

	shard.mu.RLock()
	entry, ok := shard.entries[key]
	if !ok {
		shard.mu.RUnlock()
		return nil, false, nil
	}
	if entry.expiresAt > 0 && entry.expiresAt <= now {
		shard.mu.RUnlock()
		c.deleteExpired(shard, key, entry, now)
		return nil, false, nil
	}

	entry.lastAccess.Store(now)
	value := entry.value
	shard.mu.RUnlock()

	return value, true, nil
}

// Set stores value without copying it. The caller transfers read-only ownership
// of the byte slice to the cache for as long as the entry remains reachable.
func (c *MemoryCache) Set(key string, value []byte, ttl time.Duration) error {
	if ttl <= 0 {
		return ErrInvalidTTL
	}

	return c.set(key, value, c.now().Add(ttl).UnixNano())
}

// Forever stores value without expiration and without copying it.
func (c *MemoryCache) Forever(key string, value []byte) error {
	return c.set(key, value, 0)
}

func (c *MemoryCache) Forget(key string) error {
	shard := c.shardFor(key)
	shard.mu.Lock()
	if entry, ok := shard.entries[key]; ok {
		delete(shard.entries, key)
		c.current.Add(-entry.cost)
	}
	shard.mu.Unlock()

	return nil
}

func (c *MemoryCache) Touch(key string, ttl time.Duration) error {
	if ttl <= 0 {
		return ErrInvalidTTL
	}

	now := c.now()
	shard := c.shardFor(key)
	expiresAt := now.Add(ttl).UnixNano()

	shard.mu.Lock()
	if entry, ok := shard.entries[key]; ok {
		if entry.expiresAt > 0 && entry.expiresAt <= now.UnixNano() {
			delete(shard.entries, key)
			c.current.Add(-entry.cost)
		} else {
			entry.expiresAt = expiresAt
		}
	}
	shard.mu.Unlock()

	return nil
}

func (c *MemoryCache) set(key string, value []byte, expiresAt int64) error {
	cost := itemCost(key, value)
	if int64(cap(value)) > c.maxItemSize || cost > c.maxMemory {
		return ErrItemTooLarge
	}

	shard := c.shardFor(key)

	for {
		now := c.now().UnixNano()
		entry := &memoryEntry{
			value:     value,
			expiresAt: expiresAt,
			cost:      cost,
		}
		entry.lastAccess.Store(now)

		shard.mu.Lock()
		old := shard.entries[key]
		oldCost := int64(0)
		if old != nil {
			oldCost = old.cost
		}
		delta := cost - oldCost

		if delta <= 0 || c.tryReserve(delta) {
			shard.entries[key] = entry
			if delta < 0 {
				c.current.Add(delta)
			}
			shard.mu.Unlock()
			return nil
		}
		shard.mu.Unlock()

		if err := c.evictFor(delta); err != nil {
			return err
		}
	}
}

func (c *MemoryCache) tryReserve(bytes int64) bool {
	if bytes <= 0 {
		return true
	}

	for {
		current := c.current.Load()
		if current+bytes > c.maxMemory {
			return false
		}
		if c.current.CompareAndSwap(current, current+bytes) {
			return true
		}
	}
}

func (c *MemoryCache) evictFor(required int64) error {
	c.evictionMu.Lock()
	defer c.evictionMu.Unlock()

	if required <= 0 || c.current.Load()+required <= c.maxMemory {
		return nil
	}

	now := c.now().UnixNano()
	c.purgeExpired(now)

	target := c.maxMemory - required
	lowWater := c.maxMemory * evictionTargetPct / 100
	if lowWater < target {
		target = lowWater
	}
	if target < 0 {
		target = 0
	}

	for c.current.Load() > target {
		if !c.evictOneLRU() {
			break
		}
	}

	if c.current.Load()+required > c.maxMemory {
		return ErrCacheFull
	}

	return nil
}

func (c *MemoryCache) purgeExpired(now int64) {
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()
		for key, entry := range shard.entries {
			if entry.expiresAt > 0 && entry.expiresAt <= now {
				delete(shard.entries, key)
				c.current.Add(-entry.cost)
			}
		}
		shard.mu.Unlock()
	}
}

type evictionCandidate struct {
	shard      *memoryShard
	key        string
	entry      *memoryEntry
	lastAccess int64
}

func (c *MemoryCache) evictOneLRU() bool {
	var oldest *evictionCandidate

	attempts := c.lruSamples * 4
	if attempts < len(c.shards) {
		attempts = len(c.shards)
	}

	sampled := 0
	for i := 0; i < attempts && sampled < c.lruSamples; i++ {
		shard := &c.shards[rand.IntN(len(c.shards))]
		shard.mu.RLock()
		for key, entry := range shard.entries {
			candidate := evictionCandidate{
				shard:      shard,
				key:        key,
				entry:      entry,
				lastAccess: entry.lastAccess.Load(),
			}
			if oldest == nil || candidate.lastAccess < oldest.lastAccess {
				copyCandidate := candidate
				oldest = &copyCandidate
			}
			sampled++
			break
		}
		shard.mu.RUnlock()
	}

	if oldest == nil {
		// Sparse caches can miss all random shards. Fall back to the first
		// available entry so eviction can always make progress.
		for i := range c.shards {
			shard := &c.shards[i]
			shard.mu.RLock()
			for key, entry := range shard.entries {
				oldest = &evictionCandidate{
					shard:      shard,
					key:        key,
					entry:      entry,
					lastAccess: entry.lastAccess.Load(),
				}
				break
			}
			shard.mu.RUnlock()
			if oldest != nil {
				break
			}
		}
	}

	if oldest == nil {
		return false
	}

	oldest.shard.mu.Lock()
	current, ok := oldest.shard.entries[oldest.key]
	if ok && current == oldest.entry {
		delete(oldest.shard.entries, oldest.key)
		c.current.Add(-oldest.entry.cost)
		oldest.shard.mu.Unlock()
		return true
	}
	oldest.shard.mu.Unlock()

	return true
}

func (c *MemoryCache) deleteExpired(shard *memoryShard, key string, expected *memoryEntry, now int64) {
	shard.mu.Lock()
	current, ok := shard.entries[key]
	if ok && current == expected && current.expiresAt > 0 && current.expiresAt <= now {
		delete(shard.entries, key)
		c.current.Add(-current.cost)
	}
	shard.mu.Unlock()
}

func (c *MemoryCache) shardFor(key string) *memoryShard {
	index := maphash.String(c.hashSeed, key) % uint64(len(c.shards))
	return &c.shards[index]
}

func itemCost(key string, value []byte) int64 {
	return int64(len(key)+cap(value)) + entryOverheadBytes
}
