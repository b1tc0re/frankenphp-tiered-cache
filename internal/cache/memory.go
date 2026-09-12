package cache

import (
	"fmt"
	"hash/maphash"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

const lruClockResolution = time.Second

type memoryEntry struct {
	value      []byte
	expiresAt  int64
	lastAccess atomic.Uint64
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
	lruClock    atomic.Uint64
	evictionMu  sync.Mutex
	lruSamples  int
	now         func() time.Time

	maintenanceStop chan struct{}
	maintenanceDone chan struct{}
	closeOnce       sync.Once
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

	cache := &MemoryCache{
		shards:          shards,
		hashSeed:        maphash.MakeSeed(),
		maxMemory:       cfg.MaxMemoryBytes,
		maxItemSize:     cfg.MaxItemSizeBytes,
		lruSamples:      lruSamples,
		now:             time.Now,
		maintenanceStop: make(chan struct{}),
		maintenanceDone: make(chan struct{}),
	}
	cache.lruClock.Store(1)
	go cache.runMaintenance()

	return cache, nil
}

// Get returns the cached value without copying it. A nil value with a nil error
// means cache miss. Returned bytes are read-only and must not be modified.
func (c *MemoryCache) Get(key string) ([]byte, error) {
	shard := c.shardFor(key)
	now := c.now().UnixNano()

	shard.mu.RLock()
	entry, ok := shard.entries[key]
	if !ok {
		shard.mu.RUnlock()
		return nil, nil
	}
	if entry.expiresAt > 0 && entry.expiresAt <= now {
		shard.mu.RUnlock()
		c.deleteExpired(shard, key, entry, now)
		return nil, nil
	}

	c.recordAccess(entry)
	value := entry.value
	shard.mu.RUnlock()

	return value, nil
}

// Set stores value with expiration without copying it. The caller transfers
// read-only ownership of value to the cache while the entry remains reachable.
func (c *MemoryCache) Set(key string, value []byte, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, ErrInvalidTTL
	}
	if value == nil {
		return false, ErrNilValue
	}

	return c.set(key, value, c.now().Add(ttl).UnixNano())
}

// Forever stores value without expiration and without copying it.
func (c *MemoryCache) Forever(key string, value []byte) (bool, error) {
	if value == nil {
		return false, ErrNilValue
	}

	return c.set(key, value, 0)
}

// Forget removes a live key. It returns false when the key is missing or has expired.
func (c *MemoryCache) Forget(key string) (bool, error) {
	shard := c.shardFor(key)
	now := c.now().UnixNano()

	shard.mu.Lock()
	entry, ok := shard.entries[key]
	if !ok {
		shard.mu.Unlock()
		return false, nil
	}

	delete(shard.entries, key)
	c.current.Add(-entry.cost)
	expired := entry.expiresAt > 0 && entry.expiresAt <= now
	shard.mu.Unlock()

	return !expired, nil
}

// Touch updates the TTL and access order of a live key. It returns false when the key is missing or expired.
func (c *MemoryCache) Touch(key string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, ErrInvalidTTL
	}

	now := c.now()
	nowUnixNano := now.UnixNano()
	shard := c.shardFor(key)
	expiresAt := now.Add(ttl).UnixNano()

	shard.mu.Lock()
	entry, ok := shard.entries[key]
	if !ok {
		shard.mu.Unlock()
		return false, nil
	}
	if entry.expiresAt > 0 && entry.expiresAt <= nowUnixNano {
		delete(shard.entries, key)
		c.current.Add(-entry.cost)
		shard.mu.Unlock()
		return false, nil
	}

	entry.expiresAt = expiresAt
	c.recordAccess(entry)
	shard.mu.Unlock()

	return true, nil
}

// Flush removes every entry from this MemoryCache instance.
func (c *MemoryCache) Flush() (bool, error) {
	c.evictionMu.Lock()
	defer c.evictionMu.Unlock()

	for i := range c.shards {
		c.shards[i].mu.Lock()
	}

	for i := range c.shards {
		clear(c.shards[i].entries)
	}
	c.current.Store(0)

	for i := len(c.shards) - 1; i >= 0; i-- {
		c.shards[i].mu.Unlock()
	}

	return true, nil
}

// Close stops MemoryCache background maintenance. The cache must not be used after Close.
func (c *MemoryCache) Close() error {
	c.closeOnce.Do(func() {
		close(c.maintenanceStop)
		<-c.maintenanceDone
	})

	return nil
}

func (c *MemoryCache) set(key string, value []byte, expiresAt int64) (bool, error) {
	cost := itemCost(key, value)
	if cost > c.maxItemSize {
		return false, fmt.Errorf(
			"%w: item size %d bytes exceeds limit %d bytes",
			ErrItemTooLarge,
			cost,
			c.maxItemSize,
		)
	}

	shard := c.shardFor(key)

	for {
		entry := &memoryEntry{
			value:     value,
			expiresAt: expiresAt,
			cost:      cost,
		}

		shard.mu.Lock()
		old := shard.entries[key]
		oldCost := int64(0)
		if old != nil {
			oldCost = old.cost
		}
		delta := cost - oldCost

		if delta <= 0 || c.tryReserve(delta) {
			c.recordAccess(entry)
			shard.entries[key] = entry
			if delta < 0 {
				c.current.Add(delta)
			}
			shard.mu.Unlock()
			return true, nil
		}
		shard.mu.Unlock()

		if !c.evictFor(delta) {
			return false, nil
		}
	}
}

func (c *MemoryCache) recordAccess(entry *memoryEntry) {
	storeMaxAccessClock(entry, c.lruClock.Load())
}

func storeMaxAccessClock(entry *memoryEntry, clock uint64) {
	for {
		current := entry.lastAccess.Load()
		if current >= clock {
			return
		}
		if entry.lastAccess.CompareAndSwap(current, clock) {
			return
		}
	}
}

func (c *MemoryCache) runMaintenance() {
	ticker := time.NewTicker(lruClockResolution)
	defer ticker.Stop()
	defer close(c.maintenanceDone)

	for {
		select {
		case <-ticker.C:
			c.lruClock.Add(1)
		case <-c.maintenanceStop:
			return
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

func (c *MemoryCache) evictFor(required int64) bool {
	c.evictionMu.Lock()
	defer c.evictionMu.Unlock()

	if required <= 0 || c.current.Load()+required <= c.maxMemory {
		return true
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

	return c.current.Load()+required <= c.maxMemory
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
	lastAccess uint64
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
	return int64(len(key) + cap(value))
}
