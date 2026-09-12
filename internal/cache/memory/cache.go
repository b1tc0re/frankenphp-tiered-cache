package memory

import (
	"fmt"
	"hash/maphash"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	cachecontract "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache"
)

const (
	lruClockResolution      = time.Second
	maxEvictionRetries      = 3
	backgroundCleanupSample = 64
)

type memoryEntry struct {
	value      []byte
	expiresAt  time.Time
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

var _ cachecontract.Cache = (*MemoryCache)(nil)

func New(config Config) (*MemoryCache, error) {
	return newMemoryCache(config, defaultShardCount, defaultLRUSamples)
}

func newMemoryCache(config Config, shardCount, lruSamples int) (*MemoryCache, error) {
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

	shard.mu.RLock()
	entry, ok := shard.entries[key]
	if !ok {
		shard.mu.RUnlock()
		return nil, nil
	}

	now := c.now()
	if isExpired(entry.expiresAt, now) {
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
		return false, cachecontract.ErrInvalidTTL
	}
	if value == nil {
		return false, cachecontract.ErrNilValue
	}
	return c.set(key, value, ttl, false)
}

// Forever stores value without expiration and without copying it.
func (c *MemoryCache) Forever(key string, value []byte) (bool, error) {
	if value == nil {
		return false, cachecontract.ErrNilValue
	}
	return c.set(key, value, 0, true)
}

// Forget removes a live key. It returns false when the key is missing or has expired.
func (c *MemoryCache) Forget(key string) (bool, error) {
	shard := c.shardFor(key)

	shard.mu.Lock()
	entry, ok := shard.entries[key]
	if !ok {
		shard.mu.Unlock()
		return false, nil
	}

	now := c.now()
	delete(shard.entries, key)
	c.current.Add(-entry.cost)
	expired := isExpired(entry.expiresAt, now)
	shard.mu.Unlock()
	return !expired, nil
}

// Touch updates the TTL and access order of a live key. It returns false when the key is missing or expired.
func (c *MemoryCache) Touch(key string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, cachecontract.ErrInvalidTTL
	}

	shard := c.shardFor(key)
	shard.mu.Lock()
	entry, ok := shard.entries[key]
	if !ok {
		shard.mu.Unlock()
		return false, nil
	}

	now := c.now()
	if isExpired(entry.expiresAt, now) {
		delete(shard.entries, key)
		c.current.Add(-entry.cost)
		shard.mu.Unlock()
		return false, nil
	}

	entry.expiresAt = now.Add(ttl)
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
		c.shards[i].entries = make(map[string]*memoryEntry)
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

func (c *MemoryCache) set(key string, value []byte, ttl time.Duration, forever bool) (bool, error) {
	cost := itemCost(key, value)
	if cost > c.maxItemSize {
		return false, fmt.Errorf(
			"%w: item size %d bytes exceeds limit %d bytes",
			cachecontract.ErrItemTooLarge,
			cost,
			c.maxItemSize,
		)
	}

	shard := c.shardFor(key)
	for {
		shard.mu.Lock()
		old := shard.entries[key]
		oldCost := int64(0)
		if old != nil {
			oldCost = old.cost
		}
		delta := cost - oldCost

		if delta <= 0 || c.tryReserve(delta) {
			expiresAt := time.Time{}
			if !forever {
				expiresAt = c.now().Add(ttl)
			}
			entry := &memoryEntry{
				value:     value,
				expiresAt: expiresAt,
				cost:      cost,
			}
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

	nextCleanupShard := 0

	for {
		select {
		case now := <-ticker.C:
			c.lruClock.Add(1)
			nextCleanupShard = c.purgeExpiredBackgroundBatch(now, nextCleanupShard)
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

	now := c.now()
	c.purgeExpired(now)

	target := c.maxMemory - required
	lowWater := c.maxMemory * evictionTargetPct / 100
	if lowWater < target {
		target = lowWater
	}
	if target < 0 {
		target = 0
	}

	failedEvictions := 0
	for c.current.Load() > target {
		before := c.current.Load()
		if c.evictOneLRU() {
			failedEvictions = 0
			continue
		}
		if c.current.Load() < before {
			failedEvictions = 0
			continue
		}
		failedEvictions++
		if failedEvictions >= maxEvictionRetries {
			break
		}
	}
	return c.current.Load()+required <= c.maxMemory
}

func (c *MemoryCache) purgeExpired(now time.Time) {
	for i := range c.shards {
		c.purgeExpiredShard(&c.shards[i], now)
	}
}

func (c *MemoryCache) purgeExpiredShard(shard *memoryShard, now time.Time) {
	shard.mu.Lock()
	for key, entry := range shard.entries {
		if isExpired(entry.expiresAt, now) {
			delete(shard.entries, key)
			c.current.Add(-entry.cost)
		}
	}
	shard.mu.Unlock()
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
	return c.tryEvictCandidate(oldest)
}

func (c *MemoryCache) tryEvictCandidate(candidate *evictionCandidate) bool {
	candidate.shard.mu.Lock()
	current, ok := candidate.shard.entries[candidate.key]
	if ok && current == candidate.entry {
		delete(candidate.shard.entries, candidate.key)
		c.current.Add(-candidate.entry.cost)
		candidate.shard.mu.Unlock()
		return true
	}
	candidate.shard.mu.Unlock()
	return false
}

func (c *MemoryCache) deleteExpired(shard *memoryShard, key string, expected *memoryEntry, now time.Time) {
	shard.mu.Lock()
	current, ok := shard.entries[key]
	if ok && current == expected && isExpired(current.expiresAt, now) {
		delete(shard.entries, key)
		c.current.Add(-current.cost)
	}
	shard.mu.Unlock()
}

func (c *MemoryCache) shardFor(key string) *memoryShard {
	index := maphash.String(c.hashSeed, key) % uint64(len(c.shards))
	return &c.shards[index]
}

func isExpired(expiresAt, now time.Time) bool {
	return !expiresAt.IsZero() && !expiresAt.After(now)
}

func itemCost(key string, value []byte) int64 {
	return int64(len(key) + cap(value))
}
