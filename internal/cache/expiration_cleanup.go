package cache

import (
	"reflect"
	"time"
)

type expirationScanner struct {
	iterator *reflect.MapIter
}

func (s *expirationScanner) reset() {
	s.iterator = nil
}

type expirationCandidate struct {
	key   string
	entry *memoryEntry
}

func (c *MemoryCache) purgeExpiredSample(shard *memoryShard, now time.Time, sampleSize int) {
	if sampleSize <= 0 {
		return
	}

	var inlineCandidates [backgroundCleanupSample]expirationCandidate
	candidates := inlineCandidates[:0]
	if sampleSize > len(inlineCandidates) {
		candidates = make([]expirationCandidate, 0, sampleSize)
	}

	shard.mu.RLock()
	if shard.cleanup.iterator == nil {
		shard.cleanup.iterator = reflect.ValueOf(shard.entries).MapRange()
	}

	sampled := 0
	for sampled < sampleSize {
		if !shard.cleanup.iterator.Next() {
			shard.cleanup.reset()
			break
		}

		key := shard.cleanup.iterator.Key().String()
		entry := shard.cleanup.iterator.Value().Interface().(*memoryEntry)
		if isExpired(entry.expiresAt, now) {
			candidates = append(candidates, expirationCandidate{
				key:   key,
				entry: entry,
			})
		}
		sampled++
	}
	shard.mu.RUnlock()

	for i := range candidates {
		c.deleteExpired(shard, candidates[i].key, candidates[i].entry, now)
	}
}
