package cache

import "time"

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
	sampled := 0
	for key, entry := range shard.entries {
		if isExpired(entry.expiresAt, now) {
			candidates = append(candidates, expirationCandidate{
				key:   key,
				entry: entry,
			})
		}

		sampled++
		if sampled >= sampleSize {
			break
		}
	}
	shard.mu.RUnlock()

	for i := range candidates {
		c.deleteExpired(shard, candidates[i].key, candidates[i].entry, now)
	}
}
