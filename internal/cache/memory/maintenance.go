package memory

import "time"

const backgroundCleanupShardsPerTick = 16

func (c *MemoryCache) purgeExpiredBackgroundBatch(now time.Time, nextShard int) int {
	if len(c.shards) == 0 {
		return 0
	}

	shardsToScan := backgroundCleanupShardsPerTick
	if shardsToScan > len(c.shards) {
		shardsToScan = len(c.shards)
	}

	for i := 0; i < shardsToScan; i++ {
		c.purgeExpiredSample(&c.shards[nextShard], now, backgroundCleanupSample)

		nextShard++
		if nextShard == len(c.shards) {
			nextShard = 0
		}
	}

	return nextShard
}
