package memory

// Observer receives diagnostic events from MemoryCache.
// Callbacks run synchronously after internal eviction locks are released.
// Implementations must be safe for concurrent use and should return quickly.
type Observer interface {
	OnEviction(EvictionSummary)
}

// EvictionSummary reports live entries removed because MemoryCache needed to
// free space for a write.
type EvictionSummary struct {
	Entries uint64
	Bytes   int64
}
