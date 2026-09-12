package memory

// Observer receives diagnostic events from MemoryCache.
// Implementations must be safe for concurrent use and should return quickly.
type Observer interface {
	OnEviction(EvictionEvent)
}

// EvictionEvent reports that a live entry was removed because MemoryCache
// needed to free space for a write.
type EvictionEvent struct {
	Bytes int64
}
