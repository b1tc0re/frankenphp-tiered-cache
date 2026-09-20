package memory

import (
	"testing"
	"time"
)

func TestMemoryCacheStatsTrackEntriesAndBytes(t *testing.T) {
	cache := newTestMemoryCache(t, Config{MaxMemoryBytes: 1 << 20, MaxItemSizeBytes: 1 << 20})

	if _, err := cache.Set("a", []byte("one"), time.Minute); err != nil {
		t.Fatalf("Set(a): %v", err)
	}
	if _, err := cache.Forever("b", []byte("two")); err != nil {
		t.Fatalf("Forever(b): %v", err)
	}

	stats := cache.Stats()
	if stats.Entries != 2 {
		t.Fatalf("entries = %d, want 2", stats.Entries)
	}
	if stats.Bytes != itemCost("a", []byte("one"))+itemCost("b", []byte("two")) {
		t.Fatalf("bytes = %d, want accounted item costs", stats.Bytes)
	}

	if _, err := cache.Forget("a"); err != nil {
		t.Fatalf("Forget(a): %v", err)
	}
	if stats := cache.Stats(); stats.Entries != 1 {
		t.Fatalf("entries after Forget = %d, want 1", stats.Entries)
	}

	if _, err := cache.Flush(); err != nil {
		t.Fatalf("Flush(): %v", err)
	}
	if stats := cache.Stats(); stats.Entries != 0 || stats.Bytes != 0 {
		t.Fatalf("stats after Flush = %#v, want empty", stats)
	}
}
