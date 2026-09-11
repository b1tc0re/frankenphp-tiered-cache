package cache

import (
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
)

const (
	memoryBenchTargetBytes int64 = 64 << 20
	memoryBenchLimitBytes  int64 = 128 << 20
	memoryBenchCycles            = 5
)

type memoryBenchSnapshot struct {
	heapAlloc uint64
	heapInuse uint64
	heapSys   uint64
	rss       uint64
	rssOK     bool
}

func TestMemoryCacheMemoryFootprint(t *testing.T) {
	requireMemoryBench(t)

	cases := []struct {
		name        string
		payloadSize int
	}{
		{name: "4MiB", payloadSize: 4 << 20},
		{name: "256KiB", payloadSize: 256 << 10},
		{name: "64KiB", payloadSize: 64 << 10},
		{name: "4KiB", payloadSize: 4 << 10},
		{name: "1KiB", payloadSize: 1 << 10},
		{name: "128B", payloadSize: 128},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseline := collectMemoryBenchSnapshot()
			cache := newMemoryBenchCache(t)
			entryCount := int(memoryBenchTargetBytes / int64(tc.payloadSize))
			payloadBytes := int64(entryCount * tc.payloadSize)
			var keyBytes int64

			for i := 0; i < entryCount; i++ {
				key := "memory-bench:" + strconv.Itoa(i)
				value := makeMemoryBenchPayload(tc.payloadSize, i)
				keyBytes += int64(len(key))

				stored, err := cache.Forever(key, value)
				if err != nil {
					t.Fatalf("Forever(%q) error = %v", key, err)
				}
				if !stored {
					t.Fatalf("Forever(%q) refused value; accounted=%d max=%d", key, cache.current.Load(), cache.maxMemory)
				}
			}

			filled := collectMemoryBenchSnapshot()
			accounted := cache.current.Load()
			actualEntries := memoryBenchEntryCount(cache)

			t.Logf(
				"filled payload=%s entries=%d payload=%.2fMiB keys=%.2fMiB accounted=%.2fMiB accounted/payload=%.3fx heap_alloc_delta=%.2fMiB heap_inuse_delta=%.2fMiB heap_sys_delta=%.2fMiB rss_delta=%s",
				tc.name,
				actualEntries,
				bytesToMiB(payloadBytes),
				bytesToMiB(keyBytes),
				bytesToMiB(accounted),
				float64(accounted)/float64(payloadBytes),
				bytesDeltaToMiB(filled.heapAlloc, baseline.heapAlloc),
				bytesDeltaToMiB(filled.heapInuse, baseline.heapInuse),
				bytesDeltaToMiB(filled.heapSys, baseline.heapSys),
				formatRSSDelta(filled, baseline),
			)

			flushed, err := cache.Flush()
			if err != nil {
				t.Fatalf("Flush() error = %v", err)
			}
			if !flushed {
				t.Fatal("Flush() = false, want true")
			}

			afterFlush := collectMemoryBenchSnapshot()
			t.Logf(
				"after_flush payload=%s entries=%d accounted=%.2fMiB heap_alloc_delta=%.2fMiB heap_inuse_delta=%.2fMiB heap_sys_delta=%.2fMiB rss_delta=%s",
				tc.name,
				memoryBenchEntryCount(cache),
				bytesToMiB(cache.current.Load()),
				bytesDeltaToMiB(afterFlush.heapAlloc, baseline.heapAlloc),
				bytesDeltaToMiB(afterFlush.heapInuse, baseline.heapInuse),
				bytesDeltaToMiB(afterFlush.heapSys, baseline.heapSys),
				formatRSSDelta(afterFlush, baseline),
			)

			runtime.KeepAlive(cache)
		})
	}
}

func TestMemoryCacheMemoryRetentionCycles(t *testing.T) {
	requireMemoryBench(t)

	const payloadSize = 1 << 10
	entryCount := int(memoryBenchTargetBytes / payloadSize)
	cache := newMemoryBenchCache(t)
	baseline := collectMemoryBenchSnapshot()

	for cycle := 1; cycle <= memoryBenchCycles; cycle++ {
		for i := 0; i < entryCount; i++ {
			key := "memory-cycle:" + strconv.Itoa(cycle) + ":" + strconv.Itoa(i)
			value := makeMemoryBenchPayload(payloadSize, i+cycle)
			stored, err := cache.Forever(key, value)
			if err != nil {
				t.Fatalf("cycle %d Forever(%q) error = %v", cycle, key, err)
			}
			if !stored {
				t.Fatalf("cycle %d Forever(%q) refused value", cycle, key)
			}
		}

		filled := collectMemoryBenchSnapshot()
		t.Logf(
			"cycle=%d state=filled entries=%d accounted=%.2fMiB heap_alloc_delta=%.2fMiB heap_inuse_delta=%.2fMiB heap_sys_delta=%.2fMiB rss_delta=%s",
			cycle,
			memoryBenchEntryCount(cache),
			bytesToMiB(cache.current.Load()),
			bytesDeltaToMiB(filled.heapAlloc, baseline.heapAlloc),
			bytesDeltaToMiB(filled.heapInuse, baseline.heapInuse),
			bytesDeltaToMiB(filled.heapSys, baseline.heapSys),
			formatRSSDelta(filled, baseline),
		)

		flushed, err := cache.Flush()
		if err != nil {
			t.Fatalf("cycle %d Flush() error = %v", cycle, err)
		}
		if !flushed {
			t.Fatalf("cycle %d Flush() = false, want true", cycle)
		}

		afterFlush := collectMemoryBenchSnapshot()
		t.Logf(
			"cycle=%d state=flushed entries=%d accounted=%.2fMiB heap_alloc_delta=%.2fMiB heap_inuse_delta=%.2fMiB heap_sys_delta=%.2fMiB rss_delta=%s",
			cycle,
			memoryBenchEntryCount(cache),
			bytesToMiB(cache.current.Load()),
			bytesDeltaToMiB(afterFlush.heapAlloc, baseline.heapAlloc),
			bytesDeltaToMiB(afterFlush.heapInuse, baseline.heapInuse),
			bytesDeltaToMiB(afterFlush.heapSys, baseline.heapSys),
			formatRSSDelta(afterFlush, baseline),
		)
	}

	runtime.KeepAlive(cache)
}

func requireMemoryBench(t *testing.T) {
	t.Helper()
	if os.Getenv("MEMORY_BENCH") != "1" {
		t.Skip("set MEMORY_BENCH=1 to run memory footprint tests")
	}
}

func newMemoryBenchCache(t *testing.T) *MemoryCache {
	t.Helper()

	cache, err := NewMemoryCache(MemoryConfig{
		MaxMemoryBytes:   memoryBenchLimitBytes,
		MaxItemSizeBytes: DefaultMaxItemSizeBytes,
	})
	if err != nil {
		t.Fatalf("NewMemoryCache() error = %v", err)
	}

	return cache
}

func makeMemoryBenchPayload(size, seed int) []byte {
	value := make([]byte, size)
	if size == 0 {
		return value
	}

	pageSize := os.Getpagesize()
	marker := byte(seed)
	for offset := 0; offset < size; offset += pageSize {
		value[offset] = marker
	}
	value[size-1] = marker

	return value
}

func memoryBenchEntryCount(cache *MemoryCache) int {
	total := 0
	for i := range cache.shards {
		shard := &cache.shards[i]
		shard.mu.RLock()
		total += len(shard.entries)
		shard.mu.RUnlock()
	}
	return total
}

func collectMemoryBenchSnapshot() memoryBenchSnapshot {
	runtime.GC()
	debug.FreeOSMemory()

	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	rss, rssOK := readProcessRSSBytes()

	return memoryBenchSnapshot{
		heapAlloc: stats.HeapAlloc,
		heapInuse: stats.HeapInuse,
		heapSys:   stats.HeapSys,
		rss:       rss,
		rssOK:     rssOK,
	}
}

func readProcessRSSBytes() (uint64, bool) {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}

	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0, false
	}

	residentPages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, false
	}

	return residentPages * uint64(os.Getpagesize()), true
}

func bytesToMiB(bytes int64) float64 {
	return float64(bytes) / float64(1<<20)
}

func bytesDeltaToMiB(after, before uint64) float64 {
	return float64(int64(after)-int64(before)) / float64(1<<20)
}

func formatRSSDelta(after, before memoryBenchSnapshot) string {
	if !after.rssOK || !before.rssOK {
		return "n/a"
	}

	return strconv.FormatFloat(bytesDeltaToMiB(after.rss, before.rss), 'f', 2, 64) + "MiB"
}
