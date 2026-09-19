package memory

import (
	"fmt"
	"sync"
	"testing"
)

var memoryCacheGetWorkerCounts = [...]int{1, 2, 4, 8, 16, 32, 64}

func BenchmarkMemoryCacheGetContention(b *testing.B) {
	for _, workers := range memoryCacheGetWorkerCounts {
		workers := workers

		b.Run(fmt.Sprintf("same-key/workers=%d", workers), func(b *testing.B) {
			benchmarkMemoryCacheGet(b, workers, false)
		})
		b.Run(fmt.Sprintf("distinct-keys/workers=%d", workers), func(b *testing.B) {
			benchmarkMemoryCacheGet(b, workers, true)
		})
	}
}

func benchmarkMemoryCacheGet(b *testing.B, workers int, distinctKeys bool) {
	b.Helper()

	cache, err := New(Config{})
	if err != nil {
		b.Fatalf("New() error = %v", err)
	}
	defer func() {
		if err := cache.Close(); err != nil {
			b.Errorf("Close() error = %v", err)
		}
	}()

	keys := make([]string, workers)
	if distinctKeys {
		for i := range keys {
			keys[i] = fmt.Sprintf("bench:get:%d", i)
			stored, err := cache.Forever(keys[i], []byte("value"))
			if err != nil || !stored {
				b.Fatalf("Forever(%q) = %v, %v; want true, nil", keys[i], stored, err)
			}
		}
	} else {
		for i := range keys {
			keys[i] = "bench:get"
		}
		stored, err := cache.Forever(keys[0], []byte("value"))
		if err != nil || !stored {
			b.Fatalf("Forever(%q) = %v, %v; want true, nil", keys[0], stored, err)
		}
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(workers)

	baseIterations := b.N / workers
	remainder := b.N % workers

	for worker := 0; worker < workers; worker++ {
		worker := worker
		iterations := baseIterations
		if worker < remainder {
			iterations++
		}

		go func() {
			defer wg.Done()
			<-start

			key := keys[worker]
			for i := 0; i < iterations; i++ {
				value, _, err := cache.Get(key)
				if err != nil || value == nil {
					b.Errorf("Get(%q) = %q, %v; want hit", key, value, err)
					return
				}
			}
		}()
	}

	b.ReportAllocs()
	b.ResetTimer()
	close(start)
	wg.Wait()
	b.StopTimer()
}
