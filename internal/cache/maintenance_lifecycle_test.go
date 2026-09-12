package cache

import "testing"

func TestNewMemoryCacheCloseStopsRunningMaintenance(t *testing.T) {
	cache, err := NewMemoryCache(MemoryConfig{})
	if err != nil {
		t.Fatalf("NewMemoryCache() error = %v", err)
	}

	if err := cache.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	select {
	case <-cache.maintenanceDone:
	default:
		t.Fatal("maintenance goroutine is still running after Close()")
	}
}
