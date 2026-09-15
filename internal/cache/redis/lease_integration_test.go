package redis

import (
	"os"
	"strconv"
	"testing"
	"time"
)

func TestRedisCacheFenceLeaseExpires(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("set REDIS_ADDR to run the Redis integration test")
	}

	lease := 50 * time.Millisecond
	cache, err := New(Config{
		Addr:       addr,
		FenceLease: lease,
		KeyPrefix:  "frankenphp-fence-integration-" + strconv.FormatInt(time.Now().UnixNano(), 10) + ":",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		_, _ = cache.Flush()
		_ = cache.Close()
	})

	token, err := cache.ReserveFence("key")
	if err != nil {
		t.Fatalf("ReserveFence() error = %v", err)
	}
	time.Sleep(3 * lease)

	if ok, err := cache.SetWithFence("key", []byte("stale"), time.Minute, token); err != nil || ok {
		t.Fatalf("SetWithFence() after lease expiry = (%t, %v), want (false, nil)", ok, err)
	}
}
