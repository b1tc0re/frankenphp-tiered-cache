package redis

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	cachecontract "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache"
	goredis "github.com/redis/go-redis/v9"
)

const (
	maxInt64 = int64(^uint64(0) >> 1)
	minInt64 = -maxInt64 - 1
)

func TestRedisCacheSetGetAndForever(t *testing.T) {
	client := newFakeClient()
	cache := newWithClient(client, "test:")

	if ok, err := cache.Set("temporary", []byte("value"), time.Minute); err != nil || !ok {
		t.Fatalf("Set() = (%t, %v), want (true, nil)", ok, err)
	}
	if ok, err := cache.Forever("permanent", []byte("forever")); err != nil || !ok {
		t.Fatalf("Forever() = (%t, %v), want (true, nil)", ok, err)
	}

	for key, want := range map[string]string{
		"temporary": "value",
		"permanent": "forever",
	} {
		got, _, err := cache.Get(key)
		if err != nil {
			t.Fatalf("Get(%q) error = %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%q) = %q, want %q", key, got, want)
		}
	}

	if _, ttl, err := cache.Get("temporary"); err != nil || ttl != time.Minute {
		t.Fatalf("Get(temporary) TTL = %v, %v; want %v, nil", ttl, err, time.Minute)
	}
	if _, ttl, err := cache.Get("permanent"); err != nil || ttl != 0 {
		t.Fatalf("Get(permanent) TTL = %v, %v; want 0, nil", ttl, err)
	}

	got, _, err := cache.Get("missing")
	if err != nil {
		t.Fatalf("Get(missing) error = %v", err)
	}
	if got != nil {
		t.Fatalf("Get(missing) = %q, want nil", got)
	}
}

func TestRedisCacheRejectsInvalidValuesAndTTL(t *testing.T) {
	cache := newWithClient(newFakeClient(), "test:")

	tests := []struct {
		name string
		call func() (bool, error)
		want error
	}{
		{
			name: "set nil value",
			call: func() (bool, error) { return cache.Set("key", nil, time.Minute) },
			want: cachecontract.ErrNilValue,
		},
		{
			name: "set invalid ttl",
			call: func() (bool, error) { return cache.Set("key", []byte("value"), 0) },
			want: cachecontract.ErrInvalidTTL,
		},
		{
			name: "forever nil value",
			call: func() (bool, error) { return cache.Forever("key", nil) },
			want: cachecontract.ErrNilValue,
		},
		{
			name: "touch invalid ttl",
			call: func() (bool, error) { return cache.Touch("key", -time.Second) },
			want: cachecontract.ErrInvalidTTL,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ok, err := test.call()
			if ok {
				t.Error("operation returned ok=true, want false")
			}
			if !errors.Is(err, test.want) {
				t.Errorf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestRedisCacheForgetAndTouch(t *testing.T) {
	client := newFakeClient()
	cache := newWithClient(client, "test:")

	if _, err := cache.Forever("key", []byte("value")); err != nil {
		t.Fatalf("Forever() error = %v", err)
	}

	if ok, err := cache.Touch("key", time.Minute); err != nil || !ok {
		t.Fatalf("Touch(existing) = (%t, %v), want (true, nil)", ok, err)
	}
	if ok, err := cache.Touch("missing", time.Minute); err != nil || ok {
		t.Fatalf("Touch(missing) = (%t, %v), want (false, nil)", ok, err)
	}

	if ok, err := cache.Forget("key"); err != nil || !ok {
		t.Fatalf("Forget(existing) = (%t, %v), want (true, nil)", ok, err)
	}
	if ok, err := cache.Forget("key"); err != nil || ok {
		t.Fatalf("Forget(missing) = (%t, %v), want (false, nil)", ok, err)
	}
}

func TestRedisCacheIncrementAndDecrement(t *testing.T) {
	client := newFakeClient()
	cache := newWithClient(client, "test:")

	if got, err := cache.Increment("counter", 2); err != nil || got != 2 {
		t.Fatalf("Increment(missing) = (%d, %v), want (2, nil)", got, err)
	}
	if got, err := cache.Increment("counter", 3); err != nil || got != 5 {
		t.Fatalf("Increment(existing) = (%d, %v), want (5, nil)", got, err)
	}
	if got, err := cache.Decrement("counter", 4); err != nil || got != 1 {
		t.Fatalf("Decrement(existing) = (%d, %v), want (1, nil)", got, err)
	}

	value, ttl, err := cache.Get("counter")
	if err != nil || string(value) != "1" || ttl != 0 {
		t.Fatalf("Get(counter) = (%q, %v, %v), want (1, 0, nil)", value, ttl, err)
	}
}

func TestRedisCacheCounterErrorsArePropagated(t *testing.T) {
	wantErr := errors.New("counter is not an integer")
	client := newFakeClient()
	client.incrErr = wantErr
	client.decrErr = wantErr
	cache := newWithClient(client, "test:")

	if _, err := cache.Increment("counter", 1); !errors.Is(err, wantErr) {
		t.Fatalf("Increment() error = %v, want %v", err, wantErr)
	}
	if _, err := cache.Decrement("counter", 1); !errors.Is(err, wantErr) {
		t.Fatalf("Decrement() error = %v, want %v", err, wantErr)
	}
}

func TestRedisCacheWrapsKnownCounterErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "non integer", err: fakeRedisError("ERR value is not an integer or out of range")},
		{name: "overflow", err: fakeRedisError("ERR increment or decrement would overflow")},
		{name: "wrong type", err: fakeRedisError("WRONGTYPE Operation against a key holding the wrong kind of value")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			client.incrErr = test.err
			cache := newWithClient(client, "test:")

			if _, err := cache.Increment("counter", 1); !errors.Is(err, cachecontract.ErrRedisCommand) || !errors.Is(err, test.err) {
				t.Fatalf("Increment() error = %v, want ErrRedisCommand wrapping the server error", err)
			}
		})
	}
}

func TestRedisCacheLeavesInfrastructureErrorsForTiered(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "loading", err: fakeRedisError("LOADING Redis is loading the dataset")},
		{name: "readonly", err: fakeRedisError("READONLY You can't write against a read only replica")},
		{name: "clusterdown", err: fakeRedisError("CLUSTERDOWN The cluster is down")},
		{name: "masterdown", err: fakeRedisError("MASTERDOWN Link with MASTER is down")},
		{name: "tryagain", err: fakeRedisError("TRYAGAIN Temporary error")},
		{name: "maxclients", err: fakeRedisError("ERR max number of clients reached")},
		{name: "noauth", err: fakeRedisError("NOAUTH Authentication required")},
		{name: "noperm", err: fakeRedisError("NOPERM this user has no permissions")},
		{name: "execabort", err: fakeRedisError("EXECABORT Transaction discarded")},
		{name: "oom", err: fakeRedisError("OOM command not allowed when used memory > 'maxmemory'")},
		{name: "noreplicas", err: fakeRedisError("NOREPLICAS Not enough good replicas")},
		{name: "moved", err: fakeRedisError("MOVED 3999 127.0.0.1:6381")},
		{name: "ask", err: fakeRedisError("ASK 3999 127.0.0.1:6381")},
		{name: "crossslot", err: goredis.ErrCrossSlot},
		{name: "noscript", err: goredis.ErrNoScript},
		{name: "misconf", err: fakeRedisError("MISCONF Redis is configured to save RDB snapshots, but is currently not able to persist on disk")},
		{name: "unknown", err: fakeRedisError("ERR arbitrary server error")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			client.incrErr = test.err
			cache := newWithClient(client, "test:")

			if _, err := cache.Increment("counter", 1); errors.Is(err, cachecontract.ErrRedisCommand) {
				t.Fatalf("Increment() error = %v, must not be marked as a safe command error", err)
			}
		})
	}
}

func TestRedisCacheFlushOnlyDeletesItsPrefix(t *testing.T) {
	client := newFakeClient()
	cache := newWithClient(client, "test:")

	client.values["test:first"] = []byte("1")
	client.values["test:second"] = []byte("2")
	client.values["other:key"] = []byte("keep")

	ok, err := cache.Flush()
	if err != nil || !ok {
		t.Fatalf("Flush() = (%t, %v), want (true, nil)", ok, err)
	}

	if len(client.values) != 1 {
		t.Fatalf("remaining values = %d, want 1", len(client.values))
	}
	if _, ok := client.values["other:key"]; !ok {
		t.Fatal("Flush() removed a key outside the cache prefix")
	}
}

func TestRedisCacheCloseIsIdempotent(t *testing.T) {
	client := newFakeClient()
	cache := newWithClient(client, "test:")

	if err := cache.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if client.closeCalls != 1 {
		t.Fatalf("client Close() calls = %d, want 1", client.closeCalls)
	}
}

func TestRedisCachePropagatesClientErrors(t *testing.T) {
	wantErr := errors.New("redis unavailable")
	client := newFakeClient()
	client.getErr = wantErr
	cache := newWithClient(client, "test:")

	if _, _, err := cache.Get("key"); !errors.Is(err, wantErr) {
		t.Fatalf("Get() error = %v, want %v", err, wantErr)
	}
}

func TestParseGetWithTTLResult(t *testing.T) {
	tests := []struct {
		name    string
		result  interface{}
		want    []byte
		wantTTL time.Duration
		wantErr bool
	}{
		{name: "expiring value", result: []interface{}{"value", int64(1500)}, want: []byte("value"), wantTTL: 1500 * time.Millisecond},
		{name: "forever value", result: []interface{}{"value", int64(-1)}, want: []byte("value")},
		{name: "imminently expiring value", result: []interface{}{"value", int64(0)}, want: []byte("value"), wantTTL: time.Nanosecond},
		{name: "missing value", result: []interface{}{nil, int64(-2)}},
		{name: "invalid result", result: "value", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, gotTTL, err := parseGetWithTTLResult(test.result)
			if (err != nil) != test.wantErr {
				t.Fatalf("error = %v, wantErr %t", err, test.wantErr)
			}
			if string(got) != string(test.want) {
				t.Errorf("value = %q, want %q", got, test.want)
			}
			if gotTTL != test.wantTTL {
				t.Errorf("TTL = %v, want %v", gotTTL, test.wantTTL)
			}
		})
	}
}

func TestConfigDefaultsAndValidation(t *testing.T) {
	config, err := (Config{}).normalized()
	if err != nil {
		t.Fatalf("normalized() error = %v", err)
	}
	if config.Addr != DefaultAddr {
		t.Errorf("Addr = %q, want %q", config.Addr, DefaultAddr)
	}
	if config.KeyPrefix != DefaultKeyPrefix {
		t.Errorf("KeyPrefix = %q, want %q", config.KeyPrefix, DefaultKeyPrefix)
	}

	for name, invalid := range map[string]Config{
		"negative db":            {DB: -1},
		"negative dial timeout":  {DialTimeout: -time.Second},
		"negative read timeout":  {ReadTimeout: -time.Second},
		"negative write timeout": {WriteTimeout: -time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := invalid.normalized(); err == nil {
				t.Error("normalized() error = nil, want validation error")
			}
		})
	}
}

func TestInvalidationChannelUsesRedisKeyPrefix(t *testing.T) {
	if got, want := invalidationChannel("test:"), "test:__invalidation"; got != want {
		t.Fatalf("invalidationChannel() = %q, want %q", got, want)
	}
}

func TestRedisScanPatternEscapesGlobCharacters(t *testing.T) {
	if got, want := redisScanPattern("test\\*[?"), "test\\\\\\*\\[\\?*"; got != want {
		t.Fatalf("redisScanPattern() = %q, want %q", got, want)
	}
}

type fakeClient struct {
	mu sync.Mutex

	values map[string][]byte
	ttls   map[string]time.Duration

	getErr    error
	setErr    error
	incrErr   error
	decrErr   error
	delErr    error
	expireErr error
	scanErr   error
	closeErr  error

	closeCalls int
}

type fakeRedisError string

func (e fakeRedisError) Error() string { return string(e) }

func (fakeRedisError) RedisError() {}

func newFakeClient() *fakeClient {
	return &fakeClient{
		values: make(map[string][]byte),
		ttls:   make(map[string]time.Duration),
	}
}

func (f *fakeClient) Get(_ context.Context, key string) ([]byte, time.Duration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.getErr != nil {
		return nil, 0, f.getErr
	}
	value, ok := f.values[key]
	if !ok {
		return nil, 0, goredis.Nil
	}

	return append([]byte(nil), value...), f.ttls[key], nil
}

func (f *fakeClient) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.setErr != nil {
		return f.setErr
	}
	f.values[key] = append([]byte(nil), value...)
	f.ttls[key] = ttl
	return nil
}

func (f *fakeClient) IncrBy(_ context.Context, key string, value int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.incrErr != nil {
		return 0, f.incrErr
	}
	return f.changeCounter(key, value, false)
}

func (f *fakeClient) DecrBy(_ context.Context, key string, value int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.decrErr != nil {
		return 0, f.decrErr
	}
	return f.changeCounter(key, value, true)
}

func (f *fakeClient) changeCounter(key string, value int64, decrement bool) (int64, error) {
	current := int64(0)
	if raw, ok := f.values[key]; ok {
		parsed, err := strconv.ParseInt(string(raw), 10, 64)
		if err != nil {
			return 0, err
		}
		current = parsed
	}
	if decrement {
		value = -value
	}
	if (value > 0 && current > maxInt64-value) || (value < 0 && current < minInt64-value) {
		return 0, errors.New("counter overflow")
	}
	current += value
	f.values[key] = []byte(strconv.FormatInt(current, 10))
	return current, nil
}

func (f *fakeClient) Del(_ context.Context, keys ...string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.delErr != nil {
		return 0, f.delErr
	}

	var deleted int64
	for _, key := range keys {
		if _, ok := f.values[key]; ok {
			delete(f.values, key)
			delete(f.ttls, key)
			deleted++
		}
	}
	return deleted, nil
}

func (f *fakeClient) Expire(_ context.Context, key string, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.expireErr != nil {
		return false, f.expireErr
	}
	_, ok := f.values[key]
	if ok {
		f.ttls[key] = ttl
	}
	return ok, nil
}

func (f *fakeClient) Scan(_ context.Context, _ uint64, match string, _ int64) ([]string, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.scanErr != nil {
		return nil, 0, f.scanErr
	}

	prefix := strings.TrimSuffix(match, "*")
	keys := make([]string, 0, len(f.values))
	for key := range f.values {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, 0, nil
}

func (f *fakeClient) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.closeCalls++
	return f.closeErr
}
