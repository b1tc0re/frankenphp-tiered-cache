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

func TestRedisCacheAddUsesAtomicSetNX(t *testing.T) {
	client := newFakeClient()
	cache := newWithClient(client, "test:")

	if ok, err := cache.Add("key", []byte("value"), time.Minute); err != nil || !ok {
		t.Fatalf("Add(missing) = (%t, %v), want (true, nil)", ok, err)
	}
	if value, ttl, err := cache.Get("key"); err != nil || string(value) != "value" || ttl != time.Minute {
		t.Fatalf("Get() after Add() = (%q, %v, %v), want (value, %v, nil)", value, ttl, err, time.Minute)
	}

	if ok, err := cache.Add("key", []byte("new"), 2*time.Minute); err != nil || ok {
		t.Fatalf("Add(existing) = (%t, %v), want (false, nil)", ok, err)
	}
	if value, ttl, err := cache.Get("key"); err != nil || string(value) != "value" || ttl != time.Minute {
		t.Fatalf("Get() after rejected Add() = (%q, %v, %v), want (value, %v, nil)", value, ttl, err, time.Minute)
	}
	if client.setNXCalls != 2 || client.setCalls != 0 {
		t.Fatalf("Redis calls = (SetNX=%d, Set=%d), want (2, 0)", client.setNXCalls, client.setCalls)
	}
}

func TestRedisCacheSetManyUsesOneAtomicBatch(t *testing.T) {
	client := newFakeClient()
	cache := newWithClient(client, "test:")
	values := map[string][]byte{
		"first":  []byte("one"),
		"second": []byte("two"),
		"third":  []byte("three"),
	}

	stored, err := cache.SetMany(values, time.Minute)
	if !stored || err != nil {
		t.Fatalf("SetMany() = (%t, %v), want (true, nil)", stored, err)
	}
	if client.setManyCalls != 1 || client.setCalls != 0 {
		t.Fatalf("Redis calls = (SetMany=%d, Set=%d), want (1, 0)", client.setManyCalls, client.setCalls)
	}
	for key, want := range values {
		value, ttl, getErr := cache.Get(key)
		if getErr != nil || string(value) != string(want) || ttl != time.Minute {
			t.Errorf("Get(%q) = (%q, %v, %v), want prefixed value and one-minute TTL", key, value, ttl, getErr)
		}
		if _, ok := client.values["test:"+key]; !ok {
			t.Errorf("Redis key %q was not prefixed", key)
		}
	}
}

func TestRedisCacheSetManyValidationAndEmptyBatch(t *testing.T) {
	client := newFakeClient()
	cache := newWithClient(client, "test:")

	if stored, err := cache.SetMany(nil, time.Minute); stored || err != nil {
		t.Fatalf("SetMany(empty) = (%t, %v), want (false, nil)", stored, err)
	}
	if client.setManyCalls != 0 {
		t.Fatalf("SetMany(empty) calls = %d, want 0", client.setManyCalls)
	}

	if stored, err := cache.SetMany(map[string][]byte{"key": nil}, time.Minute); stored || !errors.Is(err, cachecontract.ErrNilValue) {
		t.Fatalf("SetMany(nil value) = (%t, %v), want false and ErrNilValue", stored, err)
	}
	if client.setManyCalls != 0 {
		t.Fatalf("SetMany(invalid) calls = %d, want 0", client.setManyCalls)
	}

	if stored, err := cache.SetMany(map[string][]byte{"key": []byte("value")}, 0); stored || !errors.Is(err, cachecontract.ErrInvalidTTL) {
		t.Fatalf("SetMany(invalid TTL) = (%t, %v), want false and ErrInvalidTTL", stored, err)
	}
}

func TestRedisCacheGetManyUsesOnePrefixedBatch(t *testing.T) {
	client := newFakeClient()
	client.values["test:first"] = []byte("one")
	client.ttls["test:first"] = time.Minute
	client.values["test:empty"] = []byte{}
	client.ttls["test:empty"] = 0
	cache := newWithClient(client, "test:")

	result, err := cache.GetMany([]string{"first", "missing", "first", "empty"})
	if err != nil {
		t.Fatalf("GetMany() error = %v", err)
	}
	if client.getManyCalls != 1 {
		t.Fatalf("Redis GetMany() calls = %d, want 1", client.getManyCalls)
	}
	if len(client.getManyKeys) != 3 || client.getManyKeys[0] != "test:first" || client.getManyKeys[1] != "test:missing" || client.getManyKeys[2] != "test:empty" {
		t.Fatalf("Redis GetMany() keys = %v, want one prefixed deduplicated batch", client.getManyKeys)
	}
	if item, ok := result["first"]; !ok || string(item.Value) != "one" || item.TTL != time.Minute {
		t.Fatalf("GetMany(first) = (%q, %v, %t), want value and TTL", item.Value, item.TTL, ok)
	}
	if _, ok := result["missing"]; ok {
		t.Fatal("GetMany(missing) returned an item")
	}
	if item, ok := result["empty"]; !ok || item.Value == nil || len(item.Value) != 0 || item.TTL != 0 {
		t.Fatalf("GetMany(empty) = (%q, %v, %t), want found empty value with TTL 0", item.Value, item.TTL, ok)
	}
}

func TestRedisCacheGetManyEmptyBatchDoesNotCallRedis(t *testing.T) {
	client := newFakeClient()
	cache := newWithClient(client, "test:")

	result, err := cache.GetMany(nil)
	if err != nil || result == nil || len(result) != 0 {
		t.Fatalf("GetMany(empty) = (%v, %v), want non-nil empty result", result, err)
	}
	if client.getManyCalls != 0 {
		t.Fatalf("Redis GetMany() calls for empty batch = %d, want 0", client.getManyCalls)
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
			name: "add nil value",
			call: func() (bool, error) { return cache.Add("key", nil, time.Minute) },
			want: cachecontract.ErrNilValue,
		},
		{
			name: "add invalid ttl",
			call: func() (bool, error) { return cache.Add("key", []byte("value"), 0) },
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
		name      string
		err       error
		decrement bool
	}{
		{name: "non integer", err: fakeRedisError("ERR value is not an integer or out of range")},
		{name: "overflow", err: fakeRedisError("ERR increment or decrement would overflow")},
		{name: "decrement min int64", err: fakeRedisError("ERR decrement would overflow"), decrement: true},
		{name: "wrong type", err: fakeRedisError("WRONGTYPE Operation against a key holding the wrong kind of value")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			if test.decrement {
				client.decrErr = test.err
			} else {
				client.incrErr = test.err
			}
			cache := newWithClient(client, "test:")

			var err error
			if test.decrement {
				_, err = cache.Decrement("counter", 1)
			} else {
				_, err = cache.Increment("counter", 1)
			}
			if !errors.Is(err, cachecontract.ErrRedisCommand) || !errors.Is(err, test.err) {
				t.Fatalf("counter operation error = %v, want ErrRedisCommand wrapping the server error", err)
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

func TestParseGetManyWithTTLResult(t *testing.T) {
	result, err := parseGetManyWithTTLResult([]interface{}{
		[]byte("value"), int64(-1),
		false, int64(-2),
		[]byte{}, int64(0),
	}, []string{"forever", "missing", "empty"})
	if err != nil {
		t.Fatalf("parseGetManyWithTTLResult() error = %v", err)
	}
	if item, ok := result["forever"]; !ok || string(item.Value) != "value" || item.TTL != 0 {
		t.Fatalf("forever item = (%q, %v, %t), want value with TTL 0", item.Value, item.TTL, ok)
	}
	if _, ok := result["missing"]; ok {
		t.Fatal("missing key was included in parsed result")
	}
	if item, ok := result["empty"]; !ok || item.Value == nil || len(item.Value) != 0 || item.TTL != time.Nanosecond {
		t.Fatalf("empty item = (%q, %v, %t), want empty value with minimal TTL", item.Value, item.TTL, ok)
	}
}

func TestParseGetManyWithTTLResultRejectsUnknownNegativeTTL(t *testing.T) {
	if _, err := parseGetManyWithTTLResult([]interface{}{false, int64(-5)}, []string{"key"}); err == nil {
		t.Fatal("parseGetManyWithTTLResult() error = nil, want error")
	}
}

type fakeClient struct {
	mu sync.Mutex

	values map[string][]byte
	ttls   map[string]time.Duration

	getErr     error
	getManyErr error
	setErr     error
	setNXErr   error
	setManyErr error
	incrErr    error
	decrErr    error
	delErr     error
	expireErr  error
	scanErr    error
	closeErr   error

	setCalls     int
	setNXCalls   int
	setManyCalls int
	getManyCalls int
	getManyKeys  []string
	closeCalls   int
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

func (f *fakeClient) GetMany(_ context.Context, keys []string) (map[string]cachecontract.Item, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.getManyCalls++
	f.getManyKeys = append([]string(nil), keys...)
	if f.getManyErr != nil {
		return nil, f.getManyErr
	}

	result := make(map[string]cachecontract.Item, len(keys))
	for _, key := range keys {
		value, ok := f.values[key]
		if !ok {
			continue
		}
		copyValue := make([]byte, len(value))
		copy(copyValue, value)
		result[key] = cachecontract.Item{Value: copyValue, TTL: f.ttls[key]}
	}
	return result, nil
}

func (f *fakeClient) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setCalls++

	if f.setErr != nil {
		return f.setErr
	}
	f.values[key] = append([]byte(nil), value...)
	f.ttls[key] = ttl
	return nil
}

func (f *fakeClient) SetNX(_ context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setNXCalls++

	if f.setNXErr != nil {
		return false, f.setNXErr
	}
	if _, exists := f.values[key]; exists {
		return false, nil
	}
	f.values[key] = append([]byte(nil), value...)
	f.ttls[key] = ttl
	return true, nil
}

func (f *fakeClient) SetMany(_ context.Context, values map[string][]byte, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setManyCalls++

	if f.setManyErr != nil {
		return f.setManyErr
	}
	for key, value := range values {
		f.values[key] = append([]byte(nil), value...)
		f.ttls[key] = ttl
	}
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
