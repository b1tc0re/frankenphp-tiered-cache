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

func TestRedisCacheFencingRejectsWriteAfterForget(t *testing.T) {
	client := newFakeClient()
	cache := newWithClient(client, "test:")

	token, err := cache.ReserveFence("key")
	if err != nil {
		t.Fatalf("ReserveFence() error = %v", err)
	}
	if ok, err := cache.SetWithFence("key", []byte("v1"), time.Minute, token); err != nil || !ok {
		t.Fatalf("SetWithFence() = (%t, %v), want (true, nil)", ok, err)
	}

	if _, err := cache.ForgetWithFence("key"); err != nil {
		t.Fatalf("ForgetWithFence() error = %v", err)
	}
	if ok, err := cache.SetWithFence("key", []byte("stale"), time.Minute, token); err != nil || ok {
		t.Fatalf("stale SetWithFence() = (%t, %v), want (false, nil)", ok, err)
	}
	if value, _, err := cache.Get("key"); err != nil || value != nil {
		t.Fatalf("Get() after fenced forget = (%q, %v), want (nil, nil)", value, err)
	}
}

func TestRedisCacheFencingAllowsLatestWrite(t *testing.T) {
	client := newFakeClient()
	cache := newWithClient(client, "test:")

	first, err := cache.ReserveFence("key")
	if err != nil {
		t.Fatalf("first ReserveFence() error = %v", err)
	}
	second, err := cache.ReserveFence("key")
	if err != nil {
		t.Fatalf("second ReserveFence() error = %v", err)
	}
	if first == second {
		t.Fatal("ReserveFence() returned duplicate tokens")
	}
	if ok, err := cache.SetWithFence("key", []byte("stale"), time.Minute, first); err != nil || ok {
		t.Fatalf("stale SetWithFence() = (%t, %v), want (false, nil)", ok, err)
	}
	if ok, err := cache.SetWithFence("key", []byte("latest"), time.Minute, second); err != nil || !ok {
		t.Fatalf("latest SetWithFence() = (%t, %v), want (true, nil)", ok, err)
	}
	if value, _, err := cache.Get("key"); err != nil || string(value) != "latest" {
		t.Fatalf("Get() = (%q, %v), want (latest, nil)", value, err)
	}
}

func TestRedisCacheFencingCleansSuccessfulMutationTokens(t *testing.T) {
	client := newFakeClient()
	cache := newWithClient(client, "test:")

	token, err := cache.ReserveFence("key")
	if err != nil {
		t.Fatalf("ReserveFence() error = %v", err)
	}
	if ok, err := cache.SetWithFence("key", []byte("value"), time.Minute, token); err != nil || !ok {
		t.Fatalf("SetWithFence() = (%t, %v), want (true, nil)", ok, err)
	}
	assertFenceFieldAbsent(t, client, cache, "key")

	token, err = cache.ReserveFence("key")
	if err != nil {
		t.Fatalf("ReserveFence() before TouchWithFence() error = %v", err)
	}
	if ok, err := cache.TouchWithFence("key", time.Minute, token); err != nil || !ok {
		t.Fatalf("TouchWithFence(existing) = (%t, %v), want (true, nil)", ok, err)
	}
	assertFenceFieldAbsent(t, client, cache, "key")

	token, err = cache.ReserveFence("missing")
	if err != nil {
		t.Fatalf("ReserveFence(missing) error = %v", err)
	}
	if ok, err := cache.TouchWithFence("missing", time.Minute, token); err != nil || ok {
		t.Fatalf("TouchWithFence(missing) = (%t, %v), want (false, nil)", ok, err)
	}
	assertFenceFieldAbsent(t, client, cache, "missing")
}

func TestRedisCacheFlushInvalidatesReservedFences(t *testing.T) {
	client := newFakeClient()
	cache := newWithClient(client, "test:")

	token, err := cache.ReserveFence("key")
	if err != nil {
		t.Fatalf("ReserveFence() error = %v", err)
	}
	if ok, err := cache.Flush(); err != nil || !ok {
		t.Fatalf("Flush() = (%t, %v), want (true, nil)", ok, err)
	}
	if ok, err := cache.SetWithFence("key", []byte("stale"), time.Minute, token); err != nil || ok {
		t.Fatalf("stale SetWithFence() after Flush = (%t, %v), want (false, nil)", ok, err)
	}
}

func assertFenceFieldAbsent(t *testing.T, client *fakeClient, cache *RedisCache, key string) {
	t.Helper()

	client.mu.Lock()
	defer client.mu.Unlock()

	if _, exists := client.hashes[cache.fenceHashKey()][cache.fenceField(key)]; exists {
		t.Fatalf("fence field for %q remains after mutation", key)
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
		{
			name:    "expiring value",
			result:  []interface{}{"value", int64(1500)},
			want:    []byte("value"),
			wantTTL: 1500 * time.Millisecond,
		},
		{
			name:   "forever value",
			result: []interface{}{"value", int64(-1)},
			want:   []byte("value"),
		},
		{
			name:    "imminently expiring value",
			result:  []interface{}{"value", int64(0)},
			want:    []byte("value"),
			wantTTL: time.Nanosecond,
		},
		{
			name:   "missing value",
			result: []interface{}{nil, int64(-2)},
		},
		{
			name:    "invalid result",
			result:  "value",
			wantErr: true,
		},
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
	hashes map[string]map[string][]byte

	getErr    error
	setErr    error
	delErr    error
	expireErr error
	scanErr   error
	closeErr  error

	closeCalls int
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		values: make(map[string][]byte),
		ttls:   make(map[string]time.Duration),
		hashes: make(map[string]map[string][]byte),
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

func (f *fakeClient) Eval(_ context.Context, script string, keys []string, args ...interface{}) (interface{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(args) == 0 {
		return nil, errors.New("invalid fake EVAL arguments")
	}
	valueKey := ""
	if len(keys) > 0 {
		valueKey = keys[0]
	}

	switch script {
	case reserveFenceScript:
		if len(keys) != 1 || len(args) != 2 {
			return nil, errors.New("invalid fake reserve fence arguments")
		}
		field, fieldOK := args[0].(string)
		token, tokenOK := args[1].(string)
		if !fieldOK || !tokenOK {
			return nil, errors.New("invalid fake reserve fence values")
		}
		if f.hashes[keys[0]] == nil {
			f.hashes[keys[0]] = make(map[string][]byte)
		}
		f.hashes[keys[0]][field] = []byte(token)
		return int64(1), nil

	case setWithFenceScript:
		if len(keys) != 2 || len(args) != 4 {
			return nil, errors.New("invalid fake fenced set arguments")
		}
		if f.setErr != nil {
			return nil, f.setErr
		}
		field, fieldOK := args[0].(string)
		token, tokenOK := args[1].(string)
		if !fieldOK || !tokenOK || string(f.hashes[keys[1]][field]) != token {
			return int64(0), nil
		}
		value, ok := args[2].([]byte)
		if !ok {
			return nil, errors.New("invalid fake value")
		}
		ttlText, ok := args[3].(string)
		if !ok {
			return nil, errors.New("invalid fake ttl")
		}
		ttlMillis, err := strconv.ParseInt(ttlText, 10, 64)
		if err != nil {
			return nil, err
		}
		f.values[valueKey] = append([]byte(nil), value...)
		f.ttls[valueKey] = time.Duration(ttlMillis) * time.Millisecond
		if ttlMillis == 0 {
			f.ttls[valueKey] = 0
		}
		delete(f.hashes[keys[1]], field)
		return int64(1), nil

	case forgetWithFenceScript:
		if len(keys) != 2 || len(args) != 1 {
			return nil, errors.New("invalid fake fenced forget arguments")
		}
		if f.delErr != nil {
			return nil, f.delErr
		}
		field, ok := args[0].(string)
		if !ok {
			return nil, errors.New("invalid fake fence field")
		}
		if f.hashes[keys[1]] != nil {
			delete(f.hashes[keys[1]], field)
		}
		if _, exists := f.values[valueKey]; !exists {
			return int64(0), nil
		}
		delete(f.values, valueKey)
		delete(f.ttls, valueKey)
		return int64(1), nil

	case forgetIfFenceScript:
		if len(keys) != 2 || len(args) != 2 {
			return nil, errors.New("invalid fake conditional forget arguments")
		}
		if f.delErr != nil {
			return nil, f.delErr
		}
		field, fieldOK := args[0].(string)
		token, tokenOK := args[1].(string)
		if !fieldOK || !tokenOK || string(f.hashes[keys[1]][field]) != token {
			return int64(0), nil
		}
		delete(f.hashes[keys[1]], field)
		if _, exists := f.values[valueKey]; !exists {
			return int64(0), nil
		}
		delete(f.values, valueKey)
		delete(f.ttls, valueKey)
		return int64(1), nil

	case touchWithFenceScript:
		if len(keys) != 2 || len(args) != 3 {
			return nil, errors.New("invalid fake fenced touch arguments")
		}
		if f.expireErr != nil {
			return nil, f.expireErr
		}
		field, fieldOK := args[0].(string)
		token, tokenOK := args[1].(string)
		if !fieldOK || !tokenOK {
			return nil, errors.New("invalid fake fenced touch values")
		}
		if string(f.hashes[keys[1]][field]) != token {
			return int64(0), nil
		}
		if _, exists := f.values[valueKey]; !exists {
			delete(f.hashes[keys[1]], field)
			return int64(0), nil
		}
		seconds, ok := args[2].(int64)
		if !ok {
			return nil, errors.New("invalid fake touch ttl")
		}
		f.ttls[valueKey] = time.Duration(seconds) * time.Second
		delete(f.hashes[keys[1]], field)
		return int64(1), nil
	default:
		return nil, errors.New("unknown fake EVAL script")
	}
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
		if _, ok := f.hashes[key]; ok {
			delete(f.hashes, key)
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
