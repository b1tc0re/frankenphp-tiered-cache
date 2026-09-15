package redis

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	cachecontract "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache"
	goredis "github.com/redis/go-redis/v9"
)

type client interface {
	Get(ctx context.Context, key string) ([]byte, time.Duration, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error)
	Del(ctx context.Context, keys ...string) (int64, error)
	Expire(ctx context.Context, key string, ttl time.Duration) (bool, error)
	Scan(ctx context.Context, cursor uint64, match string, count int64) ([]string, uint64, error)
	Close() error
}

type redisClient struct {
	client *goredis.Client
}

var getWithTTLScript = goredis.NewScript(
	"local value = redis.call(\"GET\", KEYS[1])\n" +
		"if not value then\n" +
		"\treturn {false, -2}\n" +
		"end\n" +
		"return {value, redis.call(\"PTTL\", KEYS[1])}",
)

func (c redisClient) Get(ctx context.Context, key string) ([]byte, time.Duration, error) {
	result, err := getWithTTLScript.Run(ctx, c.client, []string{key}).Result()
	if err != nil {
		return nil, 0, err
	}

	return parseGetWithTTLResult(result)
}

func (c redisClient) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.client.Set(ctx, key, value, ttl).Err()
}

func (c redisClient) Eval(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error) {
	return c.client.Eval(ctx, script, keys, args...).Result()
}

func (c redisClient) Del(ctx context.Context, keys ...string) (int64, error) {
	return c.client.Del(ctx, keys...).Result()
}

func (c redisClient) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return c.client.Expire(ctx, key, ttl).Result()
}

func (c redisClient) Scan(ctx context.Context, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return c.client.Scan(ctx, cursor, match, count).Result()
}

func (c redisClient) Close() error {
	return c.client.Close()
}

// RedisCache is a synchronous Redis-backed implementation of cache.Cache.
//
// It does not start background workers. Asynchronous write-behind belongs to
// the composition layer, such as TieredCache, so this backend remains usable
// on its own and has straightforward operation and shutdown semantics.
type RedisCache struct {
	client     client
	prefix     string
	fenceLease time.Duration

	closeOnce sync.Once
	closeErr  error
}

var _ cachecontract.Cache = (*RedisCache)(nil)
var _ cachecontract.FencedCache = (*RedisCache)(nil)

const (
	fenceMetadataPrefix = "\x00frankenphp-tiered-cache/fence:"

	reserveFenceScript = `
redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
return 1
`

	releaseFenceScript = `
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
    return 0
end
return redis.call("DEL", KEYS[1])
`

	setWithFenceScript = `
local current = redis.call("GET", KEYS[2])
if current ~= ARGV[1] then
    return 0
end
if ARGV[3] == "0" then
	redis.call("SET", KEYS[1], ARGV[2])
else
	redis.call("SET", KEYS[1], ARGV[2], "PX", ARGV[3])
end
redis.call("DEL", KEYS[2])
return 1
`

	forgetWithFenceScript = `
redis.call("DEL", KEYS[2])
return redis.call("DEL", KEYS[1])
`

	forgetIfFenceScript = `
if redis.call("GET", KEYS[2]) ~= ARGV[1] then
    return 0
end
redis.call("DEL", KEYS[2])
return redis.call("DEL", KEYS[1])
`

	touchWithFenceScript = `
local current = redis.call("GET", KEYS[2])
if current ~= ARGV[1] then
    return 0
end
local touched = redis.call("EXPIRE", KEYS[1], ARGV[2])
redis.call("DEL", KEYS[2])
return touched
`
)

func New(config Config) (*RedisCache, error) {
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}

	return &RedisCache{
		client: redisClient{client: goredis.NewClient(&goredis.Options{
			Addr:         config.Addr,
			Username:     config.Username,
			Password:     config.Password,
			DB:           config.DB,
			DialTimeout:  config.DialTimeout,
			ReadTimeout:  config.ReadTimeout,
			WriteTimeout: config.WriteTimeout,
		})},
		prefix:     config.KeyPrefix,
		fenceLease: config.FenceLease,
	}, nil
}

func newWithClient(c client, prefix string) *RedisCache {
	return &RedisCache{client: c, prefix: prefix, fenceLease: DefaultFenceLease}
}

func (c *RedisCache) Get(key string) ([]byte, time.Duration, error) {
	value, ttl, err := c.client.Get(context.Background(), c.prefixedKey(key))
	if errors.Is(err, goredis.Nil) {
		return nil, 0, nil
	}

	return value, ttl, err
}

func (c *RedisCache) Set(key string, value []byte, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, cachecontract.ErrInvalidTTL
	}
	if value == nil {
		return false, cachecontract.ErrNilValue
	}

	err := c.client.Set(context.Background(), c.prefixedKey(key), value, ttl)
	return err == nil, err
}

func (c *RedisCache) ReserveFence(key string) (cachecontract.FenceToken, error) {
	token, err := newFenceToken()
	if err != nil {
		return "", err
	}
	leaseMillis := c.fenceLease.Milliseconds()
	if leaseMillis <= 0 {
		leaseMillis = 1
	}
	if _, err := c.client.Eval(
		context.Background(),
		reserveFenceScript,
		[]string{c.fenceKey(key)},
		string(token),
		strconv.FormatInt(leaseMillis, 10),
	); err != nil {
		return "", err
	}

	return token, nil
}

func (c *RedisCache) ReleaseFence(key string, token cachecontract.FenceToken) (bool, error) {
	result, err := c.client.Eval(
		context.Background(),
		releaseFenceScript,
		[]string{c.fenceKey(key)},
		string(token),
	)
	if err != nil {
		return false, err
	}

	released, err := redisIntegerResult(result)
	return released > 0, err
}

func (c *RedisCache) SetWithFence(key string, value []byte, ttl time.Duration, token cachecontract.FenceToken) (bool, error) {
	if ttl <= 0 {
		return false, cachecontract.ErrInvalidTTL
	}
	if value == nil {
		return false, cachecontract.ErrNilValue
	}

	ttlMillis := ttl.Milliseconds()
	if ttlMillis <= 0 {
		ttlMillis = 1
	}
	return c.evalFenceWrite(key, value, token, strconv.FormatInt(ttlMillis, 10))
}

func (c *RedisCache) Forever(key string, value []byte) (bool, error) {
	if value == nil {
		return false, cachecontract.ErrNilValue
	}

	err := c.client.Set(context.Background(), c.prefixedKey(key), value, 0)
	return err == nil, err
}

func (c *RedisCache) ForeverWithFence(key string, value []byte, token cachecontract.FenceToken) (bool, error) {
	if value == nil {
		return false, cachecontract.ErrNilValue
	}

	return c.evalFenceWrite(key, value, token, "0")
}

func (c *RedisCache) Forget(key string) (bool, error) {
	deleted, err := c.client.Del(context.Background(), c.prefixedKey(key))
	return deleted > 0, err
}

func (c *RedisCache) ForgetWithFence(key string) (bool, error) {
	result, err := c.client.Eval(
		context.Background(),
		forgetWithFenceScript,
		[]string{c.prefixedKey(key), c.fenceKey(key)},
	)
	if err != nil {
		return false, err
	}

	deleted, err := redisIntegerResult(result)
	return deleted > 0, err
}

func (c *RedisCache) ForgetIfFence(key string, token cachecontract.FenceToken) (bool, error) {
	result, err := c.client.Eval(
		context.Background(),
		forgetIfFenceScript,
		[]string{c.prefixedKey(key), c.fenceKey(key)},
		string(token),
	)
	if err != nil {
		return false, err
	}

	deleted, err := redisIntegerResult(result)
	return deleted > 0, err
}

func (c *RedisCache) Touch(key string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, cachecontract.ErrInvalidTTL
	}

	return c.client.Expire(context.Background(), c.prefixedKey(key), ttl)
}

func (c *RedisCache) TouchWithFence(key string, ttl time.Duration, token cachecontract.FenceToken) (bool, error) {
	if ttl <= 0 {
		return false, cachecontract.ErrInvalidTTL
	}

	result, err := c.client.Eval(
		context.Background(),
		touchWithFenceScript,
		[]string{c.prefixedKey(key), c.fenceKey(key)},
		string(token),
		int64((ttl+time.Second-1)/time.Second),
	)
	if err != nil {
		return false, err
	}

	touched, err := redisIntegerResult(result)
	return touched > 0, err
}

func (c *RedisCache) Flush() (bool, error) {
	ctx := context.Background()
	if err := c.deleteMatching(ctx, redisScanPattern(c.prefix)); err != nil {
		return false, err
	}
	if err := c.deleteMatching(ctx, c.fenceKeyPrefix()+"*"); err != nil {
		return false, err
	}
	return true, nil
}

func (c *RedisCache) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.client.Close()
	})

	return c.closeErr
}

func (c *RedisCache) prefixedKey(key string) string {
	return c.prefix + key
}

func (c *RedisCache) fenceKeyPrefix() string {
	return fenceMetadataPrefix + hex.EncodeToString([]byte(c.prefix)) + ":"
}

func (c *RedisCache) fenceKey(key string) string {
	return c.fenceKeyPrefix() + c.fenceField(key)
}

func (c *RedisCache) fenceField(key string) string {
	digest := sha256.Sum256([]byte(c.prefixedKey(key)))
	return hex.EncodeToString(digest[:])
}

func (c *RedisCache) evalFenceWrite(key string, value []byte, token cachecontract.FenceToken, ttlMillis string) (bool, error) {
	result, err := c.client.Eval(
		context.Background(),
		setWithFenceScript,
		[]string{c.prefixedKey(key), c.fenceKey(key)},
		string(token),
		value,
		ttlMillis,
	)
	if err != nil {
		return false, err
	}

	stored, err := redisIntegerResult(result)
	return stored > 0, err
}

func (c *RedisCache) deleteMatching(ctx context.Context, match string) error {
	cursor := uint64(0)
	for {
		keys, nextCursor, err := c.client.Scan(ctx, cursor, match, flushScanCount)
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if _, err := c.client.Del(ctx, keys...); err != nil {
				return err
			}
		}
		cursor = nextCursor
		if cursor == 0 {
			return nil
		}
	}
}

func newFenceToken() (cachecontract.FenceToken, error) {
	tokenBytes := make([]byte, 16)
	if _, err := cryptorand.Read(tokenBytes); err != nil {
		return "", fmt.Errorf("redis: generate fence token: %w", err)
	}

	return cachecontract.FenceToken(hex.EncodeToString(tokenBytes)), nil
}

func redisIntegerResult(result interface{}) (int64, error) {
	switch value := result.(type) {
	case int64:
		return value, nil
	case int:
		return int64(value), nil
	case string:
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("redis: invalid integer result %q: %w", value, err)
		}
		return parsed, nil
	case []byte:
		parsed, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("redis: invalid integer result %q: %w", value, err)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("redis: unexpected integer result type %T", result)
	}
}

func redisScanPattern(prefix string) string {
	var pattern strings.Builder
	pattern.Grow(len(prefix) + 1)

	for _, char := range prefix {
		switch char {
		case '\\', '*', '?', '[':
			pattern.WriteByte('\\')
		}
		pattern.WriteRune(char)
	}
	pattern.WriteByte('*')

	return pattern.String()
}

func parseGetWithTTLResult(result interface{}) ([]byte, time.Duration, error) {
	parts, ok := result.([]interface{})
	if !ok || len(parts) != 2 {
		return nil, 0, fmt.Errorf("redis: unexpected GET result type %T", result)
	}

	value, err := redisResultBytes(parts[0])
	if err != nil {
		return nil, 0, err
	}
	if value == nil {
		return nil, 0, nil
	}

	ttlMillis, ok := parts[1].(int64)
	if !ok {
		return nil, 0, fmt.Errorf("redis: unexpected PTTL result type %T", parts[1])
	}
	if ttlMillis == -1 {
		return value, 0, nil
	}
	if ttlMillis == -2 {
		return nil, 0, nil
	}
	if ttlMillis < 0 {
		return nil, 0, fmt.Errorf("redis: unexpected PTTL value %d", ttlMillis)
	}
	if ttlMillis == 0 {
		return value, time.Nanosecond, nil
	}

	return value, time.Duration(ttlMillis) * time.Millisecond, nil
}

func redisResultBytes(result interface{}) ([]byte, error) {
	switch value := result.(type) {
	case nil:
		return nil, nil
	case bool:
		if !value {
			return nil, nil
		}
		return nil, fmt.Errorf("redis: unexpected boolean GET result")
	case string:
		return []byte(value), nil
	case []byte:
		return append([]byte(nil), value...), nil
	default:
		return nil, fmt.Errorf("redis: unexpected value result type %T", result)
	}
}
