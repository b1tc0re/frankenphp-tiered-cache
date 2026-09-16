package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	cachecontract "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache"
	goredis "github.com/redis/go-redis/v9"
)

type client interface {
	Get(ctx context.Context, key string) ([]byte, time.Duration, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	IncrBy(ctx context.Context, key string, value int64) (int64, error)
	DecrBy(ctx context.Context, key string, value int64) (int64, error)
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

func (c redisClient) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	return c.client.IncrBy(ctx, key, value).Result()
}

func (c redisClient) DecrBy(ctx context.Context, key string, value int64) (int64, error) {
	return c.client.DecrBy(ctx, key, value).Result()
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
// TieredCache performs every L2 mutation synchronously and uses this backend
// as the source of truth.
type RedisCache struct {
	client client
	prefix string

	closeOnce sync.Once
	closeErr  error
}

var _ cachecontract.Cache = (*RedisCache)(nil)

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
		prefix: config.KeyPrefix,
	}, nil
}

func newWithClient(c client, prefix string) *RedisCache {
	return &RedisCache{client: c, prefix: prefix}
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

func (c *RedisCache) Forever(key string, value []byte) (bool, error) {
	if value == nil {
		return false, cachecontract.ErrNilValue
	}

	err := c.client.Set(context.Background(), c.prefixedKey(key), value, 0)
	return err == nil, err
}

// Increment atomically increments a signed Redis counter. Redis keeps the
// existing TTL, or creates a persistent key when the counter is missing.
func (c *RedisCache) Increment(key string, value int64) (int64, error) {
	result, err := c.client.IncrBy(context.Background(), c.prefixedKey(key), value)
	return result, wrapRedisCommandError(err)
}

// Decrement atomically decrements a signed Redis counter. Redis keeps the
// existing TTL, or creates a persistent key when the counter is missing.
func (c *RedisCache) Decrement(key string, value int64) (int64, error) {
	result, err := c.client.DecrBy(context.Background(), c.prefixedKey(key), value)
	return result, wrapRedisCommandError(err)
}

func wrapRedisCommandError(err error) error {
	if err == nil {
		return nil
	}

	var redisErr goredis.Error
	if errors.As(err, &redisErr) && isLogicalCounterError(err) {
		return fmt.Errorf("%w: %w", cachecontract.ErrRedisCommand, err)
	}

	return err
}

func isLogicalCounterError(err error) bool {
	return goredis.HasErrorPrefix(err, "value is not an integer or out of range") ||
		goredis.HasErrorPrefix(err, "increment or decrement would overflow") ||
		goredis.HasErrorPrefix(err, "WRONGTYPE")
}

func (c *RedisCache) Forget(key string) (bool, error) {
	deleted, err := c.client.Del(context.Background(), c.prefixedKey(key))
	return deleted > 0, err
}

func (c *RedisCache) Touch(key string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, cachecontract.ErrInvalidTTL
	}

	return c.client.Expire(context.Background(), c.prefixedKey(key), ttl)
}

func (c *RedisCache) Flush() (bool, error) {
	if err := c.deleteMatching(context.Background(), redisScanPattern(c.prefix)); err != nil {
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
	case []byte:
		return append([]byte(nil), value...), nil
	case string:
		return []byte(value), nil
	case bool:
		if !value {
			return nil, nil
		}
		return nil, fmt.Errorf("redis: unexpected boolean GET result")
	default:
		return nil, fmt.Errorf("redis: unexpected value result type %T", result)
	}
}
