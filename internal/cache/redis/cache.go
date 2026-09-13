package redis

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	cachecontract "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache"
	goredis "github.com/redis/go-redis/v9"
)

type client interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Del(ctx context.Context, keys ...string) (int64, error)
	Expire(ctx context.Context, key string, ttl time.Duration) (bool, error)
	Scan(ctx context.Context, cursor uint64, match string, count int64) ([]string, uint64, error)
	Close() error
}

type redisClient struct {
	client *goredis.Client
}

func (c redisClient) Get(ctx context.Context, key string) ([]byte, error) {
	return c.client.Get(ctx, key).Bytes()
}

func (c redisClient) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.client.Set(ctx, key, value, ttl).Err()
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

func (c *RedisCache) Get(key string) ([]byte, error) {
	value, err := c.client.Get(context.Background(), c.prefixedKey(key))
	if errors.Is(err, goredis.Nil) {
		return nil, nil
	}

	return value, err
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
	ctx := context.Background()
	match := redisScanPattern(c.prefix)
	cursor := uint64(0)

	for {
		keys, nextCursor, err := c.client.Scan(ctx, cursor, match, flushScanCount)
		if err != nil {
			return false, err
		}

		if len(keys) > 0 {
			if _, err := c.client.Del(ctx, keys...); err != nil {
				return false, err
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			return true, nil
		}
	}
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
