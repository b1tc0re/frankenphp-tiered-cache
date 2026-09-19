package frankencache

/*
#include "extension.h"
#include <stdlib.h>
*/
import "C"

import (
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/caddyserver/caddy/v2"
	"github.com/dunglas/frankenphp"

	cachecontract "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache"
	"github.com/b1tc0re/frankenphp-tiered-cache/internal/cache/memory"
	redisbackend "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache/redis"
	tieredcache "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache/tiered"
	"github.com/b1tc0re/frankenphp-tiered-cache/internal/observability"
)

var phpTieredCache cachecontract.Cache

const phpTieredPressureLogInterval = 10 * time.Second

type phpMemoryObserver struct {
	reporter *observability.PressureReporter
}

//go:embed version.json
var extensionVersionFile []byte

func extensionVersion() string {
	var metadata struct {
		Version string `json:"version"`
	}

	if err := json.Unmarshal(extensionVersionFile, &metadata); err != nil {
		panic(fmt.Sprintf("franken_cache: parse version.json: %v", err))
	}

	version := strings.TrimSpace(metadata.Version)
	if version == "" {
		panic("franken_cache: version.json contains an empty version")
	}

	return version
}

func (o *phpMemoryObserver) OnEviction(summary memory.EvictionSummary) {
	o.reporter.Observe(summary.Entries, summary.Bytes)
}

func init() {
	version := C.CString(extensionVersion())
	C.franken_cache_set_version(version)
	C.free(unsafe.Pointer(version))

	reporter, err := observability.NewPressureReporter(phpTieredPressureLogInterval, func(summary observability.PressureSummary) {
		caddy.Log().Named("franken_cache").Warn(fmt.Sprintf(
			"MemoryCache evicted live entries due to memory pressure: evicted_entries=%d evicted_bytes=%d window=%s",
			summary.EvictedEntries,
			summary.EvictedBytes,
			summary.Window,
		))
	})
	if err != nil {
		panic(fmt.Sprintf("franken_cache: initialize pressure reporter: %v", err))
	}

	l1, err := memory.New(memory.Config{Observer: &phpMemoryObserver{reporter: reporter}})
	if err != nil {
		reporter.Close()
		panic(fmt.Sprintf("franken_cache: initialize MemoryCache: %v", err))
	}

	redisConfig, err := phpRedisConfigFromEnv()
	if err != nil {
		_ = l1.Close()
		reporter.Close()
		panic(fmt.Sprintf("franken_cache: parse Redis configuration: %v", err))
	}

	l2, err := redisbackend.New(redisConfig)
	if err != nil {
		_ = l1.Close()
		reporter.Close()
		panic(fmt.Sprintf("franken_cache: initialize RedisCache: %v", err))
	}

	bus, err := redisbackend.NewInvalidationBus(redisConfig)
	if err != nil {
		_ = l2.Close()
		_ = l1.Close()
		reporter.Close()
		panic(fmt.Sprintf("franken_cache: initialize Redis invalidation bus: %v", err))
	}

	tieredConfig, err := phpTieredConfigFromEnv()
	if err != nil {
		_ = bus.Close()
		_ = l2.Close()
		_ = l1.Close()
		reporter.Close()
		panic(fmt.Sprintf("franken_cache: parse TieredCache configuration: %v", err))
	}

	cache, err := tieredcache.NewWithInvalidation(tieredConfig, l1, l2, bus)
	if err != nil {
		_ = bus.Close()
		_ = l2.Close()
		_ = l1.Close()
		reporter.Close()
		panic(fmt.Sprintf("franken_cache: initialize TieredCache: %v", err))
	}
	phpTieredCache = cache

	frankenphp.RegisterExtension(unsafe.Pointer(&C.franken_cache_module_entry))
}

//export franken_cache_tiered_get_go
func franken_cache_tiered_get_go(key *C.zend_string, status *C.int) *C.zend_string {
	value, _, err := phpTieredCache.Get(frankenphp.GoString(unsafe.Pointer(key)))
	if err != nil {
		*status = -1
		return nil
	}
	if value == nil {
		*status = 0
		return nil
	}

	*status = 1
	if len(value) == 0 {
		return nil
	}

	valueString := unsafe.String(unsafe.SliceData(value), len(value))
	return (*C.zend_string)(frankenphp.PHPString(valueString, false))
}

//export franken_cache_tiered_many_go
func franken_cache_tiered_many_go(items *C.franken_cache_many_item, count C.size_t) C.int {
	if count == 0 {
		return 0
	}

	itemSlice := unsafe.Slice(items, int(count))
	keys := make([]string, int(count))
	for i, item := range itemSlice {
		if item.key == nil {
			return -1
		}
		keys[i] = frankenphp.GoString(unsafe.Pointer(item.key))
	}

	values, err := phpTieredCache.GetMany(keys)
	if err != nil {
		return -1
	}
	for i, key := range keys {
		item, ok := values[key]
		if !ok {
			continue
		}
		itemSlice[i].found = 1
		itemSlice[i].value = (*C.zend_string)(frankenphp.PHPString(string(item.Value), false))
		if len(item.Value) > 0 && itemSlice[i].value == nil {
			return -1
		}
	}

	return 0
}

//export franken_cache_tiered_add_go
func franken_cache_tiered_add_go(key, value *C.zend_string, ttlSeconds C.zend_long) C.int {
	ttl, ok := phpTTL(ttlSeconds)
	if !ok {
		return -1
	}

	added, err := phpTieredCache.Add(
		frankenphp.GoString(unsafe.Pointer(key)),
		phpBytes(value),
		ttl,
	)
	if err != nil && !errors.Is(err, tieredcache.ErrPostCommit) {
		return -1
	}
	return phpBoolResult(added, nil)
}

//export franken_cache_tiered_put_many_go
func franken_cache_tiered_put_many_go(items *C.franken_cache_put_many_item, count C.size_t, ttlSeconds C.zend_long) C.int {
	ttl, ok := phpTTL(ttlSeconds)
	if !ok {
		return -1
	}
	if count == 0 {
		return 0
	}

	itemSlice := unsafe.Slice(items, int(count))
	values := make(map[string][]byte, int(count))
	for _, item := range itemSlice {
		if item.key == nil || item.value == nil {
			return -1
		}
		values[frankenphp.GoString(unsafe.Pointer(item.key))] = phpBytes(item.value)
	}

	stored, err := phpTieredCache.SetMany(values, ttl)
	if err != nil && !errors.Is(err, tieredcache.ErrPostCommit) {
		return -1
	}
	return phpBoolResult(stored, nil)
}

//export franken_cache_tiered_set_go
func franken_cache_tiered_set_go(key, value *C.zend_string, ttlSeconds C.zend_long) C.int {
	ttl, ok := phpTTL(ttlSeconds)
	if !ok {
		return -1
	}

	stored, err := phpTieredCache.Set(
		frankenphp.GoString(unsafe.Pointer(key)),
		phpBytes(value),
		ttl,
	)
	return phpBoolResult(stored, err)
}

//export franken_cache_tiered_forever_go
func franken_cache_tiered_forever_go(key, value *C.zend_string) C.int {
	stored, err := phpTieredCache.Forever(
		frankenphp.GoString(unsafe.Pointer(key)),
		phpBytes(value),
	)
	return phpBoolResult(stored, err)
}

//export franken_cache_tiered_forget_go
func franken_cache_tiered_forget_go(key *C.zend_string) C.int {
	removed, err := phpTieredCache.Forget(frankenphp.GoString(unsafe.Pointer(key)))
	return phpBoolResult(removed, err)
}

//export franken_cache_tiered_touch_go
func franken_cache_tiered_touch_go(key *C.zend_string, ttlSeconds C.zend_long) C.int {
	ttl, ok := phpTTL(ttlSeconds)
	if !ok {
		return -1
	}

	touched, err := phpTieredCache.Touch(frankenphp.GoString(unsafe.Pointer(key)), ttl)
	return phpBoolResult(touched, err)
}

//export franken_cache_tiered_flush_go
func franken_cache_tiered_flush_go() C.int {
	flushed, err := phpTieredCache.Flush()
	return phpBoolResult(flushed, err)
}

//export franken_cache_tiered_increment_go
func franken_cache_tiered_increment_go(key *C.zend_string, value C.zend_long, result *C.zend_long) C.int {
	changed, err := phpTieredCache.Increment(
		frankenphp.GoString(unsafe.Pointer(key)),
		int64(value),
	)
	if err != nil && !errors.Is(err, tieredcache.ErrPostCommit) {
		return -1
	}

	*result = C.zend_long(changed)
	return 0
}

//export franken_cache_tiered_decrement_go
func franken_cache_tiered_decrement_go(key *C.zend_string, value C.zend_long, result *C.zend_long) C.int {
	changed, err := phpTieredCache.Decrement(
		frankenphp.GoString(unsafe.Pointer(key)),
		int64(value),
	)
	if err != nil && !errors.Is(err, tieredcache.ErrPostCommit) {
		return -1
	}

	*result = C.zend_long(changed)
	return 0
}

func phpRedisConfigFromEnv() (redisbackend.Config, error) {
	config := redisbackend.Config{
		Addr:      os.Getenv("FRANKEN_CACHE_REDIS_ADDR"),
		Username:  os.Getenv("FRANKEN_CACHE_REDIS_USERNAME"),
		Password:  os.Getenv("FRANKEN_CACHE_REDIS_PASSWORD"),
		KeyPrefix: os.Getenv("FRANKEN_CACHE_REDIS_PREFIX"),
	}

	if value, ok := os.LookupEnv("FRANKEN_CACHE_REDIS_DB"); ok {
		db, err := strconv.Atoi(value)
		if err != nil {
			return redisbackend.Config{}, fmt.Errorf("FRANKEN_CACHE_REDIS_DB must be an integer: %w", err)
		}
		config.DB = db
	}

	for _, setting := range []struct {
		name   string
		target *time.Duration
	}{
		{name: "FRANKEN_CACHE_REDIS_DIAL_TIMEOUT", target: &config.DialTimeout},
		{name: "FRANKEN_CACHE_REDIS_READ_TIMEOUT", target: &config.ReadTimeout},
		{name: "FRANKEN_CACHE_REDIS_WRITE_TIMEOUT", target: &config.WriteTimeout},
	} {
		if value, ok := os.LookupEnv(setting.name); ok {
			duration, err := time.ParseDuration(value)
			if err != nil {
				return redisbackend.Config{}, fmt.Errorf("%s must be a duration: %w", setting.name, err)
			}
			*setting.target = duration
		}
	}

	return config, nil
}

func phpTieredConfigFromEnv() (tieredcache.Config, error) {
	config := tieredcache.Config{}
	if value, ok := os.LookupEnv("FRANKEN_CACHE_RECOVERY_INTERVAL"); ok {
		duration, err := time.ParseDuration(value)
		if err != nil {
			return tieredcache.Config{}, fmt.Errorf("FRANKEN_CACHE_RECOVERY_INTERVAL must be a duration: %w", err)
		}
		config.RecoveryInterval = duration
	}

	return config, nil
}

func phpBytes(value *C.zend_string) []byte {
	if value == nil || value.len == 0 {
		return make([]byte, 0)
	}
	if uint64(value.len) > math.MaxInt32 {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(&value.val[0]), C.int(value.len))
}

func phpTTL(seconds C.zend_long) (time.Duration, bool) {
	value := int64(seconds)
	if value <= 0 || value > int64(math.MaxInt64)/int64(time.Second) {
		return 0, false
	}
	return time.Duration(value) * time.Second, true
}

func phpBoolResult(value bool, err error) C.int {
	if err != nil {
		return -1
	}
	if value {
		return 1
	}
	return 0
}
