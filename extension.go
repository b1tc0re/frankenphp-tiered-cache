package frankencache

/*
#include "extension.h"
*/
import "C"

import (
	"fmt"
	"math"
	"time"
	"unsafe"

	"github.com/caddyserver/caddy/v2"
	"github.com/dunglas/frankenphp"

	cachecontract "github.com/b1tc0re/frankenphp-tiered-cache/internal/cache"
	"github.com/b1tc0re/frankenphp-tiered-cache/internal/cache/memory"
)

var phpMemoryCache cachecontract.Cache

type phpMemoryObserver struct{}

func (phpMemoryObserver) OnEviction(memory.EvictionEvent) {
	caddy.Log().Named("franken_cache").Warn("MemoryCache evicted a live entry due to memory pressure")
}

func init() {
	cache, err := memory.New(memory.Config{Observer: phpMemoryObserver{}})
	if err != nil {
		panic(fmt.Sprintf("franken_cache: initialize MemoryCache: %v", err))
	}
	phpMemoryCache = cache

	frankenphp.RegisterExtension(unsafe.Pointer(&C.franken_cache_module_entry))
}

//export franken_cache_memory_get_go
func franken_cache_memory_get_go(key *C.zend_string, status *C.int) *C.zend_string {
	value, err := phpMemoryCache.Get(frankenphp.GoString(unsafe.Pointer(key)))
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

//export franken_cache_memory_set_go
func franken_cache_memory_set_go(key, value *C.zend_string, ttlSeconds C.zend_long) C.int {
	ttl, ok := phpTTL(ttlSeconds)
	if !ok {
		return -1
	}

	stored, err := phpMemoryCache.Set(
		frankenphp.GoString(unsafe.Pointer(key)),
		phpBytes(value),
		ttl,
	)
	return phpBoolResult(stored, err)
}

//export franken_cache_memory_forever_go
func franken_cache_memory_forever_go(key, value *C.zend_string) C.int {
	stored, err := phpMemoryCache.Forever(
		frankenphp.GoString(unsafe.Pointer(key)),
		phpBytes(value),
	)
	return phpBoolResult(stored, err)
}

//export franken_cache_memory_forget_go
func franken_cache_memory_forget_go(key *C.zend_string) C.int {
	removed, err := phpMemoryCache.Forget(frankenphp.GoString(unsafe.Pointer(key)))
	return phpBoolResult(removed, err)
}

//export franken_cache_memory_touch_go
func franken_cache_memory_touch_go(key *C.zend_string, ttlSeconds C.zend_long) C.int {
	ttl, ok := phpTTL(ttlSeconds)
	if !ok {
		return -1
	}

	touched, err := phpMemoryCache.Touch(frankenphp.GoString(unsafe.Pointer(key)), ttl)
	return phpBoolResult(touched, err)
}

//export franken_cache_memory_flush_go
func franken_cache_memory_flush_go() C.int {
	flushed, err := phpMemoryCache.Flush()
	return phpBoolResult(flushed, err)
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
