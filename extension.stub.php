<?php

/** @generate-class-entries */

function franken_cache_tiered_get(string $key): string|false {}

function franken_cache_tiered_set(string $key, string $value, int $ttl): bool {}

function franken_cache_tiered_forever(string $key, string $value): bool {}

function franken_cache_tiered_forget(string $key): bool {}

function franken_cache_tiered_touch(string $key, int $ttl): bool {}

function franken_cache_tiered_flush(): bool {}

function franken_cache_tiered_increment(string $key, int $value): int {}

function franken_cache_tiered_decrement(string $key, int $value): int {}
