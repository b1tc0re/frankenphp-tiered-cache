<?php

/** @generate-class-entries */

function franken_tiered_memory_get(string $key): string|false {}

function franken_tiered_memory_set(string $key, string $value, int $ttl): bool {}

function franken_tiered_memory_forever(string $key, string $value): bool {}

function franken_tiered_memory_forget(string $key): bool {}

function franken_tiered_memory_touch(string $key, int $ttl): bool {}

function franken_tiered_memory_flush(): bool {}
