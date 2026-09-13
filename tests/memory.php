<?php

declare(strict_types=1);

function fail(string $message): never
{
    fwrite(STDERR, $message . "\n");
    exit(1);
}

function expect_true(bool $condition, string $message): void
{
    if (! $condition) {
        fail($message);
    }
}

$functions = [
    'franken_cache_memory_get',
    'franken_cache_memory_set',
    'franken_cache_memory_forever',
    'franken_cache_memory_forget',
    'franken_cache_memory_touch',
    'franken_cache_memory_flush',
];

foreach ($functions as $function) {
    expect_true(function_exists($function), "Missing PHP function {$function}().");
}

expect_true(franken_cache_memory_flush(), 'Initial MemoryCache flush failed.');
expect_true(franken_cache_memory_get('missing') === false, 'Cache miss must return false.');
expect_true(franken_cache_memory_touch('missing', 60) === false, 'Touch must return false for a missing key.');

try {
    franken_cache_memory_set('invalid-ttl', 'value', 0);
    fail('Set with ttl=0 must throw ValueError.');
} catch (ValueError) {
}

expect_true(franken_cache_memory_set('plain', 'value', 60), 'Set failed.');
expect_true(franken_cache_memory_get('plain') === 'value', 'Get returned an unexpected value.');

$binary = "A\0B\xFF\x00C";
expect_true(franken_cache_memory_set('binary', $binary, 60), 'Binary Set failed.');
expect_true(franken_cache_memory_get('binary') === $binary, 'Binary payload was not preserved.');

expect_true(franken_cache_memory_set('empty', '', 60), 'Empty payload Set failed.');
expect_true(franken_cache_memory_get('empty') === '', 'Empty payload was not preserved.');

expect_true(franken_cache_memory_forever('forever', 'persistent'), 'Forever failed.');
expect_true(franken_cache_memory_get('forever') === 'persistent', 'Forever value is missing.');

expect_true(franken_cache_memory_set('touch', 'value', 60), 'Touch fixture Set failed.');
expect_true(franken_cache_memory_touch('touch', 1), 'Touch failed.');
usleep(1_100_000);
expect_true(franken_cache_memory_get('touch') === false, 'Touched key did not expire.');

expect_true(franken_cache_memory_forget('plain'), 'Forget failed for a live key.');
expect_true(franken_cache_memory_get('plain') === false, 'Forgotten key is still present.');
expect_true(franken_cache_memory_forget('plain') === false, 'Forget must return false for a missing key.');

expect_true(franken_cache_memory_flush(), 'Final MemoryCache flush failed.');
expect_true(franken_cache_memory_get('binary') === false, 'Flush did not remove cached values.');
expect_true(franken_cache_memory_get('forever') === false, 'Flush did not remove forever values.');

fwrite(STDOUT, "franken_cache MemoryCache PHP bridge smoke test passed.\n");
