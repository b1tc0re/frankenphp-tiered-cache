<?php

declare(strict_types=1);

$extension = 'franken_cache';
$versionFile = dirname(__DIR__) . '/version.json';
$metadata = json_decode(
    (string) file_get_contents($versionFile),
    true,
     flags: JSON_THROW_ON_ERROR,
);
$expectedVersion = trim((string) ($metadata['version'] ?? ''));

if ($expectedVersion === '') {
    fwrite(STDERR, "Extension version.json is empty or missing.\n");
    exit(1);
}

if (! extension_loaded($extension)) {
    fwrite(STDERR, "Extension {$extension} is not loaded.\n");
    exit(1);
}

$version = phpversion($extension);

if ($version !== $expectedVersion) {
    fwrite(
        STDERR,
        sprintf(
            "Unexpected %s version: expected %s, got %s.\n",
            $extension,
            $expectedVersion,
            var_export($version, true),
        ),
    );
    exit(1);
}

fwrite(STDOUT, "franken_cache smoke test passed ({$version}).\n");
