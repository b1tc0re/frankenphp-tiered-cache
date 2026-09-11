<?php

declare(strict_types=1);

$extension = 'franken_tiered';
$expectedVersion = '0.0.0-dev';

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

fwrite(STDOUT, "franken_tiered smoke test passed ({$version}).\n");
