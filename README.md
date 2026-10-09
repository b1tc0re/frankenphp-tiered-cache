# FrankenPHP Tiered Cache
[![Software License](https://img.shields.io/badge/license-MIT-brightgreen.svg?style=flat-square)](LICENSE)
[![Go Coverage](https://codecov.io/gh/b1tc0re/frankenphp-tiered-cache/branch/main/graph/badge.svg)](https://codecov.io/gh/b1tc0re/frankenphp-tiered-cache)
[![Documentation](https://img.shields.io/badge/docs-VitePress-646cff)](https://b1tc0re.github.io/frankenphp-tiered-cache/)
[![Latest Release](https://img.shields.io/github/v/release/b1tc0re/frankenphp-tiered-cache)](https://github.com/b1tc0re/frankenphp-tiered-cache/releases/latest)

Go-модуль и PHP-расширение для FrankenPHP. `TieredCache` хранит общий источник
данных в Redis, а `MemoryCache` использует память каждого процесса как быстрый L1.

> Проект экспериментальный. API и конфигурация могут меняться без обратной
> совместимости.

## Быстрый старт

Для локальной сборки нужны Docker и [Task](https://taskfile.dev/):

```bash
task build
task test:integration
```

Интеграционная проверка поднимает Redis и собирает FrankenPHP с расширением.
Для production-сборки и подключения Laravel adapter смотрите
[руководство по развёртыванию](docs/src/deployment.md).

## Документация

Полная документация: [b1tc0re.github.io/frankenphp-tiered-cache](https://b1tc0re.github.io/frankenphp-tiered-cache/).

## Лицензия

MIT.
