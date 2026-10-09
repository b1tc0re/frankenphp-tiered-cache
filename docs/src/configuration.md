---
title: Конфигурация
---

[← Ко всем разделам](index.md)

# Конфигурация

Расширение читает параметры окружения при инициализации Go-модуля. После их
изменения перезапустите FrankenPHP; пересобирать image не нужно.

## Redis и recovery

| Переменная | Назначение | Значение по умолчанию |
| --- | --- | --- |
| `FRANKEN_CACHE_REDIS_ADDR` | адрес Redis, включая порт | `127.0.0.1:6379` |
| `FRANKEN_CACHE_REDIS_USERNAME` | Redis ACL username | пусто |
| `FRANKEN_CACHE_REDIS_PASSWORD` | пароль Redis | пусто |
| `FRANKEN_CACHE_REDIS_DB` | номер Redis DB | `0` |
| `FRANKEN_CACHE_REDIS_PREFIX` | префикс ключей и invalidation channel | `franken_cache:` |
| `FRANKEN_CACHE_REDIS_DIAL_TIMEOUT` | timeout подключения | default go-redis |
| `FRANKEN_CACHE_REDIS_READ_TIMEOUT` | timeout чтения Redis | default go-redis |
| `FRANKEN_CACHE_REDIS_WRITE_TIMEOUT` | timeout записи Redis | default go-redis |
| `FRANKEN_CACHE_RECOVERY_INTERVAL` | пауза между retry в `degraded` | `5s` |

Timeout и recovery interval задаются в формате Go duration: например `500ms` или
`5s`. `FRANKEN_CACHE_REDIS_ADDR` должен содержать порт, например
`redis.internal:6379`.

Все экземпляры одного кеша должны подключаться к одному Redis endpoint и
использовать одинаковые DB и `KeyPrefix`. Эти значения задают общий namespace,
Pub/Sub channel и ключ tracking-маркера.

## Доступ Redis для invalidation

Cross-pod invalidation требует Redis Pub/Sub. Для обнаружения внешней очистки L1
также использует Redis 6.0+, RESP3 и `CLIENT TRACKING`/`CLIENT ID`. Redis ACL
должны разрешать эти команды, `GET` и `SETNX` для ключа
`<KeyPrefix>\x00frankenphp-tiered-cache:l1-generation`, а также Pub/Sub-команды
`SUBSCRIBE` и `PUBLISH`. Redis-клиент включает RESP3 сам; прокси между приложением
и Redis должен пропускать tracking push-уведомления. Отдельно настраивать
`notify-keyspace-events` не нужно.

При недоступном tracking или отказе ACL подписка не становится healthy: экземпляр
очищает локальный L1 и остаётся в `degraded`, пока не восстановит подписку.

## Память L1

PHP-расширение использует фиксированные defaults `64 MiB` на весь L1 и `4 MiB`
на одну запись. Переменных окружения для изменения этих лимитов сейчас нет. Их
учёт и политика eviction описаны в разделе [MemoryCache](memory-cache.md).
