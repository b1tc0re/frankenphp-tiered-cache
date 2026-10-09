---
title: Развёртывание
---

[← Ко всем разделам](index.md)

# Развёртывание

В production FrankenPHP собирается с расширением через `xcaddy`. Закрепляйте
версию модуля на release tag или commit, а не на плавающую ветку.

```dockerfile
ARG FRANKENPHP_VERSION=1.12.7
ARG PHP_VERSION=8.4-bookworm
ARG FRANKEN_CACHE_VERSION=<release-tag-or-commit>

FROM dunglas/frankenphp:${FRANKENPHP_VERSION}-builder-php${PHP_VERSION} AS upstream

ARG FRANKEN_CACHE_VERSION

COPY --from=caddy:builder /usr/bin/xcaddy /usr/bin/xcaddy

RUN CGO_ENABLED=1 \
    XCADDY_SETCAP=0 \
    XCADDY_GO_BUILD_FLAGS="-ldflags='-w -s' -tags=nobadger,nomysql,nopgx" \
    CGO_CFLAGS="$(php-config --includes)" \
    CGO_LDFLAGS="$(php-config --ldflags) $(php-config --libs)" \
    xcaddy build \
        --output /usr/local/bin/frankenphp \
        --with github.com/dunglas/frankenphp=./ \
        --with github.com/dunglas/frankenphp/caddy=./caddy/ \
        --with github.com/dunglas/caddy-cbrotli \
        --with github.com/b1tc0re/frankenphp-tiered-cache@${FRANKEN_CACHE_VERSION}

FROM dunglas/frankenphp:${FRANKENPHP_VERSION}-php${PHP_VERSION}

COPY --from=upstream /usr/local/bin/frankenphp /usr/local/bin/frankenphp
```

Если этот репозиторий включён в Docker build context, модуль можно собрать по
локальному пути:

```dockerfile
COPY frankenphp-tiered-cache /src/frankenphp-tiered-cache
...
--with github.com/b1tc0re/frankenphp-tiered-cache=/src/frankenphp-tiered-cache
```

## Переменные контейнера

Передайте Redis-параметры при запуске контейнера, например в Compose или
Kubernetes:

```yaml
environment:
  FRANKEN_CACHE_REDIS_ADDR: redis.internal:6379
  FRANKEN_CACHE_REDIS_DB: "1"
  FRANKEN_CACHE_REDIS_USERNAME: cache-user
  FRANKEN_CACHE_REDIS_PASSWORD: ${REDIS_PASSWORD}
  FRANKEN_CACHE_REDIS_PREFIX: "franken_cache:"
  FRANKEN_CACHE_RECOVERY_INTERVAL: 5s
```

Полный список находится в [конфигурации](configuration.md). PHP adapter должен
вызывать `franken_cache_tiered_*` и отдельно выбираться приложением как его
cache store. Само расширение не меняет Laravel `CACHE_STORE`; PHP Redis extension
для подключения TieredCache не требуется.

## Laravel adapter

Установите пакет `b1tc0re/laravel-franken-cache`:

```bash
composer require b1tc0re/laravel-franken-cache
```

Зарегистрируйте его store `franken` и выберите `CACHE_STORE=franken` в Laravel.
Инструкции по версии пакета и его настройке смотрите в README самого adapter:
его выпуск и совместимость управляются отдельно от Go-модуля.
