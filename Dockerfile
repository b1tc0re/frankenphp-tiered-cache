ARG FRANKENPHP_VERSION=1.12.7

FROM dunglas/frankenphp:${FRANKENPHP_VERSION}-builder-php8.4-bookworm AS builder

COPY --from=caddy:builder /usr/bin/xcaddy /usr/bin/xcaddy
COPY . /src/frankenphp-tiered-cache

WORKDIR /go/src/app

RUN CGO_ENABLED=1 \
    XCADDY_SETCAP=1 \
    XCADDY_GO_BUILD_FLAGS="-ldflags='-w -s' -tags=nobadger,nomysql,nopgx" \
    CGO_CFLAGS="$(php-config --includes)" \
    CGO_LDFLAGS="$(php-config --ldflags) $(php-config --libs)" \
    xcaddy build \
        --output /usr/local/bin/frankenphp \
        --with github.com/dunglas/frankenphp=./ \
        --with github.com/dunglas/frankenphp/caddy=./caddy/ \
        --with github.com/dunglas/caddy-cbrotli \
        --with github.com/b1tc0re/frankenphp-tiered-cache=/src/frankenphp-tiered-cache

FROM dunglas/frankenphp:${FRANKENPHP_VERSION}-php8.4-bookworm

COPY --from=builder /usr/local/bin/frankenphp /usr/local/bin/frankenphp
COPY tests/smoke.php /tmp/frankenphp-tiered-cache-smoke.php

RUN frankenphp php-cli /tmp/frankenphp-tiered-cache-smoke.php \
    && rm /tmp/frankenphp-tiered-cache-smoke.php
