# FrankenPHP Tiered Cache

Экспериментальное расширение для FrankenPHP с `TieredCache`: локальный L1-кэш
в памяти Go-процесса и Redis L2 как authoritative storage.

> [!WARNING]
> Проект находится на ранней стадии разработки и создаётся в первую очередь под собственные задачи автора. API, конфигурация и внутреннее устройство могут меняться без обратной совместимости. Используйте на свой риск и обязательно проверяйте поведение под своей нагрузкой.

## Архитектура

```text
PHP / FrankenPHP workers
          ↓
      TieredCache
       ┌────┴────┐
       ↓         ↓
  Memory L1   Redis L2
                │
         Pub/Sub invalidation
```

`MemoryCache` — только локальное ускорение чтения. Redis — единственный
источник истины для данных TieredCache и общий storage для нескольких
экземпляров FrankenPHP. Pub/Sub сообщает другим экземплярам только о том, что
локальная копия ключа больше недостоверна.

### Семантика TieredCache

- `Get` сначала читает L1. При miss значение синхронно читается из Redis и
  прогревает L1 с оставшимся TTL.
- `Set` и `Forever` сначала синхронно изменяют Redis, затем обновляют локальный
  L1 и публикуют invalidation для других pod'ов.
- `Forget`, `Touch` и `Flush` синхронно выполняются в Redis и L1. После успешной
  Redis-операции публикуется соответствующее событие.
- Асинхронной очереди записи, writer goroutine, fencing и dirty-key recovery
  больше нет.
- `Increment` и `Decrement` выполняются атомарно в Redis; после операции ключ
  удаляется из локального L1 и публикуется invalidation.

После успешной Redis mutation значение считается committed. Ошибка последующего
обновления L1 или публикации Pub/Sub не откатывает Redis: TieredCache сохраняет
pending invalidation и пытается доставить её во время recovery.

### Prometheus metrics

Метрики подключаются отдельным Caddy global option. Добавьте его в глобальный
блок Caddyfile:

```caddyfile
{
	franken_cache_metrics
	admin 0.0.0.0:2019
}
```

После этого collector регистрируется в metrics registry текущего Caddy context,
а стандартный Caddy admin endpoint отдаёт его по `/metrics`:

```bash
curl http://127.0.0.1:2019/metrics
```

Если admin API отключён или слушает на другом адресе/порту, используйте ваш
фактический admin endpoint. `franken_cache_metrics` не поднимает отдельный HTTP
сервер и не использует глобальный Prometheus registerer. Поэтому при Caddy
reload новый collector регистрируется в новом context без повторной регистрации
в старом registry.

Основные группы метрик:

- lookup L1/L2 и cache misses;
- операции, ошибки и latency Redis L2, включая batch operations;
- entries, bytes, limits и pressure evictions L1;
- degraded/recovery и состояние Pub/Sub invalidation;
- post-commit и ошибки обновления L1.

Метрики собираются из атомарного process-local состояния. Prometheus objects не
создаются на пути `Get`/`Set`; значения формируются только во время scrape.

### TTL и expiration

TTL задаётся как относительное время жизни записи. Значения `ttl <= 0` не используются как специальное значение для вечного хранения: для этого есть отдельная операция `Forever`.

Runtime deadline хранится как `time.Time`, полученный через `time.Now().Add(ttl)`. В production такой `time.Time` содержит monotonic-компонент, который сохраняется при `Add` и используется методами сравнения `time.Time`. Поэтому изменение wall clock системой или NTP не должно продлевать или преждевременно завершать локальный TTL. Для `Forever` используется нулевой `time.Time{}`.

Monotonic-компонент `time.Time` существует только внутри текущего Go-процесса и не предназначен для сериализации или сравнения между pod'ами. `TieredCache` передаёт Redis оставшийся TTL, а не сохраняет внутренний `expiresAt` из `MemoryCache`.

Expired запись считается отсутствующей сразу после достижения `expiresAt`, даже если физически она ещё находится в map. `Get` возвращает miss, а `Forget` и `Touch` возвращают `false`. При обращении такая запись удаляется и её учтённый размер освобождается.

Перед eviction при нехватке памяти MemoryCache также сначала удаляет expired записи.

Background maintenance постепенно и opportunistically освобождает память от expired записей, к которым приложение больше не обращается. Одна maintenance goroutine просыпается раз в секунду и последовательно обрабатывает до 16 shards по round-robin. В каждом выбранном shard просматривается не более 64 записей под `RLock`, поэтому параллельные `Get` продолжают выполняться. При стандартных 64 shards каждый shard посещается раз в четыре секунды. Найденные expired candidates затем удаляются короткими `Lock` с повторной проверкой, что запись не была заменена конкурентным `Set`. Bounded sampling не обещает строгий последовательный обход каждой записи: его задача — дешёвая фоновая уборка без дополнительного bookkeeping в hot path. Полная очистка expired записей перед eviction остаётся отдельным slow path и может пройти все shards, когда память нужно освободить немедленно.

Background maintenance не использует busy loop и не создаёт отдельные goroutine или ticker для каждого shard. Cleanup не добавляет per-request bookkeeping в `Get` или `Set`.

`lastAccess` используется только для локального approximate-LRU eviction. Он хранит значение локального coarse logical clock конкретного `MemoryCache`, а не wall-clock timestamp и не глобальный порядковый номер каждого `Get`. Clock увеличивается background maintenance loop раз в секунду. Успешное создание/замена записи, успешный `Get` и успешный `Touch` записывают текущее значение clock в `lastAccess`; повторные обращения в рамках того же tick не требуют увеличения глобального счётчика. Обновление `lastAccess` не может откатиться назад при конкурентных обращениях.

`MemoryCache` запускает одну maintenance goroutine. Она обслуживает coarse LRU clock и bounded cleanup expired записей. `Close()` останавливает background maintenance, после чего экземпляр кэша больше не должен использоваться.

Значение `lastAccess` не является временем, версией данных или распределённым идентификатором и не должно сравниваться между процессами или pod'ами.

### Cross-pod invalidation

Для нескольких pod'ов `TieredCache` можно создать с Redis Pub/Sub bus:

```go
redisCache, err := redis.New(redisConfig)
if err != nil {
	return err
}

invalidationBus, err := redis.NewInvalidationBus(redisConfig)
if err != nil {
	return err
}

tieredCache, err := tiered.NewWithInvalidation(
	tieredConfig,
	memoryCache,
	redisCache,
	invalidationBus,
)
```

`Set`, `Forever`, `Forget` и `Touch` публикуют key-invalidation после успешной L2-операции. `Flush` публикует отдельное событие после успешной очистки L2. Полученное событие удаляет только соответствующий ключ или весь локальный L1; оно никогда не вызывает операцию TieredCache и не приводит к циклу публикаций.

Redis Pub/Sub является at-most-once транспортом. Если subscriber теряет соединение, pod переводится в `degraded`, его L1 очищается, а после reconnect L1 очищается ещё раз до возобновления healthy-состояния. Поэтому потерянные во время reconnect события не оставляют stale-данные в локальном cache. Ошибка публикации после успешной L2-мутации также переводит pod в `degraded`; recovery повторяет только недоставленное invalidation-событие, не изменяя committed Redis value. Для `Flush` recovery повторяет только Pub/Sub-событие, но не сам `Flush`.

Связка L2-мутации и `PUBLISH` состоит из двух Redis-команд. Между успешной L2-мутацией и публикацией остаётся небольшой crash gap: если процесс завершится в этот момент, уже работающий другой pod может некоторое время держать старое значение в L1. Для строгой гарантии без этого gap потребуется отдельный атомарный Redis Lua-протокол или durable outbox/Streams; текущий Pub/Sub слой сохраняет быстрый hot path и корректно восстанавливается при обычных ошибках соединения.

### Memory limits and pressure diagnostics

`MemoryCache` использует консервативные process-local defaults: `DefaultMaxMemoryBytes = 64 MiB` и `DefaultMaxItemSizeBytes = 4 MiB`. Это policy defaults, а не значения, подобранные benchmark'ом.

Нормализация `Config` работает так:

```text
MaxMemoryBytes = 0
→ 64 MiB

MaxItemSizeBytes = 0
→ min(4 MiB, MaxMemoryBytes)

любое отрицательное значение
→ configuration error

явно заданный MaxItemSizeBytes > MaxMemoryBytes
→ configuration error
```

Лимит `MaxMemoryBytes` относится к памяти, учитываемой самим cache (`len(key) + cap(value)` для каждой записи), а не ко всему Go heap или RSS процесса. Если пользователь меняет общий memory budget, default item limit остаётся до `4 MiB` и clamp'ится только когда сам `MaxMemoryBytes` меньше `4 MiB`; фиксированная доля от общего budget намеренно не навязывается.

Достижение memory limit само по себе не является ошибкой: перед записью `MemoryCache` может удалить expired entries, а затем при необходимости вытеснить live entry через approximate-LRU. Удаление live entry из-за memory pressure считается диагностическим событием и передаётся через optional `Observer` как `EvictionEvent`. TTL cleanup, `Forget()` и `Flush()` такого события не создают.

`Observer` не занимается логированием и не зависит от FrankenPHP. Его задача — сообщить верхнему integration layer факт pressure eviction. Callback вызывается после освобождения shard lock; реализация observer должна быть concurrency-safe и возвращаться быстро. Уже integration layer может превратить событие, например, в warning FrankenPHP о том, что cache начал вытеснять полезные записи и, возможно, требуется увеличить memory limit.

### MemoryCache tuning

Часть внутренних параметров `MemoryCache` выбрана после отдельных benchmark-серий и intentionally не вынесена в публичный `Config`. Это implementation defaults: если реальные production-профили покажут другую картину, их можно менять внутри backend без расширения пользовательского API.

| Параметр | Значение | Почему выбрано |
| --- | ---: | --- |
| `defaultShardCount` | `64` | `128` быстрее на distinct-key workload, но дальнейший выигрыш уже с diminishing returns и сопровождается дополнительным overhead. |
| `defaultLRUSamples` | `5` | `1` заметно хуже удерживает hot entries, `3` уже близок к достаточному качеству, а `8` и `16` не дали практически дополнительного retention, но сделали eviction дороже. |
| `evictionTargetPct` | `95` | `80` и `90` сильнее недозаполняют cache после pressure eviction, а `99` почти не оставляет свободного запаса и примерно вдвое удорожает pressure path. |
| `backgroundCleanupShardsPerTick` | `16` | Для стандартных `64` shards это полный round примерно за четыре секунды при стоимости порядка `23 µs` на maintenance tick. |

`lruClockResolution = 1s` и `maxEvictionRetries = 3` рассматриваются отдельно от performance tuning: первое является частью выбранной coarse-LRU модели, второе — bounded safety limit для pressure eviction.

### MemoryCache status

На текущем этапе semantics, memory policy и tuning `MemoryCache` считаются
стабилизированными как L1-backend для `TieredCache`. Новых архитектурных
изменений в backend не планируется без конкретной причины: bugfix,
обнаруженный invariant issue, новый production profile или измеримое требование
производительности.

Это stage freeze, а не обещание долгосрочной backward compatibility всего
проекта: репозиторий остаётся экспериментальным, и общий extension API ещё
может меняться.

## Ветки

- `main` — стабильное состояние и релизы.
- `develop` — активная разработка.

Рабочие изменения находятся в `develop`. В `main` изменения попадают только
через Pull Request/Merge Request; это стабильная ветка, от которой создаются
релизы.

### Проверки и выпуск версии

GitHub Actions разделён на небольшие независимые workflows. Их имена совпадают
с локальными Task-задачами:

| Проверка | Локальная команда |
| --- | --- |
| `fmt:check` | `task fmt:check` |
| `mod:check` | `task mod:check` |
| `version:check` | `task version:check` |
| `lint:check` | `task lint:check` |
| `vuln:check` | `task vuln:check` |
| `vet:check` | `task vet:check` |
| `race` | `task test` |
| `integration` | `task test:integration` |
| Все проверки кроме benchmark | `task ci` |

Локальные версии `golangci-lint` и `govulncheck` фиксируются в `Taskfile.yml` и
устанавливаются в `.tools/bin`. `task ci` автоматически
устанавливает отсутствующие инструменты; системный `PATH` настраивать не нужно.
Отдельно установить их можно командой `task tools:install`. В GitHub Actions
инструменты устанавливаются соответствующими официальными actions. Каждый
workflow запускается отдельно для Pull Request/Merge Request и push в `develop`
или `main`, поэтому обязательные проверки можно настраивать в правилах веток
независимо друг от друга.

Запустить весь локальный CI одним последовательным сценарием без benchmark:

```bash
task ci
```

Команда останавливается на первой ошибке. Для `test:integration` нужен Docker;
временный Redis-контейнер автоматически удаляется после завершения проверки.

Единая версия хранится в `version.json`. Release Please обновляет это поле и
changelog в release Pull Request. После его merge создаются Git tag и GitHub
Release. Go-модуль получает свою версию из Git tag `vX.Y.Z`, а Go-код встраивает
то же значение из `version.json` в PHP module entry. Поэтому
`phpversion('franken_cache')` и версия Go-модуля используют один номер.

Релизный цикл:

1. Изменения из `develop` попадают в `main` через Pull Request/Merge Request.
2. После push в `main` workflow `Release Please` создаёт или обновляет release
   Pull Request.
3. После merge release Pull Request Release Please создаёт tag `vX.Y.Z`,
   GitHub Release и обновляет manifest/`version.json` в рамках release commit.

Тип релиза определяется Conventional Commits (`feat`, `fix`, `perf`, `breaking
change` и т.д.). Ручное редактирование версии и отдельный ручной release
workflow не требуются.

## Сборка

Для локальной разработки нужны Docker и [Task](https://taskfile.dev/).

Собрать FrankenPHP с расширением:

```bash
task build
```

Собрать image и выполнить Go Redis/PubSub и PHP FrankenPHP integration-тесты
через один локальный Redis:

```bash
task test:integration
```

`task test:integration` автоматически поднимает Redis из корневого
`compose.yaml`, поэтому отдельно задавать `FRANKEN_CACHE_REDIS_ADDR` для этого
сценария не нужно.
Успешная проверка выводит две строки:

```text
franken_cache smoke test passed (0.0.1).
franken_cache TieredCache PHP bridge smoke test passed.
```

Dockerfile собирает FrankenPHP через `xcaddy` и подключает этот модуль
непосредственно в бинарник. PHP-тесты запускаются после сборки image, поэтому их
результат не нужно искать в Docker build log.

### PHP extension API и конфигурация

Расширение экспортирует функции:

```text
franken_cache_tiered_get
franken_cache_tiered_many
franken_cache_tiered_add
franken_cache_tiered_put_many
franken_cache_tiered_set
franken_cache_tiered_forever
franken_cache_tiered_forget
franken_cache_tiered_touch
franken_cache_tiered_flush
franken_cache_tiered_increment
franken_cache_tiered_decrement
```

Redis и recovery настраиваются через environment variables:

`franken_cache_tiered_many` принимает список ключей и возвращает associative
array с raw payload для найденных значений и `false` для cache miss. Сначала
проверяется локальный Memory L1, а все отсутствующие ключи читаются из Redis
одним batch-запросом; пустой список возвращает `[]` без обращения к Redis.

| Переменная | Назначение | Default |
| --- | --- | --- |
| `FRANKEN_CACHE_REDIS_ADDR` | адрес Redis | `127.0.0.1:6379` |
| `FRANKEN_CACHE_REDIS_USERNAME` | Redis username | пусто |
| `FRANKEN_CACHE_REDIS_PASSWORD` | Redis password | пусто |
| `FRANKEN_CACHE_REDIS_DB` | номер Redis DB | `0` |
| `FRANKEN_CACHE_REDIS_PREFIX` | prefix ключей Redis | `franken_cache:` |
| `FRANKEN_CACHE_REDIS_DIAL_TIMEOUT` | timeout подключения | backend default |
| `FRANKEN_CACHE_REDIS_READ_TIMEOUT` | timeout чтения | backend default |
| `FRANKEN_CACHE_REDIS_WRITE_TIMEOUT` | timeout записи | backend default |
| `FRANKEN_CACHE_RECOVERY_INTERVAL` | пауза между повторными попытками recovery и Pub/Sub reconnect в `degraded` | `5s` |

Значения timeout и recovery interval задаются в формате Go duration, например
`500ms` или `5s`.

`FRANKEN_CACHE_RECOVERY_INTERVAL` не является периодом обработки обычных
запросов и не запускает Redis-проверки в healthy-состоянии. Он используется
только после ошибки, когда экземпляр перешёл в `degraded`:

```text
degraded
  ↓ ждём RecoveryInterval
проверяем Redis
повторяем недоставленные Pub/Sub invalidation events
очищаем L1 перед возвратом в healthy
```

Если Redis или Pub/Sub всё ещё недоступны, следующая попытка выполняется после
ещё одного такого интервала. При восстановлении Pub/Sub subscriber также ждёт
этот интервал перед повторным подключением. Поэтому значение `5s` означает,
что retry обычно начинается не позже чем через 5 секунд после предыдущей
неудачной попытки, плюс время самой проверки или подключения.

Меньшее значение ускоряет recovery, но чаще создаёт Redis probes и попытки
reconnect. Большее значение уменьшает эту нагрузку, но дольше оставляет pod в
`degraded`.

### Production Docker image

В production расширение подключается во время сборки FrankenPHP через
`xcaddy`. В вашем существующем multi-stage Dockerfile достаточно добавить
модуль в тот же `xcaddy build`:

```dockerfile
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

`FRANKEN_CACHE_VERSION` должен быть закреплён на release tag или commit, а не
на плавающей ветке. Если исходники этого репозитория уже входят в Docker build
context, вместо versioned module можно использовать локальный путь:

```dockerfile
COPY frankenphp-tiered-cache /src/frankenphp-tiered-cache
...
--with github.com/b1tc0re/frankenphp-tiered-cache=/src/frankenphp-tiered-cache
```

При запуске контейнера передайте Redis-конфигурацию как runtime environment,
например в Compose или Kubernetes:

```yaml
environment:
  FRANKEN_CACHE_REDIS_ADDR: redis.internal:6379
  FRANKEN_CACHE_REDIS_DB: "1"
  FRANKEN_CACHE_REDIS_USERNAME: cache-user
  FRANKEN_CACHE_REDIS_PASSWORD: ${REDIS_PASSWORD}
  FRANKEN_CACHE_REDIS_PREFIX: "franken_cache:"
  FRANKEN_CACHE_RECOVERY_INTERVAL: 5s
```

Пересборка image при изменении адреса, DB, prefix или credentials не нужна:
достаточно перезапустить контейнер с новым environment. `ARG` для версии
FrankenPHP/PHP и модуля относится к build-time, а `FRANKEN_CACHE_*` — только к
runtime.

После сборки расширение само по себе не меняет Laravel `CACHE_STORE`. PHP
adapter должен вызывать функции `franken_cache_tiered_*`, а приложение должно
выбрать этот adapter как свой cache store. PHP `redis` extension может
оставаться установленным для других задач, но TieredCache подключается к Redis
самостоятельно из Go.

Текущий Dockerfile этого репозитория проверен с FrankenPHP `1.12.7` и PHP
`8.4`. Для другой пары версий, например FrankenPHP `1.11`, сначала нужно
отдельно проверить сборку и smoke-тест: совместимость не следует считать
автоматически подтверждённой.

### Redis integration test

Для проверки cross-pod invalidation нужен доступный Redis. Удобный локальный сценарий:

```bash
task test:integration
```

`task test:integration` поднимает зафиксированный в `compose.yaml` Redis
`7-alpine`, запускает Go-тест с двумя независимыми L1 и двумя Redis L2/Pub/Sub
connections, затем собирает FrankenPHP image и запускает PHP bridge smoke-тесты.
После этого запускается Caddy smoke-тест Prometheus `/metrics` с включённым
`franken_cache_metrics`. Все проверки используют один Redis-контейнер. После
завершения или ошибки Redis, Compose-сеть и volume автоматически удаляются.
Локальный Docker image сохраняется для повторного запуска и удаляется отдельной
командой `task clean`.

Тот же тест можно запускать без Docker, если Redis уже доступен:

```bash
REDIS_ADDR=127.0.0.1:6379 go test -count=1 -run '^TestTieredCacheRedisPubSubIntegration$' ./internal/cache/tiered
```

Обычный `task test` не требует Redis: интеграционный тест пропускается, если `REDIS_ADDR` не задан.

## Laravel benchmark stand

В репозитории есть отдельный Docker-стенд для сравнения пяти cache-сценариев:

```text
plain Laravel        — тот же production-like DTO без cache
Laravel + Array      — встроенный Laravel ArrayStore
Laravel + PhpRedis   — стандартный Laravel RedisStore через PhpRedis
Laravel + Predis     — стандартный Laravel RedisStore через Predis
Laravel + TieredCache — текущий Go MemoryCache + RedisCache + Pub/Sub
```

Стенд находится в [`bench/laravel`](bench/laravel). Он запускает отдельный
контейнер для каждого сравниваемого backend'а (`plain`, `array`, `phpredis`,
`predis`, `tiered`) с общим Redis. Дополнительно можно поднять второй
`tiered`-контейнер для проверки cross-pod invalidation. RoadRunner в этот стенд
не входит. Laravel оставляет `serialize()/unserialize()` на PHP-стороне во всех
cache-сценариях, чтобы измерять реальный путь приложения.

Для быстрого локального baseline:

```bash
task bench:laravel:local-up
task bench:laravel:local
task bench:laravel:two-pod
task bench:laravel:local-down
```

Локальный Redis ограничен Compose-параметрами `REDIS_CPUS`, `REDIS_MEMORY` и
`REDIS_MAXMEMORY`. Это воспроизводимый baseline, но не замена измерению сетевой
задержки до удалённого Redis.

Для Redis на другой машине задайте `BENCH_REDIS_HOST` и при необходимости
`BENCH_REDIS_PORT`,
а затем выполните:

```bash
cp bench/laravel/.env.example bench/laravel/.env
# заполните bench/laravel/.env и задайте BENCH_REDIS_HOST, BENCH_REDIS_PORT и креды

make -C bench/laravel remote-up
make -C bench/laravel remote-bench
make -C bench/laravel remote-two-pod
make -C bench/laravel remote-down
```

Используйте отдельную Redis DB или namespace через `BENCH_REDIS_DB`,
`BENCH_KEY_PREFIX` и `BENCH_TIERED_REDIS_PREFIX`; стенд не выполняет `FLUSHDB`.
Параметры нагрузки: `DURATION`, `THREADS`, `CONNECTIONS`, `SIZE`, `MODE` и
`PROFILE`. Для production-подобных трасс можно запускать `PROFILE=warm` или
`PROFILE=cold`; каждый benchmark-запрос воспроизводит целую последовательность
cache-операций одного запроса из одного pod'а.
Подробности находятся в [`bench/laravel/README.md`](bench/laravel/README.md).

## Лицензия

MIT.
