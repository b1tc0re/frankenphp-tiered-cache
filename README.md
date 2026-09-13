# FrankenPHP Tiered Cache

Экспериментальное расширение для FrankenPHP: локальный L1-кэш в памяти Go-процесса с Redis в качестве L2.

> [!WARNING]
> Проект находится на ранней стадии разработки и создаётся в первую очередь под собственные задачи автора. API, конфигурация и внутреннее устройство могут меняться без обратной совместимости. Используйте на свой риск и обязательно проверяйте поведение под своей нагрузкой.

## Идея

```text
PHP / FrankenPHP workers
          ↓
   shared in-process L1
          ↓
        Redis L2
```

Планируемые свойства L1: ограничение памяти, TTL и вытеснение по LRU-подобной политике. Redis остаётся общим L2 для нескольких экземпляров FrankenPHP.

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

Redis Pub/Sub является at-most-once транспортом. Если subscriber теряет соединение, pod переводится в `degraded`, его L1 очищается, а после reconnect L1 очищается ещё раз до возобновления healthy-состояния. Поэтому потерянные во время reconnect события не оставляют stale-данные в локальном cache. Ошибка публикации после успешной L2-мутации также переводит pod в `degraded`; recovery повторяет invalidation-событие и удаляет dirty key из L2. Для `Flush` recovery повторяет только Pub/Sub-событие, но не сам `Flush`.

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

На текущем этапе semantics, memory policy и tuning `MemoryCache` считаются стабилизированными для перехода к `RedisCache`. Новых архитектурных изменений в backend не планируется без конкретной причины: bugfix, обнаруженный invariant issue, новый production profile или измеримое требование производительности.

Это stage freeze, а не обещание долгосрочной backward compatibility всего проекта: репозиторий остаётся экспериментальным, и общий extension API ещё может меняться по мере появления `RedisCache` и `TieredCache`.

## Ветки

- `main` — стабильное состояние и релизы.
- `develop` — активная разработка.

## Сборка

Для локальной разработки нужны Docker и [Task](https://taskfile.dev/).

Собрать FrankenPHP с расширением:

```bash
task build
```

Собрать image и проверить загрузку расширения:

```bash
task smoke
```

Успешная проверка выводит:

```text
franken_tiered smoke test passed (0.0.0-dev).
```

Dockerfile собирает FrankenPHP через `xcaddy` и подключает этот модуль непосредственно в бинарник. Smoke-test запускается отдельно после сборки, поэтому его результат не нужно искать в Docker build log.

### Redis integration test

Для проверки cross-pod invalidation нужен доступный Redis. Удобный локальный сценарий:

```bash
task test:integration
task redis:down
```

`task test:integration` поднимает зафиксированный в `compose.yaml` Redis `7-alpine`, запускает реальный тест с двумя независимыми L1 и двумя Redis L2/Pub/Sub connections, затем оставляет сервис запущенным для повторных прогонов. `task redis:down` останавливает и удаляет контейнер Compose.

Тот же тест можно запускать без Docker, если Redis уже доступен:

```bash
REDIS_ADDR=127.0.0.1:6379 go test -count=1 -run '^TestTieredCacheRedisPubSubIntegration$' ./internal/cache/tiered
```

Обычный `task test` не требует Redis: интеграционный тест пропускается, если `REDIS_ADDR` не задан.

Интеграция с Laravel будет разрабатываться отдельно и не является частью этого репозитория.

## Лицензия

MIT.
