---
title: Наблюдаемость
---

[← Ко всем разделам](index.md)

# Наблюдаемость

## Caddy и Prometheus

Включите встроенный Caddy metrics handler и зарегистрируйте collector расширения
в глобальном блоке Caddyfile:

```text
{
	metrics
	franken_cache_metrics
	admin 0.0.0.0:2019
}
```

Метрики доступны через admin endpoint:

```bash
curl http://127.0.0.1:2019/metrics
```

`franken_cache_metrics` регистрирует collector в текущем Caddy context; сама
опция не создаёт HTTP endpoint. Его включает глобальная опция `metrics`. Так как
endpoint также предоставляет Caddy admin API, ограничьте его доступ сетью.

## Основные метрики

- `franken_cache_lookup_total` и `franken_cache_l1_misses_total` — результаты
  поиска в кеше.
- `franken_cache_l2_operations_total`, `franken_cache_l2_errors_total` и
  `franken_cache_l2_duration_seconds` — операции Redis.
- `franken_cache_l1_entries`, `franken_cache_l1_bytes` и
  `franken_cache_l1_evictions_total` — состояние и pressure eviction L1.
- `franken_cache_degraded`, `franken_cache_recovery_*` и
  `franken_cache_invalidation_ready` — состояние и восстановление экземпляра.
- `franken_cache_invalidation_total{direction,type,result}` — публикация и
  получение invalidation, включая Redis client tracking.
- `franken_cache_invalidation_subscriber_errors_total{stage}` — ошибки
  подписки, получения и применения событий.

Внешний Redis reset увеличивает счётчик
`franken_cache_invalidation_total{direction="received",type="flush",result="success"}`.
Сейчас labels не различают Pub/Sub flush и событие Redis Tracking.

Все значения относятся к одному процессу и снимаются во время Prometheus scrape.
Для работы endpoint нужны обе Caddy options: `metrics` и
`franken_cache_metrics`.
