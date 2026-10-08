---
title: PHP API
---

[← Ко всем разделам](index.md)

# PHP API

Расширение экспортирует следующие функции:

| Функция | Сигнатура |
| --- | --- |
| `franken_cache_tiered_get` | `(string $key): string|false` |
| `franken_cache_tiered_many` | `(array $keys): array` |
| `franken_cache_tiered_add` | `(string $key, string $value, int $ttl): bool` |
| `franken_cache_tiered_put_many` | `(array $values, int $ttl): bool` |
| `franken_cache_tiered_set` | `(string $key, string $value, int $ttl): bool` |
| `franken_cache_tiered_forever` | `(string $key, string $value): bool` |
| `franken_cache_tiered_forget` | `(string $key): bool` |
| `franken_cache_tiered_touch` | `(string $key, int $ttl): bool` |
| `franken_cache_tiered_flush` | `(): bool` |
| `franken_cache_tiered_increment` | `(string $key, int $value): int` |
| `franken_cache_tiered_decrement` | `(string $key, int $value): int` |

`get` возвращает `false` при промахе. `many` возвращает массив с найденными
значениями и `false` для отсутствующих ключей; пустой список возвращает `[]` без
запроса к Redis. Значения передаются как строки и могут содержать бинарные данные.

`add` записывает значение только если ключ отсутствует. `put_many` записывает
пары `ключ => значение` с общим TTL. TTL для `add`, `put_many`, `set` и `touch`
задаётся в секундах; для вечного хранения используется `forever`.

Функции расширения не меняют Laravel `CACHE_STORE`. Для использования через
Laravel Cache необходимо установить и выбрать отдельный
[Laravel adapter](deployment.md#laravel-adapter).
