---
layout: home
title: FrankenPHP Tiered Cache
hero:
  name: FrankenPHP Tiered Cache
  text: Двухуровневый кеш для FrankenPHP
  tagline: Redis хранит общие данные, а локальный MemoryCache ускоряет чтение.
  actions:
    - theme: brand
      text: Настроить подключение
      link: /configuration
    - theme: alt
      text: Посмотреть архитектуру
      link: /architecture
features:
  - title: Общий Redis и быстрый L1
    details: Данные хранятся в Redis, локальная память процесса ускоряет повторное чтение.
    link: /architecture
    linkText: Как устроено
  - title: PHP API и Laravel
    details: Функции расширения доступны напрямую; Laravel подключается отдельным adapter.
    link: /php-api
    linkText: Открыть PHP API
  - title: Запуск и диагностика
    details: Настройте Redis, проверьте invalidation и наблюдайте состояние через Prometheus.
    link: /deployment
    linkText: Руководство по развёртыванию
---

> Проект экспериментальный. API и конфигурация могут меняться без обратной
> совместимости.

Исходный код документации находится в [`docs/src`](https://github.com/b1tc0re/frankenphp-tiered-cache/tree/main/docs/src). Для локального предпросмотра и сборки смотрите раздел [разработки](/development).
