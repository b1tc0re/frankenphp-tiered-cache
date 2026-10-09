---
title: Разработка
---

[← Ко всем разделам](index.md)

# Разработка

Для локальной сборки нужны Docker и [Task](https://taskfile.dev/).

```bash
task build
task test:integration
```

`task build` собирает FrankenPHP image с расширением. Интеграционная задача
поднимает временный Redis, запускает Go cross-pod тесты, PHP bridge smoke tests и
проверку Caddy `/metrics`, затем удаляет Compose-контейнеры и volume.

## Проверки

Запустить полный CI без benchmark:

```bash
task ci
```

`task ci` последовательно выполняет проверки форматирования, модулей, версии,
lint, vet, уязвимостей, race-тесты и интеграционный сценарий. `golangci-lint` и
`govulncheck` закреплены в `Taskfile.yml`; отсутствующие инструменты можно
установить командой `task tools:install`.

Отдельные задачи: `task fmt:check`, `task mod:check`, `task version:check`,
`task lint:check`, `task vuln:check`, `task vet:check`, `task test` и
`task test:integration`.

## Документация

Сайт документации собран на VitePress. Для локального предпросмотра установите
[Bun](https://bun.sh/), затем выполните:

```bash
cd docs
bun install --frozen-lockfile
bun run dev
```

Production-сборка выполняется командой `bun run build`; результат появится в
`docs/.vitepress/dist`. GitHub Actions публикует эту сборку в GitHub Pages после
создания релиза.

## Ветки и релиз

- `develop` — активная разработка.
- `main` — стабильная ветка и источник релизов.

Изменения попадают в `main` через Pull Request. Release Please создаёт или
обновляет release PR; после его merge создаются Git tag и GitHub Release.
Публикация статической документации запускается из release workflow по этому
тегу. Ручной запуск доступен в GitHub Actions через workflow `Documentation`.

Единая версия хранится в `version.json` и `.release-please-manifest.json`.
Conventional Commit types (`feat`, `fix`, `perf` и др.) определяют тип изменения
версии.
