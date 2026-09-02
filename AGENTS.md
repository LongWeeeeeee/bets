# Ingame — Dota 2 Analytics Platform

> **Единый источник правды для всех ИИ-агентов.** Этот файл (`AGENTS.md`) — канонический.
> `CLAUDE.md`, `GEMINI.md`, `.cursorrules` и `.github/copilot-instructions.md` — симлинки на него.
> Правь ТОЛЬКО `AGENTS.md`. Claude Code, Codex, Gemini CLI, Cursor, Copilot читают одно и то же.

Система анализа и прогнозирования Dota 2 матчей: live-данные, букмекерские кэфы, ML-модели и ELO-рейтинги, связка с Telegram-ботом. Автор: alex.

> **Этот файл — горячее ядро (always-loaded).** Здесь только то, что нужно В КАЖДОЙ задаче: маршрутизация OMC, правила, безопасность и индекс знаний. Подробные справочники — в `docs/`, подгружай ПО НЕОБХОДИМОСТИ. Каталог агентов, скиллы и `/team` уже даёт блок oh-my-claudecode — сюда их не копируй.

**Hero ID = OpenDota IDs.** Единый справочник: `base/hero_features_processed.json` (ключ = hero_id). Геттеры в коде: `get_hero_id(name)`, `get_hero_name(id)`, `get_hero_slug(name)`. Те же ID использует `base/dota2protracker.py`.

---

## 📑 Индекс знаний (читай нужный док под задачу)

Перед работой определи область задачи и прочитай ТОЛЬКО релевантные доки (инструментом Read). НЕ сканируй исходники заново, если ответ есть в доке.

| Если задача касается… | Прочитай |
|---|---|
| **любой идеи «а давай улучшим метрику / порог / гейт»** — что уже пробовали, чем мерить, где искать ошибку в замере | **`docs/EXPERIMENTS.md` (читать ДО начала работы)** |
| структуры проекта, «какой файл за что отвечает», где функция | `docs/CODE_MAP.md` |
| логики сигналов, tier/roster/star, ML, потока данных | `docs/ARCHITECTURE.md` |
| правок `cyberscore_try.py`, режимов запуска, env-переменных | `docs/CODE_MAP.md` → раздел `cyberscore_try.py` |
| draft-метрик, dota2protracker, cp1vs1 / synergy / lane | `docs/CODE_MAP.md` → `dota2protracker.py` + `check_old_maps.py` |
| Camoufox / антидетект / парсинг страниц | `docs/CAMOUFOX.md` |
| sleep-политики, расписания опроса, quiet hours | `docs/SCHEDULING.md` |
| полного объяснения операционных правил, деплоя, примеров запуска | `docs/RUNTIME_RULES.md` |
| RuFlo (выключен) — rollback / anti-stall archive | `docs/RUFLO_RUNTIME.md` |
| раскладки файлов на serv1: куда класть скрипт, артефакт, бэкап, что не трогать | `docs/SERVER_LAYOUT.md` |
| legacy opencode Plan→Worker→Review (`base/agent_workflow.py`) | `docs/MULTI_AGENT_WORKFLOW.md` |

> **Правило свежести:** если содержимое дока противоречит реальному коду — **верь коду** и отметь расхождение (док устарел → обнови через subagent `scribe`).

> **Правило журнала:** любой эксперимент над метриками, порогами и гейтами
> записывается в `docs/EXPERIMENTS.md` в ТОМ ЖЕ ходу — с харнессом, командой
> запуска и разделом «где искать ошибку». Запись без них нельзя ни
> воспроизвести, ни опровергнуть, а значит и опираться на неё нельзя.
> Отрицательный результат тоже записывается: он дороже положительного.

---

## 4. Маршрутизация OMC

Оркестр — oh-my-claudecode. Префикс: `oh-my-claudecode:`. Каталог плагина сюда не копируй.
RuFlo выключен (все `ruflo-*` в `.claude/settings.json` = false). Не поднимай его MCP/плагины.
`designer` / `writer` / `qa-tester` запрещены. Консультация, коммит, правка 1–2 строк — lead, не спавн.

| Запрос | Куда |
|---|---|
| найти / посмотреть / где | `explore` |
| план / как делать | `planner` (`architect`, если границы неясны) |
| сделай / почини / правь код | `executor` |
| почему сломалось | `debugger` |
| проверь что готово | `verifier` |
| ревью диффа | `code-reviewer` |
| параллельные независимые куски | `/team`, не стопка one-shot |
| жирный executor, задача кончилась или другая область | handoff → kill → новый, не compact |

`/team`: teammates только внутри одного прогона и той же файловой нитки. Lead сжимается; воркер после чужой задачи — нет.

Handoff (10–20 строк) в `.omc/handoffs/<stage>.md` перед kill:

```markdown
## Handoff: <from> → <to>
- **Decided**:
- **Rejected**:
- **Files**:
- **Remaining**:
```

---

## Раскладка файлов (обязательно)

**Каждый создаваемый файл кладётся сразу в свой каталог — не в корень.** Полная карта — `docs/SERVER_LAYOUT.md`.

| Что создаёшь | Куда |
|---|---|
| модуль прод-пайплайна | `base/` |
| скрипт эксперимента / ресёрча | `runtime/experiments/<тема>/` |
| json/csv/log/pid/отчёт прогона | `runtime/artifacts/<тема>/` |
| каталог тяжёлого завершённого прогона | `runtime/_archive/runs/` |
| бэкап прод-файла (`*.bak_*`, `*.orig`) | `base/_archive/backups/` |
| документацию (.md) | `docs/` |
| раннер/скрипт обслуживания (.sh) | `scripts/run/` или `scripts/ops/` |
| датасет, модель | `data/`, `ml-models/` |

Темы: `odds-winline`, `dltv-protracker`, `cyberscore-prod`, `draft-cp`, `star-dispatch`,
`kills`, `elo`, `pubs-rebuild`, `orchestration`, `misc`.

В корне `runtime/` — только живое состояние (локи, очереди, `*_shadow.jsonl`,
`live_elo_*`, `map_id_check*`, `sourcetv_matches.json`) и импортируемые модули.
В корне репо — `AGENTS.md`, активные конфиги, `requirements*.txt`, живой `log.txt`.

---

## Runtime Rules (безопасность — обязательно к соблюдению)

Краткие инварианты. Деплой, sync и примеры запуска — `docs/RUNTIME_RULES.md`. Тесты: `pytest base/tests/ -v`.

- **Отвечай пользователю на русском.** Код, идентификаторы, commit-сообщения и строки логов — на исходном языке.
- Делай только поставленную задачу; ничего своевольного сверх неё. Неоднозначность / логическая ошибка у пользователя → сначала **спроси**.
- **venv:** ТОЛЬКО `/Users/alex/Documents/ingame/venv_catboost/bin/python3`. Других venv не создавай и не используй.
- **НИКОГДА не удаляй** файлы, папки, бэкапы, `.json` source dicts, sqlite DB, `*.bak_*`, `*.shards/`, логи, кэши — локально или на сервере — **без явного подтверждения**. Сомневаешься — спроси.
- **Rebuild-then-replace, никогда delete-then-create.** Пиши новую версию в `<target>.tmp` и атомарно переименовывай поверх только после успешной сборки и проверки.
- **Коммить каждую правку кода** в том же ходу, отдельным коммитом. На serv1 (`/root/main`) — коммит на месте, без `push`. `git add` — только конкретные файлы, никогда `git add .` / `git add runtime/`. Не коммитить: `base/keys.py`, `*.bak_*`, содержимое `runtime/`. Разовые дампы — в `scratch/`. Машины сводятся только через `origin` (`github.com:LongWeeeeeee/bets`), не rsync.
- **Не запускай / не перезапускай локальный `base/cyberscore_try.py` runtime** без явного запроса. Изменения в live pipeline реализуй и тестируй без локального запуска по умолчанию.
- **`log.txt`:** усекать ТОЛЬКО при bug-fix деплое (git push → pull → restart). На тестах, пробах и логических изменениях — НЕ трогать.
- **`map_id_check.txt`** (`~/.local/state/ingame/`, `MAP_ID_CHECK_PATH`) — единый для всех режимов; при любом перезапуске чисти.
- В `base/keys.py` при чистке мёртвых прокси трогай только runtime proxy constants/pools; **НЕ трогай** `api_to_proxy` / `api_to_keys` и API-ключи. Приватные логи и данные ставок не выноси наружу.
- **Долгие задачи** — только `nohup ... > runtime/<name>.log 2>&1 &` + `echo $!` для PID. Никогда не используй встроенный background-инструмент. Всегда давай кликабельную ссылку на лог и команду проверки статуса.
- **Деплой live pipeline:** git push `main` → на сервере `git pull --ff-only` → `systemctl stop cyberscore` → чисти `map_id_check.txt` → `systemctl start cyberscore`. **Правка логики ставки = рестарт прода + чистка `map_id_check.txt` в том же ходу.** `kill`/`pkill` НЕ использовать: systemd поднимет процесс сам и получится второй экземпляр (`docs/RUNTIME_RULES.md:13`); рестарт — через `scripts/run/restart_cyberscore.sh` (systemd-процедура).
- **Production:** `serv1` = `root@23.26.193.167` (`96300.koara.live`), путь `/root/main`, python **3.12.3** в `/root/main/venv` (локальный venv — 3.9, поэтому перед рестартом гоняй `py_compile` + import-смоук на сервере). Юнит `cyberscore.service`, stdout/stderr — в `base/runtime/cyberscore_sourcetv.log`, НЕ в `log.txt`. `runtime/` — намеренно git-ignored scratch. На serv1 всегда грязные `base/id_to_names.py` (дописывает сам рантайм) и `data/team_org_aliases.json` (ночная пересборка): перед `git pull` сверяй `git diff --name-only` с входящими коммитами. Адрес `147.45.216.225` (внесён 30.04.2026, `e1be48f`) не используется — ноль записей в `known_hosts` против трёх у `212.113.104.102` и трёх у `23.26.193.167`, и ни один скрипт или док его не упоминает.
- **Doc-sync:** публичный контракт (сигнатура, env, CLI, формат I/O, поток сигнала) в том же ходу обнови в `docs/CODE_MAP.md` и/или `docs/ARCHITECTURE.md`. Внутренние правки док не трогают. Источник правды — код.
