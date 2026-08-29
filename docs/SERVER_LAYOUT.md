# Раскладка файлов на serv1 (`root@23.26.193.167`)

> Наведено 2026-08-04. Манифесты всех перемещений: `/root/archive/_moves/20260804_phase*.tsv`
> (формат `старый_путь<TAB>новый_путь`) — откат любой фазы делается обратным `mv` по колонкам.

## `/root` — по проектам

| Путь | Что это |
|---|---|
| `main/` | ingame — live-пайплайн Dota 2 (см. ниже) |
| `diary_bot/` | Telegram-дневник (`diary-bot.service`) |
| `camoufox/` | сборка антидетект-браузера (runtime-профиль — в `~/.cache/camoufox`) |
| `the-ai-counsel/`, `speech-awareness/`, `research/`, `android-sdk/` | сторонние/исследовательские проекты |
| `scratch/<проект>-<тема>/` | одноразовые патч-скрипты, пробы, логи ручных прогонов |
| `archive/<проект>/` | завершённые копии, evidence-папки, снятые с эксплуатации деревья |
| `archive/_moves/` | манифесты уборки |

Конфиги сервисов вне `/root`: Xray — `/usr/local/etc/xray/config.json`, MTProxy telemt —
`/etc/telemt/telemt.toml` + `/var/lib/telemt`, TorrServer — `/var/lib/torrserver`.

**Правило:** временный скрипт/лог руками — в `~/scratch/<тема>/`, а не в `~`.

## `/root/main` (ingame)

```
base/                   прод-код (layout НЕ меняем — монолит cyberscore_try.py импортирует по имени)
  _archive/backups/     *.bak_*, *.orig, *.baseline_* (в git не коммитятся)
  _archive/research-data/  дампы исследований, которые код не читает
  runtime/              ЖИВОЕ состояние прод-процесса: cyberscore_sourcetv.log, локи, очереди
data/  ml_dataset/  ml-models/  bets_data/  pro_heroes_data/  output/   датасеты и модели
docs/                   вся документация (.md); docs/_archive/ — устаревшие копии
scripts/run/            раннеры (run_dltv*.sh, restart_*.sh)
scripts/ops/            обслуживание (opencode-switch.sh, apply-autodocs.sh, setup-protection.sh)
scripts/legacy/         разовые утилиты анализа
_archive/               разовое из корня репо: configs/, chats/, vendor/
services/               ВЕРСИОНИРУЕМЫЕ демоны и их юниты (код, не состояние):
  winline/              winline_current_map_odds_poller.py, winline_parser_monitor.py,
                        winline_shadow_activation.py, winline_shadow_event_watchdog.py,
                        winline_shadow_probe.py — namespace-пакет `services.winline`
  anti_stall_supervisor/  пакет супервизора + contracts/policy JSON + deploy/ (.service, .timer)
  worker676-gateway/    gateway.py + тесты; деплой в /opt/worker676-gateway/
  systemd/              каноничные копии юнитов: ruflo-orchestrator@, ruflo-universal-gateway,
                        openhands-universal-gateway, worker676-gateway
  tests/                тесты сервисного слоя (kanban plan lint, pwr controller)
runtime/                ЖИВОЕ состояние и эксперименты — и больше НИЧЕГО. Версионируемого кода
                        здесь нет: очереди сигналов, локи, shadow/telegram jsonl, state демонов,
                        omniroute.log. Каталог целиком git-ignored (см. `.gitignore`).
  experiments/<тема>/   .py/.sh экспериментов
  artifacts/<тема>/     их результаты: json/csv/log/md/pid
  _archive/runs/        завершённые каталоги прогонов (git-ignored, ~29 ГБ)
  _archive/2026-04_legacy_runs/  старые .out/.log апрельских запусков
```

Темы (одни и те же в `experiments/` и `artifacts/`): `odds-winline`, `dltv-protracker`,
`cyberscore-prod`, `draft-cp`, `star-dispatch`, `kills`, `elo`, `pubs-rebuild`,
`orchestration`, `misc`.

**Куда класть новое**
- скрипт эксперимента → `runtime/experiments/<тема>/`;
- его json/csv/log/pid → `runtime/artifacts/<тема>/` (одно имя-префикс на прогон);
- отчёт по эксперименту (.md) → рядом с артефактами; в `docs/` — только сквозная документация;
- каталог тяжёлого прогона после разбора → `runtime/_archive/runs/`;
- бэкап прод-файла → `base/_archive/backups/` (в корне `base/` бэкапам не место);
- долгоживущий демон, его юнит или тест к нему → `services/<сервис>/`, а НЕ в `runtime/`:
  всё, что должно пережить уборку и приехать через git, живёт только там.

**Не трогать при уборке**
- `base/runtime/*` и `runtime/*.lock`, `runtime/delayed_signal_queue*.json`,
  `runtime/*_shadow.jsonl`, `runtime/*_telegram_sent.jsonl`, `runtime/live_elo_*`,
  `runtime/sourcetv_matches.json`, state-файлы демонов kanban/hermes;
- `services/anti_stall_supervisor/`, `services/worker676-gateway/`, `services/systemd/`,
  `runtime/ruflo*/` — на них ссылаются systemd-юниты;
- `runtime/_wl_stage3/` — хардлинк-копия дерева, в ней тот же inode, что у живого прод-лога;
- `base/keys.py` и `base/keys.py.bak_*` — файлы с ключами, пути не двигаем.

`winline_parser_monitor.*` из этого списка убран. Проверка на serv1 (2026-08-26): systemd-юнита
нет, строки в crontab нет, ссылок из `scripts/` нет, процессы не запущены — прежнее утверждение
«на него ссылается systemd-юнит» было неверным. Скрипт переехал в `services/winline/` как обычный
версионируемый код; живого состояния за ним не числится.

Перед переносом чего-либо в `runtime/`: `lsof -n +D /root/main/runtime`, проверка pid'ов
(`kill -0`) и `grep` имени файла по `base/`, `ELO/`, `services/`, `scripts/`,
`/etc/systemd/system/` и `crontab -l`.

## Артефакты моделей не ездят через git — их возят руками

`.gitignore` глушит `*.joblib` (строка 177) и `*.cbm` (строка 176). Значит **любая
модель попадает на serv1 только копированием**, а `git pull` привозит один паспорт:
`manifest.json`, `panel.json`, `results.json`. Расхождение молчит: код грузит модель,
не находит её и уходит в отказ, а карточка просто теряет строку.

**Инцидент 29.08.2026.** Панель семи предматчевых моделей (`w_5_15`, `w_10_20`,
`w_15_25`, `w_20_30`, `dur43`, `total_55_50`, `rad_30_25`) стояла в проде мёртвой:
на serv1 лежали `manifest.json` + `panel.json` + `feature_names.json`, а **ни одного
`.cbm` не было вообще** (`find /root/main -name "*.cbm"` пуст). В логе шло
`[win_model] панель молчит: bundle=НЕТ, ошибка загрузчика='артефакт панели не готов'`
и шло давно — ранние записи того же лога показывают `bundle=есть, ready=True`, то есть
модели когда-то были и пропали. Каталог и json датированы 26.08 — пересборка обновила
паспорт и не привезла модели. Вылечено копированием 13 МБ с Mac; `panel.json` совпал
по md5, fingerprint `54dee6998569` — то есть калибровка на serv1 всё это время ждала
ровно те файлы.

**После КАЖДОГО деплоя, который трогает модели, проверять наличие, а не только код:**

```bash
ssh serv1 'find /root/main/ml-models /root/main/data -name "*.cbm" -o -name "*.joblib" | wc -l'
ssh serv1 'grep -a "панель молчит\|не готов\|ошибка загрузчика" /root/main/base/runtime/cyberscore_sourcetv.log | tail -3'
```

Каталоги, где модели обязаны лежать: `ml-models/prematch_panel/` (7 `.cbm`),
`data/public_draft_hero10_experiment/<версия>/` и `data/late_draft_win/<версия>/`
(по два `.joblib`). Паспорт без модели — это отказ, а не работа.

## Бэкапы

**Код, конфиги репо, доки, тесты бэкапить файлами не нужно** — они в git и в `origin`, обе машины
сводятся через него. Снимки вида `*.bak_20260730_1905` рядом с прод-файлом ничего не добавляют:
проверка 2026-08-04 показала, что 47 из 129 таких файлов байт-в-байт совпадали с объектами,
достижимыми из коммитов, а остальные были промежуточными состояниями одного вечера правок.
Гард в `scripts/ops/hooks/pre-commit` не даёт складывать новые бэкапы в `base/`.

**Бэкапить нужно то, чего в git нет:**

| Данные | Размер | Чем закрыто |
|---|---|---|
| `bets_data/analise_pub_matches/*.sqlite3` — словари паблика | ~23 ГБ | `scripts/ops/backup-heavy.sh` (с Mac) |
| `pro_heroes_data/`, `data/`, `ml_dataset/`, `base/ml_dataset/`, `output/`, `ELO/output/` | ~800 МБ | там же |
| `base/keys.py`, `/root/.config/dota_probe/*`, юниты systemd, `xray/config.json`, `telemt.toml` | КБ | **не закрыто** — сделать отдельно |
| живое состояние `runtime/` (очереди, `*_telegram_sent.jsonl`, elo state) | МБ | **не закрыто** |
| база `diary_bot` | 150 КБ | `diary-bot-backup.timer`, ежедневно |

```bash
bash scripts/ops/backup-heavy.sh --dry-run   # что и куда поедет
bash scripts/ops/backup-heavy.sh             # запускать С MAC, не на сервере
```

Скрипт снимает sqlite консистентно (`sqlite3 .backup`, прод не останавливается), тянет снимок в
`~/Backups/serv1/<дата>/`, повторяющиеся файлы жёстко линкует на предыдущий снимок (`--link-dest`),
проверяет `PRAGMA quick_check` и хранит последние `BACKUP_KEEP` (по умолчанию 2) снимков.

**Чистка 2026-08-04.** Освобождено 41 ГБ (диск был занят на 97%, стал на 63%): 129 бэкапов кода
(уникальные сложены в `/root/archive/backups-unique-20260804.tar.gz`), копии дерева
`.baseline_87049d40`, `.shadow_base`, `_wl_stage3`, `winline-correction-review-20260724` — все
проверены на отсутствие уникальных коммитов, 34 тяжёлых файла завершённых прогонов (27 ГБ,
манифест — `/root/archive/_moves/20260804_deleted_heavy_runs.tsv`, логи и отчёты прогонов
сохранены), journald ужат до 500 МБ, для `/var/log/xray` добавлена ротация.
