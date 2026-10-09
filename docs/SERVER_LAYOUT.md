# Раскладка файлов на serv1 (ssh-алиас `serv1`)

> Наведено 2026-08-04. Манифесты всех перемещений: `/root/archive/_moves/20260804_phase*.tsv`
> (формат `старый_путь<TAB>новый_путь`) — откат любой фазы делается обратным `mv` по колонкам.

## `/root` — по проектам

| Путь | Что это |
|---|---|
| `main/` | ingame — live-пайплайн Dota 2 (см. ниже) |
| `pro_chain/` | ingame — ОТДЕЛЬНОЕ дерево сборки ночной цепочки про-корпуса (git worktree от `main/`, создаёт `scripts/ops/setup_pro_chain_serv1.sh`): корпус `pro_heroes_data/json_parts_split_from_object/*.json.gz`, кэши `runtime/artifacts/misc/`, `base/keys.py` — симлинк на боевой. Сборка внутри `main/` запрещена (`pro_chain_guard`): её выходы — это файлы, которые читает прод |
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
| `bets_data/analise_pub_matches/json_parts_split_from_object/*_partNNN.json` — корпус паблик-карт | ~36 ГБ, 101 файл | **только на Mac** с 2026-09-16 (удалены на serv1 после md5-сверки 104/104 и двух пробных обходов); на serv1 остались `pub_player_steam_ids.json` + `processed_ids.txt` + `part_counters.json` (см. `docs/CODE_MAP.md` → `maps_research.py`). Следствия: `scripts/run/rebuild_dicts.sh` (`explore_database.py`) на serv1 больше не запустить — словари пересобирать на Mac и переливать sqlite; новые части, которые обход дописывает на serv1, переносятся на Mac (`rsync` part-файлов + `processed_ids.txt`, `part_counters.json`, `pub_player_steam_ids.json`) и удаляются на serv1 автоматически каждый час (`pub_parts_offload`, ниже); туда же уходят `temp_files/*.txt` слитого обхода (4,9 ГБ накопилось к 08.10.2026) — на serv1 остаётся только map-id state |

**Автоматический перенос part-файлов serv1 → Mac** (`scripts/ops/pub_parts_offload.sh` → `pub_parts_offload.py`, тесты `tests/test_pub_parts_offload.py`, шаблон `scripts/ops/launchd/com.ingame.pub-parts-offload.plist`, **каждый час в :40** (`StartCalendarInterval` только с `Minute`; владелец 08.10.2026: на serv1 остаются только map id для сверки карт, сами данные живут на Mac и уходят с serv1, как только обход закончился); плист кладётся в `~/Library/LaunchAgents` вручную, сам не ставится). Часть-файл только создаётся (`maps_research.py:3027` `_open_part`, номер = max(диск, `part_counters`)+1, запись через `.tmp` + `os.replace`), дописывания существующей части нет — поэтому «последняя часть патча» не исключается; кандидат = `<patch>_partNNN.json[.gz]` старше 1 ч (`--min-age-hours`, было 6: часть создаётся целиком через `.tmp` + `os.replace`, слияние идёт внутри обхода под `pub_recrawl.lock`, а замок, проверка процесса merge, свежий `.tmp` и окно тишины 30 мин у `processed_ids`/`part_counters` уже доказывают, что часть окончательная; 1 ч нужен лишь чтобы быть больше окна тишины). Fail closed — любая проверка не прошла = отказ целиком, на serv1 ничего не удаляется, админу уходит `notify_admin.py`, exit 1. Проверки: нет процесса merge на serv1 и `pub_recrawl.lock` свободен (занят = exit 3, пропуск; `--allow-busy-sweep` — идти при занятом замке); на Mac не идёт сборка словарей (`base/explore_database.py` читает `json_parts_split_from_object/` один раз на процесс, новые части между группами сборки дали бы словари на разных корпусах; маркеры в командной строке: `explore_database.py`, `rebuild_dicts.sh`, `build_driver.py` — драйвер групп спит между ними; свой процесс и его предки не считаются; идёт = exit 3, пропуск, уведомление если кандидаты ждут >72 ч; сборка, стартовавшая во время прогона, = отказ до записи частей на Mac); `processed_ids.txt` и `part_counters.json` не менялись >30 мин и не менялись за время прогона; `rsync -a --timeout=300` + sha256 serv1 == Mac (имя уже есть на Mac: тот же хэш = уже перенесено, другой = отказ); каждый ключ карты каждой части входит в `processed_ids.txt` serv1 (иначе краул переписал бы карты); `processed_ids` serv1 ⊇ Mac, счётчик serv1 ≥ Mac и ≥ номера каждой переносимой части; на Mac старые `processed_ids.txt`/`part_counters.json`/`pub_player_steam_ids.json` (лежит в `analise_pub_matches/`, не в `json_parts_split_from_object/`) сохраняются как `<имя>.bak_before_offload_<YYYYMMDD_HHMM>`; удаляются на serv1 только проверенные файлы (`rm --` по явным путям, под `flock` на `pub_recrawl.lock`, размер+mtime те же, что при хэшировании). Лог: `runtime/artifacts/pubs-rebuild/pub_parts_offload.log`. Проверка без изменений: `bash scripts/ops/pub_parts_offload.sh --dry-run`.

**Фаза 2 — `bets_data/analise_pub_matches/temp_files/*.txt`** (обход сливает их в части с `cleanup=False`, `maps_research.py:1360-1364`, поэтому они копились: 14 064 файла / 4,9 ГБ на 08.10.2026). Выполняется в том же прогоне после фазы 1 и **только когда обход закончен**; файл удаляется, лишь если выполнено всё: (1) `pub_recrawl.lock` на serv1 свободен и `runtime/pub_recrawl.json` → `status == "complete"` (упавший/идущий обход возобновляется, используя `temp_files` как набор дедупа, `maps_research.py:1107-1124`, его слияние ещё не прошло); (2) на serv1 не осталось ни одной части (и `.tmp`) — все части проверены и лежат на Mac; (3) файл целиком разобран **на serv1** (`nice -n 10`, `/root/venvnp/bin/python`, отсортированный `int64` + `searchsorted`; по ssh идёт только список вердиктов, 4,9 ГБ на Mac не копируются): mtime < `completed_at` обхода, непустой JSON-объект, каждый ключ — числовой map id и **каждый** id есть в `processed_ids.txt` serv1 (слияние кладёт id туда только после записи карты в часть); битые / пустые / более новые / с любым отсутствующим id / больше 128 МБ файлы **остаются** и попадают в лог с причиной и именами (`ids_missing`, `unparsable`, `newer_than_sweep`, `empty`, `not_object`, `bad_key`, `too_large`); (4) удаление идёт под тем же `flock` на `pub_recrawl.lock` и заново проверяет замок, `status`/`completed_at`, отсутствие частей, штампы `processed_ids.txt`/`part_counters.json` и размер+mtime_ns каждого файла; намерение (имена + число id) пишется в `runtime/artifacts/pubs-rebuild/pub_temp_cleanup_manifest.jsonl` ДО отправки удаления, итог — после. Условия «ещё рано» (обход идёт, части ещё переезжают, замок/merge, файлы состояния моложе 30 мин) молча пропускают фазу, следующий прогон повторит. `processed_ids.txt`, `processed_ids_to_graph.txt`, `part_counters.json`, `pub_player_steam_ids.json`, `trash_maps.txt` и прочий map-id state на serv1 фаза не трогает. `--no-temp-phase` отключает фазу.

**Тишина при почасовом запуске**: ssh/connect-сбой до первого ответа serv1 только пишется в лог, админу — одно сообщение, когда serv1 недоступен >6 ч (напоминание раз в 24 ч); одинаковый ABORT — раз в 24 ч; исход удаления UNKNOWN/PARTIAL — всегда. Состояние: `runtime/artifacts/pubs-rebuild/pub_parts_offload_state.json` (также кэш вердикта temp_files: неизменный набор не разбирается заново каждый час).

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
