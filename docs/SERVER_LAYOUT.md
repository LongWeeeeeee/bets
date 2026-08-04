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
runtime/                ЖИВОЕ: очереди сигналов, локи, shadow/telegram jsonl, state демонов,
                        winline_parser_monitor.* (на него ссылается systemd-юнит), omniroute.log
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
- бэкап прод-файла → `base/_archive/backups/` (в корне `base/` бэкапам не место).

**Не трогать при уборке**
- `base/runtime/*` и `runtime/*.lock`, `runtime/delayed_signal_queue*.json`,
  `runtime/*_shadow.jsonl`, `runtime/*_telegram_sent.jsonl`, `runtime/live_elo_*`,
  `runtime/sourcetv_matches.json`, state-файлы демонов kanban/hermes;
- `runtime/winline_parser_monitor.py|.log`, `runtime/anti_stall_supervisor/`,
  `runtime/ruflo*/`, `runtime/worker676-gateway/` — на них ссылаются systemd-юниты;
- `runtime/_wl_stage3/` — хардлинк-копия дерева, в ней тот же inode, что у живого прод-лога;
- `base/keys.py` и `base/keys.py.bak_*` — файлы с ключами, пути не двигаем.

Перед переносом чего-либо в `runtime/`: `lsof -n +D /root/main/runtime`, проверка pid'ов
(`kill -0`) и `grep` имени файла по `base/`, `ELO/`, `/etc/systemd/system/`.
