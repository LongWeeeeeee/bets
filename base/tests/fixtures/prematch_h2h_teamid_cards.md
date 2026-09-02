# prematch_h2h_teamid_cards.{npz,json}

**Захвачено:** 02.09.2026

**Команда захвата:**

```
/Users/alex/Documents/ingame/venv_catboost/bin/python3 \
    /private/tmp/claude-501/-Users-alex-Documents-ingame/f9bc5dd1-4122-4a05-b0d0-eb359c101538/scratchpad/h2h/capture_fixture.py
```

(скрипт капчи сохранён рядом со сценарием исполнения задачи, не в репозитории;
логика воспроизводима — см. ниже.)

## Источник

- Полный артефакт: `runtime/artifacts/misc/prematch_audit_artifact_cut1774742400.npz`
  (боевой снимок `PrematchModel`, не коммитится — `runtime/` в `.gitignore`).
- Корпус карт/команд: `runtime/artifacts/misc/pro_corpus_compact.npz`
  (`zc["mids"]`, `zc["accounts"]`, `zc["teams"]`, `zc["heroes"]`, `zc["ts"]`, `zc["wins"]`),
  путь и `TEST_FROM=1774742400` — из `runtime/experiments/misc/ideas_batch2.py`.

## Отбор 50 карт

Из тестовой выборки (`ts >= TEST_FROM`) взяты первые 50 карт, у которых
ОДНОВРЕМЕННО:

1. `radiant_team_id`, `dire_team_id` > 0;
2. командный ключ `(min(rt,dt), max(rt,dt))` есть в `PrematchModel.h2h`;
3. `resolve_org(rt, racc)` / `resolve_org(dt, dacc)` дают разные org id > 0,
   и организационный ключ есть в `PrematchModel.h2h_org`;
4. `PrematchModel(FULL_ARTIFACT).score(..., strictness="teams", now_ts=None,
   max_age_days=0.0)` не бросает `MissingData` (все 10 игроков и оба
   командных id известны снимку) — без этого гейта фикстура один раз забрала
   карту с неизвестным аккаунтом и падала в `MissingData` при полном прогоне
   50 карт вместо частичного.

49 из 50 карт: командное и организационное значение `h2h` РАЗЛИЧАЮТСЯ — то
есть фикстура ловит перепутанный порядок предпочтения (сначала org вместо
сначала team), а не просто «team_id != 0».

## Содержимое

- `prematch_h2h_teamid_cards.npz` — минимальный валидный артефакт
  `PrematchModel`: те же ключи, что в боевом (`mu/sd/coef/intercept`,
  `ctx_mu/ctx_sd`, `feature_names`, `accounts`, `acc_hero`, `acc_pos`,
  `hero_wr30`, `hero_farm`, `vs_pairs`, `h2h`, `h2h_org`, `team_merge`,
  `org_roster`), но СТРОКИ отфильтрованы только под 50 отобранных карт
  (аккаунты/герои/команды/организации, встречающиеся в них). `vs_pos`/
  `syn_pos`/`vs_flat`/`syn_flat` не нужны — `_draft_cells` деградирует к
  нейтральным значениям на отсутствующих ячейках, `MissingData` не бросает.
  496 КБ против 93.6 МБ полного артефакта.
- `prematch_h2h_teamid_cards.json` — по одной записи на карту: аккаунты,
  герои (позиции 1..5), `radiant_team_id`/`dire_team_id`, исход, и
  `raw_val`/`org_val` — НЕОБработанное (без учёта свопа знака) значение из
  `self.h2h.get(key)` / `self.h2h_org.get(key)` для канонического ключа
  `(min(id1,id2), max(id1,id2))`. Тест сам вычисляет знак через
  `radiant_team_id > dire_team_id` (`swap` в `PrematchModel.score`).
