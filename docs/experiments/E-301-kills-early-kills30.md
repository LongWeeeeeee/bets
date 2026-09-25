---
id: E-301
title: "Ставка на ≥30 килов от ранних моделей: Early Win ≥0.60 (или Early NW без Early Win против) + E-281 P(сторона ≥30) ≥0.60"
date: "2026-09-19"
area: kills
status: full
corpus: "Два боевых кейса 19.09.2026 (панели + /root/main/runtime/ml_dispatch_decisions.jsonl); офлайн-выборки под это правило нет"
verdict: "Правило владельца, не измеренная граница. До 19.09 kills_total рождался только у ELO-андердога (Early NW/Win ★ за него) или в ветке 4.3 (early★+All★ против одиночного Late★) и резался гейтом E-281 0.60/0.70; обе карты 19.09 отпали именно там (no_underdog; kills30_below_threshold 0.658<0.70). Новый путь kills_early: Early Win ★ за A (Early NW против не мешает) либо Early NW ★ за A при молчащем Early Win, и P(A ≥30) ≥ 0.60 → kills_total на A, независимо от ELO и Late/All. По E-289 бин P 0.6–0.7 сбывается в 60.7% — прибавка от ранних ★ офлайн NOT_CHECKED. Откат ML_DISPATCH_KILLS_EARLY=0."
harness: "base/tests/test_ml_dispatch.py (секция kills_early); боевой замер — runtime/ml_dispatch_decisions.jsonl, записи с rule=kills_early_*"
---

# E-301 — Ставка на ≥30 килов от ранних моделей + E-281 (19.09.2026)

board: `ingame-4xop`

## Вопрос

Владелец, 19.09.2026, две карты без ставки на ≥30 килов:

- `dltv.org/matches/9006060043.17` Team Nemesis (Radiant) vs 1w, карта 3: Early NW Radiant 76.9★, Early Win Radiant 79.5★,
  Late Dire 61.7★, All Radiant 52.4, ELO 2282/2247; E-281 Radiant ≥30 — 60.4%, Dire — 44.1%.
- `dltv.org/matches/9006135810.23` Nemiga (Radiant) vs Team Synapse, карта 2: Early NW Dire 69.5★, Early Win Radiant 63.2★,
  Late Dire 54.2, All Dire 50.7, 🤖 Dire 79.6★, ELO 1860/2183; E-281 Radiant ≥30 — 65.8%, Dire — 46.4%.

«Если early_win ≥60 и команда ≥30 килов ≥60% — отправляем. Также если early_win/early_nw за эту же команду и ни одна
из них не против 60+, и команда ≥30 килов ≥60».

## Почему ставки не было (OBSERVED)

Источник: `/root/main/runtime/ml_dispatch_decisions.jsonl` на serv1 (одно чтение, 7 записей по двум картам).

| карта | skipped kills_total | причина в коде |
|---|---|---|
| 9006060043.17 | `no_underdog` (elo_diff 34.69) | `_evaluate_kills_underdog` требует ELO-андердога (diff ≥ 50); Late★ 1w против early★ Nemesis при All 52% — подветка «a» (ждать 31:00, win на 1w ушёл в 31:57), ветка 4.3 не открылась (нужен All★). Даже при решении гейт: 0.604 < 0.70 (равные команды). |
| 9006135810.23 | `kills30_below_threshold` (0.658 < 0.7, underdog) | решение `kills_underdog_total` создано (Early Win 63.2★ за андердога), срезано гейтом E-281 13.09 (`_apply_kills_total_gate`: 0.70 для андердога/равных). kills_window — `kills_requires_tier1_team`. |

## Правило (реализовано в `base/ml_dispatch.py`, `_evaluate_kills_early`)

Выполняется после `_apply_kills_total_gate`; существующие пути и гейт 0.60/0.70 не тронуты.

1. Если `kills_total` уже решён любым путём (любая сторона) — ничего не добавляется (одна `kills_total` на карту).
2. A = сторона Early Win при `conf ≥ ML_DISPATCH_MIN_CONF` (0.60); Early NW против A не мешает (кейс Nemiga).
   Иначе A = сторона Early NW ★ (Early Win при этом не ★ ни за кого, т.е. «не против»). Иначе — выхода нет.
3. `P(A ≥ 30)` из E-281 (`Ctx.kills30`): нет числа → `kills30_missing` (fail-closed); `< ML_DISPATCH_KILLS_EARLY_MIN_KILLS30`
   (0.60, включительно) → `kills30_below_threshold`, detail с пометкой `(kills_early)`.
4. Решение `kills_total`, rule `kills_early_win_kills30` / `kills_early_nw_kills30`, `models_for` = ранние ★ + `kills30`,
   `timing="now"`, `expected_wr = P(A ≥ 30)` (модель самого события, а не P(NW-лид) ранних моделей — сознательно),
   `min_odds = 1/expected_wr`. Ценового гейта у kills-рынков в доставке нет (`_deliver_and_persist_signal`), число информационное.
5. Дедуп — общий ключ `(base_url, map_num, "kills_total", side)`; независимо от ELO, `underdog_side`, Late/All★ против A,
   `late_conflict_wait` (win-рынок при этом ждёт 31-ю минуту как раньше).
6. Выключатель `ML_DISPATCH_KILLS_EARLY=0` (systemd drop-in, без деплоя) возвращает поведение до 19.09 в точности.

На двух картах 19.09 правило даёт: Nemesis (Radiant) — `kills_early_win_kills30`, WR≈60.4%; Nemiga (Radiant) — `kills_early_win_kills30`, WR≈65.8%.

## Контрдоказательства и оговорки

- E-289 (про-тест, 1374 стороны): бин P 0.6–0.7 сбывается в 60.7% [0.548; 0.663], 0.7–0.8 — 72.4%. Правило 13.09 ставило 0.70
  для андердога/равных именно поэтому; новое правило опускает планку до 0.60 при наличии ранней ★. Есть ли прибавка от
  Early Win/NW ★ к P(≥30) — офлайн **NOT_CHECKED** (нет выборки «ранние вердикты + E-281 + исход по килам стороны»).
- Кейс Nemiga: Early NW 69.5★ против A — правило сознательно игнорирует, это решение владельца.
- Nemesis: Late★ против A и win-рынок в ожидании 31-й минуты; kills-ставка на A и win-ставка на B на одной карте —
  допустимо по правилу, но противоречие моделей стоит учитывать при разборе исходов.
- Один `kills_total` на карту: если существующий путь уже дал решение на другую сторону, новое правило молчит.

## Как измерить в бою

`runtime/ml_dispatch_decisions.jsonl` на serv1: записи `decisions[].rule ∈ {kills_early_win_kills30, kills_early_nw_kills30}`
и `delivered`. Исход — килы стороны по `match_id` из `MAP_VERDICTS_PATH` (OpenDota `radiant_score`/`dire_score`). Считать
попадание ≥30 отдельно по rule и по бинам `expected_wr` (0.60–0.65 / 0.65–0.70 / ≥0.70); сравнить с E-289-калибровкой той же
модели. Отдельно — доля карт, где kills-ставка на A совпала с win-ставкой на B (late_conflict).

## Где искать ошибку

- Сторона: `Ctx.kills30_radiant/dire` заполняются из `win_model_veto.last_kills30(index)`; перепутанная ориентация даст
  ставки на «не ту» команду при внешне правдоподобных числах — сверять с панельной строкой «Radiant ≥30 килов».
- Порядок в `evaluate()`: `_evaluate_kills_early` обязан идти ПОСЛЕ `_apply_kills_total_gate`, иначе гейт 0.70 срежет его решения.
- Стейл-скипы: после добавления решения удаляются `kills_total`-скипы той же стороны/None; если в логе видны и decision, и skip
  для одной стороны — сломан этот шаг.
- Тесты гейта 13.09 пиннят `kills_early_enabled=False`; «зелёный» набор без этой пометки означает, что тесты ловят не гейт.

## Команды

```bash
venv_catboost/bin/python3 -m pytest base/tests/test_ml_dispatch.py -q -p no:cacheprovider
ssh serv1 "grep -c kills_early_ /root/main/runtime/ml_dispatch_decisions.jsonl"
```
