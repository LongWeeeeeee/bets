---
id: E-07
title: "Зеркальная ориентация vs-ключа в kills"
date: "2026-08-05"
area: kills
status: full
corpus: "прод-отбор"
verdict: "нейтрально"
harness: "`runtime/experiments/kills/mirror_prod_selection.py` (в лоб), `runtime/experiments/kills/mirror_matched_volume.py` (на равном n) - **Команда:** дампы — `KILLS_WINDOW_MIRROR_POOLING"
---

# E-07. Зеркальная ориентация vs-ключа в kills
- **Дата:** 2026-08-05
- **Гипотеза:** билдер пишет матчап один раз с якорем на radiant, поэтому
  `A_vs_B` и `B_vs_A` — непересекающиеся наборы матчей. Драфтовые читатели пулят
  обе стороны, читатель kills брал только прямую — то есть терял ровно половину
  выборки в каждой ячейке слоёв 1v1/1v2/2v1.
- **Как мерили:** прод-отбор kills (4 связки |ed| + NW + полоса), 1099 карт.
  **Обязательно на равном объёме** — см. правило 2.
- **Харнесс:** `runtime/experiments/kills/mirror_prod_selection.py` (в лоб),
  `runtime/experiments/kills/mirror_matched_volume.py` (на равном n)
- **Команда:** дампы — `KILLS_WINDOW_MIRROR_POOLING=0|1 KC_TAG=_mir0|_mir1
  python3 runtime/experiments/kills/kills_combo_extract_v2.py`, затем скрипты выше
- **Артефакт:** `runtime/artifacts/kills/mirror_matched_volume.md`
- **Результат:** в лоб выглядело победой — 75.0% на 511 ставках против 72.7% на
  627. Но пулинг **сжимает `|ed|` к нулю**, при том же пороге проходит на 18%
  меньше карт. На равном объёме: −1.0 / +0.6 / +0.5 п.п., то есть ноль.
  Знак `ed` при этом почти никогда не переворачивается.
- **Вердикт:** нейтрально. Флаг `KILLS_WINDOW_MIRROR_POOLING` оставлен выключенным.
  Тот же эффект даёт множитель порога без кода: ×1.5 → 73.8% на 461 ставке,
  ×2 → 75.1% на 313.
- **Где искать ошибку:** (1) `invert=True` должен менять местами лиды и поражения
  И переворачивать `diff_sum` — покрыто `base/tests/test_kills_window_mirror_pooling.py`;
  (2) порог `min_games` применяется к ОБЪЕДИНЁННОЙ ячейке, иначе добор не работает
  там, где нужен; (3) выбор окна в харнессе — см. раздел 2, дефект открыт.
- **Коммиты:** `9fdc3ed` / serv1 `27733ec`
