---
id: E-241
title: "Обычная ставка не уходит, когда Late ML-модель против target"
date: "2026-08-30"
area: star-dispatch
status: rule-change
corpus: "нет — правка правила по решению владельца, без предварительного замера"
verdict: "внедрено без замера. 01.09.2026 доработка: предсказанный дефект №1 сработал в бою — путь star_signal_sent_now_prematch_model не передавал stake_multiplier_context, и гейт на нём молчал; 4 отправки из 68 за 30.08-01.09 ушли при late-модели против таргета (map_verdicts.json, serv1). Контекст передан, неизвестная сторона ставки стала запретом (late_model_target_unknown), late-гейт диспатча согласован с ML late"
harness: "base/tests/test_late_bet_requires_late_win_model.py, base/tests/test_late27_guard_agrees_with_late_model.py"
---

# E-241. Обычная ставка не уходит, когда Late ML-модель против target

- **Дата:** 30.08.2026. Запрос владельца дословно: «запрети ставку когда Late
  ML-модель против target». Утром того же дня гейт `_late_win_model_reject_for_delivery`
  уже закрывал только late-driven ставки (`_is_late_driven_context`: late-звезда
  без same-sign early). Early/all при строке `🕑 Late ML-модель` за другую
  сторону уходили.

- **Вопрос:** НЕ ставился как замер. Это изменение правила подачи, внедрённое
  по решению владельца. Прибавка к точности или ROI **не измерялась**.

- **Что изменилось в коде:** `late_model_against` читается **до** проверки
  late-driven. Если в панели есть строка late-модели и её сторона не target —
  обычная ставка не уходит (immediate, delayed watcher, спекулятив — та же
  точка `_deliver_and_persist_signal`). `late_model_missing` не расширен:
  молчание модели по-прежнему режет только late-driven; у early/all строки
  часто нет, и это не «против». Kills-ставки не затрагиваются (нет заголовка
  множителя). Откат — `BET_REQUIRE_LATE_WIN_MODEL=0`.

- **Харнесс:** `base/tests/test_late_bet_requires_late_win_model.py` — against
  на late-driven, on early-confirmed, без late-звезды и когда сторона ставки
  не из late-знака; missing остаётся только у late-driven; проводка в
  `_deliver_and_persist_signal` и для early-подтверждённой ставки.
- **Запуск:** `venv_catboost/bin/python3 -m pytest base/tests/test_late_bet_requires_late_win_model.py -q`
- **Где искать ошибку:**
  1. Контекст без `target_side` (radiant/dire) — сверка сторон пропускается,
     как у предматчевого `model_against`. Ставка уйдёт, хотя строка модели
     против имени команды.
  2. Строка late-модели читается из текста панели, не из `last_late(index)`:
     если панель не напечатала строку (артефакт молчит), early/all не режутся.
  3. Эффект по объёму: режутся карты, где late-модель есть и против, а ставку
     задаёт early/all. Считать по логу `Ставка заблокирована late-моделью`
     и reason `late_model_against`, не по `late_model_missing`.

---

## Доработка 01.09.2026 — «где искать ошибку» п.1 сработал в бою

- **Замер утечки.** `/root/.local/state/ingame/map_verdicts.json` на serv1
  (400 карт, 30.08-01.09.2026): 191 карта с панелью `СТАВКА НА <team> x<mult>`,
  из них отправлено 68. Строка `🕑 Late ML-модель` есть у 68 из 68. Сторона
  модели расходится с ставкой у **4 из 68**, и все четыре — с одного пути,
  `star_signal_sent_now_prematch_model` (карты `8976012755.0/.1`, `8976441468.0/.1`).
  Гейт при этом работает: reject `late_model_against` — 27 карт, последний
  31.08 21:04; соседний предматчевый `model_against` — 43.

- **Причина, `file:line`.** `_try_dispatch_prematch_model_bet`
  (`base/cyberscore_try.py`) звал `_deliver_and_persist_signal` без
  `stake_multiplier_context` — единственный из 21 вызова доставки, который
  строит обычную ставку `СТАВКА НА <team> x<mult>` и контекст не передаёт
  (остальные 8 вызовов без контекста шлют не-ставочные панели: kills, минимальные
  кэфы, protracker-only, tempo). Без контекста `target_side` пуст, и гейт
  возвращал `None` — молча. Юнит-тесты этого не ловили: тест
  `test_missing_context_does_not_block` закреплял именно молчание как ожидаемое.

- **Что изменилось.** (1) Путь предматчевой модели передаёт контекст
  (`target_side`, имена команд, `late_model_side`). (2) Неизвестная сторона
  ставки при названной стороне модели теперь ЗАПРЕТ — `late_model_target_unknown`,
  а не пропуск. (3) Сторона модели берётся из строки панели, а при её отсутствии
  из `stake_multiplier_context["late_model_side"]` (текст delayed-записи мог быть
  пересобран). (4) Late-гейт диспатча `_evaluate_late27_dispatch_guard` получил
  тот же вердикт в снимке и блокирует расхождение (`late_model_against`) —
  просьба владельца «late dispatch gate также должен быть согласован с ML late».
  Молчание модели в диспатче НЕ блокирует: отказ там зовёт `add_url` и закрывает
  карту, а вердикта может не быть временно (история разложений держит 32 записи).

- **Харнесс:** `base/tests/test_late_bet_requires_late_win_model.py`
  (+3 теста: fail-closed без контекста, сторона из контекста, проводка пути
  предматчевой модели по причине `late_model_against` с известным `target_side`),
  `base/tests/test_late27_guard_agrees_with_late_model.py` (8 тестов).
- **Запуск:** `venv_catboost/bin/python3 -m pytest base/tests/test_late_bet_requires_late_win_model.py base/tests/test_late27_guard_agrees_with_late_model.py -q`
- **RED до правки:** 8 failed, 16 passed. **GREEN после:** 24 passed.
  Соседние наборы (`test_bet_requires_win_model`, `test_half_stake_elo_underdog`,
  `test_early_nw_win_model` + оба выше): 77 passed.
  `tests/test_networth_dispatch_gates.py` локально пропускается целиком
  (60 skipped) по предсуществующему условию отсутствующих status-констант.
- **Где искать ошибку:**
  1. Fail-closed по `late_model_target_unknown` бьёт по ЛЮБОМУ будущему пути,
     который построит `СТАВКА НА <team> x<mult>` и не передаст контекст. Считать
     по логу `Ставка заблокирована late-моделью` с этой причиной: ненулевой
     счётчик = появился ещё один путь без контекста, чинить путь, а не гейт.
  2. `late_model_side` в контексте считается один раз, при сборке сигнала,
     через `_late_model_side_from_blocks` -> `win_model_veto.last_late(index)`.
     История разложений — 32 записи; при большем числе живых карт в такте запись
     вытесняется, и в контекст уедет `None`. Тогда строки в панели тоже не будет,
     и late-driven ставку срежет мягкий `late_model_missing`.
  3. Диспатч-гейт закрывает карту (`add_url`). Если объём late-ставок просядет
     сильнее ожидаемого, смотреть `late27_guard_late_model_side` и
     `late27_guard_target_side` в details отказа.
