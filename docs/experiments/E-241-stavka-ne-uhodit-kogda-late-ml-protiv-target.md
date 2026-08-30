---
id: E-241
title: "Обычная ставка не уходит, когда Late ML-модель против target"
date: "2026-08-30"
area: star-dispatch
status: rule-change
corpus: "нет — правка правила по решению владельца, без предварительного замера"
verdict: "внедрено без замера; замер прибавки не проводился и здесь не заявляется"
harness: "base/tests/test_late_bet_requires_late_win_model.py"
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
