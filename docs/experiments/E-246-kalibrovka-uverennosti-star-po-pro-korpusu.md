---
id: E-246
title: "Калибровка уверенности STAR по про-корпусу: номинальные WR60–95 завышают факт во всех фазах"
date: "2026-09-02"
area: star
status: full
corpus: "про-корпус, 66 336 дедуп-матчей (как E-70) × draft_features_pro.jsonl → 3 873 карты с STAR-вердиктом; отбор воспроизводит _star_block_diagnostics/block_valid из star_block_dispatch_ab.py БЕЗ вето E-73/E-142/E-238 (win_model_veto, draft_against_side) — шире прод-отбора"
verdict: "Строка «WR≈N% от кэфа» в Telegram по умолчанию — номинальный ярлык уровня (STAR_ODDS_USE_CALIBRATION=False, cyberscore_try.py:5548). Факт на про: early 60→58.4%, 65→62.0%, 70→68.2%, 75→74.7%, 80→79.7% (n 625/912/614/356/178); late 60→56.1%, 65→56.8%, 70→62.1%, 75+→65.0% (n 355/567/499/112); all (post-lane) 60→52.6%, 65→49.9%, 70→57.0%, 75→61.9% (n 399/417/537/310). Безубыточный кэф выше номинального во всех ячейках (early +0.00…+0.20, late +0.11…+0.49). Уровни 85–95 на про пусты (n≤32) и складываются в плато. Дефект загрузчика: _load_star_confidence_calibration читает только фазы early/late (hardcoded), фаза all выбрасывается — post-lane нельзя откалибровать без правки кода. Таблица data/star_confidence_calibration.json заменена (старая, 03.08, без писателя — в base/_archive/backups/), флаг пока выключен."
harness: "runtime/experiments/misc/build_star_confidence_calibration.py (не в git — runtime/ игнорируется, как у соседних харнессов); отчёт runtime/artifacts/misc/star_confidence_calibration_2026-09-02.md"
---

# E-246. Калибровка уверенности STAR по про-корпусу

- **Дата:** 2026-09-02. Запрос alex «fix everything» после аудита формул, где
  выяснилось, что показываемый бетору «WR≈N%» — ярлык уровня, а не вероятность.
- **Вопрос:** какой фактический винрейт стоит за каждым номинальным уровнем STAR
  60..95 в каждой фазе (early / late / all) на прод-подобном отборе, и что
  изменит включение `STAR_ODDS_USE_CALIBRATION`.
- **Харнесс:** `runtime/experiments/misc/build_star_confidence_calibration.py`.
- **Запуск:** `venv_catboost/bin/python3 runtime/experiments/misc/build_star_confidence_calibration.py`
  → пишет `data/star_confidence_calibration.json.tmp` и отчёт с n и ДИ Уилсона.
- **Схема файла** (как читает `_load_star_confidence_calibration`, cyberscore_try.py:5515-5540):
  `{"phases": {"early": {"60": WR, …}, "late": {…}, "all": {…}}}`; `early_output`/
  `early_end_output` → early, `mid_output` → late, `all_output` → all (загрузчик
  фазу all не читает — дефект).
- **Правило сглаживания:** при n<60 уровень объединяется с соседними сверху
  вниз до n≥60, объединённый WR присваивается всем членам (плато 85–95).
- **Результат:** см. таблицу в verdict; полная с ДИ — в отчёте.
- **Контроль:** отбор без ML-вето шире продового, значит числа могут быть чуть
  пессимистичнее прод-отбора; порядок величин с E-18 (WR70 pro cp1vs1 сбылся на
  8 из 4 680 карт) и E-76 согласуется.
- **Где искать ошибку:** (1) отбор шире прод; (2) уровни 80+ с n≤32; (3) одна
  таблица на все метрики уровня, тогда как реальный WR зависит и от того, какая
  метрика дала уровень; (4) фаза all в загрузчике мертва — калибровка post-lane
  не включится без правки кода.
- **Вывод:** таблица заменена; включить флаг и читать фазу all — отдельным
  коммитом в cyberscore_try.py (в этом ходу, после слияния веток). Включение
  только ужесточает минимальный кэф, ослабить ставку оно не может.
