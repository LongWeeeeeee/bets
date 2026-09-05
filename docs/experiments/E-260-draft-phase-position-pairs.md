---
id: E-260
title: "Четыре draft-модели: пары с позициями и Early NW без отброса карт без маркера"
date: "2026-09-05"
area: ml
status: in_progress
corpus: "6 638 652 публичных карты >=20 мин (24.03–04.09.2026); 1 151 356 про-карт после канонизации; Early NW no_marker 2 461 376 в паблике"
verdict: "Обучение запущено; оценки качества пока не получены, прод не переключён"
harness: "scripts/run/retrain_draft_phases.sh; base/build_draft_phase_corpus.py; base/train_draft_phase_models.py"
---

# E-260 — draft phase position-pair corpus/model training

**Статус:** IN PROGRESS (2026-09-05). Численных результатов пока нет; фактический статус и результаты добавит lead.

## Цель и ограничения

Проверить для свежего канонического корпуса opt-in position-pair encoder (`hero_role_position_pair`) в четырёх фазах: `early_nw`, `late`, `all`, `early_win`. Целевые популяции: `early_nw` — минимум 20 минут, окно 20–28, joint 3 classes с факторизацией occurrence + direction; `late` — минимум 36 минут; `all` — минимум 20 минут; `early_win` — 20–34 минуты включительно. Разбиение causal 60/20/20. Baseline — matched corpus с `hero_role_pair`; production/runtime legacy artifacts не переключаются.

## Harness и воспроизведение

Раннер — `scripts/run/retrain_draft_phases.sh`. Команда (долгий прогон запускать через `nohup`):

```bash
bash scripts/run/retrain_draft_phases.sh 2026-09-05_position_pairs
```

Основные контракты: `base/build_draft_phase_corpus.py`, `base/train_draft_phase_models.py`, `base/draft_phase_model.py`, `base/draft_features.py`. Проверки: `base/tests/` тесты corpus canonicalization/dedup и draft feature/model contracts; перед дорогим прогоном проверить `manifest.json` (`complete=true`), fingerprint, duplicate/conflict counts и chronological split.

## Evaluation contract

`evaluation_model.joblib` обучен без held-out test outcomes и сопровождается `evaluation_results.json`; `model.joblib` — full refit и не имеет честного held-out score. Pro evaluation — future-only после соответствующего cutoff, с исключением `mid`, общих с public corpus. Никаких численных выводов до появления артефактов и отчётов.

## Известные дефекты и где искать ошибку

В коде исправлен alias неизвестного героя в role-признак; введены проверки дублей/конфликтов, конечности и полноты окна нетворта, изменения source во время чтения и инвалидирование кэша по SHA256. Это дефекты/регрессионные сценарии кода, а не уже измеренные частоты ошибок в новом корпусе. Точные counts берутся только из итогового `manifest.json`. При странных n проверять canonicalizer, `global_counts`, `duplicate_conflict` и `conflicting_map_ids_rejected` в `build_draft_phase_corpus.py`. При подозрении на leakage проверять `chronological_cuts`, fit vocabulary/C selection и shared-ID mask в `train_draft_phase_models.py`; при неверной раскладке классов — `DraftPhaseModel.predict_proba`.

Проверка 05.09 до старта: `venv_catboost/bin/python3 -m pytest base/tests/test_draft_phase_training.py base/tests/test_draft_phase_corpus.py base/tests/test_draft_position_pairs.py base/tests/test_train_public_draft_hero10_experiment.py base/tests/test_late_win_model.py base/tests/test_early_nw_win_model.py -q` — **84 passed, 1 skipped**. Ресурсы проверены `bash scripts/ops/capacity.sh`: локально 16 GiB RAM / 116 GiB свободного диска; serv2 8 GiB RAM, serv1 20 GiB свободного диска с живым продом. Сборка — два процесса локально, fit — по одному компоненту с disk-backed CSR и двумя BLAS threads.

## Следующий шаг

Дождаться завершения harness, затем внести сюда фактические counts, fingerprints, split boundaries, baseline/position-pair honest metrics и pro future-only metrics с путями к `results.json`; отдельно зафиксировать любые convergence или unknown-hero gaps.

## Текущий запуск 05.09

- Run: `2026-09-05_position_pairs`; runner PID **54502**, monitoring PID **56173**. Оба пережили отсоединение (PPID=1).
- Лог: `runtime/artifacts/draft-cp/2026-09-05_position_pairs/run.log`; состояние: `status.json` рядом. Проверка: `cat runtime/artifacts/draft-cp/2026-09-05_position_pairs/status.json`.
- Канонические корпуса: `data/draft_phase_corpus/2026-09-05_position_pairs/{public,pro}/`; модели: `data/draft_phase_models/2026-09-05_position_pairs/`.
- Фон: `nohup` + отдельная POSIX-сессия + `disown`; при запуске дождаться status/monitor JSON до закрытия shell, иначе дочерний процесс может завершиться раньше detachment.
- Не-LLM монитор `scripts/ops/watch_draft_phase_training.py`: пульс в `monitor.json`, журнал в `monitor.log`; `codex queue --thread 01a06fef-d6a5-71b1-8327-81dd4ac3e076 --message ...` только на DONE/FAIL/STALL/UNREACHABLE. Команда проверена по installed `--help`; нормальный прогресс не вызывает модель. Тесты монитора: `base/tests/test_draft_training_monitor.py` — 2 passed.
- После DONE проверить четыре `model.joblib` и результаты reload, controls на тех же датах, фактические source cutoffs и калибровку. После FAIL сначала проверить `status.json`, лог и PID; не создавать второй экземпляр. Частичные сырьевые шарды переиспользуются по SHA; `--resume` переиспользует только завершённые обучения с совпадающим training identity.
- **Prod не переключать автоматически.** Формат Early NW теперь трёхклассовый и несовместим с прежним бинарным reader; пользователь разрешил обучение, подключение в live в этот запуск не входит.

## Корпуса собраны, fit начался

Сборка public завершена: **6 638 652** карты, диапазон UTC **2026-03-24 00:00:01 — 2026-09-04 00:18:14**; дубли/конфликты среди принятых карт отсутствуют. Early NW: Dire **1 950 286**, Radiant **2 226 990**, no_marker **2 461 376**; все принятые публичные карты имеют пригодную для этой цели разметку. Карты no_marker учат occurrence-компонент, direction учится только на маркерах; joint accuracy нельзя напрямую сравнивать со старой conditional accuracy 65.85%.

Сборка pro завершена: **1 151 356** карт после фильтров; одинаковых дублей **20 168**, конфликтующих ID исключено **2**. Early NW: unknown **856 122**, Dire **93 160**, Radiant **94 437**, no_marker **107 637**. Unknown не превращается в no_marker и исключается только из Early NW, сохраняя карты для win-целей.

Обучение началось 05.09 ~16:38 MSK. Первый реальный fit (baseline Early NW occurrence, 3 983 191 train rows, 16 764 columns, C=.001) сошёлся за 89 итераций / 35.8 с. Это свидетельство работоспособности и стоимости запуска, **не результат сравнения моделей**. Итоговые quality metrics ожидаются; монитор остаётся активным.
