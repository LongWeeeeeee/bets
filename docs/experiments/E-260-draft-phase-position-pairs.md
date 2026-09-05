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
