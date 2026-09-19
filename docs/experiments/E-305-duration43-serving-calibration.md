# E-305 — простая ≥43: калибровка, пороги и serving

## STATUS

Выкатка выполнена 19.09.2026: commit `f362c599`, serv1 fast-forward,
штатный systemd stop/clear-map-cache/start в 16:49:05 UTC. Новый PID1088646,
один процесс, NRestarts0. Модель загрузилась в естественном цикле с ожидаемым
SHA, новые завершённые карты записываются в durable history. Фактические
счётчики циклов и наблюдений — в `server_health.json`/`deployment.json`
под `runtime/artifacts/misc/duration43_serving_20260919/`.
Новая duration-строка в естественной Telegram-карточке пока не наблюдалась.

## SUMMARY

OBSERVED: пользователь выбрал простую E299-модель вместо фазового stacking.
Весовой артефакт августовского fold неизменён: обучение до июня 2026,
early stopping в июне, отдельный Platt в июле (4242 карты), test в августе.
Калибратор не переобучен на просмотренных тестовых исходах.
1983 августовских предсказания воспроизведены точно: max absolute error 0
для raw и Platt. Параметры Platt: a=0.9501570089531581,
b=-0.08547214780081874; sigmoid(a*logit(clipped_raw)+b), clip=1e-6.

OBSERVED: четыре исторических refit May–Aug, 19964 карты / 123 UTC-дня:

| P(≥43) от | Успехи / N | Факт WR | 95% CI, bootstrap по дням |
|---|---:|---:|---:|
| 50% | 926 / 1807 | 51.25% | 48.63–53.86% |
| 55% | 361 / 674 | 53.56% | 49.43–57.57% |
| 60% | 125 / 215 | 58.14% | 51.94–64.25% |
| 65% | 33 / 52 | 63.46% | 51.06–76.47% |
| 70% | 5 / 10 | 50.00% | 15.33–85.71% |

Порог 60% по месяцам: May 33/55; June 30/52; July 58/100; August 4/8.
Это совокупность четырёх прошлых обучений, не 215 независимых проверок именно
выкатываемых августовских весов. Верхняя часть шкалы слегка завышена:
для ≥60 средний прогноз 63.44%, факт 58.14%. Выше 70% данных почти нет.

DERIVED: арифметический кэф безубытка для наблюдаемого ≥60-cohort —
1/0.581395=1.72; по нижней границе day-bootstrap — около 1.93.
Условие P≥60 и кэф≥2.0 можно рассматривать только как исследовательский
фильтр. Доступность таких цен, ROI, маржа/исполнение и устойчивость после
выбора порога НЕ проверены. Проверенного порога для реальных ставок нет.

## CHANGED

- `duration43_serving.py`: 762 draft + 32 causal history; вместо старого dur43
  в панели. Другие модели панели и betting dispatch не меняются.
- Начальный снимок: 232622 завершённых карты с 2023-01-01, максимальный end
  1789775316; availability/built_at 1789826674.528845. Статистика — сумма
  длительности, >=43, >=36 и количество по игрокам/героям; smoothing20.
- Дополнения из pro STRATZ results сохраняются независимо от трёхдневного
  prune общей delta; одна первая полная версия match_id. Допуск: end>
  snapshot max_end, end<asof, observed_at<asof; текущая карта исключена.
- ELO evaluation timestamp не гарантирует actual start. Новый прогноз
  создаётся только при отрицательном game clock; сохраняется по model+input
  hash и переносится в live-карточки того же драфта. Первое наблюдение уже
  начавшейся карты не выдаёт выдуманную предматчевую вероятность.
- Карточка: именно P(длительность>=2580), а не max(p,1-p); подпись
  «предматчевая», фактическая доля >=43 в OOF-бине и N. Без зелёной
  рекомендации/автоматической ставки. Версии и asof попадают в journal.
- `DURATION43_SERVING=0` возвращает старый скрытый dur43 scorer; другие
  env: `DURATION43_MODEL_DIR`, `DURATION43_HISTORY`, `DURATION43_PREDICTIONS`.

## CHECKS

Harness: `base/tools/audit_duration_confidence.py`, seed305, 5000 bootstrap
resamples UTC-дней; пороги фиксированы 0.50/0.55/0.60/0.65/0.70.

```sh
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 venv_catboost/bin/python3 base/tools/audit_duration_confidence.py --predictions runtime/artifacts/misc/duration43_historical_20260919/pooled_predictions.npz --output runtime/artifacts/misc/duration43_serving_20260919/confidence.json
venv_catboost/bin/python3 base/tools/build_duration43_serving.py --snapshot ml-models/duration43_shadow_20260919_v2 --confidence runtime/artifacts/misc/duration43_serving_20260919/confidence.json --output ml-models/duration43_production
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 venv_catboost/bin/python3 -m pytest base/tests/test_duration43_serving.py base/tests/test_duration43_shadow.py base/tests/test_ml_panel.py base/tests/test_prematch_live_delta.py base/tests/test_prematch_panel_scorer.py -q
```

Builder требует новый output-каталог. Уже собранный пакет не перезаписывать
для воспроизведения — задать другой путь. 133 focused tests PASS после review, включая два процесса, partial append,
сохранение предматчевого прогноза после рестарта и изоляцию ошибки duration.
Объединённый набор с ≤35:139 PASS. На серверном Python3.12: py_compile,
imports,11 serving tests PASS; raw/Platt совпали с Mac до1e−12. Для
серверных тестов нужен `PYTHONPATH=base:.` (первый сбор без него завершился
import error). Production acceptance — в deployment receipt.

## POINTERS

- `runtime/artifacts/misc/duration43_serving_20260919/confidence.json`
- `runtime/artifacts/misc/duration43_serving_20260919/replay.json`
- `ml-models/duration43_production/manifest.json` содержит копию confidence
  evidence, source hashes и параметры serving.
- CBM SHA256: `b21442378c3c96d3a80a4b43630b08f906c4ecb556186b466009bbd3540f692c`.
- Исторический recipe/splits: [E299](E-299-duration43-causal-rolling.md).

## RISKS

Где искать ошибку: граница >=2580 секунд; сравнение именно положительной
вероятности; не смешивать bin и cumulative threshold; независимость July
calibration от August test; same-ID dedup; end и локальный observed_at;
полнота 10 героев; привязка позиций и неизвестные аккаунты; прерывание
history ingestion. Исторический корпус предполагает availability по end,
а не доказывает источник fetch timestamp. Новые history-дополнения ограничены
полнотой результата, который приносит действующий collector; это не гарантия
покрытия всех профессиональных карт мира.

NOT_CHECKED: превосходство над old928 при одинаковых live-входах, будущий
винрейт, прибыльность и доставка новой строки в естественной Telegram-карточке.
E300 давал худший кандидат на несовершенной live-подвыборке 339 карт;
это ограничение не снимается калибровкой. Поля metadata — расширение journal4;
старые поля и другие модели сохраняются.

Отдельное наблюдение: `_ml_dispatch_tick` падает при сортировке `None` и
строки в skipped dedup view (`cyberscore_try.py:12894`). Та же ошибка была
89 раз в2MB лога непосредственно ДО рестарта; соответствующий код не менялся.
Зафиксировано в `dispatch_existing_error.json`, отдельная задача `ingame-o66k`.
Это ограничивает общую health-оценку: новая модель загружена, но отсутствие
ошибок всего runtime не заявляется. Диспетчер в рамках E305 не исправлялся.

## NEXT

Следующее свидетельство качества — естественные сохранённые предматчевые
прогнозы с последующими исходами; исторические пороги не являются доказанной
доходной политикой. ≤35 — отдельный E306, без автоматической промоции
его результата в production.

```orchestra-evidence-v1
{"schema":"orchestra-evidence-v1","constraints":["User chose simple >=43 with calibration","No duration betting dispatch","Historical <=35 is a separate experiment"],"sources":[{"id":"C","path":"runtime/artifacts/misc/duration43_serving_20260919/confidence.json"},{"id":"R","path":"runtime/artifacts/misc/duration43_serving_20260919/replay.json"}],"claims":[{"id":"F1","kind":"OBSERVED","claim":">=60 gives 125/215, day CI .5194-.6425; August only 4/8","sources":["C"]},{"id":"F2","kind":"OBSERVED","claim":"1983 August raw and calibrated predictions replay exactly","sources":["R"]},{"id":"U1","kind":"NOT_CHECKED","claim":"Profitability and future calibration","scope":"Production duration bets"}],"checks":[{"id":"R1","status":"PASS","observed":"Raw and calibrated replay max absolute error 0","sources":["R"]}],"limitations":["Pooled four historical refits, not independent future validation","No matched odds","No equal-input incumbent comparison"]}
```
