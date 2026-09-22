# E-314 — сигнатурные герои и индивидуальные признаки убийств

## STATUS

DONE

Офлайн-обучение и проверка завершены. Универсальное улучшение не подтверждено;
production и действующие модели не менялись.

## SUMMARY

**OBSERVED.** Построен датасет из 16 523 карт, добавлены исходы 461 632 исторических
rich-карт. Все 16 523 пересечения источников прошли проверку времени, исхода и
identity; повторно они не считаются. Все 11 прежних массивов E-312 идентичны.
Обучены шесть вариантов для каждой из шести целей: 36 моделей с калибровками.
Победитель выбирался по calibrated log loss августа до расчёта сентября.

Точность при пороге 0.5 и log loss на прежних 17 сентябрьских днях.
«До» — ранее выбранная модель E-312, «после» — выбор E-314 по августу.
Для окон n — карты без ничьей; для команды n — строки сторон, 808 строк/404 карты.
Отрицательная ΔLL лучше. CI — парный bootstrap по UTC-дням, 1000 повторов.

| Цель | n | Выбранный вариант | Точность до → после | LL до → после | 95% CI ΔLL |
|---|---:|---|---:|---:|---|
| 5–15 | 361 | signature_individual | 60.39% → 61.22% | 0.65892 → 0.65644 | [-0.01373; +0.00737] |
| 10–20 | 384 | experience | 60.16% → 60.16% | 0.66738 → 0.66738 | [+0.00000; +0.00000] |
| 15–25 | 355 | recent_experience | 58.59% → 58.59% | 0.67477 → 0.67477 | [+0.00000; +0.00000] |
| 20–30 | 288 | signature | 55.56% → 55.90% | 0.68609 → 0.68110 | [-0.01086; +0.00124] |
| Команда ≥30 | 808 | signature | 63.61% → 60.15% | 0.64412 → 0.64958 | [-0.00074; +0.01045] |
| Карта ≥55 | 404 | experience | 62.87% → 62.87% | 0.62772 → 0.62772 | [+0.00000; +0.00000] |

**DERIVED.** Небольшие улучшения выбранных 5–15 (+0.83 п.п.) и 20–30 (+0.35 п.п.)
не отделяются от шума по парным интервалам LL и accuracy. Для команды ≥30
августовский выбор `signature` ухудшил accuracy на 3.47 п.п.; CI accuracy
[−5.78; −0.94] п.п. Его сравнение с простым baseline скрывало бы ухудшение
относительно прежнего выбранного `experience`.

Фиксированные ablation-сравнения показывают более узкий эффект:

- Для команды ≥30 individual против усреднённого signature: ΔLL −0.00657,
  CI [−0.01095; −0.00203]; accuracy +4.08 п.п., CI [+1.58; +6.31].
  Это свидетельство потери полезной информации при усреднении в данном рецепте.
- Для команды ≥30 полный набор против recent: ΔLL −0.01350,
  CI [−0.02072; −0.00683]; accuracy +2.10 п.п., но её CI [−0.62; +4.77] п.п.
  Оба сравнения проходят заранее заданный исследовательский критерий
  «CI LL ниже нуля без снижения observed accuracy».
- У `signature−recent` ни по одной цели CI LL не исключает ноль.
- Individual ухудшает LL относительно signature в 15–25 и 20–30:
  +0.01117 [0.00327; 0.01783] и +0.00756 [0.00094; 0.01440].
- Full/performance не выбран по августу ни для одной цели. Для команды ≥30
  его LL 0.63991 лучше прежнего experience 0.64412, но accuracy 62.38% ниже
  63.61%. Сентябрьский лучший вариант не назначается победителем задним числом.

**OBSERVED.** Покрытие по всем десяти игрокам всех 16 523 query-карт
(165 230 наблюдений, каждая сторона один раз): медиана истории игрок—герой
всего 2 игры; в 28.95% случаев история пары отсутствует. За 90 дней медиана
1 игра, отсутствует в 45.83%. У игрока в целом медиана 83 игры.
Это покрытие сохранённого корпуса, не фактическое число сыгранных игроком игр.
Первый feature oracle считает coverage только Radiant; итоговая полная оценка
обеих сторон отдельно сохранена в `coverage_all_players.json`.

**INFERRED.** Этот опыт не опровергает существование сигнатурных героев:
история пары часто неполна, а результат зависит от формы признаков и цели.
Увеличение числа признаков само по себе не обеспечивает лучшего прогноза.

## CHANGED

Новый `base/tools/kills_signature_research.py` дополняет E-312 блоками:

- `signature`: 78 усреднённых признаков (39 own +39 own−opponent).
- `signature_individual`: 390 признаков (по39 для каждого из10 игроков).
- `signature_performance`: те же390 плюс750 индивидуальных performance.

Все три сохраняют baseline+experience+recent. Три прежних контроля:
`baseline`, `experience`, `recent_experience`. DotaPlus XP исключён из новых fits;
старый массив сохранён только для совместимости и проверки E-312.
У `train_target` добавлены необязательные arms/comparisons, прежний вызов работает.

39 индивидуальных признаков: исторически назначенная роль, объём практики,
давность последней завершённой игры, частоты/доли за7/30/90дней; wins/counts/WR
игрока, героя и пары за всю историю и90дней, разности pair−player и pair−hero.
Строки упорядочены `(hero, account)` в соответствии с categorical draft baseline,
не по текущим ролям из результата карты. Account ID не является категорией.

Победы rich — `wins=int(didRadiantWin)`, не поле `winrates` с поминутными
оценками. DB владеет временем/identity/outcome пересекающихся карт; rich даёт
роль только при проверенном совпадении. Только `end < query.start`, включая
строгое исключение незавершённой и текущей карты. Историческая роль может быть
ошибочно размечена Stratz; текущая назначается по прошлым частотам.

Marginal WR: `(wins+5)/(n+10)`. Pair WR: `(pair_wins+10*player_wr)/(pair_n+10)`.
Нулевой count отличает prior0.5 от наблюдений. У performance пять pseudo-games
со средним игрока: pair mean, разность с player mean, pair count для25метрик.
Это nullable OpenDota K/D/A/GPM/XPM и4×deaths/gold/xp/lh/dn. Незавершённые
исторические окна цензурированы; gold_t — накопленное золото, не net worth.
Rich pstats исключён: producer `float(p.get(k) or 0.0)` теряет missingness.

После fit выявлено отсутствие явного thread cap в CatBoost predict_proba.
Теперь обе ветки inference явно используют `thread_count=1`; регрессионный stub
требует этот аргумент. Fit уже использовал1поток. Все36 старых прогнозов
независимо воспроизведены с `thread_count=1` с допуском1e-12; переобучения не было.
Замороженный training code остаётся в снимке кампании, не переписан задним числом.

## CHECKS

- 29 tests passed in2.45s из frozen-копии финальных четырёх code/test-файлов.
  Проверены strict end, lower90d, smoothing, ориентация, порядок игроков,
  current/future poison, реальные source-less DB records, конфликтные overlap,
  индивидуальные истории с одинаковым средним, три типа target и1thread inference.
- Независимая арифметика без History/Wins/rows: 25 query-карт,
  60 900 значений, max error0 для всех трёх блоков. Source loader общий;
  его ориентация отдельно проверена по producer и regression.
- 114 сохранённых файлов проверены по SHA; 36 наборов метрик и36 model/calibrator
  replay совпали с допуском1e-12. Все SQL labels совпали; bootstrap CI пересчитаны.
  Все18 прежних контрольных прогнозов E-312 неизменны, MID/side/y совпадают.
- Все7jobs train-кампании completed, snapshot_verified_after=true;
  один процесс/fit-thread, nice10. Независимый oracle был выполнен последним:
  scheduler сортирует job IDs, а не сохраняет порядок manifest. Приёмка — после него.
- Исходные ресурсы восстановлены владельцем E-313; проверено byte-for-byte,
  SHA0491d63bded2e28da52ff2eaae4b9d4a31eb74dc62f5a9575565270e4d3f3f44.
  E-314 сам resources.json не изменял. Файлы и неудачные прогоны не удалялись.

Автодоставка отказала в `task revise/ready`: параллельный commit E-313 изменил
HEAD относительно регистрации. Между baseline9080d992 и HEADa144ebc3 изменены
только документы E-313/индекс; код обучения не затронут. Автодоставка отменена,
запись E-313 сохранена, семь собственных файлов проверяются и коммитятся явно.
`supervised_scope_extension.json` фиксирует чистое исходное состояние добавленного
test-файла; frozen29tests и hashes связывают финальный код. Это не автоматический
READY/DONE receipt и не разрешение на deploy.

Первый build `run-a40f240c5a4975f8af70c322` остановился до обучения с
`KeyError: source`: `_db_maps` не возвращает source, fixture ошибочно его добавлял.
DB-событие теперь получает source явно, тест воспроизводит настоящий контракт.
Read-only review этот дефект не нашёл. Failed receipt сохранена; snapshot after
у неё verified. Исправленный build `run-88f8d6c42053a8a9c26a64fa` completed.
Train: `run-446df815d8b934ee82d702f2`, все7jobs completed, без повторения fit.

Фиксированный рецепт:400/depth5/lr.05/l2=12/seed20260921, early-stop40;
даты/калибровка не менялись, protocol.json создан до fit. Воспроизведение:

```sh
OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 venv_catboost/bin/python3 -m pytest base/tests/test_kills_signature_research.py base/tests/test_kills_opendota_research.py -q
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources preflight --plan runtime/artifacts/kills/signature_20260922/build_campaign_v2.json
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/kills/signature_20260922/build_campaign_v2.json --background
venv_catboost/bin/python3 runtime/artifacts/kills/signature_20260922/prepare_train.py
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources preflight --plan runtime/artifacts/kills/signature_20260922/train_campaign.json
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/kills/signature_20260922/train_campaign.json --background
OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 venv_catboost/bin/python3 runtime/artifacts/kills/signature_20260922/verify_models.py
```

Manifests привязаны к точным входным SHA; для нового запуска нужен новый manifest
с проверенными hash и ресурсами. prepare_train/train_run связывают конкретные
завершённые run IDs; команды выше документируют цепочку, не авто-retry старого fit.

**Где искать ошибку:** `source_events` — side/win/source/дедупликация;
`Wins` — границы90дней и strict end; `win_values` — priors/counts;
`rows` — соответствие игроку/герою; `_shrunk` — nullable means;
`augment` — прежние control arrays; `train_target` — August selection, calibration,
ориентация окон, current/default thread policy. Oracle разделяет проверку
математики и ограничения общего source loader.

## POINTERS

- Код/тесты: `base/tools/kills_signature_research.py`, `base/tests/test_kills_signature_research.py`;
  общий trainer `base/tools/kills_opendota_research.py` и его regression.
- Полный отчёт чисел, CIs, калибровки и всех arms:
  `runtime/artifacts/kills/signature_20260922/results.json`.
- Независимая проверка: `feature_verification.json`, `coverage_all_players.json`,
  `corrected_verification.json`, `verification.log`, `regression_final.log`
  в том же каталоге. `final_code_hashes.json` — код после ограничения inference.
- Датасет: `data/kills_signature_20260922/{dataset.npz,metadata.json}`.
- Модели: `ml-models/kills_signature_20260922/`, все6выбранных +36candidates.
- Источник rich: `runtime/experiments/misc/pro_corpus_rich.py:71–105,118–171`.
- Предыдущий эксперимент: [E-312](E-312-kills-recent-practice.md).

## RISKS

Сентябрь использован повторно: это retrospective, не свежий holdout.
17дней, без поправки на множественные сравнения. Raw WR зависит от силы команды
и соперников; разности с marginal не устраняют все confounders. Hero WR рассчитан
по наблюдаемому cohort игроков, не всему населению Dota. Корпус не покрывает
всю историю пабликов; median pair2 нельзя трактовать как реальный опыт игрока.
End-causal reconstruction не подтверждает историческое время публикации API.
Окна условны на отсутствии ничьей. Новые признаки не включают raw rich pstats,
поскольку пропуски уже необратимо смешаны с нулями. Production, прибыльность
и свежий prospective результат не проверялись.

## NEXT

Запрошенный офлайн-цикл завершён. Все варианты сохранены для дальнейшего
исследования; выбранные E-314 artifacts не заменяют действующие модели.
Сильный общий прирост не доказан. Для следующего независимого вывода нужны
свежие даты и более полная player-hero история с проверенным identity/outcome;
текущий сентябрь не подходит для нового подбора рецепта.

```orchestra-evidence-v1
{
  "schema": "orchestra-evidence-v1",
  "constraints": [
    "Offline kills models only",
    "One fit thread, lowered priority; no production",
    "No tuning on September or deletion of retained evidence"
  ],
  "sources": [
    {
      "id": "S1",
      "path": "runtime/artifacts/kills/signature_20260922/results.json",
      "sha256": "48db97597508af0251c1936d93a6e11160a9bac475a4e51d28141dd9840928dd"
    },
    {
      "id": "S2",
      "path": "runtime/artifacts/kills/signature_20260922/feature_verification.json",
      "sha256": "58600bd40b50e4d0ee12bfb221f3c7d74481494bb83a9c5c704b84c269c4e3bb"
    },
    {
      "id": "S3",
      "path": "runtime/artifacts/kills/signature_20260922/dataset_audit.json",
      "sha256": "1b3d5e21f73a66765f5246db2be06c1e93c12359c64f26f9e2591707d822ef4c"
    },
    {
      "id": "S4",
      "path": "runtime/artifacts/kills/signature_20260922/regression_final.log",
      "sha256": "3abd5549c9c623cdb7a8a63704ebbcb094c9ca770742be8e1f72090a367b9b4a"
    },
    {
      "id": "S5",
      "path": "runtime/artifacts/kills/signature_20260922/coverage_all_players.json",
      "sha256": "d49c429a1861bb867beb96141043aed5c6b43bca61ae90d07cd0908a4c336930"
    },
    {
      "id": "S6",
      "path": "runtime/artifacts/kills/signature_20260922/resource_restoration_observed.json",
      "sha256": "25e70c773847be192f75ec7afe45546426fba9ba07c327a3050fa9369c6f675f"
    },
    {
      "id": "S7",
      "path": "runtime/artifacts/kills/signature_20260922/corrected_verification.json",
      "sha256": "51391d5b3117b8ed9b079b892e7568dfefe2bc2b89e32492e075ea33269aebaa"
    }
  ],
  "claims": [
    {
      "id": "F1",
      "kind": "OBSERVED",
      "claim": "All36candidate predictions replay and18oldcontrols match within1e-12",
      "sources": [
        "S7"
      ],
      "scope": "Six E314 targets, saved terminal predictions"
    },
    {
      "id": "F2",
      "kind": "OBSERVED",
      "claim": "Independent arithmetic matched60900feature values with zero error",
      "sources": [
        "S2"
      ],
      "scope": "25query maps, three new blocks; shared source loader"
    },
    {
      "id": "F3",
      "kind": "OBSERVED",
      "claim": "Selected E314 side30 accuracy is lower than previously selected E312 by3.47pp",
      "sources": [
        "S1"
      ],
      "scope": "808side rows across404maps in17exposed September days"
    },
    {
      "id": "F4",
      "kind": "OBSERVED",
      "claim": "All-pair historical games median2, no observations in28.95percent",
      "sources": [
        "S5"
      ],
      "scope": "165230query-player observations; observed corpus only"
    },
    {
      "id": "U1",
      "kind": "NOT_CHECKED",
      "claim": "Prospective generalization and production profitability",
      "scope": "New dates and actual live delivery"
    }
  ],
  "checks": [
    {
      "id": "T1",
      "status": "PASS",
      "sources": [
        "S4"
      ],
      "observed": "29 passed in2.45s in frozen final copy"
    },
    {
      "id": "T2",
      "status": "PASS",
      "sources": [
        "S7"
      ],
      "observed": "114hashes,36metric/replay checks,18oldcontrols unchanged"
    },
    {
      "id": "T3",
      "status": "PASS",
      "sources": [
        "S6"
      ],
      "observed": "resources restored byte-for-byte to original0491d63..."
    }
  ],
  "limitations": [
    "No fresh holdout;17days and multiple comparisons",
    "Incomplete player-hero history",
    "Fit snapshots predate explicit inference-only thread cap; single-thread replay matches"
  ],
  "contradictions": [
    "August selected side30 aggregate signature underperforms previous experience on September; individual beats aggregate locally"
  ],
  "decision_required": []
}
```
