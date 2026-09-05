---
id: E-260
title: "Четыре draft-модели: пары с позициями и Early NW с картами без маркера"
date: "2026-09-05"
area: ml
status: full
corpus: "6 638 652 публичных карты >=20 мин; про-архив 1 151 356 карт, forward test 189–639; Early NW no_marker 2 461 376"
verdict: "Четыре модели обучены, parent prematch пересчитан, и 05.09.2026 подтверждена активация на serv1 (код ccf1873, restart 23:39:38 MSK). EarlyNW conditional direction, Late >=36 и All >=20 подключены к трём reader; EarlyWin 20..34 сохранён неактивным. Независимый pro uplift не подтверждён; историческая диагностика parent не является независимой проверкой качества."
harness: "scripts/run/retrain_draft_phases.sh; base/build_draft_phase_corpus.py; base/train_draft_phase_models.py"
---

# E-260 — четыре draft phase модели после исправления позиций в парах

**Обучение завершено 05.09.2026 в 19:37 MSK.** `status.json`: `DONE`, `stage=complete`; `summary.json`: `complete`. Все четыре position-aware модели сохранены после refit на полном соответствующем публичном корпусе. Активация на serv1 подтверждена: код `ccf1873`, restart `2026-09-05 23:39:38 MSK`, PID `3170124 -> 3233020`, unit active. Live model/card event после restart пока не наблюдался.

## Цели и исправления

| Модель | Фильтр фактической длительности | Target |
|---|---|---|
| `early_nw` | >=1200 с | Первый пороговый маркер в окне 20–28 мин: Dire / Radiant / no_marker |
| `late` | >=2160 с | Победитель карты |
| `all` | >=1200 с | Победитель карты |
| `early_win` | 1200–2040 с включительно | Победитель карты |

Длительность служит фильтром обучающей популяции, в признаки не входит. `early_win` оценивает победителя среди карт длительностью 20–34 минуты; это не вероятность закончить любую карту победой до 34-й минуты.

В `base/draft_features.py` добавлены признаки пары токенов `(position, hero)`: перестановка героев между позициями теперь меняет признаки synergy/counter. Общие hero, hero×role и пары без позиций сохранены для сглаживания; редкие позиционные пары требуют минимум 30 наблюдений. Старый `hero_role_pair` сохранён как контроль и для совместимости старых артефактов. Неизвестный герой больше не попадает по индексу -1 в чужой role-признак.

Сборщик проверяет 10 уникальных героев и ровно позиции 1–5 каждой стороны, строгие side/winner, duration/id/time; отбрасывает конфликтующие дубли целиком. Для NW проверяет полноту и конечность наблюдённого окна, отсекает значения после окончания карты. Missing/invalid networth отличается от отсутствия маркера. Кэш зависит от SHA256 источника и правил разметки; изменение источника во время чтения прерывает сборку. Сборка и сохранение используют temporary file + atomic replace.

## Карты без маркера

**Используются.** У Early NW два компонента: occurrence предсказывает `q=P(marker)` на всех корректно размеченных картах, direction — `p=P(Radiant | marker)` на картах с маркером. Выход `[q*(1-p), q*p, 1-q]` соответствует `[dire, radiant, no_marker]`. Occurrence использует признаки, инвариантные к смене сторон; direction — признаки со знаком.

Публичных no_marker **2 461 376**. Если карта закончилась между 20-й и 28-й минутами, проверяется окно до её фактического окончания. Отсутствующий/неполный/нечисловой ряд NW не превращается в no_marker: такие карты исключаются только из NW-задачи и могут учить win-модели. Для прежнего бинарного live reader экспортирован direction-классификатор: он возвращает P(Radiant | marker). Полный трёхклассовый bundle сохранён вместе с occurrence, который пока не подключён к live-выходу.

## Данные и разбиение

Public source: `bets_data/analise_pub_matches/json_parts_split_from_object`; pro source: `pro_heroes_data/json_parts_split_from_object`. Public: **6 638 652** карты за **24.03.2026 00:00:01 — 04.09.2026 00:18:14 UTC**, 99 JSON данных, 2 metadata-файла пропущены. Принятые дубли/конфликты: 0/0. NW counts: Dire 1 950 286, Radiant 2 226 990, no_marker 2 461 376.

Из pro-архива принято **1 151 356** карт; одинаковых дублей 20 168, конфликтующих ID исключено 2. NW counts: unknown 856 122, Dire 93 160, Radiant 94 437, no_marker 107 637. Размер исторического архива не равен размеру независимого свежего pro-теста. Все **225** ID, общие с публичным корпусом, удалены из pro-оценки. Про-карты в этом прогоне служат проверкой переноса; на них модели не дообучались.

Хронологическое разбиение по времени начала: train 60%, validation 20%, test 20%, одинаковые timestamps не разделяются. Vocabulary fit только на первых 60%; C выбирается на validation по log loss. Честная оценочная модель refit на первых 80% с тем же словарём. Test не используется для выбора C. Позиционная модель дополнительно refit на всех 100% с новым full-fit словарём.

| Модель | Весь корпус | Train | Validation | Test | Последний start_time честного fit (UTC) |
|---|---:|---:|---:|---:|---|
| Early NW | 6 638 652 | 3 983 191 | 1 327 730 | 1 327 731 | 13.08 01:57:04 |
| Late | 4 074 862 | 2 444 916 | 814 973 | 814 973 | 12.08 21:13:59 |
| All | 6 638 652 | 3 983 191 | 1 327 730 | 1 327 731 | 13.08 01:57:04 |
| Early Win | 2 009 564 | 1 205 738 | 401 913 | 401 913 | 13.08 07:51:10 |

**Оговорка о времени:** это split по start_time, без purge по моменту окончания карты. Непересечение ID и времён начала не доказывает доступность всех train-labels в реальном времени на самой границе. Для следующего строгого as-of/walk-forward замера нужен embargo по завершению матчей. На cutoff ещё не закончились 756 карт train+validation для Early NW/All, 1085 для Late и 145 для Early Win. Для NW это верхняя оценка задержки label: маркер может стать известен до окончания карты. Независимость соседних дней/серий также не гарантирована.

## Сравнение на одном публичном test

Контроль и кандидат переобучены на одинаковых новых данных, с одинаковыми split и target. Это **не сравнение со старыми числами прода на другом периоде**. AUC больше — лучше; log loss меньше — лучше.

| Модель / target | AUC: пары без позиций → с позициями | Δ AUC | Log loss: пары без позиций → с позициями | Accuracy с позициями |
|---|---:|---:|---:|---:|
| Early NW direction, только маркеры (n=828 966) | 0.718232 → 0.721135 | +0.002902 | 0.614310 → 0.612180 | 66.296% |
| Late winner | 0.626136 → 0.625656 | -0.000481 | 0.667899 → 0.668125 | 58.968% |
| All winner | 0.624061 → 0.624645 | +0.000584 | 0.666879 → 0.666708 | 59.173% |
| Early Win winner | 0.721012 → 0.722227 | +0.001215 | 0.603213 → 0.602377 | 67.371% |

NW joint 3-class log loss: **1.041587 → 1.040813**, accuracy **44.276% → 44.282%**. Эти числа нельзя сравнивать с бинарной direction accuracy. Occurrence AUC, напротив, ухудшился: **0.551371 → 0.548995**; способность определить сам факт маркера по драфту слабая.

Парная диагностика разницы log loss (`candidate - control`) с bootstrap по UTC-дням, 10 000 повторов, seed 260:

| Target | Δ log loss | 95% percentile interval | Дней с улучшением |
|---|---:|---|---:|
| NW joint | -0.000774 | [-0.000894, -0.000645] | 21/23 |
| NW direction | -0.002130 | [-0.002339, -0.001932] | 22/23 |
| NW occurrence | +0.000556 | [+0.000481, +0.000629] | 0/23 |
| Late | +0.000226 | [+0.000131, +0.000332] | 7/24 |
| All | -0.000171 | [-0.000274, -0.000071] | 17/23 |
| Early Win | -0.000837 | [-0.001131, -0.000559] | 20/23 |

Это диагностические интервалы одного периода, без гарантии независимости дней; они не подтверждают перенос на pro и не использованы для дополнительного подбора модели.

На validation позиции победили только в **Early NW и Early Win**; у **Late и All** validation предпочёл контроль. Небольшой положительный All test не отменяет этот результат. Выбранные C кандидата: NW occurrence .001 / direction .003, Late .001, All .001, Early Win .003. Для occurrence/Late/All минимум находится на нижней границе сетки `[.001, .003, .01, .03]`; оптимум ниже .001 не исследован. Поэтому модели нельзя называть полностью оптимизированными.

## Перенос на свежие pro-карты

Только карты после cutoff соответствующего честного fit, без public/pro ID overlap. На NW joint n=639, из них direction n=374.

| Target | Pro n | AUC: пары без позиций → с позициями | Log loss: пары без позиций → с позициями |
|---|---:|---:|---:|
| NW direction | 374 | 0.734375 → 0.729596 | 0.609161 → 0.612293 |
| NW joint | 639 | — | 1.034249 → 1.037626 |
| Late | 401 | 0.617823 → 0.606160 | 0.671521 → 0.677159 |
| All | 639 | 0.640178 → 0.628950 | 0.670660 → 0.675454 |
| Early Win | 189 | 0.758455 → 0.755767 | 0.600597 → 0.602890 |

На доступных свежих pro-картах **нет улучшения AUC/log loss ни у одной модели**. Когорты малы; это не доказательство универсального вреда позиций, но достаточная причина не объявлять преимущество для прода. При одинаковом top-20% coverage pro accuracy: NW joint 59.843% → 59.843%; Late 68.75% → 66.25%; All 64.567% → 62.992%; Early Win 81.081% → 81.081%. Это отбор по уверенности драфт-модели, не симуляция реальных STAR/odds-гейтов и не оценка доходности.

После последнего full-fit cutoff остаётся только 24 / 13 / 24 / 8 pro-карт для Early NW / Late / All / Early Win. Эти числа слишком малы для содержательной оценки финальных full-fit артефактов.

## Артефакты и проверка

- Модели: `data/draft_phase_models/2026-09-05_position_pairs/<phase>/hero_role_position_pair/model.joblib`.
- Честные оценочные модели: `<phase>/<design>/evaluation_model.joblib`, исходные test predictions: `evaluation_predictions.npz`.
- Результаты: `<phase>/<design>/results.json`, `evaluation_results.json`; общий `summary.json` в корне моделей.
- Корпуса и manifest: `data/draft_phase_corpus/2026-09-05_position_pairs/{public,pro}/`.
- Run evidence: `runtime/artifacts/draft-cp/2026-09-05_position_pairs/{status.json,run.log,monitor.json,monitor.log}`.
- Парная диагностика: `runtime/artifacts/draft-cp/2026-09-05_position_pairs/paired_loss_diagnostics.json`.

`evaluation_model.joblib` имеет `held_out_test=true`; `model.joblib` — **`held_out_test=false`**, поскольку fit использовал весь соответствующий корпус. Публичные test-числа выше принадлежат оценочной модели, а не full-fit файлу. В тренировочном reload probe у всех четырёх `max_delta=0`. Все fit завершились без ConvergenceWarning; trainer прерывает запуск при несходимости/неконечных коэффициентах.

Перед запуском профильная регрессия: **84 passed, 1 skipped**; монитор: **2 passed**. Это не весь репозиторный test suite. Проверялись position-aware признаки, unknown heroes, corpus/target boundaries, missing vs no_marker, dedup/conflicts, cache integrity, phase training и совместимость прежних readers.

Независимая проверка отдельным свежим процессом: **103/103 PASS** (`runtime/artifacts/draft-cp/2026-09-05_position_pairs/independent_verification.json`). Загрузка 4 full-fit и 8 evaluation artifacts, схемы классов, нормировка и конечность вероятностей, signed side-swap и occurrence invariance, отсутствие пересечения train/test ID, corpus hash и воспроизведение основных метрик сохранённых test predictions. Погрешность воспроизведения этих метрик — 0 в сохранённом отчёте. Дополнительная проверка: **28/28 PASS** (`inference_verification.json` рядом): все 8 evaluation artifacts на 200 реальных heldout-картах каждый совпали с NPZ (`max_delta=0`); пересчитаны pro forward-метрики, подтверждены 225 shared ID и 24 сравнения хэшей исходников. В обеих проверках прод не запускался и не переключался.

## Harness и воспроизведение

Сборка и fit выполнены локально: два процесса сборщика, последовательные LR fit, два BLAS threads, disk-backed CSR. Fit стартовал 16:38 MSK и завершился 19:37 MSK. Исходные JSON, старые модели и scratch не удалялись.

Основной harness (для повторного обучения нужен новый run name; долгий запуск — через `nohup` согласно runtime rules):

```bash
bash scripts/run/retrain_draft_phases.sh NEW_RUN_NAME
```

Точная конфигурация fit существующего корпуса:

```bash
/Users/alex/Documents/ingame/venv_catboost/bin/python3 -u base/train_draft_phase_models.py \
  --corpus data/draft_phase_corpus/2026-09-05_position_pairs/public \
  --pro-corpus data/draft_phase_corpus/2026-09-05_position_pairs/pro \
  --output-dir data/draft_phase_models/2026-09-05_position_pairs \
  --scratch runtime/artifacts/draft-cp/2026-09-05_position_pairs/sparse_scratch \
  --models early_nw late all early_win --threads 2 --resume
```

`--resume` повторно использует завершённые обучения только с совпадающим training identity; при изменении кода/данных нужен новый output. Парную диагностику можно повторить без fit:

```bash
/Users/alex/Documents/ingame/venv_catboost/bin/python3 runtime/experiments/draft-cp/analyze_position_pairs_completed.py
```

Регрессионная команда перед fit:

```bash
/Users/alex/Documents/ingame/venv_catboost/bin/python3 -m pytest \
  base/tests/test_draft_phase_training.py base/tests/test_draft_phase_corpus.py \
  base/tests/test_draft_position_pairs.py base/tests/test_train_public_draft_hero10_experiment.py \
  base/tests/test_late_win_model.py base/tests/test_early_nw_win_model.py -q
```

## Где искать ошибку и что остаётся

При неверных counts смотреть source stats/manifest, canonicalizer и whole-ID conflict rejection в `base/build_draft_phase_corpus.py`; при неверном target — окно и finite/complete checks, duration boundaries. При leakage — `chronological_cuts`, train-only vocabulary, shared-ID mask и реальный момент доступности исхода. При ошибке порядка классов — `DraftPhaseModel.classes_`/`predict_proba`. При расхождении метрик — сверять `evaluation_predictions.npz` и соответствующий evaluation artifact, а не full-fit `model.joblib`.

Следующие содержательные исследования: меньшие C для позиционных occurrence/Late/All на отдельном validation; отдельный выбор encoder для occurrence и direction NW; более крупная последующая pro-когорта с embargo по завершению карт и замером реального отбора. Generic pro fine-tune нельзя обещать как улучшение: предыдущие E-99/E-218 показали, что рост изолированной метрики не обязан улучшать финальный ML-стек.

Подключение в live выполнено и подтверждено preflight/postflight: все 4 модели × 512 реальных draft-кейсов, `maxdelta <= 1.11e-16`; три reader wrappers совпали на 512 кейсах; parent — 35 features / 6 branches. Drop-in задаёт `WIN_MODEL_DIR`, `EARLY_NW_MODEL_DIR`, `LATE_WIN_MODEL_DIR`; `PREMATCH_ARTIFACT` unset, используется default artifact с заменёнными весами и прежним snapshot. Server SHA: `a6cf61c8a09d5d4649d4f7f9fc787b7986cb811617b70f1dd919c9cb320bae4d`; backup: `base/_archive/backups/prematch_model_artifact_v3.npz.bak_20260905_draft_phases` (old SHA `a7c1...`). Runtime PID/env и свежий log checks PASS, Traceback/load failure не обнаружены. Live model/card event пока не наблюдался.

## Fingerprints этого запуска

```json
{
  "corpus_sha256": "cac0fdb31ff9c812bda556397633a314fcb507cd1461026a4d2f4c47c2be4902",
  "code_sha256": {
    "train_draft_phase_models.py": "5df4e3df6299a56d251f24e5f5a92dbee69fffcf7661e9a2a221076ef9f81991",
    "draft_features.py": "ffba45a61db5a9208c179139506a52d45f0db59578c1832175d5a77d894ee2e3",
    "draft_phase_model.py": "e0ff323a3d2c7be1f48263c8caaf63d5b46e7b0855a1ea4846e3db4b2a3ab88e"
  },
  "C_grid": [
    0.001,
    0.003,
    0.01,
    0.03
  ],
  "support": 30,
  "full_refit": true
}
```

Rule fingerprint обоих корпусов: `d68a26b101c733ffdbca62839d1797344c9499f2fdc1b9fe97f449d6393bd10c`. SHA отдельных raw sources сохранены в manifest.

## Замена продовых моделей 05.09.2026

По отдельному запросу пользователя заменены три действующих reader: Early NW — условное направление маркера, Late — победитель среди карт >=36 минут, All — победитель среди карт >=20 минут в составе prematch. Early Win (20–34 минуты) проверен и сохранён на serv1, активного reader для него нет. Калибровка и пороги оставлены прежними; предупреждение о несовпадении каталога с прежней калибровкой не подавлялось. Улучшение на pro не доказано.

Parent переобучен по четырём зависимым веткам (`full`, `no_org`, `no_account`, `no_account_no_org`); `pre_draft` и `rating_only` сохранены. В frozen train: 22 193 карты после удаления shared public ID; диагностический test: 25 841. Full AUC old 0.719248 → new 0.720957, log loss 0.611572 → 0.610344. Это историческая диагностика совместимости: новый public draft видел более поздние карты, поэтому эти числа не являются независимым forward-тестом нового стека.

При сборке заменены восемь массивов весов, 26 частей snapshot и calibration сохранены побайтово. Все 34 ZIP members локального и серверного артефактов идентичны; SHA ZIP-контейнеров различаются из-за metadata. Первичные предупреждения NumPy проверены независимым пересчётом old/frozen/new через non-BLAS `einsum`: метрики совпали (`maxdelta=0`), веса конечны.

Активные пути на serv1:

```text
/root/main/data/draft_phase_serving/2026-09-05_position_pairs/{all,early_nw,late}
/root/main/data/prematch_model_artifact_v3.npz
/etc/systemd/system/cyberscore.service.d/draft-phase-models.conf
```

Согласованные локальные источники ночной сборки (`runtime/artifacts/misc/prematch_weights_win120.npz`, `branch_weights.npz`, `prematch_model_artifact_v3_hybrid.npz`) заменены атомарно и проверены против новых весов. `PREMATCH_ARTIFACT` не переопределён: ночная доставка продолжает обновлять стандартный серверный путь. SHA отдельной calibration не изменена. Резервные копии локальных источников и серверного parent лежат в `base/_archive/backups/<filename>.bak_20260905_draft_phases`; прежние три каталога моделей сохранены. Для отката необходимо восстановить согласованный набор: parent, три каталога в drop-in и локальные источники ночной сборки, затем `daemon-reload` и штатный systemd-рестарт с очисткой `map_id_check.txt`.

Отчёты в `runtime/artifacts/draft-cp/2026-09-05_deploy/`: `prematch_refit.json`, `prematch_merge.json`, `local_nightly_sources.json`, `server_deployment_state.json`, `server_postflight.json`, `server_prematch_merge.json`, `server_runtime_check.json`. SHA старого parent, настройки для отката, PID и время рестарта сохранены в `server_deployment_state.json`. Логи refit/merge лежат рядом. Последняя проверка реального PID/env: 23:43 MSK, active, обе отдельные модели enabled по умолчанию, новых Traceback/load failures нет; живой model/card event ещё не наблюдался.

Воспроизведение parent-refit (выходы должны быть новыми; исходные веса после активации берутся из backup):

```bash
venv_catboost/bin/python3 base/tools/refit_prematch_draft_component.py \
  --matrix runtime/artifacts/misc/win_model_base_matrix.npz \
  --weights base/_archive/backups/prematch_weights_win120.npz.bak_20260905_draft_phases \
  --compact runtime/artifacts/misc/pro_corpus_compact.npz \
  --public-corpus data/draft_phase_corpus/2026-09-05_position_pairs/public/rows.npz \
  --draft-model data/draft_phase_serving/2026-09-05_position_pairs/all/model.joblib \
  --output data/draft_phase_serving/RECHECK_weights.npz \
  --report runtime/artifacts/draft-cp/2026-09-05_deploy/RECHECK_refit.json
```

Проверка активных моделей без запуска live pipeline и без отправки сообщений:

```bash
ssh serv1 '/root/main/venv/bin/python3 - /root/main/data/draft_phase_serving/2026-09-05_position_pairs /root/main/runtime/artifacts/draft-cp/2026-09-05_deploy/recheck.json /root/main/data/prematch_model_artifact_v3.npz' \
  < runtime/experiments/draft-cp/verify_phase_deployment_20260905.py
```

**Где искать ошибку:** несовпадение прогноза — SHA файлов/ширина encoder и `verification_probe.npz`; отказ parent — `baseline_identity`, `inputs` и `rows` refit-отчёта; повреждение сборки — `output_member_sha256` merge-отчётов; старый результат после доставки — реальные env/PID процесса и singleton cache (требуется restart). Сравнение AUC parent выше нельзя использовать как доказательство будущего pro uplift. Ночная сборка должна брать новые top/branch weights одновременно, иначе рассогласуются ветки и `draft_logit`.
