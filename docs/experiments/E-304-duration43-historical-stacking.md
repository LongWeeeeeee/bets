# E-304. Историческое ≥43: причинные прогнозы фаз и других временных диапазонов

19.09.2026; Beads ingame-xpi0. Пользователь отменил план shadow E-302:
«зачем теневой делай на исторических данных». Производственный сбор не включается.

## STATUS

DONE

Подготовка и все четыре исторических fold завершены; прогнозы и калибровка
проверены. Прирост от добавления фазовых прогнозов не подтверждён.

## SUMMARY

OBSERVED: 19964 уникальных test-карты,123 UTC-дня. Меньший logloss лучше.

| Вариант | Raw logloss | Platt logloss | Brier после Platt | AUC после Platt |
|---|---:|---:|---:|---:|
| E299 causal baseline | 0.611233 | 0.610343 | 0.212902 | 0.640884 |
| + Early NW / Early Win / All / Late | 0.611317 | 0.610310 | 0.212867 | 0.641469 |
| + фазы, новые winner bands и прямые пороги | 0.611242 | 0.610889 | 0.213136 | 0.639103 |

Paired delta Platt logloss candidate−baseline:

- Четыре фазы: −0.00003271;95% CI [−0.00074326;+0.00070208];
  99% CI [−0.00095414;+0.00089911].
- Расширенный: +0.00054591;95% CI [−0.00027913;+0.00139348];
  99% CI [−0.00054577;+0.00165731].

| Test месяц | N | Baseline Platt | +4 фазы | Расширенный |
|---|---:|---:|---:|---:|
| May | 6817 | 0.617274 | 0.617259 | 0.616972 |
| June | 6922 | 0.605328 | 0.604547 | 0.606043 |
| July | 4242 | 0.603840 | 0.604368 | 0.604656 |
| August | 1983 | 0.617932 | 0.619251 | 0.620227 |

DERIVED: четыре фазы практически равны baseline, знак меняется по месяцам;
расширенный вариант хуже в среднем, хотя его CI тоже включает ноль.
В этом фиксированном протоколе усложнение не обосновано. Это не доказательство,
что любые другие фазовые модели бесполезны.

Проверяется прибавка к воспроизводимому baseline E-299, а не восстановление
утраченных 928 исторических входов production. Используются те же четыре
test-месяца May–Aug 2026, train/early-stop/calibration раздельны; baseline
не переобучается: прогнозы сохранённой модели пересчитываются и должны совпасть
до 1e-12 с E-299. Эти месяцы уже просмотрены, это историческая ablation,
не новый untouched holdout. September05–18 тоже уже использован E-298.

Вспомогательные draft-модели дают out-of-time признаки: для строк 2024 года
учатся только на завершённых картах 2023, для 2025 — 2023–24, для 2026 —
2023–25, строго end < Jan1. Никакие сегодняшние full-fit phase artifacts
не применяются к прошлым обучающим строкам. Это pro-only исторические аналоги
Early NW/Early Win/All/Late, а не точные копии public-trained serving-моделей.

Протокол до результатов:

- Контроль: E-299 causal history raw и Platt.
- Primary candidate: тот же baseline + уверенность/согласованность четырёх
  фаз, плюс вероятность появления NW-маркера.
- Secondary candidate: дополнительно winner-бэнды 34<минуты<43 и ≥43,
  прямые P(длительность≥32/36/40/43/47).
- Aux: fixed logistic C=.03, signed/unsigned hero_role_pair; duration:
  CatBoost iterations600, depth6, lr.05, l2=6, seed299, early-stop80, как E-299.
- Platt C=1 на отдельном calibration-месяце, raw показывается отдельно.
  Primary metric: paired calibrated logloss; AUC/Brier вторичны.
  Bootstrap по UTC-дням5000, CI95/99. Подбор вариантов по test запрещён.

OBSERVED: canonical pro NW corpus содержит raw-derived метки -1 missing,
0 Dire,1 Radiant,2 valid no-marker. Его join с rich по ID даёт1150798 общих
карт без конфликтов ts/duration/winner/heroes. Missing label не заменяется
нулём или no-marker; padded `rich.nw` не используется. Новые признаки в
строке карты используют только draft и предшествующие результаты, а не её
реальное время окончания, победителя или NW.

## CHANGED

`base/tools/duration43_historical.py`: два этапа — prepare и fold. Первый
строит причинную историю и ежегодные auxiliary модели; второй перепроверяет
baseline и обучает две заранее выбранные надстройки. Все артефакты остаются
offline. `base/tests/test_duration43_historical.py` проверяет strict end-time,
точность join, missing NW, side-invariance и границы диапазонов.

E-302 помечен как отменённый план активации; код default-off сохраняется,
никакого удаления артефактов и изменения production нет.

## CHECKS

9 tests PASS (4 новых,5 end-time regressions):

```sh
venv_catboost/bin/python3 -m pytest base/tests/test_duration43_historical.py base/tests/test_end_time_prior.py -q
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources preflight --plan runtime/artifacts/misc/duration43_historical_20260919/prepare_campaign.json
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/misc/duration43_historical_20260919/prepare_campaign.json --background
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources preflight --plan runtime/artifacts/misc/duration43_historical_20260919/fold_campaign_dense.json
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/misc/duration43_historical_20260919/fold_campaign_dense.json --background
venv_catboost/bin/python3 runtime/experiments/misc/duration43_historical_aggregate.py --run run-0ba4aca81b9fbaddfeeba6a0 --output-dir runtime/artifacts/misc/duration43_historical_20260919
venv_catboost/bin/python3 runtime/experiments/misc/duration43_replay_check.py run-0ba4aca81b9fbaddfeeba6a0
venv_catboost/bin/python3 runtime/experiments/misc/duration43_calibration_check.py run-0ba4aca81b9fbaddfeeba6a0
venv_catboost/bin/python3 runtime/experiments/misc/duration43_auxiliary_metrics.py
```

Ресурсы: только local enabled;4/4 fold jobs completed,0 failed/unknown в
исправленном run,568.76s суммарного wall time jobs. Exit0, post-run input
hash verification true во всех receipts;16 заявленных артефактов collected.
Исходный resource config побайтно восстановлен. Все восемь моделей точно
воспроизводят test и calibration predictions; calibration mids/y совпали
с календарными масками. Параметры моделей соответствуют frozen протоколу.

Sklearn Platt выдавал RuntimeWarnings о matmul. Они сохранены в stderr,
не скрыты. Ручной expit воспроизводит predictions точно; max component
gradient≤9.953e-5, gap к устойчивому scalar-sum optimum≤5.106e-8.
Диагностическое уточнение того же calibration objective меняет test logloss
каждого fold не более2.735e-6; вывод не меняется. Исходные прогнозы не заменены.

Подготовка `run-9de1771a460da0b5bb41d2a3`: exit0,64s, snapshot verified.
232060 строк; NW join225158, conflicts0. Независимый replay всех annual
моделей: max error≤2.981e-8 (float32), всех четырёх E299 baseline: error0.

Первый fold run `run-7d4816f971c4abd05e65106f`, aug: exit-9 после23s,
без результатов. macOS kernel18:31:34: `killing largest compressed process
Python [49903] 79749 MB`. Входная CSR finite/canonical,160062×805,81MB.
Те же значения передаются dense float32; параметры/семантика не меняются.
Старый неуспешный receipt сохраняется. После повторных CPU-блокировок
пользователь явно разрешил этому прогону одно ядро с nice10. Исходный
resources.json сохранён и восстановлен после окончания кампании.
Чужие процессы не останавливались.

## POINTERS

- Frozen plan: `runtime/artifacts/misc/duration43_historical_20260919/protocol.json`;
  SHA256 `68273b1504a042bf4da2e83ea3fd6b02f9e6a2cb6fb340f115e590161f83b571`.
- `runtime/artifacts/misc/duration43_historical_20260919/prepare_campaign.json`.
- Исполняемый fold manifest: `runtime/artifacts/misc/duration43_historical_20260919/fold_campaign_dense.json`.
- Подготовка: `.orchestra/jobs/run-9de1771a460da0b5bb41d2a3/prepare/output/`.
- Новый fold run: `.orchestra/jobs/run-0ba4aca81b9fbaddfeeba6a0/<fold>/output/`.
- Prepared features SHA256 `f3781340121af34f52ade8a633cfbea915131572e30cc10fa36cd1aec2d330c1`.
- `runtime/artifacts/misc/duration43_historical_20260919/auxiliary_metrics.json`: raw метрики разных условных целей; не сравнивать AUC целей между собой.
- Dense baseline parity: `runtime/artifacts/misc/duration43_historical_20260919/dense_baseline_parity.json`: ошибка0 на всех19964 картах.
- Итог: `runtime/artifacts/misc/duration43_historical_20260919/pooled.json` и `pooled_predictions.npz`.
- Проверки: `replay_check.json`, `calibration_check.json`, `tests.log` в том же каталоге.
- Failed-run kernel log: `failed_job_system.log`; receipt первого run сохранён.
- Runtime helpers и точные версии входов сохранены также в executor input snapshots;
  запуск требует этих локальных данных, одного Git checkout недостаточно.
- Rich source `runtime/artifacts/misc/pro_corpus_rich.npz`.
- NW source `data/draft_phase_corpus/2026-09-05_position_pairs/pro/rows.npz`;
  target logic `base/build_draft_phase_corpus.py:138`, sentinel validation там же.
- E299 baseline artifacts `.orchestra/jobs/run-2dcf60a5ff61532186ec2781/<fold>/output/`.

## RISKS

Где искать ошибку: end==boundary, источник NW и padding, сопоставление IDs и
порядка ролей, утечка fit в год прогнозирования, побочный Radiant intercept,
NaN признаков до начала auxiliary периода, отступление от E299 splits,
выбор лучшего варианта по уже просмотренным тестам. История моделирует конец
карты, но не задержку получения данных. Ас-of достоверность исторических
позиций/аккаунтов не подтверждена внешним архивом. Годовые auxiliary модели
меньше текущих public-trained фазовых моделей. Даже улучшение logloss не
доказывает прибыльность или готовность к замене serving.

## NEXT

Исторический эксперимент завершён. Для этого семейства сохранить более
простой causal baseline E299; не заменять serving на основании E304.
Новых переборов по этим же test-месяцам не запускать. Shadow не включается.

```orchestra-evidence-v1
{
  "schema": "orchestra-evidence-v1",
  "constraints": [
    "Historical offline only; no shadow activation or production changes",
    "Fixed May-Aug protocol; no test-based tuning",
    "User approved one CPU with nice10; original resource settings restored"
  ],
  "sources": [
    {
      "id": "S1",
      "path": "runtime/artifacts/misc/duration43_historical_20260919/pooled.json",
      "sha256": "0d74b5c9fef1f048108e6f8b4402cb4e284987fa0e8ec15ad65d3b899eb1f9cd"
    },
    {
      "id": "S2",
      "path": "runtime/artifacts/misc/duration43_historical_20260919/replay_check.json",
      "sha256": "dd1408653d3e781b8c341624cd9e217ecc1fb551585a6f4f3fa40101bc1ad6d5"
    },
    {
      "id": "S3",
      "path": "runtime/artifacts/misc/duration43_historical_20260919/calibration_check.json",
      "sha256": "c3a5aea087c69427ea7a2a404e3a6e1a6bd3023c63b21e133c9fba6d72a16985"
    },
    {
      "id": "S4",
      "path": "runtime/artifacts/misc/duration43_historical_20260919/tests.log",
      "sha256": "f3cbc82d98d22c1f832292118c177aba7f5b370b92ceb0d0706ef661fec82bbe"
    },
    {
      "id": "S5",
      "path": "runtime/artifacts/misc/duration43_historical_20260919/protocol.json",
      "sha256": "68273b1504a042bf4da2e83ea3fd6b02f9e6a2cb6fb340f115e590161f83b571"
    },
    {
      "id": "S6",
      "path": "runtime/artifacts/misc/duration43_historical_20260919/dense_baseline_parity.json",
      "sha256": "41ff08bd82fee51acfa9b3f4221c93764b6380d37c444de1dc5918f0396211c0"
    }
  ],
  "claims": [
    {
      "id": "F1",
      "kind": "OBSERVED",
      "claim": "All four folds completed: 19964 unique maps. Baseline Platt LL 0.6103429346; phases 0.6103102250; expanded 0.6108888492.",
      "sources": [
        "S1"
      ]
    },
    {
      "id": "F2",
      "kind": "OBSERVED",
      "claim": "Both paired 95 and 99 percent day-bootstrap intervals include zero.",
      "sources": [
        "S1"
      ]
    },
    {
      "id": "D1",
      "kind": "DERIVED",
      "claim": "No demonstrated improvement from the two preregistered phase additions.",
      "basis": [
        "F1",
        "F2"
      ],
      "method": "Compare fixed primary paired Platt logloss and sign consistency across months."
    },
    {
      "id": "F3",
      "kind": "OBSERVED",
      "claim": "Eight candidate test/calibration replays and four dense baseline replays have zero max prediction error; nine tests pass.",
      "sources": [
        "S2",
        "S4",
        "S6"
      ]
    },
    {
      "id": "F4",
      "kind": "OBSERVED",
      "claim": "Calibration outputs finite; stable scalar-objective diagnostic changes each test fold logloss by at most 0.000002735.",
      "sources": [
        "S3"
      ]
    },
    {
      "id": "U1",
      "kind": "NOT_CHECKED",
      "claim": "Equal-input superiority to the production 928-feature model and profitability.",
      "scope": "Current serving model and betting odds"
    }
  ],
  "checks": [
    {
      "id": "T1",
      "status": "PASS",
      "sources": [
        "S2",
        "S6"
      ],
      "observed": "Exact prediction replay"
    },
    {
      "id": "T2",
      "status": "PASS",
      "sources": [
        "S4"
      ],
      "observed": "9 passed"
    },
    {
      "id": "T3",
      "status": "PASS",
      "sources": [
        "S3"
      ],
      "observed": "Finite calibrators and bounded numerical sensitivity"
    }
  ],
  "limitations": [
    "Previously inspected retrospective holdouts",
    "Pro-only auxiliary analogs, not current full public-trained phase models",
    "Historical ingestion delays and original role availability are not established",
    "No ROI or production model replacement evidence"
  ],
  "contradictions": [
    "Small pooled phase gain lacks statistical support and monthly sign consistency",
    "Successful jobs emitted sklearn numerical warnings; retained and quantified",
    "Initial CSR run failed due OS memory kill; successful run uses dense identical feature values"
  ],
  "decision_required": []
}
```
