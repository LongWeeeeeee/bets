# E-306 — историческая causal-проверка длительности <=35 минут

## STATUS

Offline-эксперимент завершён 19.09.2026; production, shadow и runtime не
менялись. Результат — кандидат для дальнейшей отдельной serving-оценки, не
основание заменять существующий порог >=43 минут или включать новый сигнал.

## SUMMARY

**OBSERVED.** На фиксированных четырёх тестовых месяцах May–Aug 2026 (19 964
уникальные карты) draft-only logloss `0.654013`, causal history raw `0.603941`;
paired day-cluster Δ(raw−draft) `−0.050072`, 95% CI
`[−0.054806; −0.045388]`, 123 дня. Literal target — `duration_seconds <=2100`,
то есть карта ровно 2100 секунд положительна.

**OBSERVED.** Отдельный Platt, fitted только на предшествующем календарном
месяце каждого fold, не улучшил pooled raw: `0.604095` против `0.603941`,
Δ(Platt−raw) `+0.000155`, 95% CI `[−0.000598; +0.000946]`.

**DERIVED/INFERRED.** История даёт устойчивый retrospective-прирост над
draft-only; преимущество калибровки над raw не подтверждено. Это не
prospective/прибыльностная проверка и не сравнение с production928.

**NOT_CHECKED.** Реальная предматчевая доступность истории, одинаковый input
replay с serving, цены/прибыльность и будущая prospective cohort.

## DESIGN

- Source: `pro_corpus_rich.npz`, SHA256
  `db99dacb1ab9f2ab53f1126cbdcb6c926750fdc22c54a98e803290bc0b17ed5e`;
  duration is int32 seconds, target exactly `<=2100`.
- Folds exactly как E-299: `may` boundaries Mar/Apr/May/Jun, далее сдвигом до
  `aug` Jun/Jul/Aug/Sep. Train starts 2024-01-01; each split also requires
  `end_ts < its boundary`. Train / early-stop / calibration / test disjoint.
- Baseline: unsigned role draft encoder + CatBoost. Candidate adds 24
  end-time-causal aggregates over account and hero keys: past duration mean,
  past `<=2100` rate and count, each mean/std/min/max over ten slots. В отличие
  от E-299, нет past `>=36`/`>=43` label fields; history is target-specific,
  not described as target-agnostic.
- CatBoost fit uses one executor thread; fixed config 600/depth6/lr.05/l2=6,
  early stopping 80. Platt `C=1` learns only candidate calibration raw logits;
  no test labels enter fit.

| test month | train / early / calibration / test | draft trees | causal trees | draft LL | raw LL | Platt LL |
|---|---:|---:|---:|---:|---:|---:|
| May | 139478 / 6889 / 6866 / 6817 | 573 | 435 | .654671 | .609601 | .608705 |
| Jun | 146371 / 6866 / 6817 / 6922 | 517 | 587 | .654536 | .606919 | .606312 |
| Jul | 153242 / 6817 / 6922 / 4242 | 552 | 278 | .651608 | .594474 | .597429 |
| Aug | 160062 / 6922 / 4242 / 1983 | 569 | 576 | .655063 | .594338 | .594768 |

## CALIBRATION AND THRESHOLDS

Pooled calibrated bins are saved in `pooled.json`. At fixed threshold values
(`n`, realised positive rate, Wilson 95% CI):

| Platt threshold | n | realised <=2100 | Wilson 95% |
|---:|---:|---:|---:|
| .60 | 1141 | .8133 | [.7897; .8349] |
| .65 | 897 | .8562 | [.8317; .8776] |
| .70 | 803 | .8742 | [.8495; .8954] |

The saved calibrators reproduce a scalar sigmoid exactly (max absolute error
zero). Maximum component gradient of the mean C=1 objective was `2.82e-05`.
All calibration rows lie in their designated calibration month, all test rows
in their designated test month and end before its test boundary. sklearn
emitted matmul RuntimeWarnings (divide-by-zero/overflow/invalid) in every fold;
they are preserved in stderr. No ConvergenceWarning occurred; the manual
reproduction/gradient check is evidence for the saved finite outputs, not proof
that the warnings are harmless in another environment.

## >=.70 TAIL SANITY AUDIT

**OBSERVED.** The calibrated `causal_platt >=.70` tail joins exactly by `mids`
to the immutable corpus: 803 unique maps, 702 positives (`.8742`). It is not
dominated by ultra-short maps: zero maps are `<=5m` or `<=10m`, and 5 (`.62%`)
are `<=15m`; duration quantiles are p05 `1237s`, median `1758s`, p95 `2297s`
(min `842s`, max `3767s`).

| UTC test month | all OOF n | >=.70 n (share) | realised <=2100 | <=15m in tail |
|---|---:|---:|---:|---:|
| May | 6817 | 206 (3.02%) | .8738 | 1 |
| Jun | 6922 | 255 (3.68%) | .8980 | 4 |
| Jul | 4242 | 274 (6.46%) | .8504 | 0 |
| Aug | 1983 | 68 (3.43%) | .8824 | 0 |

**OBSERVED (contrary evidence).** The tail is substantially concentrated:
league ID `18959` accounts for 731/803 (`91.03%`) maps. It has 487 unique
account IDs and 127 hero IDs; top team IDs by slot occurrence are `9814269`
(189), `9878494` (183), `9426115` (159), `9768002` (158), and `10145429`
(142). IDs are retained deliberately: this audit does not assign identities
or causes to them.

**DERIVED/INFERRED.** The duration distribution rules out a tail explained
primarily by maps under ten minutes, but the observed quality cannot be
generalised beyond the highly concentrated league cohort.

**NOT CHECKED.** This corpus has no verified abandonment/void/settlement field
used here. Thus the audit cannot rule out other non-standard-map mechanisms,
nor establish bookmaker-settled win rate, odds availability or profitability.

## CHECKS

```sh
venv_catboost/bin/python3 -m py_compile base/tools/duration35_historical.py
venv_catboost/bin/python3 -m pytest base/tests/test_duration35_historical.py -q
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources preflight --plan runtime/artifacts/misc/duration35_20260919/fold_campaign.json
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/misc/duration35_20260919/fold_campaign.json --background
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources collect --run run-397b57aec1180b0ad68eadea
```

Final source checks: `6 passed`, py_compile exit 0, diff --check exit 0. Four
campaign jobs exit 0, each declared-files snapshot verified after run; 20/20
declared artifacts collected. The preflight used one enabled local slot and one
thread, `process_nice=0`; no resource exception was made. The campaign input
source SHA was `49b5a219...6884836c`. Afterwards the final source changed only
to repair the boolean overlap guard and pass explicit `thread_count` to inference
(`9d208c68...f377c707`); no refit was run. The fixed split inequalities are
pairwise disjoint; the test suite now exercises both the overlap guard and real
pairwise masks. Original inference used the CatBoost default thread count;
therefore the one-thread fit setting does not establish one-core inference.

## POINTERS

- Frozen campaign: `runtime/artifacts/misc/duration35_20260919/fold_campaign.json`.
- Pooled metrics/bins/CI/threshold samples: `runtime/artifacts/misc/duration35_20260919/pooled.json`, SHA256 `d1eb84e311fec8ffa4a614b37d4059645662c34ca9e98ddf9c04808672ccfe0d`.
- OOF predictions: `runtime/artifacts/misc/duration35_20260919/pooled_predictions.npz`, SHA256 `9344af5ebfc509b25e5c50ca491c4adcdb98e02e698b0c112a5216196e1c77bc`.
- Platt split/replay/gradient validation: `runtime/artifacts/misc/duration35_20260919/platt_validation.json`, SHA256 `5b4739c4982f2f47f19a9b3f41deeae548256e79bd358d1903b012c0bec0ecd8`.
- >=.70 duration/cohort concentration audit: `runtime/artifacts/misc/duration35_20260919/tail_audit_ge070.json`, SHA256 `b7e121a76ab8a8881c004c33541f247be5091b33423b4e612f3b5523542a4524`.
- Executor receipts and collected hash index: `.orchestra/campaigns/run-397b57aec1180b0ad68eadea/`.

## RISKS

The history assumes immediate availability at map end, while live source delay
is unmeasured. The shared rich corpus is historical and may not match serving
draft completeness. Retrospective test months have now been inspected, so do
not tune more variants on them; reserve a fresh prospective or held-out period.
The per-fold calibration result is heterogeneous, and the saved sklearn warnings
remain a portability risk despite finite/reproducible outputs. The >=.70 tail
is 91.03% league ID `18959`, so it is not evidence for a cross-league threshold.

## NEXT

Keep raw causal history as the historical reference. A lead must decide whether
to design a strict prestart/as-of capture and untouched prospective comparison;
do not add <=35 to serving or choose a betting threshold from this study alone.

```orchestra-evidence-v1
{
  "schema": "orchestra-evidence-v1",
  "constraints": [
    "Offline only; no production, shadow or runtime activation",
    "Literal target duration_seconds <=2100 inclusive",
    "Fixed E299 May-August temporal folds; no post-test variant search",
    "Platt fitted only on each fold calibration month"
  ],
  "sources": [
    {"id": "S1", "path": "runtime/artifacts/misc/duration35_20260919/pooled.json", "sha256": "d1eb84e311fec8ffa4a614b37d4059645662c34ca9e98ddf9c04808672ccfe0d"},
    {"id": "S2", "path": "runtime/artifacts/misc/duration35_20260919/platt_validation.json", "sha256": "5b4739c4982f2f47f19a9b3f41deeae548256e79bd358d1903b012c0bec0ecd8"},
    {"id": "S3", "path": ".orchestra/campaigns/run-397b57aec1180b0ad68eadea/collected-artifacts.json"},
    {"id": "S4", "path": "runtime/artifacts/misc/duration35_20260919/tail_audit_ge070.json", "sha256": "b7e121a76ab8a8881c004c33541f247be5091b33423b4e612f3b5523542a4524"}
  ],
  "claims": [
    {"id": "F1", "kind": "OBSERVED", "claim": "Four isolated folds completed with 19964 unique OOF maps; raw causal LL 0.603941 versus draft 0.654013.", "sources": ["S1", "S3"]},
    {"id": "F2", "kind": "OBSERVED", "claim": "Raw-to-Platt LL delta is +0.000155 and its 95% day-cluster interval includes zero.", "sources": ["S1"]},
    {"id": "F3", "kind": "OBSERVED", "claim": "Saved Platt outputs are finite and equal manual scalar sigmoid; calibration/test split and end-time checks pass.", "sources": ["S2"]},
    {"id": "F4", "kind": "OBSERVED", "claim": "The calibrated >=.70 tail has no maps <=10m, only 5 maps <=15m, and 91.03% of maps are league ID 18959.", "sources": ["S4"]},
    {"id": "D1", "kind": "DERIVED", "claim": "Raw causal history improves over the fixed draft baseline historically, while Platt advantage is unconfirmed.", "basis": ["F1", "F2"]},
    {"id": "D2", "kind": "DERIVED", "claim": "The tail is not primarily made of <=10m maps but is cohort-concentrated, so its observed rate is not cross-league evidence.", "basis": ["F4"]},
    {"id": "U1", "kind": "NOT_CHECKED", "claim": "Prospective prestart availability, serving parity, abandonment/void labels, bookmaker settlement and profitability.", "sources": ["S1", "S4"]}
  ],
  "checks": [
    {"id": "T1", "status": "PASS", "observed": "Final local py_compile and six duration35 regression tests pass."},
    {"id": "T2", "status": "PASS", "observed": "Resource preflight passed; four full jobs completed exit 0 with post-run snapshot verification; 20 artifacts collected."}
  ],
  "limitations": [
    "Retrospective months already observed",
    "sklearn RuntimeWarnings preserved",
    "No prospective/serving or economic evaluation",
    ">=.70 tail is 91.03% league ID 18959"
  ]
}
```
