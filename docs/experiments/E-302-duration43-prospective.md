# E-302. Проспективное парное сравнение P(карта ≥43 минут)

19.09.2026; продолжение E-300 по «делай честно»; Beads ingame-xpi0.

## STATUS

BLOCKED

Позднее 19.09 пользователь отклонил теневой запуск и выбрал исторические данные.
План активации отменён; продолжение — E-304. Режим остаётся выключенным.

Локальная реализация и замороженный пакет готовы. Производственный сбор не
включён, реальных новых наблюдений 0. Требуются отдельное включение на serv1
с перезапуском и затем будущие карты; качество кандидата пока не измерено.

## SUMMARY

OBSERVED: E-300 не позволил честно сравнить предматчевые вероятности:
0 записей до старта, нет снимков входов. E-302 сохраняет обе вероятности,
точный 928-вектор incumbent, вектор кандидата, общий порядок 10 heroes/accounts,
позиции R1–5/D1–5, пропуски, времена и SHA моделей/манифеста.

OBSERVED: пакет `ml-models/duration43_shadow_20260919_v2` собран без обучения.
Манифест SHA256 `c25b7a4f2724460f71917ad783fd3107b56e41dfca7f4bc34be82aa3fd9959d7`.
232622 завершённые карты в замороженной истории, 32 проверки паритета encoder.
Кандидат — точные веса E-299 August: обучение до июня, early stopping в июне,
Platt в июле; CBM/encoder/calibrator закреплены SHA в builder. Incumbent —
`dur43.cbm` с SHA `80bb42c8f141a54b0804d85c26a07c0618c790143bb5747aa5e23ca2499747d8`.
Значения history зафиксированы при сборке; это отличается от обновляемых
end-time history в rolling E-299. Улучшение E-299 не переносится сюда автоматически.

DERIVED: запись обеих моделей на одном доступном драфте устраняет отсутствие
входов в E-300. Это сравнение двух реально исполнимых pipelines, а не равенство
наборов признаков: у incumbent 928 колонок, у кандидата draft + 32 history.

NOT_CHECKED: доставка на serv1, реальная доля доступных prestart карт, качество,
прибыльность и задержка работающего сервиса после включения.

## CHANGED

- `base/duration43_shadow.py`: выключен по умолчанию; submit копирует вход в
  очередь на 16 элементов. Один daemon worker выполняет inference и I/O вне
  `_PREDICTION_LOCK`; заполнение очереди приводит к пропуску, flock неблокирующий.
  Счётчики ошибок/пропусков доступны через `prematch_panel_live.status()`.
- `score(..., row_observer=None)`, `evaluate_map(..., shadow_context=None)` и
  передача явного map context из `win_model_veto`. Ошибка observer не меняет
  готовые вердикты. Bundle хранит SHA фактически загруженных файлов.
- Builder фиксирует веса, калибровку, encoder, историю и протокол в новом
  каталоге. Runtime не загружает joblib. Старые пакеты не перезаписываются.
- Evaluator закрывает endpoint по времени capture без чтения исходов,
  ждёт все исходы, перепроверяет сохранённые прогнозы через frozen models.

## CHECKS

PASS: 60 тестов, включая NaN/точный вектор, отказ при смене модели, дубликаты,
  ошибку observer, блокировку журнала, заполнение очереди при зависшем capture,
  запрет другой E-299 версии, неполную выборку и неизвестный источник исхода.

```sh
venv_catboost/bin/python3 -m pytest base/tests/test_duration43_shadow.py base/tests/test_prematch_panel_scorer.py base/tests/test_panel_journal_map_id.py -q

venv_catboost/bin/python3 base/tools/build_duration43_shadow.py \
  --candidate-dir .orchestra/jobs/run-2dcf60a5ff61532186ec2781/aug/output \
  --corpus runtime/artifacts/misc/pro_corpus_rich.npz \
  --output-dir ml-models/duration43_shadow_20260919_v2

venv_catboost/bin/python3 base/tools/evaluate_duration43_shadow.py \
  --artifact-dir ml-models/duration43_shadow_20260919_v2 \
  --journal runtime/artifacts/misc/duration43_shadow_20260919/synthetic_smoke_v2.jsonl \
  --outcomes runtime/artifacts/misc/duration43_shadow_20260919/empty_outcomes.jsonl \
  --output runtime/artifacts/misc/duration43_shadow_20260919/pending_smoke_final.json
```

Builder и evaluator требуют новые пути результата при повторении. Сборка
запускалась через nohup, PID1265 завершён, лог `build_v2_retry.log`. Первый
запуск PID670 завершился до создания пакета без вывода; результатом не считается.
Smoke с настоящими моделями и синтетическим ID1: запись создана, 928 колонок,
`PENDING_ENROLLMENT`, метрик нет. Синтетический журнал не использовать в бою.

## POINTERS

- Harness: `base/tools/build_duration43_shadow.py`, `base/tools/evaluate_duration43_shadow.py`.
- Доказательства: `runtime/artifacts/misc/duration43_shadow_20260919/`:
  `tests_final.log`, `build_v2_retry.log`, `build_smoke_v2.json`, `pending_smoke_final.json`.
- Исходы: `prematch_prediction_journal.record_outcome`, schema_version=1;
  producers `stratz_map_result._journal_result`, `prematch_live_delta`.
  Источник `runtime/prematch_model_outcomes.jsonl` либо PREMATCH_OUTCOME_JOURNAL.
- Рабочая панель: `prematch_panel_scorer.score` → observer → `duration43_shadow.submit`.

## RISKS

Протокол до исходов: первая успешно зафиксированная запись каждой карты;
это не первые eligible попытки сбора. Первый префикс журнала с
≥300 enrolled и интервалом первого/последнего capture ≥30 суток. Ожидать
исходы всех карт этого префикса. Для метрик нужно ≥300 eligible с
`recorded_at < actual_start`; иначе `INSUFFICIENT_ELIGIBLE`, без продления
endpoint и без метрик. Первичный показатель — paired calibrated logloss,
кандидат минус incumbent; Brier/AUC вторичные. Bootstrap по UTC-дням,
5000 повторов, seed302, CI95/99. Автопродвижения модели нет.

Где искать ошибку: отрицательный clock сам по себе не доказывает prestart.
`submitted_at` — локальное время передачи в очередь, `source_observed_at`
остаётся null: свежесть исходного fetch неизвестна. STRATZ start/end —
операционное определение фактической длительности; `start_hint` может быть
расписанием, различия отражаются в отчёте, но не переопределяют actual_start.
Source/schema allowlist и SHA журналов дают проверяемый локальный след,
не криптографическую аттестацию STRATZ и не независимое подтверждение результата.
Конфликтующие start/end останавливают evaluator. Producer сам дедуплицирует
исходы; пропущенные им исправления потребуется сверять с источником отдельно.

Когорта условна на успешной основной модели/панели, отрицательном clock,
10 известных уникальных героях и аккаунтах. Точность ролей не доказана.
Очередь может потерять записи при перегрузке/выходе процесса; её счётчики
process-local, не полный durable denominator. Фоновый worker потребляет
один inference thread и диск, поэтому нулевое влияние на общие ресурсы не обещается.
Полное чтение журнала для dedup линейно его размеру, выполняется только worker.

## NEXT

Подготовленный режим включается только после scoped разрешения на доставку
кода/пакета и restart `cyberscore.service`; env ниже в ARCHITECTURE. До первого
боевого capture проверить SHA пакета и incumbent на serv1. Проверить реальные
записи, health/error/drop counters и не менять модель/правила отбора внутри
эксперимента. Отдельный frozen протокол нужен при смене incumbent.

```orchestra-evidence-v1
{
  "schema": "orchestra-evidence-v1",
  "constraints": ["P(map duration >=2580 seconds)", "No production mutation", "No quality claim before prospective endpoint"],
  "sources": [
    {"id":"T","path":"runtime/artifacts/misc/duration43_shadow_20260919/tests_final.log","sha256":"2b378e03a88587b6ac233011da8da7a1250bf1b2871bcaeaf2ce457caedcb607"},
    {"id":"S","path":"runtime/artifacts/misc/duration43_shadow_20260919/build_smoke_v2.json","sha256":"6bb678e2ca9bedfe9cc99e6d9f78428b388bfbf18ec12ca73a01af619aa30927"},
    {"id":"P","path":"runtime/artifacts/misc/duration43_shadow_20260919/pending_smoke_final.json"}
  ],
  "claims": [
    {"id":"F1","kind":"OBSERVED","claim":"60 targeted tests passed","sources":["T"],"scope":"local Python3.9"},
    {"id":"F2","kind":"OBSERVED","claim":"Real-artifact synthetic capture recorded; evaluator pending without metrics","sources":["S","P"],"scope":"offline synthetic ID1"},
    {"id":"U1","kind":"NOT_CHECKED","claim":"Real prospective quality and production activation","scope":"serv1"}
  ],
  "checks": [{"id":"C1","status":"PASS","sources":["T"],"outcome":"60 passed"},{"id":"C2","status":"PASS","sources":["S","P"],"outcome":"recorded, PENDING_ENROLLMENT"}],
  "limitations": ["0 real new maps", "No verified source-fetch timestamp", "Frozen history differs from E299 rolling history", "Conditional cohort and lossy bounded queue"],
  "contradictions": ["E300 logged-live candidate was worse; no superiority inferred here"],
  "decision_required": ["Scoped production shadow deployment/restart approval"]
}
```
