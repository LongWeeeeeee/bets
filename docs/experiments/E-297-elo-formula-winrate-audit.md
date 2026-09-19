---
id: E-297
title: Формула ELO и поиск лучшего винрейта среди 18 вариантов
area: elo
status: full
date: 2026-09-19
corpus: warmup 1396050 карт; оценка 64448 карт 2026; holdout 660
harness: runtime/experiments/elo/tier3_wr_search.py; verify_tier3_wr_search.py; run-20a2a28f485000b1affcca94
verdict: Выбранный actual-tier не подтвердился; K24 лучший на свежих картах 64.39% против 63.03%, но CI включает ноль; decay не помог
---

# E-297 — формула ELO и поиск лучшего винрейта

## STATUS

DONE

Завершено offline-исследование; внедрения новой формулы нет.

## SUMMARY

**OBSERVED.** Сравнены 18 вариантов на 64 448 картах 2026 года, после
прогрева на полной истории из 1 396 050 уникальных карт. Каждый прогноз
использует только результаты, завершившиеся строго раньше старта карты.
Победитель выбирался только на 1 января–11 августа: `actual_tier`.
На последующих 660 картах он дал 408 верных прогнозов против 416 у текущего
TIER3 readout: **61.82% против 63.03%**. Предварительный выбор не подтвердился.

**OBSERVED.** Самый высокий WR на свежем периоде среди 18 методов у K24:
**425/660 = 64.39%**, на 9 верных прогнозов больше текущего. Парный 95% CI
разницы: **−1.20…+3.99 п.п.** Это наблюдаемый лидер диагностического сравнения,
а не независимо подтверждённый победитель. Все проверенные decay-readout
проиграли текущему на свежих картах; вес TIER3 0.16 дал ровно тот же WR.

**DERIVED.** Оснований заменять production-формулу по этому тесту нет.
K24 — кандидат для следующей проверки на новых данных, без подбора по этому
же holdout. Исправление одного примера Nemesis–1w не является критерием выбора.
Источники: полный `report.json`, `predictions.npz`, независимый `verification.json`
в POINTERS; формула и фактический кейс ниже.

### Формула и обновление

Текущий prematch-контракт всегда читает TIER3:

```
S = 0.68 * mean(player_global) + 0.32 * mean(player_local[TIER3])
P(Radiant) = 1 / (1 + 10 ** ((S_Dire - S_Radiant) / 400))
error = actual_radiant_win - expected_radiant_win
R_global,player += side_sign * K_global,tier * uncertainty_global * error
R_local,player  += side_sign * K_local,tier * uncertainty_local * error
```

Обновляющая модель вычисляет ожидание в **фактическом tier матча**, с ролями
для TIER1 и learned side bias. Формула отображаемой вероятности выше исключает
side bias. Таким образом, рецепт обновления и фиксированный TIER3 readout
не идентичны. Их соответствие качеству требует эмпирического сравнения.

Глобальный K для T1/T2/T3: 28.8/12.0/7.2; локальный: 18.4/25.3/20.7.
Локальный K делится между tier- и role-рейтингом без дублирования суммы.
Одинаковое обновление всех пяти игроков меняет среднее на одну величину
`K * error`. Дополнительного деления на пять не требуется.

Множители неопределённости могут отличаться между игроками/сторонами, поэтому
сумма рейтингов не обязана сохраняться. Для нового состава множитель до 2
на первых четырёх наблюдениях; для нового player-org stint — до 2 на первых
15. Они могут перемножаться. Это запрограммированная модель, не обнаруженная
арифметическая ошибка. Роль/roster в фиксированном TIER3 readout не участвуют.

Decay выключен, inactivity keep=1. Patch reset сбрасывает TIER1 local/role,
но не глобальный рейтинг и не TIER3. Sweep bonus 0.05 есть в series evaluator,
однако штатный snapshot builder и benchmark его не применяют.

### Кейс Nemesis–1w

Точный live snapshot + 824 delta updates воспроизводят показанные числа:

| Компонент | Nemesis | 1w | Вклад в разницу Nemesis−1w |
|---|---:|---:|---:|
| Global | 1817.243 | 1980.344 | −110.909 после веса 0.68 |
| Local TIER3 | 3252.309 | 2833.097 | +134.148 после веса 0.32 |
| Итог | 2276.464 | 2253.225 | +23.239 |

Следовательно, глобальный рейтинг предпочитает 1w, а старый TIER3-компонент
переворачивает фаворита. В этом примере речь о вероятности Nemesis 53.34%,
а не о сильном фаворите. Даты последних обновлений отдельных TIER3 рейтингов
отстают на 50–80 дней; это не доказательство отсутствия реальных игр.

ML получила ту же разницу `hybrid_strength=+0.058096988`. Её отрицательный
агрегат «ELO −63%» преимущественно пришёл от отдельного `opp_elo`, а не от
этого гибридного рейтинга. Точное разложение и ограничения доставки:
`runtime/artifacts/elo/nemesis_1w_20260919/followup_report.md` и `report.md`.

### Винрейт в сохранённом причинном replay E-263

Числа заново прочитаны из локального `report.json`, не пересказаны по памяти.
ELO обновлялся после `start + duration`, до следующего доступного прогноза.

| Период UTC | Карт | Гибрид TIER3 | K24 | Разница K24, п.п. |
|---|---:|---:|---:|---:|
| Январь–февраль | 13472 | 60.273% | 60.719% | +0.445 |
| Март–май | 29645 | 60.466% | 60.310% | −0.155 |
| 1 июня–11 августа | 19885 | 61.021% | 61.056% | +0.035 |
| 12 августа–3 сентября | 776 | 65.722% | 67.655% | +1.933 |
| Все окна | 63778 | 60.662% | 60.718% | +0.056 |

Это accuracy с правилом `p >= 0.5` (равенство выбирает Radiant), а не WR
после исключения ничейных рейтингов. Общая разница соответствует 36 картам,
последнего окна — 15. В E-263 нет paired confidence interval для WR;
интервал log loss нельзя выдавать за интервал винрейта. По периодам знак
разницы меняется. Совокупный log loss: 0.654665 у гибрида, 0.653080 у K24.

### Зафиксированный протокол завершённого поиска

Первый пятикомпонентный smoke не дошёл до worker из-за macOS EACCES при
публикации snapshot (`run-518c1e40f9172aa8bfcc1564`); его метрики отсутствуют.
После повторного запроса пользователя поиск расширен до 18 методов **до
получения их метрик**. Старый протокол сохранён, не выдаётся за выполненный.
В новом протоколе decay направлен к текущему global, а не к начальному 1500:

```
R_player = global + w * 2**(-days_since_last_TIER3/H) * (local_TIER3 - global)
S_team = mean(R_player)
```

Для постоянного веса H=∞. При неизвестной истории TIER3 decay-вариант читает
только global. Проверены постоянные веса 0.08/0.16/0.24/0.32/0.48/0.64/1.00;
веса 0.16/0.32/0.48 с H=30/90 дней; 0.32 с H=60/180 дней; global,
фактический tier и независимый K24. `served_tier3` — текущие 0.68/0.32.
`actual_tier` использует штатный readout фактического tier, включая роли T1.
K24 — среднее пяти глобальных рейтингов; каждый игрок получает
`side_sign * 24 * (outcome - logistic(gap/400))` после завершения карты.

Все методы, кроме K24, читают **одно неизменённое гибридное состояние**.
Это сравнение рецептов чтения, а не переобучение динамики K/boost/reset.
Полный chronological warmup; результат применяется после start+duration,
при совпадении timestamps сначала прогноз. Sweep bonus не применяется.
Дубликаты удалены по match_id, конфликтов исхода/состава/timestamp/duration
нет; неизвестных durations после разбора нет. 313 514 исходных записей
отсеяны стандартным parser как invalid, 286 как non-dict. Полнота внешней
истории и все причины отбраковки отдельно не аудировались.

Выбор — максимальный WR на development, равный рейтинг получает 0.5 успеха;
при равенстве WR используются log loss и порядок методов. Знаменатель у всех
18 методов одинаков. Дополнительно сохраняется WR только ненулевых разниц.
Парный bootstrap по сериям (fallback пара команд/день), 2000 повторов, seed297.
Серии, пересекающие границы, удаляются из раннего периода; результат карты
должен закончиться до его границы. Удалена 1 development-карта и 2 validation-
карты; итого 63 001/784/660, ещё 3 карты остаются только в общей диагностике.

### Результаты всех 18 вариантов

Периоды UTC: development 01.01–11.08, validation 12.08–04.09,
holdout 05.09–19.09 (последний старт 19.09 00:11:45 UTC). Метрики — WR
с половиной успеха за равный рейтинг; в holdout ничьих нет.

| Метод | Development, 63001 | Validation, 784 | Holdout, 660 |
|---|---:|---:|---:|
| served_tier3 | 60.599% | 65.880% | 63.030% |
| global | 60.572% | 66.135% | 62.121% |
| actual_tier | 60.750% | 66.901% | 61.818% |
| k24 | 60.629% | 67.538% | 64.394% |
| t3_w0.08_hinf | 60.680% | 65.497% | 62.424% |
| t3_w0.16_hinf | 60.724% | 65.497% | 63.030% |
| t3_w0.24_hinf | 60.651% | 66.263% | 63.333% |
| t3_w0.48_hinf | 60.493% | 66.135% | 63.030% |
| t3_w0.64_hinf | 60.439% | 65.115% | 61.970% |
| t3_w1_hinf | 60.231% | 64.349% | 60.758% |
| t3_w0.16_h30 | 60.615% | 64.094% | 62.576% |
| t3_w0.16_h90 | 60.683% | 64.349% | 62.424% |
| t3_w0.32_h30 | 60.548% | 63.712% | 62.121% |
| t3_w0.32_h90 | 60.602% | 64.477% | 62.273% |
| t3_w0.48_h30 | 60.345% | 62.819% | 61.212% |
| t3_w0.48_h90 | 60.386% | 63.457% | 62.727% |
| t3_w0.32_h60 | 60.574% | 63.967% | 62.273% |
| t3_w0.32_h180 | 60.609% | 64.605% | 62.273% |

`actual_tier` на development улучшает 95 прогнозов (+0.151 п.п.), validation
ещё 8 (+1.020 п.п.), но на holdout теряет 8 (−1.212 п.п., CI −3.044…+0.752).
Смена знака не позволяет объявить устойчивое улучшение. CI development
после выбора из 18 методов не является независимым подтверждением.

K24 на validation даёт 67.54% против 65.88%, на holdout 64.39% против 63.03%.
По всей оценочной истории — 60.751% против 60.687% (+41 верный прогноз,
CI разницы −0.138…+0.266 п.п.). Его преимущество пока мало относительно
неопределённости. Лучший свежий результат среди изменений именно веса TIER3 —
0.24: 63.33%, всего на 2 прогноза лучше baseline; это диагностический максимум.

### Разрезы tier и противоположные свидетельства

Tier здесь — классификация **лиги** существующим алгоритмом, а не утверждение
о человеческой оценке уровня каждой команды. Полный год по tiers включает
development и не является дополнительным независимым тестом.

| Период / tier | Карт | Текущий | Actual tier | K24 |
|---|---:|---:|---:|---:|
| Весь 2026, T1 | 1507 | 60.25% | 63.70% | 62.97% |
| Весь 2026, T2 | 2488 | 61.58% | 63.30% | 63.67% |
| Весь 2026, T3 | 60453 | 60.66% | 60.66% | 60.58% |
| Holdout, T2 | 146 | 69.18% | 63.70% | 67.81% |
| Holdout, T3 | 514 | 61.28% | 61.28% | 63.42% |

В holdout **нет T1** по используемой разметке: свежая проверка T1 отсутствует.
Для T3 K24 улучшает 11 прогнозов, но CI прироста −0.57…+4.96 п.п.; для T2
теряет 2. Следовательно, общий выигрыш K24 не переносится автоматически
на наиболее интересные профессиональные матчи или весь Tier3.

На 74 свежих картах, где global и TIER3 выбирают разных фаворитов, текущий
прав в 40 случаях, global в 34. На 197 картах с TIER3 старше 30 дней текущий
и K24 имеют одинаковые 67.51%. Отказ от TIER3 только потому, что он старый
или расходится с global, этим тестом не оправдан.

Для Nemesis–1w global даёт Δ−163.10, фиксированный вес 0.16 — Δ−69.93,
но вес 0.32 с H=30 даёт **Δ+43.72** (ещё сильнее в сторону Nemesis против
текущих +23.24). Разный возраст рейтингов делает действие decay неодинаковым
для двух составов. Все значения сохранены в `incident_candidates.json`.

## CHANGED

Исследовательские harness/регрессионные проверки/артефакты, этот отчёт и индекс.
Production ELO, snapshot, ML-модель, сигналы и сервис не менялись/не перезапускались.

Локально исправлен необходимый сбой research-executor: root каталога snapshot
получает chmod0500 **после** os.replace; вложенные файлы и каталоги защищены,
хеши и post-run verification сохранены. Установлен явно маркированный local
release `orchestra-core-1.2.1-schema3-20260919-local-macos-snapshot-r1`,
fingerprint `a433f6355d60fe3d0177a6ab0370f5a6a1bd45e75e4d00fac416e21859d03139`.
Resource/deploy policy/hooks не менялись. Подробности и regression evidence:
`/Users/alex/Library/Application Support/Orchestra/local-release-sources/orchestra-core-1.2.1-local-macos-snapshot-20260919-r1/REPAIR.md`.
Проблема telemetry SQLite отмечена там отдельно, не ремонтировалась.

## CHECKS

- Smoke `run-81a6af126b39b19966526d08`: exit0, 200 карт ×18, snapshot verified.
- Full `run-20a2a28f485000b1affcca94`: exit0, 344.4s worker, 1 CPU local;
  1 396 050 starts = updates, 64 448 unique evaluation IDs, все gap конечны.
  Копия 322 raw files, 10.97GB; `snapshot_verified_after=true`, stderr пустой.
- Harness regression: 3 PASS (направление decay, изоляция выбора от holdout,
  одинаковый знаменатель/ничьи). Model series/overlay regression: 23 PASS.
- Независимый пересчёт `verify_tier3_wr_search.py`: PASS. Все 16 доступных
  компонентных формул восстановлены с ошибкой <1e-8; actual-tier=TIER3 на T3;
  90 сочетаний метод/период совпали; победитель development тот же;
  группы между периодами не пересекаются, результатов из будущего нет.
  Это проверка сохранённых чисел, не независимый второй исторический replay.

Команды воспроизведения (существующий завершённый run не перезапускается):

```bash
venv_catboost/bin/python3 -m pytest runtime/experiments/elo/test_tier3_wr_search.py -q
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources preflight --plan runtime/artifacts/elo/tier3_search_20260919/smoke_campaign.json
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/elo/tier3_search_20260919/smoke_campaign.json --background
# После успешного smoke:
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources preflight --plan runtime/artifacts/elo/tier3_search_20260919/full_campaign.json
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/elo/tier3_search_20260919/full_campaign.json --background
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources status --run run-20a2a28f485000b1affcca94
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources collect --run run-20a2a28f485000b1affcca94
venv_catboost/bin/python3 runtime/experiments/elo/verify_tier3_wr_search.py
venv_catboost/bin/python3 scripts/ops/experiments_index.py --check
```

## POINTERS

- `runtime/experiments/elo/tier3_wr_search.py`: frozen replay/selection/metrics.
- `runtime/experiments/elo/verify_tier3_wr_search.py`: independent scalar check.
- `runtime/artifacts/elo/tier3_search_20260919/protocol.json`: preregistered grid/splits.
- `runtime/artifacts/elo/tier3_search_20260919/verification.json`: counts, source hashes.
- `runtime/artifacts/elo/tier3_search_20260919/incident_candidates.json`: Nemesis readouts.
- `.orchestra/jobs/run-20a2a28f485000b1affcca94/finished-replay/output/report.json`: all metrics.
- Same directory: `predictions.npz`, `inputs.json`; parent: `receipt.json`, logs.
- `.orchestra/campaigns/run-20a2a28f485000b1affcca94/collected-artifacts.json`: collection receipt.
- `ELO/models.py:1037-1119,1309-1418`; `ELO/config.py:43-105`; `ELO/replay.py`.
- `ELO/tiering.py:166-218`: as-of participants, static curated tier priors.

## RISKS

**Где искать ошибку:** start/end и одинаковые timestamps; дубликаты; состав,
позиции и team identity; current tier/alias seeds вместо исторических;
неодинаковые знаменатели/ничьи; зависимость карт одной серии; выбор метода
по diagnostic-разрезу после просмотра результата; перенос readout на ML без
переоценки входного распределения. Bootstrap учитывает серии, но не всю
долговременную зависимость повторяющихся команд и игроков.

**NOT_CHECKED:** Полная временная достоверность статических tier/alias priors;
история identity десяти аккаунтов; полнота raw corpus; самостоятельно
переобученные update weights/K/boost/decay; новая ML-модель; ставки, кэфы,
dispatch selection и доходность. Holdout не использован для выбора в этом
эксперименте, но его неприкосновенность для всех прежних исследований не доказана.
Результаты относятся к данной разметке и корпусу, не ко всем матчам Dota.

## NEXT

Оставить production без изменения. Зафиксировать K24 как кандидата для
следующего нового периода и отдельно проверить качество на целевом
профессиональном потоке; заново использовать этот holdout для подтверждения
нельзя. Пересборку ML или deployment этот эксперимент не авторизует.

```orchestra-evidence-v1
{
  "schema": "orchestra-evidence-v1",
  "constraints": [
    "Offline research; WR primary; preserve production and unrelated work"
  ],
  "sources": [
    {
      "id": "R",
      "path": ".orchestra/jobs/run-20a2a28f485000b1affcca94/finished-replay/output/report.json",
      "sha256": "52e483441abe26d7fe4089e6d4d55c9f30d5a5fa54201b02e86e11f13c9f29e0"
    },
    {
      "id": "V",
      "path": "runtime/artifacts/elo/tier3_search_20260919/verification.json",
      "sha256": "21b3740cc7295a5ec8e1ee00ef910cf9c2fcde7d4d1aa7f11544785793b21450"
    },
    {
      "id": "J",
      "path": ".orchestra/jobs/run-20a2a28f485000b1affcca94/finished-replay/receipt.json",
      "sha256": "7a4e221247b8ac29f50c8e3bc8a462ba5f45bd2fa5a4e3a36ac23107028af1dc"
    }
  ],
  "claims": [
    {
      "id": "F1",
      "kind": "OBSERVED",
      "claim": "Development selects actual_tier; holdout actual_tier408/660 versus baseline416/660",
      "sources": [
        "R",
        "V"
      ],
      "scope": "Frozen18-method causal readout comparison"
    },
    {
      "id": "F2",
      "kind": "OBSERVED",
      "claim": "K24 holdout425/660; paired delta95CI [-0.0119773986,0.0399385561]",
      "sources": [
        "R",
        "V"
      ],
      "scope": "Diagnostic comparison, not locked development winner"
    },
    {
      "id": "D1",
      "kind": "DERIVED",
      "claim": "No independently confirmed replacement; K24 is a candidate for new data",
      "basis": [
        "F1",
        "F2"
      ],
      "method": "Locked winner fails later period; K24 paired interval includes zero"
    },
    {
      "id": "U1",
      "kind": "NOT_CHECKED",
      "claim": "Production profitability and ML migration",
      "scope": "Live serving/betting"
    }
  ],
  "checks": [
    {
      "id": "C1",
      "status": "PASS",
      "sources": [
        "J"
      ],
      "observed": "exit0; snapshot_verified_after=true"
    },
    {
      "id": "C2",
      "status": "PASS",
      "sources": [
        "V"
      ],
      "observed": "64448 unique predictions, formula reconstruction and split checks"
    }
  ],
  "limitations": [
    "Static current team tier priors",
    "17readouts share baseline hybrid updates",
    "No TIER1 in holdout",
    "K24 fresh maximum exploratory"
  ],
  "contradictions": [
    "Actual-tier wins development but loses holdout",
    "K24 improves holdoutT3 but loses holdoutT2"
  ],
  "decision_required": []
}
```
