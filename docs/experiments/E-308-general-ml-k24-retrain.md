# E-308 — переобучение общей winner ML против causal K24

Дата: 21.09.2026. Offline research; production не меняется.

## Протокол до итогового теста

Цель — увеличить парную точность минимум на 5 п.п. относительно K24,
получить положительную нижнюю границу 95% CI при bootstrap целых серий
и улучшить log loss. Сравниваются одинаковые карты, без удаления трудных
примеров ради красивого WR. Второй baseline — отдельно откалиброванный K24.
Это исследовательский критерий существенного улучшения, не обещание результата.

- История признаков: все доступные законченные карты с допустимыми полями.
- Выбор рецепта: June 1–July 1 и July 1–15, 2026, обучение до каждого окна.
- Калибровка: July 15–August 1, с embargo по окончанию карты 1 час.
- Итоговый тест: August 1–September 21 exclusive, завершённые до cutoff−1 час.
- Разные части одной серии не пересекают границы обучения/калибровки/теста.
- После итогового отчёта выбранный рецепт переобучается на доступных исходах.
  Этот финальный raw-score artifact не наследует калибратор другого fit.

Итоговый набор — ретроспективный temporal holdout. Часть реальных событий
августа–сентября уже могла анализироваться в других задачах; он не называется
никогда прежде не виденным. Daxak BO5 и 113 карт E-307 не используются как
отдельная цель оптимизации. До выбора рецепта итоговые метрики не открываются.

## Источники и причинность

`runtime/artifacts/misc/pro_corpus_rich.npz`: 1 261 469 карт с 2016 года.
Исходные слоты Radiant position 1..5, затем Dire 1..5 получены postgame.
Для query они **не используются**: Hungarian assignment назначает позиции
по частотам ролей игрока только в прошлых завершённых картах; равенства
разрешаются в детерминированном порядке account ID. Драфтовые snapshots
затем применяются заново к этому causal порядку героев. Поля результата и статистика
игроков текущей карты используются только после её окончания, для будущих карт.
Условия допуска raw event stream K24 берутся из `ELO.data_loader`, а не из
rich subset: позиции не обязательны, обе команды должны иметь ID или имя,
10 аккаунтов уникальны. Дубликаты ID не дают второго обновления рейтинга.
Рейтинг K24 обновляется по `(end, mid)`, только при `end < query start`.
Raw export `run-a10fd2de5d48924fa5a80497`: 1 731 906 исходных записей,
1 418 105 валидных записей loader, 21 947 дубликатов исключены,
**1 396 158 уникальных завершённых событий** с 2012 года до
2026-09-20 00:57:13 UTC. SHA256 events:
`8d06c22f1461e51400851f0ac73277cee4cc863f7866f6b5aeb74c7df4b08651`.
Это реконструкция из сегодняшнего архива по времени окончания, не replay
задержек поступления каждого исторического сообщения production.

Основная матрица `run-a851fbe625235f12216f54ae`: 1 261 465 допустимых карт,
4 строки отклонены, 71 численный признак. На 998 541 пересечении с raw K24
направление сторон, исход и время совпали; расхождений 0. Для сравнения
принимаются одинаковые законченные карты с league > 0 и допустимым K24.
Остальная допустимая история участвует в построении признаков и обучении;
карты без league получают четверть веса в fit.
Dataset SHA256: `8428a928f779b729477a02ac7b99161f8eade65a9db23375a8c606d2766ee6f1`.

Паблики: 6 638 652 исходных карт, исключены 225 пересечений с rich corpus,
6 638 427 допущены. Два draft SGD: hero/role и hero/role/position-pairs.
Словарь фиксируется на первом допустимом префиксе, затем каждый паблик
потребляется один раз. Месячные прогнозы используют только прошлые завершённые
паблики; до первого snapshot прогноз отсутствует. Исходы pro не читаются.

Public campaign `run-d4c1e80325d03cc332d22d95`: 2/2 jobs completed на local,
единственном включённом host. Все источники и исполняемый код скопированы
executor в hash-verified snapshot. На промежуточных 9038 pro-картах June–July15
hero/role AUC 0.56575, position-pair AUC 0.60271. Этот начальный probe использовал
архивные postgame позиции. Его цифры не являются causal оценкой общей ML:
перед её обучением прогнозы пересчитываются по ожидаемым прошлым ролям.
225/225 общих rich/public ID совпали по времени, исходу и всем 10 героям;
все 225 исключены из public fit. Проверка делалась после сортировки обоих
MID-массивов по ID, поскольку rich хранится по времени.
Повторная проекция `run-ab773f2101cf77c8bf5aaea1` завершена 2/2:
используются те же замороженные public models и причинно назначенные роли.
На June–July15 auxiliary probe AUC соответственно 0.56974 и 0.60226;
это не финальный winner result и не прибыльность.

## Харнесс и воспроизведение

Протокол и manifests: `runtime/artifacts/elo/general_ml_20260921/`.
Исполнители: `base/tools/prematch_causal_dataset.py`,
`base/tools/prematch_public_draft.py`, `base/tools/prematch_winner_research.py`.

```sh
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources preflight --plan runtime/artifacts/elo/general_ml_20260921/public_campaign.json
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/elo/general_ml_20260921/public_campaign.json --background
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources status --run run-d4c1e80325d03cc332d22d95
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources collect --run run-d4c1e80325d03cc332d22d95
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/elo/general_ml_20260921/events_campaign.json --background
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/elo/general_ml_20260921/features_campaign.json --background
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/elo/general_ml_20260921/projection_campaign.json --background
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/elo/general_ml_20260921/search_campaign.json --background
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/elo/general_ml_20260921/search_remaining_campaign.json --background
```

Следующие manifests и результаты дописываются по завершении шагов; повторный
запуск создаёт новый immutable campaign, а не затирает предыдущую оценку.

Поиск `run-7265e7843b1e2180656da721` сохранил 6 завершённых проверенных jobs.
При добавлении четырёх новых рецептов исходник изменился; executor отклонил
запуск следующего job до создания его каталога/процесса. Старый campaign
оставлен с `needs_reconciliation`; его готовые результаты не потеряны.
После проверки отсутствия процесса три незапущенных рецепта и четыре новых
объединены в `run-be54fa00b85cdd0c2860de18` с актуальными input hashes.
Шесть готовых fit повторно не запускаются. Изменений executor/resource policy нет.

Все 13 базовых рецептов завершены. На 9 018 законченных validation-картах
лучший single `lgb31_all`: 64.105% (K24: 61.233%). Ансамбль
`lgb31_all,lgb15_all,lgb31_recent`: 64.693%, log loss 0.62369 против
0.64430 у K24; в 2 254 расхождениях ML верна 1 283 раза, K24 — 971.
Это адаптивно выбранный validation result, не доказательство итогового качества.
Более глубокие и короткие fit не улучшили результат. До открытия terminal
добавлен ещё один осмысленный вариант признаков: 100 role-specific колонок
(5 ролей × 10 исторических метрик × radiant/diff) после исходных 71.
Текущие исходы/статистика не добавляются; роли по-прежнему назначены по прошлому.
На 998 реальных emitted rows все исходные 71 признака и identity arrays
побитово совпали с прежней версией. Полная role-матрица строится в
`run-6ba69476c5ff5dcc79178b84`; на ней заранее выбраны четыре рецепта:
`lgb31_all`, `lgb15_all`, `linear_short`, `lgb31_recent`.

Полная матрица 171 признака завершена: SHA256
`53c55fa159d949f94678b99344ff12821612ffd82ab41ef8637f58dfa0929bb6`.
Все 1 261 465 строк старого 71-column prefix и все identity/output arrays
сверены побитово; `role_features/prefix_check.json`. Благодаря тождественным
mid/ts/heroes разрешено повторно использовать уже causal public projections.
Первый role fit `run-4feab7331ab1b242c090bc74/lgb15_all` завершён.
Затем worker по ошибке применил patch к main вместо изолированного worktree;
preflight следующего dispatch отклонил изменённый hash до запуска процесса.
Main восстановлен байт-в-байт к исходному hash `e3857b508ba1f7f9f4c69114b64624d62f4a60f3a17a9e219b7df710d55ba89d`.
Три незапущенных fit перенесены в `run-8a55084f0ec9387e1a86e9e4`
(`role_search_remaining_campaign.json`); готовый fit не повторяется.

Остальные три role fit завершены; ансамбль 64.615%, против 64.693% у base71.
Sparse roster logistic learner (`run-62ec9dba84202a15b6df6e2d`, 2/2 completed)
использовал train-only account и hero/role vocabularies, signed R/D indicators,
несколько исторических diff и public logits. Full/recent WR: 61.566%/61.710%,
log loss: 0.67956/0.64845. Solver сошёлся за 6/5 итераций; рецепты отклонены.
Таким образом проверены 19 individual fits (13 base, 4 role, 2 roster) и три
ансамбля: промежуточный top3 деревьев, итоговый top3 base включая linear и
top3 role. Первый ансамбль остался лучшим по заранее заданной метрике.

Выбор заморожен до terminal в `selection.json`, SHA256:
`60fae63d3b306b3a6038dbc4d47d7eb70cc87eb1fbb398b0382df46a6d89c874`.
Выбраны равные веса `lgb31_all,lgb15_all,lgb31_recent` и base71 dataset.
Тест `run-b1562e842f8986bf8d66042e` завершён один раз после этого выбора.

Дополнительные команды (перед каждым run применяется resources preflight):

```sh
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/elo/general_ml_20260921/role_features_campaign.json --background
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/elo/general_ml_20260921/role_search_remaining_campaign.json --background
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/elo/general_ml_20260921/roster_search_campaign.json --background
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/elo/general_ml_20260921/terminal_campaign.json --background
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/elo/general_ml_20260921/refit_campaign.json --background
venv_catboost/bin/python3 -m pytest base/tests/test_prematch_causal_dataset.py base/tests/test_prematch_winner_research.py -q
```

## Где искать ошибку

Проверять population K24 (raw loader против rich position-complete subset),
ориентацию Radiant/Dire, `mid` join вспомогательных прогнозов, end-time вместо
start-time при допуске исходов, дубликаты ID, same-series overlap и выбор
рецепта до terminal. Регрессия query permutation проверяет независимость
признаков от raw postgame порядка игроков текущей карты. Сравнение полного драфта
не доказывает качество веток с пропущенными аккаунтами/героями.

Нельзя переносить July-калибратор на refit, обученный на другом периоде,
и называть эти вероятности проверенными. Главный результат должен включать
калиброванный K24, парные расхождения, CI, крупнейшие лиги и ограничения.
Не интерпретировать WR/AUC как прибыльность без исторических доступных кэфов.

## Результат

**Критерий сильного превосходства не выполнен.** После заморозки рецепта
terminal дал 2 613 карт / 1 977 кластеров серий. Обучение закончилось до July 15,
2 125 карт использованы для July-калибровки; пересечений серий с fit — 0.

| Модель | Верных / карт | WR | Log loss |
|---|---:|---:|---:|
| ML, July-калибровка | 1676 / 2613 | 64.1408% | 0.622089 |
| ML, raw | 1672 / 2613 | 63.9878% | 0.622274 |
| K24, raw | 1666 / 2613 | 63.7581% | 0.633644 |
| K24, своя July-калибровка | 1675 / 2613 | 64.1026% | 0.633160 |

Парная разница ML с raw K24: +0.3827 п.п., series-bootstrap 95% CI
[-1.3410; +2.1213] п.п. В 556 расхождениях ML верна 283 раза, K24 — 273.
С калиброванным K24: +0.0383 п.п., CI [-1.7051; +1.7699]; в 567
расхождениях счёт **284 : 283**. По точности убедительного преимущества нет.
Log loss лучше на 0.011071, CI [0.002433; 0.019943]; это преимущество
вероятностного прогноза, а не доказательство более высокого WR или прибыли.
Без крупнейшей лиги 19944 (208 карт): 2 405 карт, +0.0416 п.п. к
калиброванному K24, CI [-1.8435; +1.8916], расхождения 271 : 270.
Вывод не меняется. Все интервалы — 4 000 bootstrap целых серий.

Для конкретной карты Daxak BO5 `9006963816` (третья карта) terminal-модель
дала Radiant 52.2506% (raw 52.9876%), K24 — 45.8333% (калиброванный 47.4737%);
исход `y=0`, победила Dire. Новая ML здесь также ошиблась. Это диагностика
после итогового теста, рецепт по этому примеру не менялся.

Итоговый raw refit `run-ef9ca268cf88ea6881148c25` завершён, exit 0,
input snapshot проверен executor. Два full-history компонента использовали
все 1 261 465 допустимых исходов, recent-компонент — 130 288 карт за два года.
Все три fit завершены за 84 секунды. Сохранён
`data/prematch_general_retrained_20260921/candidate.joblib`, SHA256
`fb6c186dc618ba2971f78503aaa6a361fc04292fab390aafb8dd54e4a4a5329d`.
Повторная загрузка сохранённой копии на 32 контрольных строках: max error 0.
Рядом — manifest, точный порядок 86 входных признаков, selection, метрики refit
и контрольные строки. Веса публичных draft-моделей указаны в manifest с hashes.
Текущий builder выдаёт 171 historical column: для выбранного кандидата нужно
явно взять исходные 71 по именам. Несовпадение схемы inference отклоняется.
Это отдельный исследовательский artifact, не готовая замена production scorer.

July-калибратор не перенесён на новый fit: `calibrator=None`.
Метрики таблицы относятся к более раннему fit, а качество финального refit
на новых матчах ещё не измерено. Production не менялся, рестартов и push нет.
Дальнейший подбор после открытого terminal будет новой исследовательской фазой
и потребует другого отложенного периода; этот результат нельзя переименовать
в независимую проверку следующего рецепта.
