---
id: E-278
title: "Аудит четырёх phase-моделей и более сильная регуляризация"
date: "2026-09-10"
area: ml
status: full
corpus: "Public E-260; 141 новая pro-карта после предыдущего cutoff"
verdict: "Аудит PASS; public accuracy: NW/EW без изменений, All −0.041 п.п., Late +0.019 п.п. (CI включает ноль). Late public log loss улучшился; убедительного прироста accuracy и pro-переноса нет. Прод-модели сохранены."
harness: "base/tools/build_fresh_phase_holdout.py; base/tools/tune_draft_phase_models.py"
---

# E-278 — четыре модели: serving, регуляризация и новые pro-карты

Запрос: проверить Early NW, Early Win, All, Late в проде и попробовать улучшить точность. Прод в этом эксперименте не изменяется. Исходные модели E-260 используют только десять hero ID по позициям; признаки игроков/ELO и новые цели не добавляются.

## Проверка действующего serving

10.09.2026: процесс serv1 PID 401516, HEAD `afae954`, active/running, NRestarts=0. Пути взяты из фактического `/proc/<pid>/environ`, Early Win использует свой штатный default. Четыре encoder/model SHA совпали с E-260 manifest; по 512 реальных драфтов на модель дали raw delta ≤1.11e-16. Через production readers отличие только от округления (All ≤4.99e-6; остальные ≤5e-7). Это проверка файлов и вызова ридеров в отдельном процессе, а не новая оценка качества.

Evidence: `runtime/artifacts/draft-cp/20260910_phase_improvement/production_audit.json` и `.log`. Сообщение All о старой калибровке порогов вето сохранено; менять шкалу/пороги в рамках offline исследования нельзя.

Повторная read-only проверка во время обучения: PID `401516`, `active/running`, `NRestarts=0`, HEAD `afae954` и все восемь SHA encoder/classifier остались прежними. Evidence: `runtime/artifacts/draft-cp/20260910_phase_improvement/production_recheck.json`.

Финальная проверка 23:30 MSK: `active/running`, PID `411470`, `NRestarts=0`, HEAD `3512822`. Другая задача перезапустила сервис в 23:05; все восемь SHA моделей и пути serving по-прежнему совпадают с начальным аудитом. Этот эксперимент не менял прод. Evidence: `production_final_recheck.json` в том же каталоге.

## Замороженный протокол до результатов

- Current binary targets: Early NW — сторона при наличии маркера 20–28 минут; Early Win — победитель среди карт 20–34 минуты; All — победитель при длительности ≥20 минут; Late — победитель при ≥36 минутах. NW occurrence не участвует в текущей строке и здесь не переобучается.
- Public E-260 заканчивается 04.09.2026. Его test уже опубликован; повторная проверка на нём — **диагностика**, не новый независимый test (по уточнению пользователя — основной набор для итогового сравнения).
- Границы 60/20/20 рассчитываются по полной соответствующей phase-популяции до удаления NW no_marker. Train и validation очищаются по времени `start + duration + 3600`, которое должно быть строго раньше начала следующего блока. Vocabulary учится только на допустимом train.
- Сетка position-pair C: 0.0001, 0.0003, 0.001, 0.003. Отдельный кандидат — прежние role-pairs с исторически выбранным C. Incumbent recipe обязательно входит в сравнение; выбирается минимум validation log loss, на равенстве предпочтителен incumbent. Accuracy/AUC/калибровка и equal coverage сообщаются отдельно.
- На public test сравниваются выбранный recipe и incumbent после одинакового fit на доступных train+validation. Парные интервалы считаются по дням. Выбранный изменённый recipe дополнительно refit на полном public-корпусе; в prod не подключается.
- **Новые pro-карты** выделяются из новых/изменённых raw-шардов до просмотра метрик. Исключаются все ID старого public/pro корпуса; начало строго после `max(public max end + 3600, previous pro max start)`. Список, SHA и правила фиксируются в отдельном manifest. Финальное сравнение выбранных моделей с действующими full-fit артефактами проводится один раз после фиксации всех четырёх вариантов. По pro параметры не подбираются.
- Даже положительный public результат не доказывает улучшение на pro или пользу betting gates. Новый pro-интервал короткий; неопределённость будет указана отдельно.

## Harness и команды

Все долгие команды запускаются через `nohup`, PID/логи записываются в `runtime/artifacts/draft-cp/20260910_phase_improvement/`. Никаких периодических LLM heartbeat.

```bash
/Users/alex/Documents/ingame/venv_catboost/bin/python3 -u base/tools/build_fresh_phase_holdout.py \
  --source pro_heroes_data/json_parts_split_from_object \
  --public-corpus data/draft_phase_corpus/2026-09-05_position_pairs/public \
  --previous-pro-corpus data/draft_phase_corpus/2026-09-05_position_pairs/pro \
  --output data/draft_phase_corpus/20260910_fresh_pro
```

Holdout построен до обучения: **141 новая pro-карта**, 05.09 03:08:34 — 09.09 23:22:33 UTC; 5 новых/изменённых шардов, 6 карт до cutoff исключены. Public/pro пересечений ID нет. `fresh_after_ts=1788550139` (старый pro max start), public max end + 1 час = 1788486483. SHA holdout rows: `0bb357fdb81450c53cb5fb272bf7642b8564e9c66922e5d38d1a1c7489d4e229`. Это короткий временной интервал; фазовые когорты будут ещё меньше.

Точная команда обучения (параметры зафиксированы до запуска):

```bash
/Users/alex/Documents/ingame/venv_catboost/bin/python3 -u base/tools/tune_draft_phase_models.py \
  --corpus data/draft_phase_corpus/2026-09-05_position_pairs/public \
  --baseline-dir data/draft_phase_models/2026-09-05_position_pairs \
  --output-dir data/draft_phase_models/20260910_regularization_r2 \
  --scratch runtime/artifacts/draft-cp/20260910_phase_improvement/sparse_r2 \
  --fresh-after-ts 1788550139 --threads 2 --embargo-seconds 3600
```

Фактический запуск: 10.09 22:32 MSK, PID `55585`, лог `runtime/artifacts/draft-cp/20260910_phase_improvement/train_r2.log`. Предыдущая попытка завершилась при закрытии группы процессов exec-оболочки до первого fit; каталог `20260910_regularization` сохранён как незавершённый. Это подтверждено малым `nohup sleep`/`setsid` probe. Для устойчивого запуска используется `nohup venv_catboost/bin/python3 -c 'import os,sys; os.setsid(); os.execv(sys.executable, [sys.executable, *sys.argv[1:]])' -u ... > train_r2.log 2>&1 < /dev/null & echo $!`. Статистический протокол и сетка не менялись. Проверка процесса: `ps -p 55585 -o pid,etime,%cpu,rss`.

После `status=DONE` всех четырёх фаз выполняется единственная финальная оценка:

```bash
/Users/alex/Documents/ingame/venv_catboost/bin/python3 base/tools/evaluate_phase_forward.py \
  --candidates data/draft_phase_models/20260910_regularization_r2 \
  --holdout data/draft_phase_corpus/20260910_fresh_pro \
  --production-dir data/draft_phase_serving/2026-09-05_position_pairs \
  --audit runtime/artifacts/draft-cp/20260910_phase_improvement/production_audit.json \
  --output runtime/artifacts/draft-cp/20260910_phase_improvement
```

Проверки: `venv_catboost/bin/python3 -m pytest base/tests/test_fresh_phase_holdout.py base/tests/test_tune_draft_phase_models.py base/tests/test_phase_forward_evaluation.py base/tests/test_draft_phase_training.py -q`.

## Где искать ошибку

Проверить не путать NW direction с occurrence/joint, раннюю/позднюю условную популяцию с любой картой. Проверить индексы после phase/NW фильтров, одинаковые timestamps и end-time embargo. Нельзя использовать старый full-fit артефакт для оценки на его public train; public baseline здесь переобучается тем же протоколом. Не переиспользовать CSR другого encoder/порядка строк. Для fresh pro проверить источник, duplicate/conflict rejection, SHA, отсутствие public/pro ID overlap и сравнение обеих моделей на одних строках. Малое n и зависимость карт одной серии ограничивают интервалы; изменение accuracy без улучшения log loss не объявлять улучшенной вероятностной моделью автоматически.

### Ограничения исполнения и интервалов

Обучение r2 использует код `b9ae063`. С 22:37 MSK будущие scratch-каталоги NW/Late/All направлены symlink в один временный слот: `runtime/artifacts/draft-cp/20260910_phase_improvement/scratch_aliases.json`. Это ограничивает расход диска; `disk_design` **каждый раз полностью пересобирает** CSR с нужным encoder/порядком строк через `.tmp` → replace. Готовая матрица не считается кешем. Перед заменой предыдущий fit/view освобождён; все фазы строго последовательны. Scratch Early Win сохранён отдельно. Старый output прерванной попытки также сохранён.

`public_test.baseline/candidate.accuracy` — доля правильных прогнозов по всем картам. `public_test.paired_day_bootstrap` в замороженном trainer — средняя разность метрик с **равным весом каждого UTC-дня**, включая неполные крайние дни; это иной estimand. Его CI нельзя подписывать как интервал для pooled accuracy. Например, у Late pooled accuracy delta = +0.00019019, а mean-day delta = −0.01387591, CI95 [−0.04183841, +0.00043527]. Оба результата сохраняются; точность по дням не демонстрирует устойчивого улучшения. Forward evaluator использует другой заранее зафиксированный контракт: resample UTC-дней с сохранением числа карт внутри каждого дня и pooled paired delta; при одном дне интервал не строится.


## Результат на public test

По уточнению пользователя public test — основной результат этого эксперимента; новые pro-карты — дополнительная проверка. Public test уже использовался в E-260, поэтому это сравнение на известном временном блоке. Параметры выбирались только по validation, test не использовался для повторного подбора.

| Модель | Public test, карт | Accuracy incumbent | Accuracy candidate | Разность, п.п. | Выбранный recipe |
|---|---:|---:|---:|---:|---|
| Early NW direction | 828 966 | 66.2957% | 66.2957% | 0 | прежний position-pair C=0.003 |
| Early Win | 401 913 | 67.3643% | 67.3643% | 0 | прежний position-pair C=0.003 |
| All | 1 327 731 | 59.1709% | 59.1299% | −0.0410 | role-pair C=0.01 |
| Late | 814 973 | 58.9708% | 58.9898% | +0.0190 | role-pair C=0.003 |

Early NW / Early Win: более сильная регуляризация не помогла, остаётся incumbent. All: выбор по validation не перенёсся на test — на 545 правильных прогнозов меньше, log loss вырос на 0.000160690. Late: на 155 правильных прогнозов больше, log loss снизился 0.668135220 → 0.667904354 (−0.000230866).

Для интервалов именно pooled-метрик дополнительно применён существующий `evaluate_phase_forward.paired_intervals` к уже сохранённым public predictions: 5 000 resamples UTC-дней, seed=278, без изменения моделей. Артефакт `runtime/artifacts/draft-cp/20260910_phase_improvement/public_pooled_intervals.json` содержит SHA входных NPZ. У Late 95% CI Δaccuracy = [−0.0316; +0.0685] п.п.; убедительного прироста accuracy нет. 95% CI Δlog loss = [−0.000333990; −0.000133359], 98.75% CI = [−0.000362812; −0.000109465]. У All 95% CI Δaccuracy = [−0.0805; +0.0020] п.п., Δlog loss = [+0.000061557; +0.000262396]. Интервалы описательные: известный test, 23–24 временных блока и исторический выбор семейства моделей не делают их новым независимым подтверждением.

Воспроизведение pooled-интервалов, без fit или выбора параметров:

```bash
/Users/alex/Documents/ingame/venv_catboost/bin/python3 - <<'PYCODE'
from pathlib import Path
import numpy as np
from base.tools.evaluate_phase_forward import paired_intervals
for phase in ['early_nw', 'early_win', 'all', 'late']:
    path = Path('data/draft_phase_models/20260910_regularization_r2') / phase / 'public_test_predictions.npz'
    with np.load(path) as z:
        print(phase, paired_intervals(z['y'], z['baseline'], z['candidate'], z['ts']))
PYCODE
```


## Однократная проверка новых pro-карт и решение

Обучение r2 завершилось `DONE` для всех четырёх фаз 10.09 в 23:32:57 MSK; PID `55585` завершён. Последний full fit All занял 476.2 секунды, 157 итераций. Перед чтением holdout проверены замороженные SHA report/full artifacts и audited production export. Forward evaluator завершился с exit 0, `forward_results.json.complete=true`; receipt `forward_started.json` сохранён. Повторного подбора или оценки новых параметров на этих картах не было.

| Модель | Новых pro-карт | Правильно incumbent → candidate | Accuracy incumbent → candidate | Log loss incumbent → candidate |
|---|---:|---:|---:|---:|
| Early NW direction | 94 | 62 → 62 | 65.957% → 65.957% | 0.642746 → 0.642746 |
| Early Win | 69 | 39 → 39 | 56.522% → 56.522% | 0.684720 → 0.684720 |
| All | 141 | 76 → 77 | 53.901% → 54.610% | 0.693581 → 0.696120 |
| Late | 62 | 29 → 30 | 46.774% → 48.387% | 0.698168 → 0.699247 |

Всего пять UTC-дней. У All сменились девять сторон, чистый итог +1 верный прогноз; у Late сменилась одна сторона и оказалась верной. 95% day-block CI Δaccuracy: All [−4.848; +6.767] п.п., Late [0; +4.688] п.п. Нулевая нижняя граница Late при одной смене стороны и пяти днях не доказывает устойчивую прибавку; log loss у обеих изменённых моделей вырос. Pro-набор теперь использован и больше не считается untouched.

**Итог:** достоверно воспроизводится небольшое улучшение public log loss Late; убедительного роста accuracy не установлено даже на основном большом public test. Для Early NW/Early Win сохраняется прежний recipe; All-кандидат хуже на public test. На основании этого эксперимента четыре production-модели оставлены прежними. Ни деплоя, ни изменения betting gates этим экспериментом не выполнялось.

Проверки реализации: регрессии frozen holdout, validation/embargo, целостности и защиты однократного forward; artifact reload; 4×512 serving probes; независимый пересчёт public метрик из сохранённых NPZ. Независимая финальная сверка также воспроизвела все четыре forward NPZ, проверила receipt/status и SHA report/artifact; расхождений нет. Финальные report/artifact SHA согласованы с `DONE`. Все исследовательские harness/tests закоммичены в `b9ae063`; большие corpus/model/scratch остаются локальными артефактами. Временная задержка последнего fit сопровождалась paging (около 8 ГБ swap); процесс не перезапускался.
