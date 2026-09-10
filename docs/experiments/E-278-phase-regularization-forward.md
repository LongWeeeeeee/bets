---
id: E-278
title: "Аудит четырёх phase-моделей и более сильная регуляризация"
date: "2026-09-10"
area: ml
status: full
corpus: "Public E-260; 141 новая pro-карта после предыдущего cutoff"
verdict: "Serving SHA и 4x512 probes PASS. Обучение идёт; протокол заранее фиксирует end-time embargo, validation-only выбор и однократную проверку новых pro ID. Улучшение пока не установлено."
harness: "base/tools/build_fresh_phase_holdout.py; base/tools/tune_draft_phase_models.py"
---

# E-278 — четыре модели: serving, регуляризация и новые pro-карты

Запрос: проверить Early NW, Early Win, All, Late в проде и попробовать улучшить точность. Прод в этом эксперименте не изменяется. Исходные модели E-260 используют только десять hero ID по позициям; признаки игроков/ELO и новые цели не добавляются.

## Проверка действующего serving

10.09.2026: процесс serv1 PID 401516, HEAD `afae954`, active/running, NRestarts=0. Пути взяты из фактического `/proc/<pid>/environ`, Early Win использует свой штатный default. Четыре encoder/model SHA совпали с E-260 manifest; по 512 реальных драфтов на модель дали raw delta ≤1.11e-16. Через production readers отличие только от округления (All ≤4.99e-6; остальные ≤5e-7). Это проверка файлов и вызова ридеров в отдельном процессе, а не новая оценка качества.

Evidence: `runtime/artifacts/draft-cp/20260910_phase_improvement/production_audit.json` и `.log`. Сообщение All о старой калибровке порогов вето сохранено; менять шкалу/пороги в рамках offline исследования нельзя.

## Замороженный протокол до результатов

- Current binary targets: Early NW — сторона при наличии маркера 20–28 минут; Early Win — победитель среди карт 20–34 минуты; All — победитель при длительности ≥20 минут; Late — победитель при ≥36 минутах. NW occurrence не участвует в текущей строке и здесь не переобучается.
- Public E-260 заканчивается 04.09.2026. Его test уже опубликован; повторная проверка на нём — **диагностика**, не новый независимый test.
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
