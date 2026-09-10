---
id: E-266
title: "Общий ML Laning: перевес команды по золоту на 10-й минуте"
date: "2026-09-09"
area: ml
status: full
corpus: "E264: 6 692 272 public-карты; train 800k, validation 100k, отдельный test 100k"
verdict: "Готова history600 + temperature0.854316: test100k accuracy63.925%, loss0.634192 против draft0.646181; day95CI разности [-0.013076,-0.010751]. Public, не pro."
harness: "scripts/ops/train_team_laning_model.py; scripts/run/train_team_laning.sh"
---

# E-266 — общий прогноз золота к 10-й минуте

## Цель и фиксированный протокол

Alex выбрал цель «перевес команды по золоту на 10-й минуте» для одной строки
`ML Laning`. Это отдельная модель, а не среднее вероятностей трёх линий E264/E265
и не вероятность победы карты All. Три существующих lane-модели не переобучаются.

Target — знак `team_nw10`, то есть `radiantNetworthLeads[10]` исходного STRATZ:
классы `[Dire, точное равенство, Radiant]`. Все 6 692 272 карты имеют конечное
значение: 3 188 223 Dire, 1 292 равенства, 3 502 757 Radiant. Равенство не
приписывается одной из сторон; его доля ~0.019%, отдельные метрики этого класса
будут иметь большую неопределённость. Фактическое золото текущей карты служит
только target, в предматчевые признаки не входит.

Ровно два кандидата CatBoost: весь позиционный драфт 10 героев; тот же драфт
с историями 10 игроков по роли и герою, all-time и 30 дней, плюс разности
Radiant-minus-Dire для каждой позиции. Истории сохраняют контракт E265:
только завершённые карты `end < start - 3600`; неизвестный account даёт нули.
Истории содержат прежние исходы линий, без исходов текущей карты. Stacking
по in-sample прогнозам трёх линий не используется.

Train 800k / validation 100k из E265 сохраняют прежние временные границы
13 июля / 13 августа UTC и purge по окончанию матча. До fit резервируются
100k test IDs с исключением обоих уже просмотренных тестов E264 и E265.
Это новые IDs того же исторического периода, а не новый prospective период.
Признаки и параметры замораживаются до просмотра нового теста.

Параметры: depth6, learning_rate0.08, максимум600 деревьев, early stop60,
четыре потока. Выбор по validation log loss; scalar temperature также только
на validation. Selection записывается перед test predictions/metrics.
На тесте: log loss, accuracy, AUC направления, калибровка, baseline частот
train, paired day-bootstrap разности loss; артефакт проверяется повторной
загрузкой. Новые параметры по результатам теста не подбираются.

## Запуск

```bash
nohup bash scripts/run/train_team_laning.sh 20260909_team_nw10_v1 > runtime/artifacts/laning/20260909_team_nw10_v1/run.log 2>&1 &
echo $!
```

Модель/прогнозы/план: `data/laning_models/20260909_team_nw10_v1/`.
Статус/лог: `runtime/artifacts/laning/20260909_team_nw10_v1/`.
Полный fit завершён 10.09.2026 за ~4 минуты: draft600 деревьев — 27с, history600 — 130с. Train800k / validation100k / новый test100k; все цели конечны. Оба fit дошли до лимита деревьев; это не доказательство насыщения модели.

## Serving

`base/laning_serving.py` загружает selected `team.cbm` и immutable mmap
`base/laning_history_store.py`. История экспортируется из E264 отдельными
событиями для каждой роли; запрос использует timestamp текущей карты и строгий
causal cutoff. Кэш учитывает героев, аккаунты и timestamp; проценты не связываются
с округлённым ensemble-индексом. Последняя исходная карта закончилась 04.09.2026;
это фиксированный снимок, автоматического обновления настоящая задача не добавляет.

Строка All получает standalone вероятность существующего `WIN_MODEL_DIR`
через `win_model_veto.win_index_draft`, до смешивания в prematch ensemble.
Обе строки только для отображения. Отказ любой из них не меняет гейты ставок.
На serv1 подтверждён E260 `data/draft_phase_serving/2026-09-05_position_pairs/all`.

## Где искать ошибку

1. `team_nw10`: знак Radiant, исходный индекс10; не итоговый networth игрока
   и не минута завершения игры. Проверить raw sample при расхождении.
2. Все 10 героев и аккаунтов — R1..5,D1..5, позиции совпадают с corpus.
3. Histories: `end < timestamp - 3600`, lower bound 30 дней включительно,
   Dire-score перевёрнут на сторону игрока; mmap сверить с `build_history`.
4. Дедуп IDs, оба исключённых теста, purge, selection до test, temperature
   только validation, код/артефакты совпадают с записанными хешами.
5. All — исходная draft probability, не ensemble index; проверить несколько
   карт с одинаковым ensemble index, разные прогнозы не должны смешиваться.
6. Public-позиции размечены после матча, в проде нужны известные корректные
   позиции; задержка поступления истории моделируется, не измерена. Public
   holdout не доказывает точность на pro и не оценивает прибыльность ставок.

## Результат и проверки

Выбрана history по validation loss0.634053 против draft0.645459; температура
0.8543161738 выбрана только на validation. Selection записан до теста; исходные
хеши feature/trainer совпали после завершения. Новые настройки по тесту не подбирались.

| Модель | Test log loss | Accuracy3 | Brier sum |
|---|---:|---:|---:|
| Частоты train | 0.693905 | 52.320% | 0.499124 |
| Только драфт | 0.646181 | 62.620% | 0.453328 |
| Драфт + история | 0.635137 | 63.925% | 0.443269 |
| Выбранная + calibration | 0.634192 | 63.925% | 0.442428 |

В тесте 47 659 Dire / 21 exact tie / 52 320 Radiant, 22 дня. Независимый
пересчёт всех loss/accuracy из NPZ совпал. Direction AUC без точных равенств
0.693230; top-label ECE10=0.002507. Дополнительный paired day-bootstrap5000:
selected-minus-draft loss=-0.011989,95CI[-0.013076,-0.010751]. Против частот train
основной bootstrap2000 дал loss=-0.059712,95CI[-0.061448,-0.057681].

Выбранный `team.cbm` SHA256:
`c02aa53f2d3119ed9b310dd9886b4dc39db61aac04c3386c9837a673565002ce`.
32 профильных теста прошли; mmap на 64 случайных картах (7 680 значений) побитно совпал с обучающей историей, SHA256 всех 25 файлов проверены локально и на serv1. Выбранная модель и истории доставлены на serv1; код f1078dc получен через origin, py_compile на Python3.12 и preflight прошли. Systemd restart через scripts/run/restart_cyberscore.sh выполнен 10.09.2026 21:56:49 MSK: прежний PID382651 → новый397023, active/running, NRestarts0, ровно один экземпляр; map_id_check очищен штатным скриптом. Логи не усекались. Loader загружается однократно: после ошибки загрузки нужен рестарт с исправленным комплектом артефактов. Integrity проверяется отдельным preflight `base/tools/verify_laning_serving.py` до рестарта; штатный mmap loader не сканирует 750 MB при каждом старте.

Actual local serving:64 frozen probe rows, history delta0, probability delta1.11e-16; All512 probes delta<=4.99e-6 (rounding existing index). Медианная latency4.8ms,max15.6ms. Reports: `runtime/artifacts/laning/20260909_team_nw10_v1/{history_audit,independent_metrics,local_serving_audit}.json`.

Production preflight:64 probes,history delta0,probability delta1.67e-16;All512 probes delta4.99e-6;median13.4ms,max29.9ms. SHA модели/историй/All совпадают с local. Evidence: `runtime/artifacts/laning/20260909_team_nw10_v1/production_serving_audit.json`, `pre_restart.json`, `post_restart.json`. Откат отображения: LANING_MODEL_ENABLED=0 с тем же systemd restart; прежние артефакты и betting gates не заменялись.
