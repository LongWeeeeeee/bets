---
id: E-17
title: "Метрики блока Mix по бакетам индекса"
date: "2026-08-05"
area: star-dispatch
status: full
corpus: "holdout 50k"
verdict: "измерено"
harness: "`runtime/experiments/misc/holdout_mix_metrics.py` - **Команда:** `MIX_MAX=50000 venv_catboost/bin/python3 runtime/experiments/misc/holdout_mix_metrics.py` (кэш — `runtime/artifacts"
---

# E-17. Метрики блока Mix по бакетам индекса
- **Дата:** 2026-08-05
- **Гипотеза:** посмотреть, что вообще предсказывают четыре метрики ProTracker
  и как винрейт зависит от величины индекса. DLTV не мерился: голосование
  существует только для про-матчей.
- **Как мерили:** holdout 50k, метка — победитель карты; значения считаются
  прод-путём `enrich_with_pro_tracker` + `_build_dota2protracker_star_output`.
  Источник данных — **снимок кэша protracker** (128 героев, 05.08), а НЕ наши
  словари: `_traintest/` в этом замере не участвует.
- **Харнесс:** `runtime/experiments/misc/holdout_mix_metrics.py`
- **Команда:** `MIX_MAX=50000 venv_catboost/bin/python3
  runtime/experiments/misc/holdout_mix_metrics.py`
  (кэш — `runtime/artifacts/misc/protracker_cache_20260805/`, снят с serv1)
- **Артефакт:** `runtime/artifacts/misc/holdout_mix_metrics.md`
- **Результат:** все четыре метрики дают монотонную лестницу винрейта.
  `Protracker_duo` покрытие 87%, AUC **0.6322**, верхний бакет |идx|>=10 —
  13 695 карт при **67.8%**; `Protracker_1vs1` 94%, AUC **0.6266**,
  51.6% -> 84.8% по бакетам; `Protracker_solo` 99%, AUC 0.5727;
  `Protracker_solo_overall` 100%, AUC 0.5697. Для сравнения, наши словарные
  метрики на том же holdout в блоке All: cp1vs1 0.5970, synergy_duo 0.5846.
- **Вердикт:** открыто. Два наблюдения на будущее: (1) самая сильная метрика
  блока — `Protracker_duo` — в STAR НЕ входит, только `cp1vs1`; (2) пороги
  cp1vs1 (3/5/8 для WR60/65/70) занижают фактическую точность на 4-9 п.п.
  (при 3-4 -> 63.9%, при 5-7 -> 72.6%, при 7-10 -> 78.9%).
- **Где искать ошибку:** (1) кэш protracker протухает по календарному дню и
  тогда лезет в сеть — харнесс подменяет `parse_hero_matchups` на офлайн-чтение
  снимка, без этого замер начнёт ходить в Camoufox; (2) данные protracker
  собраны с высокоранговых ПАБЛИК-матчей, а holdout оттуда же — это оценка на
  своём распределении, и прецедент разрыва с про уже есть
  ([[post-lane-solo-pro-nontransfer]]: паблик WR60, про 50.4%);
  (3) метрика берёт `_late`-вариант с фолбэком на `_early` — если поменять
  фазу, числа поедут.
- **Что делать дальше:** прежде чем вводить `Protracker_duo` в STAR или трогать
  пороги — замер на про-корпусе, затем на прод-отборе (правило 1).
