---
id: E-176
title: "Новые конструкции к 29: смесь, пики, chemistry, SVD — потолок всё ещё Hybrid+Glicko +0.0040"
date: "2026-08-14"
area: ml
status: full
corpus: "482 486 про-карт, тест 26 016; pickbans 26 015 тестовых карт; база — боевые 29, AUC 0.7142"
verdict: "ADD≥0.03: 0. Якорь hybrid+glicko +0.0040 (как E-170). Новое: hybrid/|draft| +0.0030, career_days +0.0005, exact-5 +0.0003. Порядок пиков (сторона и герой) 0.0000 даже в stack на тесте, где pickbans есть у 26 015/26 016. Смесь 29 по |elo|/stype −0.0044/−0.0034. CatBoost 29 −0.0074. SVD героев с паблика −0.0002. OOF forward_predictions uni 0.7124, ADD +0.0002. В прод не клалось."
harness: "`runtime/experiments/misc/undercount_8h_p5.py`, `undercount_8h_p5b.py`"
---

# E-176. Конструкции, которых не было в E-172/E-175

- **Дата:** 2026-08-14
- **Харнесс:** `runtime/experiments/misc/undercount_8h_p5.py`,
  `undercount_8h_p5b.py`
- **Артефакты:** `runtime/artifacts/misc/undercount_8h_p5.{md,json,log}`,
  `undercount_8h_p5b.{md,json}`
- **База:** боевые 29, AUC **0.7142**, хвост теста 0.7137
- Без состояния текущей карты. Не повтор колонок E-172.

## 1. Что мерили

| семья | зачем | лучший ΔAUC |
|---|---|---:|
| OOF `forward_predictions` (= `lan_model_P`) | чужая модель, uni 0.7124, finite train 0.206 / test 1.0 | +0.0002 |
| ext не из 29 (rest, cohesion, career, spread, tier1, hero_wr, …) | выпали из 14 E-77 | career_days **+0.0005** |
| произведения draft×form/gpm, elo×form/vs | нелинейность внутри 29 | ≤ +0.0000 |
| hybrid / (1+\|draft\|) | E-171: Hybrid силён, когда драфт молчит | **+0.0030** |
| hybrid+glicko | якорь E-170 | **+0.0040** |
| порядок пиков ±1 сторона | idea 18, pickbans на всём тесте | 0.0000 (stack тоже) |
| first/last/phase1 **герой** × as-of WR | та же идея, не сторона | 0.0000 |
| exact-5 WR + pairwise games | roster chemistry | exact5 **+0.0003** |
| смесь: отдельная 29 на половинах \|elo\| / stype | одна линейная модель плохо специфицирована | −0.0044 / −0.0034 |
| только окно 180д вместо ансамбля | | +0.0002 |
| CatBoost 29 и 29+hybrid+glicko | деревья vs линейный | −0.0074 / −0.0032 |
| SVD-16 героев с паблика до TEST_FROM | эмбеддинг драфта | −0.0002 |

## 2. Пики

`pickbans.jsonl` — 26 015 строк, джойн на тест 26 015 / 26 016. На train колонка
нулевая, поэтому ADD ens_fit обязан быть ~0; stack на хвосте теста — честный
протокол. И сторона (±1), и сила героя first/last/ban/phase1: все **0.0000**.
uni pick_mean_wr 0.5383 — слабый драфт, уже сидит в `draft_logit`/`wr30`.

## 3. Вердикт

Порог +0.03 к боевым 29 этими конструкциями не берётся. Живой потолок
прежний: Hybrid+Glicko **+0.0040**. Взаимодействие Hybrid с тихим драфтом
даёт +0.0030, не новый источник. Смесь и бустинг линейную 29 не бьют.

В прод не клалось.
