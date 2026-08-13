---
id: E-141
title: "Causal Hybrid prematch prior добавляет +0.0010 AUC внутри live-модели"
date: "2026-08-13"
area: ml
status: full
corpus: "13 653 OOF про-карты / 79 202 строки карта×минута; live-train prior — 25 225 causal OOF-карт; три непересекающихся forward-окна; 11 435 series/match-кластеров"
verdict: "Цель — didRadiantWin; total-kills target и kills-фильтра нет. Первое вычисление +0.00119 отозвано: Reviewer нашёл in-sample prematch predictions в live-train и future-duration filter minute+3. После полного исправления live state даёт 0.8347 AUC, state+E99 — 0.8480, state+E99+level/denies+production Hybrid — 0.8490. Чистый эффект нового prior против E99: +0.00099, series-bootstrap 95% ДИ [+0.00034,+0.00164], положителен во всех трёх окнах (+0.00018,+0.00082,+0.00197). Train prior теперь только nested/canonical OOF, strict as-of нарушений 0; eligibility — duration > текущая минута, без знания будущей длительности. Независимый readback 15/15 PASS"
harness: "`runtime/experiments/misc/map_winner_live_new_prior_forward.py`"
---

# E-141. Новый prematch prior сохраняет часть выигрыша внутри live

## Вопрос

E-137 поднял честный prematch с 0.7133 до 0.7166 за счёт exact production
`HybridPlayerRosterEloModel`. Проверено, переживает ли эта прибавка сильное
состояние уже идущей карты: networth, XP, текущую разницу убийств и динамику.

Цель — только **победа на карте** (`didRadiantWin`). Условия по total kills и
таргета убийств нет. Текущая разница убийств остаётся обычным live-state
признаком, как networth или XP.

## Исправление первого прогона

Первый артефакт показывал +0.00119, но независимый Reviewer нашёл два P1:

1. prematch-модель обучалась на всех прошлых картах окна, а её in-sample
   predictions подавались в live-train;
2. строка минуты `t` включалась только при `duration > t+3`, то есть отбор знал,
   что карта проживёт ещё три минуты.

Этот результат отозван и полностью пересчитан. Исправленный протокол:

- для live-train используются только expanding nested OOF и уже прошедшие
  canonical outer OOF; warm-up без OOF исключён для всех трёх моделей;
- в causal prematch-кэше 25 225 карт, у каждой `prior_asof < map_timestamp`;
- test prior побитово совпадает с уже проверенными canonical E99/E-137 OOF;
- строка существует только если состояние доступно сейчас:
  `duration_minutes > minute`, без дополнительного future-buffer;
- state-only, state+E99 и state+current обучаются на строго одинаковых строках.

## Результат после исправления

| модель | AUC | WR top-10% | WR top-25% |
|---|---:|---:|---:|
| live state | 0.8347 | 98.4% | 94.6% |
| state + E99 prior | 0.8480 | 98.8% | 95.6% |
| **state + новый Hybrid prior** | **0.8490** | **98.8%** | **95.7%** |

| сравнение | ΔAUC | series-bootstrap 95% ДИ |
|---|---:|---:|
| E99 prior − state | +0.01327 | [+0.01097,+0.01567] |
| новый prior − state | +0.01426 | [+0.01197,+0.01646] |
| **новый prior − E99 prior** | **+0.00099** | **[+0.00034,+0.00164]** |

По трём окнам дополнительный эффект нового prior: `+0.00018`, `+0.00082`,
`+0.00197`. Значит улучшение prematch не исчезает после добавления live-state,
хотя основной live-лифт уже находится в E99.

Абсолютные AUC выше старого прогона не из-за исправленного Hybrid: удаление
future-duration filter добавило 5 699 воспроизводимых строк и causal OOF изменил
live-train population. Корректно сравнивать только три модели внутри новой
таблицы на одинаковых строках.

## Где перенос сильнее — только descriptive

| минута | \|NW lead\| | строк | state+E99 | state+новый | разница |
|---|---|---:|---:|---:|---:|
| 10 | 0–2k | 8 835 | 0.7045 | 0.7076 | +0.0032 |
| 20 | 0–2k | 3 173 | 0.6552 | 0.6617 | +0.0065 |
| 30 | 0–2k | 1 341 | 0.6074 | 0.6111 | +0.0037 |
| 40 | 0–2k | 560 | 0.6105 | 0.6081 | −0.0024 |
| 20 | 6k+ | 4 930 | 0.9549 | 0.9546 | −0.0003 |
| 30 | 6k+ | 7 125 | 0.9430 | 0.9431 | +0.0001 |

У этих разрезов нет отдельного cluster CI. На 40-й минуте при 0–2k знак
отрицательный и всего 560 строк, поэтому таблица локализует возможный эффект,
но не служит отдельным доказательством.

## Проверка

Независимый readback: **15/15 PASS**.

- 79 202 строки и 13 653 уникальные карты;
- causal prematch-кэш: 25 225 карт, in-sample source codes отсутствуют;
- strict as-of violations: 0;
- canonical external OOF max abs diff: 0;
- `duration > minute`, non-positive duration margins: 0;
- одинаковые строки у всех трёх моделей;
- finite predictions, fit warnings: 0;
- synthetic causality/eligibility tests: PASS;
- pooled AUC и 1 000× series-bootstrap пересчитаны независимо.

## Вывод

Практический путь подтверждён на двух этажах:

1. prematch: E99+level/denies `0.7133` → +production Hybrid `0.7166`;
2. live: causal upgrade даёт ещё `+0.0010` поверх state+E99.

Это исследование ranking AUC результата карты, не проверка betting edge против
архивной линии. Перенос на будущий untouched период всё ещё обязателен.
