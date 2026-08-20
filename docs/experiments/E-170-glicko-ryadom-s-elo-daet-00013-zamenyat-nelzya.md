---
id: E-170
title: "Glicko рядом с ELO даёт +0.0013, заменять нельзя; поверх Hybrid ещё +0.0011"
date: "2026-08-13"
area: ml
status: full
corpus: "482 486 про-карт, проверка 26 016; база — боевые 29 признаков, AUC 0.7142"
verdict: "В боевых 29 нет Glicko/TrueSkill — их не строили, в партиях 1–11 их нет. ADD glicko +0.0013, ADD trueskill +0.0013. REPLACE elo ← glicko −0.0007, ← trueskill −0.0004: это не замена простого ELO (corr 0.701 / 0.539) и не замена Hybrid (corr 0.675 / 0.553). Поверх hybrid_str (+0.0029) Glicko добавляет ещё +0.0011 до +0.0040; TrueSkill рядом с Glicko ноль (corr 0.761). glicko_p дублирует glicko (corr 0.975). Expanded public draft на 13 653 (все внутри теста): полный EXPANDED 0.7179 против G29 0.7173, как E-161; как колонку в 29 не положить — на train значений нет. В прод не клалось."
harness: "`runtime/experiments/misc/undercount_glicko.py`"
---

# E-170. Glicko — то, чего в 29 нет в принципе

- **Дата:** 2026-08-13 (alex: «еще еще» — после исчерпания партий E-169)
- **Харнесс:** `runtime/experiments/misc/undercount_glicko.py`
- **Артефакты:** `runtime/artifacts/misc/undercount_glicko.{md,json,log,features.npz}`
- **База:** боевые 29, тест 26 016, AUC **0.7142**

## 1. Что это значит

Партии 1–11 Glicko не считали. В 29 стоит простой player ELO (K=24, без RD).
Glicko-1 as-of по тем же `accounts` — другая рейтинговая система: есть RD и
распад неопределённости со временем. Corr с ELO **0.701**, с Hybrid **0.675**,
с драфт-логитом **0.033**. Поэтому его нельзя подставить вместо ELO: замена
**−0.0007**. Рядом он даёт **+0.0013**.

TrueSkill то же самое как одиночная добавка (+0.0013), ещё дальше от ELO
(corr 0.539). Рядом с Glicko он не нужен (corr 0.761, вместе +0.0016 против
0.0013 у одного Glicko). Позиционный Glicko слабее общего (+0.0010) и сильнее
вредит как замена (−0.0013).

Поверх Hybrid, который в E-168 забирал +0.0029: `hybrid_str + glicko` = **+0.0040**.
Glicko не съеден Hybrid. Это недобор «фичи нет», не кривая конструкция ELO.

## 2. Цифры

| замер | ΔAUC |
|---|---:|
| ADD glicko | **+0.0013** |
| ADD glicko_p (ожидание с RD) | +0.0013 |
| ADD trueskill | +0.0013 |
| ADD pos_glicko | +0.0010 |
| ADD trueskill_sig | +0.0004 |
| ADD glicko_rd | +0.0002 |
| REPLACE elo ← glicko | **−0.0007** |
| REPLACE elo ← trueskill | −0.0004 |
| REPLACE elo ← pos_glicko | −0.0013 |
| COMBINED glicko+p+rd+pos+ts | +0.0021 |
| ADD hybrid_str (повтор E-168) | +0.0029 |
| ADD hybrid_str+glicko | **+0.0040** |
| ADD hybrid_str+ts | +0.0037 |
| ADD glicko+ts | +0.0016 |
| ADD hybrid_str+glicko+ts | +0.0040 |
| ADD hybrid_str+glicko+ts+sig | +0.0047 |

## 3. Expanded draft — не колонка

Все 13 653 OOF-карты expanded/current-only лежат внутри теста 26 016.
На train логитов нет, линейная модель выучит вес 0.

| скор на 13 653 | AUC |
|---|---:|
| G29 | 0.7173 |
| EXPANDED | 0.7179 |
| BASE_2M | 0.7173 |
| M_ALL / M_CP / M_SYN | 0.7173 / 0.7168 / 0.7170 |

+0.0006 у EXPANDED против G29 на этом срезе совпадает с E-161 (expanded vs
current-only Hybrid-стек). `mean(G29, EXPANDED)=0.7208` — смесь двух полных
моделей на OOF, не признак; в 29 так не кладётся.

corr(G29, EXPANDED)=0.949; corr(draft_logit, EXPANDED)=0.307.

## 4. Где искать ошибку

- Glicko-1, не Glicko-2: волатильность фиксирована, период распада RD — месяц,
  c=63.2, старт 1500/350; апдейт каждого игрока против среднего рейтинга
  пятёрки соперника, как K-ELO в `pro_features_wide`;
- TrueSkill — двухкомандная схема Weng-Lin, не полная фактор-графика;
- as-of: состояние выписывается до апдейта текущей карты; порядок корпуса
  хронологический;
- изоляция поверх Hybrid — отдельный прогон `ens_fit` после харнесса, не в нём;
- hybrid_vs_elo на тесте 0.929 против 0.920 в E-168 (там, предположительно, не
  только test);
- `trueskill_sig` +0.0007 сверх hybrid+glicko одним фитом, без повторной проверки;
- expanded нельзя честно ADD без пересчёта логитов на всём train.
