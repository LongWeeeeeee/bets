---
id: E-162
title: "Порог hero-role pair уже неактивен, а нормализация C вредит current draft"
date: "2026-08-13"
area: ml
status: full
corpus: "5 093 540 public draft-карт; strict-forward fit по окнам 2 082 721 / 2 763 879 / 3 729 094; 13 653 exact pro OOF-карты"
verdict: "ОТВЕРГНУТО. Снижение pair_min_support 30 -> 5 даёт побайтно те же 16 764 колонки, draft logits и финальные predictions: AUC 0.717891 -> 0.717891, CI [0,0]. Нормализация liblinear C по числу train-строк (0.003*2M/fit_n) ухудшает AUC до 0.717689: −0.000203, series-CI95 [−0.000299,−0.000104], отрицательно во всех трёх окнах. Вход — только десять героев текущей карты по позициям; date/patch/epoch/time и kills не используются"
harness: "`runtime/experiments/misc/map_winner_expanded_draft_support5_forward.py`, `map_winner_expanded_draft_cnormalized_forward.py`"
---

# E-162. Старые настройки draft-encoder не прячут новый current edge

## Вопрос

E-161 поднял нынешнюю модель до 0.717891 заменой последних 2 млн public-карт
на весь причинно доступный до каждого test-окна корпус. Но encoder остался со
старыми настройками E-96: `pair_min_support=30`, `liblinear C=0.003`.

Проверены две конструкции, которые могли вести себя иначе на 2–3.7 млн карт:

1. снизить поддержку hero-role pair с 30 до 5 — этот вариант был сильнее в
   маленьком pro-fit E-90, но не проверялся поверх полной current-модели;
2. сохранить силу L2-штрафа при росте train-корпуса правилом
   `C_window = 0.003 × 2 000 000 / fit_n`. Правило задано из старого 2M
   objective до просмотра outer-окон.

Во всех вариантах признаки текущей карты неизменны: десять героев по пяти
позициям каждой стороны. Timestamp нужен только для `public_ts < test_min_ts`;
date, patch, epoch, time, kills, KDA и farm в модель не входят.

## Порог поддержки пары: exact ноль

| окно | public fit | колонок support=30/5 | AUC support=30 | AUC support=5 |
|---:|---:|---:|---:|---:|
| 1 | 2 082 721 | 16 764 / 16 764 | 0.725221 | 0.725221 |
| 2 | 2 763 879 | 16 764 / 16 764 | 0.713248 | 0.713248 |
| 3 | 3 729 094 | 16 764 / 16 764 | 0.715076 | 0.715076 |
| **pooled** | — | — | **0.717891** | **0.717891** |

Все три draft-logit checkpoint и все 13 653 финальных predictions совпали
побайтно (`max abs diff = 0`). При 127 героях 16 764 колонки — полный signed
дизайн `6H + 2*C(H,2)`; на миллионах карт все 8 001 unordered hero pairs уже
имеют минимум 30 наблюдений. Поэтому bootstrap любой серии детерминированно
даёт Δ=0: CI95 `[0,0]`, one-sided p=1, `P(Δ>0)=0`.

## Нормализация C: хуже во всех окнах

| окно | effective C | fixed C=0.003 | normalized C | ΔAUC |
|---:|---:|---:|---:|---:|
| 1 | 0.00288085 | 0.725221 | 0.725203 | −0.000019 |
| 2 | 0.00217086 | 0.713248 | 0.713035 | −0.000213 |
| 3 | 0.00160897 | 0.715076 | 0.714681 | −0.000395 |
| **pooled** | — | **0.717891** | **0.717689** | **−0.000203** |

Series-cluster bootstrap 10 000×: CI95
`[−0.000299,−0.000104]`, one-sided p улучшения = 1, `P(Δ>0)=0`.
Top-10 WR не изменился (90.33%); top-25 снизился на 0.029 п.п.

Это измеряет только проверенное правило нормализации. Причина отрицательного
результата отдельно не измерена; из него нельзя заключать, что любое другое C
обязательно хуже. Честный вывод уже: старый fixed `C=0.003` заменять этим
размерно-нормализованным правилом не надо.

## Проверка

- support5: verifier 16/16, полный hash provenance, `COMPACT` участвует в
  checkpoint invalidation, draft checkpoint identity проверяется напрямую;
- normalized C: verifier 15/15, exact rows/mids/target/baseline, strict as-of,
  полный provenance и независимый replay всех 10 000 series draws;
- все fit завершились без warnings;
- вычисление выполнено локально; serv1 не использовался из-за load выше числа
  ядер и 94% диска, serv2 оставался недоступен.

## Артефакты

- `runtime/artifacts/misc/map_winner_expanded_draft_support5_forward/`;
- `runtime/artifacts/misc/map_winner_expanded_draft_cnormalized_forward/`.
