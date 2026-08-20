---
id: E-172
title: "Десять фич с +0.03 есть только к draft-only; к боевым 29 ни одной"
date: "2026-08-14"
area: ml
status: full
corpus: "482 486 про-карт compact∩rich, проверка 26 016; базы — боевые 29 (0.7142) и один draft_logit (0.6093)"
verdict: "К боевым 29 ADD≥0.03: 0, потолок Hybrid +0.0029. К одному draft_logit найдено 23 неиспользуемых колонки ≥+0.03 (без состояния текущей карты). Десять различных: hybrid_str +0.0730, glicko +0.0680, pos_glicko +0.0627, trueskill +0.0558, ctx_pt_x_elo +0.0540, v_gpm30 +0.0493, st_q_rating +0.0474, i1_elo_core +0.0463, ext_winrate +0.0387, a_xpm_rel_pos +0.0341. Это сила/форма, которых нет в одном драфт-логите; в 29 они уже съедены. f37_pred_nw10 +0.0736 к draft — сжатие G, не новый вход (на 29 −0.0005, E-169). В прод не клалось."
harness: "`runtime/experiments/misc/undercount_prematch03.py` + `undercount_prematch03b.py`"
---

# E-172. Десять фич +0.03 — только к draft-only

- **Дата:** 2026-08-14 (alex: «Найди 10 фич дающих каждая хотя бы плюс +0.03 без ingame данных»)
- **Харнесс:** `runtime/experiments/misc/undercount_prematch03.py`,
  `runtime/experiments/misc/undercount_prematch03b.py`
- **Артефакты:** `runtime/artifacts/misc/undercount_prematch03.{md,json}`,
  `runtime/artifacts/misc/undercount_prematch03b.{md,json}`
- **Базы:** боевые 29, AUC **0.7142**; один `draft_logit`, AUC **0.6093**.
  Состояние текущей карты (NW/XP/килы/длительность этой игры) не использовалось.

## 1. К боевым 29

Промерены 69 неиспользуемых предматчевых колонок (Hybrid, Glicko/TrueSkill,
контекст 50–53, стрики, стиль, качество предыдущей карты, leftover партий 1–11).
ADD ≥ 0.03: **0**. Топ:

| замер | ΔAUC к 29 |
|---|---:|
| hybrid_str | +0.0029 |
| hybrid_logit | +0.0026 |
| glicko / glicko_p / trueskill | +0.0013 |
| pos_glicko | +0.0010 |
| glicko_g_max | +0.0009 |

Это тот же потолок, что E-168…E-171. Партии 1–11 на 29 закрыты в E-169
(одиночный максимум +0.0003).

## 2. Десять фич с ADD ≥ 0.03 к одному draft_logit

Не в боевых 29. Каждая — отдельный ADD, не жадный набор.

| # | замер | ΔAUC к draft | что это |
|---|---|---:|---|
| 1 | `hybrid_str` | +0.0730 | as-of Hybrid-рейтинг (E-168) |
| 2 | `glicko` | +0.0680 | Glicko-1 as-of (E-170) |
| 3 | `pos_glicko` | +0.0627 | Glicko по позиции |
| 4 | `trueskill` | +0.0558 | TrueSkill μ as-of |
| 5 | `ctx_pt_x_elo` | +0.0540 | дни с патча × разница ELO |
| 6 | `v_gpm30` | +0.0493 | gpm к норме позиции, окно 30 (в 29 лежит EWMA, не это окно) |
| 7 | `st_q_rating` | +0.0474 | quality-рейтинг из E-115 |
| 8 | `i1_elo_core` | +0.0463 | ELO только кора (pos1–3) |
| 9 | `ext_winrate` | +0.0387 | пожизненный винрейт со шринкеджем |
| 10 | `a_xpm_rel_pos` | +0.0341 | XPM к норме позиции |

Ещё ≥0.03 и не в 29, но ближе к клонам/окнам тех же семейств:
`hybrid_logit` +0.0714, `glicko_p` +0.0668, `glicko_g_max` +0.0534,
`v_gpm10` +0.0460, `i1_elo_pos` +0.0462, `i1_elo_max` +0.0449,
`glicko_g_min` +0.0426, `v_gpm_core` +0.0411, `st_q_rating_opp` +0.0338,
`i1_elo_supp` +0.0312, `a_nw_rel_pos` +0.0301.

Уже в 29 и тоже ≥0.03 к draft: `elo` +0.0661, `gpm_ewma` +0.0507,
`gpm_rel_pos` +0.0466, `opp_elo` +0.0376, `kda_player` +0.0342,
`pos_games` +0.0332.

`f37_pred_nw10` +0.0736 / `f37_pred_dur` +0.0621 к draft — Ridge по G на
промежуточную цель. На 29 это −0.0005 (E-169): не новый вход, а сжатие уже
вложенного.

## 3. Что из этого следует

Порог +0.03 к боевым 29 предматчем без состояния текущей карты **не берётся**:
всё, что даёт такие плюсы к голому драфту, уже сидит в 29 как ELO/форма/gpm
или дублирует их (Hybrid/Glicko/q_rating). В прод не клалось.
