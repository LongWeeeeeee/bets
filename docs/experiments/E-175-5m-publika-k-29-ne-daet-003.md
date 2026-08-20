---
id: E-175
title: "5M паблика к боевым 29 не даёт +0.03: ни as-of, ни статика, ни stack"
date: "2026-08-14"
area: ml
status: full
corpus: "482 486 про-карт, тест 26 016; паблик 5 093 540 карт 7.41 (24.03–09.08.2026); база — боевые 29, AUC 0.7142"
verdict: "ADD≥0.03 к 29: 0. Честный потолок паблика +0.0004 (static 4 колонки / stack asof_pub25+syn). As-of пары +0.0002, player-hero −0.0004 (cover 0.209). REPLACE draft ← public encoder −0.0106. Тренды формы и CatBoost-драфт на про: dotaPlusHeroXp +0.0002, REPLACE draft ← CB −0.0134. Uni pub_elo_minus_pro +0.1192 в фазе 1 — это AUC−0.5, не ADD. Кривая конструкция проверена: статика до TEST_FROM, nz-стандартизация и stack на первой трети теста — все ≈0. В прод не клалось."
harness: "`runtime/experiments/misc/undercount_8h.py`, `undercount_8h_p2.py`, `undercount_8h_p3.py`, `undercount_8h_p4.py`"
---

# E-175. Пять миллионов паблика к боевым 29

- **Дата:** 2026-08-14
- **Харнесс:** `runtime/experiments/misc/undercount_8h.py`,
  `undercount_8h_p2.py`, `undercount_8h_p3.py`, `undercount_8h_p4.py`
- **Артефакты:** `runtime/artifacts/misc/undercount_8h.{md,json}`,
  `undercount_8h_p2.{md,json,pairs.npz}`, `undercount_8h_p3.{md,json,cols.npz}`,
  `undercount_8h_p4.{md,json,cols.npz}`
- **База:** боевые 29, тест 26 016, AUC **0.7142**
- **Паблик:** 5 093 540 карт патча 7.41, slim mmap
  `runtime/artifacts/misc/undercount_8h_slim/`

Состояние текущей карты (NW/XP/килы/длительность этой игры) не использовалось.
Метрика — ADD/REPLACE/stack ΔAUC к 29. `uni` = AUC−0.5 колонки, **не HIT**.

## 1. Почему стандартный ADD на паблике учит ноль

Паблик начинается **2026-03-24**, `TEST_FROM` = **2026-03-29**. На train
2016–2026 колонка as-of почти вся нули (~5 дней пересечения, 1 079 про-карт).
`ens_fit` (окна 90/180/365/730) ставит вес ≈ 0. Это и есть «кривая конструкция»
из E-75: там переносили веса паблик-модели; здесь вес учится на про, но вход
на истории пустой.

Фаза 1 это подтвердила: все `ADD pub_*` / `ADD all_elo_*` ≈ 0 или минус
(`pub_n_mean` / `pub_age_days` ≈ **−0.10**). Лучшее честное —
`ADD hybrid+glicko+pub_resid` **+0.0041** (порядок E-168/170, паблик ни при чём).
Late-train 29 на 7.41: n_train=1 079, AUC **0.7039** (−0.0103 к полной 29).
CatBoost 29+pub vs CatBoost 29 **−0.0029**.

Ложные HIT фазы 1 (`uni pub_elo_minus_pro +0.1192` и т.п.) — AUC−0.5, не ADD.

## 2. Фаза 2: as-of 25 пар и forward-драфт

| замер | ΔAUC |
|---|---:|
| ADD pub25_pair_mean | +0.0002 |
| ADD pub_hero_wr_pre | +0.0002 |
| ADD forward_pub_lin / cb | +0.0000 |
| REPLACE draft ← forward_pub_* | −0.0131 |
| REPLACE draft ← pub_hero_wr_pre | −0.0125 |

Forward CatBoost на сырых hero-id (не `DraftFeatureEncoder`) дал uni 0.5336 —
сломанная кодировка, не опровержение энкодера. E-161 уже мерил правильный
expanded public draft: **+0.0006** поверх Hybrid, не +0.03.

## 3. Фаза 3: починка конструкции

Плотная таблица пар/линий/hero-pos по паблику **строго до TEST_FROM** (154 435
карт) — признак определён на всём train. Stack: вес новой колонки на первой
трети теста (там паблик заполнен), оценка на хвосте (база хвоста 0.7137).
Player-hero as-of только для 353 748 про-аккаунтов. Нормальный
`DraftFeatureEncoder` (16 426 колонок) на тех же 154k.

| замер | ΔAUC |
|---|---:|
| ADD static_pub25+lane5+hpos+syn | +0.0004 |
| stack asof_pub25+syn | +0.0004 |
| nzADD pub_player_hero | +0.0003 |
| REPLACE vs_wr ← asof_pub25 | +0.0003 |
| ADD static_pub_hpos | +0.0003 |
| ADD pub_player_hero | −0.0004 |
| stack-cb ALL pub | −0.0025 |
| REPLACE draft ← enc_pub_draft | −0.0106 |
| REPLACE draft ← static_hpos | −0.0122 |

Player-hero cover на тесте **0.209**. Encoder uni 0.5551; REPLACE хуже линейного
`draft_logit` в 29 (тот учился на миллионах, здесь 154k до сплита).

## 4. Фаза 4: тренды формы и CatBoost-драфт на про

Не паблик: as-of `gpm/xpm/hdmg` окно 10 минус 50, `dotaPlusHeroXp` на текущем
(acc, hero), винрейт команды в том же `stype`, CatBoost по 10 героям-категориям
с forward OOF (покрытие теста 1.000).

| замер | ΔAUC |
|---|---:|
| ADD dotaPlusHeroXp | +0.0002 |
| ADD team_stype_wr | +0.0000 |
| ADD gpm/xpm/hdmg trend | −0.0001…−0.0003 |
| ADD cb_hero_oof | −0.0001 |
| REPLACE draft ← cb_hero_oof | −0.0134 |

`team_stype_wr` uni 0.5694, ADD 0.0000 — съеден 29. CatBoost-драфт uni 0.5332,
слабее линейного `draft_logit` (0.6093).

## 5. Вердикт

К боевым 29 из 5M паблика и из трендов/нелинейного драфта на про **нет**
признака с ADD ≥ 0.03. Потолок измеренного паблик-добавления **+0.0004**.
Конструкция «вес на нулях» не прятала сигнал: три протокола, где паблик
плотный, дали тот же ноль. Это согласуется с E-75, E-161 (+0.0006) и E-172
(к 29 потолок Hybrid +0.0029).

В прод не клалось.
