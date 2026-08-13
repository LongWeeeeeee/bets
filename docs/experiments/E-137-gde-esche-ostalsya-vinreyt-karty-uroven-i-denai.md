---
id: E-137
title: "Где ещё остался винрейт карты: production Hybrid ELO дал +0.0032 поверх level/denies"
date: "2026-08-12"
area: ml
status: full
corpus: "482 486 про-карт; exact Hybrid-прогрев на 922 069 уникальных исторических картах; исправленный forward-протокол E-96: три непересекающихся окна после 26.05.2026, 13 653 OOF-карты после исключения 1 984 строк с repeated timestamp; pickBans snapshot — 11 263 карты / 4 505 rolling OOF"
verdict: "Цель во всех новых прогонах — didRadiantWin, total-kills target и kills-фильтры не используются. Исправленная полная предматчевая модель даёт 0.7121 AUC; level/denies поднимают её до 0.7133, +0.0013 [95% ДИ +0.0001,+0.0025]. Exact production HybridPlayerRosterElo поверх E99+level/denies даёт 0.7166, ещё +0.0032 [95% ДИ +0.0011,+0.0055], положительно во всех трёх окнах. После удаления всех epoch/time/KDA/farm конструкций найден ещё один актуальный current-match блок: causal `cp_lane + positional synergy + previous-map/coverage` поднимает exact Hybrid до 0.7173, +0.00077 [95% ДИ +0.00017,+0.00137], положительно во всех окнах; top-10 WR 90.0% -> 90.4%, top-25 82.1% -> 82.0%. По отдельности CP/SYN не проходят Holm, previous-map около нуля, поэтому результат относится только к заранее фиксированному совместному блоку. Time/era interactions из поиска исключены по решению alex: они описывают эпоху корпуса, но не различают команды в матче сейчас. Causal live-transfer удержался после исправления двух P1: state+E99 0.8480 -> state+Hybrid 0.8490, +0.0010 [+0.0003,+0.0016] на 79 202 строках. PVP/roster/meta/novelty/style и четыре pick/ban семейства, включая stage-aware flex-regret, плюса не дали. Collector продолжает сбор на serv1; serv2 недоступен по сети"
harness: "`runtime/experiments/misc/combined_all_that_works.py`, `verify_combined_all_that_works.py`, `map_winner_level_denies_forward.py`, `map_winner_hybrid_joint_forward.py`, `map_winner_current_stack_forward.py`, `map_winner_current_only_forward.py`, `map_winner_pvp_residual_forward.py`, `map_winner_roster_meta_novelty_forward.py`, `map_winner_team_style_draft_fit_forward.py`, `map_winner_live_new_prior_forward.py`; `runtime/experiments/elo/build_compact_hybrid_features.py`; `runtime/experiments/draft-cp/pickbans_{mapwinner,signature_bans,reactive_counter,flex_regret}_pilot.py`"
---

# E-137. После полного аудита нашлись level/denies и production Hybrid ELO

- **Запрос alex:** найти, где ещё упускается винрейт именно **победы на карте**;
  пересмотреть не только непробованные идеи, но и старые нули, которые могли
  быть неверно сконструированы; использовать local, serv1 и serv2.
- **Цель всех новых прогонов:** `didRadiantWin`. Условия по числу убийств нет.
- **Прод не менялся:** это только research harness и артефакты.

## 1. Сначала исправлен бар, относительно которого ищется прибавка

Reviewer нашёл три реальных проблемы старого E-99: выбранные 23 признака
читались из перезаписываемого результата; кэши из 128 колонок не несли ID;
часть legacy-builders обновляла состояние последовательно внутри одинакового
timestamp; convergence большого draft-fit не проверялся.

Исправление:

1. зафиксирован immutable `pool_snapshot.npz` с exact `mids`, source SHA-256 и
   manifest;
2. сохранён immutable `selection.json`: все 128 кандидатов, folds, полный trace,
   input/code hashes;
3. все **1 984 строки из 983 repeated-timestamp групп** исключены целиком из
   selection/train/test;
4. проверяются warnings/`n_iter_` у draft logistic и `minimize.success` у обоих
   fine-tune;
5. отдельный verifier пересчитал OOF-метрики и provenance: **31/31 PASS**.

После починки состав 23 признаков изменился в пяти местах, но общий результат
почти тот же:

| модель | AUC | WR top-10% | WR top-25% |
|---|---:|---:|---:|
| draft | 0.5993 | 69.6% | 64.8% |
| ELO | 0.6500 | 80.8% | 72.6% |
| draft + ELO | 0.6696 | 82.9% | 76.3% |
| E96 prematch | 0.6957 | 87.4% | 79.6% |
| **полный causal pool** | **0.7121** | **90.4%** | **81.0%** |
| большой public draft | 0.7127 | 90.4% | 81.0% |
| pro fine-tune | 0.7117 | 90.7% | 80.9% |
| + series | 0.7117 | 90.5% | 80.9% |

Главный вклад полного пула относительно E96: **+0.0163**, series-bootstrap
95% ДИ `[+0.0120,+0.0211]`. Большой draft даёт ещё +0.0006
`[+0.0002,+0.0010]`; pro fine-tune стабильно портит −0.0009.

## 2. Новый найденный инкремент: level и denies

Аудит сырых полей показал два показателя игрока, которых не было в полном пуле:
`level` и `numDenies`. Старый batch-11 дал слабый плюс, но был одним holdout и
legacy-конструкцией. Перепроверка сделана поверх исправленной E99 на тех же трёх
forward-окнах, с исключением всех timestamp-коллизий.

Для каждого игрока до текущей карты считается исторический остаток показателя:

- относительно глобальной нормы его позиции;
- относительно глобальной нормы выбранного героя.

Пять значений стороны усредняются, в модель идёт Radiant − Dire.

| вариант | AUC | к базе | WR top-25% |
|---|---:|---:|---:|
| исправленная E99 | 0.7121 | — | 81.0% |
| + `level_rel_pos` | 0.7127 | +0.0007 | 81.4% |
| **+ level и denies, четыре колонки** | **0.7133** | **+0.0013** | **81.6%** |

Прямой series-cluster bootstrap «все четыре против базы»: **95% ДИ
`[+0.0001,+0.0025]`**. Знак положительный во всех окнах: +0.0011, +0.0010,
+0.0017. Baseline воспроизведён с max abs diff `0`.

**Граница вывода:** четыре колонки выбраны после просмотра старого batch-11,
поэтому это честная forward-диагностика, но не pristine first-look holdout.
В прод до будущего нетронутого окна не переносить.

## 3. Качество игрока было настоящим сигналом, но не новым

Исправленный timestamp-batched блок из GPM/LH/IMP дал относительно E96:

```
0.6957 -> 0.7069, +0.0112
95% ДИ [+0.0077,+0.0150]
```

Но при **joint refit** с полным пулом:

```
0.7121 -> 0.7119, -0.0002
95% ДИ [-0.0008,+0.0005]
```

Следовательно, прибавки E99 и quality складывать нельзя. Сигнал GPM/LH/IMP
реален относительно слабой базы, но уже вложен в `imp`, `gpm/lh relative`, form
и player-hero comfort полной модели.

## 4. Pick/ban sequence: плюса нет и на 11.3k, но класс данных не закрыт

На serv1 собран snapshot 11 263 карты; 10 picks и их порядок валидны у всех.
Это в 3.9 раза больше первого прогона на 2 892 картах. Пилоты используют
rolling OOF только по более ранним собранным картам:

| группа | rolling OOF | ΔAUC к той же recalibrated quality-базе | 95% ДИ |
|---|---:|---:|---:|
| простой pick order / information advantage | 4 505 | −0.0025 | [−0.0045,−0.0006] |
| bans сигнатурных/привычных героев | 4 454 | −0.0000 | [−0.0007,+0.0007] |
| counter-quality только по уже видимым picks | 4 505 | +0.0007 | [−0.0017,+0.0030] |
| stage-aware counterfactual flex-regret | 3 824 | −0.0004 | [−0.0009,+0.0002] |

Ни одна группа не дала подтверждённой положительной прибавки. Signature bans
теперь имеет узкий интервал около нуля, reactive counter всё ещё неразличим с нулём,
а простой order-блок стал статистически отрицательным. Во время fit sklearn
эмитил intermediate overflow warnings, но конечные признаки/predictions finite,
timestamp-safe validation прошла и `ConvergenceWarning` в выводе не было. Так
как pilot не сохраняет `n_iter_`, отрицательный order-результат не объявляется
окончательным доказательством вреда; для поиска плюса это всё равно уверенный
стоп-сигнал этой конструкции.

Stage-aware тест строит causal hero→position history за 180 дней, в момент
каждого из первых четырёх picks стороны перебирает ещё доступных героев и мерит
потерю flex-entropy относительно лучшей альтернативы. Он прошёл independent
readback 16/16 и leakage-tests, но тоже остался нулём. Это не закрывает
адаптацию между картами серии, однако уже показывает, что проблема не только в
слишком простой сумме order. Collector на serv1 жив и продолжает сбор.

## 5. Главный новый результат: exact production Hybrid ELO

E96/E99 использовали простой causal ELO-вход, хотя production
`HybridPlayerRosterEloModel` отдельно ведёт игроков, roster lineage, роли, tier
и актуальный состав. Первый exact-проход не дошёл до артефакта: raw loader
покрыл только 468 334 / 482 486 карт, а построитель до checkpoint оказался
квадратичным по числу команд лиги.

Повторный построитель исправлен без изменения математики:

- production Hybrid прогрет на **922 069 уникальных исторических картах**;
- exact-кэш заполнен для **482 486 / 482 486** compact-карт;
- 14 152 строки без пригодной raw team identity восстановлены из compact:
  известный numeric team id сохраняется, а при его отсутствии используется
  pseudo-name только по exact набору пяти игроков — разные unknown-команды не
  склеиваются;
- все матчи одного timestamp сначала предсказываются и только затем обновляют
  состояние; проверка `asof < timestamp` прошла для каждой строки;
- цель в builder и joint refit — `didRadiantWin`, kills не читаются и не
  фильтруются.

Конструкции были зафиксированы до просмотра OOF: заменить legacy ELO на Hybrid,
добавить Hybrid-logit к E99, добавить один либо оба production-выхода к
E99+level/denies.

| вариант | AUC | к своей базе | WR top-10% | WR top-25% |
|---|---:|---:|---:|---:|
| E99 | 0.7121 | — | 90.4% | 81.0% |
| E99 + level/denies | 0.7133 | +0.0013 | 90.4% | 81.6% |
| Hybrid вместо legacy ELO | 0.7148 | +0.0028 к E99 | 90.0% | 81.5% |
| Hybrid-logit поверх E99 | 0.7147 | +0.0026 к E99 | 89.9% | 81.6% |
| E99 + level/denies + Hybrid-logit | 0.7160 | +0.0027 к 0.7133 | 89.8% | 82.1% |
| **E99 + level/denies + оба Hybrid-выхода** | **0.7166** | **+0.0032 к 0.7133** | **90.0%** | **82.1%** |

Прямой series-cluster bootstrap последней строки против E99+level/denies:
**95% ДИ `[+0.0011,+0.0055]`**. По окнам прирост равен +0.0015, +0.0031 и
+0.0053. Второй Hybrid-выход (`raw strength diff / 400`) добавляет поверх
production logit ещё +0.0006, ДИ `[+0.0001,+0.0011]`.

Независимый readback-пересчёт прошёл **23/23 проверок**: exact mids/labels,
finite features/predictions, strict as-of, unique OOF, полное исключение
timestamp-коллизий, побитовое воспроизведение обеих баз и отдельный повтор
главного 2 000× series-bootstrap с теми же точными границами.

Это лучший найденный дополнительный prematch-слой. Он улучшает общую AUC и
top-25, но не top-10: на самом узком экстремальном хвосте WR 90.4% → 90.0%.
Поэтому использовать его как новый prior обоснованнее, чем обещать больше
самых уверенных сигналов. Как и любой диагностический ladder с несколькими
фиксированными конструкциями, результат желательно подтвердить на будущем
нетронутом окне.

## 6. Новый prior проверен внутри live-модели

E-134 уже показал на 140 616 строках, что prematch prior нельзя заменять
нетвортом:

```
только live state:             0.8126 AUC
live state + prematch prior:   0.8306 AUC
```

На больших перевесах 6k+ prematch добавляет только +0.0055…+0.0091. На равных
картах 0–2k — **+0.042…+0.074 даже после 20–40-й минуты**. Значит прематчевую
модель нельзя просто заменить нетвортом: её надо подавать как prior/residual,
особенно в late/post-lane при небольшом текущем перевесе. Это по-прежнему цель
победы карты и не связано с условием `kills >= 27`.

Теперь отдельно проверено, переносится ли **новая** prematch-прибавка 0.7133 →
0.7166 в этот второй этаж. Первый результат был отозван после Reviewer: в
live-train попали in-sample prematch predictions, а eligibility `minute+3`
знала будущую длительность. После исправления train prior строится только из
25 225 nested/canonical OOF-карт, strict as-of violations = 0, а строка
включается по доступности текущего checkpoint (`duration > minute`). На тех же
13 653 OOF-картах получилось 79 202 наблюдения карта×минута:

| вариант | live AUC | к state | к state+E99 |
|---|---:|---:|---:|
| state | 0.8347 | — | — |
| state + E99 | 0.8480 | +0.0133 | — |
| **state + current-best Hybrid prior** | **0.8490** | **+0.0143** | **+0.0010** |

Для последнего сравнения series-bootstrap 95% ДИ
`[+0.0003,+0.0016]`; по окнам +0.0002, +0.0008, +0.0020. Top-25 WR вырос
95.6% → 95.7%, top-10 практически не изменился. Независимый readback прошёл
15/15, все fit без warnings.

Descriptive-прибавка current-best против E99 максимальна на ровных картах:
+0.0032 на 10-й, +0.0065 на 20-й, +0.0037 на 30-й минуте. У этих разрезов нет
отдельного CI; на 40-й минуте при 0–2k знак отрицательный (−0.0024, n=560),
поэтому локализацию не следует выдавать за самостоятельный результат. Полный
протокол — E-141.

## 7. Аудит E-140: где оказался ещё один слой

E-140 появился в ветке позже начала E-137 и заявлял 0.7152 на одном старом
holdout. Поэтому отдельно проверено, не был ли Hybrid сложен с устаревшей
базой. Дубликаты исключены: `lvl_rel_pos` и `series_score` уже входят в 0.7166.

| fixed вариант поверх 0.7166 | AUC | ΔAUC | 95% ДИ |
|---|---:|---:|---:|
| KDA игрока + farm dependence + 6 deployable interactions | 0.7167 | +0.0001 | [−0.0009,+0.0011] |
| + `cp_lane`, positional synergy, previous-map | 0.7175 | +0.0009 | [−0.0003,+0.0021] |
| + линейные interactions с эпохой (неактуально для current-match ranking) | 0.7189 | +0.0024 | [+0.0010,+0.0037] |

У полного deployable-набора общий CI против 0.7166 ещё пересекает ноль. Но
`cp_lane + synergy + previous-map` добавляют поверх первого leftovers-блока
+0.0008 с ДИ `[+0.0002,+0.0014]` и положительны во всех окнах. Линейный
time-блок измерил дрейф корпуса, но одинаков для двух команд текущей карты и не
является искомым current-match edge. По решению alex он не развивается и не
участвует в выборе модели. Independent verifier: 30 checks PASS.

## 8. Где винрейт вероятнее всего ещё остался

### Приоритет 1 — current-only draft/series residual

Проверка завершена напрямую поверх 0.7166, без нулевого KDA/farm/context блока
и без любой даты/эпохи:

| current-only вариант | AUC | Δ к 0.7166 | 95% ДИ |
|---|---:|---:|---:|
| только `cp_lane` | 0.71685 | +0.00029 | [−0.00004,+0.00063] |
| только positional synergy | 0.71702 | +0.00046 | [−0.00004,+0.00095] |
| только previous-map + coverage | 0.71657 | +0.00001 | [−0.00008,+0.00010] |
| **все четыре current-only колонки** | **0.71733** | **+0.00077** | **[+0.00017,+0.00137]** |

Общий блок положителен во всех окнах: +0.00033, +0.00107, +0.00087. Top-10
WR вырос 90.0% → 90.4%, top-25 слегка снизился 82.10% → 82.01%. Отдельные
add/drop-one атрибуции после Holm незначимы; previous-map сам по себе нулевой.
Значит найден совместный residual текущего драфта, а не доказан один
самостоятельный победитель. Previous-map восстановлен только из строго прошлой
карты серии: 2 213 OOF-карт с coverage, as-of violations 0.

### Приоритет 2 — адаптация draft между картами серии

После досбора проверять не ещё одну линейную сумму rank, а конструкции, которым
действительно нужна последовательность:

- blind/reactive pick отдельно по позиции и стадии;
- ban regret: качество лучшего доступного comfort-пика до и после банов;
- flex entropy **на момент выбора**, а не по финальным ролям;
- адаптацию между картами серии: проиграл герою → ban/pick в следующей.

Четыре pilot на 11 263 картах, включая counterfactual flex-regret, не
подтверждают order/signature/reactive/flex конструкции. Из списка по-настоящему
непроверенной остаётся адаптация между картами серии.

### Исправленные старые нули теперь перепроверены

Все пять конструкций из первоначального списка пересчитаны causal поверх 0.7166:

| исправление | ΔAUC к 0.7166 | 95% ДИ |
|---|---:|---:|
| PVP residual, роли 1–3, update от Hybrid expectation | +0.00001 | [−0.00051,+0.00055] |
| PVP residual, все роли 1–5 | −0.00002 | [−0.00052,+0.00050] |
| roster freshness, lineage продолжается при ≥3 игроках | −0.00003 | [−0.00041,+0.00034] |
| recent team hero distribution × global 30-day meta | +0.00008 | [−0.00030,+0.00046] |
| side-specific U-shaped draft novelty | +0.00000 | [−0.00018,+0.00020] |
| recent no-kills team style history | +0.00027 | [−0.00021,+0.00075] |
| style history × current late/tempo/push draft | +0.00014 | [−0.00049,+0.00073] |

PVP coverage по ролям 38.8–47.3%, новые roster/meta/novelty признаки ненулевые
на 46–81%, style — на 41–81%. Значит нули нельзя объяснить тем, что признаки
почти везде пусты. У style history направление интересное в двух поздних окнах,
но CI пересекает ноль и top-10 WR ухудшается; это не найденная прибавка.

## 9. Что больше не стоит перебирать

- generic hero capabilities / ручные архетипы: несколько независимых нулей;
- player embeddings: поглощены ELO/form/comfort;
- streaks, fatigue, H2H, LAN как feature, series score;
- обучение только на tournament/T1-T2: не помогает;
- pro fine-tune draft: после честной проверки вредит;
- новый GPM/LH/IMP quality-блок рядом с полным пулом: уже продублирован;
- специализация модели на underdog: после правильной ориентации равна общей.
- PVP/roster/meta/novelty/style в перечисленных выше исправленных конструкциях.

## 10. Машины

- **local:** полный исправленный forward, независимый verifier, level/denies,
  exact Hybrid-прогрев, joint refit, пять исправленных старых семейств и
  live-transfer;
- **serv1:** независимая проверка quality OOF выполнена ранее; сейчас живой
  многопоточный collector `pickBans` (11 263 строки на момент повторного snapshot);
- **serv2:** проверен обязательным `capacity.sh` и прямым SSH с password-only
  параметрами — `No route to host`; вычисление там в этом ходе невозможно.

Serv1 не использовался для тяжёлого joint-refit: у него 9.4 ГБ свободного диска,
на нём прод и 19 Python-процессов. CPU был свободен, но перенос 3–6 ГБ временных
draft-артефактов ради дублирования локального fit был бы неоправданным риском
для production-хоста.

## 11. Артефакты

- исправленная E99: `runtime/artifacts/misc/combined_all_that_works/`;
- проверка 31/31: `independent_verification.json` в том же каталоге;
- level/denies: `runtime/artifacts/misc/map_winner_level_denies_forward/`;
- quality: `runtime/artifacts/misc/map_winner_hybrid_quality_forward/`;
- exact Hybrid joint: `runtime/artifacts/misc/map_winner_hybrid_joint_forward/`;
- current stack E-140 audit: `runtime/artifacts/misc/map_winner_current_stack_forward/`;
- current-only stack: `runtime/artifacts/misc/map_winner_current_only_forward/`;
- PVP: `runtime/artifacts/misc/map_winner_pvp_residual_forward/`;
- roster/meta/novelty: `runtime/artifacts/misc/map_winner_roster_meta_novelty_forward/`;
- team style: `runtime/artifacts/misc/map_winner_team_style_draft_fit_forward/`;
- live-transfer: `runtime/artifacts/misc/map_winner_live_new_prior_forward/`;
- pick/ban pilots: `runtime/artifacts/draft-cp/pickbans_*_pilot/`.
