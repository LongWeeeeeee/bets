---
id: E-163
title: "Причинный bias стороны текущей лиги даёт +0.0004 current map winner"
date: "2026-08-13"
area: ml
status: full
corpus: "482 486 про-карт; 13 653 exact strict-forward OOF-карты; 276 лиг в test; baseline E-161 AUC 0.717891"
verdict: "ПОДТВЕРЖДЕНО для prematch map winner. Исторический Radiant-bias текущей лиги, посчитанный только по картам с меньшим timestamp и усаженный к глобальному Radiant-rate, поднимает нынешний expanded baseline 0.717891 -> 0.718257: +0.000366, series-CI95 [+0.000051,+0.000682], raw p=0.0109, Holm p=0.0327 внутри заранее фиксированной семьи TEAM/LEAGUE/ALL. Знак положителен во всех трёх окнах. Историческая side susceptibility текущих команд сама по себе нулевая; совместный TEAM+LEAGUE слабее league-only. Признак известен для текущего матча до старта; timestamp — только as-of barrier, date/patch/epoch/time и kills не используются"
harness: "`runtime/experiments/misc/map_winner_current_side_bias_forward.py`"
---

# E-163. У текущей лиги есть остаточный Radiant/Dire bias

## Вопрос

После E-161/E-162 основной current baseline равен 0.717891. Ещё не проверялся
класс, который не является эпохой: некоторые **текущие команды** или
**текущие лиги** могут систематически по-разному реализовывать Radiant и Dire.
ELO, состав и draft описывают общую силу, но не обязательно взаимодействие
этой силы со стороной карты.

Заранее зафиксированы три варианта поверх одного baseline:

1. `TEAM_SIDE`: сумма исторической Radiant-vs-Dire склонности двух текущих
   команд;
2. `LEAGUE_SIDE`: исторический Radiant-rate текущей лиги минус глобальный
   Radiant-rate;
3. `ALL_SIDE`: обе колонки вместе.

Это current-match признаки: team id, league id и выбранная сторона известны до
карты. Date, patch, epoch, time, kills, KDA и farm не подаются в модель.

## Каузальный builder

- корпус отсортирован по timestamp;
- вся timestamp-группа сначала получает признаки из старого состояния, и
  только затем её исходы обновляют историю;
- team rate усажен Beta-prior эквивалентом 20 игр;
- league Radiant-rate усажен к текущему глобальному Radiant-rate эквивалентом
  100 игр;
- для каждой из двух команд и лиги сохранён last-as-of; нарушений
  `feature_asof < current_ts` — 0;
- цель — `didRadiantWin`, 13 653 точных OOF-строки E-161.

## Результат

| модель | AUC | Δ к 0.717891 | CI95 | raw p | Holm p |
|---|---:|---:|---:|---:|---:|
| TEAM_SIDE | 0.717894 | +0.000003 | [−0.000165,+0.000168] | 0.4877 | 0.4877 |
| **LEAGUE_SIDE** | **0.718257** | **+0.000366** | **[+0.000051,+0.000682]** | **0.0109** | **0.0327** |
| ALL_SIDE | 0.718229 | +0.000338 | [+0.000004,+0.000674] | 0.0239 | 0.0478 |

Holm посчитан по ровно трём заранее фиксированным primary-сравнениям. Победил
самый простой вариант `LEAGUE_SIDE`; добавление team susceptibility немного
ухудшило ranking и не нужно.

| окно | baseline | + league side | ΔAUC |
|---:|---:|---:|---:|
| 1 | 0.725221 | 0.725736 | +0.000515 |
| 2 | 0.713248 | 0.713501 | +0.000252 |
| 3 | 0.715076 | 0.715385 | +0.000309 |

Знак положителен во всех трёх окнах. Top-10 WR не изменился (90.33%); top-25
вырос с 82.07% до 82.16%: найден общий ranking lift, а не доказанный выигрыш
самого узкого хвоста.

## Что именно можно заключить

Проверено: текущая league id несёт остаточный side-bias сверх expanded draft,
Hybrid, ELO, CP/SYN и causal feature pool. Это не доказывает конкретную
причину: формат турнира, сервер, правила first-pick или иной скрытый механизм
отдельно не идентифицированы. Поэтому в модель идёт только причинная агрегатная
величина, а объяснение механизма остаётся гипотезой.

## Проверка

Independent reviewer: APPROVE. Verifier **20/20**:

- exact E-161 rows/mids/target/clusters/baseline, max abs diff 0;
- независимая реконструкция side features и всех as-of;
- strict feature/train/draft as-of;
- полный hash provenance десяти входных источников;
- finite predictions, warnings 0;
- независимый replay всех 10 000 series draws и exact Holm family из трёх
  сравнений.

Артефакт: `runtime/artifacts/misc/map_winner_current_side_bias_forward/`.
