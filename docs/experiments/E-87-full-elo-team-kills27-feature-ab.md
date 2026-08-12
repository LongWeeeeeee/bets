# E-87 — full-history ELO feature A/B

Проверенный offline-прогон выполнен локально одним процессом: `./venv_catboost/bin/python3 runtime/experiments/elo/team_kills27_elo_gate_ab.py`. Перед запуском выполнен `bash scripts/ops/capacity.sh`. Serv1 независимо пересчитал замороженный shadow-снапшот: 3 161 строк, 112 уникальных пар, 58 observed и 58 воспроизведённых dispatch (`exact=true`); SHA-256 совпал. Serv2 после Wake-on-LAN и повторного SSH-запроса остался недоступен (`No route to host`), поэтому в расчётах не участвовал.

Источник: 3 161 shadow-запись, 112 уникальных match-side пар. Input SHA-256 совпал с ожидаемым: `d7f69c8a527f489a8908f12213a2d20612451e9fb934069fde27dd80b2828510`. Artifact SHA-256: `7cbc11b7a6840b8f7d249374a42b5c3302bd419c04884b7211d930db9db23292`. Baseline точно воспроизвёл 58 фактически отправленных Telegram-сигналов; не-ELO различий признаков: 0.

ELO строится хронологически на пятилетнем окне от Unix `1627726393`; сила для карты берётся pre-update из единственного `process_match()`. В full-arm заменяются только `elo_target_win_prob` и `elo_target_diff`, после чего ML пересчитывается. Production dispatch остаётся `ML >= threshold AND nw_max_wr >= min_wr AND roster_kills.available`; ELO — только ML-признаки. Диагностический порог ELO 0.45 в отправку не входит.

| Метрика | Current | Full-history ELO |
|---|---:|---:|
| Отправлено | 58 | 54 |
| С outcome | 46 | 46 |
| Hits | 28 | 27 |
| Precision | 60.87% | 58.70% |
| Recall (из всех 51 positive outcome-пар) | 54.90% | 52.94% |
| Accuracy (все outcome-пары; неотправленное = negative) | 54.44% | 52.22% |
| ROI при фиксированных 1.8 | 9.57% | 5.65% |
| Profit, units | +4.4 | +2.6 |
| Brier (n=90) | 0.237467 | 0.239152 |
| Logloss (n=90) | 0.666610 | 0.670071 |

Dispatch-flips: 49 в обеих ветках, 9 только current, 5 только full, 49 ни в одной. Bootstrap (seed `20260812`, 4 000 реплик) для full − current: 95% CI precision `[-0.093074, 0.045455]`, ROI `[-0.167532, 0.081818]`.

## Coverage decision

ELO match coverage — 92.86%, outcome coverage — 80.36%; обе величины ниже заданного 95% порога, поэтому `decision_blocked: true`. По этому прогону изменение не является основанием для production-решения.

## Time-aware league tiering

Вместо статической `classify_leagues()`/`attach_league_tiers()` применён `attach_league_tiers_time_aware(unique_matches, include_current_match_teams=True)`: на момент карты используются только уже наблюдавшиеся в лиге команды и команды самой карты, без будущего состава лиги. Итог tiering: TIER1 8 867, TIER2 10 038, TIER3 301 747.

Известное ограничение: `KNOWN_TIER1_IDS` и `KNOWN_TIER2_IDS` — текущая статическая таксономия из `base.id_to_names`; time-aware membership убирает leakage будущих участников лиги, но не делает эту таксономию исторической.

Полные машиночитаемые результаты: `runtime/artifacts/elo/team_kills27_ab/metrics.json`, `validation.json`, `paired_candidates.jsonl`.
