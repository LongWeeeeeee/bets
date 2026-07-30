# kills_window pro Tier-1 backtest (7.41 + patch)

- generated: `2026-07-16T17:27:19.967243+00:00`
- parts: `7.41*.json` (7 files)
- scanned pro matches: **4659**
- with ≥1 Tier-1 team: **1377**
- bad draft skipped: **68**
- dict: `/root/main/bets_data/analise_pub_matches/kills_window_dict_raw.sqlite3`
- elapsed: **2.6s**

## Per-window

| window | n | kill WR | base map WR | skip no actual | skip no pred | skip 0 pred |
|---|---:|---:|---:|---:|---:|---:|
| `5_15` | 1309 | 52.79% | 51.95% | 0 | 0 | 0 |
| `10_20` | 1293 | 55.30% | 52.44% | 16 | 0 | 0 |
| `15_25` | 1253 | 54.99% | 52.43% | 56 | 0 | 0 |
| `20_30` | 1169 | 55.18% | 53.38% | 140 | 0 | 0 |

## Buckets by |expected_diff|

### `5_15`

| |expected| | n | kill WR | base map WR |
|---|---:|---:|---:|
| 0-0.5 | 724 | 46.55% | 49.72% |
| 0.5-1 | 401 | 58.60% | 54.86% |
| 1-2 | 181 | 65.75% | 55.25% |
| 2-3 | 3 | 0.00% | 0.00% |

### `10_20`

| |expected| | n | kill WR | base map WR |
|---|---:|---:|---:|
| 0-0.5 | 700 | 52.57% | 51.00% |
| 0.5-1 | 397 | 55.16% | 55.67% |
| 1-2 | 189 | 66.67% | 51.32% |
| 2-3 | 7 | 28.57% | 42.86% |

### `15_25`

| |expected| | n | kill WR | base map WR |
|---|---:|---:|---:|
| 0-0.5 | 677 | 49.04% | 49.93% |
| 0.5-1 | 389 | 61.95% | 57.07% |
| 1-2 | 181 | 63.54% | 51.93% |
| 2-3 | 6 | 16.67% | 50.00% |

### `20_30`

| |expected| | n | kill WR | base map WR |
|---|---:|---:|---:|
| 0-0.5 | 734 | 52.59% | 51.23% |
| 0.5-1 | 340 | 59.12% | 56.76% |
| 1-2 | 92 | 60.87% | 57.61% |
| 2-3 | 2 | 100.00% | 100.00% |
| 3+ | 1 | 0.00% | 0.00% |

## Notes

- kill WR: predicted side has **strictly more** kills in the half-open window.
- base map WR: predicted kill-lead side wins the map.
- actual kill-diff = 0 counts as miss for kill WR.
- expected_diff = 0 is skipped (not scored).

