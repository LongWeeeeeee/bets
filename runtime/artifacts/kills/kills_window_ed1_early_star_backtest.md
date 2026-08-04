# Kill-leader WR: Early STAR + |expected_diff|≥1

- generated: `2026-07-17T10:44:29.319665+00:00`
- universe: pro **7.41+**
- STAR thresholds: **WR60** signal metrics `counterpick_1vs1, counterpick_1vs2, solo`
- filter: **|expected_diff| ≥ 1.0** and **sign(ed) == early star**
- bet: star team has **strictly more kills** in window; **equal = LOSE**
- meta: scanned=4659 draft_ok=3919 nw_star=2816 match_star=2431 either=3137 both_same=2078 errors=0
- elapsed: 178.73s

## Early NW STAR + |ed|≥1 + same sign

| window | n | win | lose | **WR%** | skip |ed|<1 | skip sign≠ | skip no ed | skip no actual |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `5_15` | 51 | 35 | 16 | **68.63** | 2765 | 0 | 0 | 0 |
| `10_20` | 92 | 66 | 26 | **71.74** | 2666 | 0 | 0 | 58 |
| `15_25` | 73 | 50 | 23 | **68.49** | 2528 | 0 | 0 | 215 |
| `20_30` | 13 | 9 | 4 | **69.23** | 2304 | 0 | 0 | 499 |

## Early Winner (match-win) STAR + |ed|≥1 + same sign

| window | n | win | lose | **WR%** | skip |ed|<1 | skip sign≠ | skip no ed | skip no actual |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `5_15` | 42 | 30 | 12 | **71.43** | 2389 | 0 | 0 | 0 |
| `10_20` | 87 | 64 | 23 | **73.56** | 2295 | 0 | 0 | 49 |
| `15_25` | 71 | 49 | 22 | **69.01** | 2175 | 0 | 0 | 185 |
| `20_30` | 13 | 9 | 4 | **69.23** | 1972 | 0 | 0 | 446 |

## Either Early STAR (NW preferred) + |ed|≥1 + same sign

| window | n | win | lose | **WR%** | skip |ed|<1 | skip sign≠ | skip no ed | skip no actual |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `5_15` | 51 | 35 | 16 | **68.63** | 3086 | 0 | 0 | 0 |
| `10_20` | 92 | 66 | 26 | **71.74** | 2979 | 0 | 0 | 66 |
| `15_25` | 73 | 50 | 23 | **68.49** | 2825 | 0 | 0 | 239 |
| `20_30` | 13 | 9 | 4 | **69.23** | 2564 | 0 | 0 | 560 |

## Both Early STAR same sign + |ed|≥1 + same sign

| window | n | win | lose | **WR%** | skip |ed|<1 | skip sign≠ | skip no ed | skip no actual |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `5_15` | 42 | 30 | 12 | **71.43** | 2036 | 0 | 0 | 0 |
| `10_20` | 87 | 64 | 23 | **73.56** | 1950 | 0 | 0 | 41 |
| `15_25` | 71 | 49 | 22 | **69.01** | 1847 | 0 | 0 | 160 |
| `20_30` | 13 | 9 | 4 | **69.23** | 1688 | 0 | 0 | 377 |

## Baseline: Early NW STAR only (no ed filter)

| window | n | win | lose | **WR%** | skip |ed|<1 | skip sign≠ | skip no ed | skip no actual |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `5_15` | 2816 | 1555 | 1261 | **55.22** | 0 | 0 | 0 | 0 |
| `10_20` | 2758 | 1547 | 1211 | **56.09** | 0 | 0 | 0 | 58 |
| `15_25` | 2601 | 1492 | 1109 | **57.36** | 0 | 0 | 0 | 215 |
| `20_30` | 2317 | 1306 | 1011 | **56.37** | 0 | 0 | 0 | 499 |

## Baseline: Early Winner STAR only (no ed filter)

| window | n | win | lose | **WR%** | skip |ed|<1 | skip sign≠ | skip no ed | skip no actual |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `5_15` | 2431 | 1323 | 1108 | **54.42** | 0 | 0 | 0 | 0 |
| `10_20` | 2382 | 1330 | 1052 | **55.84** | 0 | 0 | 0 | 49 |
| `15_25` | 2246 | 1295 | 951 | **57.66** | 0 | 0 | 0 | 185 |
| `20_30` | 1985 | 1122 | 863 | **56.52** | 0 | 0 | 0 | 446 |

