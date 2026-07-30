# kills_window policy pick @ odds 1.8 (kill-lead)

Break-even WR = 1/1.8 = **55.556%**. Stake model: unit stake, WIN pays +0.8, LOSE −1.

Universe: pro 7.41* + ≥1 Tier-1; marker = more kills for predicted side.

## Overall (no |e| gate) unit-stake profit

| policy | avg WR | total n | ROI | profit u |
|---|---:|---:|---:|---:|
| **core_1v1_with_p100** | **56.09%** | 5024 | **+0.96%** | **+48.4** |
| blend_all_p30 | 55.51% | 5024 | −0.08% | −3.8 |
| blend_all_p100 | 55.49% | 5024 | −0.11% | −5.6 |
| best_abs_p100 | 55.35% | 5024 | −0.36% | −18.2 |
| first_hit_p100 (old) | 54.54% | 5024 | −1.83% | −92.0 |

Only **core_1v1_with** is +EV without gate at 1.8.

## Decision

Ship default `KILLS_WINDOW_LAYER_POLICY=core_1v1_with` (prior stays 100).
Legacy: `KILLS_WINDOW_LAYER_POLICY=first_hit`.
