# ELO TODO

## Done

- [x] `player-local volatility` after org change added to core model
- [x] `lineup volatility` enabled only outside `TIER1`
- [x] runtime live `ELO` now invalidates stale state when snapshot config changes
- [x] Swept player inactivity decay
  Result: not adopted, baseline stayed best for both overall and `TIER1`
- [x] Patch-aware local reset added for `TIER1`
  Result: kept in default config, improved `TIER1` and overall metrics
- [x] Tested older matches only as `player_global` prior
  Result: not adopted, full warmup stayed better on all recent windows
- [x] Tested `role-aware player local`
  Result: pinned-`prod` fine sweep found and confirmed a real `TIER1` candidate.
  Promoted to `prod` with `player_role_weight=0.12`, `player_role_tier1_only=True`.
  New target metrics after promotion:
  `TIER1 accuracy 0.6701`, `log_loss 0.6196`, `brier 0.2151`
- [x] Tested stronger inactivity penalty for long breaks
  Result: adopted after extending pro history; `TIER1` now uses local+roster shrink after 60-day gaps

## Experiment Queue

- [ ] Add a new hypothesis after role-local decision
  Goal: next candidate should beat the current confirmed baseline or the role-local candidate if promoted

## Rules

- Keep a change only if it improves the chosen target slice.
- `TIER1`-focused changes should not be merged if they hurt `TIER1` accuracy without a clear compensating gain.
- Broader changes can target overall metrics, but note the `TIER1` tradeoff explicitly.
