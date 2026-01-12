# TODO: pred_all Regression Improvements

- [x] Rebuild live priors to version 8 (`ml-models/pro_kills_priors.json`).
- [ ] Test training with full early data only (`has_full_early=1`) and compare 7.40 MAE/EV. (Blocked: 7.40 has 0 full-early rows.)
- [x] Encode unknown tiers as `match_tier=3` (fix default) and retrain.
- [x] Drop XP lead series features for 7.40 focus; keep (MAE improved, backtest EV up).
- [x] Filter kill-series mismatches (<=1) — worse MAE; skip.
- [ ] Consider residual model: predict remaining kills after minute 10 and sum with `total10`.
