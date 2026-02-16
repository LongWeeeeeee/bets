# ML Wrapper Repro Manifest (phase_context_metric_objDeltaCov)

Reference issue: `ingame-1p2` (bd)

Target sample-level wrapper metrics:
- early WR: `0.7464131712913513`, coverage: `0.41090641120117904`
- late WR: `0.6892554759979248`, coverage: `0.45840997343143264`

Rounded values used in runtime notes:
- early WR `0.7464`, coverage `0.4109`
- late WR `0.6893`, coverage `0.4584`

Canonical rebuild command:

```bash
/Users/alex/Documents/ingame/venv_catboost/bin/python \
  /Users/alex/Documents/ingame/tools/train_ml_phase_wrappers.py \
  --input /Users/alex/Documents/ingame/pro_heroes_data/pro_new_holdout_200kfiles.txt \
  --max-matches 50000 \
  --threshold-mode phase_metric \
  --threshold-objective delta_times_cov \
  --output-json /Users/alex/Documents/ingame/reports/metric_experiments/ml_wrapper_vs_baseline_phase_models_50k_allidx_phase_context_metric_objDeltaCov_restored_20260214.json
```

Expected runtime artifact params:
- `ml-models/phase_signal_wrapper_early.pkl`: threshold `0.55`, boost_strength `0.0`, phase `early`
- `ml-models/phase_signal_wrapper_late.pkl`: threshold `0.51`, boost_strength `0.0`, phase `late`

Dependency snapshot used for this variant:
- Python `3.9.6`
- scikit-learn `1.6.1`
- numpy `2.0.2`
- pandas `2.3.3`

Heavy data intentionally excluded from VCS:
- `pro_heroes_data/pro_new_holdout_200kfiles.txt`
- bulky public-match dictionaries/dumps

SHA256:
- `tools/train_ml_phase_wrappers.py`: `d4e0835db4444c752c3619325decf05c0fc9c56743d956b709a536906cba7abb`
- `base/signal_wrappers.py`: `3fa6d342a805e0cfbc5ac0db4a53d4b60c0b036867b54347b67d75cc016eb24d`
- `ml-models/phase_signal_wrapper_early.pkl`: `622f6da0be7388017424009dbbe4cb9cfc623de0e7e443349605fd33a2493463`
- `ml-models/phase_signal_wrapper_late.pkl`: `043e1e0a57f01b7caf3c905c352be9d3f999e313df2b0575bfe6ceffc066b4b1`
- `reports/metric_experiments/ml_wrapper_vs_baseline_phase_models_50k_allidx_phase_context_metric_objDeltaCov.json`: `32bf8ea869558799e5d3c8c532d84fdfe8a8757fa58a3f487ba6bfad1aeca29a`
- `reports/metric_experiments/ml_wrapper_vs_baseline_phase_models_50k_allidx_phase_context_metric_objDeltaCov_restored_20260214.json`: `8ed6272d9e82804a67548aa09333df7d59ac129b9ccd33bf880f029e4fcc2e03`
