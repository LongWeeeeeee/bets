# Ingame Agent Notes

`CLAUDE.md` has the full project guide. This file keeps the operational rules that future Codex-style agents must see immediately.

## Runtime Rules

- Always use `/Users/alex/Documents/ingame/venv_catboost/bin/python3` locally.
- Do not create or use a different virtualenv.
- Do not start or restart the local `base/cyberscore_try.py` live runtime unless the user explicitly asks.
- For any live runtime restart, first kill old `cyberscore` processes and clear `~/.local/state/ingame/map_id_check.txt`.
- Production server: `root@212.113.104.102`, project path `/root/main`.
- Runtime/output artifacts under `runtime/` are intentionally ignored.
- Long-running local jobs must be launched in the background with `nohup` and logs/pid files under `runtime/`, so they survive Codex turn interruptions and can be monitored later.

## Key Files

- `base/cyberscore_try.py`: live runtime and Telegram dispatch.
- `base/functions.py`: draft dictionary metrics, lane metrics, output formatting.
- `base/dota2protracker.py`: Dota2ProTracker cache/parser and protracker draft metrics.
- `base/check_old_maps.py`: offline historical metric collector.
- `base/metrics_winrate.py`: bucket winrate analysis for JSON collected by `check_old_maps.py`.

## Offline Metric Validation

Use `base/check_old_maps.py` to collect historical draft metrics into a JSON file, then `base/metrics_winrate.py` to compute bucket winrate.

Public patch 7.41 50k example:
```bash
/Users/alex/Documents/ingame/venv_catboost/bin/python3 base/check_old_maps.py \
  --maps-path bets_data/analise_pub_matches/json_parts_split_from_object \
  --patch 7.41 \
  --max-matches 50000 \
  --dicts \
  --dota2protracker \
  --post-lane-max-cached-shards 127 \
  --output runtime/pub_7.41_50k_metrics.json

/Users/alex/Documents/ingame/venv_catboost/bin/python3 base/metrics_winrate.py \
  --input runtime/pub_7.41_50k_metrics.json \
  --bucket-mode \
  --min-matches 6 \
  > runtime/pub_7.41_50k_winrate.txt
```

`--patch 7.41` resolves split files like `7.41_part*.json` and uses start timestamp `1774310400` when `--start-date-time` is not provided. `--post-lane-max-cached-shards 127` is for offline public backtests only; it avoids sharded post-lane cache thrashing and does not change live runtime defaults.
