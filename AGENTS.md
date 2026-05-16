# Ingame Agent Notes

`CLAUDE.md` has the full project guide. This file keeps the operational rules that future Codex-style agents must see immediately.

## Runtime Rules

- Agents must not take initiative beyond the user's requested task or explicit instructions in project `.md` files; complete the requested task and nothing more.
- If the user's request is ambiguous, the agent is unsure, or the user appears to have made a logical mistake, ask the user before acting.
- Always use `/Users/alex/Documents/ingame/venv_catboost/bin/python3` locally.
- Do not create or use a different virtualenv.
- Do not start or restart the local `base/cyberscore_try.py` live runtime unless the user explicitly asks.
- For any live runtime restart, first kill old `cyberscore` processes, clear `~/.local/state/ingame/map_id_check.txt`, and truncate the active server log (`/root/main/log.txt`) before starting the new process.
- Do NOT truncate `log.txt` during testing, probes, or mid-investigation restarts. Only truncate the log when applying a **bug fix** (git push → production pull → restart). Logic changes (new thresholds, new dispatch rules, multiplier tweaks) must NOT clear the log — keep it intact so previous match data is preserved for comparison.
- Before the live runtime enters its main CyberScore/DLTV loop, validate every live proxy up to 3 times; remove dead proxies from the in-memory live proxy pool immediately so they do not keep cycling or spam logs. Do not delete or edit the API key mappings in `base/keys.py` for this runtime pruning.
- After code changes in the `cyberscore` live pipeline (`base/cyberscore_try.py`, `base/functions.py`, `base/dota2protracker.py`, bookmaker/live dispatch logic), push the git commit to `main`, pull the new code on production, kill the old server process, clear `~/.local/state/ingame/map_id_check.txt`, and restart the server runtime with the new code.
- When pruning dead proxies in `base/keys.py`, only remove them from runtime proxy constants/pools; do not delete or edit `api_to_proxy` / `api_to_keys` entries or their API keys.
- Production server: `root@147.45.216.225`, project path `/root/main`.
- Runtime/output artifacts under `runtime/` are intentionally ignored.
- Long-running local jobs must be launched in the background with `nohup` and logs/pid files under `runtime/`, so they survive Codex turn interruptions and can be monitored later. **NEVER** use Kiro's built-in background process tool for long-running tasks — always `nohup ... > runtime/<name>.log 2>&1 &` with `echo $!` to capture PID.
- Whenever launching or continuing a long-running process, always report a clickable local log/status file link and the exact command to check process state.

## Key Files

- `base/cyberscore_try.py`: live runtime and Telegram dispatch.
- `base/functions.py`: draft dictionary metrics, lane metrics, output formatting.
- `base/dota2protracker.py`: Dota2ProTracker cache/parser and protracker draft metrics.
- `base/check_old_maps.py`: offline historical metric collector.
- `base/metrics_winrate.py`: bucket winrate analysis for JSON collected by `check_old_maps.py`.

## Camoufox Browser Configuration

The live runtime uses Camoufox (anti-detect Firefox) for all CyberScore page fetches. Key settings:

- **Mode**: One-shot per cycle (no long-living pages, no live-watcher). Each cycle opens a fresh page, fetches, parses, closes.
- **Humanize**: `True` (max cursor movement realism, up to 1.5s per move).
- **Window**: Auto-generated random realistic size (no fixed dimensions — avoids fingerprinting).
- **Locale**: Auto-detected from proxy IP via GeoIP (no hardcoded locale).
- **OS fingerprint**: `windows` (most common, blends in).
- **GeoIP**: Enabled — timezone, geolocation, locale all match the proxy IP.
- **WebRTC**: Blocked (`block_webrtc=True`).
- **Cache**: Disabled (`enable_cache=False`) — prevents cache-timing fingerprinting, ensures fresh responses.
- **Proxy**: Required; direct requests to CyberScore are disabled.

Env overrides (all optional):
| Variable | Default | Description |
|---|---|---|
| `CYBERSCORE_CAMOUFOX_HUMANIZE` | `true` | Cursor humanization (true/false/float) |
| `CYBERSCORE_CAMOUFOX_OS` | `windows` | Target OS fingerprint |
| `CYBERSCORE_CAMOUFOX_LOCALE` | _(empty, auto from geoip)_ | Force locale |
| `CYBERSCORE_CAMOUFOX_WINDOW` | _(empty, auto random)_ | Force window size e.g. `1920x1080` |
| `CYBERSCORE_CAMOUFOX_BLOCK_WEBRTC` | `1` | Block WebRTC |
| `CYBERSCORE_CAMOUFOX_ENABLE_CACHE` | `0` | Browser cache |
| `CYBERSCORE_CAMOUFOX_GEOIP` | `1` | GeoIP locale/timezone matching |
| `CYBERSCORE_LONG_PAGE_ENABLED` | `0` | Long-living page mode (disabled) |
| `CYBERSCORE_LIVE_WATCHER_ENABLED` | `0` | Live watcher mode (disabled) |

## Schedule Sleep Policy

When no live matches are found on CyberScore:
- If nearest scheduled match is **>30 min** away → sleep **30 min** (cap).
- If nearest scheduled match is **≤30 min** away → sleep **exactly** that many seconds until match start.
- If match already started (schedule says 0 or negative) → poll every 3 min.
- Quiet hours (0:00–6:00 MSK): sleep is capped by time until quiet window starts.

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
