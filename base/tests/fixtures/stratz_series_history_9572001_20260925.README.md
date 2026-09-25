# stratz_series_history_9572001_20260925.json

**Capture date:** 2026-09-25 (MSK), by the lead session (a8eb8337), from the main checkout with the project proxy pool.

**What it is:** the raw, unmodified response of the Stratz series-history query used by the live ELO winner lookup
(`base/stratz_map_result.py` `_QUERY`, which requests `durationSeconds`), for team 9572001 since 1787460000,
`take = TAKE`. 1,009 bytes; 5 maps, each with `durationSeconds`.

**Capture command:**
```bash
cd /Users/alex/Documents/ingame/base && venv_catboost/bin/python3 - <<'PY'
import json, sys; sys.path.insert(0, '.')
import stratz_map_result as S
r = S._post(S._QUERY % (9572001, 1787460000, S.TAKE))
open('stratz_series_history_9572001_20260925.json', 'w').write(json.dumps(r, ensure_ascii=False, indent=1))
PY
```
