# OpenDota explorer captured slices

Capture date: **08.10.2026 03:00 MSK**. Source is the existing offline capture
`runtime/artifacts/elo/prod_review_20261006/od_supp/` in the main checkout.
Rows are unchanged; listing slices have 150 rows, the player slice has 200.
These slices test pagination/cache behavior, not the full-capture acceptance count.

Slicing command (no network):

```sh
/Users/alex/Documents/ingame/venv_catboost/bin/python3 - <<'PY'
from pathlib import Path
import json
src = Path('/Users/alex/Documents/ingame/runtime/artifacts/elo/prod_review_20261006/od_supp')
dst = Path('tests/fixtures/od_explorer_20261008')
dst.mkdir(parents=True, exist_ok=True)
for name in ('listing_1704067200.json', 'listing_1719792000.json',
             'listing_1735689600.json', 'listing_1783209600.json',
             'players_7515635423.json'):
    rows = json.loads((src / name).read_text())[:150 if name.startswith('listing_') else 200]
    (dst / name).write_text(json.dumps(rows, indent=2) + '\n')
PY
```
