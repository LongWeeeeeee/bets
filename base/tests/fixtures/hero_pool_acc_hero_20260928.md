# Hero-pool serving capture — 2026-09-28

Source: `runtime/artifacts/misc/prematch_model_artifact_v3_hybrid.npz`, local
snapshot with internal `snapshot_ts` 2026-09-25T20:40:32Z and the production
`acc_hero` table schema. The serv1 snapshot could not be read in this sandbox.
Map inputs come from
`runtime/artifacts/misc/pro_corpus_compact.npz`; all rows for the 30 accounts
on three recent pro maps are copied without editing values. The source artifact
is a cumulative snapshot; these fixture counts are the values this local
snapshot would serve, not historical as-of counts at each map start.

Capture command, run from the repository root:

```sh
/Users/alex/Documents/ingame/venv_catboost/bin/python3 - <<'PY'
from pathlib import Path
import numpy as np

root = Path('runtime/artifacts/misc')
fixture = Path('base/tests/fixtures/hero_pool_acc_hero_20260928.npz')
mids = np.array([9004428798, 8986613843, 9004867501], dtype=np.int64)
with np.load(root / 'pro_corpus_compact.npz') as corpus:
    indices = [int(np.flatnonzero(corpus['mids'] == mid)[0]) for mid in mids]
    heroes = corpus['heroes'][indices].copy()
    accounts = corpus['accounts'][indices].copy()
    ts = corpus['ts'][indices].copy()
with np.load(root / 'prematch_model_artifact_v3_hybrid.npz') as artifact:
    table = artifact['acc_hero']
    rows = table[np.isin(table[:, 0].astype(np.int64), accounts.ravel())].copy()
with open(str(fixture) + '.tmp', 'wb') as output:
    np.savez_compressed(output, mids=mids, ts=ts, heroes=heroes,
                        accounts=accounts, acc_hero=rows)
Path(str(fixture) + '.tmp').replace(fixture)
print(f'{fixture}: {len(rows)} captured rows, {len(mids)} maps')
PY
```
