# add_org_identity equivalence fixture

`compact.npz` contains 2,617 unchanged real maps in source order: the chronological
prefix `[0, 2500)` of the full compact corpus, unioned with the previous 157-row
selection (117 additional rows beyond the prefix). Every full-corpus key and dtype
is retained. `provenance.json` records all zero-based source row indices, hashes,
commands, trace counts and the first missing-index-removal divergence.

The prefix preserves the roster history needed to catch missing removals from
`account_orgs`: at source/fixture row 2135, Radiant team 490292 must resolve to
organization 2706076; the mutant resolves to stale organization 370337. The union
also retains the ambiguous-choice coverage. The legacy trace has 39 new-team
merges, 2,128 roster replacements, 1,730 invalid-team calls, 4 short-roster calls,
and 3 calls with multiple eligible organizations. In all 3 ambiguous calls the
first organization has a smaller overlap than a later candidate. Counts are per
team call, not per map; a replacement includes an unchanged repeated roster.

`src.npz` is the unchanged tiny artifact with integer, floating-point, Unicode and
empty arrays. `golden.npz` comes from the unmodified OLD script at commit
`6ac173b3`, executed on exactly these compact/source inputs. The new script must
match every golden array's key order, dtype, shape and value, as well as stdout.
The test needs only checked-in fixtures and the script, never the full corpus,
main checkout data, Git or network.

Regenerate in a temporary tree from the worktree (requires the full compact):

```sh
WT=/Users/alex/Documents/ingame-wt-org-identity
PY=/Users/alex/Documents/ingame/venv_catboost/bin/python3
TMP=$(mktemp -d /private/tmp/oi-fixture-rebuild.XXXXXX)
export DRAFT_ROOT="$TMP" PYTHONPYCACHEPREFIX=/private/tmp/oi-pyc
export PYTHONPATH="$WT/base:$WT/scripts/pro_chain"
"$PY" - "$WT" <<'PY'
import json, os, shutil, sys
from pathlib import Path
import numpy as np
wt = Path(sys.argv[1])
fixture = wt / "tests/fixtures/add_org_identity"
output = Path(os.environ["DRAFT_ROOT"]) / "runtime/artifacts/misc"
output.mkdir(parents=True)
indices = json.loads((fixture / "provenance.json").read_text())["source_row_indices"]
with np.load(wt / "runtime/artifacts/misc/pro_corpus_compact.npz") as full:
    np.savez_compressed(output / "pro_corpus_compact.npz",
                        **{key: full[key][indices] for key in full.files})
shutil.copyfile(fixture / "src.npz", output / "prematch_model_artifact_v2.npz")
PY
git -C "$WT" show 6ac173b3:scripts/pro_chain/add_org_identity.py > "$TMP/legacy.py"
"$PY" "$TMP/legacy.py"
cmp "$TMP/runtime/artifacts/misc/pro_corpus_compact.npz" "$WT/tests/fixtures/add_org_identity/compact.npz"
cmp "$TMP/runtime/artifacts/misc/prematch_model_artifact_v3.npz" "$WT/tests/fixtures/add_org_identity/golden.npz"
```

Run the hermetic equivalence test:

```sh
PYTHONPYCACHEPREFIX=/private/tmp/oi-pyc \
  /Users/alex/Documents/ingame/venv_catboost/bin/python3 -m pytest \
  tests/test_add_org_identity_equivalence.py -q -p no:cacheprovider \
  --basetemp="$(mktemp -d /private/tmp/oi-pytest.XXXXXX)/run"
```

Set `ADD_ORG_IDENTITY_TEST_SCRIPT` to a temporary copy for each RED check. Mutants:
largest-overlap selection; stale postings on repeated roster replacement
(including the known-tid path); omitted previous-minus-members index removals;
and last-inserted organization selection. Each must fail with
`AssertionError: value/order mismatch: team_merge`. Never mutate the worktree's
production script. Successful mutant process execution alone is not a RED check.
