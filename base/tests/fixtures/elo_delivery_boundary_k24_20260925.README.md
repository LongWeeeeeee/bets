# Telegram ELO block, b48db8de K24 capture

`elo_delivery_boundary_k24_20260925.txt` is the exact `_format_team_elo_block`
text from `b48db8de:base/cyberscore_try.py`. Its input is a small snapshot made
from the first row of `ELO/tests/variant_a_real_maps_20260925.json`: an empty
model updated with that one finished map, queried at `end + 1` with
`ELO_SERVED_COMPOSITION=k24`. The test builds the same snapshot and calls the
current Telegram summary and formatting functions.

Capture from the `elo-variant-a` worktree root with the project's Python. The
baseline source is copied to scratch and parsed for the two formatting
functions, so importing the old full live process is unnecessary:

```bash
git show b48db8de:base/cyberscore_try.py > /private/tmp/elo-a4-b48-cyberscore_try.py
/Users/alex/Documents/ingame/venv_catboost/bin/python3 - <<'PY'
import ast, json, os, tempfile
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from ELO import live_team_strength as live
from ELO.config import HybridEloConfig
from ELO.domain import LeagueTier, MatchRecord
from ELO.models import HybridPlayerRosterEloModel
from ELO.replay import result_record
row = json.loads(Path('ELO/tests/variant_a_real_maps_20260925.json').read_text())['maps'][0]
match = MatchRecord(match_id=row['match_id'], timestamp=row['start'], radiant_win=row['radiant_win'], radiant_team_id=1, radiant_team_name='Radiant', dire_team_id=2, dire_team_name='Dire', radiant_player_ids=tuple(row['radiant_player_ids']), dire_player_ids=tuple(row['dire_player_ids']), league_id=1, league_name='fixture', source_league_tier=None, series_id=None, series_type=None, derived_league_tier=LeagueTier.TIER1, duration_seconds=row['duration_seconds'])
model = HybridPlayerRosterEloModel(HybridEloConfig())
model.process_match(result_record(match, row['end']), duration_seconds=row['duration_seconds'])
with tempfile.TemporaryDirectory() as directory:
    root = Path(directory)
    snapshot = root / 'snapshot.json'
    snapshot.write_text(json.dumps({'meta': {**live._rating_replay_meta(), 'reference_timestamp': row['end']}, 'teams_by_org_key': {}, 'model_state': model.export_state()}))
    os.environ['ELO_SERVED_COMPOSITION'] = 'k24'
    summary = live.get_matchup_summary(radiant_team_id=1, dire_team_id=2, radiant_team_name='Radiant', dire_team_name='Dire', radiant_account_ids=list(match.radiant_player_ids), dire_account_ids=list(match.dire_player_ids), timestamp=row['end'] + 1, snapshot_path=snapshot, data_dir=root, rebuild_if_missing=False, runtime_model_state_path=root / 'absent.json')
source = Path('/private/tmp/elo-a4-b48-cyberscore_try.py').read_text()
functions = [node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name in ('_elo_probability_from_ratings', '_format_team_elo_block')]
namespace = {'Optional': Optional, 'Dict': Dict, 'Any': Any, 'Tuple': Tuple}
exec(compile(ast.Module(body=functions, type_ignores=[]), 'b48db8de:base/cyberscore_try.py', 'exec'), namespace)
text, _ = namespace['_format_team_elo_block'](summary, radiant_team_name='Radiant', dire_team_name='Dire')
assert summary['source'] == 'elo_composition_k24'
Path('base/tests/fixtures/elo_delivery_boundary_k24_20260925.txt').write_text(text)
PY
```
