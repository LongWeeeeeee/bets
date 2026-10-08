Captured inputs: OpenDota proMatches captured 2026-10-05
(`runtime/artifacts/misc/corpus_gap_20261005/od_promatches.json`); production ledger
captured from serv1 by scp 2026-10-06 (`ledger_20261006.json`,
applied_maps["dltv.org/matches/9021470896.0"]). Listing metadata/duration/winner come
from OpenDota; ten accounts/sides come from that ledger (winner agrees). Player
slots 0..4/128..132 encode the captured sides; original slots were not captured.

Tier/series/names follow the OpenDota explorer row format: lowercase `tier`, integer
`series_type`, and `radiant_name`/`dire_name` (joined from the OpenDota `teams` table,
null when unknown). In this transitional fixture, `professional` is a representative
source tier, not the ledger's `TIER2`; Team Nemesis/Team Spirit are readable names
adapted from the local team-ID registry, not captured explorer name fields.
These transitional listing/player rows are not cuts from the real explorer fetch:
`runtime/artifacts/elo/prod_review_20261006/od_supp/listing_*.json` and
`players_*.json`, produced by
`runtime/experiments/elo/prod_review_20261006/od_explorer_fetch.py`
The lead owns their replacement with actual explorer rows. The tests do not fetch data or spend OpenDota quota.

Corpus fixture: exactly two unchanged, valid, disjoint-account records streamed via
ijson from `pro_heroes_data/json_parts_split_from_object/7.41e_part091.json` on
2026-10-07. No full corpus build.

## Exact historical capture commands

Ledger capture: 2026-10-06T17:25:13.500Z, Claude session
`5ffa5463-6afc-4f23-a13d-60c2a7705f54`, cwd `/Users/alex/Documents/ingame`:

```sh
S=/private/tmp/claude-501/-Users-alex-Documents-ingame/5ffa5463-6afc-4f23-a13d-60c2a7705f54/scratchpad; scp -q serv1:/root/main/runtime/live_elo_progress.json $S/ledger_20261006.json
```

Fixture extraction: 2026-10-07T18:37:52.958Z, Codex session
`01a117a6-7406-7a03-bc65-bff0077530ed`, recorded CommandExecution ordinal 78,
cwd `/Users/alex/Documents/ingame-wt-elo-supp`. The command below is copied
verbatim from the execution receipt, including the original temporary ledger
path and initial README. It is historical provenance, not an instruction to
fetch again or load the real corpus during tests. The source OpenDota dump was
already captured on 2026-10-05; this command extracts from that dump offline.

<!-- capture-command:start -->
```sh
/Users/alex/Documents/ingame/venv_catboost/bin/python3 - <<'PY'
import json
from pathlib import Path
import ijson
from ELO.data_loader import _parse_match
root=Path('ELO/tests/fixtures/elo_supplement_20261006'); root.mkdir(parents=True,exist_ok=True)
ledger_path=Path('/private/tmp/claude-501/-Users-alex-Documents-ingame/5ffa5463-6afc-4f23-a13d-60c2a7705f54/scratchpad/ledger_20261006.json')
entry=json.loads(ledger_path.read_text())['applied_maps']['dltv.org/matches/9021470896.0']; rec=entry['match_record']
od_path=Path('/Users/alex/Documents/ingame/runtime/artifacts/misc/corpus_gap_20261005/od_promatches.json')
od=next(r for r in json.loads(od_path.read_text()) if r['match_id']==9021470896)
listing={k:od[k] for k in ('match_id','start_time','duration','radiant_win','leagueid','league_name','radiant_team_id','dire_team_id','series_id','series_type')}; listing['tier']=rec['source_league_tier']
assert listing['radiant_win']==entry['radiant_win']
players=[{'match_id':od['match_id'],'account_id':a,'player_slot':i+slot} for ids,slot in ((rec['radiant_player_ids'],0),(rec['dire_player_ids'],128)) for i,a in enumerate(ids)]
accounts={r['account_id'] for r in players}; corpus={}
source=Path('/Users/alex/Documents/ingame/pro_heroes_data/json_parts_split_from_object/7.41e_part091.json')
with source.open('rb') as f:
 for key,raw in ijson.kvitems(f,'',use_float=True):
  m=_parse_match(raw) if isinstance(raw,dict) else None
  if m and m.duration_seconds and m.duration_seconds>0 and not accounts.intersection(m.radiant_player_ids+m.dire_player_ids) and abs(m.timestamp-od['start_time'])<10*86400:
   corpus[key]=raw; accounts.update(m.radiant_player_ids+m.dire_player_ids)
   if len(corpus)==2: break
assert len(corpus)==2
for name,rows in [('listing_captured.json',[listing]),('players_captured.json',players),('corpus_captured.json',corpus)]:
 (root/name).write_text(json.dumps(rows,indent=2)+'\n')
(root/'README.md').write_text('Captured inputs: OpenDota proMatches captured 2026-10-05 (`runtime/artifacts/misc/corpus_gap_20261005/od_promatches.json`); production ledger captured from serv1 by scp 2026-10-06 (`ledger_20261006.json`, applied_maps["dltv.org/matches/9021470896.0"]). Listing metadata/duration/winner come from OpenDota; tier and ten accounts/sides come from that ledger (winner agrees). Player slots 0..4/128..132 encode the captured sides; original slots were not captured. Corpus fixture: exactly two unchanged, valid, disjoint-account records streamed via ijson from `pro_heroes_data/json_parts_split_from_object/7.41e_part091.json` on 2026-10-07. No full corpus build.\n')
print({'fixture':str(root),'corpus_ids':list(corpus),'supplement_id':od['match_id']})
PY
```
<!-- capture-command:end -->

Subsequent fixture adaptation: 2026-10-07T19:03:17.212Z, Codex session
`01a117bc-9880-7372-a4e5-a8a07f6f356d`, exact `apply_patch` input for the listing:

```diff
*** Begin Patch
*** Update File: /Users/alex/Documents/ingame-wt-elo-supp/ELO/tests/fixtures/elo_supplement_20261006/listing_captured.json
@@
     "dire_team_id": 7119388,
+    "radiant_name": "Team Nemesis",
+    "dire_name": "Team Spirit",
@@
-    "tier": "TIER2"
+    "tier": "professional"
*** End Patch
```

The corpus and player fixture bytes were not changed by this adaptation.
The historical commands were inspected, not rerun in the C1-fix session.
