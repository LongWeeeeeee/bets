Captured 2026-10-10 from the Mac launchd rank-snapshot runs (card ingame-qe6y):
- collection_europe_sslerror.json = runtime/artifacts/misc/rank_snapshots/1791613147486920000/collection.json
  (09:19:07 MSK run inside a battery DarkWake, pmset "DarkWake from Deep Idle ... rtc/Maintenance Using BATT";
  europe failed with SSLEOFError, the other three regions HTTP 200)
- collection_complete.json = runtime/artifacts/misc/rank_snapshots/1791613635676987000/collection.json
  (manual rerun 09:27 MSK on AC, complete=true, 4 regions, 20018 rows)
Capture command: cp runtime/artifacts/misc/rank_snapshots/<run>/collection.json base/tests/fixtures/rank_snapshots_20261010/<name>.json
