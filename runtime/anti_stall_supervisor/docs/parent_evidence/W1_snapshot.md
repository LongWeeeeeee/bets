# W1 snapshot adapter — EVIDENCE

## Outcome
SUCCESS — staged read-only fail-closed `snapshot_v1` adapter for anti-stall supervisor.

## Owned files (exclusive lane)
- `/root/main/runtime/anti_stall_staging/t_3c0b64e6/snapshot/snapshot.py`
- `/root/main/runtime/anti_stall_staging/t_3c0b64e6/snapshot/test_snapshot.py`
- `/root/main/runtime/anti_stall_staging/t_3c0b64e6/snapshot/CONTRACT.json`
- `/root/main/runtime/anti_stall_staging/t_3c0b64e6/snapshot/EVIDENCE.md`

## Public API
- `discover_boards(hermes_root: Path) -> list[Path]`
- `read_board_snapshot(db_path: Path, *, now_ns: int, proc_reader) -> dict`
- `collect_snapshot(hermes_root: Path, *, now_ns: int, proc_reader) -> dict`
- `default_proc_reader(pid: int) -> Optional[dict]`
- CLI: `snapshot.py --self-test` (fixture counts only); `snapshot.py --collect <hermes_root>`

## Invariants verified
1. SQLite open `mode=ro` + `PRAGMA query_only=ON`; write attempt raises; board file bytes unchanged after collect.
2. Discovery includes only machine-verifiable non-archived boards; sorts default then named slug; dedupes resolved paths; malformed `board.json` → diagnostic, excluded.
3. Archived tasks excluded from `tasks[]`.
4. Missing `ANTI_STALL_ARTIFACTS_V1=` marker ⇒ `artifacts_declared=false` (no path inference).
5. Artifact paths: absolute + lexical/physical inside `owner_prefix` + regular file + non-secret; symlink escape rejected; secrets-like names rejected.
6. `progress_digest` changes when artifact content or events change.
7. PID states covered: absent / dead / alive / reused / foreign / permission_unknown.
8. Secrets redacted in block_reason and free text; signatures derived from redacted text.
9. Resolution markers parsed only; no stall classification fields emitted.
10. Deterministic `snapshot_digest` for identical inputs.
11. Protocol/failure signatures captured from run/task evidence without deciding recovery.

## Commands run

```bash
cd /root/main && /root/main/venv/bin/python -m pytest \
  runtime/anti_stall_staging/t_3c0b64e6/snapshot/test_snapshot.py -v --tb=line
# => 22 passed in 0.86s  EXIT:0

/root/main/venv/bin/python \
  runtime/anti_stall_staging/t_3c0b64e6/snapshot/snapshot.py --self-test
# => {"self_test":"ok","fixture_count":3,"board_count":2,"task_count":2}  EXIT:0
```

## File digests (sha256)
```
2f57dd78fa6088254500010be49f6f4770c1a8cfd33fdecd756c83c3088a683f  snapshot.py
5c7ea4c90b952d4c9dbe100954fe4a95fe6609232c7e22d77c0b84b7ef4c8c83  test_snapshot.py
3f58a0115b60cfa960edb82bff5aaca013097068991b53e40539f28cb900c954  CONTRACT.json
```

## How INT should re-check
```bash
cd /root/main
/root/main/venv/bin/python -m pytest \
  runtime/anti_stall_staging/t_3c0b64e6/snapshot/test_snapshot.py -v
/root/main/venv/bin/python \
  runtime/anti_stall_staging/t_3c0b64e6/snapshot/snapshot.py --self-test
# optional: import and collect against a *temporary* hermes root only
# Never point collect at live boards for mutation tests — adapter is RO but INT must not mutate live tasks either.
```

## Out of scope (intentionally)
- Stall classification / decision policy (W2)
- Kanban action executor (W3)
- Tick runner / lock (W4)
- systemd deploy (W5)
- Final assembly under `runtime/anti_stall_supervisor/**` (INT)
- Any write/reset/clean against live Hermes boards

## Commit
No commit performed (staging lane only; task did not request commit).
