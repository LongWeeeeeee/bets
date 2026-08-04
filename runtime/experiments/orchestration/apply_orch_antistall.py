#!/usr/bin/env python3
"""Apply orchestration anti-stall rules: failure_limit 3 + SOUL/skill updates."""
from __future__ import annotations

import re
from pathlib import Path

import yaml

CONFIGS = {
    "default": Path("/root/.hermes/config.yaml"),
    "worker": Path("/root/.hermes/profiles/worker/config.yaml"),
    "orchestration1": Path("/root/.hermes/profiles/orchestration1/config.yaml"),
    "orchestration2": Path("/root/.hermes/profiles/orchestration2/config.yaml"),
    "planner": Path("/root/.hermes/profiles/planner/config.yaml"),
    "reviewer": Path("/root/.hermes/profiles/reviewer/config.yaml"),
}

ORCH_RULES = r'''
## Orchestration anti-stall (mandatory)

Freeze still holds: do **not** change roles/models/assignees/toolsets without explicit user approval.
These rules change **dispatch/retry/plan granularity only**.

### A. Retry budget (ceiling, not quota)

- Default card/global: **`failure_limit: 3`** total attempts (initial + 2 retries). Create cards with `--max-retries 3` unless a tighter budget applies.
- Task-level `--max-retries` overrides global config — never leave stale `5` on new cards.
- **Retries are not mandatory.** Same normalized failure signature + no artifact delta ⇒ **no further attempt**, even if budget remains.

Signature-aware circuit breaker (identical outcome class):

| Failure class | Same-signature retries | Action |
|---|---|---|
| Transient provider/spawn (502, probe fail) | up to budget (max 3 total) | inference-probe, then retry |
| **Protocol violation** (`rc=0` without `kanban_complete`/`kanban_block`) with healthy worker profile | **max 1 retry** (max 2 attempts total) | 2nd identical → **salvage partial + REPLAN/split**, do not burn budget |
| Timeout **with** new required artifacts | 1 continue | pass exact checkpoint paths |
| Timeout **without** progress | 0–1 | **split immediately** |
| Worker `FAILED` with evidence | 0 blind retries | replan open signatures only |
| **Reviewer ISSUES** | **0** identical re-runs | immediate replan of open signatures only; preserve approved work |
| Checksum / parent-evidence / stale-approval mismatch | **0** | fix graph/contract; do not re-fire Reviewer on same broken inputs |
| Ownership / production-safety breach | **0** | rollback, block, replan |

Project emergency ceiling for Reviewer fix iterations remains separate (`MAX_FIX_ITERS` style) — **do not confuse** with per-card technical retries.

### B. Plan-lint **before** Worker dispatch (reject fat plans)

Return plan to Planner (do not dispatch) if **any**:

1. One card has **more than one** independently verifiable outcome
2. **More than 4–5 final artifacts of different types** on one Worker
3. **Collection + interpretation + integration** mixed in one card
4. **Archival evidence** and **live system state** probes mixed in one card
5. Multiple services/safety domains in one card
6. Missing exact ownership, verify command, and expected result
7. Independent pieces could run in parallel but Planner emitted a single Worker
8. Shared final output dir owned by multiple writers (must be **staging per Worker + single INT**)

### C. Mandatory graph shape for multi-part evidence

```
W1 staging/...  -+
W2 staging/...  -+--> INT (only writer to final/) --> Reviewer
W3 staging/...  -+
```

- Parallel Workers: **read-only** shared immutable inputs; **write only** own staging.
- **One INT** assembles/validates final contract and is the only card that may complete the evidence pack.
- Reviewer starts **only after INT SUCCESS** (all required siblings SUCCESS).
- One sibling `FAILED` ⇒ wave FAILED for review gate; **do not redo SUCCESS siblings**; replan only failed/open.

### D. Progress check before any retry

Before redispatching the **same** card body, verify:

- new required files appeared; and/or
- hashes/sizes of required artifacts changed; and/or
- RESULT / lifecycle terminal (`complete`/`block`) occurred; and
- failure signature is **not** identical with zero artifact delta.

If signature unchanged and delta empty → **block + REPLAN/split**, not another attempt.

### E. Downstream gates (no brittle ID freeze)

- Create Reviewer/cutover cards **after** the producing verdict exists, or depend via **parent edge** on the live card.
- Do **not** hard-code stale Reviewer/approval card IDs into long-lived bodies.
- Immutable identity = **artifact content hash**, not historical card id.
- Downstream must not start on partial/protocol-crashed producers.

### F. Protocol / Worker lifecycle

- Every Worker/Planner run **must** end with `kanban complete` or `kanban block` (or equivalent terminal tool). Silent `rc=0` = protocol violation.
- Partial work: leave files in staging + `block` with missing list; never exit clean mid-pack.
- After protocol x2 identical: salvage paths into REPLAN body; force slice (staging + INT).

### G. What we still do **not** do

- No role/model/assignee freeze breaks
- No Reviewer bypass / no production cutover without APPROVE
- No burning retries on deterministic contract/graph errors
- No single mega-card that fixes the whole evidence contract alone
'''.lstrip()

SKILL_SECTION = r'''
## 12. Orchestration anti-stall (failure budget + plan-lint)

### Config
- Global `kanban.failure_limit: **3**` (was 5) on all profiles.
- New cards: `--max-retries 3` unless tighter. Task-level max-retries overrides global.

### Circuit breaker (same signature)
- Transient provider: retry within budget.
- **Protocol violation** (clean exit without complete/block): **max 2 attempts total**; then salvage partial + REPLAN/split.
- Timeout with progress: 1 continue with checkpoints.
- Timeout without progress: split.
- Reviewer ISSUES / checksum / parent-evidence / stale approval: **0** identical re-runs → replan/fix graph.
- Ownership/prod safety: 0 retries → rollback/block/replan.
- Same signature + **zero artifact delta** ⇒ no next attempt even if budget remains.

### Plan-lint before Worker dispatch
Reject (return to Planner) if: multi-outcome card; >4–5 heterogeneous final artifacts; collection+integration mixed; archival+live state mixed; multi safety domain; missing ownership/verify/expected; parallelizable work forced into one Worker; shared final dir with multiple writers.

### Graph
`W* → staging/*` only; **one INT** owns `final/`; Reviewer only after INT SUCCESS. Preserve SUCCESS siblings on wave fail.

### Lifecycle
Worker must `kanban complete` or `kanban block`. Silent rc=0 = protocol violation. Downstream cards use parent edges + artifact hashes, not frozen old Reviewer IDs.
'''.lstrip()


def atomic_write(path: Path, text: str) -> None:
    tmp = Path(str(path) + ".tmp")
    tmp.write_text(text)
    tmp.chmod(path.stat().st_mode & 0o777)
    tmp.replace(path)


def set_failure_limit() -> None:
    for name, p in CONFIGS.items():
        c = yaml.safe_load(p.read_text())
        k = c.setdefault("kanban", {})
        old = k.get("failure_limit")
        k["failure_limit"] = 3
        c["kanban"] = k
        atomic_write(p, yaml.safe_dump(c, sort_keys=False, allow_unicode=True))
        print(f"config {name}: failure_limit {old} -> 3")


def upsert_soul(path: Path) -> None:
    t = path.read_text()
    marker = "## Orchestration anti-stall (mandatory)"
    if marker in t:
        start = t.find(marker)
        rest = t[start + len(marker) :]
        m = re.search(r"\n## (?!#)", rest)
        if m:
            end = start + len(marker) + m.start()
            t2 = t[:start] + ORCH_RULES.rstrip() + "\n\n" + t[end + 1 :].lstrip("\n")
        else:
            t2 = t[:start] + ORCH_RULES.rstrip() + "\n"
    else:
        t2 = t.rstrip() + "\n\n" + ORCH_RULES.rstrip() + "\n"
    atomic_write(path, t2)
    print(f"SOUL {path}: anti-stall present={marker in t2}")


def patch_skill() -> None:
    paths = [
        Path("/root/.hermes/profiles/worker/skills/autonomous-ai-agents/hermes-local-ops/SKILL.md"),
    ]
    default_copy = Path("/root/.hermes/skills/autonomous-ai-agents/hermes-local-ops/SKILL.md")
    if default_copy.exists():
        paths.append(default_copy)

    for p in paths:
        t = p.read_text()
        t2 = re.sub(r"(?m)^version: .*$", "version: 1.3.0", t, count=1)
        if "## 12. Orchestration anti-stall" in t2:
            idx = t2.find("## 12. Orchestration anti-stall")
            t2 = t2[:idx].rstrip() + "\n\n" + SKILL_SECTION.rstrip() + "\n"
        else:
            t2 = t2.rstrip() + "\n\n" + SKILL_SECTION.rstrip() + "\n"
        # description nudge
        t2 = t2.replace(
            "kanban hygiene/stuck/assignee guard, drift watchdog, worker-profile config.",
            "kanban hygiene/stuck/assignee guard, drift watchdog, anti-stall (failure_limit=3, plan-lint, protocol circuit breaker), worker-profile config.",
        )
        atomic_write(p, t2)
        print(f"skill updated {p}")


def patch_drift_watchdog() -> None:
    """Document failure_limit=3 in baseline note if present; optional check."""
    p = Path("/root/main/runtime/hermes_drift_watchdog.py")
    if not p.exists():
        return
    t = p.read_text()
    # add failure_limit to baseline checks if not already
    if "failure_limit" not in t:
        # inject after default_assignee check in check_profile
        needle = 'if kanban.get("default_assignee") != exp["default_assignee"]:'
        if needle in t:
            insert = '''
    fl = kanban.get("failure_limit")
    if exp.get("failure_limit") is not None and fl != exp["failure_limit"]:
        drifts.append({
            "profile": name,
            "field": "kanban.failure_limit",
            "expected": exp["failure_limit"],
            "actual": fl,
        })
'''
            t = t.replace(needle, insert + "\n    " + needle, 1)
        # add to each BASELINE entry
        t = t.replace(
            '"default_assignee": "worker",\n        "max_turns":',
            '"default_assignee": "worker",\n        "failure_limit": 3,\n        "max_turns":',
        )
        t = t.replace(
            "orchestration freeze + lean CLI + timeout=1200 + planner max_turns=40",
            "orchestration freeze + lean CLI + timeout=1200 + planner max_turns=40 + failure_limit=3",
        )
        atomic_write(p, t)
        print("drift watchdog: failure_limit baseline added")
    else:
        print("drift watchdog: already mentions failure_limit")


def main() -> None:
    set_failure_limit()
    for p in [
        Path("/root/.hermes/SOUL.md"),
        Path("/root/.hermes/profiles/worker/SOUL.md"),
        Path("/root/.hermes/profiles/orchestration1/SOUL.md"),
        Path("/root/.hermes/profiles/orchestration2/SOUL.md"),
        Path("/root/.hermes/profiles/planner/SOUL.md"),
        Path("/root/.hermes/profiles/reviewer/SOUL.md"),
    ]:
        if p.exists():
            upsert_soul(p)
        else:
            print("missing SOUL", p)
    patch_skill()
    patch_drift_watchdog()
    print("DONE")


if __name__ == "__main__":
    main()
