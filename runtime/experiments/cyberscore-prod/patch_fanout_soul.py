#!/usr/bin/env python3
"""Patch SOUL + skill for max fan-out / plan-lint runtime pointers."""
from pathlib import Path
import re

FANOUT = """
### H. Maximum parallel fan-out (default bias)

Prefer **as many independent Workers as safe**, not fewer mega-cards.

1. If two outcomes can run without shared **writes**, they are **separate cards** (parallel).
2. Shared **reads** of immutable evidence are always OK in parallel.
3. Each Worker writes only `.../staging/<card-or-lane>/`. **Never** multiple writers to `final/`.
4. **One INT** per evidence pack: only INT may write `final/`, validate required files, and complete the pack.
5. Reviewer depends on **INT SUCCESS** (parent edge), not on raw W* partials.
6. Hard plan-lint (runtime `kanban_plan_lint.py`, nested in hygiene every 5m):
   - >5 heterogeneous final artifacts on one Worker -> fat
   - collection+integration mixed -> fat
   - archival+live state mixed -> fat
   - final/ without staging -> fat
   - critical fat on todo/ready -> auto-block + comment (force replan/split)
   - protocol_no_complete streak >=2 -> needs_replan (no more identical retries)
7. Commander/Planner: when lint fires, **split first**, do not raise retries.
8. Card create defaults: `--max-retries 3`, one outcome, staging ownership path in body.
""".lstrip()

EXTRA = """
### Runtime enforcement
- `/root/main/runtime/kanban_plan_lint.py` — fat-card + protocol streak + max_retries>3
- Nested from `kanban_hygiene_guard.py` every 5m (`/etc/cron.d/kanban-hygiene-guard`)
- Reports: `runtime/kanban_plan_lint_last.json`, log `runtime/kanban_plan_lint.log`
- Thresholds: max **5** final artifacts/Worker; body smell >4500 chars; protocol streak **2**
- **Max fan-out bias:** split into parallel staging Workers + one INT; never multi-writer final/

```bash
/usr/local/lib/hermes-agent/venv/bin/python /root/main/runtime/kanban_plan_lint.py --dry-run
/usr/local/lib/hermes-agent/venv/bin/python /root/main/runtime/kanban_hygiene_guard.py --dry-run
```
""".lstrip()


def atomic_write(path: Path, text: str) -> None:
    tmp = Path(str(path) + ".tmp")
    tmp.write_text(text)
    tmp.chmod(path.stat().st_mode & 0o777)
    tmp.replace(path)


def main() -> None:
    for p in [
        Path("/root/.hermes/SOUL.md"),
        Path("/root/.hermes/profiles/worker/SOUL.md"),
        Path("/root/.hermes/profiles/orchestration1/SOUL.md"),
        Path("/root/.hermes/profiles/orchestration2/SOUL.md"),
        Path("/root/.hermes/profiles/planner/SOUL.md"),
        Path("/root/.hermes/profiles/reviewer/SOUL.md"),
    ]:
        t = p.read_text()
        if "### H. Maximum parallel fan-out" in t:
            print("already", p)
            continue
        if "### G. What we still do" in t:
            t = t.replace("### G. What we still do", FANOUT.rstrip() + "\n\n### G. What we still do", 1)
        elif "## Orchestration anti-stall" in t:
            t = t.rstrip() + "\n\n" + FANOUT.rstrip() + "\n"
        else:
            t = t.rstrip() + "\n\n## Orchestration anti-stall (mandatory)\n\n" + FANOUT.rstrip() + "\n"
        atomic_write(p, t)
        print("patched", p)

    skill = Path(
        "/root/.hermes/profiles/worker/skills/autonomous-ai-agents/hermes-local-ops/SKILL.md"
    )
    st = skill.read_text()
    if "### Runtime enforcement" not in st:
        st = st.rstrip() + "\n\n" + EXTRA.rstrip() + "\n"
        st = re.sub(r"(?m)^version: .*$", "version: 1.4.0", st, count=1)
        atomic_write(skill, st)
        print("skill -> 1.4.0")
    else:
        print("skill already has runtime enforcement")


if __name__ == "__main__":
    main()
