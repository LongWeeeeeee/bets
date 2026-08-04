#!/usr/bin/env python3
"""Salvage kanban attempt work so retries don't lose progress.

Problem (verified on host):
  - On max_runtime timeout, task_runs get outcome=timed_out with EMPTY summary.
  - Worker stdout/stderr lives in board logs (`{task_id}.log`, append-mode).
  - Agent session lives in profile state.db (messages/tools) but is NOT linked
    into task_runs / next spawn prompt.
  - Next attempt starts cold: "work kanban task <id>" only.

This guard (cron-safe, idempotent):
  1) For each timed_out/crashed/protocol-failed run without salvage summary:
     - resolve worker log (default board + named boards)
     - match profile session by start window / task-id in messages
     - collect artifact fingerprints (workspace, runtime staging, PARTIAL)
     - write runtime/task_salvage/<task_id>/run_<run_id>/ pack
     - set task_runs.summary to salvage path + short extract
     - merge session_id + salvage_dir into task_runs.metadata JSON
  2) For ready/todo tasks with prior failed runs: inject SALVAGE_RESUME block
     into body once, pointing at latest salvage pack (all assignees).
  3) Reviewer-specific PARTIAL path still honored; general pack is universal.

Does not modify Hermes package sources.
"""
from __future__ import annotations
# --- bootstrap раскладки: соседние эксперименты живут в runtime/experiments/<тема>/
import sys as _sys, pathlib as _pathlib
_repo_root = next((p for p in _pathlib.Path(__file__).resolve().parents if (p / '.git').exists()), None)
if _repo_root is not None:
    for _exp_dir in sorted((_repo_root / 'runtime' / 'experiments').glob('*')):
        if _exp_dir.is_dir() and str(_exp_dir) not in _sys.path:
            _sys.path.insert(0, str(_exp_dir))

import hashlib
import json
import os
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional

from kanban_lane_paths import lane_home, lane_runtime_root, selected_lane

LANE = selected_lane()
HERMES = lane_home(LANE)
KANBAN_DB = HERMES / "kanban.db"
BOARDS_ROOT = HERMES / "kanban" / "boards"
DEFAULT_LOGS = HERMES / "kanban" / "logs"
RUNTIME_ROOT = lane_runtime_root(LANE)
SALVAGE_ROOT = RUNTIME_ROOT / "task_salvage"
STATE_PATH = RUNTIME_ROOT / "task_salvage_guard_state.json"
LAST_PATH = RUNTIME_ROOT / "task_salvage_guard_last.json"
LOG_PATH = RUNTIME_ROOT / "task_salvage_guard.log"

RESUME_MARK = "<!-- TASK_SALVAGE_RESUME_v1 -->"
PROFILE_STATE = {
    "worker": HERMES / "profiles" / "worker" / "state.db",
    "reviewer": HERMES / "profiles" / "reviewer" / "state.db",
    "planner": HERMES / "profiles" / "planner" / "state.db",
    "default": HERMES / "state.db",
    "orchestration1": HERMES / "profiles" / "orchestration1" / "state.db",
    "orchestration2": HERMES / "profiles" / "orchestration2" / "state.db",
    "orch1planner": Path("/root/.hermes/profiles/orch1planner/state.db"),
    "orch1worker": Path("/root/.hermes/profiles/orch1worker/state.db"),
    "orch1reviewer": Path("/root/.hermes/profiles/orch1reviewer/state.db"),
    "orch2planner": Path("/root/.hermes/profiles/orch2planner/state.db"),
    "orch2worker": Path("/root/.hermes/profiles/orch2worker/state.db"),
    "orch2reviewer": Path("/root/.hermes/profiles/orch2reviewer/state.db"),
}

ARTIFACT_GLOBS = [
    "runtime/reviewer_partials/{tid}/**",
    "runtime/**/staging/**",
    "runtime/**/{tid}/**",
]


def log(msg: str) -> None:
    line = f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {msg}"
    print(line)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def load_state() -> dict:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text())
        except Exception:
            return {}
    return {}


def save_state(st: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(st, indent=2, sort_keys=True) + "\n")
    tmp.replace(STATE_PATH)


def find_worker_log(task_id: str) -> Optional[Path]:
    candidates = []
    p = DEFAULT_LOGS / f"{task_id}.log"
    if p.exists():
        candidates.append(p)
    if BOARDS_ROOT.exists():
        for b in BOARDS_ROOT.iterdir():
            lp = b / "logs" / f"{task_id}.log"
            if lp.exists():
                candidates.append(lp)
    if not candidates:
        return None
    # prefer largest / newest
    candidates.sort(key=lambda x: (x.stat().st_size, x.stat().st_mtime), reverse=True)
    return candidates[0]


def tail_text(path: Path, max_bytes: int = 80_000) -> str:
    data = path.read_bytes()
    if len(data) > max_bytes:
        data = data[-max_bytes:]
    # strip CR-heavy TUI noise a bit
    text = data.decode("utf-8", errors="replace")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return text


def extract_log_signals(log_text: str) -> dict[str, Any]:
    lines = [ln.strip() for ln in log_text.splitlines() if ln.strip()]
    # keep last non-spinner useful lines
    interesting = []
    pats = (
        re.compile(r"SUCCESS|FAILED|APPROVE|ISSUES|kanban complete|kanban block", re.I),
        re.compile(r"wrote |created |saved |commit |pytest|PASS|FAIL|error|Error|Traceback"),
        re.compile(r"PARTIAL|staging/|runtime/|final/"),
        re.compile(r"^#{1,3}\s"),
    )
    for ln in lines[-400:]:
        if any(p.search(ln) for p in pats):
            interesting.append(ln[:300])
    # always keep last 40 lines stripped of pure spinner
    tail = []
    for ln in lines[-80:]:
        if "preparing " in ln and ln.count("…") and len(ln) < 80:
            continue
        tail.append(ln[:300])
    return {
        "interesting_tail": interesting[-60:],
        "raw_tail": tail[-40:],
        "n_lines": len(lines),
    }


def match_session(profile: str, started_at: Optional[int], ended_at: Optional[int], task_id: str) -> Optional[dict]:
    """Link run → session.

    Priority:
      1) session whose early user prompt is exactly ``work kanban task <id>``
         inside the run start window (unique per spawn)
      2) time-window closest session on primary profile
    Never prefer a session that only *mentions* the id in compressed todos.
    """
    if not started_at:
        return None

    primary = PROFILE_STATE.get(profile or "")
    dbs: list[Path] = []
    if primary and primary.exists():
        dbs.append(primary)
    for alt in ("worker", "reviewer", "planner"):
        p = PROFILE_STATE.get(alt)
        if p and p.exists() and p not in dbs:
            dbs.append(p)
    dbs = dbs[:2]

    st = float(started_at)
    en = float(ended_at) if ended_at else st + 7200.0
    prompt_exact = f"work kanban task {task_id}"
    best = None
    best_score = -1

    for db_path in dbs:
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only=ON")
        except Exception:
            continue
        try:
            # 1) Exact spawn prompt near run start (first user messages)
            exact = conn.execute(
                """
                SELECT m.session_id AS sid, MIN(m.timestamp) AS first_ts, COUNT(*) AS c
                FROM messages m
                WHERE m.role = 'user'
                  AND m.timestamp BETWEEN ? AND ?
                  AND m.content LIKE ?
                GROUP BY m.session_id
                ORDER BY first_ts ASC
                LIMIT 8
                """,
                (st - 30.0, st + 180.0, f"%{prompt_exact}%"),
            ).fetchall()
            for hit in exact:
                r = conn.execute(
                    """
                    SELECT id, source, title, message_count, tool_call_count,
                           started_at, ended_at, end_reason
                    FROM sessions WHERE id = ?
                    """,
                    (hit["sid"],),
                ).fetchone()
                if not r:
                    continue
                # Verify FIRST user message is the spawn for THIS task_id
                # (avoid siblings that only mention id in later compressed todos).
                first_user = conn.execute(
                    """
                    SELECT content FROM messages
                    WHERE session_id = ? AND role = 'user'
                    ORDER BY timestamp ASC, id ASC
                    LIMIT 3
                    """,
                    (hit["sid"],),
                ).fetchall()
                first_blob = "\n".join((x[0] or "") for x in first_user)
                # Require exact spawn phrase early; reject if another task id is primary.
                if prompt_exact not in first_blob:
                    continue
                # Prefer messages that start with the spawn line (or SALVAGE-augmented)
                head = first_blob.lstrip()[:200]
                if not (
                    head.startswith(prompt_exact)
                    or prompt_exact in head.split("\n", 1)[0]
                    or f"work kanban task {task_id}" in head[:120]
                ):
                    # still accept if exact phrase is in first 3 user msgs (salvage inject)
                    if prompt_exact not in first_blob[:800]:
                        continue
                score = 200 + min(int(r["tool_call_count"] or 0), 50)
                # closer first_ts to started_at is better
                score += max(0, 30 - int(abs(float(hit["first_ts"]) - st)))
                if score > best_score:
                    best_score = score
                    best = {
                        "session_id": r["id"],
                        "profile_db": str(db_path),
                        "profile": next((k for k, v in PROFILE_STATE.items() if str(v) == str(db_path)), profile),
                        "message_count": r["message_count"],
                        "tool_call_count": r["tool_call_count"],
                        "started_at": r["started_at"],
                        "ended_at": r["ended_at"],
                        "end_reason": r["end_reason"],
                        "score": score,
                        "match": "exact_prompt",
                    }

            if best_score >= 200:
                # unique enough
                break

            # 2) Time-window fallback only if no exact prompt
            rows = conn.execute(
                """
                SELECT id, source, title, message_count, tool_call_count,
                       started_at, ended_at, end_reason
                FROM sessions
                WHERE started_at BETWEEN ? AND ?
                ORDER BY started_at ASC
                LIMIT 30
                """,
                (st - 60.0, st + 180.0),
            ).fetchall()
            for r in rows:
                # verify this session's first user msg mentions this task id uniquely if possible
                um = conn.execute(
                    """
                    SELECT content FROM messages
                    WHERE session_id = ? AND role = 'user'
                    ORDER BY id ASC LIMIT 3
                    """,
                    (r["id"],),
                ).fetchall()
                blob = "\n".join((x["content"] or "") for x in um)
                if prompt_exact in blob:
                    score = 200 + min(int(r["tool_call_count"] or 0), 50)
                    match = "exact_prompt_first_user"
                elif task_id in blob and "work kanban task" in blob:
                    # first user is some kanban task but maybe not ours
                    if f"work kanban task {task_id}" in blob:
                        score = 200
                        match = "exact_prompt_first_user"
                    else:
                        # wrong task — skip
                        continue
                elif task_id in blob:
                    # weak — mention only (todos etc.)
                    score = 20 + min(int(r["tool_call_count"] or 0), 20)
                    match = "mention_only"
                else:
                    # pure time proximity — weak, only if single session in window
                    score = 5 + min(int(r["tool_call_count"] or 0), 10)
                    match = "time_only"
                score += max(0, 20 - int(abs(float(r["started_at"]) - st)))
                if score > best_score:
                    best_score = score
                    best = {
                        "session_id": r["id"],
                        "profile_db": str(db_path),
                        "profile": next((k for k, v in PROFILE_STATE.items() if str(v) == str(db_path)), profile),
                        "message_count": r["message_count"],
                        "tool_call_count": r["tool_call_count"],
                        "started_at": r["started_at"],
                        "ended_at": r["ended_at"],
                        "end_reason": r["end_reason"],
                        "score": score,
                        "match": match,
                    }

            if best_score >= 200 and primary and db_path == primary:
                break
        finally:
            conn.close()

    # Drop very weak time-only matches when batch fan-out makes them ambiguous
    if best and best.get("match") == "time_only" and best_score < 40:
        return None
    if best and best.get("match") == "mention_only" and best_score < 50:
        return None
    return best


def session_excerpt(profile_db: str, session_id: str, max_chars: int = 60_000) -> str:
    conn = sqlite3.connect(profile_db)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT role, content, tool_name, reasoning, timestamp
            FROM messages
            WHERE session_id = ? AND IFNULL(active,1)=1
            ORDER BY id ASC
            """,
            (session_id,),
        ).fetchall()
    finally:
        conn.close()
    parts: list[str] = []
    total = 0
    # Prefer last N messages for recency, but keep first user prompt
    if not rows:
        return "(no messages)"
    keep_idx = set(range(min(6, len(rows)))) | set(range(max(0, len(rows) - 40), len(rows)))
    for i, r in enumerate(rows):
        if i not in keep_idx:
            continue
        role = r["role"] or "?"
        content = (r["content"] or "").strip()
        tool = r["tool_name"] or ""
        reasoning = ""
        try:
            reasoning = (r["reasoning"] or "") if "reasoning" in r.keys() else ""
        except Exception:
            reasoning = ""
        if not reasoning:
            try:
                reasoning = (r["reasoning_content"] or "") if "reasoning_content" in r.keys() else ""
            except Exception:
                reasoning = ""
        block = f"### {role}" + (f" tool={tool}" if tool else "") + "\n"
        if content:
            # compress huge tool dumps
            if role == "tool" and len(content) > 1500:
                content = content[:1200] + f"\n…[truncated tool output {len(content)} chars]"
            block += content + "\n"
        if reasoning and role == "assistant" and len(reasoning) > 0:
            block += "\n[reasoning excerpt]\n" + reasoning[:2000] + "\n"
        if total + len(block) > max_chars:
            parts.append("…[session excerpt cap reached]")
            break
        parts.append(block)
        total += len(block)
    return "\n".join(parts)


def collect_artifacts(task_id: str, workspace: Optional[str]) -> list[dict]:
    found: list[dict] = []
    roots: list[Path] = [
        RUNTIME_ROOT / "reviewer_partials" / task_id,
        SALVAGE_ROOT / task_id,
    ]
    if workspace:
        wp = Path(workspace)
        # Never walk giant project roots (/root/main) — only task-scoped dirs
        skip_roots = {"/root", "/root/main", "/home", "/"}
        if wp.exists() and str(wp.resolve()) not in skip_roots and wp.name not in {"main", "root"}:
            roots.append(wp)
            for sub in ("staging", "final", "artifacts", "out", "output"):
                p = wp / sub
                if p.exists():
                    roots.append(p)
        elif wp.exists():
            # giant root: only look for task-id / staging children one level deep
            for sub in ("staging", "final", "artifacts", "runtime"):
                p = wp / sub
                if p.is_dir():
                    tdir = p / task_id
                    if tdir.exists():
                        roots.append(tdir)
                    # names containing task id
                    try:
                        for child in p.iterdir():
                            if task_id in child.name and child.is_dir():
                                roots.append(child)
                    except Exception:
                        pass

    # Cheap targeted scans only — never full runtime/** rglob (too slow on this host)
    rt = Path("/root/main/runtime")
    if rt.exists():
        for child in rt.iterdir():
            name = child.name
            if task_id in name and child.is_dir():
                roots.append(child)
            if child.is_dir():
                for sub in ("staging", "final", "work_staging"):
                    p = child / sub
                    if p.is_dir() and task_id in str(p):
                        roots.append(p)
                tdir = child / task_id
                if tdir.is_dir():
                    roots.append(tdir)

    seen = set()
    for root in roots:
        if not root.exists():
            continue
        try:
            if root.is_file():
                paths = [root]
            else:
                # depth-limited walk
                paths = []
                for dirpath, dirnames, filenames in os.walk(root):
                    rel_depth = Path(dirpath).relative_to(root).parts
                    if len(rel_depth) > 4:
                        dirnames[:] = []
                        continue
                    dirnames[:] = [
                        d
                        for d in dirnames
                        if d not in {".git", "node_modules", "__pycache__", ".venv", "venv"}
                        or task_id in d
                    ][:40]
                    for fn in filenames[:80]:
                        paths.append(Path(dirpath) / fn)
                        if len(paths) > 200:
                            break
                    if len(paths) > 200:
                        break
        except Exception:
            continue
        for f in paths:
            if not f.is_file():
                continue
            if f.suffix.lower() in {".pyc", ".png", ".jpg", ".jpeg", ".mp4", ".zip", ".7z", ".gguf", ".bin"}:
                continue
            try:
                sz = f.stat().st_size
            except Exception:
                continue
            if sz > 2_000_000:
                continue
            try:
                key = str(f.resolve())
            except Exception:
                key = str(f)
            if key in seen:
                continue
            seen.add(key)
            try:
                h = hashlib.sha256(f.read_bytes()).hexdigest()[:16]
            except Exception:
                h = "?"
            try:
                mt = int(f.stat().st_mtime)
            except Exception:
                mt = 0
            found.append({"path": str(f), "size": sz, "mtime": mt, "sha256_16": h})
    found.sort(key=lambda x: x["mtime"], reverse=True)
    return found[:80]



def session_id_exclusive(conn: sqlite3.Connection, session_id: str, task_id: str) -> bool:
    """True if session_id is free or already owned by this task_id."""
    if not session_id:
        return False
    try:
        row = conn.execute(
            "SELECT id FROM tasks WHERE session_id = ? AND id != ? LIMIT 1",
            (session_id, task_id),
        ).fetchone()
        return row is None
    except Exception:
        return True


def already_salvaged(summary: Optional[str], metadata: Optional[str]) -> bool:
    if summary and "task_salvage/" in summary:
        return True
    if metadata:
        try:
            m = json.loads(metadata)
            if m.get("salvage_dir"):
                return True
        except Exception:
            pass
    return False


def write_salvage_pack(
    *,
    task_id: str,
    run_id: int,
    title: str,
    profile: str,
    outcome: str,
    error: str,
    started_at: Optional[int],
    ended_at: Optional[int],
    workspace: Optional[str],
) -> dict[str, Any]:
    out_dir = SALVAGE_ROOT / task_id / f"run_{run_id}"
    out_dir.mkdir(parents=True, exist_ok=True)

    log_path = find_worker_log(task_id)
    log_signals = {}
    if log_path:
        log_text = tail_text(log_path)
        (out_dir / "log_tail.txt").write_text(log_text)
        log_signals = extract_log_signals(log_text)
        (out_dir / "log_signals.json").write_text(json.dumps(log_signals, ensure_ascii=False, indent=2) + "\n")
        # symlink/copy pointer
        (out_dir / "worker_log_path.txt").write_text(str(log_path) + "\n")

    sess = match_session(profile, started_at, ended_at, task_id)
    session_id = None
    if sess:
        session_id = sess["session_id"]
        (out_dir / "session_meta.json").write_text(json.dumps(sess, indent=2) + "\n")
        excerpt = session_excerpt(sess["profile_db"], session_id)
        (out_dir / "session_excerpt.md").write_text(excerpt + "\n")

    arts = collect_artifacts(task_id, workspace)
    (out_dir / "artifacts.json").write_text(json.dumps(arts, indent=2) + "\n")

    # PARTIAL convenience copy pointer
    partial = RUNTIME_ROOT / "reviewer_partials" / task_id / "PARTIAL.md"
    partial_note = str(partial) if partial.exists() else None

    interesting = log_signals.get("interesting_tail") or []
    md = [
        f"# SALVAGE pack — {task_id} run_{run_id}",
        "",
        f"- title: {title}",
        f"- profile/assignee: {profile}",
        f"- outcome: {outcome}",
        f"- error: {error}",
        f"- started_at: {started_at}",
        f"- ended_at: {ended_at}",
        f"- worker_log: {log_path}",
        f"- session_id: {session_id}",
        f"- partial: {partial_note}",
        f"- artifacts: {len(arts)}",
        "",
        "## How to resume",
        "1. Read this pack BEFORE broad re-analysis.",
        "2. Reuse verified evidence/paths below; do not redo completed tool work.",
        "3. Continue only remaining outcomes; write updates back into PARTIAL/salvage.",
        "4. End with kanban complete/block and SUCCESS/FAILED evidence.",
        "",
        "## Interesting log lines",
    ]
    if interesting:
        md.extend([f"- `{x}`" for x in interesting[-40:]])
    else:
        md.append("- (none extracted)")
    md.append("")
    md.append("## Artifact index (newest first)")
    for a in arts[:30]:
        md.append(f"- `{a['path']}` sha={a['sha256_16']} size={a['size']}")
    if session_id:
        md.append("")
        md.append(f"## Session excerpt: `{out_dir / 'session_excerpt.md'}`")
    md.append("")
    md.append(f"## Raw log tail: `{out_dir / 'log_tail.txt'}`")
    salvage_md = out_dir / "SALVAGE.md"
    salvage_md.write_text("\n".join(md) + "\n")

    # fingerprint for delta detection
    fp_src = json.dumps({"arts": arts[:20], "sess": session_id, "err": error, "outcome": outcome}, sort_keys=True)
    fp = hashlib.sha256(fp_src.encode()).hexdigest()[:24]
    (out_dir / "fingerprint.txt").write_text(fp + "\n")

    summary = (
        f"salvage:{out_dir} session={session_id or '-'} arts={len(arts)} "
        f"log={'yes' if log_path else 'no'} outcome={outcome}"
    )
    return {
        "salvage_dir": str(out_dir),
        "salvage_md": str(salvage_md),
        "session_id": session_id,
        "session_meta": sess,
        "log_path": str(log_path) if log_path else None,
        "artifacts": len(arts),
        "fingerprint": fp,
        "summary": summary,
    }


def inject_resume(body: str, pack: dict, task_id: str, fails: int) -> str:
    if RESUME_MARK in (body or ""):
        # refresh path if old mark exists — replace block
        body = re.sub(
            rf"\n*{re.escape(RESUME_MARK)}[\s\S]*?(?=\n<!--|\Z)",
            "\n",
            body or "",
            count=1,
        )
    appendix = (
        f"\n\n{RESUME_MARK}\n"
        f"## SALVAGE / do-not-lose-work (auto, attempt≥{fails + 1})\n"
        f"Previous attempt ended non-success. **Continue from salvage, do not restart cold.**\n"
        f"- Salvage pack: `{pack['salvage_md']}`\n"
        f"- Dir: `{pack['salvage_dir']}`\n"
        f"- Session: `{pack.get('session_id') or 'unlinked'}`\n"
        f"- Worker log: `{pack.get('log_path') or 'n/a'}`\n"
        f"- Artifacts indexed: {pack.get('artifacts', 0)}\n"
        f"- Reviewer PARTIAL (if any): `runtime/reviewer_partials/{task_id}/PARTIAL.md`\n"
        f"\n"
        f"Required first tools:\n"
        f"1. Read SALVAGE.md + session_excerpt.md + artifacts.json\n"
        f"2. List what is already proven vs remaining\n"
        f"3. Only execute remaining work; update PARTIAL/salvage\n"
        f"4. Terminal: kanban complete/block + SUCCESS/FAILED evidence\n"
    )
    return (body or "").rstrip() + appendix


def process(conn: sqlite3.Connection, state: dict, dry_run: bool = False, only_task_id: str | None = None, only_run_id: int | None = None) -> dict:
    stats = {
        "salvaged_runs": [],
        "resume_injected": [],
        "skipped_already": [],
        "no_sources": [],
        "errors": [],
    }

    # 1) Backfill / salvage failed runs
    runs = conn.execute(
        """
        SELECT r.id AS run_id, r.task_id, r.profile, r.outcome, r.summary, r.metadata,
               r.error, r.started_at, r.ended_at, r.worker_pid,
               t.title, t.assignee, t.status, t.body, t.consecutive_failures,
               t.workspace_path, t.last_failure_error
        FROM task_runs r
        JOIN tasks t ON t.id = r.task_id
        WHERE r.outcome IN ('timed_out','crashed','failed','blocked')
           OR (r.error IS NOT NULL AND (
                r.error LIKE '%elapsed%limit%' OR
                r.error LIKE '%protocol violation%' OR
                r.error LIKE '%timed_out%' OR
                r.error LIKE '%timeout%'
           ))
        ORDER BY r.id DESC
        LIMIT 200
        """
    ).fetchall()

    # latest pack per task for resume injection
    if only_task_id:
        runs = [r for r in runs if r["task_id"] == only_task_id]
        if only_run_id is not None:
            runs = [r for r in runs if int(r["run_id"]) == int(only_run_id)]

    latest_pack: dict[str, dict] = {}

    for r in runs:
        run_id = int(r["run_id"])
        tid = r["task_id"]
        if already_salvaged(r["summary"], r["metadata"]):
            # Re-verify session link; batch fan-out previously collided on time-only matches.
            try:
                meta = json.loads(r["metadata"] or "{}")
                if not isinstance(meta, dict):
                    meta = {}
            except Exception:
                meta = {}
            old_sid = meta.get("session_id")
            new_sess = match_session(
                r["profile"] or r["assignee"] or "",
                r["started_at"],
                r["ended_at"],
                tid,
            )
            new_sid = new_sess.get("session_id") if new_sess else None
            need_rewrite = False
            if new_sid and new_sid != old_sid:
                need_rewrite = True
            elif not old_sid and new_sid:
                need_rewrite = True
            # also rewrite if pack dir missing
            sdir = meta.get("salvage_dir")
            if not sdir or not Path(str(sdir)).exists():
                need_rewrite = True
            if need_rewrite:
                try:
                    pack = write_salvage_pack(
                        task_id=tid,
                        run_id=run_id,
                        title=r["title"] or "",
                        profile=r["profile"] or r["assignee"] or "",
                        outcome=r["outcome"] or "",
                        error=r["error"] or r["last_failure_error"] or "",
                        started_at=r["started_at"],
                        ended_at=r["ended_at"],
                        workspace=r["workspace_path"],
                    )
                    meta.update(
                        {
                            "salvage_dir": pack["salvage_dir"],
                            "salvage_md": pack["salvage_md"],
                            "session_id": pack.get("session_id"),
                            "log_path": pack.get("log_path"),
                            "artifacts": pack.get("artifacts"),
                            "salvage_fp": pack.get("fingerprint"),
                            "salvaged_at": int(time.time()),
                            "session_match": (pack.get("session_meta") or {}).get("match"),
                        }
                    )
                    if not dry_run:
                        conn.execute(
                            "UPDATE task_runs SET summary = ?, metadata = ? WHERE id = ?",
                            (pack["summary"][:2000], json.dumps(meta, ensure_ascii=False), run_id),
                        )
                        if pack.get("session_id") and session_id_exclusive(conn, pack["session_id"], tid):
                            conn.execute(
                                "UPDATE tasks SET session_id = ? WHERE id = ? AND (session_id IS NULL OR session_id = '' OR session_id = ?)",
                                (pack["session_id"], tid, old_sid or ""),
                            )
                        elif pack.get("session_id") and not session_id_exclusive(conn, pack["session_id"], tid):
                            # sibling collision — keep pack metadata but do not steal shared session
                            pack["session_id_collision"] = True
                    stats["salvaged_runs"].append(
                        {
                            "run_id": run_id,
                            "task_id": tid,
                            "session_id": pack.get("session_id"),
                            "salvage_dir": pack["salvage_dir"],
                            "artifacts": pack.get("artifacts"),
                            "relink": True,
                        }
                    )
                    latest_pack.setdefault(tid, pack)
                    continue
                except Exception as e:
                    stats["errors"].append(f"relink{run_id}:{e}")
            stats["skipped_already"].append(run_id)
            if sdir:
                latest_pack.setdefault(
                    tid,
                    {
                        "salvage_dir": sdir,
                        "salvage_md": meta.get("salvage_md") or str(Path(str(sdir)) / "SALVAGE.md"),
                        "session_id": meta.get("session_id"),
                        "log_path": meta.get("log_path"),
                        "artifacts": meta.get("artifacts", 0),
                        "summary": r["summary"],
                    },
                )
            continue

        try:
            pack = write_salvage_pack(
                task_id=tid,
                run_id=run_id,
                title=r["title"] or "",
                profile=r["profile"] or r["assignee"] or "",
                outcome=r["outcome"] or "",
                error=r["error"] or r["last_failure_error"] or "",
                started_at=r["started_at"],
                ended_at=r["ended_at"],
                workspace=r["workspace_path"],
            )
        except Exception as e:
            stats["errors"].append(f"run{run_id}:{e}")
            continue

        if not pack.get("log_path") and not pack.get("session_id") and pack.get("artifacts", 0) == 0:
            stats["no_sources"].append(run_id)

        # update task_runs summary/metadata
        meta = {}
        try:
            meta = json.loads(r["metadata"] or "{}")
            if not isinstance(meta, dict):
                meta = {"prev": meta}
        except Exception:
            meta = {"raw": r["metadata"]}
        meta.update(
            {
                "salvage_dir": pack["salvage_dir"],
                "salvage_md": pack["salvage_md"],
                "session_id": pack.get("session_id"),
                "log_path": pack.get("log_path"),
                "artifacts": pack.get("artifacts"),
                "salvage_fp": pack.get("fingerprint"),
                "salvaged_at": int(time.time()),
            }
        )
        if not dry_run:
            conn.execute(
                "UPDATE task_runs SET summary = ?, metadata = ? WHERE id = ?",
                (pack["summary"][:2000], json.dumps(meta, ensure_ascii=False), run_id),
            )
            # also set tasks.session_id if empty and we found one — never steal sibling session
            if pack.get("session_id") and session_id_exclusive(conn, pack["session_id"], tid):
                conn.execute(
                    "UPDATE tasks SET session_id = COALESCE(NULLIF(session_id,''), ?) WHERE id = ?",
                    (pack["session_id"], tid),
                )
            elif pack.get("session_id") and not session_id_exclusive(conn, pack["session_id"], tid):
                pack["session_id_collision"] = True
                meta["session_id_collision"] = True
                if not dry_run:
                    conn.execute(
                        "UPDATE task_runs SET metadata = ? WHERE id = ?",
                        (json.dumps(meta, ensure_ascii=False), run_id),
                    )
        stats["salvaged_runs"].append({"run_id": run_id, "task_id": tid, **{k: pack.get(k) for k in ("session_id", "salvage_dir", "artifacts")}})
        # keep newest run pack per task (runs are DESC)
        latest_pack.setdefault(tid, pack)

    # 2) Inject resume into ready/todo with failures
    tasks = conn.execute(
        """
        SELECT id, title, body, status, assignee, consecutive_failures, last_failure_error, workspace_path
        FROM tasks
        WHERE status IN ('ready','todo','blocked')
          AND IFNULL(consecutive_failures,0) >= 1
        ORDER BY created_at DESC
        LIMIT 200
        """
    ).fetchall()

    if only_task_id:
        tasks = [t for t in tasks if t["id"] == only_task_id]

    for t in tasks:
        tid = t["id"]
        pack = latest_pack.get(tid)
        if not pack:
            # try build from latest failed run if any not in window
            run = conn.execute(
                """
                SELECT id, profile, outcome, error, started_at, ended_at, summary, metadata
                FROM task_runs WHERE task_id=?
                ORDER BY id DESC LIMIT 1
                """,
                (tid,),
            ).fetchone()
            if not run:
                continue
            if already_salvaged(run["summary"], run["metadata"]):
                try:
                    meta = json.loads(run["metadata"] or "{}")
                    pack = {
                        "salvage_dir": meta.get("salvage_dir"),
                        "salvage_md": meta.get("salvage_md") or str(Path(meta.get("salvage_dir") or "") / "SALVAGE.md"),
                        "session_id": meta.get("session_id"),
                        "log_path": meta.get("log_path"),
                        "artifacts": meta.get("artifacts", 0),
                    }
                except Exception:
                    pack = None
            else:
                try:
                    pack = write_salvage_pack(
                        task_id=tid,
                        run_id=int(run["id"]),
                        title=t["title"] or "",
                        profile=run["profile"] or t["assignee"] or "",
                        outcome=run["outcome"] or "",
                        error=run["error"] or t["last_failure_error"] or "",
                        started_at=run["started_at"],
                        ended_at=run["ended_at"],
                        workspace=t["workspace_path"],
                    )
                    if not dry_run:
                        meta = {}
                        try:
                            meta = json.loads(run["metadata"] or "{}") or {}
                        except Exception:
                            meta = {}
                        meta.update(
                            {
                                "salvage_dir": pack["salvage_dir"],
                                "salvage_md": pack["salvage_md"],
                                "session_id": pack.get("session_id"),
                                "log_path": pack.get("log_path"),
                                "artifacts": pack.get("artifacts"),
                                "salvage_fp": pack.get("fingerprint"),
                                "salvaged_at": int(time.time()),
                            }
                        )
                        conn.execute(
                            "UPDATE task_runs SET summary=?, metadata=? WHERE id=?",
                            (pack["summary"][:2000], json.dumps(meta, ensure_ascii=False), int(run["id"])),
                        )
                except Exception as e:
                    stats["errors"].append(f"task{tid}:{e}")
                    pack = None
        if not pack or not pack.get("salvage_md"):
            continue

        # Inject into ready/todo always; blocked only if resume missing (for later unblock)
        if t["status"] in ("ready", "todo") or RESUME_MARK not in (t["body"] or ""):
            new_body = inject_resume(t["body"] or "", pack, tid, int(t["consecutive_failures"] or 0))
            if new_body != (t["body"] or ""):
                if not dry_run:
                    conn.execute("UPDATE tasks SET body=? WHERE id=?", (new_body, tid))
                stats["resume_injected"].append(tid)

        state[f"task:{tid}"] = {
            "salvage_md": pack.get("salvage_md"),
            "session_id": pack.get("session_id"),
            "ts": int(time.time()),
            "fails": int(t["consecutive_failures"] or 0),
            "status": t["status"],
        }

    return stats


def main(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--task-id", default=None, help="Salvage only this task id")
    ap.add_argument("--run-id", type=int, default=None, help="Salvage only this task_runs id")
    args = ap.parse_args(argv)
    only_task = args.task_id or (os.environ.get("HERMES_SALVAGE_TASK_ID") or "").strip() or None
    only_run = args.run_id
    if only_run is None:
        env_run = (os.environ.get("HERMES_SALVAGE_RUN_ID") or "").strip()
        if env_run.isdigit():
            only_run = int(env_run)

    if not KANBAN_DB.exists():
        log(f"FAIL no db {KANBAN_DB}")
        return 2

    # also process board DBs
    dbs = [KANBAN_DB]
    if BOARDS_ROOT.exists():
        for b in BOARDS_ROOT.iterdir():
            p = b / "kanban.db"
            if p.exists():
                dbs.append(p)

    state = load_state()
    all_stats: dict[str, Any] = {"dbs": {}}
    for db in dbs:
        conn = sqlite3.connect(str(db), timeout=60)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA busy_timeout=60000")
            if not args.dry_run:
                conn.execute("BEGIN")
            # skip empty
            n = conn.execute("SELECT COUNT(*) FROM task_runs").fetchone()[0]
            if n == 0:
                all_stats["dbs"][str(db)] = {"empty": True}
                continue
            st = process(conn, state, dry_run=args.dry_run, only_task_id=only_task, only_run_id=only_run)
            if not args.dry_run:
                conn.commit()
            all_stats["dbs"][str(db)] = st
            log(f"db={db} salvaged={len(st['salvaged_runs'])} resume={len(st['resume_injected'])} err={len(st['errors'])}")
        except Exception as e:
            all_stats["dbs"][str(db)] = {"error": str(e)}
            log(f"ERR db={db} {e}")
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            conn.close()

    if not args.dry_run:
        save_state(state)
    LAST_PATH.write_text(json.dumps({"ts": int(time.time()), "dry_run": args.dry_run, "stats": all_stats}, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
