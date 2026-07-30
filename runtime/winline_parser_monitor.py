#!/usr/bin/env python3
"""Continuous read-only Winline parser monitor.

The supervisor process launches each browser attempt in a bounded child process.
It never imports or calls Telegram dispatch code.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
MATCHES_PATH = ROOT / "runtime" / "sourcetv_matches.json"
EVIDENCE_PATH = ROOT / ".hermes" / "runtime" / "winline-parser-monitor" / "latest.json"
LOCK_PATH = ROOT / "runtime" / "winline_parser_monitor.lock"
INTERVAL_SECONDS = 30.0
ATTEMPT_TIMEOUT_SECONDS = 140.0
MAX_TECHNICAL_ATTEMPTS = 3

_stopping = False


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    tmp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(tmp, path)


def _load_current_matches() -> list[dict[str, Any]]:
    try:
        age = time.time() - MATCHES_PATH.stat().st_mtime
        if age > 180:
            return []
        raw = json.loads(MATCHES_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return []
    values = raw.values() if isinstance(raw, dict) else raw if isinstance(raw, list) else []
    matches: list[dict[str, Any]] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        team1 = str(item.get("radiant_team_name") or "").strip()
        team2 = str(item.get("dire_team_name") or "").strip()
        try:
            map_num = int(item.get("series_game_number") or 0)
        except (TypeError, ValueError):
            map_num = 0
        try:
            played = int(item.get("radiant_series_wins") or 0) + int(
                item.get("dire_series_wins") or 0
            )
        except (TypeError, ValueError):
            played = 0
        # series_game_number приходит из WebAPI/GC и бывает устаревшим между
        # картами; число сыгранных карт — надёжная нижняя граница номера карты.
        map_num = max(map_num, played + 1)
        if team1 and team2 and 1 <= map_num <= 5:
            matches.append(
                {
                    "match_id": item.get("match_id"),
                    "series_id": item.get("series_id"),
                    "team1": team1,
                    "team2": team2,
                    "map_num": map_num,
                }
            )
    return matches[:3]


def _result_payload(result: Any, request: dict[str, Any]) -> dict[str, Any]:
    odds = list(getattr(result, "odds", None) or [])
    return {
        "request": request,
        "status": getattr(result, "status", None),
        "match_found": bool(getattr(result, "match_found", False)),
        "odds": odds[:2],
        "market_kind": getattr(result, "market_kind", None),
        "map_num": getattr(result, "map_num", None),
        "p1_team": getattr(result, "p1_team", None),
        "p2_team": getattr(result, "p2_team", None),
        "market_closed": bool(getattr(result, "market_closed", False)),
        "details": str(getattr(result, "details", "") or "")[:500],
        "strict_current_map_odds": bool(
            getattr(result, "market_kind", None) == "current_map_winner"
            and len(odds) >= 2
            and all(isinstance(value, (int, float)) and float(value) > 1 for value in odds[:2])
        ),
    }


def _select_reachable_bookmaker_proxy() -> tuple[str, str]:
    try:
        from base import keys as proxy_keys
    except ImportError:
        import keys as proxy_keys  # type: ignore

    # The inventory is DE-first and US-last. Probe in reverse order because a
    # reachable proxy is more valuable than stale geographic preference.
    for proxy in reversed(list(getattr(proxy_keys, "BOOKMAKER_PROXY_POOL", []) or [])):
        parsed = urlparse(str(proxy))
        if not parsed.hostname or not parsed.port:
            continue
        try:
            with socket.create_connection((parsed.hostname, parsed.port), timeout=2):
                return str(proxy), parsed.hostname
        except OSError:
            continue
    raise RuntimeError("no reachable bookmaker proxy in configured pool")


def run_attempt() -> int:
    started = time.monotonic()
    matches = _load_current_matches()
    payload: dict[str, Any] = {
        "schema": "winline_parser_monitor.v1",
        "attempt_started_at": _utc_now(),
        "matches_seen": len(matches),
        "results": [],
        "technical_ok": True,
        "strict_success": False,
    }
    if not matches:
        payload["state"] = "idle_no_fresh_sourcetv_match"
        payload["elapsed_seconds"] = round(time.monotonic() - started, 3)
        print(json.dumps(payload, ensure_ascii=False))
        return 0

    sys.path.insert(0, str(ROOT / "base"))
    try:
        import bookmaker_selenium_odds as bookmaker

        selected_proxy, proxy_host = _select_reachable_bookmaker_proxy()
        bookmaker.BOOKMAKER_PROXY_URL = selected_proxy
        payload["proxy_host"] = proxy_host
        url = bookmaker.BOOKMAKER_URLS["live"]["winline"]
        for request in matches:
            results = bookmaker.run_sites_in_camoufox(
                selected_sites=["winline"],
                urls={"winline": url},
                team1=request["team1"],
                team2=request["team2"],
                mode="live",
                forced_map_num=request["map_num"],
            )
            if not results:
                raise RuntimeError("winline parser returned no SiteResult")
            payload["results"].append(_result_payload(results[0], request))
        load_failures = [
            item
            for item in payload["results"]
            if item.get("status") in {"error", "partial_load"}
            and "load_error=" in str(item.get("details") or "")
        ]
        if load_failures:
            payload["technical_ok"] = False
            payload["state"] = "technical_load_error"
            payload["error"] = "Winline page load did not complete"
        payload["strict_success"] = any(
            item["strict_current_map_odds"] for item in payload["results"]
        )
        if payload["technical_ok"]:
            payload["state"] = (
                "odds_parsed"
                if payload["strict_success"]
                else "market_not_available_or_not_matched"
            )
    except Exception as exc:
        payload["technical_ok"] = False
        payload["state"] = "technical_error"
        payload["error"] = f"{type(exc).__name__}: {exc}"[:700]

    payload["elapsed_seconds"] = round(time.monotonic() - started, 3)
    payload["attempt_finished_at"] = _utc_now()
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if payload["technical_ok"] else 2


def _run_bounded_attempt() -> tuple[dict[str, Any], int]:
    command = [sys.executable, str(Path(__file__).resolve()), "--attempt"]
    last: dict[str, Any] = {}
    for attempt_no in range(1, MAX_TECHNICAL_ATTEMPTS + 1):
        try:
            completed = subprocess.run(
                command,
                text=True,
                capture_output=True,
                timeout=ATTEMPT_TIMEOUT_SECONDS,
                check=False,
            )
            try:
                last = json.loads(completed.stdout.strip().splitlines()[-1])
            except (ValueError, IndexError):
                last = {
                    "schema": "winline_parser_monitor.v1",
                    "state": "invalid_child_output",
                    "technical_ok": False,
                    "stderr": completed.stderr[-700:],
                }
            last["attempt_number"] = attempt_no
            last["child_exit_code"] = completed.returncode
            if completed.returncode == 0:
                return last, attempt_no
        except subprocess.TimeoutExpired:
            last = {
                "schema": "winline_parser_monitor.v1",
                "state": "attempt_timeout",
                "technical_ok": False,
                "attempt_number": attempt_no,
                "timeout_seconds": ATTEMPT_TIMEOUT_SECONDS,
            }
        if attempt_no < MAX_TECHNICAL_ATTEMPTS and not _stopping:
            time.sleep(5 * (2 ** (attempt_no - 1)))
    return last, MAX_TECHNICAL_ATTEMPTS


def run_supervisor(*, once: bool) -> int:
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOCK_PATH.open("w", encoding="utf-8") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("winline parser monitor is already running", file=sys.stderr)
            return 3
        lock_file.write(str(os.getpid()))
        lock_file.flush()

        sequence = 0
        while not _stopping:
            sequence += 1
            cycle_started = time.monotonic()
            evidence, attempts = _run_bounded_attempt()
            evidence.update(
                {
                    "supervisor_pid": os.getpid(),
                    "sequence": sequence,
                    "technical_attempts": attempts,
                    "updated_at": _utc_now(),
                    "telegram_dispatch_enabled": False,
                    "next_cycle_seconds": None if once else INTERVAL_SECONDS,
                }
            )
            _atomic_write_json(EVIDENCE_PATH, evidence)
            print(
                json.dumps(
                    {
                        "sequence": sequence,
                        "state": evidence.get("state"),
                        "strict_success": evidence.get("strict_success", False),
                        "technical_attempts": attempts,
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
            if once:
                return 0 if evidence.get("technical_ok", False) else 2
            # Pause after completion as well as between fast cycles. A slow or
            # retried browser cycle must not cause a zero-delay request storm.
            deadline = time.monotonic() + INTERVAL_SECONDS
            while not _stopping and time.monotonic() < deadline:
                time.sleep(min(1.0, deadline - time.monotonic()))
    return 0


def _handle_stop(_signum: int, _frame: Any) -> None:
    global _stopping
    _stopping = True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", action="store_true")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if args.attempt:
        return run_attempt()
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)
    return run_supervisor(once=args.once)


if __name__ == "__main__":
    raise SystemExit(main())
