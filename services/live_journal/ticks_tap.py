"""Append compact SourceTV ticks without importing or controlling the probe.

Run from the repository root: python3 -m services.live_journal.ticks_tap
Source paths use base.sourcetv_bridge; --src takes precedence. LIVE_JOURNAL_DIR
defaults to <repo>/runtime/live_journal. Daily filenames use UTC read wall time.
Files are append-only, with no rotation, truncation or deletion.

State is in memory: restart re-emits first sightings, including picks/extra;
consumers must accept these duplicates. Equal game_time suppresses ticks even
if scores/picks changed; changes remain pending until game_time changes.
Extra changes accompany the next eligible tick rather than triggering one.
The probe's per-dump `timestamp` goes to top-level `src_ts` on every row and is
excluded from `extra`, otherwise `extra` would differ (and repeat) on every tick.
A repeated identical source-read error is logged once until a read succeeds.
Unreadable/invalid source polls are skipped, never evidence of disappearance.
Only a successfully read snapshot establishes absence; unchanged snapshots
still advance the gone timer. A returning match cancels its pending absence.
"""

import argparse
import json
import math
import os
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from base.sourcetv_bridge import resolve_sourcetv_matches_path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
COPIED_FIELDS = (
    "game_time", "radiant_lead", "radiant_score", "dire_score", "spectators",
    "league_id", "series_id", "series_game_number", "series_type",
    "radiant_series_wins", "dire_series_wins", "radiant_team_id", "dire_team_id",
    "radiant_team_name", "dire_team_name",
)
RESERVED_FIELDS = frozenset(COPIED_FIELDS) | {
    "schema", "event", "wall", "src_mtime", "match_id", "picks", "extra",
}
PICKS_KEY = "_cyberscore_heroes_and_pos"
SRC_TS_KEY = "timestamp"  # Probe dump time; changes on every dump.


def log(message):
    print(message, flush=True)


def extra(entry):
    return {key: value for key, value in entry.items()
            if key not in RESERVED_FIELDS and key != SRC_TS_KEY
            and not key.startswith("_")}


@dataclass
class MatchState:
    latest: dict
    written: dict
    missing_since: Optional[float] = None


class TickTap:
    def __init__(self, src, output_dir=None, min_gap_s=30.0, gone_after_s=120.0):
        for value in (min_gap_s, gone_after_s):
            if not math.isfinite(value) or value <= 0:
                raise ValueError("intervals must be finite and positive")
        self.src = Path(src)
        self.output_dir = Path(output_dir if output_dir is not None else
                               os.getenv("LIVE_JOURNAL_DIR", str(
                                   PROJECT_ROOT / "runtime/live_journal"))).expanduser()
        self.min_gap_s = min_gap_s
        self.gone_after_s = gone_after_s
        self.matches = {}
        self.mtime_ns = None
        self.src_mtime = None
        self.last_skip = None

    def _append(self, match_id, entry, previous, event, wall):
        row = {"schema": "live_ticks.v1", "event": event, "wall": wall,
               "src_mtime": self.src_mtime, "match_id": match_id}
        row.update((key, entry[key]) for key in COPIED_FIELDS if key in entry)
        if SRC_TS_KEY in entry:
            row["src_ts"] = entry[SRC_TS_KEY]
        if PICKS_KEY in entry and (previous is None or
                                  entry[PICKS_KEY] != previous.get(PICKS_KEY)):
            row["picks"] = entry[PICKS_KEY]
        current_extra = extra(entry)
        if previous is None or current_extra != extra(previous):
            row["extra"] = current_extra
        line = json.dumps(row, separators=(",", ":"), ensure_ascii=False) + "\n"
        day = datetime.fromtimestamp(wall, timezone.utc).strftime("%Y%m%d")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        with (self.output_dir / ("ticks_%s.jsonl" % day)).open(
                "a", encoding="utf-8") as journal:
            journal.write(line)
            journal.flush()

    def _observe(self, match_id, entry, wall):
        if not isinstance(entry, dict):
            raise ValueError("entry is not an object")
        game_time = entry.get("game_time")
        if (isinstance(game_time, bool) or not isinstance(game_time, (int, float))
                or not math.isfinite(game_time)):
            raise ValueError("game_time is not a finite number")
        if PICKS_KEY in entry and not isinstance(entry[PICKS_KEY], dict):
            raise ValueError("picks is not an object")
        state = self.matches.get(match_id)
        if state is None:
            self._append(match_id, entry, None, "tick", wall)
            self.matches[match_id] = MatchState(entry, entry)
            log("new match %s" % match_id)
            return
        previous = state.written
        if game_time in (state.latest["game_time"], previous["game_time"]):
            state.latest = entry
            return
        if (game_time - previous["game_time"] >= self.min_gap_s
                or entry.get("radiant_score") != previous.get("radiant_score")
                or entry.get("dire_score") != previous.get("dire_score")
                or entry.get(PICKS_KEY) != previous.get(PICKS_KEY)):
            self._append(match_id, entry, previous, "tick", wall)
            state.written = entry
        state.latest = entry

    def _expire(self, wall):
        for match_id, state in list(self.matches.items()):
            if (state.missing_since is not None
                    and wall - state.missing_since >= self.gone_after_s):
                self._append(match_id, state.latest, state.written, "gone", wall)
                del self.matches[match_id]
                log("gone match %s" % match_id)

    def poll(self):
        """Read a changed source once; malformed entries cannot block peers."""
        try:
            stat = self.src.stat()
            if stat.st_mtime_ns == self.mtime_ns:
                self._expire(time.time())
                return
            with self.src.open(encoding="utf-8") as source:
                stat = os.fstat(source.fileno())  # Bind mtime to the opened inode.
                payload = json.load(source)
            if not isinstance(payload, dict):
                raise ValueError("source is not an object")
        except (OSError, ValueError, UnicodeError, RecursionError) as exc:
            message = "skip source %s: %s" % (self.src, exc)
            if message != self.last_skip:
                log(message)
                self.last_skip = message
            return
        self.last_skip = None
        wall = time.time()
        self.src_mtime = stat.st_mtime
        present = set()
        for key, entry in payload.items():
            try:
                match_id = int(key)
                present.add(match_id)
                state = self.matches.get(match_id)
                if state is not None:
                    state.missing_since = None  # Invalid present entry is not absent.
                self._observe(match_id, entry, wall)
            except (ValueError, TypeError, OverflowError, RecursionError) as exc:
                log("skip match %s: %s" % (key, exc))
        for match_id, state in self.matches.items():
            if match_id not in present and state.missing_since is None:
                state.missing_since = wall
        self._expire(wall)
        # Failed writes propagate; do not mark that snapshot consumed.
        self.mtime_ns = stat.st_mtime_ns


def positive_seconds(text):
    value = float(text)
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError("must be finite and positive")
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--src", type=Path, help="override SourceTV bridge path")
    parser.add_argument("--poll-s", type=positive_seconds, default=5.0)
    parser.add_argument("--min-gap-s", type=positive_seconds, default=30.0)
    parser.add_argument("--gone-after-s", type=positive_seconds, default=120.0)
    args = parser.parse_args(argv)
    src = args.src if args.src is not None else resolve_sourcetv_matches_path(PROJECT_ROOT)
    tap = TickTap(src, min_gap_s=args.min_gap_s, gone_after_s=args.gone_after_s)
    stopping = False

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    previous_handler = signal.signal(signal.SIGTERM, stop)
    try:
        while not stopping:
            try:
                tap.poll()
            except OSError as exc:
                tap.mtime_ns = None  # Retry pending writes on the next poll.
                log("journal write failed: %s" % exc)
            if not stopping:
                time.sleep(args.poll_s)
    except KeyboardInterrupt:
        pass
    finally:
        signal.signal(signal.SIGTERM, previous_handler)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
