"""Offline, leak-safe OpenDota kills research dataset and training entrypoint.

This is deliberately an experiment harness.  It never contacts OpenDota, changes
the input SQLite database, publishes a model, or starts a runtime service.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import heapq
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time

import joblib
import numpy as np
import pandas as pd
from catboost import CatBoostClassifier
from scipy.optimize import linear_sum_assignment
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score


BOUNDARIES = (300, 600, 900, 1200, 1500, 1800)
WINDOWS = ((300, 900), (600, 1200), (900, 1500), (1200, 1800))
WINDOW_TARGETS = ("lead_5_15", "lead_10_20", "lead_15_25", "lead_20_30")
TARGETS = WINDOW_TARGETS + ("side30", "total55")
DATE_SPLITS = ("2026-06-01", "2026-08-01", "2026-08-10", "2026-08-20", "2026-09-01")
# OpenDota's raw kills_log counts Lone Druid bear events as kills.  The matching
# player deaths are hero-key matched, so deaths (but not individual raw kills)
# are safe in the primary timeline block.
TIMELINE_METRICS = ("deaths", "gold", "xp", "lh", "dn")
PRO_METRICS = ("kills", "deaths", "assists", "gold_per_min", "xp_per_min")
ARMS = ("baseline", "timelines", "experience", "combined", "combined_dpxp")
PRIMARY_ARMS = ARMS[:-1]
SEED = 20260921


def epoch(date: str) -> int:
    return int(datetime.fromisoformat(date).replace(tzinfo=timezone.utc).timestamp())


SPLIT_CUTS = tuple(map(epoch, DATE_SPLITS))


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", dir=path.parent, delete=False, encoding="utf-8") as handle:
        json.dump(value, handle, sort_keys=True, indent=2, allow_nan=False)
        temp = Path(handle.name)
    os.replace(temp, path)


def atomic_npz(path: Path, **arrays) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp.npz")
    np.savez_compressed(temp, **arrays)
    os.replace(temp, path)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def fixed_splits(starts, ends, series_ids) -> np.ndarray:
    """Fixed partitions, with strict end-before-next-boundary and series purge."""
    starts, ends, series_ids = map(np.asarray, (starts, ends, series_ids))
    split = np.full(len(starts), -1, np.int8)
    for number, (lo, hi) in enumerate(zip(SPLIT_CUTS[:-1], SPLIT_CUTS[1:])):
        split[(starts >= lo) & (ends < hi)] = number
    split[starts >= SPLIT_CUTS[-1]] = 4
    # A known series that appears on both sides of any boundary is unusable in
    # every partition.  Unknown series IDs are intentionally independent maps.
    periods = np.searchsorted(SPLIT_CUTS[1:], starts, side="right")
    ranges: dict[int, list[int]] = {}
    for sid, period in zip(series_ids, periods):
        if int(sid) > 0:
            present = ranges.setdefault(int(sid), [int(period), int(period)])
            present[0] = min(present[0], int(period)); present[1] = max(present[1], int(period))
    crossing = {sid for sid, (lo, hi) in ranges.items() if lo != hi}
    if crossing:
        split[np.isin(series_ids, list(crossing))] = -1
    return split


def lead_labels(duration: int, hero_kills: np.ndarray) -> np.ndarray:
    """Two oriented strict-lead labels; ties, short and absent windows are NaN."""
    out = np.full((4, 2), np.nan, dtype=np.float32)
    for i, (start, end) in enumerate(WINDOWS):
        if duration < end or not np.isfinite(hero_kills[:, (BOUNDARIES.index(start), BOUNDARIES.index(end))]).all():
            continue
        radiant = float(hero_kills[0, BOUNDARIES.index(end)] - hero_kills[0, BOUNDARIES.index(start)])
        dire = float(hero_kills[1, BOUNDARIES.index(end)] - hero_kills[1, BOUNDARIES.index(start)])
        if radiant != dire:
            out[i] = (float(radiant > dire), float(dire > radiant))
    return out


@dataclass
class Moment:
    sums: np.ndarray
    counts: np.ndarray

    @classmethod
    def empty(cls, width: int):
        return cls(np.zeros(width, dtype=np.float64), np.zeros(width, dtype=np.int32))

    def add(self, values) -> None:
        values = np.asarray(values, dtype=float)
        finite = np.isfinite(values)
        self.sums[finite] += values[finite]
        self.counts[finite] += 1

    def mean(self) -> np.ndarray:
        return np.divide(self.sums, self.counts, out=np.full(len(self.sums), np.nan), where=self.counts > 0)


@dataclass
class Experience:
    games: int = 0
    last_end: int = 0
    roles: np.ndarray = field(default_factory=lambda: np.zeros(5, dtype=np.int32))

    def add(self, end: int, role: int | None = None) -> None:
        self.games += 1; self.last_end = max(self.last_end, int(end))
        if role is not None and 0 <= role < 5:
            self.roles[role] += 1


class History:
    """Only ``apply`` mutates state, and callers apply it after strict end < start."""
    def __init__(self):
        self.account_timeline: dict[int, Moment] = {}
        self.account_hero_timeline: dict[tuple[int, int], Moment] = {}
        self.account_pro: dict[int, Moment] = {}
        self.team_window_hero_kills: dict[int, Moment] = {}
        self.player: dict[int, Experience] = {}
        self.player_hero: dict[tuple[int, int], Experience] = {}
        self.player_position: dict[tuple[int, int], Experience] = {}
        self.last_dpxp: dict[tuple[int, int], float] = {}
        self.source_counts = {"db": 0, "rich": 0}

    def apply(self, event: dict) -> None:
        end = int(event["end"])
        raw_players = event["players"]
        # DB query records retain [radiant, dire] nesting for orientation;
        # standalone rich source events are already flat.
        players = raw_players if not raw_players or isinstance(raw_players[0], dict) else [p for side in raw_players for p in side]
        for player in players:
            account, hero = int(player["account"]), int(player["hero"])
            if account <= 0 or hero <= 0:
                continue
            timeline = np.asarray(player.get("timeline", (np.nan,) * (len(WINDOWS) * len(TIMELINE_METRICS))), dtype=float)
            if event.get("timeline_ok", False) and len(timeline) == len(WINDOWS) * len(TIMELINE_METRICS):
                self.account_timeline.setdefault(account, Moment.empty(len(timeline))).add(timeline)
                self.account_hero_timeline.setdefault((account, hero), Moment.empty(len(timeline))).add(timeline)
            pro = player.get("pro")
            if pro is not None and len(pro) == len(PRO_METRICS):
                self.account_pro.setdefault(account, Moment.empty(len(PRO_METRICS))).add(pro)
            role = player.get("role")
            self.player.setdefault(account, Experience()).add(end, role)
            self.player_hero.setdefault((account, hero), Experience()).add(end, role)
            if role is not None:
                self.player_position.setdefault((account, int(role)), Experience()).add(end, role)
            dpxp = player.get("dpxp")
            if dpxp is not None and np.isfinite(dpxp):
                self.last_dpxp[(account, hero)] = float(dpxp)
        hero_windows = event.get("team_hero_windows")
        if hero_windows is not None:
            for team, values in zip(event.get("teams", (0, 0)), hero_windows):
                if int(team) > 0:
                    self.team_window_hero_kills.setdefault(int(team), Moment.empty(len(WINDOWS))).add(values)
        self.source_counts[event["source"]] += 1

    def role_assignment(self, players: list[dict]) -> list[int]:
        # The query has no current role.  Hungarian assignment uses only past
        # account role frequencies; deterministic player ordering breaks ties.
        scores = np.zeros((len(players), 5), dtype=float)
        for row, player in enumerate(players):
            exp = self.player.get(int(player["account"]))
            if exp is not None:
                scores[row] = exp.roles
        rows, cols = linear_sum_assignment(-scores)
        assigned = np.zeros(len(players), dtype=np.int8)
        assigned[rows] = cols
        return assigned.tolist()

    @staticmethod
    def _shrunk(account: Moment | None, pair: Moment | None, width: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        account_mean = account.mean() if account else np.full(width, np.nan)
        pair_mean = pair.mean() if pair else np.full(width, np.nan)
        nc = account.counts.astype(float) if account else np.zeros(width)
        pc = pair.counts.astype(float) if pair else np.zeros(width)
        # Pair gets five pseudo-observations from the account history; absent
        # metrics remain absent, never silently converted to zero.
        value = np.where(pc > 0, (pc * pair_mean + 5.0 * account_mean) / (pc + 5.0), account_mean)
        return value, nc, pc

    @staticmethod
    def _mean_rows(rows, width: int) -> np.ndarray:
        values = np.asarray(rows, dtype=float)
        finite = np.isfinite(values)
        return np.divide(np.where(finite, values, 0).sum(axis=0), finite.sum(axis=0),
                         out=np.full(width, np.nan), where=finite.sum(axis=0) > 0)

    def features(self, sides: list[list[dict]], teams: tuple[int, int], start: int) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        pro_sides, timeline_sides, exp_sides, dpxp_sides = [], [], [], []
        for side_index, players in enumerate(sides):
            players = sorted(players, key=lambda p: (int(p["hero"]), int(p["account"])))
            roles = self.role_assignment(players)
            pro_rows, timeline_rows, exp_rows, dpxp_rows = [], [], [], []
            for player, role in zip(players, roles):
                account, hero = int(player["account"]), int(player["hero"])
                pro = self.account_pro.get(account)
                pro_rows.append(pro.mean() if pro else np.full(len(PRO_METRICS), np.nan))
                value, account_n, pair_n = self._shrunk(
                    self.account_timeline.get(account), self.account_hero_timeline.get((account, hero)),
                    len(WINDOWS) * len(TIMELINE_METRICS))
                timeline_rows.append(np.r_[value, account_n, pair_n])
                personal = self.player.get(account, Experience())
                hero_exp = self.player_hero.get((account, hero), Experience())
                pos_exp = self.player_position.get((account, role), Experience())
                recency = float(start - personal.last_end) if personal.last_end else np.nan
                hero_recency = float(start - hero_exp.last_end) if hero_exp.last_end else np.nan
                exp_rows.append((personal.games, hero_exp.games, pos_exp.games, personal.roles[role], recency, hero_recency))
                dpxp_rows.append(self.last_dpxp.get((account, hero), np.nan))
            pro_sides.append(self._mean_rows(pro_rows, len(PRO_METRICS)))
            player_timeline = self._mean_rows(timeline_rows, len(WINDOWS) * len(TIMELINE_METRICS) * 3)
            team_window = self.team_window_hero_kills.get(int(teams[side_index]))
            team_values = team_window.mean() if team_window else np.full(len(WINDOWS), np.nan)
            timeline_sides.append(np.r_[player_timeline, team_values])
            exp_sides.append(self._mean_rows(exp_rows, 6))
            dpxp_sides.append(float(np.nanmean(np.asarray(dpxp_rows, dtype=float))) if np.isfinite(dpxp_rows).any() else np.nan)
        # Side feature contains own level and an explicit own-minus-opponent
        # contrast. It can be oriented without using current outcomes.
        return tuple(np.asarray(x, dtype=float) for x in (pro_sides, timeline_sides, exp_sides, dpxp_sides))


def _timeline_values(row: sqlite3.Row) -> np.ndarray:
    values = []
    for start, end in WINDOWS:
        for metric in TIMELINE_METRICS:
            left, right = row[f"{metric}_{start}"], row[f"{metric}_{end}"]
            values.append(np.nan if left is None or right is None else float(right) - float(left))
    return np.asarray(values, dtype=float)


def _db_maps(db_path: Path) -> tuple[list[dict], dict]:
    # The completed artifact is deliberately a frozen WAL-mode snapshot without
    # a writable sibling -wal file.  SQLite immutable mode avoids attempting WAL
    # recovery/journal creation while preserving read-only source semantics.
    conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    sql = """
      SELECT mw.*, m.status, m.radiant_score AS final_radiant_score,
             m.dire_score AS final_dire_score
      FROM match_windows mw JOIN matches m ON m.match_id=mw.match_id
      WHERE m.status='complete'
      ORDER BY mw.start_time, mw.match_id
    """
    matches = [dict(row) for row in conn.execute(sql)]
    player_rows = defaultdict(list)
    for row in conn.execute("SELECT * FROM player_windows ORDER BY match_id, row_index"):
        player_rows[int(row["match_id"])].append(row)
    conn.close()
    records, audit = [], {"complete_maps": len(matches), "short_or_bad_roster": 0, "timeline_sources": 0,
                          "score_mismatch_maps": 0, "total55_score_flip_maps": 0}
    for match in matches:
        players = player_rows.get(int(match["match_id"]), [])
        by_side = [[row for row in players if row["side"] == side] for side in ("radiant", "dire")]
        if any(len(side) != 5 for side in by_side):
            audit["short_or_bad_roster"] += 1; continue
        record_players = []
        final_kills = []
        boundary_deaths = np.full((2, len(BOUNDARIES)), np.nan, dtype=float)
        for side in by_side:
            final_kills.append(sum(float(row["kills"]) for row in side) if all(row["kills"] is not None for row in side) else np.nan)
            record_players.append([{"account": row["account_id"] or 0, "hero": row["hero_id"] or 0,
                                    "timeline": _timeline_values(row),
                                    "pro": [row[name] for name in PRO_METRICS], "role": None, "dpxp": None}
                                   for row in side])
        for side_index, side in enumerate(by_side):
            for boundary_index, boundary in enumerate(BOUNDARIES):
                vals = [row[f"deaths_{boundary}"] for row in side]
                if all(value is not None for value in vals):
                    boundary_deaths[side_index, boundary_index] = sum(float(value) for value in vals)
        # A side's credited hero kills equal the five opponents' hero deaths.
        hero_kills = boundary_deaths[::-1]
        team_hero_windows = np.full((2, len(WINDOWS)), np.nan, dtype=float)
        for side_index in (0, 1):
            for window_index, (start, end) in enumerate(WINDOWS):
                a, b = BOUNDARIES.index(start), BOUNDARIES.index(end)
                if np.isfinite(hero_kills[side_index, (a, b)]).all():
                    team_hero_windows[side_index, window_index] = hero_kills[side_index, b] - hero_kills[side_index, a]
        score_total = (match["final_radiant_score"] or 0) + (match["final_dire_score"] or 0)
        final_total = float(np.nansum(final_kills)) if np.isfinite(final_kills).all() else np.nan
        if np.isfinite(final_total) and score_total != final_total:
            audit["score_mismatch_maps"] += 1
            audit["total55_score_flip_maps"] += int((score_total >= 55) != (final_total >= 55))
        timeline_ok = bool(match["status"] == "complete")
        audit["timeline_sources"] += int(timeline_ok)
        record = {"mid": int(match["match_id"]), "start": int(match["start_time"]),
                  "end": int(match["start_time"] + match["duration"]), "duration": int(match["duration"]),
                  "series": int(match["series_id"] or 0), "players": record_players, "counts": match,
                  "final_kills": final_kills, "hero_kills": hero_kills, "team_hero_windows": team_hero_windows,
                  "teams": (int(match["radiant_team_id"] or 0), int(match["dire_team_id"] or 0)), "timeline_ok": timeline_ok}
        records.append(record)
    return records, audit


def _rich_events(rich_path: Path, wanted_accounts: np.ndarray, db_by_mid: dict[int, dict]) -> list[dict]:
    rich = np.load(rich_path, allow_pickle=False)
    required = ("mids", "ts", "durations", "sids", "heroes", "accounts", "pstats", "pstat_names")
    missing = set(required) - set(rich.files)
    if missing:
        raise ValueError(f"rich corpus misses {sorted(missing)}")
    # NPZ members are compressed independently.  Read each required member once:
    # indexing ``rich['pstats']`` in the loop would decompress ~700MB per row.
    mids, timestamps, durations, sids = (rich[name] for name in ("mids", "ts", "durations", "sids"))
    heroes, accounts, pstats = (rich[name] for name in ("heroes", "accounts", "pstats"))
    names = [str(x) for x in rich["pstat_names"].tolist()]
    if "dotaPlusHeroXp" not in names:
        raise ValueError("rich pstat_names misses exact dotaPlusHeroXp")
    dpxp_column = names.index("dotaPlusHeroXp")
    picked = np.flatnonzero(np.any(np.isin(accounts, wanted_accounts), axis=1))
    max_query_start = max(record["start"] for record in db_by_mid.values())
    picked = picked[timestamps[picked] <= max_query_start]
    # Keep only the scalar DotaPlus column and selected source rows before
    # event materialization; the full pstats member is roughly 700MB.
    selected_dpxp = np.asarray(pstats[picked, :, dpxp_column], dtype=np.float32).copy()
    selected_mids, selected_ts, selected_durations, selected_sids = (np.asarray(value[picked]).copy() for value in (mids, timestamps, durations, sids))
    selected_accounts, selected_heroes = np.asarray(accounts[picked]).copy(), np.asarray(heroes[picked]).copy()
    del pstats, mids, timestamps, durations, sids, accounts, heroes
    wanted_set = set(map(int, wanted_accounts.tolist()))
    events = []; seen_non_db_mids: set[int] = set()
    for count in range(len(picked)):
        mid = int(selected_mids[count]); start = int(selected_ts[count]); duration = int(selected_durations[count])
        if duration <= 0:
            continue
        if mid in seen_non_db_mids:
            raise ValueError(f"rich corpus has duplicate match_id {mid}")
        seen_non_db_mids.add(mid)
        players = []
        for slot in range(10):
            account = int(selected_accounts[count, slot])
            if account not in wanted_set:
                continue
            players.append({"account": account, "hero": int(selected_heroes[count, slot]),
                            "role": slot % 5, "dpxp": float(selected_dpxp[count, slot])})
        if mid in db_by_mid:
            # Same map is one historical observation.  OpenDota boundaries live
            # in DB; rich supplies only role ordering and historical DotaPlus.
            for db_side, slots in zip(db_by_mid[mid]["players"], (players[:5], players[5:])):
                by_identity = {(p["account"], p["hero"]): p for p in slots}
                for db_player in db_side:
                    extra = by_identity.get((int(db_player["account"]), int(db_player["hero"])))
                    if extra:
                        db_player["role"], db_player["dpxp"] = extra["role"], extra["dpxp"]
            continue
        events.append({"mid": mid, "start": start, "end": start + duration, "duration": duration,
                       "series": int(selected_sids[count] or 0), "players": players,
                       "timeline_ok": False, "source": "rich"})
        if count and count % 100000 == 0:
            print(f"rich filtered source rows={count}", flush=True)
    return events


def _side_vector(heroes, own, other) -> np.ndarray:
    return np.r_[np.asarray(heroes, dtype=np.float32), own, own - other].astype(np.float32)


def build_dataset(db_path: Path, rich_path: Path, output_dir: Path) -> dict:
    """Build all causal arms.  Source state changes only after ``end < start``."""
    records, audit = _db_maps(db_path)
    if not records:
        raise ValueError("no completed DB maps")
    db_by_mid = {record["mid"]: record for record in records}
    wanted = np.unique(np.asarray([p["account"] for r in records for side in r["players"] for p in side if p["account"] > 0], dtype=np.int64))
    rich_events = _rich_events(rich_path, wanted, db_by_mid)
    source_events = rich_events + [{**record, "source": "db"} for record in records]
    source_events.sort(key=lambda event: (event["start"], event["mid"], event["source"]))
    records.sort(key=lambda record: (record["start"], record["mid"]))
    history, pending, event_at = History(), [], 0
    base, timelines, experience, dpxp, labels = [], [], [], [], []
    starts, ends, mids, sids = [], [], [], []
    max_history_end = 0
    for query_index, record in enumerate(records):
        start = record["start"]
        while event_at < len(source_events) and source_events[event_at]["start"] <= start:
            event = source_events[event_at]
            # The exact current map is queued as a source but cannot be visible
            # to itself; all matching mids are excluded at feature time.
            heapq.heappush(pending, (event["end"], event["mid"], event_at, event)); event_at += 1
        while pending and pending[0][0] < start:
            end, mid, _, event = heapq.heappop(pending)
            if mid != record["mid"]:
                if end >= start:
                    raise AssertionError("history source is not strictly before query start")
                history.apply(event); max_history_end = max(max_history_end, int(end))
        pro, timeline, exp, dp = history.features(record["players"], record["teams"], start)
        sorted_heroes = [sorted((int(p["hero"]) for p in side)) for side in record["players"]]
        for side in (0, 1):
            other = 1 - side
            base.append(_side_vector(sorted_heroes[side] + sorted_heroes[other], pro[side], pro[other]))
            timelines.append(_side_vector([], timeline[side], timeline[other]))
            experience.append(_side_vector([], exp[side], exp[other]))
            dpxp.append(_side_vector([], np.asarray([dp[side]]), np.asarray([dp[other]])))
        win = lead_labels(record["duration"], record["hero_kills"])
        side = np.asarray(record["final_kills"], dtype=float)
        extra = np.full((2, 2), np.nan, dtype=np.float32)
        if np.isfinite(side).all():
            extra[0] = (side >= 30).astype(np.float32)
            extra[1] = float(side.sum() >= 55)
        labels.append(np.vstack((win, extra)))
        starts.append(record["start"]); ends.append(record["end"]); mids.append(record["mid"]); sids.append(record["series"])
        if query_index and query_index % 2000 == 0:
            print(f"DB query maps={query_index}/{len(records)} history_end={max_history_end}", flush=True)
    split = fixed_splits(np.asarray(starts), np.asarray(ends), np.asarray(sids))
    base_names = [f"hero_{i}" for i in range(10)] + [f"pro_{x}_{metric}" for x in ("own", "diff") for metric in PRO_METRICS]
    timeline_core = [f"timeline_{start}_{end}_{metric}" for start, end in WINDOWS for metric in TIMELINE_METRICS]
    timeline_support = [f"timeline_support_{kind}_{start}_{end}_{metric}" for kind in ("account", "account_hero") for start, end in WINDOWS for metric in TIMELINE_METRICS]
    team_window_names = [f"timeline_team_hero_kills_{start}_{end}" for start, end in WINDOWS]
    timeline_names = [f"{x}_{kind}" for kind in ("own", "diff") for x in timeline_core + timeline_support + team_window_names]
    exp_core = ("player_games", "player_hero_games", "player_position_games", "assigned_role_games", "player_recency_seconds", "player_hero_recency_seconds")
    exp_names = [f"experience_{kind}_{x}" for kind in ("own", "diff") for x in exp_core]
    dpxp_names = ("dpxp_own", "dpxp_diff")
    output_dir.mkdir(parents=True, exist_ok=True)
    npz_path = output_dir / "dataset.npz"
    atomic_npz(npz_path, X_baseline=np.asarray(base, dtype=np.float32).reshape(-1, 2, len(base_names)),
               X_timeline=np.asarray(timelines, dtype=np.float32).reshape(-1, 2, len(timeline_names)),
               X_experience=np.asarray(experience, dtype=np.float32).reshape(-1, 2, len(exp_names)),
               X_dpxp=np.asarray(dpxp, dtype=np.float32).reshape(-1, 2, 2), y=np.asarray(labels, dtype=np.float32),
               mids=np.asarray(mids), starts=np.asarray(starts), ends=np.asarray(ends), series_ids=np.asarray(sids), split=split)
    metadata = {"schema": "kills-opendota-research-v1", "baseline_kind": "new_pro_only_not_production_E281",
                "targets": list(TARGETS), "arms": list(ARMS),
                "primary_selection_arms": list(PRIMARY_ARMS), "split_dates": list(DATE_SPLITS),
                "features": {"baseline": base_names, "timeline": timeline_names, "experience": exp_names, "dpxp": list(dpxp_names)},
                "hero_order": "ascending hero_id within each side; own side followed by opponent", "role_policy": "query role assigned from prior rich slot-role frequencies only",
                "causality": "source event applied only when source.end < query.start; query mid excluded", "max_history_end_seen": int(max_history_end),
                "label_semantics": "lead windows strict credited hero-kill deltas from sum of five opponent deaths at boundaries, ties omitted; side30/total55 are sums of final player kills",
                "raw_kills_policy": "individual raw kills omitted from primary timeline because OpenDota kills_log includes nonhero bear events; deaths are hero-key matched",
                "scoreboard_comparison": audit, "source_hashes": {"timeline_sqlite3": sha256(db_path), "rich_npz": sha256(rich_path)},
                "source_counts": {"db_query_maps": len(records), "rich_filtered_non_db_maps": len(rich_events), "query_accounts": len(wanted)}}
    atomic_json(output_dir / "metadata.json", metadata)
    return metadata


def _metric(y, probability) -> dict:
    probability = np.clip(np.asarray(probability, dtype=float), 1e-6, 1 - 1e-6)
    y = np.asarray(y, dtype=np.int8)
    bins = []
    ece = 0.0
    for index in range(10):
        selected = np.minimum((probability * 10).astype(int), 9) == index
        count = int(selected.sum())
        if not count:
            continue
        mean_probability = float(probability[selected].mean())
        prevalence = float(y[selected].mean())
        ece += count / len(y) * abs(mean_probability - prevalence)
        bins.append({"lower": index / 10, "upper": (index + 1) / 10, "n": count,
                     "mean_probability": mean_probability, "prevalence": prevalence})
    return {"n": int(len(y)), "prevalence": float(np.mean(y)), "log_loss": float(log_loss(y, probability, labels=[0, 1])),
            "brier": float(brier_score_loss(y, probability)), "accuracy_at_0_5": float(np.mean((probability >= .5) == y)),
            "auc": float(roc_auc_score(y, probability)) if len(np.unique(y)) == 2 else None,
            "ece_10_equal_width": float(ece), "calibration_bins": bins}


def _cal_fit(probability, y):
    logits = np.log(np.clip(probability, 1e-6, 1-1e-6) / np.clip(1-probability, 1e-6, 1-1e-6))[:, None]
    return LogisticRegression(C=1.0, max_iter=1000).fit(logits, y)


def _cal_predict(model, probability):
    logits = np.log(np.clip(probability, 1e-6, 1-1e-6) / np.clip(1-probability, 1e-6, 1-1e-6))[:, None]
    return model.predict_proba(logits)[:, 1]


def _day_delta(y, candidate, baseline, days, draws=1000) -> dict:
    c, b = np.clip(candidate, 1e-6, 1-1e-6), np.clip(baseline, 1e-6, 1-1e-6)
    loss = -(y*np.log(c)+(1-y)*np.log(1-c)) + y*np.log(b)+(1-y)*np.log(1-b)
    unique, inverse = np.unique(days, return_inverse=True)
    sums, counts = np.bincount(inverse, weights=loss), np.bincount(inverse)
    rng = np.random.default_rng(SEED); samples = np.empty(draws)
    for i in range(draws):
        picks = rng.integers(0, len(unique), len(unique)); samples[i] = sums[picks].sum() / counts[picks].sum()
    return {"mean": float(loss.mean()), "day_cluster_95ci": np.quantile(samples, [.025, .975]).tolist(), "days": int(len(unique))}


def _target_rows(data, target: str, partition: int):
    ti = TARGETS.index(target); maps = np.flatnonzero(data["split"] == partition)
    valid = np.isfinite(data["y"][maps, ti])
    map_rows, sides = np.where(valid)
    map_rows = maps[map_rows]
    return map_rows, sides.astype(np.int8), data["y"][map_rows, ti, sides].astype(np.int8)


def _matrix(data, arm: str, maps, sides):
    blocks = [data["X_baseline"]]
    if arm in ("timelines", "combined", "combined_dpxp"): blocks.append(data["X_timeline"])
    if arm in ("experience", "combined", "combined_dpxp"): blocks.append(data["X_experience"])
    if arm == "combined_dpxp": blocks.append(data["X_dpxp"])
    values = np.concatenate([block[maps, sides] for block in blocks], axis=1)
    # CatBoost rejects float-valued categorical columns.  Keep the ten draft
    # hero IDs categorical while preserving NaN for numerical history features.
    frame = pd.DataFrame(values)
    for column in range(10):
        frame[column] = frame[column].fillna(0).astype(np.int64)
    return frame


def _prediction_for_target(model, calibrator, data, arm: str, target: str, maps, sides):
    """Calibrate after averaging the two orientations for the symmetric total."""
    raw = model.predict_proba(_matrix(data, arm, maps, sides))[:, 1]
    if target != "total55":
        return _cal_predict(calibrator, raw), maps, data["y"][maps, TARGETS.index(target), sides].astype(np.int8)
    unique = np.unique(maps)
    both_maps = np.repeat(unique, 2)
    both_sides = np.tile(np.arange(2, dtype=np.int8), len(unique))
    raw = model.predict_proba(_matrix(data, arm, both_maps, both_sides))[:, 1].reshape(-1, 2).mean(axis=1)
    return _cal_predict(calibrator, raw), unique, data["y"][unique, TARGETS.index(target), 0].astype(np.int8)


def train_target(dataset: Path, metadata_path: Path, target: str, output_dir: Path, threads: int) -> dict:
    if target not in TARGETS:
        raise ValueError(f"unknown target {target}")
    data = np.load(dataset, allow_pickle=False)
    metadata = json.loads(metadata_path.read_text())
    output_dir.mkdir(parents=True, exist_ok=True)
    if any((output_dir / name).exists() for name in ("model.cbm", "selection.json", "metrics.json")):
        raise ValueError("target output already contains a completed candidate")
    rows = {part: _target_rows(data, target, part) for part in range(5)}
    if any(len(rows[part][2]) == 0 for part in range(5)):
        raise ValueError(f"target {target} has an empty fixed partition")
    began = time.monotonic()
    models, calibration, selection, predictions = {}, {}, {}, {}
    for arm in ARMS:
        train_m, train_s, train_y = rows[0]; stop_m, stop_s, stop_y = rows[1]
        X_train = _matrix(data, arm, train_m, train_s); X_stop = _matrix(data, arm, stop_m, stop_s)
        model = CatBoostClassifier(iterations=400, depth=5, learning_rate=.05, l2_leaf_reg=12,
            loss_function="Logloss", random_seed=SEED, thread_count=threads, verbose=False,
            allow_writing_files=False, early_stopping_rounds=40)
        model.fit(X_train, train_y, cat_features=list(range(10)), eval_set=(X_stop, stop_y))
        cal_m, cal_s, _ = rows[2]
        # Total55 is one map truth: average its two draft orientations before
        # fitting/calibrating/evaluating it.
        raw_cal = model.predict_proba(_matrix(data, arm, cal_m, cal_s))[:, 1]
        if target == "total55":
            unique_cal = np.unique(cal_m)
            raw_cal = model.predict_proba(_matrix(data, arm, np.repeat(unique_cal, 2), np.tile(np.arange(2), len(unique_cal))))[:, 1].reshape(-1, 2).mean(1)
            cal_y = data["y"][unique_cal, TARGETS.index(target), 0].astype(np.int8)
        else:
            cal_y = data["y"][cal_m, TARGETS.index(target), cal_s].astype(np.int8)
        cal = _cal_fit(raw_cal, cal_y)
        sel_m, sel_s, _ = rows[3]
        select_prob, _, sel_y = _prediction_for_target(model, cal, data, arm, target, sel_m, sel_s)
        selection[arm] = {"trees": int(model.tree_count_), "selection": _metric(sel_y, select_prob)}
        models[arm], calibration[arm] = model, cal
        print(f"{target} arm={arm} trees={model.tree_count_} selection_logloss={selection[arm]['selection']['log_loss']:.6f} elapsed={time.monotonic()-began:.1f}s", flush=True)
    chosen = min(PRIMARY_ARMS, key=lambda arm: selection[arm]["selection"]["log_loss"])
    # This frozen record exists before any terminal partition prediction.
    atomic_json(output_dir / "selection.json", {"chosen": chosen, "rule": "minimum calibrated selection logloss among baseline,timelines,experience,combined", "arms": selection})
    terminal_m, terminal_s, _ = rows[4]
    for arm in ARMS:
        predictions[arm], _, _ = _prediction_for_target(models[arm], calibration[arm], data, arm, target, terminal_m, terminal_s)
    _, terminal_maps, terminal_y = _prediction_for_target(models[chosen], calibration[chosen], data, chosen, target, terminal_m, terminal_s)
    metrics = {"target": target, "chosen": chosen, "selection": selection,
               "terminal": {arm: _metric(terminal_y, pred) for arm, pred in predictions.items()},
               "chosen_minus_baseline": _day_delta(terminal_y, predictions[chosen], predictions["baseline"], data["starts"][terminal_maps] // 86400),
               "partitions": {str(part): int(len(rows[part][2])) for part in range(5)}}
    feature_names = list(metadata["features"]["baseline"])
    if chosen in ("timelines", "combined", "combined_dpxp"): feature_names += list(metadata["features"]["timeline"])
    if chosen in ("experience", "combined", "combined_dpxp"): feature_names += list(metadata["features"]["experience"])
    if chosen == "combined_dpxp": feature_names += list(metadata["features"]["dpxp"])
    schema = {"schema": metadata["schema"], "target": target, "chosen_arm": chosen,
              "feature_names": feature_names, "cat_features": list(range(10)), "total55_rule": "average orientations at consumer evaluation"}
    def save_candidate(model, cal, model_path: Path, calibration_path: Path) -> None:
        model_tmp = model_path.with_name(model_path.name + ".tmp")
        model.save_model(str(model_tmp)); os.replace(model_tmp, model_path)
        calibration_tmp = calibration_path.with_name(calibration_path.name + ".tmp")
        joblib.dump(cal, calibration_tmp); os.replace(calibration_tmp, calibration_path)

    candidates = output_dir / "candidates"; candidates.mkdir()
    for arm in ARMS:
        save_candidate(models[arm], calibration[arm], candidates / f"{arm}.cbm", candidates / f"{arm}.joblib")
    # Canonical paths are the selected artifact consumed by a later serving
    # review; candidate paths preserve all frozen terminal predictions.
    save_candidate(models[chosen], calibration[chosen], output_dir / "model.cbm", output_dir / "calibration.joblib")
    atomic_json(output_dir / "schema.json", schema)
    atomic_json(output_dir / "metrics.json", metrics)
    saved_sides = terminal_s if target != "total55" else np.full(len(terminal_maps), -1, dtype=np.int8)
    atomic_npz(output_dir / "predictions.npz", mids=data["mids"][terminal_maps], sides=saved_sides, y=terminal_y,
               **{arm: prediction for arm, prediction in predictions.items()})
    for arm in ARMS:
        replay = CatBoostClassifier(); replay.load_model(str(candidates / f"{arm}.cbm"))
        replay_prediction, _, _ = _prediction_for_target(replay, joblib.load(candidates / f"{arm}.joblib"), data, arm, target, terminal_m, terminal_s)
        if not np.allclose(replay_prediction, predictions[arm], rtol=0, atol=1e-12):
            raise AssertionError(f"saved {arm} model replay prediction mismatch")
    manifest = {"dataset_sha256": sha256(dataset), "metadata_sha256": sha256(metadata_path), "target": target,
                "model_replay_atol": 1e-12, "source_hashes": metadata["source_hashes"],
                "artifact_sha256": {name: sha256(output_dir / name) for name in ("model.cbm", "calibration.joblib", "schema.json", "selection.json", "metrics.json", "predictions.npz")},
                "candidate_sha256": {arm: {"model": sha256(candidates / f"{arm}.cbm"), "calibration": sha256(candidates / f"{arm}.joblib")} for arm in ARMS},
                "code_sha256": sha256(Path(__file__))}
    atomic_json(output_dir / "manifest.json", manifest)
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("build"); build.add_argument("--db", required=True, type=Path); build.add_argument("--rich", required=True, type=Path); build.add_argument("--output-dir", required=True, type=Path)
    train = sub.add_parser("train"); train.add_argument("--dataset", required=True, type=Path); train.add_argument("--metadata", required=True, type=Path); train.add_argument("--target", required=True, choices=TARGETS); train.add_argument("--output-dir", required=True, type=Path); train.add_argument("--threads", required=True, type=int)
    args = parser.parse_args()
    if args.command == "build":
        print(json.dumps(build_dataset(args.db, args.rich, args.output_dir), sort_keys=True))
    else:
        print(json.dumps(train_target(args.dataset, args.metadata, args.target, args.output_dir, args.threads), sort_keys=True))


if __name__ == "__main__":
    main()
