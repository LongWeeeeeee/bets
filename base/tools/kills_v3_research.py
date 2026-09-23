"""Causal, offline prematch kills dataset from STRATZ and OpenDota.

All match outcomes enter history only after their end is strictly before the
query start. This module does not train or serve models.
"""
from __future__ import annotations

import argparse
from collections import defaultdict, deque
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import time
import warnings

import numpy as np


ROOT = Path(__file__).resolve().parents[2]
RICH = ROOT / "runtime/artifacts/misc/pro_corpus_rich.npz"
OLD_DB = ROOT / "runtime/artifacts/kills/opendota_windows_20260919/run/timeline.sqlite3"
NEW_DB = ROOT / "runtime/artifacts/kills/opendota_windows_20260923/run/timeline.sqlite3"
OUT = ROOT / "data/kills_v3_20260923"
HISTORY_START = 1656633600  # 2022-07-01 UTC
QUERY_START = 1672531200  # 2023-01-01 UTC
VALID_START = 1784505600  # 2026-07-20 UTC
TEST_START = 1787184000  # 2026-08-20 UTC
DAY = 86400
KILL_SUM_TOLERANCE = 2
BOUNDARIES = (5, 10, 15, 20, 25, 30)
WINDOWS = ((5, 15), (10, 20), (15, 25), (20, 30))
LABELS = ("own_kills", "opp_kills", "total", "lead_5_15", "lead_10_20", "lead_15_25", "lead_20_30", "duration")
TEAM_METRICS = ("kills_for", "kills_against", "kpm_for", "kpm_against", "duration",
                "window_5_15_for", "window_10_20_for", "window_15_25_for", "window_20_30_for",
                "window_5_15_against", "window_10_20_against", "window_15_25_against", "window_20_30_against", "win_rate")
PLAYER_METRICS = ("kills", "deaths", "assists", "kills_pm", "deaths_pm", "assists_pm",
                  "kill_participation", "death_share", "gpm", "xpm", "networth_pm",
                  "last_hits_pm", "denies_pm", "hero_damage_pm", "tower_damage_pm", "healing_pm",
                  "obs_placed", "sen_placed", "stuns", "teamfight_participation", "camps_stacked")
EXTRA_START = PLAYER_METRICS.index("obs_placed")
HERO_METRICS = ("team_kpm", "team_deaths_pm")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _columnar_rich(path: Path, history_start: int = HISTORY_START) -> dict:
    with np.load(path, allow_pickle=False) as rich:
        expected = ("kills", "deaths", "assists", "goldPerMinute", "experiencePerMinute", "networth",
                    "numLastHits", "numDenies", "heroDamage", "towerDamage", "heroHealing")
        assert tuple(rich["pstat_names"][:len(expected)]) == expected, "unexpected STRATZ pstat order"
        selected = np.flatnonzero(rich["ts"] >= history_start)
        keys = ("mids", "ts", "durations", "sids", "stypes", "leagues", "teams", "wins", "heroes", "accounts", "pstats", "rk", "dk")
        data = {name: np.asarray(rich[name][selected]).copy() for name in keys}
    return data


def _player_metrics(raw: np.ndarray, duration: np.ndarray, final: np.ndarray) -> np.ndarray:
    """STRATZ pstats -> common metrics; zero K/D/A/GPM/XPM row is absent."""
    n = len(raw)
    out = np.full((n, 10, len(PLAYER_METRICS)), np.nan, np.float32)
    minutes = np.maximum(duration[:, None] / 60.0, 1.0)
    valid = np.any(raw[:, :, :5] != 0, axis=2)
    out[:, :, :3] = raw[:, :, :3]
    out[:, :, 3:6] = raw[:, :, :3] / minutes[:, :, None]
    team_kills = np.repeat(final, 5, axis=1)
    team_deaths = np.repeat(final[:, ::-1], 5, axis=1)
    out[:, :, 6] = np.divide(raw[:, :, 0] + raw[:, :, 2], team_kills,
                             out=np.full((n, 10), np.nan, np.float32), where=team_kills > 0)
    out[:, :, 7] = np.divide(raw[:, :, 1], team_deaths,
                             out=np.full((n, 10), np.nan, np.float32), where=team_deaths > 0)
    out[:, :, 8:10] = raw[:, :, 3:5]
    out[:, :, 10] = raw[:, :, 5] / minutes
    out[:, :, 11:13] = raw[:, :, 6:8] / minutes[:, :, None]
    out[:, :, 13:16] = raw[:, :, 8:11] / minutes[:, :, None]
    out[~valid] = np.nan
    return out


def _db_records(paths: list[Path], history_start: int = HISTORY_START) -> tuple[dict[int, dict], dict]:
    records: dict[int, dict] = {}
    audit = {"db_paths_missing": [], "db_duplicate_mids": 0, "db_incomplete_roster": 0,
             "db_score_vs_player_kills_disagreements": 0, "db_score_vs_player_kills_examples": []}
    for path in paths:
        if not path.exists():
            warnings.warn(f"OpenDota DB missing, skipped: {path}", stacklevel=2)
            audit["db_paths_missing"].append(str(path))
            continue
        conn = sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)
        conn.row_factory = sqlite3.Row
        matches = conn.execute("SELECT w.*, m.status FROM match_windows w JOIN matches m ON m.match_id=w.match_id WHERE m.status='complete'").fetchall()
        players = defaultdict(list)
        for row in conn.execute("SELECT * FROM player_windows ORDER BY match_id, row_index"):
            players[int(row["match_id"])].append(row)
        conn.close()
        for row in matches:
            mid = int(row["match_id"])
            if mid in records:
                audit["db_duplicate_mids"] += 1
                continue
            slots = sorted(players[mid], key=lambda p: (0 if p["side"] == "radiant" else 1, int(p["player_slot"])))
            if len(slots) != 10 or sum(p["side"] == "radiant" for p in slots) != 5:
                audit["db_incomplete_roster"] += 1
                continue
            dur = int(row["duration"] or 0)
            if dur <= 0 or int(row["start_time"] or 0) < history_start:
                continue
            final = np.array([sum(float(p["kills"]) for p in slots[s:s + 5]) if all(p["kills"] is not None for p in slots[s:s + 5]) else np.nan for s in (0, 5)], np.float32)
            if not np.isfinite(final).all():
                audit["db_incomplete_roster"] += 1
                continue
            scoreboard = np.asarray([row["radiant_score"], row["dire_score"]], np.float32)
            if np.isfinite(scoreboard).all() and np.any(scoreboard != final):
                audit["db_score_vs_player_kills_disagreements"] += 1
                if len(audit["db_score_vs_player_kills_examples"]) < 5:
                    audit["db_score_vs_player_kills_examples"].append(
                        {"mid": mid, "score": scoreboard.astype(int).tolist(), "player_kills": final.astype(int).tolist()})
            pm = np.full((10, len(PLAYER_METRICS)), np.nan, np.float32)
            mins = dur / 60.0
            for j, p in enumerate(slots):
                values = [p["kills"], p["deaths"], p["assists"]]
                if all(v is not None for v in values):
                    pm[j, :3] = values
                    pm[j, 3:6] = np.asarray(values, np.float32) / mins
                own, opp = final[j // 5], final[1 - j // 5]
                if own > 0 and p["kills"] is not None and p["assists"] is not None:
                    pm[j, 6] = (p["kills"] + p["assists"]) / own
                if opp > 0 and p["deaths"] is not None:
                    pm[j, 7] = p["deaths"] / opp
                for col, key in ((8, "gold_per_min"), (9, "xp_per_min"), (16, "obs_placed"),
                                 (17, "sen_placed"), (18, "stuns"), (19, "teamfight_participation"),
                                 (20, "camps_stacked")):
                    if p[key] is not None:
                        pm[j, col] = p[key]
                for col, key in ((10, "net_worth"), (13, "hero_damage"), (14, "tower_damage")):
                    if p[key] is not None:
                        pm[j, col] = p[key] / mins
                # Full-map LH/DN and healing are not stored in this DB.
            # Team kills are inferred from opponent hero deaths at each boundary.
            timeline = np.full((2, len(BOUNDARIES)), np.nan, np.float32)
            for b, minute in enumerate(BOUNDARIES):
                if dur < minute * 60:
                    continue
                for side in (0, 1):
                    opp = slots[(1 - side) * 5:(2 - side) * 5]
                    vals = [p[f"deaths_{minute * 60}"] for p in opp]
                    if all(v is not None for v in vals):
                        timeline[side, b] = sum(vals)
            records[mid] = {"mid": mid, "start": int(row["start_time"]), "duration": dur,
                            "series": int(row["series_id"] or 0), "stype": int(row["series_type"] or 0),
                            "league": int(row["leagueid"] or 0),
                            "teams": [int(row["radiant_team_id"] or 0), int(row["dire_team_id"] or 0)],
                            "win": int(row["radiant_win"] or 0), "final": final,
                            "heroes": [int(p["hero_id"] or 0) for p in slots],
                            "accounts": [int(p["account_id"] or 0) for p in slots],
                            "pmetrics": pm, "timeline": timeline}
    return records, audit


def load_events(rich_path: Path, db_paths: list[Path],
                history_start: int = HISTORY_START) -> tuple[dict, dict]:
    rich = _columnar_rich(rich_path, history_start)
    db, audit = _db_records(db_paths, history_start)
    n = len(rich["mids"])
    mids = rich["mids"]
    index = {int(mid): i for i, mid in enumerate(mids)}
    raw = rich.pop("pstats")
    missing = np.all(raw[:, :, :5] == 0, axis=2)
    final = np.stack((np.where(missing[:, :5], 0, raw[:, :5, 0]).sum(axis=1),
                      np.where(missing[:, 5:], 0, raw[:, 5:, 0]).sum(axis=1)), axis=1).astype(np.float32)
    final[np.all(missing[:, :5], axis=1), 0] = np.nan
    final[np.all(missing[:, 5:], axis=1), 1] = np.nan
    pmetrics = _player_metrics(raw, rich["durations"], final)
    del raw
    timeline = np.stack((rich.pop("rk")[:, list(BOUNDARIES)], rich.pop("dk")[:, list(BOUNDARIES)]), axis=1).astype(np.float32)
    # STRATZ writes all-zero series when per-minute data is absent.
    timeline[(timeline == 0).all(axis=(1, 2))] = np.nan
    for b, minute in enumerate(BOUNDARIES):
        timeline[rich["durations"] < minute * 60, :, b] = np.nan
    source = np.zeros(n + sum(mid not in index for mid in db), np.int8)
    arrays = {"mid": mids, "start": rich["ts"], "duration": rich["durations"], "series": rich["sids"],
              "stype": rich["stypes"], "league": rich["leagues"], "teams": rich["teams"], "win": rich["wins"],
              "heroes": rich["heroes"], "accounts": rich["accounts"], "final": final,
              "pmetrics": pmetrics, "timeline": timeline}
    extras = {key: [] for key in arrays}
    diffs = []
    audit["overlap_team_id_disagreements"] = 0
    audit["overlap_team_id_examples"] = []
    audit["overlap_missing_stratz_kills"] = 0
    audit["overlap_player_stat_cells_filled_from_stratz"] = 0
    for mid, rec in db.items():
        i = index.get(mid)
        if i is None:
            for key in arrays:
                extras[key].append(rec[key])
            continue
        if int(arrays["start"][i]) != rec["start"] or int(arrays["win"][i]) != rec["win"]:
            raise ValueError(f"STRATZ/OpenDota time or outcome conflict for match {mid}")
        for side in (0, 1):
            slots = slice(side * 5, (side + 1) * 5)
            rich_players = sorted(zip(map(int, arrays["accounts"][i, slots]), map(int, arrays["heroes"][i, slots])))
            db_players = sorted(zip(map(int, rec["accounts"][slots]), map(int, rec["heroes"][slots])))
            if rich_players != db_players:
                raise ValueError(f"STRATZ/OpenDota player identity conflict for match {mid}, side {side}")
        rich_slot = {(int(a), int(h)): slot for slot, (a, h) in enumerate(zip(arrays["accounts"][i], arrays["heroes"][i]))}
        for slot, (account, hero) in enumerate(zip(rec["accounts"], rec["heroes"])):
            values = rec["pmetrics"][slot]
            fallback = pmetrics[i, rich_slot[(int(account), int(hero))]]
            fill = ~np.isfinite(values) & np.isfinite(fallback)
            audit["overlap_player_stat_cells_filled_from_stratz"] += int(fill.sum())
            values[fill] = fallback[fill]
        if not np.array_equal(arrays["teams"][i], rec["teams"]):
            audit["overlap_team_id_disagreements"] += 1
            if len(audit["overlap_team_id_examples"]) < 5:
                audit["overlap_team_id_examples"].append(
                    {"mid": mid, "stratz": arrays["teams"][i].tolist(), "opendota": rec["teams"]})
        if np.isfinite(final[i]).all():
            difference = np.abs(final[i] - rec["final"])
            if np.any(difference > KILL_SUM_TOLERANCE):
                raise ValueError(f"STRATZ/OpenDota player-kill sum conflict >{KILL_SUM_TOLERANCE} for match {mid}: {difference.tolist()}")
            diffs.append(difference)
        else:
            audit["overlap_missing_stratz_kills"] += 1
        for key in arrays:
            arrays[key][i] = rec[key]
        source[i] = 1
    for key in arrays:
        if extras[key]:
            arrays[key] = np.concatenate((arrays[key], np.asarray(extras[key], dtype=arrays[key].dtype)), axis=0)
    source[n:] = 1
    order = np.lexsort((arrays["mid"], arrays["start"]))
    arrays = {key: arr[order] for key, arr in arrays.items()}
    arrays["source"] = source[order]
    arrays["end"] = arrays["start"] + arrays["duration"]
    diff = np.asarray(diffs, np.float32).reshape(-1, 2)
    audit.update({"rich_events": n, "db_events": len(db), "overlap": len(db) - len(extras["mid"]),
                  "overlap_side_disagreements_gt0": int(np.any(diff > 0, axis=1).sum()),
                  "overlap_side_disagreements_gt2": int(np.any(diff > KILL_SUM_TOLERANCE, axis=1).sum()),
                  "overlap_max_side_disagreement": float(np.nanmax(diff)) if len(diff) else 0.0,
                  "overlap_within_tolerance_2": int(np.all(diff <= KILL_SUM_TOLERANCE, axis=1).sum())})
    assert len(np.unique(arrays["mid"])) == len(arrays["mid"]), "duplicate match_id in event union"
    return arrays, audit


class Decayed:
    __slots__ = ("sums", "counts", "last_time", "last_event")

    def __init__(self, width: int):
        self.sums = np.zeros(width, np.float64)
        self.counts = np.zeros(width, np.float64)
        self.last_time = 0
        self.last_event = 0

    def at(self, when: int, half_life_days: float) -> tuple[np.ndarray, np.ndarray]:
        if self.last_time:
            weight = 2.0 ** (-(when - self.last_time) / (half_life_days * DAY))
            return self.sums * weight, self.counts * weight
        return self.sums, self.counts

    def add(self, values: np.ndarray, when: int, half_life_days: float) -> None:
        sums, counts = self.at(when, half_life_days)
        valid = np.isfinite(values)
        self.sums = sums + np.where(valid, values, 0)
        self.counts = counts + valid
        self.last_time = when
        self.last_event = when


def shrunk(state: Decayed | None, population: Decayed, when: int, half_life_days: float, pseudo_games: float) -> tuple[np.ndarray, np.ndarray]:
    """Per-metric (sum + k*population_mean)/(effective_n+k)."""
    ps, pc = population.at(when, half_life_days)
    prior = np.divide(ps, pc, out=np.full_like(ps, np.nan), where=pc > 0)
    if state is None:
        return prior, np.zeros(len(prior), np.float64)
    sums, counts = state.at(when, half_life_days)
    result = np.divide(sums + pseudo_games * prior, counts + pseudo_games,
                       out=np.full_like(sums, np.nan), where=np.isfinite(prior))
    # When no population prior exists, the observed entity mean is still valid.
    np.divide(sums, counts, out=result, where=(counts > 0) & ~np.isfinite(prior))
    return result, counts


def _team_values(final: np.ndarray, duration: int, windows: np.ndarray, win: int) -> np.ndarray:
    out = np.full((2, len(TEAM_METRICS)), np.nan, np.float64)
    for side in (0, 1):
        own, opp = final[side], final[1 - side]
        out[side, :5] = (own, opp, own * 60 / duration, opp * 60 / duration, duration)
        out[side, 5:9] = windows[side]
        out[side, 9:13] = windows[1 - side]
        out[side, 13] = win if side == 0 else 1 - win
    return out


def _window_values(timeline: np.ndarray, duration: int) -> np.ndarray:
    out = np.full((2, len(WINDOWS)), np.nan, np.float64)
    for j, (a, b) in enumerate(WINDOWS):
        if duration >= b * 60:
            ai, bi = BOUNDARIES.index(a), BOUNDARIES.index(b)
            if np.isfinite(timeline[:, [ai, bi]]).all():
                out[:, j] = timeline[:, bi] - timeline[:, ai]
    return out


def labels_for(final: np.ndarray, timeline: np.ndarray, duration: int) -> np.ndarray:
    windows = _window_values(timeline, duration)
    out = np.full((2, len(LABELS)), np.nan, np.float32)
    for side in (0, 1):
        out[side, :3] = (final[side], final[1 - side], sum(final))
        out[side, 3:7] = windows[side] - windows[1 - side]
        out[side, 7] = duration
    return out


def feature_layout() -> tuple[list[str], dict[str, list[str]]]:
    blocks: dict[str, list[str]] = {}
    blocks["G"] = ["global_league28_total", "global_league28_kpm", "global_league28_duration", "series_type", "series_game_number"]
    paired_team = list(TEAM_METRICS) + ["effective_n", "days_since_last", "poisson_expected_kills", "elo_rating"]
    blocks["T"] = [f"team_{side}_{metric}" for side in ("own", "opp") for metric in paired_team]
    blocks["T"] += [f"team_diff_{metric}" for metric in ("kills_for", "kills_against", "kpm_for", "win_rate", "poisson_expected_kills", "elo_rating")]
    player_side = [f"{metric}_{agg}" for metric in PLAYER_METRICS for agg in ("sum", "mean")]
    player_side += ["kills_pm_max", "effective_n_mean", "days_since_last_mean", "players_known"]
    player_side += [f"{metric}_effective_n_mean" for metric in PLAYER_METRICS[EXTRA_START:]]
    player_side += ["poisson_kills_sum"]
    blocks["P"] = [f"player_{side}_{metric}" for side in ("own", "opp") for metric in player_side]
    blocks["P"] += [f"player_diff_{metric}" for metric in ("kills_pm_mean", "deaths_pm_mean", "kill_participation_mean", "gpm_mean", "xpm_mean", "poisson_kills_sum")]
    blocks["H"] = [name for side in ("own", "opp") for name in
                   ([f"hero_{side}_{i}" for i in range(5)] +
                    [f"hero_{side}_{metric}_sum" for metric in HERO_METRICS])]
    return [name for names in blocks.values() for name in names], blocks


class History:
    def __init__(self, half_life_days: float = 90, pseudo_games: float = 5, poisson_lr: float = 0.03, elo_k: float = 20):
        self.half_life_days, self.pseudo_games = half_life_days, pseudo_games
        self.poisson_lr, self.elo_k = poisson_lr, elo_k
        self.team: dict[int, Decayed] = {}
        self.player: dict[int, Decayed] = {}
        self.hero: dict[int, Decayed] = {}
        self.team_pop = Decayed(len(TEAM_METRICS))
        self.player_pop = Decayed(len(PLAYER_METRICS))
        self.hero_pop = Decayed(len(HERO_METRICS))
        self.global_total = Decayed(1)
        self.leagues: dict[int, deque] = defaultdict(deque)
        self.league_stats: dict[int, Decayed] = defaultdict(lambda: Decayed(3))
        self.attack = defaultdict(float)
        self.defense = defaultdict(float)
        self.elo = defaultdict(lambda: 1500.0)
        self.player_attack = defaultdict(float)

    def _expected(self, a: int, b: int, when: int) -> tuple[float, float]:
        base, _ = shrunk(None, self.global_total, when, self.half_life_days, self.pseudo_games)
        base_kills = float(base[0] / 2) if np.isfinite(base[0]) else 25.0
        return (base_kills * math.exp(max(-2, min(2, self.attack[a] + self.defense[b]))),
                base_kills * math.exp(max(-2, min(2, self.attack[b] + self.defense[a]))))

    def apply(self, events: dict, i: int) -> None:
        when = int(events["end"][i])
        final = events["final"][i]
        duration = int(events["duration"][i])
        if duration <= 0 or not np.isfinite(final).all():
            return
        teams = events["teams"][i]
        windows = _window_values(events["timeline"][i], duration)
        tv = _team_values(final, duration, windows, int(events["win"][i]))
        predicted = self._expected(int(teams[0]), int(teams[1]), when)
        for side in (0, 1):
            tid = int(teams[side])
            self.team_pop.add(tv[side], when, self.half_life_days)
            if tid != 0:
                self.team.setdefault(tid, Decayed(len(TEAM_METRICS))).add(tv[side], when, self.half_life_days)
        a, b = map(int, teams)
        if a != 0 and b != 0:
            for side, own, opp in ((0, a, b), (1, b, a)):
                residual = max(-1.5, min(1.5, (final[side] - predicted[side]) / max(predicted[side], 1)))
                self.attack[own] += self.poisson_lr * residual
                self.defense[opp] += self.poisson_lr * residual
            expected_win = 1 / (1 + 10 ** ((self.elo[b] - self.elo[a]) / 400))
            delta = self.elo_k * (int(events["win"][i]) - expected_win)
            self.elo[a] += delta
            self.elo[b] -= delta
        self.global_total.add(np.array([sum(final)]), when, self.half_life_days)
        league = int(events["league"][i])
        league_values = np.array([float(sum(final)), float(sum(final) * 60 / duration), float(duration)])
        self.leagues[league].append((when, league_values))
        self.league_stats[league].add(league_values, when, self.half_life_days)
        for j in range(10):
            p = int(events["accounts"][i, j])
            h = int(events["heroes"][i, j])
            val = events["pmetrics"][i, j].astype(np.float64)
            if p != 0 and np.isfinite(val[:5]).any():
                self.player.setdefault(p, Decayed(len(PLAYER_METRICS))).add(val, when, self.half_life_days)
                self.player_pop.add(val, when, self.half_life_days)
                if np.isfinite(val[0]):
                    self.player_attack[p] += self.poisson_lr * max(-1.5, min(1.5, (val[0] - 5) / 5))
            if h > 0:
                side = j // 5
                hv = np.array([final[side] * 60 / duration, final[1 - side] * 60 / duration])
                self.hero.setdefault(h, Decayed(2)).add(hv, when, self.half_life_days)
                self.hero_pop.add(hv, when, self.half_life_days)

    def features(self, events: dict, i: int, series_number: int) -> np.ndarray:
        when = int(events["start"][i])
        league = int(events["league"][i])
        q = self.leagues[league]
        stats = self.league_stats[league]
        sums, counts = stats.at(when, self.half_life_days)
        while q and q[0][0] <= when - 28 * DAY:
            event_end, values = q.popleft()
            weight = 2.0 ** (-(when - event_end) / (self.half_life_days * DAY))
            sums -= values * weight
            counts -= weight
        if not q:
            sums[:] = 0
            counts[:] = 0
        stats.sums, stats.counts, stats.last_time = sums, counts, when
        recent = np.divide(sums, counts, out=np.full(3, np.nan), where=counts > 1e-9)
        g = np.r_[recent, float(events["stype"][i]), float(series_number)]
        teams = [int(x) for x in events["teams"][i]]
        expected = self._expected(*teams, when)
        team_sides = []
        for side, tid in enumerate(teams):
            if tid == 0:
                team_sides.append(np.full(len(TEAM_METRICS) + 4, np.nan))
                continue
            state = self.team.get(tid)
            mean, count = shrunk(state, self.team_pop, when, self.half_life_days, self.pseudo_games)
            effective = float(count[0])
            recency = (when - state.last_event) / DAY if state is not None else np.nan
            team_sides.append(np.r_[mean, effective, recency, expected[side], self.elo[tid]])
        player_sides = []
        hero_sides = []
        for side in (0, 1):
            values = []
            supports = []
            recencies = []
            extras = []
            ratings = []
            hero_tempo = []
            for j in range(side * 5, (side + 1) * 5):
                p, h = int(events["accounts"][i, j]), int(events["heroes"][i, j])
                if p != 0:
                    state = self.player.get(p)
                    mean, count = shrunk(state, self.player_pop, when, self.half_life_days, self.pseudo_games)
                    mean[EXTRA_START:] = np.where(count[EXTRA_START:] > 0, mean[EXTRA_START:], np.nan)
                    values.append(mean)
                    supports.append(float(count[0]))
                    recencies.append((when - state.last_event) / DAY if state is not None else np.nan)
                    extras.append(count[EXTRA_START:])
                    ratings.append(math.exp(max(-2, min(2, self.player_attack[p]))) * 5)
                if h > 0:
                    hm, _ = shrunk(self.hero.get(h), self.hero_pop, when, self.half_life_days, self.pseudo_games)
                    hero_tempo.append(hm)
            a = np.asarray(values).reshape(-1, len(PLAYER_METRICS)) if values else np.empty((0, len(PLAYER_METRICS)))
            valid = np.isfinite(a)
            totals = np.divide(np.where(valid, a, 0).sum(axis=0), valid.sum(axis=0),
                               out=np.full(len(PLAYER_METRICS), np.nan), where=valid.sum(axis=0) > 0)
            sums = np.where(valid.sum(axis=0) > 0, np.where(valid, a, 0).sum(axis=0), np.nan)
            paired = np.stack((sums, totals), axis=1).ravel()
            killmax = float(np.nanmax(a[:, 3])) if a.size and np.isfinite(a[:, 3]).any() else np.nan
            support = float(np.mean(supports)) if supports else np.nan
            recent = float(np.mean([x for x in recencies if np.isfinite(x)])) if any(np.isfinite(recencies)) else np.nan
            extra_n = np.mean(extras, axis=0) if extras else np.full(len(PLAYER_METRICS) - EXTRA_START, np.nan)
            player_sides.append(np.r_[paired, killmax, support, recent, len(values), extra_n, sum(ratings) if ratings else np.nan])
            hv = np.asarray(hero_tempo, np.float64).reshape(-1, 2)
            hero_sum = np.where(np.isfinite(hv).any(axis=0), np.nansum(hv, axis=0), np.nan) if len(hv) else (np.nan, np.nan)
            hero_sides.append(np.r_[sorted(map(int, events["heroes"][i, side * 5:(side + 1) * 5])), hero_sum])
        return g, np.asarray(team_sides), np.asarray(player_sides), np.asarray(hero_sides)


def _orient_features(parts: tuple, names: list[str], blocks: dict) -> np.ndarray:
    g, team, player, hero = parts
    result = np.empty((2, len(names)), np.float32)
    for side in (0, 1):
        other = 1 - side
        team_diff = team[side, [0, 1, 2, 13, -2, -1]] - team[other, [0, 1, 2, 13, -2, -1]]
        player_diff_idx = [PLAYER_METRICS.index(x) * 2 + 1 for x in
                           ("kills_pm", "deaths_pm", "kill_participation", "gpm", "xpm")]
        player_diff = np.r_[player[side, player_diff_idx] - player[other, player_diff_idx], player[side, -1] - player[other, -1]]
        result[side] = np.r_[g, team[side], team[other], team_diff,
                             player[side], player[other], player_diff, hero[side], hero[other]]
    assert result.shape[1] == len(names)
    return result


def corrected_global_meta(events: dict, half_life_days: float, query_start: int = QUERY_START) -> np.ndarray:
    """Independent lightweight replay of the three 28-day league features.

    Useful to validate a completed matrix without repeating player histories.
    Rows follow the same query order as ``build_dataset``.
    """
    query = np.flatnonzero(events["start"] >= int(query_start))
    result = np.full((len(query), 3), np.nan, np.float32)
    order = np.argsort(events["end"], kind="stable")
    at_end = 0
    queues: dict[int, deque] = defaultdict(deque)
    states: dict[int, Decayed] = defaultdict(lambda: Decayed(3))
    for qi, i in enumerate(query):
        start = int(events["start"][i])
        while at_end < len(order) and events["end"][order[at_end]] < start:
            j = int(order[at_end])
            at_end += 1
            duration = int(events["duration"][j])
            final = events["final"][j]
            if duration <= 0 or not np.isfinite(final).all():
                continue
            league = int(events["league"][j])
            when = int(events["end"][j])
            total = float(sum(final))
            values = np.array([total, total * 60 / duration, float(duration)])
            queues[league].append((when, values))
            states[league].add(values, when, half_life_days)
        league = int(events["league"][i])
        state = states[league]
        sums, counts = state.at(start, half_life_days)
        q = queues[league]
        while q and q[0][0] <= start - 28 * DAY:
            when, values = q.popleft()
            weight = 2.0 ** (-(start - when) / (half_life_days * DAY))
            sums -= values * weight
            counts -= weight
        if not q:
            sums[:] = 0
            counts[:] = 0
        state.sums, state.counts, state.last_time = sums, counts, start
        result[qi] = np.divide(sums, counts, out=np.full(3, np.nan), where=counts > 1e-9)
    return result


def split_with_purge(starts: np.ndarray, series: np.ndarray) -> tuple[np.ndarray, dict]:
    split = np.where(starts < VALID_START, 0, np.where(starts < TEST_START, 1, 2)).astype(np.int8)
    later = {}
    for sid, s in zip(series, split):
        if sid != 0:
            later[int(sid)] = max(later.get(int(sid), -1), int(s))
    purge = np.array([sid != 0 and later[int(sid)] > s for sid, s in zip(series, split)], bool)
    before = {name: int(np.sum(split == j)) for j, name in enumerate(("train_pool", "valid", "test"))}
    split[purge] = -1
    return split, {"before": before, "purged_train_pool": int(np.sum(purge & (starts < VALID_START))),
                   "purged_valid": int(np.sum(purge & (starts >= VALID_START) & (starts < TEST_START)))}


def build_dataset(rich_path: Path, db_paths: list[Path], output_dir: Path,
                  half_life_days: float = 90, pseudo_games: float = 5, poisson_lr: float = 0.03,
                  cpu_pause_seconds: float = 0, history_cutoff: int | None = None,
                  visibility_delay: int = 0, history_start: int = HISTORY_START,
                  query_start: int = QUERY_START) -> dict:
    """history_cutoff freezes history like the production snapshot: an event is
    applied only if end < min(query start - visibility_delay, cutoff).
    visibility_delay models serving lag (a finished map reaches the feed late).
    Labels and stored ends are unaffected."""
    started = time.monotonic()
    rich_path = rich_path.resolve()
    db_paths = [path.resolve() for path in db_paths]
    if half_life_days <= 0 or pseudo_games < 0 or poisson_lr < 0 or cpu_pause_seconds < 0 or visibility_delay < 0:
        raise ValueError("half_life_days>0, pseudo_games>=0, poisson_lr>=0, cpu_pause_seconds>=0 and visibility_delay>=0 required")
    if history_start < 0 or query_start < history_start:
        raise ValueError("UTC epoch starts require 0 <= history_start <= query_start")
    source_hashes_at_start = {str(path): sha256(path) for path in [rich_path] + db_paths if path.exists()}
    events, audit = load_events(rich_path, db_paths, history_start)
    used_paths = [rich_path] + [path for path in db_paths if str(path) not in audit["db_paths_missing"]]
    if any(str(path) not in source_hashes_at_start for path in used_paths):
        raise RuntimeError("an OpenDota DB appeared during source loading; retry on a stable snapshot")
    query = np.flatnonzero(events["start"] >= query_start)
    names, blocks = feature_layout()
    X = np.empty((len(query), 2, len(names)), np.float32)
    y = np.empty((len(query), 2, len(LABELS)), np.float32)
    history = History(half_life_days, pseudo_games, poisson_lr)
    end_order = np.argsort(events["end"], kind="stable")
    at_end = 0
    series_started = defaultdict(int)
    at_start = 0
    for qi, i in enumerate(query):
        start = int(events["start"][i])
        visible = start - int(visibility_delay)
        if history_cutoff is not None:
            visible = min(visible, int(history_cutoff))
        while at_end < len(end_order) and events["end"][end_order[at_end]] < visible:
            history.apply(events, int(end_order[at_end]))
            at_end += 1
        while at_start < len(events["start"]) and events["start"][at_start] < start:
            sid = int(events["series"][at_start])
            if sid != 0:
                series_started[sid] += 1
            at_start += 1
        sid = int(events["series"][i])
        parts = history.features(events, int(i), series_started[sid] + 1 if sid != 0 else 0)
        X[qi] = _orient_features(parts, names, blocks)
        y[qi] = labels_for(events["final"][i], events["timeline"][i], int(events["duration"][i]))
        if cpu_pause_seconds and qi % 100 == 0:
            time.sleep(cpu_pause_seconds)
        if qi and qi % 25000 == 0:
            print(f"query={qi}/{len(query)} history={at_end} elapsed={time.monotonic()-started:.1f}s", flush=True)
    starts = events["start"][query]
    split, purge_audit = split_with_purge(starts, events["series"][query])
    for path in used_paths:
        if sha256(path) != source_hashes_at_start[str(path)]:
            raise RuntimeError(f"source changed during dataset build: {path}")
    output_dir.mkdir(parents=True, exist_ok=True)
    tmp = output_dir / "dataset.npz.tmp"
    with tmp.open("wb") as fh:
        np.savez_compressed(fh, X=X, y=y, mids=events["mid"][query], orient=np.tile(np.arange(2, dtype=np.int8), (len(query), 1)),
                            starts=starts, ends=events["end"][query], series_ids=events["series"][query],
                            team_ids=np.asarray(events["teams"][query], np.int64),
                            split=split, source=events["source"][query], feature_names=np.asarray(names), label_names=np.asarray(LABELS))
    tmp.replace(output_dir / "dataset.npz")
    rates = {}
    for j, name in enumerate(("train_pool", "valid", "test")):
        mask = split == j
        lead = y[mask, :, 3:7]
        rates[name] = {"maps": int(mask.sum()), "rows": int(mask.sum() * 2),
                       "p_own_ge30": float(np.mean(y[mask, :, 0][np.isfinite(y[mask, :, 0])] >= 30)) if mask.any() and np.isfinite(y[mask, :, 0]).any() else None,
                       "p_total_ge55": float(np.mean(y[mask, :, 2][np.isfinite(y[mask, :, 2])] >= 55)) if mask.any() and np.isfinite(y[mask, :, 2]).any() else None,
                       "lead_positive": [float(np.mean(lead[:, :, k][np.isfinite(lead[:, :, k])] > 0)) if np.isfinite(lead[:, :, k]).any() else None for k in range(4)] if mask.any() else None,
                       "lead_zero": [float(np.mean(lead[:, :, k][np.isfinite(lead[:, :, k])] == 0)) if np.isfinite(lead[:, :, k]).any() else None for k in range(4)] if mask.any() else None,
                       "lead_negative": [float(np.mean(lead[:, :, k][np.isfinite(lead[:, :, k])] < 0)) if np.isfinite(lead[:, :, k]).any() else None for k in range(4)] if mask.any() else None,
                       "lead_observed": [int(np.isfinite(lead[:, :, k]).sum()) for k in range(4)] if mask.any() else None}
    feature_idx = {name: i for i, name in enumerate(names)}
    test = split == 2
    valid = split == 1
    corr_x = X[valid, :, feature_idx["team_own_poisson_expected_kills"]].ravel()
    corr_y = y[valid, :, 0].ravel()
    corr_ok = np.isfinite(corr_x) & np.isfinite(corr_y)
    sanity = {"test_team_kills_for_coverage_pct": float(np.isfinite(X[test, :, feature_idx["team_own_kills_for"]]).mean() * 100) if test.any() else None,
              "test_player_kills_mean_coverage_pct": float(np.isfinite(X[test, :, feature_idx["player_own_kills_mean"]]).mean() * 100) if test.any() else None,
              "test_T_block_finite_pct": float(np.isfinite(X[test, :, len(blocks["G"]):len(blocks["G"]) + len(blocks["T"])]).mean() * 100) if test.any() else None,
              "test_P_block_finite_pct": float(np.isfinite(X[test, :, len(blocks["G"]) + len(blocks["T"]):len(blocks["G"]) + len(blocks["T"]) + len(blocks["P"])]).mean() * 100) if test.any() else None,
              "test_T_observed_history_pct": float(np.mean(X[test, :, feature_idx["team_own_effective_n"]] > 0) * 100) if test.any() else None,
              "test_P_observed_history_pct": float(np.mean(X[test, :, feature_idx["player_own_effective_n_mean"]] > 0) * 100) if test.any() else None,
              "valid_team_expected_own_kills_corr": float(np.corrcoef(corr_x[corr_ok], corr_y[corr_ok])[0, 1]) if corr_ok.sum() > 2 else None}
    metadata = {"schema": "kills-v3-causal-v1", "feature_names": names, "blocks": blocks, "label_names": list(LABELS),
                "parameters": {"half_life_days": half_life_days, "pseudo_games": pseudo_games,
                               "poisson_lr": poisson_lr, "elo_k": history.elo_k,
                               "history_start": int(history_start), "query_start": int(query_start),
                               "cpu_pause_seconds": cpu_pause_seconds,
                               "history_cutoff": None if history_cutoff is None else int(history_cutoff),
                               "visibility_delay": int(visibility_delay),
                               "overlap_kill_sum_tolerance_per_side": KILL_SUM_TOLERANCE},
                "causality": "history event applied only if end < query start - visibility_delay (and < history_cutoff when set); series game number counts starts < query start",
                "source_priority": "OpenDota overrides STRATZ on duplicate match_id; final labels sum five player kills per side",
                "source_flags": {"0": "STRATZ", "1": "OpenDota"},
                "source_hashes": {str(p): source_hashes_at_start[str(p)] for p in used_paths},
                "source_audit": audit, "split_audit": purge_audit,
                "source_counts": {"events_total": len(events["mid"]), "events_stratz": int((events["source"] == 0).sum()),
                                  "events_opendota": int((events["source"] == 1).sum()),
                                  "query_maps": len(query)},
                "source_by_split": {name: {"stratz": int(np.sum((split == j) & (events["source"][query] == 0))),
                                           "opendota": int(np.sum((split == j) & (events["source"][query] == 1)))}
                                    for j, name in enumerate(("train_pool", "valid", "test"))},
                "base_rates": rates, "sanity": sanity, "wall_seconds": time.monotonic() - started}
    meta_tmp = output_dir / "metadata.json.tmp"
    meta_tmp.write_text(json.dumps(metadata, ensure_ascii=False, indent=2, allow_nan=False))
    meta_tmp.replace(output_dir / "metadata.json")
    return metadata


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rich", type=Path, default=RICH)
    parser.add_argument("--db", type=Path, action="append", default=None, help="repeat for each OpenDota SQLite DB")
    parser.add_argument("--output", type=Path, default=OUT)
    parser.add_argument("--half-life-days", type=float, default=90)
    parser.add_argument("--pseudo-games", type=float, default=5)
    parser.add_argument("--poisson-lr", type=float, default=0.03)
    parser.add_argument("--cpu-pause-seconds", type=float, default=0,
                        help="optional cooperative pause every 100 query maps")
    parser.add_argument("--history-cutoff", type=int, default=None,
                        help="UTC epoch seconds; freeze history at end < cutoff (production-snapshot replay)")
    parser.add_argument("--visibility-delay", type=int, default=0,
                        help="seconds; history event visible only if end < start - delay (serving lag)")
    parser.add_argument("--history-start", type=int, default=HISTORY_START,
                        help="UTC epoch seconds; earliest history event")
    parser.add_argument("--query-start", type=int, default=QUERY_START,
                        help="UTC epoch seconds; earliest map with output features")
    args = parser.parse_args()
    meta = build_dataset(args.rich, args.db if args.db is not None else [OLD_DB, NEW_DB], args.output,
                         args.half_life_days, args.pseudo_games, args.poisson_lr, args.cpu_pause_seconds,
                         args.history_cutoff, args.visibility_delay,
                         args.history_start, args.query_start)
    print(json.dumps({k: meta[k] for k in ("source_counts", "split_audit", "base_rates", "sanity", "wall_seconds")}, indent=2))


if __name__ == "__main__":
    main()
