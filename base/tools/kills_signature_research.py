"""Offline player-hero wins and individual performance ablation for kills."""
from __future__ import annotations

import argparse
from collections import Counter, deque
from dataclasses import dataclass, field
import json
from pathlib import Path
import sqlite3
import sys

import numpy as np

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from base.tools import kills_opendota_research as core

ARMS = ("baseline", "experience", "recent_experience", "signature",
        "signature_individual", "signature_performance")
COMPARISONS = (("signature_minus_recent", "signature", "recent_experience"),
               ("individual_minus_signature", "signature_individual", "signature"),
               ("performance_minus_individual", "signature_performance", "signature_individual"),
               ("full_minus_recent", "signature_performance", "recent_experience"))
core.ARM_BLOCKS.update({
    "signature": ("baseline", "experience", "recent", "signature"),
    "signature_individual": ("baseline", "experience", "recent", "individual"),
    "signature_performance": ("baseline", "experience", "recent", "individual", "performance"),
})
WIN_NAMES = tuple(f"{period}_{name}" for period in ("all", "90d") for name in
                  ("player_n", "hero_n", "pair_n", "pair_wins", "player_wr",
                   "hero_wr", "pair_wr", "pair_minus_player", "pair_minus_hero"))
PRACTICE_NAMES = ("role", "player_games", "pair_games", "position_games",
                  "player_recency", "pair_recency") + tuple(
    f"{days}d_{name}" for days in core.RECENT_DAYS for name in core.RECENT_METRICS)
INDIVIDUAL_NAMES = PRACTICE_NAMES + WIN_NAMES
PERFORMANCE_NAMES = tuple(f"{family}_{stat}_{name}"
    for family, names in (("final", core.PRO_METRICS),
                          ("window", tuple(f"{metric}_{a}_{b}" for a, b in core.WINDOWS for metric in core.TIMELINE_METRICS)))
    for stat in ("pair_shrunk", "pair_delta", "pair_n") for name in names)


@dataclass
class Wins:
    games: int = 0
    wins: int = 0
    recent: deque = field(default_factory=deque)
    recent_wins: int = 0

    def trim(self, time: int) -> None:
        while self.recent and self.recent[0][0] < time - 90 * 86400:
            self.recent_wins -= self.recent.popleft()[1]

    def add(self, end: int, won: int) -> None:
        if won not in (0, 1) or (self.recent and end < self.recent[-1][0]):
            raise ValueError("invalid outcome or nonchronological wins")
        self.games += 1
        self.wins += won
        self.recent.append((end, won))
        self.recent_wins += won
        self.trim(end)

    def counts(self, start: int, recent: bool) -> tuple[int, int]:
        if self.recent and self.recent[-1][0] >= start:
            raise ValueError("future outcome in query history")
        self.trim(start)
        return (len(self.recent), self.recent_wins) if recent else (self.games, self.wins)


def win_values(player: Wins, hero: Wins, pair: Wins, start: int) -> list[float]:
    result = []
    for recent in (False, True):
        pn, pw = player.counts(start, recent)
        hn, hw = hero.counts(start, recent)
        n, w = pair.counts(start, recent)
        # Fixed Beta(5,5) priors for marginal rates; ten player-rate pseudo-games
        # for the pair. Counts distinguish a prior from actual observations.
        pr, hr = (pw + 5) / (pn + 10), (hw + 5) / (hn + 10)
        rate = (w + 10 * pr) / (n + 10)
        result.extend((pn, hn, n, w, pr, hr, rate, rate - pr, rate - hr))
    return result


class SignatureHistory(core.History):
    def __init__(self):
        super().__init__()
        self.player_wins, self.hero_wins, self.pair_wins = {}, {}, {}
        self.pair_pro = {}

    def apply(self, event: dict) -> None:
        super().apply(event)
        for p in event["players"]:
            a, h = p["account"], p["hero"]
            if a <= 0 or h <= 0:
                continue
            won = p.get("won")
            if won is not None:
                for mapping, key in ((self.player_wins, a), (self.hero_wins, h), (self.pair_wins, (a, h))):
                    mapping.setdefault(key, Wins()).add(event["end"], won)
            if p.get("pro") is not None:
                self.pair_pro.setdefault((a, h), core.Moment.empty(len(core.PRO_METRICS))).add(p["pro"])

    def rows(self, sides: list, start: int) -> tuple[np.ndarray, np.ndarray]:
        individual, performance = [], []
        for players in sides:
            players = sorted(players, key=lambda p: (p["hero"], p["account"]))
            side, perf = [], []
            for p, role in zip(players, self.role_assignment(players)):
                a, h = p["account"], p["hero"]
                player = self.player.get(a, core.Experience())
                pair = self.player_hero.get((a, h), core.Experience())
                position = self.player_position.get((a, role), core.Experience())
                values = [role, player.games, pair.games, position.games,
                          start - player.last_end if player.games else np.nan,
                          start - pair.last_end if pair.games else np.nan]
                for days in core.RECENT_DAYS:
                    n, hn, rn = [x.recent_games(start, days) for x in (player, pair, position)]
                    values.extend((n, hn, rn, hn / n if n else np.nan, rn / n if n else np.nan))
                values += win_values(self.player_wins.get(a, Wins()), self.hero_wins.get(h, Wins()),
                                     self.pair_wins.get((a, h), Wins()), start)
                side.append(values)
                features = []
                for general, specific, width in (
                    (self.account_pro.get(a), self.pair_pro.get((a, h)), len(core.PRO_METRICS)),
                    (self.account_timeline.get(a), self.account_hero_timeline.get((a, h)),
                     len(core.WINDOWS) * len(core.TIMELINE_METRICS))):
                    shrunk, _, count = self._shrunk(general, specific, width)
                    mean = general.mean() if general else np.full(width, np.nan)
                    features.extend(np.r_[shrunk, shrunk - mean, count])
                perf.append(features)
            individual.append(side)
            performance.append(perf)
        return np.asarray(individual), np.asarray(performance)


def source_events(db: Path, rich: Path, records: list) -> tuple[list, dict]:
    """Outcomes from rich; DB owns exact overlapping identity/time/outcome."""
    by_mid = {r["mid"]: r for r in records}
    wanted = {p["account"] for r in records for side in r["players"] for p in side if p["account"] > 0}
    with sqlite3.connect(f"file:{db.resolve()}?mode=ro&immutable=1", uri=True) as con:
        outcomes = dict(con.execute("SELECT match_id,radiant_win FROM matches"))
    events, audit = [], Counter()
    for r in records:
        won = outcomes[r["mid"]]
        if won not in (0, 1):
            raise ValueError(f"missing DB outcome {r['mid']}")
        players = [dict(p, won=int(won == (side == 0))) for side, ps in enumerate(r["players"]) for p in ps]
        events.append(dict(r, players=players, source="db"))
    by_event = {r["mid"]: r for r in events}
    with np.load(rich, allow_pickle=False) as data:
        mids, starts, durations, accounts, heroes, wins = [data[k] for k in
            ("mids", "ts", "durations", "accounts", "heroes", "wins")]
    if not np.isin(wins, (0, 1)).all():
        raise ValueError("rich wins is not binary")
    selected = np.flatnonzero(np.any(np.isin(accounts, list(wanted)), axis=1) &
                              (starts <= max(r["start"] for r in records)) & (durations > 0))
    seen = set()
    for i in selected:
        mid, start, end = int(mids[i]), int(starts[i]), int(starts[i] + durations[i])
        if mid in seen:
            raise ValueError(f"duplicate rich mid {mid}")
        seen.add(mid)
        rows = [dict(account=int(a), hero=int(h), role=slot % 5,
                     won=int(int(wins[i]) == (slot < 5)))
                for slot, (a, h) in enumerate(zip(accounts[i], heroes[i])) if a in wanted and a > 0 and h > 0]
        if len({p["account"] for p in rows}) != len(rows):
            raise ValueError(f"duplicate rich player {mid}")
        if mid in by_mid:
            event = by_event[mid]
            exact = {(p["account"], p["hero"], p["won"]): p for p in event["players"]}
            if start != event["start"] or end != event["end"] or int(wins[i]) != outcomes[mid]:
                audit["overlap_time_or_outcome_mismatch_excluded_rich"] += 1
                continue
            if any((p["account"], p["hero"], p["won"]) not in exact for p in rows):
                audit["overlap_identity_mismatch_excluded_rich"] += 1
                continue
            for p in rows:
                exact[(p["account"], p["hero"], p["won"])]["role"] = p["role"]
            audit["overlap_verified"] += 1
        else:
            events.append(dict(mid=mid, start=start, end=end, players=rows, source="rich", timeline_ok=False))
            audit["rich_maps"] += 1
    audit["db_maps"] = len(records)
    events.sort(key=lambda r: (r["end"], r["mid"]))
    return events, dict(audit)


def augment(dataset: Path, metadata: Path, db: Path, rich: Path, output: Path) -> dict:
    with np.load(dataset, allow_pickle=False) as old:
        arrays = {k: old[k] for k in old.files}
    records, _ = core._db_maps(db)
    records.sort(key=lambda r: (r["start"], r["mid"]))
    if not np.array_equal(arrays["mids"], [r["mid"] for r in records]):
        raise ValueError("query identities changed")
    events, source_audit = source_events(db, rich, records)
    history = SignatureHistory()
    at = 0
    width, pwidth = len(INDIVIDUAL_NAMES), len(PERFORMANCE_NAMES)
    sig = np.empty((len(records), 2, width * 2), np.float32)
    indiv = np.empty((len(records), 2, width * 10), np.float32)
    perf = np.empty((len(records), 2, pwidth * 10), np.float32)
    for i, r in enumerate(records):
        while at < len(events) and events[at]["end"] < r["start"]:
            event = events[at]
            if event["mid"] == r["mid"]:
                raise ValueError("query outcome in history")
            history.apply(event)
            at += 1
        rows, performance = history.rows(r["players"], r["start"])
        means = [history._mean_rows(side, width) for side in rows]
        for side in (0, 1):
            sig[i, side] = np.r_[means[side], means[side] - means[1-side]]
            indiv[i, side] = np.r_[rows[side].ravel(), rows[1-side].ravel()]
            perf[i, side] = np.r_[performance[side].ravel(), performance[1-side].ravel()]
        if i % 2000 == 0:
            print(json.dumps({"queries": i, "history_events": at}), flush=True)
    arrays.update(X_signature=sig, X_individual=indiv, X_performance=perf)
    output.mkdir(parents=True, exist_ok=True)
    core.atomic_npz(output / "dataset.npz", **arrays)
    meta = json.loads(metadata.read_text())
    meta.update(schema="kills-signature-research-v1", arms=list(ARMS), primary_selection_arms=list(ARMS),
                source_audit=source_audit, parent_dataset_sha256=core.sha256(dataset),
                signature_policy="wins=int(didRadiantWin); end < query.start; 10 marginal/pair pseudo-games; all and90d; player observations kept in hero-id order",
                performance_policy="DB nullable final K/D/A/GPM/XPM and censored window timelines only; rich pstats excluded; 5 player pseudo-games; missing stays NaN")
    meta["features"].update(
        signature=[f"signature_{side}_{name}" for side in ("own", "diff") for name in INDIVIDUAL_NAMES],
        individual=[f"individual_{side}_{slot}_{name}" for side in ("own", "opponent") for slot in range(5) for name in INDIVIDUAL_NAMES],
        performance=[f"performance_{side}_{slot}_{name}" for side in ("own", "opponent") for slot in range(5) for name in PERFORMANCE_NAMES])
    meta["block_audit"] = {k: {"shape": list(a.shape), "finite_fraction": float(np.isfinite(a).mean())}
                           for k, a in (("signature", sig), ("individual", indiv), ("performance", perf))}
    core.atomic_json(output / "metadata.json", meta)
    return meta


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("build", "train"))
    for arg in ("dataset", "metadata", "output-dir"):
        parser.add_argument(f"--{arg}", type=Path, required=True)
    parser.add_argument("--db", type=Path)
    parser.add_argument("--rich", type=Path)
    parser.add_argument("--target", choices=core.TARGETS)
    parser.add_argument("--threads", type=int, default=1)
    args = parser.parse_args()
    if args.mode == "build":
        result = augment(args.dataset, args.metadata, args.db, args.rich, args.output_dir)
    else:
        result = core.train_target(args.dataset, args.metadata, args.target, args.output_dir,
                                   args.threads, arms=ARMS, comparisons=COMPARISONS)
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
