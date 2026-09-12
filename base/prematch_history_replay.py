"""Causal, bounded history state for offline prematch replay.

The production scorer consumes a frozen snapshot.  This module supplies the
same table-shaped state as of a completed map without teaching the scorer about
replay.  Rows must arrive in completion order, not scheduled-start order.
"""
from __future__ import annotations

import math
from collections import defaultdict, deque
from itertools import combinations
from typing import Any, Iterable, Sequence

import numpy as np


PSTAT_NAMES = (
    "kills", "deaths", "assists", "goldPerMinute", "experiencePerMinute",
    "networth", "numLastHits", "numDenies", "heroDamage", "towerDamage",
    "heroHealing", "imp", "level", "dotaPlusHeroXp",
)
P_KILL, P_DEATH, P_ASSIST, P_GPM, P_NW, P_LH, P_HD, P_IMP, P_LEVEL = (0, 1, 2, 3, 5, 6, 8, 11, 12)
K = 24.0
PAIR_HL_SECONDS = 45.0 * 86400.0
EWMA_HL_SECONDS = 90.0 * 86400.0
G_RATING0, G_RD0, G_RD_MIN = 1500.0, 350.0, 30.0
_G_Q = math.log(10.0) / 400.0


class _RosterView:
    """Small ``OrgRosters``-compatible view used by ``PrematchModel``."""

    def __init__(self, rosters: dict[int, frozenset[int]]) -> None:
        self._rosters = rosters
        by_account: dict[int, list[int]] = defaultdict(list)
        for org, members in rosters.items():
            for account in members:
                by_account[account].append(org)
        self._by_account = {a: tuple(sorted(orgs)) for a, orgs in by_account.items()}

    def orgs_of(self, account: int) -> Iterable[int]:
        return self._by_account.get(int(account), ())

    def overlap(self, org: int, members: set[int]) -> int:
        return len(self._rosters.get(int(org), frozenset()) & members)


class CausalPrematchHistory:
    """Online accumulator for completed rich rows.

    ``target_accounts`` bounds player-keyed detail.  Global account Elo, hero
    norms, draft cells, team/org history and the 30-day hero window still see
    every valid completed map, because selected players' features depend on
    them.  Queries are monotonic: pruning the 30-day window at a later query is
    intentionally irreversible, as it is in a live process.
    """

    def __init__(self, target_accounts: set[int]) -> None:
        self.target_accounts = {int(a) for a in target_accounts if int(a) > 0}
        self.latest_end: int | None = None
        self._last_key: tuple[int, int] | None = None
        self._seen_mids: set[int] = set()
        self._last_bind_ts: int | None = None

        self.rating: dict[int, float] = {}
        self.team_rating: dict[int, float] = {}
        self.org_rating_raw: dict[int, float] = {}
        self.org_h2h_rating: dict[int, float] = {}
        self.org_rd: dict[int, float] = {}
        self.games: dict[int, int] = defaultdict(int)
        self.hero_games: dict[tuple[int, int], int] = defaultdict(int)
        self.pos_games: dict[tuple[int, int], int] = defaultdict(int)
        self.opp_sum: dict[int, float] = defaultdict(float)
        self.pool: dict[int, set[int]] = defaultdict(set)
        self.recent: dict[int, deque[int]] = defaultdict(lambda: deque(maxlen=20))
        self.imp_q: dict[int, deque[float]] = defaultdict(lambda: deque(maxlen=50))
        self.lh_q: dict[int, deque[float]] = defaultdict(lambda: deque(maxlen=30))
        self.kda_q: dict[int, deque[float]] = defaultdict(lambda: deque(maxlen=20))
        self.res30_imp: dict[int, deque[float]] = defaultdict(lambda: deque(maxlen=30))
        self.res30_lh: dict[int, deque[float]] = defaultdict(lambda: deque(maxlen=30))
        self.gpm_hero: dict[tuple[int, int], list[float]] = defaultdict(lambda: [0.0, 0.0])
        self.res_pos: dict[tuple[int, int], list[float]] = defaultdict(lambda: [0.0, 0.0])
        self.res_hero_lh: dict[tuple[int, int], list[float]] = defaultdict(lambda: [0.0, 0.0])
        self.res_hero_hd: dict[tuple[int, int], list[float]] = defaultdict(lambda: [0.0, 0.0])
        self.ew_gpm: dict[int, list[float]] = defaultdict(lambda: [0.0, 0.0, 0.0])
        self.lvl_res: dict[int, list[float]] = defaultdict(lambda: [0.0, 0.0])

        self.pos_sum = np.zeros((6, len(PSTAT_NAMES)), dtype=float)
        self.pos_count = np.zeros(6, dtype=float)
        self.pos_level_pm = np.zeros(6, dtype=float)
        self.hero_norm: dict[int, np.ndarray] = defaultdict(lambda: np.zeros(len(PSTAT_NAMES), dtype=float))
        self.hero_count: dict[int, float] = defaultdict(float)
        self.hero_all_gpm: dict[int, list[float]] = defaultdict(lambda: [0.0, 0.0])
        self.hero_farm: dict[int, list[float]] = defaultdict(lambda: [0.0, 0.0])

        self._hero_events: deque[tuple[int, int, int]] = deque()
        self._hero_wins: dict[int, float] = defaultdict(float)
        self._hero_games: dict[int, float] = defaultdict(float)
        self.vs: dict[tuple[int, int], list[float]] = {}
        self.vs_pos: dict[tuple[int, int, int, int], list[float]] = {}
        self.vs_flat: dict[tuple[int, int], list[float]] = {}
        self.syn_pos: dict[tuple[tuple[int, int], tuple[int, int]], list[float]] = {}
        self.syn_flat: dict[tuple[int, int], list[float]] = {}
        self.h2h: dict[tuple[int, int], list[float]] = defaultdict(lambda: [0.0, 0.0])
        self.h2h_org: dict[tuple[int, int], list[float]] = defaultdict(lambda: [0.0, 0.0])
        self.team_merge: dict[int, int] = {}
        self.org_roster: dict[int, frozenset[int]] = {}
        self._org_order: dict[int, int] = {}
        self._org_candidates: dict[int, set[int]] = defaultdict(set)

    @staticmethod
    def _mean_rating(rating: dict[int, float], accounts: Sequence[int]) -> float:
        vals = [rating.get(a, 1500.0) for a in accounts if a > 0]
        return float(np.mean(vals)) if vals else 1500.0

    @staticmethod
    def _decay(cell: list[float] | None, now: int, half_life: float = PAIR_HL_SECONDS) -> tuple[float, float]:
        if not cell or cell[1] <= 0:
            return 0.0, 0.0
        factor = math.exp(-math.log(2.0) * max(now - cell[2], 0) / half_life)
        return cell[0] * factor, cell[1] * factor

    def _update_pair(self, table: dict, key: Any, won: bool, now: int) -> None:
        w, g = self._decay(table.get(key), now)
        table[key] = [w + float(won), g + 1.0, float(now)]

    def _org_of(self, team: int, members: set[int]) -> int:
        if team <= 0 or len(members) < 5:
            return -1
        if team in self.team_merge:
            org = self.team_merge[team]
            self._set_roster(org, members)
            return org
        candidates = set().union(*(self._org_candidates[a] for a in members))
        for org in sorted(candidates, key=lambda o: self._org_order[o]):
            if len(self.org_roster[org] & members) >= 4:
                self.team_merge[team] = org
                self._set_roster(org, members)
                return org
        self.team_merge[team] = team
        self._org_order[team] = len(self._org_order)
        self._set_roster(team, members)
        return team

    def _set_roster(self, org: int, members: set[int]) -> None:
        roster = frozenset(members)
        self.org_roster[org] = roster
        # Candidates are append-only.  A stale candidate is harmless because
        # overlap is always checked against the current roster; avoiding removals
        # preserves the first-insertion choice without a global roster scan.
        for account in roster:
            self._org_candidates[account].add(org)

    @staticmethod
    def _g(rd: float) -> float:
        return 1.0 / math.sqrt(1.0 + 3.0 * _G_Q * _G_Q * rd * rd / (math.pi**2))

    def _update_org_rating(self, a: int, b: int, radiant_win: bool) -> None:
        ra, rb = self.org_rating_raw.get(a, G_RATING0), self.org_rating_raw.get(b, G_RATING0)
        da, db = self.org_rd.get(a, G_RD0), self.org_rd.get(b, G_RD0)
        ea = 1.0 / (1.0 + 10.0 ** (-self._g(db) * (ra - rb) / 400.0))
        eb = 1.0 / (1.0 + 10.0 ** (-self._g(da) * (rb - ra) / 400.0))
        for org, rating, rd, expected, opp_rd, score in (
            (a, ra, da, ea, db, float(radiant_win)),
            (b, rb, db, eb, da, float(not radiant_win)),
        ):
            gg = self._g(opp_rd)
            d2 = 1.0 / (_G_Q * _G_Q * gg * gg * expected * (1.0 - expected) + 1e-12)
            self.org_rating_raw[org] = rating + (_G_Q / (1.0 / (rd * rd) + 1.0 / d2)) * gg * (score - expected)
            self.org_rd[org] = max(math.sqrt(1.0 / (1.0 / (rd * rd) + 1.0 / d2)), G_RD_MIN)

    def observe(self, *, mid: int, start_ts: int, end_ts: int, heroes10: Sequence[int],
                accounts10: Sequence[int], teams2: Sequence[int], pstats10x14: Sequence[Sequence[float]],
                radiant_win: bool, duration_seconds: float) -> None:
        """Atomically add one completed map in strict ``(end_ts, mid)`` order."""
        mid, start, end = int(mid), int(start_ts), int(end_ts)
        heroes, accounts, teams = tuple(map(int, heroes10)), tuple(map(int, accounts10)), tuple(map(int, teams2))
        stats = np.asarray(pstats10x14, dtype=float)
        if (mid <= 0 or start < 0 or end <= start or len(heroes) != 10 or len(accounts) != 10
                or len(teams) != 2 or any(x <= 0 for x in heroes)
                or stats.shape != (10, len(PSTAT_NAMES)) or not np.isfinite(stats).all()
                or not math.isfinite(float(duration_seconds)) or float(duration_seconds) <= 0):
            raise ValueError("completed row lacks valid ids, 10x14 finite pstats, or duration")
        key = (end, mid)
        if mid in self._seen_mids:
            raise ValueError(f"duplicate completed map id: {mid}")
        if self._last_key is not None and key <= self._last_key:
            raise ValueError("completed rows must be strictly ordered by (end_ts, mid)")
        if self._last_bind_ts is not None and end < self._last_bind_ts:
            raise ValueError("cannot insert a completed result before an already emitted query")
        self._prune_wr30(end)

        team_mean = [self._mean_rating(self.rating, accounts[:5]), self._mean_rating(self.rating, accounts[5:])]
        expected_r = 1.0 / (1.0 + 10.0 ** ((team_mean[1] - team_mean[0]) / 400.0))
        dur_min = max(float(duration_seconds) / 60.0, 1.0)
        for side in range(2):
            won, expected, opp_elo = (bool(radiant_win) if side == 0 else not bool(radiant_win),
                                      expected_r if side == 0 else 1.0 - expected_r, team_mean[1 - side])
            hs = heroes[side * 5:(side + 1) * 5]
            for pos, (account, hero) in enumerate(zip(accounts[side * 5:(side + 1) * 5], hs), 1):
                st = stats[side * 5 + pos - 1]
                if account > 0:
                    self.rating[account] = self.rating.get(account, 1500.0) + K * (float(won) - expected)
                if account in self.target_accounts:
                    self.games[account] += 1; self.hero_games[(account, hero)] += 1; self.pos_games[(account, pos)] += 1
                    self.opp_sum[account] += opp_elo; self.pool[account].add(hero); self.recent[account].append(int(won))
                    self.imp_q[account].append(float(st[P_IMP])); self.lh_q[account].append(float(st[P_LH]))
                    self.kda_q[account].append((float(st[P_KILL]) + float(st[P_ASSIST])) / (1.0 + float(st[P_DEATH])))
                    gh = self.gpm_hero[(account, hero)]; gh[0] += float(st[P_GPM]); gh[1] += 1.0
                    if self.pos_count[pos] > 0:
                        pn = self.pos_sum[pos] / self.pos_count[pos]
                        for stat in (P_GPM, P_IMP, P_HD, P_NW):
                            cell = self.res_pos[(account, stat)]; cell[0] += float(st[stat]) - pn[stat]; cell[1] += 1.0
                        self.res30_imp[account].append(float(st[P_IMP]) - pn[P_IMP])
                        self.res30_lh[account].append(float(st[P_LH]) - pn[P_LH])
                        sm, wt, when = self.ew_gpm[account]
                        factor = math.exp(-math.log(2.0) * (end - when) / EWMA_HL_SECONDS) if wt else 0.0
                        self.ew_gpm[account] = [sm * factor + float(st[P_GPM]) - pn[P_GPM], wt * factor + 1.0, float(end)]
                        level_pm = float(st[P_LEVEL]) / dur_min
                        lr = self.lvl_res[account]; lr[0] += level_pm - self.pos_level_pm[pos] / self.pos_count[pos]; lr[1] += 1.0
                    if self.hero_count[hero] > 0:
                        hn = self.hero_norm[hero] / self.hero_count[hero]
                        cell = self.res_hero_lh[(account, hero)]; cell[0] += float(st[P_LH]) - hn[P_LH]; cell[1] += 1.0
                        cell = self.res_hero_hd[(account, hero)]; cell[0] += float(st[P_HD]) - hn[P_HD]; cell[1] += 1.0
                all_gpm = self.hero_all_gpm[hero]; all_gpm[0] += float(st[P_GPM]); all_gpm[1] += 1.0
                farm = self.hero_farm[hero]; farm[0] += float(st[P_LH]) / dur_min; farm[1] += 1.0
                self.pos_sum[pos] += st; self.pos_count[pos] += 1.0; self.pos_level_pm[pos] += float(st[P_LEVEL]) / dur_min
                self.hero_norm[hero] += st; self.hero_count[hero] += 1.0
                self._hero_events.append((end, hero, int(won))); self._hero_wins[hero] += float(won); self._hero_games[hero] += 1.0
            other = heroes[(1 - side) * 5:(2 - side) * 5]
            for hero in hs:
                for opponent in other:
                    self._update_pair(self.vs, (hero, opponent), won, end)
        radiant = [(heroes[p], p + 1) for p in range(5)]
        dire = [(heroes[5 + p], p + 1) for p in range(5)]
        for (h1, p1) in radiant:
            for (h2, p2) in dire:
                self._update_pair(self.vs_pos, (h1, p1, h2, p2), bool(radiant_win), end)
                self._update_pair(self.vs_flat, (h1, h2), bool(radiant_win), end)
                self._update_pair(self.vs_pos, (h2, p2, h1, p1), not bool(radiant_win), end)
                self._update_pair(self.vs_flat, (h2, h1), not bool(radiant_win), end)
        for side, result in ((radiant, bool(radiant_win)), (dire, not bool(radiant_win))):
            for a, b in combinations(side, 2):
                self._update_pair(self.syn_pos, (min(a, b), max(a, b)), result, end)
                self._update_pair(self.syn_flat, (min(a[0], b[0]), max(a[0], b[0])), result, end)
        rt, dt = teams
        if rt > 0 and dt > 0:
            er = 1.0 / (1.0 + 10.0 ** ((self.team_rating.get(dt, 1500.0) - self.team_rating.get(rt, 1500.0)) / 400.0))
            pair, sign = (min(rt, dt), max(rt, dt)), (1.0 if rt < dt else -1.0)
            self.h2h[pair][0] += sign * (float(radiant_win) - er); self.h2h[pair][1] += 1.0
            self.team_rating[rt] = self.team_rating.get(rt, 1500.0) + K * (float(radiant_win) - er)
            self.team_rating[dt] = self.team_rating.get(dt, 1500.0) + K * (float(not radiant_win) - (1.0 - er))
        org_r = self._org_of(rt, {a for a in accounts[:5] if a > 0})
        org_d = self._org_of(dt, {a for a in accounts[5:] if a > 0})
        if org_r > 0 and org_d > 0 and org_r != org_d:
            er = 1.0 / (1.0 + 10.0 ** ((self.org_h2h_rating.get(org_d, 1500.0) - self.org_h2h_rating.get(org_r, 1500.0)) / 400.0))
            pair, sign = (min(org_r, org_d), max(org_r, org_d)), (1.0 if org_r < org_d else -1.0)
            self.h2h_org[pair][0] += sign * (float(radiant_win) - er); self.h2h_org[pair][1] += 1.0
            delta = K * (float(radiant_win) - er)
            self.org_h2h_rating[org_r] = self.org_h2h_rating.get(org_r, 1500.0) + delta
            self.org_h2h_rating[org_d] = self.org_h2h_rating.get(org_d, 1500.0) - delta
            self._update_org_rating(org_r, org_d, bool(radiant_win))
        self.latest_end, self._last_key = end, key
        self._seen_mids.add(mid)

    def _prune_wr30(self, now: int) -> None:
        while self._hero_events and self._hero_events[0][0] < now - 30 * 86400:
            _end, hero, won = self._hero_events.popleft()
            self._hero_wins[hero] -= won; self._hero_games[hero] -= 1.0

    def _resolved_org(self, team: int, accounts: Sequence[int]) -> int:
        members = {int(a) for a in accounts if int(a) > 0}
        if len(members) < 4:
            return self.team_merge.get(team, team if team > 0 else -1)
        candidates = set().union(*(self._org_candidates[a] for a in members))
        best, overlap = -1, 0
        for org in candidates:
            got = len(self.org_roster[org] & members)
            if got > overlap or (got == overlap and got > 0 and org < best):
                best, overlap = org, got
        return best if overlap >= 4 else self.team_merge.get(team, team if team > 0 else -1)

    def bind_scorer(self, model: Any, *, now_ts: int, accounts10: Sequence[int],
                    heroes10: Sequence[int], teams2: Sequence[int] = (0, 0)) -> None:
        """Bind query-only history; use the same team ids in the following score."""
        now = int(now_ts)
        accounts, heroes = tuple(map(int, accounts10)), tuple(map(int, heroes10))
        teams = tuple(map(int, teams2))
        if len(teams) != 2:
            raise ValueError("query requires two team ids")
        if len(accounts) != 10 or len(heroes) != 10 or any(a <= 0 for a in accounts) or any(h <= 0 for h in heroes):
            raise ValueError("query requires ten positive account and hero ids")
        if self.latest_end is not None and now <= self.latest_end:
            raise ValueError("query time must be strictly after every observed completion")
        if self._last_bind_ts is not None and now < self._last_bind_ts:
            raise ValueError("query time cannot move backwards after WR30 pruning")
        self._prune_wr30(now)
        self._last_bind_ts = now
        query_accounts, query_heroes = set(accounts), set(heroes)
        acc: dict[int, np.ndarray] = {}
        acc_hero: dict[tuple[int, int], tuple[float, ...]] = {}
        acc_pos: dict[tuple[int, int], float] = {}
        for slot, (account, hero) in enumerate(zip(accounts, heroes), 1):
            pos = ((slot - 1) % 5) + 1
            if account not in self.target_accounts or account not in self.games:
                continue
            mean = lambda values: float(np.mean(values)) if values else 0.0
            ew = self.ew_gpm[account]
            acc[account] = np.array([
                self.rating.get(account, 1500.0), self.games[account], self.opp_sum[account] / self.games[account],
                len(self.pool[account]), mean(self.recent[account]), mean(self.imp_q[account]),
                mean(list(self.imp_q[account])[-30:]),
                self.res_pos[(account, P_GPM)][0] / max(self.res_pos[(account, P_GPM)][1], 1.0),
                self.res_pos[(account, P_IMP)][0] / max(self.res_pos[(account, P_IMP)][1], 1.0),
                ew[0] / ew[1] if ew[1] else 0.0, mean(self.lh_q[account]),
                self.lvl_res[account][0] / max(self.lvl_res[account][1], 1.0), mean(self.kda_q[account]),
                self.res_pos[(account, P_HD)][0] / max(self.res_pos[(account, P_HD)][1], 1.0),
                self.res_pos[(account, P_NW)][0] / max(self.res_pos[(account, P_NW)][1], 1.0),
                mean(list(self.imp_q[account])[-10:]), mean(self.res30_imp[account]), mean(self.res30_lh[account]),
            ])
            if (account, hero) in self.hero_games:
                gh = self.gpm_hero[(account, hero)]
                hero_gpm = self.hero_all_gpm[hero]
                acc_hero[(account, hero)] = (
                    float(self.hero_games[(account, hero)]),
                    gh[0] / gh[1] - hero_gpm[0] / max(hero_gpm[1], 1.0),
                    self.res_hero_lh[(account, hero)][0] / max(self.res_hero_lh[(account, hero)][1], 1.0),
                    self.res_hero_hd[(account, hero)][0] / max(self.res_hero_hd[(account, hero)][1], 1.0),
                )
            # The scorer checks current-role share against all five roles.
            for known_pos in range(1, 6):
                if (account, known_pos) in self.pos_games:
                    acc_pos[(account, known_pos)] = float(self.pos_games[(account, known_pos)])
        hero_wr30 = {h: (self._hero_wins[h] + 5.0) / (self._hero_games[h] + 10.0)
                     for h in query_heroes if self._hero_games[h] > 0}
        hero_farm = {h: self.hero_farm[h][0] / self.hero_farm[h][1]
                     for h in query_heroes if h in self.hero_farm and self.hero_farm[h][1] > 0}
        pair_keys = {(a, b) for a in query_heroes for b in query_heroes}
        vs = {k: self._decay(self.vs[k], now) for k in pair_keys if k in self.vs}
        rad, dire = [[(h, p) for p, h in enumerate(side, 1)] for side in (heroes[:5], heroes[5:])]
        pos_keys = {(a, p, b, q) for a, p in rad for b, q in dire}
        vs_pos = {k: tuple(self.vs_pos[k]) for k in pos_keys if k in self.vs_pos}
        vs_flat = {k: tuple(self.vs_flat[k]) for k in pair_keys if k in self.vs_flat}
        syn_keys = {(min(a, b), max(a, b)) for side in (rad, dire) for a, b in combinations(side, 2)}
        flat_syn_keys = {(min(a[0], b[0]), max(a[0], b[0])) for a, b in syn_keys}
        syn_pos = {k: tuple(self.syn_pos[k]) for k in syn_keys if k in self.syn_pos}
        syn_flat = {k: tuple(self.syn_flat[k]) for k in flat_syn_keys if k in self.syn_flat}
        candidate_orgs = set().union(*(self._org_candidates[a] for a in query_accounts))
        rosters = {o: self.org_roster[o] for o in candidate_orgs}
        orgs = [self._resolved_org(t, side) for t, side in zip(teams, (accounts[:5], accounts[5:]))]
        raw_key, org_key = tuple(sorted(teams)), tuple(sorted(orgs))
        h2h = {raw_key: self.h2h[raw_key][0] / (self.h2h[raw_key][1] + 3.0)} if raw_key in self.h2h else {}
        h2h_org = {org_key: self.h2h_org[org_key][0] / (self.h2h_org[org_key][1] + 3.0)} if org_key in self.h2h_org else {}
        model.snapshot_ts = now
        model.acc, model.acc_hero, model.acc_pos = acc, acc_hero, acc_pos
        model.hero_wr30, model.hero_farm, model.vs = hero_wr30, hero_farm, vs
        model.vs_pos, model.vs_flat, model.syn_pos, model.syn_flat = vs_pos, vs_flat, syn_pos, syn_flat
        model.h2h, model.team_merge, model.h2h_org = h2h, {t: self.team_merge[t] for t in teams if t in self.team_merge}, h2h_org
        model.org_roster = _RosterView(rosters)
        model.org_rating = {o: (self.org_rating_raw.get(o, G_RATING0), self.org_rd.get(o, G_RD0))
                            for o in candidate_orgs if o in self.org_rating_raw}
