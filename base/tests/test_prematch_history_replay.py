import copy

import numpy as np
import pytest

from base.prematch_history_replay import CausalPrematchHistory
from base import prematch_components as components
from base import prematch_scorer as scorer


ACCOUNTS = tuple(range(1, 11))
HEROES = tuple(range(101, 111))


def _stats(seed=0):
    rows = np.zeros((10, 14), dtype=float)
    for slot in range(10):
        rows[slot] = np.arange(14, dtype=float) + seed + slot
        rows[slot, 3] = 400 + seed + slot * 10
        rows[slot, 5] = 10_000 + seed + slot * 100
        rows[slot, 6] = 100 + seed + slot
        rows[slot, 8] = 5_000 + seed + slot * 100
        rows[slot, 11] = 20 + seed + slot
        rows[slot, 12] = 15 + slot / 10
    return rows


def _observe(history, mid, end, *, accounts=ACCOUNTS, heroes=HEROES, teams=(1001, 1002), seed=0, win=True, start=None):
    history.observe(mid=mid, start_ts=end - 60 if start is None else start, end_ts=end,
                    heroes10=heroes, accounts10=accounts, teams2=teams,
                    pstats10x14=_stats(seed), radiant_win=win, duration_seconds=3600)


def _model(tmp_path):
    features = scorer.BASE20 + scorer.EXTRA3 + scorer.NEW6 + [
        f"{key}_x_{ctx}" for ctx in ("elo_gap", "games_exp") for key in scorer.INTER_KEYS
    ]
    path = tmp_path / "tiny_prematch.npz"
    np.savez_compressed(
        path, snapshot_ts=np.array([0]), mu=np.zeros((1, len(features))), sd=np.ones((1, len(features))),
        coef=np.zeros((1, len(features))), intercept=np.zeros(1), feature_names=np.array(features),
        ctx_mu=np.zeros(2), ctx_sd=np.ones(2),
        accounts=np.zeros((0, 19)), acc_hero=np.zeros((0, 6)), acc_pos=np.zeros((0, 3)),
        hero_wr30=np.zeros((0, 2)), vs_pairs=np.zeros((0, 4)), h2h=np.zeros((0, 3)),
        hero_farm=np.zeros((0, 2)), vs_pos=np.zeros((0, 7)), vs_flat=np.zeros((0, 5)),
        syn_pos=np.zeros((0, 7)), syn_flat=np.zeros((0, 5)), team_merge=np.zeros((0, 2)),
        h2h_org=np.zeros((0, 3)), org_roster=np.zeros((0, 6)),
    )
    return scorer.PrematchModel(path)


def _score(model, *, teams=(1001, 1002), strictness="full"):
    return model.score(radiant_accounts=ACCOUNTS[:5], dire_accounts=ACCOUNTS[5:],
                       radiant_heroes=HEROES[:5], dire_heroes=HEROES[5:],
                       radiant_team_id=teams[0], dire_team_id=teams[1], draft_logit=0.2,
                       hybrid_strength=0.1, strictness=strictness, now_ts=model.snapshot_ts)


def test_full35_binding_uses_completed_history_and_preserves_weights(tmp_path):
    history = CausalPrematchHistory(set(ACCOUNTS))
    _observe(history, 1, 100)
    model = _model(tmp_path)
    weights = (model.mu.copy(), model.sd.copy(), model.coef.copy(), model.intercept.copy(), list(model.features))

    history.bind_scorer(model, now_ts=101, accounts10=ACCOUNTS, heroes10=HEROES)
    result = _score(model)

    assert len(result.features) == 35
    assert result.features["elo"] == pytest.approx(24.0 / 400.0)
    assert result.features["wr30"] == pytest.approx(1.0 / 11.0)
    assert result.features["vs_wr"] == pytest.approx(1.0 / 11.0)
    # Within-map order is intentional: Dire sees Radiant's same-position norm,
    # exactly as the historical snapshot builder did.
    assert result.features["gpm_rel_pos"] == pytest.approx(-0.5)
    assert np.array_equal(model.mu, weights[0]) and np.array_equal(model.sd, weights[1])
    assert np.array_equal(model.coef, weights[2]) and np.array_equal(model.intercept, weights[3])
    assert model.features == weights[4]


def test_completion_boundary_is_strict_and_same_end_order_is_deterministic(tmp_path):
    history = CausalPrematchHistory(set(ACCOUNTS))
    _observe(history, 10, 100)
    _observe(history, 11, 100, seed=1)  # tie is resolved by ascending mid
    with pytest.raises(ValueError, match="duplicate"):
        _observe(history, 11, 101)
    with pytest.raises(ValueError, match="ordered"):
        _observe(history, 9, 100)
    with pytest.raises(ValueError, match="strictly after"):
        history.bind_scorer(_model(tmp_path), now_ts=100, accounts10=ACCOUNTS, heroes10=HEROES)


def test_late_finisher_and_future_map_do_not_enter_earlier_prediction(tmp_path):
    history = CausalPrematchHistory(set(ACCOUNTS))
    _observe(history, 1, 100, start=1)
    model = _model(tmp_path)
    history.bind_scorer(model, now_ts=150, accounts10=ACCOUNTS, heroes10=HEROES)
    earlier = _score(model).features
    assert model.acc[1][1] == 1

    # This map started before the query, but completed after it.  The replay
    # iterator must not call observe until end=200; the accumulator cannot be
    # rewound after completion state has advanced.
    _observe(history, 2, 200, start=10, seed=100, win=False)
    with pytest.raises(ValueError, match="strictly after every observed"):
        history.bind_scorer(model, now_ts=150, accounts10=ACCOUNTS, heroes10=HEROES)
    history.bind_scorer(model, now_ts=201, accounts10=ACCOUNTS, heroes10=HEROES)
    assert model.acc[1][1] == 2
    assert earlier["elo"] == pytest.approx(24.0 / 400.0)


def test_selected_detail_matches_unfiltered_for_selected_players(tmp_path):
    all_accounts = set(range(1, 21))
    selected, unfiltered = CausalPrematchHistory(set(ACCOUNTS)), CausalPrematchHistory(all_accounts)
    for history in (selected, unfiltered):
        _observe(history, 1, 100, seed=1)
        _observe(history, 2, 200, accounts=tuple(range(11, 21)), heroes=tuple(range(121, 131)), teams=(2001, 2002), seed=2)
        _observe(history, 3, 300, seed=3, win=False)
    left, right = _model(tmp_path), _model(tmp_path)
    selected.bind_scorer(left, now_ts=301, accounts10=ACCOUNTS, heroes10=HEROES)
    unfiltered.bind_scorer(right, now_ts=301, accounts10=ACCOUNTS, heroes10=HEROES)
    for account in ACCOUNTS:
        assert np.allclose(left.acc[account], right.acc[account])
    assert left.acc_hero == right.acc_hero and left.acc_pos == right.acc_pos
    assert _score(left).features == pytest.approx(_score(right).features)


def test_no_org_branch_remains_usable_with_zero_team_ids(tmp_path):
    history = CausalPrematchHistory(set(ACCOUNTS))
    _observe(history, 1, 100, teams=(0, 0))
    model = _model(tmp_path)
    cols = components.columns_for_branch("no_org", model.features)
    model.branches = {"no_org": scorer.Branch(cols=cols, mu=np.zeros(len(cols)), sd=np.ones(len(cols)),
                                                coef=np.zeros(len(cols)), intercept=0.0)}
    history.bind_scorer(model, now_ts=101, accounts10=ACCOUNTS, heroes10=HEROES)
    result = _score(model, teams=(0, 0), strictness="accounts")
    assert result.branch == "no_org"
    assert "h2h_resid" not in result.features


def test_invalid_row_is_rejected_without_state_and_pair_history_decays_at_query(tmp_path):
    history = CausalPrematchHistory(set(ACCOUNTS))
    broken = _stats()
    broken[0, 3] = np.nan
    with pytest.raises(ValueError, match="finite"):
        history.observe(mid=1, start_ts=1, end_ts=100, heroes10=HEROES, accounts10=ACCOUNTS,
                        teams2=(1001, 1002), pstats10x14=broken, radiant_win=True, duration_seconds=3600)
    assert history.latest_end is None and not history.rating

    _observe(history, 1, 100)
    model = _model(tmp_path)
    half_life_later = 100 + 45 * 86400 + 1
    history.bind_scorer(model, now_ts=half_life_later, accounts10=ACCOUNTS, heroes10=HEROES)
    wins, games = model.vs[(101, 106)]
    expected = 2.0 ** (-(45 * 86400 + 1) / (45 * 86400))
    assert wins == pytest.approx(expected, rel=1e-12)
    assert games == pytest.approx(expected, rel=1e-12)


def test_sparse_queries_keep_only_thirty_days_of_hero_events():
    history = CausalPrematchHistory(set(ACCOUNTS))
    for day in range(200):
        _observe(history, day + 1, 100 + day * 86400)
    assert len(history._hero_events) == 31 * 10  # lower boundary is inclusive
    assert sum(history._hero_games.values()) == 31 * 10


def test_partial_accounts_preserve_global_history_and_skip_unknown_player():
    history = CausalPrematchHistory(set(ACCOUNTS))
    partial = (0,) + ACCOUNTS[1:]
    _observe(history, 1, 100, accounts=partial)
    assert 0 not in history.rating and sum(history.games.values()) == 9
    assert sum(history._hero_games.values()) == 10
    assert len(history.vs) == 50
    assert 1001 not in history.team_merge  # four positive players cannot seed an org
    assert history._mean_rating({2: 1700}, partial[:5]) == 1550


def test_org_h2h_uses_plain_k24_independently_of_glicko(tmp_path):
    history = CausalPrematchHistory(set(ACCOUNTS))
    _observe(history, 1, 100, win=True)
    _observe(history, 2, 200, win=False)
    expected_second = 1.0 / (1.0 + 10.0 ** (-24.0 / 400.0))
    model = _model(tmp_path)
    history.bind_scorer(model, now_ts=201, accounts10=ACCOUNTS, heroes10=HEROES)
    assert model.h2h_org[(1001, 1002)] == pytest.approx((0.5 - expected_second) / 5.0)
    assert history.org_h2h_rating[1001] != history.org_rating_raw[1001]


def test_binding_preserves_all_role_counts_for_position_guard(tmp_path):
    history = CausalPrematchHistory(set(ACCOUNTS))
    for i in range(30):
        _observe(history, i + 1, 100 + i * 100)
    swapped = (2, 3, 1) + ACCOUNTS[3:]
    model = _model(tmp_path)
    history.bind_scorer(model, now_ts=3101, accounts10=swapped, heroes10=HEROES)
    errors = []
    model._check_positions(swapped, errors, [])
    assert len(errors) == 1 and '3 слотов' in errors[0]


def test_binding_does_not_scan_global_pair_tables(tmp_path):
    class QueryOnly(dict):
        def items(self):
            raise AssertionError('global table scan')

    history = CausalPrematchHistory(set(ACCOUNTS))
    _observe(history, 1, 100)
    for name in ('hero_farm', 'vs', 'vs_pos', 'vs_flat', 'syn_pos', 'syn_flat',
                 'h2h', 'h2h_org', 'team_merge'):
        setattr(history, name, QueryOnly(getattr(history, name)))
    model = _model(tmp_path)
    history.bind_scorer(model, now_ts=101, accounts10=ACCOUNTS, heroes10=HEROES,
                        teams2=(1001, 1002))
    assert len(_score(model).features) == 35
