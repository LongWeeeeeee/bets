"""Проверки дельты по игрокам.

Стерегутся две вещи. Первая — ГРАНИЦА СНИМКА: карта, уже вошедшая в артефакт, не
имеет права попасть в дельту второй раз, иначе счётчики удвоятся. Вторая —
ИДЕМПОТЕНТНОСТЬ: одну и ту же карту прод обрабатывает многократно, и повторная
запись не должна ничего менять.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from prematch_live_delta import (counters, extra_for_accounts,  # noqa: E402
                                 player_maps, prior_contribs, record_map,
                                 retry_incomplete, set_snapshot_ts, sync_to_ts)

T0 = 1_700_000_000


def _match(mid, end, accs, heroes, rad_won=True, positions=None,
           rk=None, dk=None, nw=None, xp=None):
    pos = positions or ["POSITION_1", "POSITION_2", "POSITION_3", "POSITION_4", "POSITION_5"] * 2
    m = {"id": mid, "endDateTime": end, "startDateTime": end - 2100,
         "durationSeconds": 2100, "leagueId": 19719, "didRadiantWin": rad_won,
         "players": [{"steamAccountId": a, "heroId": h, "isRadiant": i < 5,
                      "isVictory": (i < 5) == rad_won, "position": pos[i],
                      "kills": 5, "deaths": 2, "assists": 7, "numLastHits": 200,
                      "numDenies": 10, "goldPerMinute": 500, "networth": 20000,
                      "experiencePerMinute": 600, "level": 22,
                      "heroDamage": 15000, "imp": 3}
                     for i, (a, h) in enumerate(zip(accs, heroes))]}
    if rk is not None:
        m["radiantKills"] = rk
    if dk is not None:
        m["direKills"] = dk
    if nw is not None:
        m["radiantNetworthLeads"] = nw
    if xp is not None:
        m["radiantExperienceLeads"] = xp
    return m


def _store(tmp_path: Path) -> Path:
    return tmp_path / "delta.json"


ACCS = list(range(101, 111))
HEROES = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


def test_record_and_count(tmp_path: Path) -> None:
    st = _store(tmp_path)
    assert record_map(_match(1, T0, ACCS, HEROES), store_path=st, now=T0 + 60) == 10
    c = counters(101, store_path=st)
    assert c["games"] == 1 and c["wins"] == 1
    assert c["hero_games"] == {1: 1} and c["pos_games"] == {1: 1}
    assert counters(106, store_path=st)["wins"] == 0, "дайр проиграл"


def test_same_map_recorded_twice_is_idempotent(tmp_path: Path) -> None:
    """Прод обрабатывает карту многократно — счётчики удваиваться не должны."""
    st = _store(tmp_path)
    for _ in range(3):
        record_map(_match(1, T0, ACCS, HEROES), store_path=st, now=T0 + 60)
    assert counters(101, store_path=st)["games"] == 1


def test_map_inside_snapshot_is_dropped(tmp_path: Path) -> None:
    """Карта, уже вошедшая в артефакт, в дельте не нужна — иначе двойной счёт."""
    st = _store(tmp_path)
    record_map(_match(1, T0, ACCS, HEROES), store_path=st, now=T0 + 60)
    record_map(_match(2, T0 + 5000, ACCS, HEROES), store_path=st, now=T0 + 5060)
    dropped = set_snapshot_ts(T0 + 100, store_path=st, now=T0 + 6000)
    assert dropped == 1
    assert counters(101, store_path=st)["games"] == 1


def test_two_maps_accumulate_hero_and_position(tmp_path: Path) -> None:
    st = _store(tmp_path)
    record_map(_match(1, T0, ACCS, HEROES), store_path=st, now=T0 + 60)
    record_map(_match(2, T0 + 5000, ACCS, [77] + HEROES[1:], rad_won=False),
               store_path=st, now=T0 + 5060)
    c = counters(101, store_path=st)
    assert c["games"] == 2 and c["wins"] == 1
    assert c["hero_games"] == {1: 1, 77: 1}
    assert c["pos_games"] == {1: 2}
    assert c["heroes"] == [1, 77]


def test_player_maps_are_ordered(tmp_path: Path) -> None:
    st = _store(tmp_path)
    record_map(_match(2, T0 + 5000, ACCS, HEROES), store_path=st, now=T0 + 5060)
    record_map(_match(1, T0, ACCS, HEROES), store_path=st, now=T0 + 5060)
    got = player_maps(101, store_path=st)
    assert [r["match_id"] for r in got] == [1, 2]


def test_garbage_input_is_ignored(tmp_path: Path) -> None:
    st = _store(tmp_path)
    assert record_map({}, store_path=st) == 0
    assert record_map({"id": 0, "players": []}, store_path=st) == 0
    assert record_map(None, store_path=st) == 0
    assert counters(101, store_path=st)["games"] == 0


def test_corrupted_store_is_survived(tmp_path: Path) -> None:
    st = _store(tmp_path)
    st.write_text("не json", encoding="utf-8")
    assert record_map(_match(1, T0, ACCS, HEROES), store_path=st, now=T0 + 60) == 10
    assert counters(101, store_path=st)["games"] == 1


def test_sync_snapshot_drops_what_artifact_already_knows(tmp_path: Path) -> None:
    """Приехал новый артефакт — карты внутри его среза обязаны уйти из дельты."""
    import numpy as np
    from prematch_live_delta import sync_snapshot
    st = _store(tmp_path)
    art = tmp_path / "art.npz"
    np.savez(art, snapshot_ts=np.int64(T0 + 100))
    record_map(_match(1, T0, ACCS, HEROES), store_path=st, now=T0 + 60)
    record_map(_match(2, T0 + 5000, ACCS, HEROES), store_path=st, now=T0 + 5060)
    assert sync_snapshot(artifact_path=art, store_path=st, now=T0 + 6000) == 1
    assert counters(101, store_path=st)["games"] == 1
    # повторный вызов ничего не двигает
    assert sync_snapshot(artifact_path=art, store_path=st, now=T0 + 6000) == 0


def _scorer_artifact(tmp_path: Path):
    """Минимальный артефакт с колонкой games, чтобы сверка дельты была числом."""
    import numpy as np
    import prematch_scorer as ps
    feats = ["draft_logit", "elo", "games", "hero_games", "pos_games",
             "hero_pool", "hybrid_strength"]
    n = len(feats)
    acc = np.array([[a, 1500.0, 10.0, 1500.0, 4.0, 0.5] + [0.0] * 13
                    for a in ACCS], dtype=np.float64)
    z = {
        "snapshot_ts": np.array([T0], dtype=np.int64),
        "mu": np.zeros((1, n)), "sd": np.ones((1, n)),
        "coef": np.zeros((1, n)), "intercept": np.zeros(1),
        "accounts": acc,
        "acc_hero": np.array([[101, 1, 3.0, 0.0, 0.0, 0.0]], dtype=np.float64),
        "acc_pos": np.array([[101, 1, 5.0]], dtype=np.float64),
        "hero_wr30": np.array([[h, 0.5] for h in range(1, 21)], dtype=np.float64),
        "hero_farm": np.array([[h, 0.4] for h in range(1, 21)], dtype=np.float64),
        "vs_pairs": np.zeros((0, 4)),
        "h2h": np.zeros((0, 3)),
        "feature_names": np.array(feats),
    }
    p = tmp_path / "scorer.npz"
    np.savez_compressed(p, **z)
    return ps.PrematchModel(p)


def _score(model, monkeypatch, store):
    monkeypatch.setenv("PREMATCH_LIVE_DELTA", str(store))
    return model.score(
        radiant_accounts=ACCS[:5], dire_accounts=ACCS[5:],
        radiant_heroes=HEROES[:5], dire_heroes=HEROES[5:],
        radiant_team_id=0, dire_team_id=0,
        draft_logit=0.0, hybrid_strength=0.0,
        strictness="accounts", now_ts=T0, max_age_days=1e9)


def test_score_adds_games_played_after_the_snapshot(tmp_path, monkeypatch) -> None:
    """Карта после среза снимка обязана сдвинуть games, а не ждать ночной сборки."""
    st = _store(tmp_path)
    model = _scorer_artifact(tmp_path)
    before = _score(model, monkeypatch, st)
    # лишняя карта только у 101 — иначе обе стороны сдвинутся одинаково
    extra = [101, 901, 902, 903, 904, 905, 906, 907, 908, 909]
    record_map(_match(9, T0 + 3600, extra, HEROES), store_path=st, now=T0 + 3660)
    after = _score(model, monkeypatch, st)
    assert after.features["games"] > before.features["games"]
    assert after.features["hero_games"] > before.features["hero_games"]
    assert after.features["pos_games"] > before.features["pos_games"]


def test_score_ignores_maps_already_inside_the_snapshot(tmp_path, monkeypatch) -> None:
    """Иначе ночная доставка удваивает счётчики."""
    st = _store(tmp_path)
    model = _scorer_artifact(tmp_path)
    before = _score(model, monkeypatch, st)
    record_map(_match(9, T0 - 100, ACCS, HEROES), store_path=st, now=T0)
    after = _score(model, monkeypatch, st)
    assert after.features["games"] == before.features["games"]
    assert after.features["hero_games"] == before.features["hero_games"]
    assert after.features["pos_games"] == before.features["pos_games"]


def test_sync_to_ts_does_not_create_a_missing_store(tmp_path) -> None:
    """Золотой скор не имеет права завести боевой файл дельты побочным эффектом."""
    missing = tmp_path / "no_such_delta.json"
    assert sync_to_ts(T0, store_path=missing) == 0
    assert not missing.exists()
    assert extra_for_accounts(ACCS, T0, store_path=missing) == {}


def test_score_does_not_create_a_missing_delta_store(tmp_path, monkeypatch) -> None:
    missing = tmp_path / "no_such_delta.json"
    model = _scorer_artifact(tmp_path)
    _score(model, monkeypatch, missing)
    assert not missing.exists()


def test_prior_contribs_skip_maps_inside_the_snapshot(tmp_path) -> None:
    from causal_priors import PRIOR_NAMES

    st = _store(tmp_path)
    record_map(_match(1, T0 - 10, ACCS, HEROES), store_path=st, now=T0)
    record_map(_match(2, T0 + 100, ACCS, HEROES), store_path=st, now=T0 + 160)
    got = prior_contribs(T0, store_path=st)
    assert len(got) == 1
    assert got[0].accounts[0] == 101
    assert got[0].heroes[0] == 1
    own = PRIOR_NAMES.index("own_kills")
    assert got[0].vr[own] == pytest.approx(25.0)  # 5 игроков × 5 килов
    assert got[0].vd[own] == pytest.approx(25.0)
    assert not got[0].mask[PRIOR_NAMES.index("k_0_10")]


def test_prior_contribs_open_windows_when_minute_row_is_present(tmp_path) -> None:
    from causal_priors import PRIOR_NAMES

    st = _store(tmp_path)
    record_map(_match(2, T0 + 100, ACCS, HEROES, rk=[1] * 40, dk=[0] * 40,
                      nw=[0] * 41, xp=[0] * 41),
               store_path=st, now=T0 + 160)
    got = prior_contribs(T0, store_path=st)
    assert len(got) == 1
    assert got[0].mask[PRIOR_NAMES.index("k_0_10")]
    assert got[0].vr[PRIOR_NAMES.index("k_0_10")] > 0


def test_retry_incomplete_does_not_create_a_missing_store(tmp_path) -> None:
    missing = tmp_path / "no_such_delta.json"
    assert retry_incomplete(fetch=lambda mid: None, store_path=missing) == 0
    assert not missing.exists()


def test_retry_incomplete_fills_minute_row_later(tmp_path) -> None:
    from causal_priors import PRIOR_NAMES

    st = _store(tmp_path)
    record_map(_match(2, T0 + 100, ACCS, HEROES), store_path=st, now=T0 + 160)

    def fetch(mid):
        return _match(mid, T0 + 100, ACCS, HEROES, rk=[1] * 40, dk=[0] * 40)

    assert retry_incomplete(fetch=fetch, store_path=st, now=T0 + 1000) == 1
    got = prior_contribs(T0, store_path=st)
    assert got[0].mask[PRIOR_NAMES.index("k_0_10")]
    assert retry_incomplete(fetch=fetch, store_path=st, now=T0 + 2000) == 0


def test_players_query_asks_for_minute_rows() -> None:
    from stratz_map_result import _PLAYERS_QUERY

    for field in ("radiantKills", "direKills",
                  "radiantNetworthLeads", "radiantExperienceLeads"):
        assert field in _PLAYERS_QUERY


def test_overlay_from_delta_moves_player_prior(tmp_path, monkeypatch) -> None:
    """Карта после среза обязана сдвинуть приор игрока, а не ждать ночной npz."""
    import os
    import numpy as np
    from causal_priors import K_PLAYER, PRIOR_NAMES, PriorSnapshot, overlay
    import prematch_panel_live as panel

    m = len(PRIOR_NAMES)
    st = Path(os.environ["PREMATCH_LIVE_DELTA"])
    record_map(_match(2, T0 + 100, ACCS, HEROES), store_path=st, now=T0 + 160)
    snap = PriorSnapshot(
        metrics=PRIOR_NAMES,
        hero_keys=np.zeros(0, np.int64),
        hero_sums=np.zeros((0, m)), hero_counts=np.zeros((0, m)),
        player_keys=np.array([101], dtype=np.int64),
        player_sums=np.zeros((1, m)), player_counts=np.full((1, m), 5.0),
        globals_=np.zeros(m), built_ts=T0)
    before = snap.player_priors([101])[0, 0]
    after = overlay(snap, prior_contribs(T0, store_path=st)).player_priors([101])[0, 0]
    assert after > before
    assert after == pytest.approx((25.0) / (5.0 + 1.0 + K_PLAYER))
    live, contribs = panel.live_prior_snapshot(snap)
    assert contribs and live.player_priors([101])[0, 0] == pytest.approx(after)
    assert snap.player_priors([101])[0, 0] == pytest.approx(before)
