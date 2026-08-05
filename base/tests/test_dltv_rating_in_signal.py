"""DLTV draft-vote rating as the ``dltv_rating`` STAR metric of the All block."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, List

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402


@pytest.fixture(autouse=True)
def _clear_dltv_rating_cache():
    """TTL-кэш dltv_rating общий на процесс — чистим между тестами."""
    runtime._DLTV_RATING_STAR_CACHE.clear()
    yield
    runtime._DLTV_RATING_STAR_CACHE.clear()


def test_parse_dltv_draft_vote_from_live_payload() -> None:
    payload = {
        "match_id": 123,
        "db": {
            "series": {
                "likes": {"side_0": 27, "side_1": 15},
                "is_draft_voting": True,
            },
            # Legacy fixture without is_radiant: series order = radiant/dire.
            "first_team": {"title": "Nigma Galaxy"},
            "second_team": {"title": "Team Spirit"},
        },
    }
    vote = runtime._parse_dltv_draft_vote_from_live_payload(payload)
    assert vote is not None
    assert vote["radiant_likes"] == 27
    assert vote["dire_likes"] == 15
    assert vote["radiant_pct"] == 64.3
    assert vote["dire_pct"] == 35.7
    assert vote["radiant_team"] == "Nigma Galaxy"
    assert vote["dire_team"] == "Team Spirit"
    line = runtime._format_dltv_rating_line(vote)
    assert line.startswith("DLTV rating:")
    assert "64.3%" in line and "35.7%" in line
    assert "(27-15)" in line
    assert "Nigma Galaxy" in line
    assert "Team Spirit" in line


def test_parse_dltv_draft_vote_swaps_when_first_team_is_dire() -> None:
    """first_team/second_team are series slots; is_radiant decides map sides.

    Reproduces Xtreme (radiant/second_team) vs Liquid (dire/first_team):
    side_0=29 radiant, side_1=70 dire must label Xtreme 29% / Liquid 70%,
    not the reversed series-order names.
    """
    payload = {
        "match_id": 8896140998,
        "db": {
            "series": {
                "likes": {"side_0": 29, "side_1": 70},
                "is_draft_voting": True,
            },
            "first_team": {"title": "Team Liquid", "is_radiant": False},
            "second_team": {"title": "Xtreme Gaming", "is_radiant": True},
        },
    }
    vote = runtime._parse_dltv_draft_vote_from_live_payload(payload)
    assert vote is not None
    assert vote["radiant_team"] == "Xtreme Gaming"
    assert vote["dire_team"] == "Team Liquid"
    assert vote["radiant_likes"] == 29
    assert vote["dire_likes"] == 70
    assert vote["radiant_pct"] == 29.3
    assert vote["dire_pct"] == 70.7
    line = runtime._format_dltv_rating_line(vote)
    assert line.startswith("DLTV rating: Xtreme Gaming 29.3%")
    assert "Team Liquid 70.7%" in line
    assert "(29-70)" in line


def test_format_dltv_rating_no_votes_and_unavailable() -> None:
    assert runtime._format_dltv_rating_line({"radiant_likes": 0, "dire_likes": 0}) == (
        "DLTV rating: no votes"
    )
    assert runtime._format_dltv_rating_line(None, unavailable=True) == (
        "DLTV rating: unavailable"
    )


def test_steam_id_from_dltv_json_url() -> None:
    assert runtime._steam_id_from_dltv_json_url("https://dltv.org/live/8891438996.json") == 8891438996
    assert runtime._steam_id_from_dltv_json_url("sourcetv://matches/8891438996") == 8891438996
    assert runtime._steam_id_from_dltv_json_url("https://cyberscore.live/matches/1") is None


def test_append_dltv_rating_replaces_previous() -> None:
    base = "СТАВКА НА Foo x1\nFoo VS Bar\nTime: 05:00"
    once = runtime._append_dltv_rating_line(base, "DLTV rating: A 60.0% / B 40.0% (3-2)")
    assert once.endswith("DLTV rating: A 60.0% / B 40.0% (3-2)")
    twice = runtime._append_dltv_rating_line(once, "DLTV rating: A 70.0% / B 30.0% (7-3)")
    assert twice.count("DLTV rating:") == 1
    assert twice.endswith("DLTV rating: A 70.0% / B 30.0% (7-3)")


def test_deliver_no_longer_appends_dltv_rating_footer(monkeypatch) -> None:
    """Футер убран: значение живёт star-метрикой в All-блоке, не подписью."""
    sent: List[str] = []
    monkeypatch.setattr(runtime, "DLTV_RATING_IN_SIGNAL", True, raising=False)
    monkeypatch.setattr(
        runtime,
        "_bookmaker_prepare_message_for_delivery",
        lambda _key, message: (message, True, "ok"),
    )
    monkeypatch.setattr(runtime, "_signal_fingerprint_try_reserve", lambda *_a, **_k: (True, None))
    monkeypatch.setattr(runtime, "_signal_fingerprint_mark_sent", lambda *_a, **_k: None)
    monkeypatch.setattr(runtime, "add_url", lambda *_a, **_k: None)
    monkeypatch.setattr(
        runtime,
        "send_message",
        lambda message, **_kwargs: sent.append(str(message)),
    )
    monkeypatch.setattr(
        runtime,
        "_resolve_dltv_draft_vote_for_dispatch",
        lambda *_a, **_k: (
            {
                "radiant_likes": 10,
                "dire_likes": 5,
                "radiant_pct": 66.7,
                "dire_pct": 33.3,
                "radiant_team": "Radiant",
                "dire_team": "Dire",
            },
            "fetch",
        ),
    )

    ok = runtime._deliver_and_persist_signal(
        "dltv.org/matches/test-dltv-rating.0",
        "СТАВКА НА Radiant x1\nRadiant VS Dire\n0-0",
        add_url_reason="unit_test_dltv_rating",
        skip_bookmaker_prepare=True,
        json_url="https://dltv.org/live/123.json",
    )
    assert ok is True
    assert len(sent) == 1
    assert "DLTV rating:" not in sent[0]
    assert "66.7%" not in sent[0]


def test_deliver_skips_dltv_rating_when_disabled(monkeypatch) -> None:
    sent: List[str] = []
    monkeypatch.setattr(runtime, "DLTV_RATING_IN_SIGNAL", False, raising=False)
    monkeypatch.setattr(runtime, "_signal_fingerprint_try_reserve", lambda *_a, **_k: (True, None))
    monkeypatch.setattr(runtime, "_signal_fingerprint_mark_sent", lambda *_a, **_k: None)
    monkeypatch.setattr(runtime, "add_url", lambda *_a, **_k: None)
    monkeypatch.setattr(
        runtime,
        "send_message",
        lambda message, **_kwargs: sent.append(str(message)),
    )
    called = {"n": 0}

    def _boom(*_a: Any, **_k: Any) -> Any:
        called["n"] += 1
        raise AssertionError("should not fetch when disabled")

    monkeypatch.setattr(runtime, "_resolve_dltv_draft_vote_for_dispatch", _boom)

    runtime._deliver_and_persist_signal(
        "dltv.org/matches/test-dltv-off.0",
        "СТАВКА НА Radiant x1\nbody",
        add_url_reason="unit_test_dltv_off",
        skip_bookmaker_prepare=True,
    )
    assert called["n"] == 0
    assert "DLTV rating:" not in sent[0]


# --- dltv_rating как STAR-метрика All-блока ---------------------------------


def _vote(radiant_pct: float, total: float = 100.0) -> dict:
    r_likes = round(total * radiant_pct / 100.0, 3)
    return {
        "radiant_likes": r_likes,
        "dire_likes": round(total - r_likes, 3),
        "radiant_pct": radiant_pct,
        "dire_pct": round(100.0 - radiant_pct, 1),
        "radiant_team": "Radiant",
        "dire_team": "Dire",
    }


@pytest.mark.parametrize(
    "radiant_pct, expected",
    [(80.0, 30.0), (20.0, -30.0), (73.5, 23.5), (26.5, -23.5), (50.0, None)],
)
def test_dltv_rating_star_value_is_pct_minus_50(monkeypatch, radiant_pct, expected) -> None:
    monkeypatch.setattr(runtime, "DLTV_RATING_IN_SIGNAL", True, raising=False)
    monkeypatch.setattr(
        runtime,
        "_resolve_dltv_draft_vote_for_dispatch",
        lambda *_a, **_k: (_vote(radiant_pct), "live_data"),
    )
    assert runtime._dltv_rating_star_value("key") == expected


def test_dltv_rating_star_metric_written_into_all_block(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "DLTV_RATING_IN_SIGNAL", True, raising=False)
    monkeypatch.setattr(
        runtime,
        "_resolve_dltv_draft_vote_for_dispatch",
        lambda *_a, **_k: (_vote(84.0), "live_data"),
    )
    all_output: dict = {"counterpick_1vs1": 5}
    assert runtime._apply_dltv_rating_star_metric(all_output, "key") == 34.0
    assert all_output["dltv_rating"] == 34.0


def test_dltv_rating_star_metric_absent_when_disabled(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "DLTV_RATING_IN_SIGNAL", False, raising=False)

    def _boom(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("should not resolve the vote when disabled")

    monkeypatch.setattr(runtime, "_resolve_dltv_draft_vote_for_dispatch", _boom)
    all_output: dict = {}
    assert runtime._apply_dltv_rating_star_metric(all_output, "key") is None
    assert "dltv_rating" not in all_output


@pytest.mark.parametrize("value", [30.0, -30.0, 44.9])
def test_dltv_rating_hits_star_at_abs30_with_wr65(value) -> None:
    hits = runtime._collect_star_hits_for_block({"dltv_rating": value}, "all_output")
    assert [hit["metric"] for hit in hits] == ["dltv_rating"]
    # Порог задан только на WR60/WR65 → максимальный пройденный уровень всегда 65.
    assert hits[0]["wr_level"] == 65
    assert hits[0]["value"] == value


@pytest.mark.parametrize("value", [29.9, -29.9, 0.0, 12.0])
def test_dltv_rating_below_abs30_is_not_a_star_hit(value) -> None:
    assert runtime._collect_star_hits_for_block({"dltv_rating": value}, "all_output") == []


def test_dltv_rating_not_star_in_early_and_late_blocks() -> None:
    for section in ("early_output", "mid_output"):
        assert runtime._collect_star_hits_for_block({"dltv_rating": 40.0}, section) == []


def test_dltv_rating_marked_in_all_block_display() -> None:
    decorated = runtime._decorate_star_block_for_display(
        raw_block={"dltv_rating": 31.0, "counterpick_1vs1": 1},
        section="all_output",
        target_wr=60,
    )
    assert str(decorated["dltv_rating"]).endswith("*")
    assert not str(decorated["counterpick_1vs1"]).endswith("*")


def test_dltv_rating_summary_label_and_line() -> None:
    block = runtime._build_star_hits_summary_block(
        early_output={},
        mid_output={},
        all_output={"dltv_rating": -33.0},
    )
    assert "⭐ Star hits (WR60+):" in block
    # DLTV — метрика внешнего источника, поэтому в сводке она идёт строкой Mix,
    # а не внутри All. STAR-принадлежность при этом прежняя: хит считается по
    # секции all_output, просто показывается отдельно.
    assert "Mix: DLTV_rating -33 (WR65)" in block
    assert "All: DLTV_rating" not in block


def test_dltv_rating_alone_validates_all_block_at_wr60() -> None:
    diag = runtime._star_block_diagnostics(
        raw_block={"dltv_rating": 32.0},
        target_wr=60,
        section="all_output",
    )
    assert diag["hit_metrics"] == ["dltv_rating"]
    assert diag["sign"] == 1


def test_dltv_rating_alone_validates_realistic_all_block() -> None:
    """Драфт-метрики есть, но ниже порога — единственный хит DLTV делает блок STAR."""
    diag = runtime._star_block_diagnostics(
        raw_block={
            "counterpick_1vs1": 2,
            "counterpick_1vs2": 1,
            "solo": 2,
            "dota2protracker_cp1vs1": 1,
            "dltv_rating": 33.0,
        },
        target_wr=60,
        section="all_output",
    )
    assert diag["valid"] is True
    assert diag["hit_metrics"] == ["dltv_rating"]
    assert diag["sign"] == 1


@pytest.mark.parametrize("value", ["30.0*", "33.0*", "45.0*", "-40.0*"])
def test_dltv_rating_block_wr_is_pinned_to_65(value) -> None:
    """WR блока по DLTV всегда 65, а не 60 из-за плато порогов 30/30."""
    rec = runtime._recommend_odds_for_block({"dltv_rating": value}, "all")
    assert rec is not None
    assert rec["level"] == 65
    assert rec["wr_pct"] == 65.0
    assert rec["min_odds"] == 1.54


def test_dltv_rating_below_threshold_gives_no_block_recommendation() -> None:
    assert runtime._recommend_odds_for_block({"dltv_rating": "29.9*"}, "all") is None


def test_fixed_level_does_not_touch_other_metrics() -> None:
    assert runtime._recommend_odds_for_block({"counterpick_1vs1": "6.0*"}, "all")["level"] == 65
    assert runtime._recommend_odds_for_block({"counterpick_1vs1": "11.0*"}, "all")["level"] == 75


@pytest.mark.parametrize(
    "radiant_likes, dire_likes, expected",
    [
        (1, 0, None),      # 100/0 от одного голоса — не сигнал
        (0, 3, None),      # 0/100 от трёх голосов — не сигнал
        (8, 1, None),      # total=9 < 10
        (9, 1, 40.0),      # total=10 — граница проходит
        (84, 16, 34.0),    # нормальное голосование
    ],
)
def test_dltv_rating_requires_min_votes(monkeypatch, radiant_likes, dire_likes, expected) -> None:
    monkeypatch.setattr(runtime, "DLTV_RATING_IN_SIGNAL", True, raising=False)
    monkeypatch.setattr(runtime, "DLTV_RATING_MIN_VOTES", 10, raising=False)
    total = radiant_likes + dire_likes
    monkeypatch.setattr(
        runtime,
        "_resolve_dltv_draft_vote_for_dispatch",
        lambda *_a, **_k: (
            {
                "radiant_likes": radiant_likes,
                "dire_likes": dire_likes,
                "radiant_pct": round(100.0 * radiant_likes / total, 1) if total else None,
                "radiant_team": "Radiant",
                "dire_team": "Dire",
            },
            "fetch",
        ),
    )
    assert runtime._dltv_rating_star_value("min-votes-key") == expected


def test_min_votes_gate_is_env_tunable(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "DLTV_RATING_IN_SIGNAL", True, raising=False)
    monkeypatch.setattr(
        runtime,
        "_resolve_dltv_draft_vote_for_dispatch",
        lambda *_a, **_k: (_vote(84.0, total=12.0), "fetch"),
    )
    monkeypatch.setattr(runtime, "DLTV_RATING_MIN_VOTES", 10, raising=False)
    runtime._DLTV_RATING_STAR_CACHE.clear()
    assert runtime._dltv_rating_star_value("tunable-key") == 34.0
    monkeypatch.setattr(runtime, "DLTV_RATING_MIN_VOTES", 20, raising=False)
    runtime._DLTV_RATING_STAR_CACHE.clear()
    assert runtime._dltv_rating_star_value("tunable-key") is None


def test_payload_without_likes_block_is_not_a_vote() -> None:
    """sourcetv-payload не должен выглядеть как голосование 0-0.

    Иначе мнимое «нет голосов» перебивает HTTP-резолв, в котором голоса и лежат.
    """
    assert runtime._parse_dltv_draft_vote_from_live_payload({"db": {"series": {}}}) is None
    assert runtime._parse_dltv_draft_vote_from_live_payload({"radiant_lead": 500}) is None
    genuine = runtime._parse_dltv_draft_vote_from_live_payload(
        {"db": {"series": {"likes": {"side_0": 0, "side_1": 0}}}}
    )
    assert genuine is not None
    assert genuine["radiant_likes"] == 0
    assert genuine["radiant_pct"] is None


def test_dltv_rating_resolved_via_http_when_live_data_has_no_votes(monkeypatch) -> None:
    """Полный путь: sourcetv live_data без голосов -> fetch -> метрика в All-блоке."""
    monkeypatch.setattr(runtime, "DLTV_RATING_IN_SIGNAL", True, raising=False)
    runtime._DLTV_RATING_STAR_CACHE.clear()
    calls = {"n": 0}

    def _fetch(_steam_id: int) -> dict:
        calls["n"] += 1
        return {
            "match_id": 8924057168,
            "db": {
                "series": {"likes": {"side_0": 84, "side_1": 16}, "is_draft_voting": 1},
                "first_team": {"title": "BetBoom Team", "is_radiant": True},
                "second_team": {"title": "LGD Gaming", "is_radiant": False},
            },
        }

    monkeypatch.setattr(runtime, "_fetch_dltv_live_json_for_rating", _fetch)
    all_output = {"counterpick_1vs1": 2, "solo": 2}
    value = runtime._apply_dltv_rating_star_metric(
        all_output,
        "dltv.org/matches/8924057168.15",
        json_url="https://dltv.org/live/8924057168.json",
        live_data={"radiant_lead": 500, "game_time": 1200},
        steam_id=8924057168,
    )
    assert value == 34.0
    assert all_output["dltv_rating"] == 34.0
    assert calls["n"] == 1
    runtime._DLTV_RATING_STAR_CACHE.clear()


def test_dltv_rating_value_is_cached_within_ttl(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "DLTV_RATING_IN_SIGNAL", True, raising=False)
    monkeypatch.setattr(runtime, "DLTV_RATING_STAR_TTL_SECONDS", 90.0, raising=False)
    runtime._DLTV_RATING_STAR_CACHE.clear()
    calls = {"n": 0}

    def _resolve(*_a: Any, **_k: Any):
        calls["n"] += 1
        return _vote(84.0), "fetch"

    monkeypatch.setattr(runtime, "_resolve_dltv_draft_vote_for_dispatch", _resolve)
    key = "dltv.org/matches/cache-test.1"
    assert runtime._dltv_rating_star_value(key) == 34.0
    assert runtime._dltv_rating_star_value(key) == 34.0
    assert calls["n"] == 1
    runtime._DLTV_RATING_STAR_CACHE.clear()


def test_dltv_rating_negative_result_is_cached_too(monkeypatch) -> None:
    """Матчи без голосования не должны дёргать HTTP каждый цикл."""
    monkeypatch.setattr(runtime, "DLTV_RATING_IN_SIGNAL", True, raising=False)
    monkeypatch.setattr(runtime, "DLTV_RATING_STAR_TTL_SECONDS", 90.0, raising=False)
    runtime._DLTV_RATING_STAR_CACHE.clear()
    calls = {"n": 0}

    def _resolve(*_a: Any, **_k: Any):
        calls["n"] += 1
        return {"radiant_likes": 0, "dire_likes": 0, "radiant_pct": None}, "fetch"

    monkeypatch.setattr(runtime, "_resolve_dltv_draft_vote_for_dispatch", _resolve)
    key = "dltv.org/matches/no-votes.1"
    assert runtime._dltv_rating_star_value(key) is None
    assert runtime._dltv_rating_star_value(key) is None
    assert calls["n"] == 1
    runtime._DLTV_RATING_STAR_CACHE.clear()


def test_dltv_rating_alone_passes_single_block_min_wr_gate() -> None:
    rec = runtime._recommend_odds_for_block({"dltv_rating": "33.0*"}, "all")
    gate = runtime._single_block_star_min_wr_gate(
        has_selected_early_star=False,
        has_selected_late_star=False,
        has_selected_all_star=True,
        early_wr_pct=None,
        late_wr_pct=None,
        all_wr_pct=rec["wr_pct"],
    )
    assert gate["active"] is True
    assert gate["min_wr_ok"] is True
    assert gate["valid"] is True
