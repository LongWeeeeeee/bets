"""DLTV draft-vote rating footer on signal dispatch."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, List

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402


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


def test_deliver_and_persist_appends_dltv_rating(monkeypatch) -> None:
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
    assert "DLTV rating:" in sent[0]
    assert "66.7%" in sent[0]
    assert sent[0].strip().endswith(")")


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
