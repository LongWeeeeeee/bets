"""DLTv-live фолбэк для карточек Winline без моста (E-273 продолжение).

Живые захваты 10.09.2026 ~15:35 UTC:
- base/tests/fixtures/dltv_series_snapshot_20260910.json — series.json,
  обрезан до 3 live-серий (zero-tenacity-vs-devil-kings, team-daxak-vs-team-
  recrent, cyber-nova-vs-dawn-bulls). Команда захвата:
  curl -s "https://dltv.org/live/series.json" -H "User-Agent: Mozilla/5.0".
- base/tests/fixtures/dltv_live_8991962355_20260910.json — live/8991962355.json
  (Zero Tenacity vs Devil Kings, map1, 10-9, 828s; убраны charts/canvas).
"""
import copy
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import cyberscore_try as cs

FIX_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


def _load(name):
    with open(os.path.join(FIX_DIR, name), encoding="utf-8") as fh:
        return json.load(fh)


@pytest.fixture(scope="module")
def series_snapshot():
    return _load("dltv_series_snapshot_20260910.json")


@pytest.fixture(scope="module")
def live_payload():
    return _load("dltv_live_8991962355_20260910.json")


def test_slugify_teams():
    assert cs._dltv_slugify_team("Zero Tenacity") == "zero-tenacity"
    assert cs._dltv_slugify_team("Team Daxak") == "team-daxak"
    assert cs._dltv_slugify_team("  PuckChamp  ") == "puckchamp"
    assert cs._dltv_slugify_team("") == ""


def test_find_live_series_zero_tenacity(series_snapshot):
    found = cs._dltv_find_live_series(
        "Zero Tenacity", "Devil Kings", series_snapshot)
    assert found is not None
    assert found["match_id"] == "8991962355"
    assert found["series_id"] == "427967"
    assert "blast-slam-9" in found["league_slug"]


def test_find_live_series_reversed_order(series_snapshot):
    found = cs._dltv_find_live_series(
        "Devil Kings", "Zero Tenacity", series_snapshot)
    assert found is not None
    assert found["match_id"] == "8991962355"


def test_find_live_series_daxak_recrent(series_snapshot):
    found = cs._dltv_find_live_series(
        "Team Daxak", "Team Recrent", series_snapshot)
    assert found is not None
    assert found["match_id"] == "8991822559"


def test_find_live_series_unknown_pair_returns_none(series_snapshot):
    assert cs._dltv_find_live_series(
        "Natus Vincere", "Team Spirit", series_snapshot) is None
    assert cs._dltv_find_live_series("", "", series_snapshot) is None
    assert cs._dltv_find_live_series("Zero Tenacity", "Devil Kings", {}) is None


def test_parse_live_draft_5v5(live_payload):
    draft = cs._dltv_parse_live_draft(live_payload)
    assert draft is not None
    assert draft["source"] == "dltv"
    assert draft["map_num"] == 1
    assert draft["game_time_s"] == 828
    assert draft["first"]["title"] == "Zero Tenacity"
    assert draft["second"]["title"] == "Devil Kings"
    for side in ("first", "second"):
        heroes = draft[side]["heroes"]
        assert len(heroes) == 5
        assert all(h["hero_id"] > 0 for h in heroes)
        assert all(h["player"] for h in heroes)


def test_parse_live_draft_requires_picks_ended(live_payload):
    payload = copy.deepcopy(live_payload)
    payload["is_picks_ended"] = False
    assert cs._dltv_parse_live_draft(payload) is None


def test_parse_live_draft_requires_full_5v5(live_payload):
    payload = copy.deepcopy(live_payload)
    payload["fast_picks"]["first_team"] = payload["fast_picks"]["first_team"][:4]
    assert cs._dltv_parse_live_draft(payload) is None
    payload = copy.deepcopy(live_payload)
    payload["fast_picks"]["second_team"][0]["hero_id"] = 0
    assert cs._dltv_parse_live_draft(payload) is None


def test_end_to_end_card_lookup(series_snapshot, live_payload):
    def snap_fetcher(url, timeout):
        assert url == cs.DLTV_LIVE_SERIES_URL
        return series_snapshot

    def match_fetcher(url, timeout):
        assert "8991962355" in url
        return live_payload

    draft = cs.dltv_live_draft_for_card(
        "Zero Tenacity", "Devil Kings", 1,
        snapshot_fetcher=snap_fetcher, match_fetcher=match_fetcher)
    assert draft is not None
    assert draft["map_num"] == 1
    assert len(draft["first"]["heroes"]) == 5
    assert len(draft["second"]["heroes"]) == 5


def test_end_to_end_map_mismatch_refuses(series_snapshot, live_payload):
    def snap_fetcher(url, timeout):
        return series_snapshot

    def match_fetcher(url, timeout):
        return live_payload

    # DLTv показывает карту 1 — клеить её к ряду карты 2 запрещено.
    assert cs.dltv_live_draft_for_card(
        "Zero Tenacity", "Devil Kings", 2,
        snapshot_fetcher=snap_fetcher, match_fetcher=match_fetcher) is None


def test_end_to_end_no_series_returns_none(series_snapshot, live_payload):
    def snap_fetcher(url, timeout):
        return series_snapshot

    def match_fetcher(url, timeout):
        raise AssertionError("match fetch must not happen without series")

    assert cs.dltv_live_draft_for_card(
        "Natus Vincere", "Team Spirit", 1,
        snapshot_fetcher=snap_fetcher, match_fetcher=match_fetcher) is None


def _notify_env(monkeypatch, live_payload):
    monkeypatch.setattr(cs, "_winline_odds_notify_enabled", lambda: True)
    monkeypatch.setattr(
        cs, "_winline_bridge_owns_card_pair", lambda *a, **k: False)
    monkeypatch.setattr(cs, "_winline_dltv_draft_sent", {})
    return lambda url, timeout: live_payload


def test_format_dltv_draft_message(series_snapshot, live_payload):
    draft = cs._dltv_parse_live_draft(live_payload)
    draft["league_slug"] = "blast-slam-9-europe-open-qualifier-2"
    text = cs._winline_format_dltv_draft_message(
        league="BLAST Slam 9", team1="Zero Tenacity",
        team2="Devil Kings", draft=draft)
    assert "карта 1" in text
    assert "Zero Tenacity — Devil Kings" in text
    assert "Zero Tenacity:" in text and "Devil Kings:" in text
    assert "8991962355" in text
    assert "13:48" in text  # 828s
    assert "10–9" in text


def test_card_notify_sends_once_and_dedupes(
        monkeypatch, series_snapshot, live_payload):
    match_fetcher = _notify_env(monkeypatch, live_payload)
    sent = []

    def _fake_send(message, **kwargs):
        sent.append(message)
        return True

    ok = cs._winline_card_dltv_draft_notify(
        league="BLAST Slam 9", team1="Zero Tenacity", team2="Devil Kings",
        map_num=1, series_key="winline:league:blast|zero|devil",
        send_fn=_fake_send, snapshot=series_snapshot,
        match_fetcher=match_fetcher)
    assert ok is True
    assert len(sent) == 1
    assert "Zero Tenacity" in sent[0] and "Devil Kings" in sent[0]
    # Повтор — дедуп, второй отправки нет, но результат True.
    ok2 = cs._winline_card_dltv_draft_notify(
        league="BLAST Slam 9", team1="Zero Tenacity", team2="Devil Kings",
        map_num=1, series_key="winline:league:blast|zero|devil",
        send_fn=_fake_send, snapshot=series_snapshot,
        match_fetcher=match_fetcher)
    assert ok2 is True
    assert len(sent) == 1


def test_card_notify_skips_when_bridge_owns(
        monkeypatch, series_snapshot, live_payload):
    _notify_env(monkeypatch, live_payload)
    monkeypatch.setattr(
        cs, "_winline_bridge_owns_card_pair", lambda *a, **k: True)
    sent = []
    assert cs._winline_card_dltv_draft_notify(
        league="L", team1="Zero Tenacity", team2="Devil Kings", map_num=1,
        send_fn=lambda message, **k: sent.append(message) or True,
        snapshot=series_snapshot,
        match_fetcher=lambda url, timeout: live_payload) is False
    assert sent == []


def test_card_notify_respects_notify_gate(
        monkeypatch, series_snapshot, live_payload):
    monkeypatch.setattr(cs, "_winline_odds_notify_enabled", lambda: False)
    assert cs._winline_card_dltv_draft_notify(
        league="L", team1="Zero Tenacity", team2="Devil Kings", map_num=1,
        snapshot=series_snapshot,
        match_fetcher=lambda url, timeout: live_payload) is False


def test_sweep_counts_dltv_draft(monkeypatch):
    from pathlib import Path
    snap_path = (Path(__file__).resolve().parent / "fixtures"
                 / "winline_overview_snapshot_20260910.json")
    snap = json.loads(snap_path.read_text(encoding="utf-8"))
    cs._winline_overview_inject_for_tests(snap["text"], html=snap["html"])
    monkeypatch.setattr(cs, "ensure_winline_current_map_polling",
                        lambda **kw: True, raising=False)
    monkeypatch.setattr(cs, "_winline_current_map_pollers", {}, raising=False)
    calls = []

    def _fake_notify(**kwargs):
        calls.append(kwargs)
        return True

    monkeypatch.setattr(cs, "_winline_card_dltv_draft_notify",
                        _fake_notify, raising=False)
    summary = cs._winline_sweep_cards_from_snapshot()
    assert calls, "sweep must consult DLTv-draft hook for priced rows"
    assert summary.get("dltv_draft") == len(calls)
    assert all("map_num" in kw and "team1" in kw for kw in calls)
