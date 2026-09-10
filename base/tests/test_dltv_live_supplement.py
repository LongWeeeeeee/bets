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
    monkeypatch.setattr(cs, "_winline_dltv_draft_sent", {})
    monkeypatch.setattr(cs, "_winline_dltv_draft_seeded", False)
    monkeypatch.setattr(cs, "_winline_map_clocks", {})
    return lambda url, timeout: live_payload


def _norm_pair(a, b):
    return frozenset({
        cs._winline_normalized_team_identity(a),
        cs._winline_normalized_team_identity(b),
    })


def test_format_dltv_draft_message_negative_lobby_clock(live_payload):
    # Прод 10.09.2026, ZT-DK map2: game_time_s=-112 до горна → «—», не -1:52.
    draft = cs._dltv_parse_live_draft(live_payload)
    draft["league_slug"] = "blast-slam-9-europe-open-qualifier-2"
    draft["game_time_s"] = -112
    text = cs._winline_format_dltv_draft_message(
        league="BLAST Slam 9", team1="Zero Tenacity",
        team2="Devil Kings", draft=draft)
    assert "-1:52" not in text
    assert "· — ·" in text


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
    assert ok == "sent"
    assert len(sent) == 1
    assert "Zero Tenacity" in sent[0] and "Devil Kings" in sent[0]
    # Повтор — дедуп, второй отправки нет.
    ok2 = cs._winline_card_dltv_draft_notify(
        league="BLAST Slam 9", team1="Zero Tenacity", team2="Devil Kings",
        map_num=1, series_key="winline:league:blast|zero|devil",
        send_fn=_fake_send, snapshot=series_snapshot,
        match_fetcher=match_fetcher)
    assert ok2 == "duplicate"
    assert len(sent) == 1


def test_dedupe_survives_restart_via_journal(
        monkeypatch, tmp_path, series_snapshot, live_payload):
    # Прод 10.09.2026: один драфт ZT-DK map2 ушёл дважды (19:22 и 19:29)
    # через рестарт 19:24 — in-memory дедуп обнулился. Журнал чинит это.
    match_fetcher = _notify_env(monkeypatch, live_payload)
    journal = tmp_path / "winline_telegram_sent.jsonl"
    key = "winline:league:blast|zero|devil|map1|8991962355"
    journal.write_text(
        json.dumps({"wall": 1.0, "kind": "dltv_draft",
                    "canonical_key": key, "delivered": True,
                    "message": "m"}) + "\n"
        + json.dumps({"wall": 2.0, "kind": "first",
                      "canonical_key": "other", "delivered": True,
                      "message": "x"}) + "\n",
        encoding="utf-8")
    monkeypatch.setattr(cs, "_winline_sent_journal_path", lambda: journal)
    sent = []
    ok = cs._winline_card_dltv_draft_notify(
        league="BLAST Slam 9", team1="Zero Tenacity", team2="Devil Kings",
        map_num=1, series_key="winline:league:blast|zero|devil",
        send_fn=lambda message, **k: sent.append(message) or True,
        snapshot=series_snapshot, match_fetcher=match_fetcher,
        bridge_live_pairs=set())
    assert ok == "duplicate"
    assert sent == []


def test_card_notify_skips_draft_when_bridge_sees_live(
        monkeypatch, series_snapshot, live_payload):
    match_fetcher = _notify_env(monkeypatch, live_payload)
    sent = []
    key = "winline:league:blast|devil kings|zero tenacity"
    ok = cs._winline_card_dltv_draft_notify(
        league="L", team1="Zero Tenacity", team2="Devil Kings", map_num=1,
        series_key=key,
        send_fn=lambda message, **k: sent.append(message) or True,
        snapshot=series_snapshot, match_fetcher=match_fetcher,
        bridge_live_pairs={_norm_pair("Zero Tenacity", "Devil Kings")})
    assert ok == "none"
    assert sent == []
    # Часы при этом отмечаются: 🕐/💰 чинятся независимо от драфта.
    clock = cs._winline_map_clocks.get(key)
    assert isinstance(clock, dict) and clock.get("source") == "dltv"
    assert clock.get("map_num") == 1
    assert clock.get("game_time") == 828


def test_parse_live_clock_names_and_lead(live_payload):
    clock = cs._dltv_parse_live_clock(live_payload)
    assert clock is not None
    assert clock["map_num"] == 1
    assert clock["game_time"] == 828
    assert clock["live"] is True
    assert clock["radiant_name"] == "Zero Tenacity"
    assert clock["dire_name"] == "Devil Kings"
    assert isinstance(clock["radiant_lead"], int)


def test_parse_live_clock_unmapped_sides_keep_time(live_payload):
    payload = copy.deepcopy(live_payload)
    payload["db"]["first_team"]["id"] = 424242
    clock = cs._dltv_parse_live_clock(payload)
    assert clock is not None
    assert clock["game_time"] == 828
    assert clock["radiant_name"] == ""
    assert clock["dire_name"] == "Devil Kings"


def test_parse_live_clock_requires_game_time(live_payload):
    payload = copy.deepcopy(live_payload)
    payload["game_time"] = 0
    assert cs._dltv_parse_live_clock(payload) is None


def _draft_stage_payload():
    # NAVI–KLIM 10.09.2026: лобби тикает game_time, драфт не завершён.
    return {
        "match_id": 8992034384,
        "game_time": 327,
        "radiant_score": 0,
        "dire_score": 0,
        "radiant_lead": 0,
        "is_picks_ended": None,
        "fast_picks": {"first_team": [], "second_team": []},
        "players": [],
        "db": {
            "first_team": {"id": 58, "title": "Natus Vincere"},
            "second_team": {"id": 7031, "title": "KLIM SANI4"},
            "scores": {"first_team": 0, "second_team": 0},
            "series": {"slug": "natus-vincere-vs-klim-sani4-epl-masters-2"},
        },
    }


def test_parse_live_clock_refuses_draft_stage():
    assert cs._dltv_parse_live_clock(_draft_stage_payload()) is None


def test_parse_live_clock_distrusts_picks_flag_alone():
    # Прод 10.09.2026: is_picks_ended=True, fast_picks пусты, 0–0.
    payload = _draft_stage_payload()
    payload["is_picks_ended"] = True
    payload["game_time"] = 447
    assert cs._dltv_parse_live_clock(payload) is None


def test_parse_live_clock_allows_score_without_flag():
    payload = _draft_stage_payload()
    payload["radiant_score"] = 1
    clock = cs._dltv_parse_live_clock(payload)
    assert clock is not None
    assert clock["game_time"] == 327
    assert clock["map_num"] == 1


def _draft_snap():
    return {"live": {"8992034384": 427891}, "upcoming": [{
        "id": 427891, "status": 1,
        "slug": "natus-vincere-vs-klim-sani4-epl-masters-2",
    }], "results": []}


def _no_send(message, **kwargs):
    raise AssertionError("no draft must send on draft stage")


def test_notify_resets_clock_on_draft_stage(
        monkeypatch, series_snapshot):
    _notify_env(monkeypatch, None)
    key = "winline:league:epl masters|klim sani4|natus vincere"
    cs._winline_map_clocks[key] = {
        "game_time": 207.0, "wall": 0.0, "map_num": 1, "live": True,
        "radiant_lead": 0, "radiant_name": "", "dire_name": "",
        "source": "dltv",
    }

    def _fetch(url, timeout):
        assert "8992034384" in url
        return _draft_stage_payload()

    ok = cs._winline_card_dltv_draft_notify(
        league="EPL Masters", team1="Natus Vincere", team2="KLIM SANI4",
        map_num=1, series_key=key, send_fn=_no_send,
        snapshot=_draft_snap(), match_fetcher=_fetch,
        bridge_live_pairs=set())
    assert ok == "none"
    assert key not in cs._winline_map_clocks
    assert cs._winline_map_clock_label(key + "|map1|Natus Vincere|KLIM SANI4") == "—"


def test_notify_keeps_prior_map_clock_on_draft_stage(
        monkeypatch, series_snapshot):
    _notify_env(monkeypatch, None)
    key = "winline:league:epl masters|klim sani4|natus vincere"
    frozen = {
        "game_time": 2400.0, "wall": 0.0, "map_num": 1, "live": False,
        "radiant_lead": 3000, "radiant_name": "Natus Vincere",
        "dire_name": "KLIM SANI4", "source": "dltv",
    }
    cs._winline_map_clocks[key] = dict(frozen)
    payload = _draft_stage_payload()
    payload["db"]["scores"] = {"first_team": 1, "second_team": 0}

    def _fetch(url, timeout):
        return payload

    ok = cs._winline_card_dltv_draft_notify(
        league="EPL Masters", team1="Natus Vincere", team2="KLIM SANI4",
        map_num=2, series_key=key, send_fn=_no_send,
        snapshot=_draft_snap(), match_fetcher=_fetch,
        bridge_live_pairs=set())
    assert ok == "none"
    assert cs._winline_map_clocks[key] == frozen


def test_bridge_live_pairs_from_snapshot_file(tmp_path):
    import time as _time
    rows = {
        "live_named": {
            "status": "live", "timestamp": _time.time() - 10,
            "game_time": 500.0,
            "radiant_team_name": "Zero Tenacity",
            "dire_team_name": "Devil Kings",
        },
        "stale": {
            "status": "live", "timestamp": _time.time() - 900,
            "game_time": 500.0,
            "radiant_team_name": "A", "dire_team_name": "B",
        },
        "anonymous": {
            "status": "live", "timestamp": _time.time() - 10,
            "game_time": 500.0,
            "radiant_team_name": "Zero Tenacity",
            "dire_team_name": "Dire",
        },
        "prematch": {
            "status": "draft", "timestamp": _time.time() - 10,
            "game_time": 0.0,
            "radiant_team_name": "C", "dire_team_name": "D",
        },
    }
    path = tmp_path / "sourcetv_matches.json"
    path.write_text(json.dumps(rows), encoding="utf-8")
    pairs = cs._winline_bridge_live_pairs(path=str(path))
    assert _norm_pair("Zero Tenacity", "Devil Kings") in pairs
    assert len(pairs) == 1


def test_bridge_live_pairs_missing_file_returns_empty(tmp_path):
    assert cs._winline_bridge_live_pairs(
        path=str(tmp_path / "absent.json")) == set()


def test_freeze_dltv_clock_on_series_gone(
        monkeypatch, series_snapshot, live_payload):
    match_fetcher = _notify_env(monkeypatch, live_payload)
    key = "winline:league:blast|devil kings|zero tenacity"
    cs._winline_map_clocks[key] = {
        "game_time": 800.0, "wall": 0.0, "map_num": 1, "live": True,
        "radiant_lead": 0, "radiant_name": "", "dire_name": "",
        "source": "dltv",
    }
    empty_snap = {"live": {}, "upcoming": [], "results": []}
    ok = cs._winline_card_dltv_draft_notify(
        league="L", team1="Zero Tenacity", team2="Devil Kings", map_num=1,
        series_key=key, send_fn=lambda message, **k: True,
        snapshot=empty_snap, match_fetcher=match_fetcher,
        bridge_live_pairs=set())
    assert ok == "none"
    assert cs._winline_map_clocks[key]["live"] is False


def test_card_notify_respects_notify_gate(
        monkeypatch, series_snapshot, live_payload):
    monkeypatch.setattr(cs, "_winline_odds_notify_enabled", lambda: False)
    assert cs._winline_card_dltv_draft_notify(
        league="L", team1="Zero Tenacity", team2="Devil Kings", map_num=1,
        snapshot=series_snapshot,
        match_fetcher=lambda url, timeout: live_payload) == "none"


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
        return "sent"

    monkeypatch.setattr(cs, "_winline_card_dltv_draft_notify",
                        _fake_notify, raising=False)
    summary = cs._winline_sweep_cards_from_snapshot()
    assert calls, "sweep must consult DLTv-draft hook for priced rows"
    assert summary.get("dltv_draft") == len(calls)
    assert all("map_num" in kw and "team1" in kw for kw in calls)
    assert all("bridge_live_pairs" in kw for kw in calls)
    # Дедуп не считается отправкой: счётчик только за свежие "sent".
    monkeypatch.setattr(cs, "_winline_card_dltv_draft_notify",
                        lambda **kw: "duplicate", raising=False)
    summary2 = cs._winline_sweep_cards_from_snapshot()
    assert "dltv_draft" not in summary2


def test_sweep_consults_dltv_hook_for_owned_rows(monkeypatch):
    """Хук достижим и для owned-рядов: часы чинятся и под мостом."""
    from pathlib import Path
    snap_path = (Path(__file__).resolve().parent / "fixtures"
                 / "winline_overview_snapshot_20260910.json")
    snap = json.loads(snap_path.read_text(encoding="utf-8"))
    cs._winline_overview_inject_for_tests(snap["text"], html=snap["html"])

    class _Active:
        def is_active(self):
            return True

    monkeypatch.setattr(
        cs, "_winline_current_map_pollers",
        {"sourcetv:league:20159|name:daxak club|name:recrent club"
         "|map3|Daxak Club|RECRENT CLUB": _Active()},
        raising=False)
    monkeypatch.setattr(cs, "ensure_winline_current_map_polling",
                        lambda **kw: True, raising=False)
    calls = []
    monkeypatch.setattr(cs, "_winline_card_dltv_draft_notify",
                        lambda **kw: calls.append(kw) or False,
                        raising=False)
    summary = cs._winline_sweep_cards_from_snapshot()
    owned_calls = [
        kw for kw in calls
        if {cs._winline_normalized_team_identity(kw["team1"]),
            cs._winline_normalized_team_identity(kw["team2"])}
        == {"daxak club", "recrent club"}
        and int(kw["map_num"]) == 3
    ]
    assert owned_calls, "owned map3 must still reach the DLTv hook"
    assert summary["skipped_owned"] >= 1
