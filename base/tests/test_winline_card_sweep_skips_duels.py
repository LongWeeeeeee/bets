"""Winline card sweep must not turn player-duel prop cards into pollers.

Input is a REAL captured Winline listing overview snapshot (``{wall, text, html}``),
captured 2026-10-08 18:34:37 MSK with::

    scp serv1:/root/main/runtime/winline_overview_snapshot.json \
        base/tests/fixtures/winline_overview_snapshot_20261008_blast_duel_cards.json

It holds three live player-duel prop cards ("BLAST Slam. Дуэль игроков. Убийства":
SKITER (TEAM AURORA) / MIKOTO (TEAM AURORA) / WS (TEAM AURORA) against PURE / BZM /
33 (1W)), one pre-match duel card (SATANIC vs WATSON) and one real match card
(BLAST Slam: PARIVISION vs TEAM YANDEX, pre-match at capture time).

The real production function ``_winline_sweep_cards_from_snapshot`` runs on the real
parser (``winline_enumerate_live_cards``) output.  Only what starts browser pollers
or touches the network is stubbed: ``ensure_winline_current_map_polling`` (the
created keys are captured), the DLTv series fetch and the DLTv draft notifier.
The real match card was pre-match at capture time, so the parser output is passed
through with that single card's ``live`` flag set (everything else untouched) to
prove that a real match card is still swept.
"""
from __future__ import annotations

import json
import sys
import time
import types
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

try:
    import keys  # noqa: F401,E402
except ImportError:
    _test_keys = types.ModuleType("keys")
    _test_keys.api_to_proxy = {}
    _test_keys.BOOKMAKER_PROXY_URL = None
    _test_keys.BOOKMAKER_PROXY_POOL = []
    _test_keys.DLTV_PROXY_POOL = []
    sys.modules["keys"] = _test_keys

import bookmaker_selenium_odds as odds_mod  # noqa: E402
import cyberscore_try as C  # noqa: E402

FIXTURE = (Path(__file__).parent / "fixtures"
           / "winline_overview_snapshot_20261008_blast_duel_cards.json")
REAL_LEAGUE = "BLAST Slam"
REAL_PAIR = ("PARIVISION", "TEAM YANDEX")
REAL_MAP = 1


@pytest.fixture
def sweep(monkeypatch):
    """Run the real sweep on the captured snapshot; return (run, created)."""
    monkeypatch.setenv(C.WINLINE_OVERVIEW_SNAPSHOT_ENV, str(FIXTURE))
    snapshot = json.loads(C._winline_overview_snapshot_path().read_text(encoding="utf-8"))
    monkeypatch.setitem(C._winline_overview_state, "html", snapshot["html"])
    monkeypatch.setitem(C._winline_overview_state, "fetched_at", time.time())

    created = []

    def fake_ensure(**kwargs):
        created.append(kwargs)
        return True

    real_enumerate = odds_mod.winline_enumerate_live_cards

    def enumerate_with_real_card_live(html):
        cards = real_enumerate(html)
        for card in cards:  # the real match card was pre-match at capture time
            if card.get("league") == REAL_LEAGUE:
                card["live"] = True
        return cards

    monkeypatch.setattr(odds_mod, "winline_enumerate_live_cards",
                        enumerate_with_real_card_live)
    monkeypatch.setattr(C, "ensure_winline_current_map_polling", fake_ensure)
    monkeypatch.setattr(C, "_winline_first_active", lambda: True)
    monkeypatch.setattr(C, "_dltv_live_series_snapshot", lambda *a, **k: None)
    monkeypatch.setattr(C, "_winline_card_dltv_draft_notify", lambda **k: None)
    monkeypatch.setattr(C, "_winline_bridge_live_pairs", lambda *a, **k: set())
    monkeypatch.setattr(C, "_winline_bridge_owns_card_pair", lambda *a, **k: False)
    monkeypatch.setattr(C, "_winline_prop_skip_logged_keys", set(), raising=False)
    monkeypatch.setattr(C, "WINLINE_CARD_SWEEP_MAX_CARDS", 6)
    return C._winline_sweep_cards_from_snapshot, created


def _series(created):
    return [str(c.get("series") or "") for c in created]


def _pairs(created):
    return sorted((c["team1"], c["team2"], c["map_num"]) for c in created)


def test_fixture_contains_duel_cards_and_real_match_card():
    """The capture really holds both kinds (otherwise the other tests prove nothing)."""
    snapshot = json.loads(FIXTURE.read_text(encoding="utf-8"))
    cards = odds_mod.winline_enumerate_live_cards(snapshot["html"])
    duels = [c for c in cards if "дуэль игроков" in str(c["league"]).lower()]
    real = [c for c in cards if c["league"] == REAL_LEAGUE]
    assert len(duels) == 4 and len(real) == 1
    assert (real[0]["team1"], real[0]["team2"]) == REAL_PAIR


def test_duel_cards_never_become_sweep_series_or_pollers(sweep):
    run, created = sweep
    summary = run()
    assert created, "the real match card must still be swept"
    assert not [s for s in _series(created) if "дуэль" in s.lower()], _series(created)
    assert not [c for c in created
                if "(" in c["team1"] or "(" in c["team2"]], _pairs(created)
    assert summary.get("skipped_props") == 4, summary
    # The real BLAST Slam match card is still swept, exactly this key.
    assert _pairs(created) == [(REAL_PAIR[0], REAL_PAIR[1], REAL_MAP)]
    assert _series(created) == [
        "winline:league:blast slam|parivision|team yandex"], _series(created)


def test_duel_cards_do_not_crowd_real_match_out_of_the_card_cap(sweep, monkeypatch):
    """The real card is 5th in the listing; with a cap of 4 the duels (4 of them,
    ahead of it) used to take every slot before the real card was looked at."""
    run, created = sweep
    monkeypatch.setattr(C, "WINLINE_CARD_SWEEP_MAX_CARDS", 4)
    run()
    assert _pairs(created) == [(REAL_PAIR[0], REAL_PAIR[1], REAL_MAP)]


def test_flag_off_restores_old_behaviour_and_proves_the_test_sees_duels(
        sweep, monkeypatch):
    run, created = sweep
    monkeypatch.setenv("WINLINE_CARD_SWEEP_SKIP_PROPS", "0")
    summary = run()
    duel_series = [s for s in _series(created) if "дуэль" in s.lower()]
    assert len(duel_series) == 3, _series(created)  # the 3 live duel cards, map 3
    assert {c["map_num"] for c in created if "(" in c["team1"]} == {3}
    assert "skipped_props" not in summary


def test_skip_is_logged_once_per_card_key(sweep, capsys):
    run, created = sweep
    run()
    run()
    out = capsys.readouterr().out
    lines = [l for l in out.splitlines() if "проп-карточка пропущена" in l]
    assert len(lines) == 4, lines  # 4 distinct duel cards, each logged once over 2 sweeps


# --- Review round D4 (astra R1/R2, hard-verifier F1) --------------------------------

import gzip  # noqa: E402
import re  # noqa: E402

from bs4 import BeautifulSoup  # noqa: E402

_FIXTURES = Path(__file__).parent / "fixtures"
_TOURNAMENT_TAG = "ww-feature-block-tournament-dsk"
_DUEL_IDS = {"eventId-16882197", "eventId-16882198", "eventId-16882199", "eventId-16889471"}
_REAL_ID = "eventId-16889364"
# The four captured overview pages (names fixed so a new fixture cannot silently change the claim).
_OVERVIEW_FIXTURES = (
    "winline_overview_legion_blasterbi_20261005.json",
    "winline_overview_snapshot_20260910.json",
    "winline_overview_snapshot_20261008_blast_duel_cards.json",
    "winline_unpriced_overview_20260915.json.gz",
)


def _load_html(name: str) -> str:
    path = _FIXTURES / name
    raw = gzip.open(path, "rt", encoding="utf-8").read() if name.endswith(".gz") \
        else path.read_text(encoding="utf-8")
    return json.loads(raw)["html"]


def _event_nodes(soup):
    return soup.find_all(id=re.compile(r"^eventId-\d+$"))


def _walk_reference_ids(soup) -> set:
    """The D3 document-order walk (header flag carried to the next header), as ids of eventIds."""
    found, current = set(), False
    for node in soup.descendants:
        if getattr(node, "name", None) is None:
            continue
        if any("block-tournament-header__title" in str(c) for c in (node.get("class") or [])):
            current = odds_mod.winline_league_is_prop_duel(node.get_text(" ", strip=True))
            continue
        if re.fullmatch(r"eventId-\d+", str(node.get("id") or "")) and current:
            found.add(node["id"])
    return found


def _prop_ids(html: str) -> set:
    snap = odds_mod._WinlineDOMSnapshot(html)
    return {n["id"] for n in _event_nodes(snap.soup)
            if id(n) in snap._prop_duel_event_ids()}


def test_r2_fixture_duel_ids_found_and_real_card_not():
    ids = _prop_ids(_load_html("winline_overview_snapshot_20261008_blast_duel_cards.json"))
    assert ids == _DUEL_IDS
    assert _REAL_ID not in ids


@pytest.mark.parametrize("name", _OVERVIEW_FIXTURES)
def test_r2_container_scoped_detection_matches_document_walk_on_all_overviews(name):
    """On every captured overview page each card's header lies in its own container: 0 disagreements."""
    html = _load_html(name)
    soup = BeautifulSoup(html, "html.parser")
    assert _event_nodes(soup), name
    assert all(n.find_parent(_TOURNAMENT_TAG) is not None for n in _event_nodes(soup)), name
    assert _prop_ids(html) == _walk_reference_ids(soup)


def test_r2_real_card_moved_into_a_later_headerless_container_is_not_a_duel():
    """A real card in a later container with no header of its own must not inherit the duel flag."""
    html = _load_html("winline_overview_snapshot_20261008_blast_duel_cards.json")
    soup = BeautifulSoup(html, "html.parser")
    real = soup.find(id=_REAL_ID)
    duel = soup.find(id="eventId-16882197")
    duel_block = duel.find_parent(_TOURNAMENT_TAG)
    assert duel_block is not None and real.find_parent(_TOURNAMENT_TAG) is not duel_block
    headerless = soup.new_tag(_TOURNAMENT_TAG)
    duel_block.insert_after(headerless)
    headerless.append(real.extract())
    moved = str(soup)

    # The document-order walk (the code before D4) marks the moved real card as a duel: control.
    assert _REAL_ID in _walk_reference_ids(BeautifulSoup(moved, "html.parser"))
    ids = _prop_ids(moved)
    assert _REAL_ID not in ids
    assert ids == _DUEL_IDS


def test_r2_card_without_any_tournament_container_is_not_a_duel():
    html = ('<div class="x"><span class="block-tournament-header__title">BLAST Slam. Дуэль игроков.'
            ' Убийства</span></div><div id="eventId-1">TEAM AURORA 1W</div>')
    assert _prop_ids(html) == set()


_DUEL_SECTION = ("DOTA 2 | BLAST Slam. Дуэль игроков. Убийства SKITER (TEAM AURORA) PURE (1W) "
                 "1 карта 14.5 1.72 2.00")


def _ctx(text: str, map_num: int = 1):
    return odds_mod._winline_matched_card_context(
        text, "Aurora Gaming", "1win", html="", map_num=map_num)


@pytest.mark.parametrize("real", [
    # hard-verifier textfb.py layout: a real section of <=66 chars before the duel header
    "DOTA 2 | BLAST Slam TEAM AURORA 1W 1 карта 1.61 2.22",
    # astra layout: 2карта / 2 карта
    "DOTA 2 | BLAST Slam TEAM AURORA 1W 2карта 2 карта 1.61 2.22",
])
def test_r1_short_real_section_before_a_duel_section_is_not_dropped(real):
    ctx = _ctx(f"{real} {_DUEL_SECTION}", map_num=1 if "1 карта" in real else 2)
    assert ctx is not None, "real card dropped: header window reached the next duel header"
    assert "1.61" in ctx and "2.22" in ctx
    assert "дуэль" not in ctx.lower()


def test_r1_long_real_section_before_a_duel_section_is_kept():
    real = ("DOTA 2 | BLAST Slam TEAM AURORA 1W Сегодня 13:00 +118 Матч 1.39 3.01 2.19 "
            "1 карта 1.61 2.22 Фора 1.5 1.80 1.90 Тотал 2.5 1.70 2.10")
    ctx = _ctx(f"{real} {_DUEL_SECTION}")
    assert ctx is not None and "1.61" in ctx and "дуэль" not in ctx.lower()


def test_r1_text_only_duel_card_is_still_skipped():
    assert _ctx(_DUEL_SECTION) is None
    # duel section first, real one after it: the real card is found, the duel stays out
    ctx = _ctx(f"{_DUEL_SECTION} DOTA 2 | BLAST Slam TEAM AURORA 1W 1 карта 1.61 2.22")
    assert ctx is not None and "1.61" in ctx and "дуэль" not in ctx.lower()


def test_r1_flag_off_restores_duel_selection_in_text(monkeypatch):
    monkeypatch.setenv("WINLINE_CARD_SWEEP_SKIP_PROPS", "0")
    assert _ctx(_DUEL_SECTION) is not None
