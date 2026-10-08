"""The overview enumerator must also return the live match of the page's top hero block.

Board card ingame-fgbt.  Input is the REAL captured Winline overview snapshot
``base/tests/fixtures/winline_overview_snapshot_20261008_blast_duel_cards.json``
(captured 2026-10-08 18:34:37 MSK with
``scp serv1:/root/main/runtime/winline_overview_snapshot.json <fixture>``).

On that page the live match TEAM AURORA vs 1W (map 2, "Победитель 2 карта 1.61 2.22")
exists ONLY in the selected-event hero block (``ww-feature-event-live-center-dsk``); the
feed (``ww-feature-block-event-dsk#eventId-*``) holds 3 live player-duel props and 2
pre-match cards.  The card sweep reads ``winline_enumerate_live_cards`` and therefore
never priced this match.

The merge/dedupe cases reuse the REAL feed card ``eventId-16855095`` (TEAM AURORA vs 1W,
pre-match) captured in ``winline_overview_legion_blasterbi_20261005.json``: the same event
sat in the feed on 05.10 and in the hero on 08.10.
"""
from __future__ import annotations

import gzip
import json
import re
import sys
import time
import types
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

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

FIXTURES = Path(__file__).parent / "fixtures"
HERO_PAGE = "winline_overview_snapshot_20261008_blast_duel_cards.json"
FEED_PAGE = "winline_overview_legion_blasterbi_20261005.json"
HERO_EVENT_ID = "16855095"

# name -> number of cards the enumerator returned BEFORE the hero change (measured on the
# unmodified enumerator at c9635e99); only the hero page gains a card (+1).
CARDS_WITHOUT_HERO = {
    "winline_overview_legion_blasterbi_20261005.json": 5,
    "winline_overview_snapshot_20260910.json": 8,
    "winline_overview_snapshot_20261008_blast_duel_cards.json": 5,
    "winline_unpriced_overview_20260915.json.gz": 5,
}


def _html(name: str) -> str:
    path = FIXTURES / name
    raw = (gzip.open(path, "rt", encoding="utf-8").read() if name.endswith(".gz")
           else path.read_text(encoding="utf-8"))
    return json.loads(raw)["html"]


def _aurora(cards):
    return [c for c in cards if (c["team1"], c["team2"]) == ("TEAM AURORA", "1W")]


def _rows(card):
    return {(r["kind"], r["map_num"]): r["has_prices"] for r in card["rows"]}


def test_hero_live_match_is_enumerated_once_with_map_row_and_league():
    cards = odds_mod.winline_enumerate_live_cards(_html(HERO_PAGE))
    aurora = _aurora(cards)
    assert len(aurora) == 1, [(c["event_id"], c["team1"], c["team2"]) for c in cards]
    card = aurora[0]
    assert card["event_id"] == HERO_EVENT_ID
    assert card["league"] == "BLAST Slam"
    assert card["prop_duel"] is False
    assert card["live"] is True
    assert card["header_map"] == 2
    # Captured block: "Победитель 2 карта 1.61 2.22" (+ match winner 1.22 3.90).
    assert _rows(card)[("map", 2)] is True
    assert _rows(card)[("match", None)] is True
    # The five feed cards are untouched.
    assert len(cards) == 6
    assert {c["event_id"] for c in cards} >= {
        "16882197", "16882198", "16882199", "16889471", "16889364"}


def test_hero_card_has_the_same_shape_as_feed_cards():
    cards = odds_mod.winline_enumerate_live_cards(_html(HERO_PAGE))
    feed_keys = set(next(c for c in cards if c["event_id"] == "16889364"))
    hero_keys = set(_aurora(cards)[0])
    assert feed_keys <= hero_keys


def _feed_copy(source_id: str = HERO_EVENT_ID, new_event_id: str = "", live: bool = False):
    """A REAL feed card captured on 05.10, optionally re-id'd and flagged LIVE.

    ``live=True`` adds the class ``card--live`` to the card's ``div.card`` -- the exact
    marker the enumerator reads (captured live cards carry ``<div class="card card--live">``,
    e.g. eventId-16858346 LEGION vs BLASTERBI in the same file).
    """
    feed_soup = BeautifulSoup(_html(FEED_PAGE), "html.parser")
    feed_card = feed_soup.find(id=f"eventId-{source_id}")
    assert feed_card is not None
    copy = BeautifulSoup(str(feed_card), "html.parser").find(id=True)
    if new_event_id:
        copy["id"] = f"eventId-{new_event_id}"
    if live:
        inner = copy.select_one("div.card")
        inner["class"] = list(inner.get("class") or []) + ["card--live"]
    return copy


def _page_with_feed_copy_of_hero_event(new_event_id: str = "", live: bool = False) -> str:
    """Hero page plus the REAL feed card of the same event captured on 05.10."""
    soup = BeautifulSoup(_html(HERO_PAGE), "html.parser")
    anchor = soup.find(id="eventId-16889364")
    anchor.insert_before(_feed_copy(new_event_id=new_event_id, live=live))
    return str(soup)


def _strip_logo_ids(page: str) -> str:
    """Remove the event id from the hero's team logos (plain ``img`` URLs) ONLY.

    The widget iframes (``l1``/``l2`` query values, percent-encoded) keep the id.
    """
    return (page.replace(f"/api/cls/event/1/{HERO_EVENT_ID}", "/x/1")
                .replace(f"/api/cls/event/2/{HERO_EVENT_ID}", "/x/2"))


def _strip_hero_ids(page: str) -> str:
    """Remove the event id from EVERY hero source: logos and widget iframes."""
    return (_strip_logo_ids(page)
            .replace(f"%2Fapi%2Fcls%2Fevent%2F1%2F{HERO_EVENT_ID}", "%2Fx%2F1")
            .replace(f"%2Fapi%2Fcls%2Fevent%2F2%2F{HERO_EVENT_ID}", "%2Fx%2F2"))


def test_same_event_in_hero_and_feed_is_enumerated_once_by_event_id():
    cards = odds_mod.winline_enumerate_live_cards(_page_with_feed_copy_of_hero_event())
    aurora = _aurora(cards)
    assert len(aurora) == 1, [(c["event_id"], c["live"]) for c in aurora]
    card = aurora[0]
    assert card["event_id"] == HERO_EVENT_ID
    # The feed card was pre-match at capture; the hero proves the match is live now and
    # brings the priced map-2 row.
    assert card["live"] is True
    assert _rows(card).get(("map", 2)) is True
    ids = [c["event_id"] for c in cards]
    assert len(ids) == len(set(ids)), ids


def test_distinct_event_ids_with_the_same_pair_stay_distinct_events():
    cards = odds_mod.winline_enumerate_live_cards(
        _page_with_feed_copy_of_hero_event(new_event_id="99999999"))
    assert sorted(c["event_id"] for c in _aurora(cards)) == [HERO_EVENT_ID, "99999999"]


def test_hero_without_event_id_merges_only_into_the_single_LIVE_same_pair_card():
    """No id on the hero: a LIVE same-pair feed card (hero shows no start date) takes it."""
    page = _strip_hero_ids(_page_with_feed_copy_of_hero_event(new_event_id="99999999", live=True))
    aurora = _aurora(odds_mod.winline_enumerate_live_cards(page))
    assert [c["event_id"] for c in aurora] == ["99999999"]
    assert aurora[0]["live"] is True and _rows(aurora[0]).get(("map", 2)) is True


def test_hero_without_event_id_and_without_feed_pair_stands_alone():
    alone = odds_mod.winline_enumerate_live_cards(_strip_hero_ids(_html(HERO_PAGE)))
    assert [(c["event_id"], c["live"]) for c in _aurora(alone)] == [("", True)]


def test_p1_hero_without_id_does_not_move_live_onto_a_prematch_same_pair_card():
    """Review P1 (event substitution without an id).

    HERO_PAGE with the hero's logo ids removed plus the REAL pre-match feed card of the same
    pair (id 99999999, '08.10.26 08:30', not live): one same-pair card does not prove the
    same event, so the hero is dropped and the pre-match card must stay pre-match.
    """
    page = _strip_hero_ids(_page_with_feed_copy_of_hero_event(new_event_id="99999999"))
    aurora = _aurora(odds_mod.winline_enumerate_live_cards(page))
    assert [c["event_id"] for c in aurora] == ["99999999"]
    assert aurora[0]["live"] is False
    assert _rows(aurora[0]).get(("map", 2)) is not True


def test_hero_without_id_is_dropped_when_it_shows_a_start_date():
    """A hero that shows a start date ('Завтра 15:00') next to the live scoreboard is
    inconsistent: it must not make a live feed card of the pair priced on map 2."""
    page = _strip_hero_ids(_page_with_feed_copy_of_hero_event(new_event_id="99999999", live=True))
    soup = BeautifulSoup(page, "html.parser")
    timer = soup.select_one("section.event-live-center .match-card__timer")
    timer.append(" Завтра 15:00 ")
    aurora = _aurora(odds_mod.winline_enumerate_live_cards(str(soup)))
    assert [c["event_id"] for c in aurora] == ["99999999"]
    assert _rows(aurora[0]).get(("map", 2)) is not True


def test_hero_without_id_is_dropped_when_two_feed_cards_carry_its_pair():
    page = _strip_hero_ids(_page_with_feed_copy_of_hero_event(new_event_id="99999999", live=True))
    soup = BeautifulSoup(page, "html.parser")
    soup.find(id="eventId-99999999").insert_before(
        _feed_copy(new_event_id="99999998", live=True))
    aurora = _aurora(odds_mod.winline_enumerate_live_cards(str(soup)))
    assert sorted(c["event_id"] for c in aurora) == ["99999998", "99999999"]
    assert all(_rows(c).get(("map", 2)) is not True for c in aurora)


def test_r3_p1_known_widget_id_is_not_lost_when_only_the_logo_ids_are_missing():
    """Round-3 P1: ids removed from the logos only, the widget iframes keep 16855095.

    HERO_PAGE plus a LIVE same-pair feed card with another id (99999999, REAL 05.10 card
    flagged live).  The hero's id is known from the iframe, so it must take the id path:
    stand alone as 16855095 and NEVER hand its priced map 2 to the feed card 99999999.
    """
    page = _strip_logo_ids(_page_with_feed_copy_of_hero_event(new_event_id="99999999", live=True))
    assert f"%2Fapi%2Fcls%2Fevent%2F2%2F{HERO_EVENT_ID}" in page  # iframe id kept
    aurora = {c["event_id"]: c for c in _aurora(odds_mod.winline_enumerate_live_cards(page))}
    assert sorted(aurora) == [HERO_EVENT_ID, "99999999"]
    assert _rows(aurora["99999999"]).get(("map", 2)) is not True
    assert aurora[HERO_EVENT_ID]["source"] == "hero"
    assert _rows(aurora[HERO_EVENT_ID]).get(("map", 2)) is True


def _hero_without_live_score_and_with_start_date(page: str, start_date: bool) -> str:
    soup = BeautifulSoup(page, "html.parser")
    hero = soup.select_one("section.event-live-center")
    for el in hero.select(".match-card__timer, .match-card__live-score"):
        el.decompose()
    if start_date:
        hero.select_one(".event-live-center__scoreboard").append(" Завтра 15:00 ")
    return str(soup)


@pytest.mark.parametrize("start_date", [True, False], ids=["start-date", "no-live-marker"])
def test_r3_p1_not_live_hero_does_not_make_its_rows_live_through_a_same_id_merge(start_date):
    """Round-3 P1: a hero without timer/live score (optionally showing 'Завтра 15:00') next
    to a same-id LIVE feed card must contribute nothing: the feed card keeps its own rows,
    its priced map 2 does NOT come from the hero."""
    page = _hero_without_live_score_and_with_start_date(
        _page_with_feed_copy_of_hero_event(live=True), start_date)
    hero_soup = BeautifulSoup(page, "html.parser").select_one("section.event-live-center")
    assert hero_soup.select_one(".match-card__timer, .match-card__live-score") is None
    aurora = _aurora(odds_mod.winline_enumerate_live_cards(page))
    assert [c["event_id"] for c in aurora] == [HERO_EVENT_ID]
    assert aurora[0].get("source") != "hero"
    assert aurora[0]["live"] is True  # the feed card's own flag
    assert _rows(aurora[0]).get(("map", 2)) is not True


@pytest.mark.parametrize("pair", [("PARIVISION", ""), ("", "PARIVISION")],
                         ids=["n1-only", "n2-only"])
def test_r3_p2_a_single_present_widget_name_that_conflicts_drops_the_hero(pair):
    """Round-3 P2: the widget carries one team name that is not on the scoreboard (the other
    is empty): AURORA / 1W on the scoreboard, widget says PARIVISION -> hero dropped."""
    page = _html(HERO_PAGE).replace(
        "n1=TEAM+AURORA&amp;n2=1W", f"n1={pair[0]}&amp;n2={pair[1]}")
    assert page.count(f"n1={pair[0]}&amp;n2={pair[1]}") == 5  # all five widget frames
    cards = odds_mod.winline_enumerate_live_cards(page)
    assert [c for c in cards if c.get("source") == "hero"] == []
    assert len(cards) == 5


def test_r3_p2_a_single_present_widget_name_that_matches_keeps_the_hero():
    """Counterpart: n2 empty but n1 = TEAM AURORA matches the scoreboard -> hero kept."""
    page = _html(HERO_PAGE).replace("n1=TEAM+AURORA&amp;n2=1W", "n1=TEAM+AURORA&amp;n2=")
    assert page.count("n1=TEAM+AURORA&amp;n2=") == 5
    cards = odds_mod.winline_enumerate_live_cards(page)
    assert [c["event_id"] for c in _aurora(cards)] == [HERO_EVENT_ID]


def _poll_keys(created):
    return sorted((c["team1"], c["team2"], c["map_num"]) for c in created)


def test_p1_sweep_does_not_poll_a_future_match_when_the_hero_lost_its_id(sweep, monkeypatch):
    """Review P1, sweep level: nothing is polled for the pre-match 99999999 card."""
    C, created = sweep
    page = _strip_hero_ids(_page_with_feed_copy_of_hero_event(new_event_id="99999999"))
    monkeypatch.setitem(C._winline_overview_state, "html", page)
    C._winline_sweep_cards_from_snapshot()
    assert created == []


def test_p1_hero_league_conflict_with_the_same_id_feed_card_is_dropped():
    """Review P1 (league gate bypass).

    The hero says 'DOTA 2, Winline Super Mixer' (breadcrumb AND widget title), the same-id
    feed card says 'BLAST Slam'.  The leagues disagree, so the hero is dropped and the feed
    card (pre-match) keeps its own league and is not made live.
    """
    page = _page_with_feed_copy_of_hero_event()
    soup = BeautifulSoup(page, "html.parser")
    crumb = soup.select_one("section.event-live-center .event-breadcrumbs")
    crumb.string = " DOTA 2, Winline Super Mixer "
    page = str(soup).replace("title=BLAST+Slam", "title=Winline+Super+Mixer")
    aurora = _aurora(odds_mod.winline_enumerate_live_cards(page))
    assert len(aurora) == 1
    assert aurora[0]["league"] == "BLAST Slam"
    assert aurora[0]["live"] is False
    assert _rows(aurora[0]).get(("map", 2)) is not True


def test_p1_sweep_does_not_poll_by_the_feed_league_after_a_hero_league_conflict(sweep, monkeypatch):
    C, created = sweep
    page = _page_with_feed_copy_of_hero_event()
    soup = BeautifulSoup(page, "html.parser")
    soup.select_one("section.event-live-center .event-breadcrumbs").string = (
        " DOTA 2, Winline Super Mixer ")
    page = str(soup).replace("title=BLAST+Slam", "title=Winline+Super+Mixer")
    monkeypatch.setitem(C._winline_overview_state, "html", page)
    C._winline_sweep_cards_from_snapshot()
    assert created == []


def test_p1_hero_with_replaced_teams_does_not_put_its_rows_on_the_same_id_card():
    """Review P1 (identity conflict): the team names of the hero block become PARIVISION /
    TEAM YANDEX while the logo ids still name the AURORA vs 1W event."""
    page = _page_with_feed_copy_of_hero_event()
    soup = BeautifulSoup(page, "html.parser")
    names = soup.select("section.event-live-center .match-card__team-name")
    assert [n.find("div", recursive=False).get_text(strip=True) for n in names[:1]] == ["TEAM AURORA"]
    names[0].find("div", recursive=False).string = "PARIVISION"
    names[1].find_all("div", recursive=False)[-1].string = "TEAM YANDEX"
    cards = odds_mod.winline_enumerate_live_cards(str(soup))
    aurora = _aurora(cards)
    assert len(aurora) == 1
    assert aurora[0]["live"] is False
    assert _rows(aurora[0]).get(("map", 2)) is not True
    # and no live card named after the replaced teams either
    assert [c for c in cards if c.get("source") == "hero"] == []


def test_p1_identity_conflict_is_caught_at_merge_even_when_alt_and_widget_agree():
    """The hero block consistently names PARIVISION vs TEAM YANDEX (names, left logo alt, widget
    n1/n2) but carries the event id of the AURORA vs 1W feed card: the merge itself must refuse."""
    soup = BeautifulSoup(_page_with_feed_copy_of_hero_event(), "html.parser")
    hero = soup.select_one("section.event-live-center")
    names = hero.select(".match-card__team-name")
    names[0].find("div", recursive=False).string = "PARIVISION"
    names[1].find_all("div", recursive=False)[-1].string = "TEAM YANDEX"
    for img in hero.select("img.match-card__logo-image"):
        img["alt"] = "PARIVISION"
    for frame in hero.select("iframe"):
        frame["src"] = frame["src"].replace("n1=TEAM+AURORA", "n1=PARIVISION") \
                                   .replace("n2=1W", "n2=TEAM+YANDEX")
    cards = odds_mod.winline_enumerate_live_cards(str(soup))
    aurora = _aurora(cards)
    assert len(aurora) == 1 and aurora[0]["live"] is False
    assert _rows(aurora[0]).get(("map", 2)) is not True
    assert [c for c in cards if c.get("source") == "hero"] == []


def test_p1_hero_with_replaced_teams_and_no_feed_card_is_not_a_live_card():
    """Same replacement on the bare hero page: the widget (n1/n2) and the left logo alt still
    name TEAM AURORA vs 1W, so the block contradicts itself and is dropped."""
    soup = BeautifulSoup(_html(HERO_PAGE), "html.parser")
    names = soup.select("section.event-live-center .match-card__team-name")
    names[0].find("div", recursive=False).string = "PARIVISION"
    names[1].find_all("div", recursive=False)[-1].string = "TEAM YANDEX"
    cards = odds_mod.winline_enumerate_live_cards(str(soup))
    assert [c for c in cards if c.get("source") == "hero"] == []
    assert len(cards) == 5


@pytest.mark.parametrize("edit", [
    # the two team logos name different events
    lambda page: page.replace(f"/api/cls/event/2/{HERO_EVENT_ID}", "/api/cls/event/2/16855096"),
    # the widget links name a different event than the logos
    lambda page: page.replace(f"%2Fapi%2Fcls%2Fevent%2F2%2F{HERO_EVENT_ID}",
                              "%2Fapi%2Fcls%2Fevent%2F2%2F16855096"),
])
def test_p1_hero_with_conflicting_event_ids_is_rejected(edit):
    page = edit(_html(HERO_PAGE))
    assert "16855096" in page
    cards = odds_mod.winline_enumerate_live_cards(page)
    assert [c for c in cards if c.get("source") == "hero"] == []
    assert len(cards) == 5


def test_p2_video_player_alone_does_not_make_the_hero_live():
    """Review P2: only ``player-wrapper--live`` ('Просмотр видеотрансляции') -- no timer, no
    live score -- is not a scoreboard marker; the hero must not be live."""
    soup = BeautifulSoup(_html(HERO_PAGE), "html.parser")
    hero = soup.select_one("section.event-live-center")
    assert hero.select_one(".player-wrapper--live") is not None
    for el in hero.select(".match-card__timer, .match-card__live-score"):
        el.decompose()
    cards = odds_mod.winline_enumerate_live_cards(str(soup))
    aurora = _aurora(cards)
    assert len(aurora) == 1 and aurora[0]["live"] is False


def test_p2_sweep_does_not_poll_a_hero_with_only_the_video_player(sweep, monkeypatch):
    C, created = sweep
    soup = BeautifulSoup(_html(HERO_PAGE), "html.parser")
    for el in soup.select("section.event-live-center .match-card__timer, "
                          "section.event-live-center .match-card__live-score"):
        el.decompose()
    monkeypatch.setitem(C._winline_overview_state, "html", str(soup))
    C._winline_sweep_cards_from_snapshot()
    assert created == []


def test_cap_sort_a_merged_live_hero_after_six_prematch_cards_is_still_polled(sweep, monkeypatch):
    """Approved sweep change: live cards go first (stable) before ``[:max_cards]``.

    Six REAL pre-match cards (copies of MOUZ vs NEMIGA GAMING, ids 9900000x) precede the
    AURORA feed card, which the hero makes live: with cap 6 the document order alone cut it
    off (0 polls); live-first ordering polls its priced map rows.
    """
    C, created = sweep
    soup = BeautifulSoup(_html(HERO_PAGE), "html.parser")
    last = soup.find(id="eventId-16889364")
    for i in range(6):
        copy = _feed_copy(source_id="16858347", new_event_id=f"9900000{i}")
        last.insert_after(copy)
        last = copy
    # AURORA card goes AFTER the six pre-match copies (feed order: ..., 6 x MOUZ, AURORA).
    last.insert_after(_feed_copy())
    order = [c["event_id"] for c in odds_mod.winline_enumerate_live_cards(str(soup))]
    assert order.index(HERO_EVENT_ID) == len(order) - 1, order
    monkeypatch.setitem(C._winline_overview_state, "html", str(soup))
    C._winline_sweep_cards_from_snapshot()
    # The 05.10 feed copy is PRE-MATCH (its priced map 1 is a pre-start line); the live hero
    # promotes the card and brings its own rows only - the live map 2 is polled, map 1 is not
    # (astra round-3 review: inherited pre-match rows were polled and could push map 2 out of the rows cap).
    assert _poll_keys(created) == [("TEAM AURORA", "1W", 2)]


@pytest.mark.parametrize("name", sorted(CARDS_WITHOUT_HERO))
def test_pages_without_a_hero_block_are_unchanged_and_never_duplicate_events(name):
    cards = odds_mod.winline_enumerate_live_cards(_html(name))
    expected = CARDS_WITHOUT_HERO[name] + (1 if name == HERO_PAGE else 0)
    assert len(cards) == expected, name
    ids = [c["event_id"] for c in cards if c["event_id"]]
    assert len(ids) == len(set(ids)), name
    assert all((c.get("source") == "hero") == (name == HERO_PAGE and c["event_id"] == HERO_EVENT_ID)
               for c in cards), name


def test_non_dota_or_empty_hero_block_is_ignored():
    soup = BeautifulSoup(_html(HERO_PAGE), "html.parser")
    crumb = soup.select_one("section.event-live-center .event-breadcrumbs")
    crumb.string = " Counter-Strike 2, BLAST Slam "
    cards = odds_mod.winline_enumerate_live_cards(str(soup))
    assert _aurora(cards) == []


# --- sweep level: the hero card is priced and lands inside the card cap -----------------


@pytest.fixture
def sweep(monkeypatch):
    import cyberscore_try as C
    monkeypatch.setenv(C.WINLINE_OVERVIEW_SNAPSHOT_ENV, str(FIXTURES / HERO_PAGE))
    snapshot = json.loads(C._winline_overview_snapshot_path().read_text(encoding="utf-8"))
    monkeypatch.setitem(C._winline_overview_state, "html", snapshot["html"])
    monkeypatch.setitem(C._winline_overview_state, "fetched_at", time.time())
    created = []
    monkeypatch.setattr(C, "ensure_winline_current_map_polling",
                        lambda **kw: created.append(kw) or True)
    monkeypatch.setattr(C, "_winline_first_active", lambda: True)
    monkeypatch.setattr(C, "_dltv_live_series_snapshot", lambda *a, **k: None)
    monkeypatch.setattr(C, "_winline_card_dltv_draft_notify", lambda **k: None)
    monkeypatch.setattr(C, "_winline_bridge_live_pairs", lambda *a, **k: set())
    monkeypatch.setattr(C, "_winline_bridge_owns_card_pair", lambda *a, **k: False)
    monkeypatch.setattr(C, "_winline_prop_skip_logged_keys", set(), raising=False)
    monkeypatch.setattr(C, "WINLINE_CARD_SWEEP_MAX_CARDS", 6)
    return C, created


def test_sweep_prices_the_hero_match_map_2(sweep):
    C, created = sweep
    summary = C._winline_sweep_cards_from_snapshot()
    keys_ = [(c["series"], c["team1"], c["team2"], c["map_num"]) for c in created]
    assert keys_ == [("winline:league:blast slam|1w|team aurora", "TEAM AURORA", "1W", 2)], (
        keys_, summary)
    assert summary["skipped_props"] == 4
    assert summary["ensured"] == 1


def test_hero_card_is_inside_the_default_cap_on_the_captured_page(sweep, monkeypatch):
    """Cards after the prop filter: hero (first, document order) + 1 pre-match card."""
    C, created = sweep
    monkeypatch.setattr(C, "WINLINE_CARD_SWEEP_MAX_CARDS", 1)
    C._winline_sweep_cards_from_snapshot()
    assert [(c["team1"], c["team2"], c["map_num"]) for c in created] == [("TEAM AURORA", "1W", 2)]


# ------------- Opus acceptance (09.10): duplicate-id skip and live+dated hero pinned by tests

def test_twin_hero_blocks_with_one_event_id_are_both_dropped():
    """Two hero blocks with the same event id (inconsistent snapshot): neither is trusted.
    Without the duplicate-id skip the event is listed twice (7 cards), costing a sweep cap slot
    and a duplicate notify per row."""
    import copy
    soup = BeautifulSoup(_html(HERO_PAGE), "html.parser")
    hero = soup.select_one("ww-feature-event-live-center-dsk")
    hero.insert_after(copy.copy(hero))

    cards = odds_mod.winline_enumerate_live_cards(str(soup))

    ids = [c["event_id"] for c in cards]
    assert len(ids) == len(set(ids)), ids
    assert _aurora(cards) == []
    assert len(cards) == CARDS_WITHOUT_HERO[HERO_PAGE]


def test_live_hero_with_a_start_date_is_dropped():
    """A hero block that shows a live scoreboard AND a start time is not a live event the sweep may
    poll: no hero card, the feed cards are unchanged."""
    soup = BeautifulSoup(_html(HERO_PAGE), "html.parser")
    soup.select_one(
        "ww-feature-event-live-center-dsk .event-live-center__scoreboard"
    ).append(soup.new_string(" Сегодня 13:00 "))

    cards = odds_mod.winline_enumerate_live_cards(str(soup))

    assert _aurora(cards) == []
    assert len(cards) == CARDS_WITHOUT_HERO[HERO_PAGE]
