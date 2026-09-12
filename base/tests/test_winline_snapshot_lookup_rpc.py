"""Focused regressions for Winline snapshot and browser-RPC optimizations."""
from pathlib import Path
import sys

import pytest


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import bookmaker_selenium_odds as bk  # noqa: E402
import cyberscore_try as cs  # noqa: E402


URL = "https://winline.ru/stavki/sport/kibersport/dota_2"
FIXTURES = Path(__file__).parent / "fixtures"
LAST_MAP_MATCH_ONLY = (FIXTURES / "winline_last_map_match_only_20260802.html").read_text(
    encoding="utf-8"
)

CARD_HTML = """
<ww-feature-block-event-dsk>
  <div>ALPHA BRAVO 2 карта</div>
  <ww-feature-event-market-dsk><div><span>1.80</span></div><div><span>2.05</span></div></ww-feature-event-market-dsk>
</ww-feature-block-event-dsk>
"""
CLOSED_CARD_HTML = """
<ww-feature-block-event-dsk><div>ALPHA BRAVO 2 карта</div>
  <div class="period-name">2 карта</div><div class="card__coeffs">
    <div class="coefficient-button coefficient-button_generic2 coefficient-button_locked">1.80</div>
    <div class="coefficient-button coefficient-button_generic2 coefficient-button_locked">2.05</div>
  </div>
</ww-feature-block-event-dsk>
"""


def test_snapshot_freezes_alias_plan_and_rebuilds_for_new_html(monkeypatch):
    aliases = {"Alpha": ["Legacy"]}
    calls = []

    def spellings(team):
        calls.append(team)
        return aliases.get(team, [])

    monkeypatch.setattr(bk, "_alias_spellings", spellings)
    old_snapshot = bk._WinlineDOMSnapshot("<div>LEGACY BRAVO 2 карта</div>")
    assert old_snapshot.text_matches_teams("LEGACY BRAVO", "Alpha", "Bravo")

    aliases["Alpha"] = ["Current"]
    # A snapshot keeps its original identity rules even when the mutable alias
    # registry changes between two maps in the same polling batch.
    assert not old_snapshot.text_matches_teams("CURRENT BRAVO", "Alpha", "Bravo")

    new_snapshot = bk._WinlineDOMSnapshot("<div>CURRENT BRAVO 2 карта</div>")
    assert new_snapshot.text_matches_teams("CURRENT BRAVO", "Alpha", "Bravo")
    assert calls.count("Alpha") == 2


@pytest.mark.parametrize(
    "html,team1,team2,map_num,last_map",
    [
        (CARD_HTML, "Alpha", "Bravo", 2, False),
        (CLOSED_CARD_HTML, "Bravo", "Alpha", 2, False),
        (LAST_MAP_MATCH_ONLY, "Yakult Brothers", "REKONIX", 3, True),
    ],
    ids=["reversed_open", "reversed_closed", "decider_promotion"],
)
def test_shared_snapshot_preserves_card_guard_orientation_and_decider_parity(
    html, team1, team2, map_num, last_map
):
    fresh = bk._extract_winline_current_map_winner(
        "", team1, team2, map_num, html=html, series_last_map=last_map
    )
    snapshot = bk._WinlineDOMSnapshot(html)
    shared = bk._extract_winline_current_map_winner(
        "", team1, team2, map_num, html=html, series_last_map=last_map,
        _snapshot=snapshot,
    )
    assert vars(shared) == vars(fresh)


class _PinnedSelected:
    def __init__(self, selected):
        self.selected = selected

    def count(self):
        return int(self.selected)


class _PinnedCard:
    def __init__(self, text, *, selected=False):
        self.text = text
        self.selected = selected
        self.clicks = 0

    def inner_text(self, timeout=None):
        return self.text

    def locator(self, selector):
        assert selector == ".new-card--selected"
        return _PinnedSelected(self.selected)

    def click(self, timeout=None):
        self.clicks += 1


class _PinnedCards:
    def __init__(self, cards):
        self.cards = cards

    def count(self):
        return len(self.cards)

    def nth(self, index):
        return self.cards[index]


class _PinnedPage:
    def __init__(self, snapshot, live_cards, *, unsupported=False):
        self.snapshot = snapshot
        self.live_cards = live_cards
        self.unsupported = unsupported
        self.waits = []

    def evaluate(self, script):
        if self.unsupported:
            raise RuntimeError("evaluate unsupported")
        return self.snapshot

    def locator(self, selector):
        assert selector == "ww-pinned-card"
        return _PinnedCards(self.live_cards)

    def wait_for_timeout(self, milliseconds):
        self.waits.append(milliseconds)


def test_pinned_batch_revalidates_reordered_card_and_honours_selected_flag(monkeypatch):
    monkeypatch.setattr(cs, "_bookmaker_text_matches_teams", bk._text_matches_teams)
    reordered = _PinnedPage(
        [{"index": 0, "text": "Alpha Bravo", "selected": False}],
        [_PinnedCard("Neighbour Opponent")],
    )
    assert not cs._winline_select_matching_pinned_card(reordered, team1="Alpha", team2="Bravo")
    assert reordered.live_cards[0].clicks == 0

    selected = _PinnedPage(
        [{"index": 0, "text": "Alpha Bravo", "selected": True}],
        [_PinnedCard("Alpha Bravo", selected=True)],
    )
    assert not cs._winline_select_matching_pinned_card(selected, team1="Alpha", team2="Bravo")
    assert selected.live_cards[0].clicks == 0


def test_pinned_batch_clicks_live_match_and_falls_back_without_evaluate(monkeypatch):
    monkeypatch.setattr(cs, "_bookmaker_text_matches_teams", bk._text_matches_teams)
    live = _PinnedCard("Alpha Bravo")
    batched = _PinnedPage(
        [{"index": 0, "text": "Alpha Bravo", "selected": False}], [live]
    )
    assert cs._winline_select_matching_pinned_card(batched, team1="Alpha", team2="Bravo")
    assert (live.clicks, batched.waits) == (1, [800])

    fallback_live = _PinnedCard("Alpha Bravo")
    fallback = _PinnedPage(None, [fallback_live], unsupported=True)
    assert cs._winline_select_matching_pinned_card(fallback, team1="Alpha", team2="Bravo")
    assert (fallback_live.clicks, fallback.waits) == (1, [800])


class _OverviewBody:
    def __init__(self, page):
        self.page = page

    def inner_text(self, timeout=None):
        self.page.body_reads += 1
        return self.page.fallback_body


class _OverviewPage:
    def __init__(self, combined, *, fallback_html="", fallback_body=""):
        self.url = URL
        self.combined = combined
        self.fallback_html = fallback_html
        self.fallback_body = fallback_body
        self.content_reads = 0
        self.body_reads = 0

    def evaluate(self, script):
        assert "bodyText" in script
        if isinstance(self.combined, Exception):
            raise self.combined
        return self.combined

    def content(self):
        self.content_reads += 1
        return self.fallback_html

    def locator(self, selector):
        assert selector == "body"
        return _OverviewBody(self)


def test_overview_combined_dom_read_and_full_fallback():
    combined = _OverviewPage({
        "html": "<html><body><div class='period-name'>2 карта</div></body></html>",
        "bodyText": "Alpha   Bravo",
        "url": URL + "/combined",
    })
    result = bk._run_coroutine_blocking(bk._collect_winline_live_overview_async(combined, URL))
    assert result["text"] == "Alpha Bravo"
    assert result["html"].startswith("<html>")
    assert result["page_url"] == URL + "/combined"
    assert (combined.content_reads, combined.body_reads) == (0, 0)

    fallback = _OverviewPage(
        RuntimeError("evaluate unsupported"),
        fallback_html="<html><body><div class='period-name'>2 карта</div></body></html>",
        fallback_body="Fallback  overview",
    )
    result = bk._run_coroutine_blocking(bk._collect_winline_live_overview_async(fallback, URL))
    assert result["text"] == "Fallback overview"
    assert result["html"].startswith("<html>")
    assert (fallback.content_reads, fallback.body_reads) == (1, 1)
