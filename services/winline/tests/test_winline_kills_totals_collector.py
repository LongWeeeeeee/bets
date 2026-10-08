"""Captured event markets and offline one-shot collector regressions."""
import gzip
import importlib.util
import json
import re
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[3]
FIXTURES = Path(__file__).resolve().parent / "fixtures"
sys.path.insert(0, str(ROOT / "base"))
SPEC = importlib.util.spec_from_file_location(
    "winline_kills_totals_collector", ROOT / "services/winline/winline_kills_totals_collector.py")
collector = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = collector
SPEC.loader.exec_module(collector)

CASES = [
    ("yandex_lgd", ("TEAM YANDEX", "LGD GAMING"), 1, ((31.5, 1.93, 1.88), (18.5, 1.91, 1.89))),
    ("yandex_lgd", ("TEAM YANDEX", "LGD GAMING"), 2, ((31.5, 1.94, 1.87), (18.5, 1.90, 1.90))),
    ("yandex_lgd", ("TEAM YANDEX", "LGD GAMING"), 3, ((31.5, 1.94, 1.87), (18.5, 1.90, 1.90))),
    ("aurora_1w", ("TEAM AURORA", "1W"), 1, ((28.5, 1.87, 1.94), (27.5, 1.93, 1.88))),
    ("aurora_1w", ("TEAM AURORA", "1W"), 2, ((28.5, 1.87, 1.94), (27.5, 1.94, 1.87))),
    ("aurora_1w", ("TEAM AURORA", "1W"), 3, ((28.5, 1.87, 1.94), (27.5, 1.94, 1.87))),
]
PROXY = "http://test-user:test-password@proxy.invalid:1234"
DIRECT, EXIT = "192.0.2.1", "198.51.100.1"


def body(kind="yandex_lgd"):
    return (FIXTURES / ("winline_kills_totals_prematch_%s_20261005.txt" % kind)).read_text()


def card(event_id="16855431", live=False):
    return dict(event_id=event_id, live=live, league="BLAST Slam",
                team1="TEAM YANDEX", team2="LGD GAMING", header_map=1)


def project_proxy_file(monkeypatch, tmp_path):
    # Exercise the real selection logic with private configuration as input.
    base = tmp_path / "base"
    base.mkdir()
    (base / "keys.py").write_text(
        "BOOKMAKER_PROXY_POOL = [%r]\nPROXY_INVENTORY = "
        "[{'ip': 'proxy.invalid', 'country': 'DE'}]\n" % PROXY)
    monkeypatch.setattr(collector, "REPO", tmp_path)


@pytest.mark.parametrize("flatten", [False, True])
@pytest.mark.parametrize("kind,teams,map_num,values", CASES)
def test_captured_maps(kind, teams, map_num, values, flatten):
    text = body(kind)
    if flatten:
        text = " ".join(text.split())
    # BLAST pages carry one line per team: the ladder is that single rung.
    expected = {side: dict(team=team, ladder=[list(numbers)],
                           **dict(zip(("line", "over", "under"), numbers)))
                for side, team, numbers in zip(("p1", "p2"), teams, values)}
    assert collector.parse_winline_team_kills_totals(text, map_num, *teams) == expected


@pytest.mark.parametrize("map_num", [1, 2, 3])
def test_live_absence(map_num):
    text = (FIXTURES / "winline_live_no_kills_totals_maddogs_20261005.txt").read_text()
    assert collector.parse_winline_team_kills_totals(
        text, map_num, "AZURE DRAGONS", "HELLSPAWN") == {"p1": None, "p2": None}
    assert [r["map_num"] for r in collector.build_rows(card(live=True), text, wall=10)] == [1, 2, 3]


def test_traps_exact_names_and_orientation():
    lines = body().splitlines()
    for team in ("TEAM YANDEX", "LGD GAMING"):
        index = lines.index("1 карта тотал убийств " + team)
        lines[index:index + 7] = []
    parse = collector.parse_winline_team_kills_totals
    assert parse("\n".join(lines), 1, "TEAM YANDEX", "LGD GAMING") == {"p1": None, "p2": None}
    assert parse(body(), 1, "YANDEX", "LGD") == {"p1": None, "p2": None}
    got = parse(body(), 1, " lgd  GAMING ", "team\tYandex")
    assert got["p1"]["line"] == 18.5
    assert got["p2"]["line"] == 31.5


@pytest.mark.parametrize("old,new", [
    ("м 31.5", "м 32.5"), ("1.93", "1.0"), ("1.93", "bad"),
    ("б 31.5", "б bad"), ("Меньше\nм 31.5\n1.88", "Меньше"),
    ("тотал убийств TEAM YANDEX", "тотал убийств TEAM YANDEX рошана"),
])
def test_malformed_side(old, new):
    got = collector.parse_winline_team_kills_totals(body().replace(old, new, 1), 1,
                                                  "TEAM YANDEX", "LGD GAMING")
    assert got["p1"] is None
    assert got["p2"]["line"] == 18.5


@pytest.mark.parametrize("text,map_num", [(None, 1), (123, 1), ("", 1), (body(), None), (body(), "bad")])
def test_bad_input(text, map_num):
    assert collector.parse_winline_team_kills_totals(text, map_num, "TEAM YANDEX", "LGD GAMING") == {
        "p1": None, "p2": None}


def test_captured_listing_and_provenance(monkeypatch):
    # The isolated worktree deliberately has no private keys.py. The pure
    # listing helper needs only this import-time constant, never a real secret.
    keys = ModuleType("base.keys")
    keys.BOOKMAKER_PROXY_URL = ""
    monkeypatch.setitem(sys.modules, "base.keys", keys)
    html = gzip.decompress((FIXTURES / "winline_dota2_listing_20261005.html.gz").read_bytes()).decode()
    cards = collector.enumerate_cards(html)
    assert len(cards) == 7
    target = next(c for c in cards if c["event_id"] == "16855431")
    assert (target["team1"], target["team2"], target["live"]) == ("TEAM YANDEX", "LGD GAMING", False)
    provenance = json.loads((FIXTURES / "winline_kills_totals_20261005.provenance.json").read_text())
    assert "winline_dota2_listing_20261005.html.gz" in provenance["files"]


def test_rows_and_persisted_write_on_change(tmp_path):
    path = tmp_path / "history.jsonl"
    row = collector.build_rows(card(), "1 карта победитель", wall=10)[0]
    assert row["source"] == "winline_event_page"
    assert row["kind"] == "prematch"
    assert row["team1"] == "TEAM YANDEX"
    assert all(row[f] is None for f in collector.VALUE_FIELDS)
    with collector.HistoryWriter(path) as writer:
        assert writer.write(row)
        assert not writer.write(dict(row, wall=11))
    with collector.HistoryWriter(path) as writer:
        assert not writer.write(dict(row, wall=12))
        moved = collector.build_rows(card(), body(), wall=13)[0]
        assert writer.write(moved)
        assert not writer.write(dict(moved, wall=14))
        assert writer.write(dict(moved, kills_t1_line=32.5, wall=15))
        assert writer.write(dict(moved, event_id="other", wall=16))
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert len(rows) == 4
    assert rows[0]["wall"] == 10
    assert rows[2]["kills_t1_line"] == 32.5
    assert path.with_suffix(".state.json").is_file()


def test_state_save_failure_recovers_append_without_duplicate(monkeypatch, tmp_path):
    path = tmp_path / "h.jsonl"
    row = collector.build_rows(card(), body(), wall=10)[0]
    replace = collector.os.replace
    with collector.HistoryWriter(path) as writer:
        monkeypatch.setattr(collector.os, "replace", Mock(side_effect=OSError("state save failed")))
        with pytest.raises(OSError):
            writer.write(row)
        assert writer.rows_written == 1
    monkeypatch.setattr(collector.os, "replace", replace)
    with collector.HistoryWriter(path) as writer:
        assert not writer.write(dict(row, wall=11))
        assert writer.write(dict(row, kills_t2_under=1.95))
    assert len(path.read_text().splitlines()) == 2


def test_duplicate_market_is_ambiguous_and_commas_supported():
    duplicate = "1 карта тотал убийств TEAM YANDEX Больше б 31.5 1.93 Меньше м 31.5 1.88"
    got = collector.parse_winline_team_kills_totals(body() + "\n" + duplicate, 1,
                                                  "TEAM YANDEX", "LGD GAMING")
    assert got["p1"] is None
    assert got["p2"]["line"] == 18.5
    got = collector.parse_winline_team_kills_totals(body().replace(".", ","), 1,
                                                  "TEAM YANDEX", "LGD GAMING")
    assert got["p1"]["line"] == 31.5


LADDER_BODY = "winline_kills_totals_ladder_prematch_yellow_submarine_cyberhero_20261005.txt"
LADDER_TEAMS = ("YELLOW SUBMARINE", "CYBERHERO")
# Read by eye from the captured tier-3 page (PR Universe, event 16858345): per team a block
# Больше б 27.5..30.5 / Меньше м 27.5..30.5, then a second block with one more line.
LADDER_YS = [[27.5, 1.62, 2.15], [28.5, 1.72, 2.00], [29.5, 1.83, 1.87], [30.5, 2.00, 1.72],
             [31.5, 2.15, 1.62]]
LADDER_CH = [[23.5, 1.65, 2.10], [24.5, 1.72, 2.00], [25.5, 1.83, 1.87], [26.5, 1.90, 1.80],
             [27.5, 2.00, 1.72]]
LADDER_CH3 = [[23.5, 1.65, 2.10], [24.5, 1.72, 2.00], [25.5, 1.80, 1.90], [26.5, 1.90, 1.80],
              [27.5, 2.00, 1.72]]


def ladder_card():
    return dict(event_id="16858345", live=False, league="PR Universe, Qualifier",
                team1=LADDER_TEAMS[0], team2=LADDER_TEAMS[1], header_map=1)


@pytest.mark.parametrize("flatten", [False, True])
@pytest.mark.parametrize("map_num,ch_ladder,ch_main", [
    (1, LADDER_CH, (25.5, 1.83, 1.87)),
    (2, LADDER_CH, (25.5, 1.83, 1.87)),
    # 25.5 (1.80/1.90) and 26.5 (1.90/1.80) are equally balanced: the lower line wins.
    (3, LADDER_CH3, (25.5, 1.80, 1.90)),
])
def test_ladder_markets_on_captured_tier3_page(map_num, ch_ladder, ch_main, flatten):
    text = (FIXTURES / LADDER_BODY).read_text()
    if flatten:
        text = " ".join(text.split())
    got = collector.parse_winline_team_kills_totals(text, map_num, *LADDER_TEAMS)
    assert got == {
        "p1": dict(team="YELLOW SUBMARINE", line=29.5, over=1.83, under=1.87, ladder=LADDER_YS),
        "p2": dict(team="CYBERHERO", line=ch_main[0], over=ch_main[1], under=ch_main[2],
                   ladder=ch_ladder)}


def test_ladder_rows_persist_and_dedupe(tmp_path):
    text = (FIXTURES / LADDER_BODY).read_text()
    rows = collector.build_rows(ladder_card(), text, wall=20)
    assert [r["map_num"] for r in rows] == [1, 2, 3]
    assert [(r["kills_t1_line"], r["kills_t2_line"]) for r in rows] == [(29.5, 25.5)] * 3
    assert rows[0]["kills_t1_ladder"] == LADDER_YS
    assert rows[2]["kills_t2_ladder"] == LADDER_CH3
    path = tmp_path / "history.jsonl"
    # An old-format row (before ladders were recorded) must replay without the ladder fields.
    old = dict(rows[0], wall=1, kills_t1_line=None, kills_t1_over=None, kills_t1_under=None,
               kills_t2_line=None, kills_t2_over=None, kills_t2_under=None)
    del old["kills_t1_ladder"], old["kills_t2_ladder"]
    path.write_text(json.dumps(old, ensure_ascii=False) + "\n")
    with collector.HistoryWriter(path) as writer:
        assert all(writer.write(r) for r in rows)
        assert not any(writer.write(dict(r, wall=21)) for r in rows)
    with collector.HistoryWriter(path) as writer:  # state JSON round trip: lists stay equal
        assert not any(writer.write(dict(r, wall=22)) for r in rows)
        changed = dict(rows[0], kills_t2_ladder=LADDER_CH[:-1] + [[27.5, 2.05, 1.70]], wall=23)
        assert writer.write(changed)
    stored = [json.loads(line) for line in path.read_text().splitlines()]
    assert len(stored) == 5
    assert stored[1]["kills_t1_ladder"] == LADDER_YS


@pytest.mark.parametrize("old,new", [
    ("м 27.5\n2.15", "м 32.5\n2.15"),          # over rung without an under rung
    ("б 28.5\n1.72", "б 27.5\n1.72"),          # duplicate over rung
    ("б 29.5\n1.83", "б 29.5\n1.0"),           # impossible price
    ("Меньше\nм 27.5\n2.15\nм 28.5\n2.00\nм 29.5\n1.87\nм 30.5\n1.72\n", ""),  # no under block
])
def test_malformed_ladder_side_is_null(old, new):
    text = (FIXTURES / LADDER_BODY).read_text()
    assert text.count(old) >= 1
    got = collector.parse_winline_team_kills_totals(text.replace(old, new, 1), 1, *LADDER_TEAMS)
    assert got["p1"] is None
    assert got["p2"]["ladder"] == LADDER_CH


def test_ladder_duplicate_rung_on_both_sides_is_null():
    # Both sides repeat 27.5, so the line sets still agree; only the duplicate check rejects it.
    text = (FIXTURES / LADDER_BODY).read_text()
    text = text.replace("б 28.5\n1.72", "б 27.5\n1.72", 1).replace("м 28.5\n2.00", "м 27.5\n2.00", 1)
    got = collector.parse_winline_team_kills_totals(text, 1, *LADDER_TEAMS)
    assert got["p1"] is None
    assert got["p2"]["ladder"] == LADDER_CH


def test_ladder_price_glued_to_text_is_null():
    # "1.72x" is not a whole price token: the block must not be read as a 4-rung ladder.
    text = (FIXTURES / LADDER_BODY).read_text()
    old = "м 30.5\n1.72\nБольше"
    assert text.count(old) >= 1
    got = collector.parse_winline_team_kills_totals(text.replace(old, "м 30.5\n1.72x\nБольше", 1), 1,
                                                  *LADDER_TEAMS)
    assert got["p1"] is None
    assert got["p2"]["ladder"] == LADDER_CH


def test_ladder_is_sorted_when_a_later_block_has_a_lower_line():
    text = (FIXTURES / LADDER_BODY).read_text()
    old = "б 31.5\n2.15\nМеньше\nм 31.5\n1.62"
    assert text.count(old) >= 1
    got = collector.parse_winline_team_kills_totals(
        text.replace(old, "б 26.5\n1.55\nМеньше\nм 26.5\n2.30", 1), 1, *LADDER_TEAMS)
    assert got["p1"]["ladder"] == [[26.5, 1.55, 2.30]] + LADDER_YS[:-1]
    assert got["p1"]["line"] == 29.5


def test_proxy_pool_inventory_filter(monkeypatch, tmp_path):
    project_proxy_file(monkeypatch, tmp_path)
    base = tmp_path / "base/keys.py"
    base.write_text("BOOKMAKER_PROXY_POOL = [\n"
                    " 'http://u:p@unknown.invalid:80',\n"
                    " 'socks5://u:p@proxy.invalid:80',\n"
                    " 'http://proxy.invalid:80',\n"
                    " 'http://u:p@proxy.invalid:80',\n"
                    " 'http://u:p@ru.invalid:80']\n"
                    "PROXY_INVENTORY = [{'ip': 'proxy.invalid', 'country': 'DE'},"
                    " {'ip': 'ru.invalid', 'country': 'RU'}]\n")
    assert collector.proxy_candidates() == [("http://u:p@proxy.invalid:80", "DE")]
    assert collector.proxy_candidates(0) == []
    assert collector.proxy_candidates(3) == [("http://u:p@proxy.invalid:80", "DE")]
    assert collector.proxy_candidates(-1) == []


@pytest.mark.parametrize("failure", ["equal", "direct-raises", "proxy-raises", "invalid"])
def test_proxy_fail_closed_before_browser(monkeypatch, capsys, tmp_path, failure):
    project_proxy_file(monkeypatch, tmp_path)
    def echo(url=None):
        if failure == "direct-raises" or (url and failure == "proxy-raises"):
            raise RuntimeError(PROXY + EXIT)
        return DIRECT if url is None or failure == "equal" else "not-an-ip"
    monkeypatch.setattr(collector, "requests_ip_echo", echo)
    factory = Mock(side_effect=AssertionError("browser must not launch"))
    monkeypatch.setattr(collector, "browser_factory", factory)
    assert collector.main(["--history", str(tmp_path / "history.jsonl")]) == 2
    factory.assert_not_called()
    output = capsys.readouterr()
    assert len(output.out.splitlines()) == 1
    assert "cards=0 events_opened=0 events_missing=0 events_unrendered=0 rows_written=0 loads=0" in output.out
    assert all(secret not in output.out + output.err for secret in (PROXY, EXIT, DIRECT, "proxy.invalid"))


class FakePage:
    def __init__(self, browser_ip=EXIT, event_failure=False):
        self.browser_ip = browser_ip
        self.event_failure = event_failure
        self.url = ""
        self.event = False
        self.opened = []

    def goto(self, url, **kwargs):
        self.url = url
        self.event = False

    def locator(self, selector):
        return self

    def on(self, event, callback):
        self.failure_callback = callback

    def inner_text(self, **kwargs):
        return body() if self.event else self.browser_ip

    def content(self):
        return gzip.decompress((FIXTURES / "winline_dota2_listing_20261005.html.gz").read_bytes()).decode()

    def wait_for_selector(self, *args, **kwargs):
        pass

    def wait_for_function(self, *args, **kwargs):
        pass

    def evaluate(self, script, event_id=None):
        if event_id is not None:
            if self.event_failure:
                raise RuntimeError(PROXY + EXIT)
            self.opened.append(event_id)
            self.event = True
            self.url = collector.LIST_URL + "/event/" + event_id
            return True
        return 0


def offline_browser(monkeypatch, page):
    browser = Mock()
    browser.new_page.return_value = page
    context = Mock()
    context.__enter__ = Mock(return_value=browser)
    context.__exit__ = Mock(return_value=False)
    factory = Mock(return_value=context)
    monkeypatch.setattr(collector, "browser_factory", factory)
    monkeypatch.setattr(collector, "proxy_candidates", lambda index=None: [(PROXY, "DE")])
    monkeypatch.setattr(collector, "requests_ip_echo", lambda url=None: EXIT if url else DIRECT)
    monkeypatch.setattr(collector.time, "sleep", lambda seconds: None)
    return factory, context


@pytest.mark.parametrize("browser_ip", [DIRECT, "", "203.0.113.9"])
def test_browser_ip_check_before_winline(monkeypatch, tmp_path, browser_ip):
    page = FakePage(browser_ip)
    _, context = offline_browser(monkeypatch, page)
    assert collector.main(["--history", str(tmp_path / "h.jsonl")]) == 2
    assert page.url == collector.IP_ECHO
    assert page.opened == []
    context.__exit__.assert_called_once()


def test_live_first_both_kinds_and_load_cap(monkeypatch, tmp_path, capsys):
    page = FakePage()
    factory, context = offline_browser(monkeypatch, page)
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [card("pre"), card("live", True)])
    path = tmp_path / "h.jsonl"
    assert collector.main(["--history", str(path), "--kinds", "all", "--max-loads", "4"]) == 0
    assert page.opened == ["live", "pre"]
    assert "cards=2 events_opened=2 events_missing=0 events_unrendered=0 rows_written=6 loads=4" in capsys.readouterr().out
    assert factory.call_args.kwargs["block_webrtc"] is True
    assert factory.call_args.kwargs["firefox_user_prefs"]["network.proxy.failover_direct"] is False
    assert factory.call_args.kwargs["proxy"]["server"] == "http://proxy.invalid:1234"
    context.__exit__.assert_called_once()
    page.opened.clear()
    assert collector.main(["--history", str(path), "--kinds", "all", "--max-loads", "2"]) == 0
    assert page.opened == ["live"]
    page.opened.clear()
    assert collector.main(["--history", str(path), "--kinds", "all", "--max-events", "1"]) == 0
    assert page.opened == ["live"]


def test_default_is_prematch_only_owner_decision(monkeypatch, tmp_path, capsys):
    """Owner 05.10: prematch pages only (every 3 h); live event pages are never opened by default."""
    page = FakePage()
    offline_browser(monkeypatch, page)
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [card("live", True), card("pre")])
    assert collector.main(["--history", str(tmp_path / "h.jsonl")]) == 0
    assert page.opened == ["pre"]
    assert "cards=2 events_opened=1 events_missing=0 events_unrendered=0 rows_written=3" in capsys.readouterr().out


def test_prematch_selection_on_captured_listing():
    html = gzip.open(FIXTURES / "winline_dota2_listing_20261005.html.gz").read().decode("utf-8")
    cards = collector.enumerate_cards(html)
    selected = collector.select_cards(cards, "prematch", 12)
    assert selected and all(not c.get("live") for c in selected)
    assert any(c["team1"] == "TEAM YANDEX" and c["team2"] == "LGD GAMING" for c in selected)
    assert len(collector.select_cards(cards, "prematch", 1)) == 1


def test_vanished_card_is_skipped_not_fatal(monkeypatch, tmp_path, capsys):
    class Vanishing(FakePage):
        def evaluate(self, script, event_id=None):
            if event_id == "gone":
                return False
            return super().evaluate(script, event_id)
    page = Vanishing()
    offline_browser(monkeypatch, page)
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [card("gone"), card("pre")])
    assert collector.main(["--history", str(tmp_path / "h.jsonl")]) == 0
    assert page.opened == ["pre"]
    assert "events_opened=1 events_missing=1" in capsys.readouterr().out


def test_no_cards(monkeypatch, tmp_path):
    offline_browser(monkeypatch, FakePage())
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [])
    path = tmp_path / "h.jsonl"
    assert collector.main(["--history", str(path)]) == 0
    assert not path.exists()


def test_midcycle_failure_stops_and_masks(monkeypatch, tmp_path, capsys):
    page = FakePage(event_failure=True)
    offline_browser(monkeypatch, page)
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [card(), card("second")])
    assert collector.main(["--history", str(tmp_path / "h.jsonl")]) == 5
    output = capsys.readouterr()
    assert "loads=2" in output.out
    assert all(secret not in output.out + output.err for secret in (PROXY, EXIT, DIRECT, "proxy.invalid"))


def test_async_proxy_failure_cannot_be_empty_feed_success(monkeypatch, tmp_path):
    page = FakePage()
    offline_browser(monkeypatch, page)
    original_goto = page.goto
    def goto(url, **kwargs):
        original_goto(url, **kwargs)
        if url == collector.LIST_URL:
            request = Mock(failure="NS_ERROR_PROXY_CONNECTION_REFUSED")
            page.failure_callback(request)
    page.goto = goto
    cards = Mock(return_value=[])
    monkeypatch.setattr(collector, "enumerate_cards", cards)
    assert collector.main(["--history", str(tmp_path / "h.jsonl")]) == 5
    cards.assert_not_called()
    assert page.opened == []


def test_load_spacing(monkeypatch):
    clock = [100.0]
    waits = []
    monkeypatch.setattr(collector.time, "monotonic", lambda: clock[0])
    def sleep(seconds):
        waits.append(seconds)
        clock[0] += seconds
    monkeypatch.setattr(collector.time, "sleep", sleep)
    loads = collector.Loads(2)
    loads.gate()
    loads.gate()
    assert waits == [3.0]
    assert loads.n == 2


def test_ip_echo_ignores_environment_proxies(monkeypatch):
    """The direct-IP baseline must be direct: HTTP(S)_PROXY env must not leak into the check (verifier M16)."""
    seen = {}

    class Session:
        trust_env = True

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def get(self, url, proxies=None, timeout=None, allow_redirects=None):
            seen.update(trust_env=self.trust_env, url=url, proxies=proxies)
            return Mock(text=" 192.0.2.1 \n", raise_for_status=Mock())

    fake_requests = ModuleType("requests")
    fake_requests.Session = Session
    monkeypatch.setitem(sys.modules, "requests", fake_requests)
    assert collector.requests_ip_echo() == "192.0.2.1"
    assert seen == {"trust_env": False, "url": collector.IP_ECHO, "proxies": {}}
    collector.requests_ip_echo(PROXY)
    assert seen["proxies"] == {"http": PROXY, "https": PROXY} and seen["trust_env"] is False


def test_unrendered_event_writes_no_rows_and_cycle_continues(monkeypatch, tmp_path, capsys):
    """Serv1 05.10: the page read before the full list rendered gave null rows (5/5). Such an event
    must write nothing (null would mean 'market absent'), and the next event is still collected."""
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

    class Slow(FakePage):
        def wait_for_function(self, script, *args, **kwargs):
            if script == collector.FULL_MARKETS_JS and self.opened and self.opened[-1] == "slow":
                raise PlaywrightTimeoutError("full list never rendered")

    page = Slow()
    offline_browser(monkeypatch, page)
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [card("slow"), card("pre")])
    path = tmp_path / "h.jsonl"
    assert collector.main(["--history", str(path)]) == 0
    assert page.opened == ["slow", "pre"]
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert {r["event_id"] for r in rows} == {"pre"} and len(rows) == 3
    assert "events_opened=2 events_missing=0 events_unrendered=1 rows_written=3" in capsys.readouterr().out


def test_full_market_marker_on_captured_bodies():
    """The 'Все' line separates the captured unrendered body from the full one (same event 16855095)."""
    unrendered = (FIXTURES / "winline_kills_totals_unrendered_popular_only_aurora_1w_20261005.txt").read_text()
    full = body("aurora_1w")
    marker = re.compile(r"(^|\n)Все(\n|$)")
    assert "(^|\\n)Все(\\n|$)" in collector.FULL_MARKETS_JS
    assert not marker.search(unrendered) and marker.search(full)
    rows = collector.build_rows(card("16855095"), unrendered, wall=1)
    assert [r["map_num"] for r in rows] == [1]
    assert all(r[k] is None for r in rows for k in collector.VALUE_FIELDS)
    full_rows = collector.build_rows(dict(card("16855095"), team1="TEAM AURORA", team2="1W"), full, wall=1)
    assert [r["kills_t1_line"] for r in full_rows] == [28.5, 28.5, 28.5]


# Captured prod overview 08.10.2026 18:34 MSK (provenance beside the fixture):
# BLAST Slam player-duel kill props ("SKITER (TEAM AURORA) PURE (1W)") share the
# listing with real maps. Before card ingame-vl91 the collector opened them as
# team kill totals: 18 of 116 serv1 history rows were duels (lines 6.5/7.5), and
# each one spent a slot of the --max-events budget.
DUEL_OVERVIEW = ROOT / "base/tests/fixtures/winline_overview_snapshot_20261008_blast_duel_cards.json"


def _duel_overview_cards():
    html = json.loads(DUEL_OVERVIEW.read_text(encoding="utf-8"))["html"]
    cards = collector.enumerate_cards(html)
    assert sum(1 for c in cards if c.get("prop_duel")) == 4  # the parser still sees them
    return cards


@pytest.mark.parametrize("kinds,expected", [
    ("prematch", [("PARIVISION", "TEAM YANDEX")]),
    ("live", []),
    ("all", [("PARIVISION", "TEAM YANDEX")]),
])
def test_player_duel_prop_cards_are_never_selected(kinds, expected):
    selected = collector.select_cards(_duel_overview_cards(), kinds, 12)
    assert [(c["team1"], c["team2"]) for c in selected] == expected


def test_player_duel_prop_cards_do_not_consume_the_event_budget():
    # 'all' puts live cards first; the three live cards here are all duels.
    selected = collector.select_cards(_duel_overview_cards(), "all", 1)
    assert [(c["team1"], c["team2"]) for c in selected] == [("PARIVISION", "TEAM YANDEX")]
