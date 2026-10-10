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

QUICK_FIXTURE = "winline_quick_kill_windows_legion_blasterbi_20261008.txt"
QUICK_DUMP = "16890543_20261008T184904Z_quick.txt"
ALL_FIXTURE = "winline_kills_map_totals_live_parivision_aurora_20261010.txt"
ALL_DUMP = "16899998_20261010T144346Z_all.txt"
NEW_TOTAL_FIELDS = ("kills_map_line", "kills_map_over", "kills_map_under", "kills_map_ladder",
                    "kills_map_bands", "kills_t1_bands", "kills_t2_bands")
QUICK_MARKETS = [
    dict(map_num=3, window="5-15", market="1x2", p_t1=1.85, p_draw=7.63, p_t2=2.23),
    dict(map_num=3, window="5-15", market="odd_even", p_even=1.85, p_odd=1.85),
    dict(map_num=3, window="5-15", market="handicap",
         line_t1=-0.5, p_t1=1.85, line_t2=0.5, p_t2=1.85),
]


def quick_body():
    return (FIXTURES / QUICK_FIXTURE).read_text(encoding="utf-8")


def quick_card():
    return dict(event_id="16890543", team1="LEGION", team2="BLASTERBI", live=True)


def all_body():
    return (FIXTURES / ALL_FIXTURE).read_text(encoding="utf-8")


def all_entry():
    return dict(event_id="16899998", team1="PARIVISION", team2="TEAM AURORA", live=True,
                ts_utc="20261010T144346Z", quick_clicked=False)


@pytest.mark.parametrize("map_num", [1, 2, 3])
def test_map_total_prematch_rows(map_num):
    rows = collector.build_rows(dict(card(), team1="TEAM AURORA", team2="1W"), body("aurora_1w"), wall=10)
    row = next(r for r in rows if r["map_num"] == map_num)
    last_rung = [54.5, 2.02, 1.80] if map_num == 1 else [54.5, 2.00, 1.82]
    assert row["kills_map_ladder"] == [[52.5, 1.81, 2.01], [53.5, 1.90, 1.90], last_rung]
    assert [row["kills_map_" + key] for key in ("line", "over", "under")] == [53.5, 1.90, 1.90]


def test_map_total_preserves_existing_aurora_row_fields():
    rows = collector.build_rows(dict(event_id="16855095", team1="TEAM AURORA", team2="1W", live=False),
                                body("aurora_1w"), wall=10)
    for row, map_num, over, under in zip(rows, (1, 2, 3), (1.93, 1.94, 1.94), (1.88, 1.87, 1.87)):
        expected = dict(wall=10, event_id="16855095", kind="prematch", league="", team1="TEAM AURORA",
                        team2="1W", map_num=map_num, source="winline_event_page",
                        kills_t1_line=28.5, kills_t1_over=1.87, kills_t1_under=1.94,
                        kills_t1_ladder=[[28.5, 1.87, 1.94]], kills_t2_line=27.5,
                        kills_t2_over=over, kills_t2_under=under, kills_t2_ladder=[[27.5, over, under]])
        assert {k: v for k, v in row.items() if k not in NEW_TOTAL_FIELDS} == expected


@pytest.mark.parametrize("map_num", [2, 3])
def test_map_total_live_rows(map_num):
    rows = collector.build_rows(all_entry(), all_body(), wall=10)
    assert [r["map_num"] for r in rows] == [2, 3]
    row = next(r for r in rows if r["map_num"] == map_num)
    assert row["kills_map_ladder"] == [[56.5, 1.75, 1.96], [57.5, 1.85, 1.85], [58.5, 1.95, 1.76]]
    assert [row["kills_map_" + key] for key in ("line", "over", "under")] == [57.5, 1.85, 1.85]


@pytest.mark.parametrize("map_num", [1, 2, 3])
def test_map_total_count_bands_rows(map_num):
    text = (FIXTURES / LADDER_BODY).read_text()
    row = next(r for r in collector.build_rows(ladder_card(), text, wall=10) if r["map_num"] == map_num)
    prices = {
        1: ((5.15, 3.38, 3.25, 4.81, 5.15), (3.86, 2.81, 2.58, 6.29), (2.49, 3.36, 3.34, 6.17)),
        2: ((5.15, 3.37, 3.26, 4.81, 5.15), (3.89, 2.81, 2.58, 6.29), (2.48, 3.37, 3.36, 6.17)),
        3: ((5.18, 3.38, 3.25, 4.81, 5.15), (3.85, 2.82, 2.59, 6.33), (2.51, 3.36, 3.33, 6.17)),
    }[map_num]
    for target, bounds, odds in zip(
            ("map", "t1", "t2"),
            (((0, 40), (41, 50), (51, 60), (61, 70), (71, None)),
             ((0, 20), (21, 30), (31, 40), (41, None)), ((0, 20), (21, 30), (31, 40), (41, None))),
            prices):
        assert row["kills_%s_bands" % target] == [dict(lo=lo, hi=hi, odds=price)
                                                 for (lo, hi), price in zip(bounds, odds)]


def test_map_total_absent_rows_keep_all_maps():
    text = (FIXTURES / "winline_live_no_kills_totals_maddogs_20261005.txt").read_text()
    rows = collector.build_rows(card(live=True), text, wall=10)
    assert [r["map_num"] for r in rows] == [1, 2, 3]
    assert all(row[key] is None for row in rows for key in NEW_TOTAL_FIELDS)


def test_map_total_exact_heading_traps_and_duplicate():
    text = body("aurora_1w")
    # Leave the captured team and Roshan blocks intact, remove only the bare map titles.
    trapped = re.sub(r"(?m)^([1-3] карта тотал убийств)$", r"\1 рошана", text)
    rows = collector.build_rows(dict(card(), team1="TEAM AURORA", team2="1W"), trapped, wall=10)
    assert all(row["kills_map_ladder"] is None for row in rows)
    assert rows[0]["kills_t1_line"] == 28.5
    duplicated = collector.build_rows(card(), text + "\n" + text, wall=10)
    assert all(row["kills_map_ladder"] is None for row in duplicated)
    assert collector.parse_winline_map_kills_total(trapped, 1) is None


def test_map_total_bands_exact_team_names_and_malformed_block():
    text = (FIXTURES / LADDER_BODY).read_text()
    rows = collector.build_rows(dict(ladder_card(), team1=" yellow  submarine ", team2="CYBER"), text, wall=10)
    assert all(row["kills_t1_bands"] and row["kills_t2_bands"] is None for row in rows)
    malformed = collector.build_rows(ladder_card(), text.replace("0-40\n5.15", "0-40\n1.0", 1), wall=10)
    assert malformed[0]["kills_map_bands"] is None and malformed[0]["kills_t1_bands"]
    assert malformed[1]["kills_map_bands"] and malformed[2]["kills_map_bands"]


def test_map_total_backfill_is_offline_and_idempotent(monkeypatch, tmp_path, capsys):
    entry = all_entry()
    (tmp_path / ALL_DUMP).write_bytes((FIXTURES / ALL_FIXTURE).read_bytes())
    failed = dict(entry, kind="readyfail", ts_utc="20261010T144446Z")
    (tmp_path / "16899998_20261010T144446Z_all.txt").write_text(all_body())
    missing = dict(entry, ts_utc="20261010T144546Z")
    orphan = tmp_path / "16899998_20261010T144646Z_all.txt"
    orphan.write_text(all_body())
    (tmp_path / "index.jsonl").write_text("\n".join(json.dumps(e) for e in (failed, entry, missing, entry)) + "\n")
    # Existing windows and an earlier totals capture are preserved byte for byte.
    windows = tmp_path / "windows.jsonl"
    windows.write_text(json.dumps(dict(QUICK_MARKETS[0], source_dump=QUICK_DUMP)) + "\n")
    old = json.dumps(dict(source_dump="earlier_all.txt", map_num=1)) + "\n"
    totals = tmp_path / "totals.jsonl"
    totals.write_text(old)
    monkeypatch.setattr(collector, "_cycle", Mock(side_effect=AssertionError("offline only")))
    assert collector.main(["--parse-all-dumps", str(tmp_path)]) == 0
    assert capsys.readouterr().out == "totals_written=2\n"
    assert totals.read_text().startswith(old)
    rows = [json.loads(line) for line in totals.read_text().splitlines()[1:]]
    assert [row["map_num"] for row in rows] == [2, 3]
    assert all(row["source_dump"] == ALL_DUMP and row["ts_utc"] == entry["ts_utc"]
               and row["live"] is True and row["event_id"] == entry["event_id"]
               and row["team1"] == entry["team1"] and row["team2"] == entry["team2"]
               and row["series_score"] == "1 : 0" and row["series_state"] == "Пер."
               and row["wall"] == 1791643426.0 and row["kills_map_line"] == 57.5 for row in rows)
    before = totals.read_bytes(), windows.read_bytes()
    assert collector.parse_all_dumps(tmp_path) == 0
    assert (totals.read_bytes(), windows.read_bytes()) == before


@pytest.mark.parametrize("clicked", [False, True])
def test_map_total_quick_capture_appends_totals_and_preserves_windows(monkeypatch, tmp_path, clicked):
    page = Mock()
    page.evaluate.return_value = dict(clicked=clicked, tabs=["Все", "Быстрые"])
    page.locator.return_value.inner_text.return_value = quick_body()
    monkeypatch.setattr(collector.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(collector.time, "strftime", lambda *args: all_entry()["ts_utc"])
    check = Mock()
    collector.capture_quick_tab(page, tmp_path, all_entry(), all_body(), True, (), check)
    entry = json.loads((tmp_path / "index.jsonl").read_text())
    rows = [json.loads(line) for line in (tmp_path / "totals.jsonl").read_text().splitlines()]
    expected = collector.build_rows(entry, all_body(), wall=1791643426.0)
    assert rows == [dict(row, ts_utc=entry["ts_utc"], live=True, full_markets=True, source_dump=ALL_DUMP,
                         series_score="1 : 0", series_state="Пер.") for row in expected]
    check.assert_called_once_with()
    if clicked:
        expected_windows = collector.build_quick_window_rows(entry, quick_body(), ALL_DUMP.replace("_all", "_quick"))
        assert (tmp_path / "windows.jsonl").read_text() == "".join(
            json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n" for row in expected_windows)
    else:
        assert not (tmp_path / "windows.jsonl").exists()


def test_captured_quick_windows():
    assert "−0.5" in quick_body()  # The actual capture uses U+2212.
    assert collector.parse_winline_quick_windows(quick_body(), "LEGION", "BLASTERBI") == QUICK_MARKETS


def test_quick_windows_generic_bounds_and_exact_orientation():
    text = quick_body().replace("5-15 минут", "10-20 минут").replace("1.85", "1,85")
    rows = collector.parse_winline_quick_windows(text, " blasterbi ", " legion ")
    assert all(r["window"] == "10-20" for r in rows)
    assert (rows[0]["p_t1"], rows[0]["p_t2"]) == (2.23, 1.85)
    assert (rows[2]["line_t1"], rows[2]["line_t2"]) == (0.5, -0.5)
    traps = collector.parse_winline_quick_windows(text, "LEG", "BLASTERBI")
    assert traps[0]["p_t1"] is None and traps[0]["p_t2"] is None
    assert traps[2]["line_t1"] is None and traps[2]["p_t2"] is None


def test_quick_windows_multiple_windows_and_duplicate_titles():
    text = quick_body()
    section = text[text.index("3 карта исход 1X2 убийств"):text.index("\nWINLINE")]
    rows = collector.parse_winline_quick_windows(
        text.replace("\nWINLINE", "\n" + section.replace("5-15 минут", "15-25 минут") + "\nWINLINE"),
        "LEGION", "BLASTERBI")
    assert rows == QUICK_MARKETS + [dict(r, window="15-25") for r in QUICK_MARKETS]
    first_block = section[:section.index("3 карта чет/нечет")]
    rows = collector.parse_winline_quick_windows(
        text.replace("3 карта чет/нечет", first_block + "3 карта чет/нечет", 1), "LEGION", "BLASTERBI")
    assert len(rows) == 3
    assert all(rows[0][field] is None for field in ("p_t1", "p_draw", "p_t2"))
    assert rows[1:] == QUICK_MARKETS[1:]


@pytest.mark.parametrize("old,new,market,fields", [
    ("LEGION\n1.85\nНичья", "LEGION\nНичья", "1x2", ("p_t1", "p_draw", "p_t2")),
    ("Чет\n1.85\nНечет", "Чет\nНечет", "odd_even", ("p_even", "p_odd")),
    ("−0.5\n1.85\nBLASTERBI", "−0.5\nBLASTERBI", "handicap",
     ("line_t1", "p_t1", "line_t2", "p_t2")),
    ("Ничья\n7.63", "Ничья\n1.0", "1x2", ("p_t1", "p_draw", "p_t2")),
])
def test_malformed_quick_window_block_is_null(old, new, market, fields):
    text = quick_body()
    assert text.count(old) == 1
    rows = collector.parse_winline_quick_windows(text.replace(old, new, 1), "LEGION", "BLASTERBI")
    broken, = [r for r in rows if r["market"] == market]
    assert all(broken[f] is None for f in fields)
    assert [r for r in rows if r["market"] != market] == [r for r in QUICK_MARKETS if r["market"] != market]


def test_quick_dump_backfill_is_offline_and_idempotent(monkeypatch, tmp_path, capsys):
    entry = dict(quick_card(), ts_utc="20261008T184904Z", quick_clicked=True)
    (tmp_path / QUICK_DUMP).write_bytes((FIXTURES / QUICK_FIXTURE).read_bytes())
    (tmp_path / "index.jsonl").write_text(json.dumps(entry) + "\n")
    monkeypatch.setattr(collector, "_cycle", Mock(side_effect=AssertionError("offline only")))
    assert collector.main(["--parse-quick-dumps", str(tmp_path)]) == 0
    assert capsys.readouterr().out == "windows_written=3\n"
    rows = [json.loads(line) for line in (tmp_path / "windows.jsonl").read_text().splitlines()]
    expected = [dict(r, ts_utc=entry["ts_utc"], source_dump=QUICK_DUMP, **quick_card()) for r in QUICK_MARKETS]
    assert rows == expected
    assert collector.parse_quick_dumps(tmp_path) == 0
    assert len((tmp_path / "windows.jsonl").read_text().splitlines()) == 3
    # A failed click must never backfill a different, otherwise eligible stale dump.
    no_click = dict(entry, ts_utc="20261008T185904Z", quick_clicked=False)
    (tmp_path / "16890543_20261008T185904Z_quick.txt").write_text(quick_body())
    (tmp_path / "index.jsonl").write_text(json.dumps(no_click) + "\n")
    assert collector.parse_quick_dumps(tmp_path) == 0

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
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [card("16855432"), card("16855433", True)])
    path = tmp_path / "h.jsonl"
    assert collector.main(["--history", str(path), "--kinds", "all", "--max-loads", "4"]) == 0
    assert page.opened == ["16855433", "16855432"]
    assert "cards=2 events_opened=2 events_missing=0 events_unrendered=0 rows_written=6 loads=4" in capsys.readouterr().out
    assert factory.call_args.kwargs["block_webrtc"] is True
    assert factory.call_args.kwargs["firefox_user_prefs"]["network.proxy.failover_direct"] is False
    assert factory.call_args.kwargs["proxy"]["server"] == "http://proxy.invalid:1234"
    context.__exit__.assert_called_once()
    page.opened.clear()
    assert collector.main(["--history", str(path), "--kinds", "all", "--max-loads", "2"]) == 0
    assert page.opened == ["16855433"]
    page.opened.clear()
    assert collector.main(["--history", str(path), "--kinds", "all", "--max-events", "1"]) == 0
    assert page.opened == ["16855433"]


def test_default_is_prematch_only_owner_decision(monkeypatch, tmp_path, capsys):
    """Owner 05.10: prematch pages only (every 3 h); live event pages are never opened by default."""
    page = FakePage()
    offline_browser(monkeypatch, page)
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [card("16855433", True), card("16855432")])
    assert collector.main(["--history", str(tmp_path / "h.jsonl")]) == 0
    assert page.opened == ["16855432"]
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
            if event_id == "16855430":
                return False
            return super().evaluate(script, event_id)
    page = Vanishing()
    offline_browser(monkeypatch, page)
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [card("16855430"), card("16855432")])
    assert collector.main(["--history", str(tmp_path / "h.jsonl")]) == 0
    assert page.opened == ["16855432"]
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
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [card(), card("16855432")])
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
            if script == collector.FULL_MARKETS_JS and self.opened and self.opened[-1] == "16855430":
                raise PlaywrightTimeoutError("full list never rendered")

    page = Slow()
    offline_browser(monkeypatch, page)
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [card("16855430"), card("16855432")])
    path = tmp_path / "h.jsonl"
    assert collector.main(["--history", str(path)]) == 0
    assert page.opened == ["16855430", "16855432"]
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert {r["event_id"] for r in rows} == {"16855432"} and len(rows) == 3
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


# Since 15695442 (card ingame-fgbt) the listing parser also returns the overview hero block -
# the pinned live real match TEAM AURORA - 1W (event 16855095, not a duel) - so it is the one
# live card here; the four duel props stay excluded.
@pytest.mark.parametrize("kinds,expected", [
    ("prematch", [("PARIVISION", "TEAM YANDEX")]),
    ("live", [("TEAM AURORA", "1W")]),
    ("all", [("TEAM AURORA", "1W"), ("PARIVISION", "TEAM YANDEX")]),
])
def test_player_duel_prop_cards_are_never_selected(kinds, expected):
    selected = collector.select_cards(_duel_overview_cards(), kinds, 12)
    assert [(c["team1"], c["team2"]) for c in selected] == expected


def test_player_duel_prop_cards_do_not_consume_the_event_budget():
    # 'all' puts live cards first; the three live listing cards here are all duels, the hero
    # (TEAM AURORA - 1W) is the only real live card: two slots go to it and the prematch card.
    selected = collector.select_cards(_duel_overview_cards(), "all", 2)
    assert [(c["team1"], c["team2"]) for c in selected] == [
        ("TEAM AURORA", "1W"), ("PARIVISION", "TEAM YANDEX")]


class QuickPage(FakePage):
    """Only tab interaction is synthetic; default markets use captured bodies."""
    def __init__(self, quick_present=True, full_markets=True):
        super().__init__()
        self.quick_present = quick_present
        self.full_markets = full_markets
        self.quick = False
        self.tab_calls = 0
        self.gotos = []
        self.default_text = body() if full_markets else (
            FIXTURES / "winline_live_no_kills_totals_maddogs_20261005.txt").read_text()
        # A fake tab body, never used as a parser fixture.
        self.quick_text = "Быстрые\nFAKE QUICK TAB BODY"
        self.before_click = None

    def goto(self, url, **kwargs):
        self.gotos.append(url)
        self.quick = False
        super().goto(url, **kwargs)

    def inner_text(self, **kwargs):
        if not self.event:
            return super().inner_text(**kwargs)
        assert kwargs["timeout"] == 20000
        return self.quick_text if self.quick else self.default_text

    def wait_for_function(self, script, *args, **kwargs):
        if script == collector.FULL_MARKETS_JS and not self.full_markets:
            from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
            raise PlaywrightTimeoutError(PROXY + EXIT)

    def evaluate(self, script, event_id=None):
        if event_id is None and script == getattr(collector, "QUICK_TAB_JS", None):
            self.tab_calls += 1
            if self.before_click:
                self.before_click()
            self.quick = self.quick_present
            tabs = ["Все"] if self.full_markets else []
            if self.quick_present:
                tabs.append("бЫсТрЫе")
            return dict(tabs=tabs, clicked=self.quick)
        return super().evaluate(script, event_id)


@pytest.mark.parametrize("quick_present", [True, False])
def test_quick_mode_writes_window_rows(monkeypatch, tmp_path, quick_present):
    page = QuickPage(quick_present=quick_present)
    page.quick_text = quick_body()
    offline_browser(monkeypatch, page)
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [quick_card()])
    dump_dir = tmp_path / "quick"
    assert collector.main(["--history", str(tmp_path / "h.jsonl"), "--kinds", "live",
                           "--quick-dump-dir", str(dump_dir)]) == 0
    if not quick_present:
        assert not (dump_dir / "windows.jsonl").exists()
        return
    entry = json.loads((dump_dir / "index.jsonl").read_text())
    dump, = dump_dir.glob("*_quick.txt")
    rows = [json.loads(line) for line in (dump_dir / "windows.jsonl").read_text().splitlines()]
    assert rows == [dict(r, ts_utc=entry["ts_utc"], source_dump=dump.name, **quick_card()) for r in QUICK_MARKETS]
    assert collector.parse_quick_dumps(dump_dir) == 0


def test_captured_hero_event_id():
    html = json.loads(DUEL_OVERVIEW.read_text(encoding="utf-8"))["html"]
    assert collector.hero_event_id(html) == "16855095"


@pytest.mark.parametrize("html,expected", [
    ('<img src="/api/cls/event/1/16855095">', None),
    ('<ww-feature-event-live-center-dsk></ww-feature-event-live-center-dsk>'
     '<img src="/api/cls/event/1/16855095">', None),
    ('<img src="/api/cls/event/1/999">'
     '<ww-feature-event-live-center-dsk><img src="/api/cls/event/2/16855095">'
     '<img src="/api/cls/event/3/888"></ww-feature-event-live-center-dsk>', "16855095"),
])
def test_hero_event_id_is_scoped_to_hero(html, expected):
    assert collector.hero_event_id(html) == expected


class HeroReloadPage(QuickPage):
    hero_id = "16855095"

    def __init__(self, hero_first=False, ready_error=None, hero_click=True):
        super().__init__()
        self.hero_first = hero_first
        self.ready_error = ready_error
        self.hero_click = hero_click
        self.listing_loads = 0
        self.hero_calls = 0
        self.ready_calls = []
        self.quick_text = quick_body()

    def goto(self, url, **kwargs):
        super().goto(url, **kwargs)
        if url == collector.LIST_URL:
            self.listing_loads += 1

    def feed_ids(self):
        ids = ["16855431", "16855433"]
        if self.listing_loads == 1:
            ids.insert(0 if self.hero_first else 1, self.hero_id)
        return ids

    def content(self):
        return ('<ww-feature-event-live-center-dsk>'
                '<img src="https://winline.ru/api/cls/event/123/%s">'
                '<button class="fast-bets__all-markets">Все маркеты</button>'
                '</ww-feature-event-live-center-dsk>') % self.hero_id

    def wait_for_selector(self, selector, **kwargs):
        if selector == "#eventId-" + self.hero_id:
            from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
            raise PlaywrightTimeoutError("hero has no feed element")

    def evaluate(self, script, event_id=None):
        if script == collector.OPEN_EVENT_JS and event_id == self.hero_id:
            return False
        if script == getattr(collector, "OPEN_HERO_EVENT_JS", None):
            self.hero_calls += 1
            if self.hero_click:
                super().evaluate(collector.OPEN_EVENT_JS, self.hero_id)
            return self.hero_click
        return super().evaluate(script, event_id)

    def wait_for_function(self, script, *args, **kwargs):
        if script == collector.EVENT_READY_JS:
            self.ready_calls.append((self.opened[-1], kwargs))
            if self.opened[-1] == self.hero_id and self.ready_error:
                raise self.ready_error
        return super().wait_for_function(script, *args, **kwargs)


def hero_reload_browser(monkeypatch, **kwargs):
    page = HeroReloadPage(**kwargs)
    offline_browser(monkeypatch, page)
    enumerate_cards = Mock(side_effect=lambda html: [card(id_, True) for id_ in page.feed_ids()])
    monkeypatch.setattr(collector, "enumerate_cards", enumerate_cards)
    return page


@pytest.mark.parametrize("hero_first", [False, True], ids=["reload", "initial"])
def test_hero_card_opens_and_captures(monkeypatch, tmp_path, capsys, hero_first):
    page = hero_reload_browser(monkeypatch, hero_first=hero_first)
    history = tmp_path / "h.jsonl"
    dump_dir = tmp_path / "quick"
    assert collector.main(["--history", str(history), "--kinds", "live", "--max-events", "2",
                           "--max-loads", "4", "--quick-dump-dir", str(dump_dir)]) == 0
    expected = [page.hero_id, "16855431"] if hero_first else ["16855431", page.hero_id]
    assert page.opened == expected
    assert page.hero_calls == 1 and page.listing_loads == 2
    assert len(list(dump_dir.glob("*_quick.txt"))) == 2
    rows = [json.loads(line) for line in history.read_text().splitlines()]
    assert len(rows) == 6 and {r["event_id"] for r in rows} == set(expected)
    assert all(kwargs == dict(arg=[collector.LIST_URL, card()["team1"], card()["team2"]],
                              timeout=30000) for _, kwargs in page.ready_calls)
    output = capsys.readouterr().out
    assert "events_missing=0" in output and "loads=4" in output
    assert "events_left_listing=0 events_not_clickable=0" in output
    assert "events_opened_hero=1 events_hero_open_failed=0" in output


def test_hero_ready_timeout_continues_to_next_card(monkeypatch, tmp_path, capsys):
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    page = hero_reload_browser(monkeypatch, ready_error=PlaywrightTimeoutError(PROXY + EXIT))
    history = tmp_path / "h.jsonl"
    assert collector.main(["--history", str(history), "--kinds", "live",
                           "--max-events", "3", "--max-loads", "6"]) == 0
    assert page.opened == ["16855431", page.hero_id, "16855433"]
    assert page.hero_calls == 1 and page.listing_loads == 3
    rows = [json.loads(line) for line in history.read_text().splitlines()]
    assert len(rows) == 6 and {r["event_id"] for r in rows} == {"16855431", "16855433"}
    output = capsys.readouterr()
    assert "events_opened_hero=0 events_hero_open_failed=1" in output.out
    assert "events_missing=0" in output.out and "status=0 error=-" in output.out
    assert "loads=6" in output.out and "events_left_listing=0" in output.out
    assert all(secret not in output.out + output.err for secret in (PROXY, EXIT, DIRECT, "proxy.invalid"))


def test_hero_ready_non_timeout_aborts_and_masks(monkeypatch, tmp_path, capsys):
    page = hero_reload_browser(monkeypatch, ready_error=RuntimeError(PROXY + EXIT))
    assert collector.main(["--history", str(tmp_path / "h.jsonl"), "--kinds", "live"]) == 5
    assert page.opened == ["16855431", page.hero_id]
    output = capsys.readouterr()
    assert "status=5 error=RuntimeError" in output.out
    assert "events_hero_open_failed=0" in output.out
    assert all(secret not in output.out + output.err for secret in (PROXY, EXIT, DIRECT, "proxy.invalid"))


def test_hero_button_absent_keeps_missing_counters(monkeypatch, tmp_path, capsys):
    page = hero_reload_browser(monkeypatch, hero_click=False)
    assert collector.main(["--history", str(tmp_path / "h.jsonl"), "--kinds", "live"]) == 0
    assert page.opened == ["16855431", "16855433"] and page.hero_calls == 1
    output = capsys.readouterr().out
    assert "events_missing=1" in output and "events_left_listing=1 events_not_clickable=0" in output
    assert "events_opened_hero=0 events_hero_open_failed=0" in output


class LiveReloadPage(QuickPage):
    """The second selected live card can render late or leave on listing reload."""
    def __init__(self, second_state, clock):
        super().__init__()
        self.second_state = second_state
        self.clock = clock
        self.listing_loads = 0
        self.second_ready = False
        self.card_waits = []
        self.open_attempts = []
        self.quick_text = quick_body()

    def goto(self, url, **kwargs):
        super().goto(url, **kwargs)
        if url == collector.LIST_URL:
            self.listing_loads += 1

    def content(self):
        ids = ["16855431", "16855432", "16855433"]
        if self.listing_loads > 1 and self.second_state == "left":
            ids.remove("16855432")
        return json.dumps(ids)

    def wait_for_selector(self, selector, **kwargs):
        if selector == "[id^=eventId-]":
            return
        self.card_waits.append((selector, kwargs["timeout"]))
        if selector == "#eventId-16855432":
            self.clock[0] += 6.6
            if self.second_state == "left":
                from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
                raise PlaywrightTimeoutError("card left")
            if self.second_state == "error":
                raise RuntimeError(PROXY + EXIT)
            self.second_ready = True

    def evaluate(self, script, event_id=None):
        if script == collector.OPEN_EVENT_JS:
            self.open_attempts.append(event_id)
            if event_id == "16855432" and (
                    not self.second_ready or self.second_state == "not-clickable"):
                return False
        return super().evaluate(script, event_id)


def live_reload_browser(monkeypatch, second_state):
    clock = [100.0]
    page = LiveReloadPage(second_state, clock)
    offline_browser(monkeypatch, page)
    monkeypatch.setattr(collector.time, "monotonic", lambda: clock[0])
    enumerate_cards = Mock(side_effect=lambda html: [card(id_, True) for id_ in json.loads(html)])
    monkeypatch.setattr(collector, "enumerate_cards", enumerate_cards)
    return page, enumerate_cards


def test_second_live_card_waits_for_delayed_render(monkeypatch, tmp_path, capsys):
    page, enumerate_cards = live_reload_browser(monkeypatch, "delayed")
    dump_dir = tmp_path / "quick"
    assert collector.main(["--history", str(tmp_path / "h.jsonl"), "--kinds", "live",
                           "--max-events", "2", "--max-loads", "4",
                           "--quick-dump-dir", str(dump_dir)]) == 0
    assert page.opened == ["16855431", "16855432"]
    assert page.card_waits == [("#eventId-16855432", 15000)]
    assert page.listing_loads == 2 and enumerate_cards.call_count == 1
    assert len(list(dump_dir.glob("*_quick.txt"))) == 2
    output = capsys.readouterr().out
    assert "events_missing=0" in output and "loads=4" in output
    assert "events_left_listing=0 events_not_clickable=0 card_wait_seconds_max=7" in output


def test_second_live_card_left_listing_continues_to_third(monkeypatch, tmp_path, capsys):
    page, enumerate_cards = live_reload_browser(monkeypatch, "left")
    assert collector.main(["--history", str(tmp_path / "h.jsonl"), "--kinds", "live",
                           "--max-events", "3", "--max-loads", "6"]) == 0
    assert page.opened == ["16855431", "16855433"]
    assert page.card_waits == [("#eventId-16855432", 15000), ("#eventId-16855433", 15000)]
    assert page.listing_loads == 3 and enumerate_cards.call_count == 2
    output = capsys.readouterr().out
    assert "events_opened=2 events_missing=1" in output and "loads=6" in output
    assert "events_left_listing=1 events_not_clickable=0" in output


def test_second_live_card_present_but_not_clickable(monkeypatch, tmp_path, capsys):
    page, enumerate_cards = live_reload_browser(monkeypatch, "not-clickable")
    assert collector.main(["--history", str(tmp_path / "h.jsonl"), "--kinds", "live",
                           "--max-events", "3", "--max-loads", "6"]) == 0
    assert page.opened == ["16855431", "16855433"]
    assert page.open_attempts.count("16855432") == 2
    assert page.listing_loads == 3 and enumerate_cards.call_count == 2
    output = capsys.readouterr().out
    assert "events_missing=1" in output and "loads=6" in output
    assert "events_left_listing=0 events_not_clickable=1" in output


def test_second_live_card_wait_error_aborts_and_masks(monkeypatch, tmp_path, capsys):
    page, enumerate_cards = live_reload_browser(monkeypatch, "error")
    assert collector.main(["--history", str(tmp_path / "h.jsonl"), "--kinds", "live"]) == 5
    assert page.opened == ["16855431"] and enumerate_cards.call_count == 1
    assert "16855432" not in page.open_attempts
    output = capsys.readouterr()
    assert "status=5 error=RuntimeError" in output.out
    assert all(secret not in output.out + output.err for secret in (PROXY, EXIT, DIRECT, "proxy.invalid"))


def test_non_digit_event_id_never_waited_or_clicked(monkeypatch, tmp_path, capsys):
    page, _ = live_reload_browser(monkeypatch, "delayed")
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [
        card("16855431", True), card("1,body", True), card("16855433", True)])
    assert collector.main(["--history", str(tmp_path / "h.jsonl"), "--kinds", "live"]) == 0
    assert page.opened == ["16855431", "16855433"]
    assert page.card_waits == [("#eventId-16855433", 15000)]
    assert "1,body" not in page.open_attempts and page.listing_loads == 2
    assert "events_missing=1" in capsys.readouterr().out


@pytest.mark.parametrize("quick_present", [True, False], ids=["clicked", "no-click"])
def test_quick_dump_connection_failure_aborts_capture(monkeypatch, tmp_path, capsys, quick_present):
    page = QuickPage(quick_present=quick_present)
    _, context = offline_browser(monkeypatch, page)
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [card()])

    def fail_connection():
        page.failure_callback(Mock(failure="net::err_connection_reset"))

    if quick_present:
        original_inner_text = page.inner_text

        def inner_text(**kwargs):
            text = original_inner_text(**kwargs)
            if page.quick:
                fail_connection()
            return text

        monkeypatch.setattr(page, "inner_text", inner_text)
    else:
        page.before_click = fail_connection

    dump_dir = tmp_path / "quick"
    history = tmp_path / "h.jsonl"
    assert collector.main(["--history", str(history), "--quick-dump-dir", str(dump_dir)]) == 5
    assert page.tab_calls == 1 and page.opened == ["16855431"]
    all_file, = dump_dir.glob("*_all.txt")
    assert all_file.read_text() == page.default_text
    assert not list(dump_dir.glob("*_quick.txt"))
    assert not (dump_dir / "index.jsonl").exists()
    assert not (dump_dir / "windows.jsonl").exists()
    assert not list(dump_dir.glob("*.tmp"))
    # Default history is committed before the quick-tab connection fails.
    assert len(history.read_text().splitlines()) == 3
    assert capsys.readouterr().out == (
        "cards=1 events_opened=1 events_missing=0 events_unrendered=0 rows_written=3 "
        "loads=2 country=DE status=5 error=RuntimeError "
        "events_left_listing=0 events_not_clickable=0 card_wait_seconds_max=0 "
        "events_opened_hero=0 events_hero_open_failed=0 events_ready_timeout=0\n")
    context.__exit__.assert_called_once()


@pytest.mark.parametrize("quick_mode", [False, True], ids=["default", "quick"])
def test_unrendered_event_skips_expand_and_default_body(monkeypatch, tmp_path, capsys, quick_mode):
    monkeypatch.delenv("WINLINE_QUICK_DUMP_DIR", raising=False)
    page = QuickPage(full_markets=False)
    offline_browser(monkeypatch, page)
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [card()])
    evaluate = Mock(wraps=page.evaluate)
    inner_text = Mock(wraps=page.inner_text)
    monkeypatch.setattr(page, "evaluate", evaluate)
    monkeypatch.setattr(page, "inner_text", inner_text)
    history = tmp_path / "h.jsonl"
    dump_dir = tmp_path / "quick"
    args = ["--history", str(history)]
    if quick_mode:
        args += ["--quick-dump-dir", str(dump_dir)]

    assert collector.main(args) == 0
    assert collector.EXPAND_JS not in [call.args[0] for call in evaluate.call_args_list]
    assert not history.exists()
    assert capsys.readouterr().out == (
        "cards=1 events_opened=1 events_missing=0 events_unrendered=1 rows_written=0 "
        "loads=2 country=DE status=0 error=- "
        "events_left_listing=0 events_not_clickable=0 card_wait_seconds_max=0 "
        "events_opened_hero=0 events_hero_open_failed=0 events_ready_timeout=0\n")
    if quick_mode:
        assert page.tab_calls == 1
        assert inner_text.call_count == 3  # IP echo, default body, quick body.
        assert len((dump_dir / "index.jsonl").read_text().splitlines()) == 1
    else:
        inner_text.assert_called_once_with(timeout=10000)  # Only the IP echo.
        assert page.tab_calls == 0 and not dump_dir.exists()


@pytest.mark.parametrize("quick_present,full_markets", [
    (True, True), (False, True), (True, False), (False, False),
])
def test_quick_dump_capture(monkeypatch, tmp_path, capsys, quick_present, full_markets):
    page = QuickPage(quick_present, full_markets)
    offline_browser(monkeypatch, page)
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [card(live=True)])
    waits = []
    monkeypatch.setattr(collector.time, "sleep", waits.append)
    dump_dir = tmp_path / "quick"
    history = tmp_path / "h.jsonl"

    def before_click():
        assert len(list(dump_dir.glob("*_all.txt"))) == 1
        assert not list(dump_dir.glob("*_quick.txt"))
    page.before_click = before_click
    assert collector.main(["--history", str(history), "--kinds", "live", "--max-loads", "2",
                           "--quick-dump-dir", str(dump_dir)]) == 0
    files = sorted(dump_dir.glob("*.txt"))
    assert len(files) == (2 if quick_present else 1)
    assert all(re.fullmatch(r"16855431_\d{8}T\d{6}Z_(all|quick)\.txt", f.name) for f in files)
    all_file, = dump_dir.glob("*_all.txt")
    assert all_file.read_text() == page.default_text
    if quick_present:
        quick_file, = dump_dir.glob("*_quick.txt")
        assert quick_file.read_text() == page.quick_text
        assert waits[-1] == 3.0
    assert not list(dump_dir.glob("*.tmp"))
    index_lines = (dump_dir / "index.jsonl").read_text().splitlines()
    assert len(index_lines) == 1
    entry = json.loads(index_lines[0])
    assert set(entry) == {"ts_utc", "event_id", "team1", "team2", "live", "full_markets",
                          "tabs", "quick_clicked", "chars_all", "chars_quick"}
    assert re.fullmatch(r"\d{8}T\d{6}Z", entry["ts_utc"])
    assert entry == dict(ts_utc=entry["ts_utc"], event_id="16855431", team1="TEAM YANDEX",
                         team2="LGD GAMING", live=True, full_markets=full_markets,
                         tabs=(["Все"] if full_markets else []) + (["бЫсТрЫе"] if quick_present else []),
                         quick_clicked=quick_present, chars_all=len(page.default_text),
                         chars_quick=len(page.quick_text) if quick_present else 0)
    output = capsys.readouterr()
    assert "loads=2" in output.out and "status=0 error=-" in output.out
    assert page.gotos == [collector.IP_ECHO, collector.LIST_URL]
    if full_markets:
        rows = [json.loads(line) for line in history.read_text().splitlines()]
        assert len(rows) == 3 and rows[0]["kills_t1_line"] == 31.5
    else:
        assert not history.exists()
        assert "events_unrendered=1 rows_written=0" in output.out
    captured = "".join(f.read_text() for f in dump_dir.iterdir()) + output.out + output.err
    assert all(secret not in captured for secret in (PROXY, EXIT, DIRECT, "proxy.invalid"))


def test_quick_dump_masks_private_values(monkeypatch, tmp_path, capsys):
    page = QuickPage()
    secrets = (PROXY, EXIT, DIRECT, "proxy.invalid", "test-user", "test-password")
    page.default_text += "\n" + "\n".join(secrets)
    page.quick_text += "\n" + "\n".join(secrets)
    offline_browser(monkeypatch, page)
    event = card(live=True)
    event["team1"] += " " + PROXY
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [event])
    original_evaluate = page.evaluate
    def evaluate(script, event_id=None):
        result = original_evaluate(script, event_id)
        if isinstance(result, dict):
            result["tabs"] += [EXIT, "proxy.invalid"]
        return result
    page.evaluate = evaluate
    dump_dir = tmp_path / "quick"
    assert collector.main(["--history", str(tmp_path / "h.jsonl"), "--kinds", "live",
                           "--quick-dump-dir", str(dump_dir)]) == 0
    output = capsys.readouterr()
    captured = "".join(f.read_text() for f in dump_dir.iterdir()) + output.out + output.err
    assert all(secret not in captured for secret in secrets)
    assert "FAKE QUICK TAB BODY" in captured


def test_quick_dump_environment_and_cli_override(monkeypatch, tmp_path):
    page = QuickPage(quick_present=False)
    offline_browser(monkeypatch, page)
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [card(live=True)])
    env_dir, cli_dir = tmp_path / "env", tmp_path / "cli"
    monkeypatch.setenv("WINLINE_QUICK_DUMP_DIR", str(env_dir))
    args = ["--history", str(tmp_path / "h.jsonl"), "--kinds", "live"]
    assert collector.main(args) == 0
    assert (env_dir / "index.jsonl").is_file()
    before = (env_dir / "index.jsonl").read_text()
    assert collector.main(args + ["--quick-dump-dir", str(cli_dir)]) == 0
    assert (cli_dir / "index.jsonl").is_file()
    assert (env_dir / "index.jsonl").read_text() == before


def test_quick_dump_disabled_leaves_outputs_unchanged(monkeypatch, tmp_path, capsys):
    monkeypatch.delenv("WINLINE_QUICK_DUMP_DIR", raising=False)
    page = QuickPage()
    offline_browser(monkeypatch, page)
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [card(live=True)])
    dump_dir = tmp_path / "quick"
    dump_dir.mkdir()
    history = tmp_path / "h.jsonl"
    monkeypatch.setattr(collector.time, "time", lambda: 123.0)
    assert collector.main(["--history", str(history), "--kinds", "live"]) == 0
    assert list(dump_dir.iterdir()) == []
    assert page.tab_calls == 0
    assert capsys.readouterr().out == (
        "cards=1 events_opened=1 events_missing=0 events_unrendered=0 rows_written=3 "
        "loads=2 country=DE status=0 error=- "
        "events_left_listing=0 events_not_clickable=0 card_wait_seconds_max=0 "
        "events_opened_hero=0 events_hero_open_failed=0 events_ready_timeout=0\n")
    expected = "".join(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n"
                       for row in collector.build_rows(card(live=True), page.default_text, wall=123.0))
    assert history.read_text() == expected


def test_quick_dump_browser_ip_fail_closed(monkeypatch, tmp_path, capsys):
    page = QuickPage()
    page.browser_ip = DIRECT
    offline_browser(monkeypatch, page)
    dump_dir = tmp_path / "quick"
    assert collector.main(["--history", str(tmp_path / "h.jsonl"),
                           "--quick-dump-dir", str(dump_dir)]) == 2
    assert page.gotos == [collector.IP_ECHO]
    assert page.tab_calls == 0 and not dump_dir.exists()
    output = capsys.readouterr()
    assert all(secret not in output.out + output.err for secret in (PROXY, EXIT, DIRECT))


def test_quick_dump_tab_error_masks_exception_message(monkeypatch, tmp_path, capsys):
    page = QuickPage()
    offline_browser(monkeypatch, page)
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [card(live=True)])
    original_evaluate = page.evaluate
    def evaluate(script, event_id=None):
        if event_id is None and script == getattr(collector, "QUICK_TAB_JS", None):
            print(PROXY + EXIT)
            raise RuntimeError(PROXY + EXIT)
        return original_evaluate(script, event_id)
    page.evaluate = evaluate
    dump_dir = tmp_path / "quick"
    assert collector.main(["--history", str(tmp_path / "h.jsonl"), "--kinds", "live",
                           "--quick-dump-dir", str(dump_dir)]) == 5
    output = capsys.readouterr()
    assert "status=5 error=RuntimeError" in output.out
    assert len(list(dump_dir.glob("*_all.txt"))) == 1
    assert not (dump_dir / "index.jsonl").exists()
    captured = "".join(f.read_text() for f in dump_dir.iterdir()) + output.out + output.err
    assert all(secret not in captured for secret in (PROXY, EXIT, DIRECT))


@pytest.mark.parametrize("max_loads,expected_ids", [(2, ["16855431"]), (4, ["16855431", "16855432"])])
def test_quick_dump_each_event_with_existing_load_budget(monkeypatch, tmp_path, capsys,
                                                        max_loads, expected_ids):
    page = QuickPage()
    offline_browser(monkeypatch, page)
    monkeypatch.setattr(collector, "enumerate_cards",
                        lambda html: [card("16855431", True), card("16855432", True)])
    dump_dir = tmp_path / "quick"
    assert collector.main(["--history", str(tmp_path / "h.jsonl"), "--kinds", "live",
                           "--max-loads", str(max_loads), "--quick-dump-dir", str(dump_dir)]) == 0
    entries = [json.loads(line) for line in (dump_dir / "index.jsonl").read_text().splitlines()]
    assert [entry["event_id"] for entry in entries] == expected_ids
    assert all(entry["quick_clicked"] for entry in entries)
    assert len(list(dump_dir.glob("*.txt"))) == 2 * len(expected_ids)
    assert page.opened == expected_ids and page.tab_calls == len(expected_ids)
    assert "loads=%d" % max_loads in capsys.readouterr().out


def test_normal_card_ready_timeout_continues_to_next_card(monkeypatch, tmp_path, capsys):
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

    class Page(FakePage):
        def wait_for_function(self, script, *args, **kwargs):
            if script == collector.EVENT_READY_JS and self.opened and self.opened[-1] == "16855432":
                raise PlaywrightTimeoutError("ready")

    page = Page()
    offline_browser(monkeypatch, page)
    monkeypatch.setattr(collector, "enumerate_cards",
                        lambda html: [card("16855431"), card("16855432"), card("16855433")])
    rc = collector.main(["--history", str(tmp_path / "h.jsonl")])
    out = capsys.readouterr().out
    assert rc == 0 and "status=0 error=-" in out
    assert "events_ready_timeout=1" in out and "events_hero_open_failed=0" in out
    assert page.opened == ["16855431", "16855432", "16855433"]
    assert "loads=6" in out and "rows_written=6" in out


def test_readyfail_dump_redacts_private_values(monkeypatch, tmp_path, capsys):
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    secrets = (PROXY, "proxy.invalid", "test-user", "test-password", DIRECT, EXIT)
    event = card()
    event["team1"] += " " + PROXY
    scoreboard = ["OTHER TEAM", "LGD GAMING", " ".join(secrets)]

    class Page(FakePage):
        def wait_for_function(self, script, *args, **kwargs):
            if script == collector.EVENT_READY_JS:
                self.url += "?private=" + PROXY
                raise PlaywrightTimeoutError(" ".join(secrets))

        def evaluate(self, script, event_id=None):
            if event_id is None:
                assert '.event-scoreboard-widget .match-card__team-name' in script
                self.scoreboard_calls += 1
                return scoreboard
            return super().evaluate(script, event_id)

        def inner_text(self, **kwargs):
            if self.event:
                assert kwargs == {"timeout": 10000}
                return "READYFAIL BODY\n" + "\n".join(secrets)
            return super().inner_text(**kwargs)

    page = Page()
    page.scoreboard_calls = 0
    offline_browser(monkeypatch, page)
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [event])
    dump_dir = tmp_path / "quick"
    assert collector.main(["--history", str(tmp_path / "h.jsonl"),
                           "--quick-dump-dir", str(dump_dir)]) == 0
    dump, = dump_dir.glob("*_readyfail.txt")
    assert re.fullmatch(r"16855431_\d{8}T\d{6}Z_readyfail\.txt", dump.name)
    lines = dump.read_text().splitlines()
    assert lines[0] == "url_path=/stavki/sport/kibersport/dota_2/event/16855431"
    assert json.loads(lines[1].removeprefix("scoreboard_names=")) == [
        "OTHER TEAM", "LGD GAMING", " ".join("[redacted]" for _ in secrets)]
    assert lines[2] == "card_names=TEAM YANDEX [redacted]|LGD GAMING"
    assert lines[3] == "READYFAIL BODY" and page.scoreboard_calls == 1
    entry = json.loads((dump_dir / "index.jsonl").read_text())
    assert entry == dict(ts_utc=entry["ts_utc"], event_id="16855431", kind="readyfail",
                         hero=False, url_changed=True, names_match=False)
    assert dump.name == "%s_%s_readyfail.txt" % (entry["event_id"], entry["ts_utc"])
    assert not list(dump_dir.glob("*.tmp"))
    output = capsys.readouterr()
    captured = "".join(f.read_text() for f in dump_dir.iterdir()) + output.out + output.err
    assert all(secret not in captured for secret in secrets)
    assert "events_ready_timeout=1" in output.out


def test_hero_ready_timeout_after_proxy_failure_is_not_success(monkeypatch, tmp_path, capsys):
    # Hard-verifier 08.10 F1: a proxy failure during the hero click on the last card must not end as status=0.
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    page = hero_reload_browser(monkeypatch, ready_error=PlaywrightTimeoutError("ready"))
    original = page.evaluate

    def evaluate(script, event_id=None):
        result = original(script, event_id)
        if script == collector.OPEN_HERO_EVENT_JS:
            class Request:
                failure = "NS_ERROR_NET_RESET proxy"
            page.failure_callback(Request())
        return result

    page.evaluate = evaluate
    page.feed_ids = lambda: ["16855431", page.hero_id] if page.listing_loads == 1 else ["16855431"]
    rc = collector.main(["--history", str(tmp_path / "h.jsonl"), "--kinds", "live",
                         "--max-events", "2", "--max-loads", "4"])
    out = capsys.readouterr().out
    assert rc == 5 and "error=RuntimeError" in out and "status=5" in out


@pytest.mark.parametrize("proxy_failed", [False, True], ids=["non-timeout", "proxy-timeout"])
@pytest.mark.parametrize("quick_mode", [False, True])
def test_normal_ready_failure_aborts_and_masks(monkeypatch, tmp_path, capsys,
                                              proxy_failed, quick_mode):
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

    class Page(FakePage):
        def wait_for_function(self, script, *args, **kwargs):
            if script == collector.EVENT_READY_JS:
                if proxy_failed:
                    request = Mock(failure="NS_ERROR_NET_RESET proxy " + PROXY)
                    self.failure_callback(request)
                    raise PlaywrightTimeoutError(PROXY + EXIT)
                raise ValueError(PROXY + EXIT)

    page = Page()
    offline_browser(monkeypatch, page)
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [card(), card("16855432")])
    dump_dir = tmp_path / "quick"
    args = ["--history", str(tmp_path / "h.jsonl")]
    if quick_mode:
        args += ["--quick-dump-dir", str(dump_dir)]
    assert collector.main(args) == 5
    assert page.opened == ["16855431"] and not dump_dir.exists()
    output = capsys.readouterr()
    assert "status=5 error=%s" % ("RuntimeError" if proxy_failed else "ValueError") in output.out
    assert "events_ready_timeout=0" in output.out
    assert all(secret not in output.out + output.err for secret in (PROXY, EXIT, DIRECT))


@pytest.mark.parametrize("hero", [False, True])
@pytest.mark.parametrize("body_timeout", [False, True])
@pytest.mark.parametrize("url_changed", [False, True])
def test_readyfail_dump_hero_and_body_timeout(monkeypatch, tmp_path, capsys,
                                             hero, body_timeout, url_changed):
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    event = card()

    class Page(FakePage):
        def content(self):
            return ('<ww-feature-event-live-center-dsk><img src="/api/cls/event/1/%s">'
                    '</ww-feature-event-live-center-dsk>') % event["event_id"]

        def evaluate(self, script, event_id=None):
            if script == collector.OPEN_EVENT_JS and hero:
                return False
            if script == collector.OPEN_HERO_EVENT_JS:
                return super().evaluate(collector.OPEN_EVENT_JS, event["event_id"])
            if event_id is None:
                assert script == collector.SCOREBOARD_NAMES_JS
                return [event["team1"].lower(), event["team2"].lower()]
            return super().evaluate(script, event_id)

        def wait_for_function(self, script, *args, **kwargs):
            if script == collector.EVENT_READY_JS:
                if not url_changed:
                    self.url = collector.LIST_URL
                raise PlaywrightTimeoutError(PROXY + EXIT)

        def inner_text(self, **kwargs):
            if self.event:
                assert kwargs == {"timeout": 10000}
                if body_timeout:
                    raise PlaywrightTimeoutError(PROXY + EXIT)
                return "READYFAIL BODY"
            return super().inner_text(**kwargs)

    page = Page()
    offline_browser(monkeypatch, page)
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [event])
    dump_dir = tmp_path / "quick"
    assert collector.main(["--history", str(tmp_path / "h.jsonl"), "--max-loads", "2",
                           "--quick-dump-dir", str(dump_dir)]) == 0
    dump, = dump_dir.glob("*_readyfail.txt")
    lines = dump.read_text().splitlines()
    assert len(lines) == (3 if body_timeout else 4)
    assert lines[2] == "card_names=TEAM YANDEX|LGD GAMING"
    if not body_timeout:
        assert lines[3] == "READYFAIL BODY"
    entry = json.loads((dump_dir / "index.jsonl").read_text())
    assert entry == dict(ts_utc=entry["ts_utc"], event_id=event["event_id"], kind="readyfail",
                         hero=hero, url_changed=url_changed, names_match=True)
    out = capsys.readouterr().out
    assert "events_hero_open_failed=%d events_ready_timeout=%d" % (int(hero), int(not hero)) in out
    assert "loads=2" in out and page.opened == [event["event_id"]]


@pytest.mark.parametrize("stage", ["scoreboard", "body"])
@pytest.mark.parametrize("proxy_failed", [False, True])
def test_readyfail_dump_errors_propagate(monkeypatch, tmp_path, capsys, stage, proxy_failed):
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

    class Page(FakePage):
        def fail(self):
            if proxy_failed:
                self.failure_callback(Mock(failure="NS_ERROR_NET_RESET proxy " + PROXY))
                return
            raise ValueError(PROXY + EXIT)

        def wait_for_function(self, script, *args, **kwargs):
            if script == collector.EVENT_READY_JS:
                raise PlaywrightTimeoutError(PROXY + EXIT)

        def evaluate(self, script, event_id=None):
            if event_id is None:
                if stage == "scoreboard":
                    self.fail()
                return ["TEAM YANDEX", "LGD GAMING"]
            return super().evaluate(script, event_id)

        def inner_text(self, **kwargs):
            if self.event:
                assert kwargs == {"timeout": 10000}
                if stage == "body":
                    self.fail()
                    raise PlaywrightTimeoutError(PROXY + EXIT)
                return "READYFAIL BODY"
            return super().inner_text(**kwargs)

    page = Page()
    offline_browser(monkeypatch, page)
    monkeypatch.setattr(collector, "enumerate_cards", lambda html: [card(), card("16855432")])
    dump_dir = tmp_path / "quick"
    assert collector.main(["--history", str(tmp_path / "h.jsonl"),
                           "--quick-dump-dir", str(dump_dir)]) == 5
    assert page.opened == ["16855431"] and not dump_dir.exists()
    output = capsys.readouterr()
    assert "status=5 error=%s" % ("RuntimeError" if proxy_failed else "ValueError") in output.out
    assert all(secret not in output.out + output.err for secret in (PROXY, EXIT, DIRECT))


def test_history_writes_map_total_change_with_unchanged_team_totals(tmp_path):
    """A map-total move alone is an observation; pre-upgrade rows replay once (captured aurora page)."""
    path = tmp_path / "history.jsonl"
    row = collector.build_rows(dict(card(), team1="TEAM AURORA", team2="1W"), body("aurora_1w"), wall=10)[0]
    assert (row["kills_map_line"], row["kills_map_ladder"][0]) == (53.5, [52.5, 1.81, 2.01])
    legacy = {k: v for k, v in row.items() if k not in collector.MAP_TOTAL_FIELDS}
    path.write_text(json.dumps(legacy, ensure_ascii=False) + "\n", encoding="utf-8")
    with collector.HistoryWriter(path) as writer:
        assert writer.write(dict(row, wall=11))
        assert not writer.write(dict(row, wall=12))
        ladder = [list(rung) for rung in row["kills_map_ladder"]]
        ladder[0][1] = 1.86
        assert writer.write(dict(row, kills_map_ladder=ladder, wall=13))
        assert writer.write(dict(row, kills_map_ladder=ladder, kills_map_line=54.5, wall=14))


MIDMAP_FIXTURE = "winline_kills_map_totals_live_midmap_legion_blasterbi_20261008.txt"


def test_totals_rows_mid_map_state_and_full_markets_flag():
    """Captured mid-map page (map 3 running, 1:1): state parsed, full_markets travels with the row."""
    text = (FIXTURES / MIDMAP_FIXTURE).read_text(encoding="utf-8")
    entry = dict(ts_utc="20261008T184904Z", event_id="16890543", team1="LEGION", team2="BLASTERBI",
                 live=True, full_markets=True)
    rows = collector.build_all_totals_rows(entry, text, "16890543_20261008T184904Z_all.txt")
    map3 = next(r for r in rows if r["map_num"] == 3)
    assert (map3["series_score"], map3["series_state"]) == ("1 : 1", "3 карта")
    assert map3["full_markets"] is True and map3["kills_map_line"] == 59.5
    unrendered = collector.build_all_totals_rows(dict(entry, full_markets=False), text, "x_all.txt")
    assert all(r["full_markets"] is False for r in unrendered)


def test_map_total_valid_block_then_malformed_block_is_null():
    """A partial ladder must not be returned when the market block is not fully consumed."""
    text = ("2 карта тотал убийств\nБольше\nб 56.5\n1.75\nМеньше\nм 56.5\n1.96\n"
            "Больше\nб 57.5\n1.85\nМеньше\nм 57.5\n")
    assert collector.parse_winline_map_kills_total(text, 2) is None
    live = (FIXTURES / ALL_FIXTURE).read_text(encoding="utf-8")
    assert collector.parse_winline_map_kills_total(live, 2)[0] == [56.5, 1.75, 1.96]


def test_map_total_ladder_split_into_two_column_groups():
    """Captured page where a 5-rung map ladder is rendered as two Больше/Меньше groups."""
    text = (FIXTURES / "winline_kills_map_totals_live_two_groups_1w_parivision_20261009.txt").read_text(
        encoding="utf-8")
    ladder = collector.parse_winline_map_kills_total(text, 1)
    assert [rung[0] for rung in ladder] == [49.5, 50.5, 51.5, 52.5, 53.5]
    assert ladder[0] == [49.5, 1.66, 2.13] and ladder[-1] == [53.5, 2.09, 1.69]


@pytest.mark.parametrize("tail", ["Больше\n", "Меньше\nм 53.5\n", "б 54.5\n1.80\n"])
def test_map_total_dangling_or_stray_group_is_null(tail):
    """Verifier inputs (astra 10.10): an empty/partial trailing group must not pass as a full block."""
    text = "1 карта тотал убийств\nБольше\nб 53.5\n1.9\nМеньше\nм 53.5\n1.9\n" + tail + "WINLINE\n"
    assert collector.parse_winline_map_kills_total(text, 1) is None
    two = (FIXTURES / "winline_kills_map_totals_live_two_groups_1w_parivision_20261009.txt").read_text(
        encoding="utf-8")
    assert collector.parse_winline_map_kills_total(two.replace("м 53.5\n1.69\n", "м 53.5\n1.69\nБольше\n", 1),
                                                   1) is None
