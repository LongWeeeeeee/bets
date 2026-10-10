"""Journal delivery tests derived from unedited SourceTV captures."""

import json
import os
import signal
from datetime import datetime, timezone
from pathlib import Path

import pytest

from services.live_journal import ticks_tap


ROOT = Path(__file__).resolve().parents[3]
CAPTURES = ROOT / "base/tests/fixtures/series_tempo"
MATCH = "9014406398"
NEXT_MATCH = "9014519155"


def capture(suffix="215735"):
    return json.loads((CAPTURES / ("snap_20260924T%s.json" % suffix)).read_text())


class Harness:
    def __init__(self, tmp_path, monkeypatch, **kwargs):
        self.src = tmp_path / "source.json"
        self.out = tmp_path / "journal"
        self.wall = 1790276255.0
        self.version = 0
        monkeypatch.setattr(ticks_tap.time, "time", lambda: self.wall)
        monkeypatch.setenv("LIVE_JOURNAL_DIR", str(self.out))
        self.tap = ticks_tap.TickTap(self.src, **kwargs)

    def publish(self, payload):
        self.version += 1
        text = payload if isinstance(payload, str) else json.dumps(payload)
        self.src.write_text(text, encoding="utf-8")
        os.utime(self.src, (self.wall + self.version, self.wall + self.version))

    def poll(self, payload=None, advance=0):
        self.wall += advance
        if payload is not None:
            self.publish(payload)
        self.tap.poll()

    def rows(self):
        return [json.loads(line) for path in sorted(self.out.glob("ticks_*.jsonl"))
                for line in path.read_text(encoding="utf-8").splitlines()]


@pytest.fixture
def h(tmp_path, monkeypatch):
    return Harness(tmp_path, monkeypatch)


def test_captured_sequence_delivers_ticks_and_exactly_one_gone(h, capsys):
    first = capture()
    h.poll(first)
    row = h.rows()[0]
    assert row["schema"] == "live_ticks.v1"
    assert row["event"] == "tick"
    assert row["match_id"] == int(MATCH)
    assert row["wall"] == h.wall
    assert row["src_mtime"] == h.src.stat().st_mtime
    assert row["picks"] == first[MATCH]["_cyberscore_heroes_and_pos"]
    assert row["extra"] == {k: v for k, v in first[MATCH].items()
                            if k not in ticks_tap.COPIED_FIELDS
                            and k not in ("match_id", "timestamp")
                            and not k.startswith("_")}
    assert row["src_ts"] == first[MATCH]["timestamp"]
    for key in ticks_tap.COPIED_FIELDS:
        assert row[key] == first[MATCH][key]
    h.poll(advance=5)  # Same mtime.
    h.poll(first, advance=5)  # Same content with new mtime.
    assert len(h.rows()) == 1
    h.poll(capture("220949"), advance=60)
    h.poll(capture("221901"), advance=60)
    assert [r["game_time"] for r in h.rows()] == [1508, 2205, 2523]
    assert "picks" not in h.rows()[1]
    # Captured dumps differ only in the probe timestamp: no repeated extra.
    assert [("extra" in r) for r in h.rows()] == [True, False, False]
    assert [r["src_ts"] for r in h.rows()] == [
        capture(s)[MATCH]["timestamp"] for s in ("215735", "220949", "221901")]
    h.poll(capture("224335"), advance=60)
    new = h.rows()[-1]
    assert new["match_id"] == int(NEXT_MATCH)
    assert "picks" in new
    assert new["series_game_number"] == row["series_game_number"] == 2
    h.poll(advance=119)
    assert not any(r["event"] == "gone" for r in h.rows())
    h.poll(advance=1)  # Expire even though mtime has not changed.
    gone = h.rows()[-1]
    assert (gone["event"], gone["match_id"], gone["game_time"]) == (
        "gone", int(MATCH), 2523)
    assert gone["src_mtime"] == h.src.stat().st_mtime
    h.poll(advance=120)
    assert len([r for r in h.rows() if r["event"] == "gone"]) == 1
    logs = capsys.readouterr().out.splitlines()
    assert len(logs) == 3  # Two admissions, one gone, no per-tick messages.


def test_min_gap_uses_last_written_tick_and_score_overrides_gap(h):
    h.poll(capture())
    short = capture()
    short[MATCH]["game_time"] += 20
    h.poll(short, advance=5)
    assert len(h.rows()) == 1
    paused = capture()
    paused[MATCH]["game_time"] = short[MATCH]["game_time"]
    paused[MATCH]["radiant_score"] = capture("220949")[MATCH]["radiant_score"]
    h.poll(paused, advance=5)
    assert len(h.rows()) == 1  # Pause after a tick suppressed by the gap.
    short[MATCH]["game_time"] += 10
    h.poll(short, advance=5)
    assert len(h.rows()) == 2
    short[MATCH]["game_time"] += 1
    short[MATCH]["radiant_score"] = capture("220949")[MATCH]["radiant_score"]
    h.poll(short, advance=5)
    assert len(h.rows()) == 3
    short[MATCH]["game_time"] += 1
    short[MATCH]["dire_score"] = capture("220949")[MATCH]["dire_score"]
    h.poll(short, advance=5)
    assert len(h.rows()) == 4


def test_picks_alone_triggers_below_min_gap(h):
    h.poll(capture())
    changed = capture()
    changed[MATCH]["game_time"] += 1
    changed[MATCH]["_cyberscore_heroes_and_pos"] = capture("224335")[NEXT_MATCH][
        "_cyberscore_heroes_and_pos"]
    h.poll(changed, advance=5)
    assert len(h.rows()) == 2
    assert h.rows()[-1]["picks"] == changed[MATCH]["_cyberscore_heroes_and_pos"]
    assert "extra" not in h.rows()[-1]


def test_picks_and_extra_are_sparse_and_pause_suppresses_rows(h):
    h.poll(capture())
    changed = capture()
    changed[MATCH]["_cyberscore_heroes_and_pos"] = capture("224335")[NEXT_MATCH][
        "_cyberscore_heroes_and_pos"]
    changed[MATCH]["radiant_score"] = capture("220949")[MATCH]["radiant_score"]
    h.poll(changed, advance=5)
    assert len(h.rows()) == 1  # Identical game time wins over other changes.
    changed[MATCH]["game_time"] += 1
    h.poll(changed, advance=5)
    assert h.rows()[-1]["picks"] == changed[MATCH]["_cyberscore_heroes_and_pos"]
    assert "extra" not in h.rows()[-1]
    changed[MATCH]["timestamp"] = capture("221901")[MATCH]["timestamp"]
    changed[MATCH]["status"] = "paused"
    changed[MATCH]["game_time"] += 1
    h.poll(changed, advance=5)
    assert len(h.rows()) == 2  # Extra alone does not trigger a tick.
    changed[MATCH]["game_time"] += 29
    h.poll(changed, advance=5)
    assert "picks" not in h.rows()[-1]
    assert h.rows()[-1]["extra"]["status"] == "paused"
    assert "timestamp" not in h.rows()[-1]["extra"]
    assert h.rows()[-1]["src_ts"] == changed[MATCH]["timestamp"]
    changed[MATCH]["timestamp"] += 30  # Timestamp alone never re-emits extra.
    changed[MATCH]["game_time"] += 30
    h.poll(changed, advance=5)
    assert "extra" not in h.rows()[-1]
    assert h.rows()[-1]["src_ts"] == changed[MATCH]["timestamp"]


def test_repeated_source_error_is_logged_once_until_a_good_read(h, capsys):
    for _ in range(3):
        h.poll(advance=5)  # Missing source.
    assert len([l for l in capsys.readouterr().out.splitlines()
                if l.startswith("skip source")]) == 1
    h.poll(capture(), advance=5)
    h.src.unlink()
    h.poll(advance=5)
    h.poll(advance=5)
    out = capsys.readouterr().out.splitlines()
    assert len([l for l in out if l.startswith("skip source")]) == 1
    assert len(h.rows()) == 1


def test_reappearance_cancels_gone_timer_then_readmits_after_gone(h):
    h.poll(capture())
    h.poll({}, advance=5)
    h.poll(capture(), advance=119)
    h.poll(advance=120)
    assert len(h.rows()) == 1
    h.poll({}, advance=5)
    h.poll(advance=120)
    assert h.rows()[-1]["event"] == "gone"
    h.poll(capture(), advance=5)
    assert h.rows()[-1]["event"] == "tick"
    assert "picks" in h.rows()[-1]


def test_empty_truncated_missing_and_non_object_sources_write_nothing(h):
    h.poll()  # Missing source, no deletion needed.
    h.poll({})
    h.poll("")
    text = (CAPTURES / "snap_20260924T215735.json").read_text()
    h.poll(text[:len(text) // 2])
    h.poll(json.dumps(list(capture().values())))
    assert h.rows() == []
    h.poll(capture())
    assert len(h.rows()) == 1


def test_bad_read_does_not_expire_pending_gone_and_retries_same_mtime(h):
    h.poll(capture())
    h.poll({}, advance=5)
    text = (CAPTURES / "snap_20260924T215735.json").read_text()
    h.poll(text[:100], advance=120)
    assert len(h.rows()) == 1
    mtime_ns = h.src.stat().st_mtime_ns
    h.src.write_text(text)
    os.utime(h.src, ns=(mtime_ns, mtime_ns))
    h.poll(advance=5)
    h.poll(advance=120)
    assert len(h.rows()) == 1


def test_unchanged_mtime_does_not_reparse_source(h):
    h.poll(capture())
    h.poll({}, advance=5)
    mtime_ns = h.src.stat().st_mtime_ns
    h.src.write_text("{")
    os.utime(h.src, ns=(mtime_ns, mtime_ns))
    h.poll(advance=120)
    assert h.rows()[-1]["event"] == "gone"  # Cached valid empty source.


@pytest.mark.parametrize("bad", [None, "entry", "clock", "picks"])
def test_bad_entry_does_not_prevent_good_match(h, bad, capsys):
    payload = capture("224335")
    old = capture()[MATCH]
    if bad == "clock":
        old["game_time"] = old["radiant_team_name"]
    elif bad == "picks":
        old["_cyberscore_heroes_and_pos"] = list(old["_cyberscore_heroes_and_pos"])
    else:
        old = bad
    payload[MATCH] = old
    h.poll(payload)
    assert [r["match_id"] for r in h.rows()] == [int(NEXT_MATCH)]
    assert "skip" in capsys.readouterr().out


def test_utc_rollover_and_restart_append_without_truncation(h):
    h.wall = datetime(2026, 9, 24, 23, 59, 59, tzinfo=timezone.utc).timestamp()
    h.poll(capture())
    before = (h.out / "ticks_20260924.jsonl").read_bytes()
    h.poll(capture("220949"), advance=2)
    assert (h.out / "ticks_20260924.jsonl").read_bytes() == before
    next_day = h.out / "ticks_20260925.jsonl"
    assert len(next_day.read_text().splitlines()) == 1
    h.tap = ticks_tap.TickTap(h.src)
    h.poll()
    assert len(next_day.read_text().splitlines()) == 2
    assert "picks" in h.rows()[-1]


def test_cli_resolver_defaults_and_sigterm_exit(h, monkeypatch):
    h.publish(capture())
    monkeypatch.setenv("SOURCETV_MATCHES_PATH", str(h.src))
    sleeps = []

    def stop(seconds):
        sleeps.append(seconds)
        signal.raise_signal(signal.SIGTERM)

    monkeypatch.setattr(ticks_tap.time, "sleep", stop)
    assert ticks_tap.main([]) == 0
    assert sleeps == [5.0]
    assert len(h.rows()) == 1
    # CLI takes priority over the resolver's environment convention.
    monkeypatch.setenv("SOURCETV_MATCHES_PATH", str(h.src.parent / "absent.json"))
    assert ticks_tap.main(["--src", str(h.src), "--poll-s", "1",
                           "--min-gap-s", "60", "--gone-after-s", "180"]) == 0
    assert sleeps[-1] == 1.0
    assert len(h.rows()) == 2


@pytest.mark.parametrize("args", [["--poll-s", "0"], ["--min-gap-s", "nan"],
                                  ["--gone-after-s", "-1"]])
def test_cli_rejects_busy_loop_or_invalid_intervals(args):
    with pytest.raises(SystemExit) as exc:
        ticks_tap.main(args)
    assert exc.value.code == 2
