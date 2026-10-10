"""Runner for the daily Valve rank snapshots survives a macOS DarkWake (card ingame-qe6y).

07.10, 08.10 and 10.10.2026 the launchd 09:00 run fired inside a battery DarkWake
(pmset: "DarkWake from Deep Idle ... Using BATT", back to sleep within ~2 s): DNS/TLS
failed (gaierror, SSLEOFError), the runner called the collector once and gave up.
07 and 08 are lost for good (Valve serves only the current table) and their alerts
failed on the same dead network. The runner now waits for the network, retries the
collector, runs at five launchd slots, and alerts only when the UTC day is lost.

The collector and the notifier are replaced by fakes through env seams; the
collector fakes write the captured collection.json files of 10.10
(fixtures/rank_snapshots_20261010). Nothing touches the network or Telegram.
"""
import json
import os
import plistlib
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "scripts" / "run" / "collect_rank_snapshots.sh"
PLIST = ROOT / "scripts" / "ops" / "com.ingame.rank-snapshots.plist"
FIX = Path(__file__).resolve().parent / "fixtures" / "rank_snapshots_20261010"

FAKE_COLLECT = r'''
import json, os, sys, time
from pathlib import Path
state = Path(os.environ["FAKE_STATE"]) / "collect_calls"
n = int(state.read_text()) if state.exists() else 0
state.write_text(str(n + 1))
seq = os.environ["FAKE_SEQ"].split(",")
outcome = seq[min(n, len(seq) - 1)]
fixture = Path(os.environ["FAKE_FIX"]) / ("collection_complete.json" if outcome == "ok"
                                          else "collection_europe_sslerror.json")
j = json.loads(fixture.read_text())
j["started_at"] = time.time()
out = Path(sys.argv[sys.argv.index("--output") + 1]) / str(time.time_ns())
out.mkdir(parents=True)
(out / "collection.json").write_text(json.dumps(j))
print(json.dumps({"manifest": str(out / "collection.json"), **j}))
sys.exit(0 if j["complete"] else 1)
'''

FAKE_NET = r'''
import os, sys
from pathlib import Path
state = Path(os.environ["FAKE_STATE"]) / "net_calls"
n = int(state.read_text()) if state.exists() else 0
state.write_text(str(n + 1))
sys.exit(1 if n < int(os.environ.get("FAKE_NET_FAILS", "0")) else 0)
'''

FAKE_NOTIFY = r'''
import os, sys
from pathlib import Path
state = Path(os.environ["FAKE_STATE"])
fails = state / "notify_fails_left"
left = int(fails.read_text()) if fails.exists() else 0
if left > 0:  # Telegram unreachable (DarkWake): notify_admin.py prints this and exits 0
    fails.write_text(str(left - 1))
    print("notify_admin: failed: URLError(gaierror(8, 'nodename nor servname provided'))")
    sys.exit(0)
with open(state / "notify.txt", "a") as fh:
    fh.write(sys.stdin.read() + "\n----\n")
print("notify_admin: ok")
'''


def _run(tmp_path, seq, utc_hour, net_fails=0, preexisting=None, yesterday_complete=True,
         fresh=True, notify_fails=0, start_date=None, complete_days_ago=()):
    out = tmp_path / "out"
    state = tmp_path / "state"
    out.mkdir(exist_ok=not fresh)
    state.mkdir(exist_ok=not fresh)
    if yesterday_complete and fresh:
        # The real archive always holds yesterday's complete snapshot except on a lost day.
        d = out / "0"
        d.mkdir()
        j = json.loads((FIX / "collection_complete.json").read_text())
        j["started_at"] = time.time() - 86400
        (d / "collection.json").write_text(json.dumps(j))
    for n in complete_days_ago:
        d = out / ("ago%d" % n)
        d.mkdir()
        j = json.loads((FIX / "collection_complete.json").read_text())
        j["started_at"] = time.time() - n * 86400
        (d / "collection.json").write_text(json.dumps(j))
    if notify_fails:
        (state / "notify_fails_left").write_text(str(notify_fails))
    for name, body in (("collect.py", FAKE_COLLECT), ("net.py", FAKE_NET), ("notify.py", FAKE_NOTIFY)):
        (tmp_path / name).write_text(body)
    if preexisting and fresh:
        d = out / "1"
        d.mkdir()
        j = json.loads((FIX / preexisting).read_text())
        j["started_at"] = time.time()
        (d / "collection.json").write_text(json.dumps(j))
    py = sys.executable
    env = dict(os.environ,
               RANK_SNAPSHOT_OUT=str(out),
               RANK_SNAPSHOT_COLLECT_CMD=f"{py} {tmp_path / 'collect.py'}",
               RANK_SNAPSHOT_NET_CHECK_CMD=f"{py} {tmp_path / 'net.py'}",
               RANK_SNAPSHOT_NOTIFY_CMD=f"{py} {tmp_path / 'notify.py'}",
               RANK_SNAPSHOT_RETRY_SLEEP="0",
               RANK_SNAPSHOT_NET_SLEEP="0",
               RANK_SNAPSHOT_UTC_HOUR=str(utc_hour),
               **({"RANK_SNAPSHOT_UTC_DATE": start_date} if start_date else {}),
               FAKE_STATE=str(state), FAKE_SEQ=seq, FAKE_FIX=str(FIX),
               FAKE_NET_FAILS=str(net_fails))
    proc = subprocess.run(["bash", str(RUNNER)], env=env, capture_output=True, text=True,
                          timeout=120, stdin=subprocess.DEVNULL)

    def count(name):
        p = state / name
        return int(p.read_text()) if p.exists() else 0

    notify = (state / "notify.txt").read_text() if (state / "notify.txt").exists() else ""
    log = "".join(p.read_text() for p in out.glob("collect_*.log"))
    return proc, count("collect_calls"), count("net_calls"), notify, log


def test_darkwake_failure_then_success_gives_complete_snapshot_without_warning(tmp_path):
    proc, calls, _, notify, log = _run(tmp_path, "fail,ok", utc_hour=6)
    assert calls == 2, log
    assert "✅" in notify and "⚠️" not in notify, notify
    assert "complete=True регионов=4 строк=20018" in notify
    assert proc.returncode == 0, log


def test_failure_with_later_slots_today_defers_alert(tmp_path):
    proc, calls, _, notify, log = _run(tmp_path, "fail", utc_hour=9)
    assert calls == 3, log
    assert notify == "", notify
    assert "deferred" in log
    assert proc.returncode != 0


def test_failure_in_last_slot_of_utc_day_alerts(tmp_path):
    proc, calls, _, notify, log = _run(tmp_path, "fail", utc_hour=19)
    assert "⚠️ снимок рангов Valve" in notify
    assert "сбойные=['europe']" in notify
    assert proc.returncode != 0


def test_network_gate_waits_before_first_collect(tmp_path):
    proc, calls, net_calls, notify, log = _run(tmp_path, "ok", utc_hour=6, net_fails=2)
    assert net_calls == 3, log
    assert calls == 1, log
    assert "✅" in notify
    assert proc.returncode == 0, log


def test_complete_snapshot_today_skips_collection(tmp_path):
    proc, calls, _, notify, log = _run(tmp_path, "ok", utc_hour=6,
                                       preexisting="collection_complete.json")
    assert calls == 0
    assert notify == ""
    assert "skip:" in log


def test_launchd_runs_several_slots_a_day():
    with open(PLIST, "rb") as fh:
        plist = plistlib.load(fh)
    slots = plist["StartCalendarInterval"]
    assert isinstance(slots, list) and len(slots) >= 4
    hours = sorted(s["Hour"] for s in slots)
    assert hours[0] == 9 and hours[-1] == 21


def test_lost_previous_utc_day_alerts_once(tmp_path):
    """Missed 21:00 slot caught up after 00 UTC (hard-verifier LOW a): yesterday is lost."""
    proc, calls, _, notify, log = _run(tmp_path, "ok", utc_hour=1, yesterday_complete=False)
    assert notify.count("потерян") == 1, notify
    assert "нет ни одного полного снимка" in notify
    assert "✅" in notify and proc.returncode == 0
    proc2, calls2, _, notify2, _ = _run(tmp_path, "ok", utc_hour=4, fresh=False)
    assert notify2.count("потерян") == 1, notify2  # marker: no second alert
    assert calls2 == calls  # today's complete snapshot -> skip


def test_undelivered_lost_day_alert_is_retried_next_run(tmp_path):
    """astra 10.10 P2: in a DarkWake the alert send fails; the marker must not be set."""
    proc, calls, _, notify, log = _run(tmp_path, "ok", utc_hour=1, yesterday_complete=False,
                                       notify_fails=1)
    assert "потерян" not in notify  # first send failed, nothing delivered
    assert not list((tmp_path / "out").glob(".lost_alerted_*"))
    proc2, _, _, notify2, _ = _run(tmp_path, "ok", utc_hour=4, fresh=False)
    assert notify2.count("потерян") == 1, notify2  # retried and delivered once
    assert list((tmp_path / "out").glob(".lost_alerted_*"))


def test_second_failed_run_after_18_utc_does_not_repeat_alert(tmp_path):
    """hard-verifier 10.10 LOW b: two failed runs after 18 UTC send one alert."""
    _run(tmp_path, "fail", utc_hour=19)
    proc2, _, _, notify2, log2 = _run(tmp_path, "fail", utc_hour=20, fresh=False)
    assert notify2.count("потерян") == 1, notify2
    assert "no duplicate" in log2


def test_undelivered_evening_alert_is_retried(tmp_path):
    """hard-verifier r3 M3: the >=18 UTC marker is written only after a delivered alert."""
    _run(tmp_path, "fail", utc_hour=19, notify_fails=1)
    assert not list((tmp_path / "out").glob(".lost_alerted_*"))
    proc2, _, _, notify2, _ = _run(tmp_path, "fail", utc_hour=20, fresh=False)
    assert notify2.count("потерян") == 1, notify2
    assert list((tmp_path / "out").glob(".lost_alerted_*"))


def test_run_crossing_utc_midnight_reports_its_start_day(tmp_path):
    """astra 10.10 r3: started 18 UTC on day D, finished after 00 UTC -> day D is lost, not D+1."""
    import datetime as dt
    day_d = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).date().isoformat()
    proc, _, _, notify, _ = _run(tmp_path, "fail", utc_hour=18, start_date=day_d,
                                 yesterday_complete=False, complete_days_ago=(2,))
    assert f"за {day_d} UTC потерян" in notify, notify
    assert notify.count("потерян") == 1, notify  # D-1 had a complete snapshot
    assert (tmp_path / "out" / f".lost_alerted_{day_d}").exists()
