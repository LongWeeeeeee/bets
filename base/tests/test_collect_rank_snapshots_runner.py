"""Runner for the daily Valve rank snapshots (card ingame-qe6y): DarkWake-proof, runs on serv1.

07.10, 08.10 and 10.10.2026 the launchd 09:00 run fired inside a battery DarkWake
(pmset: "DarkWake from Deep Idle ... Using BATT", back to sleep within ~2 s): DNS/TLS
failed (gaierror, SSLEOFError), the runner called the collector once and gave up.
07 and 08 are lost for good (Valve serves only the current table) and their alerts
failed on the same dead network. The runner now waits for the network, retries the
collector, runs at five timer slots, and alerts only when the UTC day is lost.
Since 10.10.2026 (owner: no collection on the Mac) it runs on serv1 under
scripts/ops/systemd/rank-snapshots.timer, so it must not depend on Mac paths.

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
TIMER = ROOT / "scripts" / "ops" / "systemd" / "rank-snapshots.timer"
SERVICE = ROOT / "scripts" / "ops" / "systemd" / "rank-snapshots.service"
FIX = Path(__file__).resolve().parent / "fixtures" / "rank_snapshots_20261010"

FAKE_COLLECT = r'''
import json, os, sys, time
from pathlib import Path
state = Path(os.environ["FAKE_STATE"]) / "collect_calls"
n = int(state.read_text()) if state.exists() else 0
state.write_text(str(n + 1))
seq = os.environ["FAKE_SEQ"].split(",")
outcome = seq[min(n, len(seq) - 1)]
if outcome == "hang":  # a stalled HTTP read: requests timeout=30 caps each read, not the whole call
    time.sleep(60)
fixture = Path(os.environ["FAKE_FIX"]) / ("collection_complete.json" if outcome == "ok"
                                          else "collection_europe_sslerror.json")
j = json.loads(fixture.read_text())
j["started_at"] = float(os.environ.get("FAKE_STARTED_AT") or time.time())
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
hang = state / "notify_hang_left"
hang_left = int(hang.read_text()) if hang.exists() else 0
if hang_left > 0:  # Telegram answers byte by byte: urllib timeout=20 is per socket op
    hang.write_text(str(hang_left - 1))
    sys.stdin.read()
    import time
    time.sleep(60)
if left > 0:  # Telegram unreachable (DarkWake): notify_admin.py prints this and exits 0
    fails.write_text(str(left - 1))
    print("notify_admin: failed: URLError(gaierror(8, 'nodename nor servname provided'))")
    sys.exit(0)
with open(state / "notify.txt", "a") as fh:
    fh.write(sys.stdin.read() + "\n----\n")
print("notify_admin: ok")
'''


def _run(tmp_path, seq, utc_hour, net_fails=0, preexisting=None, yesterday_complete=True,
         fresh=True, notify_fails=0, start_date=None, complete_days_ago=(), started_at=None, extra_env=None, notify_hangs=0,
         corrupt_started_at=False):
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
    if corrupt_started_at:
        d = out / "bad"
        d.mkdir()
        j = json.loads((FIX / "collection_complete.json").read_text())
        j["started_at"] = "not-a-number"
        (d / "collection.json").write_text(json.dumps(j))
    if notify_hangs:
        (state / "notify_hang_left").write_text(str(notify_hangs))
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
               # The interpreter comes from the test, not from the checkout: delivery runs this
               # suite in a verified-tree copy without venv_catboost/ or venv/ (10.10: all 19
               # failed there on venv/bin/python3). The venv fallback has its own test below.
               RANK_SNAPSHOT_PY=py,
               RANK_SNAPSHOT_OUT=str(out),
               RANK_SNAPSHOT_COLLECT_CMD=f"{py} {tmp_path / 'collect.py'}",
               RANK_SNAPSHOT_NET_CHECK_CMD=f"{py} {tmp_path / 'net.py'}",
               RANK_SNAPSHOT_NOTIFY_CMD=f"{py} {tmp_path / 'notify.py'}",
               RANK_SNAPSHOT_RETRY_SLEEP="0",
               RANK_SNAPSHOT_NET_SLEEP="0",
               RANK_SNAPSHOT_UTC_HOUR=str(utc_hour),
               **({"RANK_SNAPSHOT_UTC_DATE": start_date} if start_date else {}),
               FAKE_STATE=str(state), FAKE_SEQ=seq, FAKE_FIX=str(FIX),
               FAKE_NET_FAILS=str(net_fails),
               **({"FAKE_STARTED_AT": str(started_at)} if started_at else {}),
               **(extra_env or {}))
    proc = subprocess.run(["bash", str(RUNNER)], env=env, capture_output=True, text=True,
                          timeout=120, stdin=subprocess.DEVNULL, cwd=str(tmp_path))

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


def test_hung_collector_is_cut_and_retried(tmp_path):
    """astra 10.10 MEDIUM: one hung attempt must not eat the unit's whole TimeoutStartSec."""
    t0 = time.monotonic()
    proc, calls, _, notify, log = _run(tmp_path, "hang,ok", utc_hour=6,
                                       extra_env={"RANK_SNAPSHOT_COLLECT_TIMEOUT": "2"})
    elapsed = time.monotonic() - t0
    assert calls == 2, log
    assert elapsed < 30, (elapsed, log)
    assert "attempt 1: collect_exit=0" not in log, log
    assert "✅" in notify, (notify, log)
    assert proc.returncode == 0, log


def test_network_down_all_day_never_reports_success_from_an_old_snapshot(tmp_path):
    """hard-verifier 10.10 LOW: with no collector run the newest collection.json is
    yesterday's complete one; the summary then reads complete=True, so only the rc guard
    stops a false ✅."""
    proc, calls, net_calls, notify, log = _run(tmp_path, "ok", utc_hour=9, net_fails=1000)
    assert calls == 0, log
    assert net_calls == 60, log
    assert "complete=True регионов=4" in log, log
    assert "✅" not in notify, notify
    assert "deferred" in log, log
    assert proc.returncode != 0, log


def test_hung_yesterday_alert_does_not_block_todays_collection(tmp_path):
    """astra 10.10 MEDIUM (round 2): the yesterday alert runs before collecting; a hung
    send must be cut, today's snapshot still taken, and the undelivered alert retried later."""
    t0 = time.monotonic()
    proc, calls, _, notify, log = _run(tmp_path, "ok", utc_hour=6, yesterday_complete=False,
                                       notify_hangs=1,
                                       extra_env={"RANK_SNAPSHOT_NOTIFY_TIMEOUT": "2"})
    elapsed = time.monotonic() - t0
    assert elapsed < 30, (elapsed, log)
    assert calls == 1, log
    assert "✅" in notify, (notify, log)
    assert not list((tmp_path / "out").glob(".lost_alerted_*")), log
    assert proc.returncode == 0, log


def test_corrupt_started_at_does_not_hide_todays_complete_snapshot(tmp_path):
    """hard-verifier 10.10 INFO: float() outside the try crashed the whole archive scan,
    so one bad file made a complete day look missing (extra collection, false lost alert)."""
    proc, calls, _, notify, log = _run(tmp_path, "ok", utc_hour=6, corrupt_started_at=True,
                                       preexisting="collection_complete.json")
    assert calls == 0, log
    assert "skip:" in log, log
    assert notify == "", notify


def _unit_values(path, key):
    return [line.split("=", 1)[1].strip() for line in path.read_text().splitlines()
            if line.split("=", 1)[0].strip() == key]


def test_serv1_timer_slots_match_the_lost_day_threshold():
    """Five UTC slots; the last one equals the runner's LAST_SLOT_UTC_HOUR default."""
    (cal,) = _unit_values(TIMER, "OnCalendar")
    assert cal.endswith(" UTC"), cal  # independent of serv1's local time zone (MSK)
    hours = [int(h) for h in cal.split()[1].split(":")[0].split(",")]
    assert hours == [6, 9, 12, 15, 18], cal
    assert _unit_values(TIMER, "Persistent") == ["true"]
    assert _unit_values(TIMER, "Unit") == ["rank-snapshots.service"]
    runner = RUNNER.read_text()
    assert "LAST_SLOT_UTC_HOUR=${RANK_SNAPSHOT_LAST_SLOT_UTC_HOUR:-%d}" % hours[-1] in runner


def test_serv1_service_runs_the_runner_in_foreground_at_low_priority():
    assert _unit_values(SERVICE, "Type") == ["oneshot"]
    assert _unit_values(SERVICE, "WorkingDirectory") == ["/root/main"]
    assert _unit_values(SERVICE, "ExecStart") == [
        "/bin/bash /root/main/scripts/run/collect_rank_snapshots.sh"]
    assert _unit_values(SERVICE, "Nice") == ["19"]  # serv1 is prod-first (owner 08.10)
    assert _unit_values(SERVICE, "IOSchedulingClass") == ["idle"]


def test_runner_has_no_mac_paths():
    """serv1 has no /Users/alex/...: the runner must find the repo from its own location."""
    assert "/Users/" not in RUNNER.read_text()


def test_runner_works_in_a_serv1_shaped_checkout(tmp_path):
    """Repo copy with only venv/ (serv1 layout, no venv_catboost), started from another cwd:
    the default collector module and default output dir resolve inside that checkout."""
    repo = tmp_path / "main"
    (repo / "scripts" / "run").mkdir(parents=True)
    (repo / "scripts" / "run" / "collect_rank_snapshots.sh").write_text(RUNNER.read_text())
    (repo / "venv" / "bin").mkdir(parents=True)
    (repo / "venv" / "bin" / "python3").symlink_to(sys.executable)
    (repo / "base" / "tools").mkdir(parents=True)
    (repo / "base" / "__init__.py").write_text("")
    (repo / "base" / "tools" / "__init__.py").write_text("")
    (repo / "base" / "tools" / "collect_rank_snapshots.py").write_text(FAKE_COLLECT)
    state = tmp_path / "state"
    state.mkdir()
    for name, body in (("net.py", FAKE_NET), ("notify.py", FAKE_NOTIFY)):
        (tmp_path / name).write_text(body)
    env = {k: v for k, v in os.environ.items() if not k.startswith("RANK_SNAPSHOT_")}
    env.update(RANK_SNAPSHOT_NET_CHECK_CMD=f"{sys.executable} {tmp_path / 'net.py'}",
               RANK_SNAPSHOT_NOTIFY_CMD=f"{sys.executable} {tmp_path / 'notify.py'}",
               RANK_SNAPSHOT_RETRY_SLEEP="0", RANK_SNAPSHOT_NET_SLEEP="0",
               RANK_SNAPSHOT_UTC_HOUR="6",
               FAKE_STATE=str(state), FAKE_SEQ="ok", FAKE_FIX=str(FIX))
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    proc = subprocess.run(["bash", str(repo / "scripts" / "run" / "collect_rank_snapshots.sh")],
                          env=env, capture_output=True, text=True, timeout=120,
                          stdin=subprocess.DEVNULL, cwd=str(elsewhere))
    out = repo / "runtime" / "artifacts" / "misc" / "rank_snapshots"
    log = "".join(p.read_text() for p in out.glob("collect_*.log"))
    assert proc.returncode == 0, (proc.stderr, log)
    assert list(out.glob("*/collection.json")), log
    assert "✅" in (state / "notify.txt").read_text()


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


def test_snapshot_taken_before_midnight_counts_for_the_start_day(tmp_path):
    """hard-verifier 10.10 LOW: the attempt loop checked only the current UTC day. A run
    started on day D whose complete snapshot is stamped D but checked after 00 UTC kept
    collecting and then reported D as lost although D has a complete snapshot."""
    import datetime as dt
    day_d = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).date()
    stamped = dt.datetime(day_d.year, day_d.month, day_d.day, 23, 0, tzinfo=dt.timezone.utc)
    proc, calls, _, notify, log = _run(tmp_path, "ok", utc_hour=18, start_date=day_d.isoformat(),
                                       yesterday_complete=False, complete_days_ago=(2,),
                                       started_at=stamped.timestamp())
    assert calls == 1, log
    assert "потерян" not in notify, notify
    assert "✅" in notify and proc.returncode == 0, log
