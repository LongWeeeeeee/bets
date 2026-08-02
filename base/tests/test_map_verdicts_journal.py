from __future__ import annotations

import json
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402


def _use_tmp_journal(monkeypatch, tmp_path) -> Path:
    journal_path = tmp_path / "map_verdicts.json"
    monkeypatch.setenv("MAP_VERDICTS_PATH", str(journal_path))
    return journal_path


def _load_journal(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_single_entry_per_map_and_verdicts_accumulate(monkeypatch, tmp_path) -> None:
    journal_path = _use_tmp_journal(monkeypatch, tmp_path)

    runtime._record_map_verdict(
        "dltv.org/matches/123/foo.2",
        verdict="   ⏳ ВЕРДИКТ: delayed",
        kind="delayed",
        reason="late_star_pub_comeback_table_monitor",
        metrics={"early_output": {"wr": 61.0}},
        dispatch={"dispatch_mode": "delayed_late_only_27_00m", "game_time": 1500},
    )
    runtime._record_map_verdict(
        "dltv.org/matches/123/foo.2",
        verdict="   ✅ ВЕРДИКТ: sent",
        kind="send",
        reason="star_signal_sent_delayed",
        metrics={"early_output": {"wr": 63.5}},
        dispatch={"dispatch_mode": "delayed_late_only_27_00m", "game_time": 1621},
    )

    data = _load_journal(journal_path)
    assert list(data.keys()) == ["dltv.org/matches/123/foo.2"]
    entry = data["dltv.org/matches/123/foo.2"]
    assert entry["match_key"] == "dltv.org/matches/123/foo.2"
    assert [v["kind"] for v in entry["verdicts"]] == ["delayed", "send"]
    assert entry["verdicts"][0]["reason"] == "late_star_pub_comeback_table_monitor"
    assert entry["verdicts"][1]["dispatch"]["game_time"] == 1621
    # metrics replaced by the latest snapshot
    assert entry["metrics"] == {"early_output": {"wr": 63.5}}
    assert entry["first_seen_ts"] <= entry["updated_ts"]


def test_consecutive_duplicate_verdicts_collapse(monkeypatch, tmp_path) -> None:
    journal_path = _use_tmp_journal(monkeypatch, tmp_path)
    key = "dltv.org/matches/55/bar.1"

    for _ in range(3):
        runtime._record_map_verdict(
            key,
            verdict="   ⚠️ ВЕРДИКТ: ОТКАЗ (soft)",
            kind="reject",
            reason="soft_recheck",
        )
    runtime._record_map_verdict(
        key,
        verdict="   ✅ ВЕРДИКТ: sent",
        kind="send",
        reason="star_signal_sent_now",
    )
    runtime._record_map_verdict(
        key,
        verdict="   ⚠️ ВЕРДИКТ: ОТКАЗ (soft)",
        kind="reject",
        reason="soft_recheck",
    )

    entry = _load_journal(journal_path)[key]
    assert [v["kind"] for v in entry["verdicts"]] == ["reject", "send", "reject"]


def test_metrics_blocks_not_overwritten_by_none(monkeypatch, tmp_path) -> None:
    journal_path = _use_tmp_journal(monkeypatch, tmp_path)
    key = "dltv.org/matches/77/baz.3"

    runtime._record_map_verdict(
        key,
        verdict="v1",
        kind="info",
        reason="r1",
        metrics={"mid_output": {"wr": 70.0}},
        star={"has_late_star": True},
        elo={"radiant_elo": 1650},
    )
    # Second call without blocks must not erase the stored snapshot.
    runtime._record_map_verdict(key, verdict="v2", kind="send", reason="r2")

    entry = _load_journal(journal_path)[key]
    assert entry["metrics"] == {"mid_output": {"wr": 70.0}}
    assert entry["star"] == {"has_late_star": True}
    assert entry["elo"] == {"radiant_elo": 1650}
    assert len(entry["verdicts"]) == 2


def test_identity_fields_merged_into_entry(monkeypatch, tmp_path) -> None:
    journal_path = _use_tmp_journal(monkeypatch, tmp_path)
    key = "dltv.org/matches/90/qux.1"

    runtime._record_map_verdict(
        key,
        verdict="v1",
        kind="send",
        reason="r1",
        identity={
            "match_id": "90",
            "map_num": 1,
            "status": "live",
            "teams": {"radiant": "Team A", "dire": "Team B"},
        },
    )

    entry = _load_journal(journal_path)[key]
    assert entry["match_id"] == "90"
    assert entry["map_num"] == 1
    assert entry["teams"] == {"radiant": "Team A", "dire": "Team B"}


def test_non_json_values_are_sanitized(monkeypatch, tmp_path) -> None:
    journal_path = _use_tmp_journal(monkeypatch, tmp_path)
    key = "dltv.org/matches/91/quux.1"

    runtime._record_map_verdict(
        key,
        verdict="v1",
        kind="info",
        reason="r1",
        metrics={1: "one", "nested": {"a", "b"}, "blob": b"\x01\x02"},
        extra={"when": None},
    )

    entry = _load_journal(journal_path)[key]
    # orjson OPT_NON_STR_KEYS stringifies int keys; sets/blob fall back to str.
    assert entry["metrics"]["1"] == "one"
    assert isinstance(entry["metrics"]["blob"], str)
    json.dumps(entry)  # whole entry must stay JSON-serializable


def test_verdicts_history_capped(monkeypatch, tmp_path) -> None:
    journal_path = _use_tmp_journal(monkeypatch, tmp_path)
    monkeypatch.setattr(runtime, "MAP_VERDICTS_MAX_PER_MAP", 3)
    key = "dltv.org/matches/92/corge.1"

    for idx in range(5):
        runtime._record_map_verdict(
            key,
            verdict=f"verdict-{idx}",
            kind="info",
            reason=f"r{idx}",
        )

    entry = _load_journal(journal_path)[key]
    assert [v["verdict"] for v in entry["verdicts"]] == [
        "verdict-2",
        "verdict-3",
        "verdict-4",
    ]


def test_test_disable_add_url_skips_journal(monkeypatch, tmp_path) -> None:
    journal_path = _use_tmp_journal(monkeypatch, tmp_path)
    monkeypatch.setattr(runtime, "TEST_DISABLE_ADD_URL", True)

    runtime._record_map_verdict(
        "dltv.org/matches/93/grault.1",
        verdict="v1",
        kind="send",
        reason="r1",
        metrics={"x": 1},
    )

    assert not journal_path.exists()


def test_never_raises_on_unwritable_path(monkeypatch, tmp_path) -> None:
    # Point the journal at a directory: atomic write must fail internally,
    # but the pipeline-facing API must not raise.
    monkeypatch.setenv("MAP_VERDICTS_PATH", str(tmp_path))

    runtime._record_map_verdict(
        "dltv.org/matches/94/garply.1",
        verdict="v1",
        kind="send",
        reason="r1",
    )


def test_empty_key_or_verdict_ignored(monkeypatch, tmp_path) -> None:
    journal_path = _use_tmp_journal(monkeypatch, tmp_path)

    runtime._record_map_verdict("", verdict="v1", kind="send", reason="r1")
    runtime._record_map_verdict("dltv.org/matches/95/x.1", verdict="  ", kind="send", reason="r1")

    assert not journal_path.exists()
