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


def test_snapshot_only_writes_analysis_without_new_verdict(monkeypatch, tmp_path) -> None:
    """tail_log должен видеть метрики и текст ставки даже до первого вердикта."""
    journal_path = _use_tmp_journal(monkeypatch, tmp_path)
    key = "dltv.org/matches/8973949050.8"

    runtime._record_map_verdict(
        key,
        verdict="parsed",
        kind="info",
        reason="match_parsed",
        identity={"match_id": "8973949050", "map_num": 2, "status": "live"},
        create_only=True,
    )
    runtime._flush_map_analysis_snapshot(
        key,
        {
            "identity": {"score": "5 : 3"},
            "metrics": {"early_output": {"counterpick_1vs1": 3}},
            "star": {"has_late_star": True, "late_wr_pct": 65.0},
            "elo": {"radiant_base_rating": 1774},
            "bet_message": "СТАВКА НА Team Synapse x0.5\nTeam Synapse VS Inner Circle x Insanity",
        },
    )

    entry = _load_journal(journal_path)[key]
    assert [item["reason"] for item in entry["verdicts"]] == ["match_parsed"]
    assert entry["metrics"]["early_output"]["counterpick_1vs1"] == 3
    assert entry["star"]["late_wr_pct"] == 65.0
    assert entry["elo"]["radiant_base_rating"] == 1774
    assert "СТАВКА НА Team Synapse" in entry["bet_message"]
    assert entry["score"] == "5 : 3"


def test_win_model_delivery_block_records_reject_and_bet_message(
    monkeypatch, tmp_path
) -> None:
    """Отказ ML на доставке обязан попасть в журнал с полным текстом ставки."""
    journal_path = _use_tmp_journal(monkeypatch, tmp_path)
    key = "dltv.org/matches/8973949050.8"
    message = (
        "СТАВКА НА Team Synapse x0.5\n"
        "Team Synapse VS Inner Circle x Insanity\n"
        "🤖 ML-модель: Dire 63.5% | ML от кэфа: 1.54\n"
        "Оценка WR:\n"
        "All: Team Synapse WR≈65.0% от кэфа 1.54\n"
    )
    runtime._record_map_verdict(
        key, verdict="parsed", kind="info", reason="match_parsed", create_only=True
    )
    monkeypatch.setattr(
        runtime,
        "_bookmaker_prepare_message_for_delivery",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("gate leaked to prepare")),
    )

    delivered = runtime._deliver_and_persist_signal(
        key,
        message,
        add_url_reason="star_signal_sent_now",
        stake_multiplier_context={
            "target_side": "radiant",
            "stake_team_name": "Team Synapse",
        },
    )

    assert delivered is False
    entry = _load_journal(journal_path)[key]
    reject = [item for item in entry["verdicts"] if item.get("kind") == "reject"]
    assert reject, entry["verdicts"]
    assert reject[-1]["reason"] == "model_against"
    assert "ML-модель против таргета" in reject[-1]["verdict"]
    assert entry["bet_message"].startswith("СТАВКА НА Team Synapse")


def test_record_bet_message_keeps_latest_and_never_clears(monkeypatch, tmp_path) -> None:
    """bet_message (готовый текст ставки для tail_log) заменяется свежим
    снапшотом и не затирается вызовами без него."""
    journal_path = _use_tmp_journal(monkeypatch, tmp_path)
    key = "dltv.org/matches/98/bet.1"

    runtime._record_map_verdict(
        key, verdict="v1", kind="reject", reason="r1", bet_message="СТАВКА НА A x1\nA VS B"
    )
    runtime._record_map_verdict(
        key,
        verdict="v2",
        kind="reject",
        reason="r2",
        bet_message="СТАВКА НА A x0.5\nA VS B\nобновлено",
    )
    data = _load_journal(journal_path)
    assert data[key]["bet_message"] == "СТАВКА НА A x0.5\nA VS B\nобновлено"

    # Вызов без bet_message не затирает сохранённый текст.
    runtime._record_map_verdict(key, verdict="v3", kind="send", reason="r3")
    data = _load_journal(journal_path)
    assert data[key]["bet_message"] == "СТАВКА НА A x0.5\nA VS B\nобновлено"


def test_record_kills_window_block(monkeypatch, tmp_path) -> None:
    """Блок kills_window (expected_diff по окнам киллов) сохраняется в entry
    и заменяется свежим снапшотом."""
    journal_path = _use_tmp_journal(monkeypatch, tmp_path)
    key = "dltv.org/matches/97/kw.1"

    runtime._record_map_verdict(
        key,
        verdict="v1",
        kind="reject",
        reason="r1",
        kills_window={"ed_by_label": {"10-20": 1.25}},
    )
    data = _load_journal(journal_path)
    assert data[key]["kills_window"] == {"ed_by_label": {"10-20": 1.25}}

    runtime._record_map_verdict(
        key,
        verdict="v2",
        kind="reject",
        reason="r2",
        kills_window={"ed_by_label": {"10-20": 0.8, "20-30": -0.4}},
    )
    data = _load_journal(journal_path)
    assert data[key]["kills_window"] == {
        "ed_by_label": {"10-20": 0.8, "20-30": -0.4}
    }


def test_create_only_records_once_per_map(monkeypatch, tmp_path) -> None:
    """create_only (первичная запись при парсинге) создаёт entry один раз и
    не затирает накопленные вердикты при повторных вызовах."""
    journal_path = _use_tmp_journal(monkeypatch, tmp_path)
    key = "dltv.org/matches/96/parse-once.1"

    runtime._record_map_verdict(
        key,
        verdict="parsed",
        kind="info",
        reason="match_parsed",
        identity={"match_id": "96", "teams": {"radiant": "A", "dire": "B"}},
        create_only=True,
    )
    runtime._record_map_verdict(key, verdict="rejected", kind="reject", reason="no_star")
    # Повторный парсинг той же карты не должен добавлять дубль info-записи.
    runtime._record_map_verdict(
        key,
        verdict="parsed again",
        kind="info",
        reason="match_parsed",
        identity={"match_id": "96", "teams": {"radiant": "A", "dire": "B"}},
        create_only=True,
    )

    data = _load_journal(journal_path)
    verdicts = data[key]["verdicts"]
    assert [item["kind"] for item in verdicts] == ["info", "reject"]
    assert data[key]["teams"] == {"radiant": "A", "dire": "B"}


def test_tail_log_reads_journal_end_to_end(monkeypatch, tmp_path) -> None:
    """tail_log берёт последние матчи прямо из журнала вердиктов."""
    _use_tmp_journal(monkeypatch, tmp_path)
    runtime._record_map_verdict(
        "dltv.org/matches/97/lgd-vs-1win.1",
        verdict="delayed",
        kind="delayed",
        reason="late_all_no_early_star_pre27_watcher",
        metrics={"early_output": {"solo": 0.62}},
        bet_message="СТАВКА НА LGD Gaming x0.5\nLGD Gaming VS 1w\n4:7",
        identity={
            "match_id": "97",
            "status": "live",
            "score": "4 : 7",
            "teams": {"radiant": "LGD Gaming", "dire": "1w"},
        },
    )
    monkeypatch.setattr(runtime, "monitored_matches", {}, raising=False)
    sent_messages = []
    monkeypatch.setattr(
        runtime,
        "send_message",
        lambda message, **kwargs: sent_messages.append(str(message)),
    )

    runtime._send_admin_log_tail(line_count=100, raw_odds=False)

    assert len(sent_messages) == 1
    message = sent_messages[0]
    assert "LGD Gaming vs 1w" in message
    assert "97/lgd-vs-1win" in message
    assert "СТАВКА НА LGD Gaming x0.5" in message
    assert "late_all_no_early_star_pre27_watcher" in message


def test_journal_prunes_oldest_entries_over_limit(monkeypatch, tmp_path) -> None:
    """При превышении лимита записей самые старые выкидываются."""
    journal_path = _use_tmp_journal(monkeypatch, tmp_path)
    monkeypatch.setattr(runtime, "MAP_VERDICTS_MAX_ENTRIES", 5)
    for idx in range(7):
        runtime._record_map_verdict(
            f"dltv.org/matches/{900 + idx}/t-vs-t.1",
            verdict=f"v{idx}",
            kind="info",
            reason="match_parsed",
            identity={"match_id": str(900 + idx)},
        )

    data = _load_journal(journal_path)
    assert len(data) == 5
    # Две самые старые записи удалены, свежая сохранена.
    assert "dltv.org/matches/900/t-vs-t.1" not in data
    assert "dltv.org/matches/901/t-vs-t.1" not in data
    assert "dltv.org/matches/906/t-vs-t.1" in data
