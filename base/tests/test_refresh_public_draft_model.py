import json
from pathlib import Path

import numpy as np
import pytest

from base import refresh_public_draft_model as refresh
from base.evaluate_pro_transfer import load_pro_drafts, parse_pro_draft, transfer_metrics


def pro_record(map_id=1, start=1000, win=True, heroes=None):
    heroes = heroes or list(range(1, 11))
    players = [
        {"isRadiant": i < 5, "position": f"POSITION_{(i % 5) + 1}", "heroId": hero}
        for i, hero in enumerate(heroes)
    ]
    return {"id": map_id, "startDateTime": start, "didRadiantWin": win, "players": players}


# ------------------------------------------------------------------ pro parsing
def test_pro_draft_keeps_radiant_then_dire_position_order():
    draft = parse_pro_draft(pro_record(heroes=[11, 12, 13, 14, 15, 21, 22, 23, 24, 25]))
    assert draft is not None
    assert draft.heroes == (11, 12, 13, 14, 15, 21, 22, 23, 24, 25)
    assert draft.radiant_win == 1


def test_pro_draft_rejects_broken_records():
    short = pro_record(); short["players"] = short["players"][:9]
    assert parse_pro_draft(short) is None
    duplicate_hero = pro_record(heroes=[1, 1, 3, 4, 5, 6, 7, 8, 9, 10])
    assert parse_pro_draft(duplicate_hero) is None
    no_outcome = pro_record(); no_outcome["didRadiantWin"] = None
    assert parse_pro_draft(no_outcome) is None
    no_start = pro_record(); no_start.pop("startDateTime")
    assert parse_pro_draft(no_start) is None
    bad_slot = pro_record(); bad_slot["players"][0]["position"] = "POSITION_9"
    assert parse_pro_draft(bad_slot) is None
    missing_slot = pro_record()
    missing_slot["players"][0]["position"] = missing_slot["players"][1]["position"]
    assert parse_pro_draft(missing_slot) is None


def test_load_pro_drafts_dedups_and_sorts(tmp_path: Path):
    first = tmp_path / "7.41a_part001.json"
    second = tmp_path / "7.41b_part001.json"
    first.write_text(json.dumps({"1": pro_record(1, 300), "2": pro_record(2, 100)}))
    second.write_text(json.dumps({"2": pro_record(2, 100), "3": pro_record(3, 200)}))
    drafts, audit = load_pro_drafts([first, second])
    assert [d.map_id for d in drafts] == [2, 3, 1]
    assert audit["accepted"] == 3
    assert audit["duplicate_map_id"] == 1


def test_load_pro_drafts_survives_unreadable_file(tmp_path: Path):
    good = tmp_path / "good.json"; good.write_text(json.dumps({"1": pro_record(1, 10)}))
    bad = tmp_path / "bad.json"; bad.write_text("{not json")
    drafts, audit = load_pro_drafts([good, bad])
    assert len(drafts) == 1 and audit["file_error"] == 1


# ----------------------------------------------------------------- pro metrics
def test_transfer_metrics_reports_decile_spread():
    # imperfectly ordered on purpose: a flawless AUC collapses the normal-
    # approximation interval to zero, which no real corpus ever does
    outcomes = [i >= 50 for i in range(1, 101)]
    outcomes[10], outcomes[90] = True, False
    drafts = [parse_pro_draft(pro_record(i, i, win=outcomes[i - 1])) for i in range(1, 101)]
    drafts = [d for d in drafts if d is not None]
    probability = np.linspace(0.1, 0.9, len(drafts))
    metrics = transfer_metrics(drafts, probability)
    assert metrics["rows"] == len(drafts)
    assert 0.9 < metrics["auc"] < 1.0
    assert metrics["decile_spread"] == metrics["top_decile_rate"] - metrics["bottom_decile_rate"]
    assert metrics["auc_ci95"] > 0


def test_transfer_metrics_handles_single_class():
    drafts = [parse_pro_draft(pro_record(i, i, win=True)) for i in range(1, 21)]
    metrics = transfer_metrics([d for d in drafts if d is not None], np.linspace(0, 1, 20))
    assert metrics["auc"] is None and metrics["rows"] == 20


# -------------------------------------------------------------------- the gate
def test_promote_needs_both_windows_when_pro_has_enough_rows():
    assert refresh.verdict(+0.004, +0.002, 900)["decision"] == "PROMOTE"
    assert refresh.verdict(+0.004, -0.002, 900)["decision"] == "HOLD"
    assert refresh.verdict(-0.004, +0.010, 900)["decision"] == "HOLD"


def test_thin_pro_window_is_reported_not_silently_passed():
    result = refresh.verdict(+0.004, -0.050, 100)
    assert result["decision"] == "PROMOTE"
    assert any("про-окно пропущено" in reason for reason in result["reasons"])
    assert any("100" in reason for reason in result["reasons"])


def test_missing_public_window_holds():
    assert refresh.verdict(None, +0.02, 5000)["decision"] == "HOLD"


def test_fit_end_is_the_last_validation_match():
    results = {"split_boundaries": {"validation": {"last": {"start_time": 1779797679, "map_id": 1}}}}
    assert refresh.fit_end(results) == 1779797679


def test_champion_must_carry_every_artifact(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(refresh, "EXPERIMENT_DIR", tmp_path)
    incomplete = tmp_path / "2026-08-05_run"
    incomplete.mkdir()
    (incomplete / "results.json").write_text("{}")
    assert refresh.find_champion(None) is None
    complete = tmp_path / "2026-08-04_run"
    complete.mkdir()
    for name in refresh.REQUIRED_ARTIFACTS:
        (complete / name).write_text("{}")
    assert refresh.find_champion(None) == complete
    assert refresh.find_champion(incomplete) == incomplete


def test_sync_never_mirrors_deletions(monkeypatch):
    seen: list[list[str]] = []

    class Finished:
        returncode = 0
        stdout = "Number of regular files transferred: 3\n"
        stderr = ""

    monkeypatch.setattr(refresh.subprocess, "run", lambda cmd, **kw: seen.append(cmd) or Finished())
    report = refresh.sync_from_serv1()
    assert len(seen) == 2
    for command in seen:
        assert "--delete" not in command
        assert command[0] == "rsync"
    assert report["public"]["returncode"] == 0
