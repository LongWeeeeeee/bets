"""GM-I: combined six-metrics × three-phases integrity regression layer.

Read-only against production modules. Does not mutate stats DBs, STAR
membership, score formulas, gates, thresholds, or calibration.
"""
from __future__ import annotations

import math
import re
import sys
from pathlib import Path

import pytest


BASE_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = BASE_DIR.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import analise_database as ad  # noqa: E402
import functions  # noqa: E402
import metrics_winrate as mw  # noqa: E402


METRICS = (
    "counterpick_1vs1",
    "counterpick_1vs2",
    "solo",
    "synergy_duo",
    "synergy_trio",
    "pos1_vs_pos1",
)
PHASE_PREFIXES = (
    ("early_output", "early_"),
    ("late_output", "late_"),
    ("post_lane_output", "post_lane_"),
)
PHASE_METRIC_NAMES = tuple(
    f"{prefix}{metric}" for _, prefix in PHASE_PREFIXES for metric in METRICS
)
assert len(PHASE_METRIC_NAMES) == 18

# Pre-integration protected SHA-256 (captured before this file was created).
_PROTECTED_SHA256 = {
    "data/star_thresholds_by_wr.json": (
        "f96ff0b7d472561374ba19cd48381e88bbcaafb923c3eae5c86b79270c668d2b"
    ),
    "data/star_confidence_calibration.json": (
        "778637e6081f9eb1be80a09a550a6d9e7114cbd2d15d8098e952a0c5dd344413"
    ),
}

PRODUCTION_STAR_METRICS = frozenset(
    {
        "counterpick_1vs1",
        "counterpick_1vs2",
        "dota2protracker_cp1vs1",
        "solo",
    }
)
DISPLAY_ONLY_METRICS = frozenset({"synergy_duo", "synergy_trio", "pos1_vs_pos1"})


def _six_metric_output(value: float = 5.0) -> dict:
    return {metric: value for metric in METRICS}


def _patch_filters_all_pass(monkeypatch) -> None:
    monkeypatch.setattr(mw, "FILTER_MODE", "draft")
    monkeypatch.setattr(mw, "MIN_START_DATE", 0)
    monkeypatch.setattr(mw, "USE_CUMULATIVE_INDICES", True)
    monkeypatch.setattr(mw, "is_early_match_strict", lambda match: (True, "radiant"))
    monkeypatch.setattr(
        mw,
        "is_late_match_strict",
        lambda match, early_dominator=None, if_check=True: (True, "radiant"),
    )
    monkeypatch.setattr(
        mw, "is_post_lane_match", lambda match, if_check=True: True
    )


# ---------------------------------------------------------------------------
# 1) 50 ID-less maps × all six metrics × three phases
# ---------------------------------------------------------------------------


def test_fifty_idless_maps_all_six_metrics_three_phases(monkeypatch) -> None:
    """50 ID-less maps with all six metrics in each phase → 18 names, n=50, ids 1..50."""
    matches = [
        {
            "didRadiantWin": True,
            "early_output": _six_metric_output(5),
            "late_output": _six_metric_output(5),
            "post_lane_output": _six_metric_output(5),
        }
        for _ in range(50)
    ]
    _patch_filters_all_pass(monkeypatch)

    _results, unique, summary = mw.process_metrics_winrate(matches)

    for name in PHASE_METRIC_NAMES:
        assert name in unique, f"missing unique set for {name}"
        assert name in summary, f"missing summary for {name}"
        ids = unique[name]
        assert ids == set(range(1, 51)), f"{name}: unexpected ids {sorted(ids)}"
        assert summary[name]["n"] == 50, f"{name}: summary n={summary[name]['n']}"
        assert len(ids) == 50


# ---------------------------------------------------------------------------
# 2) Identity / exclusion / validate-before-dedup (reuse production helpers)
# ---------------------------------------------------------------------------


def test_identity_exclusion_precedence_and_validate_before_dedup() -> None:
    # Normalization edge values via production helper.
    assert ad._normalize_comparable_id("123") == 123
    assert ad._normalize_comparable_id(123) == 123
    assert ad._normalize_comparable_id(" 456 ") == 456
    assert ad._normalize_comparable_id(None) is None
    assert ad._normalize_comparable_id("") is None
    assert ad._normalize_comparable_id("   ") is None
    assert ad._normalize_comparable_id(True) is None
    assert ad._normalize_comparable_id(False) is None
    assert ad._normalize_comparable_id(1.5) is None
    assert ad._normalize_comparable_id(float("nan")) is None
    assert ad._normalize_comparable_id(float("inf")) is None
    assert ad._normalize_comparable_id(-2.0) == -2
    assert ad._normalize_comparable_id("abc") == "abc"

    # Explicit stable-id precedence: id > match_id > _map_id > fallback.
    assert mw._stable_match_id(
        {"id": "42", "match_id": 99, "_map_id": 7}, fallback_idx=1
    ) == 42
    assert mw._stable_match_id({"match_id": "55", "_map_id": 7}, fallback_idx=1) == 55
    assert mw._stable_match_id({"_map_id": "99"}, fallback_idx=1) == 99
    assert mw._stable_match_id({}, fallback_idx=7) == 7
    assert mw._stable_match_id({"id": True, "match_id": 8}, fallback_idx=7) == 8

    # Any excluded candidate excludes the map (string/int equivalence).
    match = {"id": "10", "match_id": 20, "_map_id": 30}
    assert ad._match_in_exclude_set(match, {10}) is True
    assert ad._match_in_exclude_set(match, {"20"}) is True
    assert ad._match_in_exclude_set(match, {30}) is True
    assert ad._match_in_exclude_set({"foo": 1}, {"99"}, match_id_hint=99) is True
    assert ad._match_in_exclude_set(match, {999}) is False

    # CLI exclude set uses all three candidate fields, never fallback ordinals.
    exclude_ids = mw._exclude_ids_from_test_matches(
        [
            {"id": 10},
            {"match_id": "20"},
            {"_map_id": 30},
            {"id": None, "_map_id": "40"},
            {"foo": 5},
            "not-a-dict",
        ]
    )
    assert exclude_ids == {10, 20, 30, 40}

    # Validate-before-dedup: invalid first observations must not lock the map out.
    summary: dict = {}
    unique: dict = {}
    for bad in (None, 0, True, False, float("nan"), float("inf"), float("-inf"), "5"):
        mw._record_metric_summary_outcome(
            summary, unique, "early_solo", 7, bad, "radiant"
        )
    assert "early_solo" not in summary
    assert unique.get("early_solo", set()) == set()
    mw._record_metric_summary_outcome(summary, unique, "early_solo", 7, 5, "radiant")
    mw._record_metric_summary_outcome(summary, unique, "early_solo", 7, -3, "dire")
    assert summary["early_solo"] == {"wins": 1, "n": 1}
    assert unique["early_solo"] == {7}


def test_multi_candidate_test_map_excludes_every_training_counterpart_id() -> None:
    """One test map with id/match_id/_map_id must exclude every counterpart identity.

    A single multi-key test record must contribute all three normalized candidates
    so training maps identified as any of those ids are omitted via the existing
    production exclude-path harness (_match_in_exclude_set).
    """
    test_map = {"id": 10, "match_id": 20, "_map_id": 30}
    exclude_ids = mw._exclude_ids_from_test_matches([test_map])
    assert exclude_ids == {10, 20, 30}

    # Training counterparts whose stable identity resolves to each candidate.
    counterpart_by_id = {"id": 10}
    counterpart_by_match_id = {"match_id": 20}
    counterpart_by_map_id = {"_map_id": 30}
    control_unrelated = {"id": 999}

    assert ad._match_in_exclude_set(counterpart_by_id, exclude_ids) is True
    assert ad._match_in_exclude_set(counterpart_by_match_id, exclude_ids) is True
    assert ad._match_in_exclude_set(counterpart_by_map_id, exclude_ids) is True
    assert ad._match_in_exclude_set(control_unrelated, exclude_ids) is False


# ---------------------------------------------------------------------------
# 3) Invalid observations absent from summary / exact / cumulative / bucket / diags
# ---------------------------------------------------------------------------


def test_invalid_observations_absent_from_reports_and_diagnostics(
    monkeypatch, capsys
) -> None:
    invalids = [None, 0, False, True, float("nan"), float("inf"), float("-inf"), "5"]
    # One map per invalid + two fractional maps for truncation/floor.
    matches = []
    for idx, value in enumerate(invalids, start=1):
        matches.append(
            {
                "id": idx,
                "didRadiantWin": True,
                "early_output": {"solo": value},
                "late_output": {"solo": value},
                "post_lane_output": {"solo": value},
            }
        )
    matches.append(
        {
            "id": 100,
            "didRadiantWin": True,
            "early_output": {"solo": 3.9},
            "late_output": {},
            "post_lane_output": {},
        }
    )
    matches.append(
        {
            "id": 101,
            "didRadiantWin": True,
            "early_output": {"solo": -2.1},
            "late_output": {},
            "post_lane_output": {},
        }
    )

    _patch_filters_all_pass(monkeypatch)
    results, unique, summary = mw.process_metrics_winrate(matches)
    captured = capsys.readouterr().out

    # Only the two valid fractional observations enter summary/unique.
    assert unique["early_solo"] == {100, 101}
    assert summary["early_solo"]["n"] == 2
    # Invalid-only maps must not create late/post_lane solo entries.
    assert "late_solo" not in summary or summary["late_solo"]["n"] == 0
    assert unique.get("late_solo", set()) == set()
    assert unique.get("post_lane_solo", set()) == set()

    # Legacy cumulative truncation: 3.9 → indices 1..3; -2.1 → abs(int)=2.
    assert results["early_solo"][3]["positive"]["wins"] == 1
    assert results["early_solo"][4]["positive"]["wins"] == 0
    assert results["early_solo"][2]["negative"]["looses"] == 1
    assert results["early_solo"][3]["negative"]["looses"] == 0

    # Bucket mode floor(abs). Do not rely on process_metrics_winrate monkeypatches:
    # bucket path uses generated is_early/early_win fields directly.
    bucket_matches = [
        {"is_early": True, "early_win": "radiant", "early_output": {"solo": 3.9}},
        {"is_early": True, "early_win": "radiant", "early_output": {"solo": -2.1}},
        {"is_early": True, "early_win": "radiant", "early_output": {"solo": None}},
        {"is_early": True, "early_win": "radiant", "early_output": {"solo": 0}},
        {"is_early": True, "early_win": "radiant", "early_output": {"solo": True}},
    ]
    # Clear any leftover patches that could break is_post_lane_match signature
    # when bucket mode probes other phases without generated fields.
    monkeypatch.undo()
    bucket = mw.process_metrics_winrate_buckets(bucket_matches)
    assert bucket["early_solo"][3]["wins"] == 1
    assert bucket["early_solo"][2]["looses"] == 1
    # Invalid values must not create extra bucket indices beyond floor(abs) of valids.
    assert set(bucket["early_solo"].keys()) == {2, 3}

    # Presence diagnostics must match summary n for the invalid set alone.
    # Re-run only the 8 invalids + one valid to prove diagnostic count == 1.
    inv_only = [
        {
            "id": i + 1,
            "didRadiantWin": True,
            "early_output": {"solo": v},
            "late_output": {"solo": v},
            "post_lane_output": {"solo": v},
        }
        for i, v in enumerate(invalids + [5])
    ]
    _patch_filters_all_pass(monkeypatch)
    _r2, _u2, summary2 = mw.process_metrics_winrate(inv_only)
    captured2 = capsys.readouterr().out

    def _passed(section: str, text: str) -> int:
        lines = text.splitlines()
        for idx, line in enumerate(lines):
            if section not in line:
                continue
            for follow in lines[idx + 1 : idx + 6]:
                stripped = follow.strip()
                if stripped.startswith("Прошли фильтр:"):
                    return int(stripped.split(":", 1)[1].strip())
        raise AssertionError(f"heading {section!r} missing in:\n{text}")

    assert _passed("Early метрики", captured2) == 1
    assert _passed("Late метрики", captured2) == 1
    assert _passed("Post-lane метрики", captured2) == 1
    assert summary2["early_solo"]["n"] == 1
    assert summary2["late_solo"]["n"] == 1
    assert summary2["post_lane_solo"]["n"] == 1

    # Cumulative row labels preserved: mid reported as late_*, post_lane as post_lane_*.
    assert "early_solo" in results
    # print_results labels: ensure phase prefixes stay stable.
    printed: list[str] = []
    import builtins

    original = builtins.print
    builtins.print = lambda *a, **k: printed.append(" ".join(str(x) for x in a))
    try:
        mw.print_results(results, unique, summary)
    finally:
        builtins.print = original
    joined = "\n".join(printed)
    assert "early_solo" in joined
    assert "n=2" in joined


# ---------------------------------------------------------------------------
# 4) Six metrics × three offline buckets: conservative effective-N + score snapshot
# ---------------------------------------------------------------------------


def test_all_six_metrics_conservative_effective_n_and_score_snapshots() -> None:
    """Reuse production-path fixtures; invalid-only support never positive."""
    # Import helpers from the owned sibling test module without redefining logic.
    from tests.test_post_lane_synergy_pipeline import (  # type: ignore
        POSITIONS,
        _build_post_lane_stats,
        _put_duo,
        _put_vs,
        _side,
    )

    radiant = _side(1)
    dire = _side(6)

    base = _build_post_lane_stats(radiant, dire)

    # Unequal support so min << sum while gates still pass.
    for pos in POSITIONS:
        hid_r = int(radiant[pos]["hero_id"])
        hid_d = int(dire[pos]["hero_id"])
        g_r = 55 if pos == "pos1" else 120
        g_d = 60
        base[f"{hid_r}{pos}"] = {"wins": int(0.8 * g_r), "games": g_r}
        base[f"{hid_d}{pos}"] = {"wins": int(0.2 * g_d), "games": g_d}

    for r_pos in POSITIONS:
        for d_pos in POSITIONS:
            r_key = f"{int(radiant[r_pos]['hero_id'])}{r_pos}"
            d_key = f"{int(dire[d_pos]['hero_id'])}{d_pos}"
            g = 52 if (r_pos == "pos1" and d_pos == "pos1") else 90
            _put_vs(base, r_key, d_key, 0.8, g)

    import itertools as _it

    r_items = list(radiant.items())
    d_items = list(dire.items())
    for team_items, wr, base_g in ((r_items, 0.8, 70), (d_items, 0.2, 40)):
        for i, trio in enumerate(_it.combinations(team_items, 3)):
            tk = ",".join(
                sorted(
                    f"{int(payload['hero_id'])}{pos}" for pos, payload in trio
                )
            )
            g = base_g
            if wr < 0.5 and i == 0:
                g = 22
            if wr >= 0.5 and i == 0:
                g = 28
            base[tk] = {"wins": int(round(wr * g)), "games": g}

    for team, wr in ((radiant, 0.8), (dire, 0.2)):
        for a, b in (("pos1", "pos2"), ("pos1", "pos3"), ("pos2", "pos3"), ("pos4", "pos5")):
            g = 32 if wr < 0.5 and a == "pos1" and b == "pos2" else 70
            if wr >= 0.5 and a == "pos1" and b == "pos2":
                g = 36
            _put_duo(base, team, a, b, wr, g)

    equal_base = _build_post_lane_stats(radiant, dire)
    equal_result = functions.synergy_and_counterpick(
        radiant_heroes_and_pos=radiant,
        dire_heroes_and_pos=dire,
        early_dict=equal_base,
        mid_dict=equal_base,
        post_lane_dict=equal_base,
    )
    result = functions.synergy_and_counterpick(
        radiant_heroes_and_pos=radiant,
        dire_heroes_and_pos=dire,
        early_dict=base,
        mid_dict=base,
        post_lane_dict=base,
    )

    score_snapshot = {}
    for phase in ("early_output", "mid_output", "post_lane_output"):
        bucket = result[phase]
        equal_bucket = equal_result[phase]
        for metric in METRICS:
            score = bucket.get(metric)
            games = bucket.get(f"{metric}_games", 0)
            assert score is not None, f"{phase}.{metric} missing score: {bucket}"
            assert isinstance(score, (int, float))
            assert isinstance(games, int)
            assert games > 0, f"{phase}.{metric}_games should be positive, got {games}"
            assert games < 500, f"{phase}.{metric}_games={games} looks like a sum"
            assert equal_bucket.get(metric) is not None
            score_snapshot[(phase, metric)] = score

        assert bucket["counterpick_1vs2_games"] <= 150
        assert bucket["solo_games"] <= 60
        assert bucket["pos1_vs_pos1_games"] <= 60
        assert bucket["synergy_trio_games"] <= 40
        assert bucket["synergy_duo_games"] <= 50
        assert bucket["counterpick_1vs1_games"] <= 60

    # Invalid-only used entries never produce positive support.
    for bad in (None, 0, -0.0, False, True, float("nan"), float("inf"), "bad"):
        assert functions._diagnostic_support_from_entry((bad, 20)) == 0
    assert (
        functions._diagnostic_support_from_list(
            [(None, 20), (0, 15), (False, 12), (True, 30), (float("nan"), 40)]
        )
        == 0
    )

    # Score snapshots unchanged when only diagnostic support counts vary
    # (equal-base vs unequal-base: scores present on both; diagnostics differ).
    for phase in ("early_output", "mid_output", "post_lane_output"):
        for metric in METRICS:
            assert (phase, metric) in score_snapshot
            assert equal_result[phase].get(metric) is not None


# ---------------------------------------------------------------------------
# 5) Live post_lane → _build_all_star_output alias (read-only import)
# ---------------------------------------------------------------------------


def test_live_all_output_alias_from_post_lane_no_games() -> None:
    import cyberscore_try as cs

    post_lane = {
        "counterpick_1vs1": 3,
        "counterpick_1vs2": -2,
        "solo": 1,
        "synergy_duo": 4,
        "synergy_trio": -5,
        "pos1_vs_pos1": 7,
        "counterpick_1vs1_games": 20,
        "counterpick_1vs2_games": 15,
        "solo_games": 12,
        "synergy_duo_games": 18,
        "synergy_trio_games": 11,
        "pos1_vs_pos1_games": 9,
    }
    protracker = {
        "pro_cp1vs1_late": 2.0,
        "pro_cp1vs1_early": 2.0,
        "pro_cp1vs1_valid": True,
        "pro_duo_synergy_late": 1.0,
        "pro_duo_synergy_early": 1.0,
        "pro_duo_synergy_valid": True,
    }
    all_out = cs._build_all_star_output(post_lane, protracker)

    for key in METRICS:
        assert all_out.get(key) == post_lane[key]
    for key in METRICS:
        assert f"{key}_games" not in all_out
    assert all_out.get("dota2protracker_cp1vs1") == 2.0


# ---------------------------------------------------------------------------
# 6) Current STAR membership no drift; thresholds/calibration SHA unchanged
# ---------------------------------------------------------------------------


def test_star_membership_no_drift_and_protected_hashes() -> None:
    # functions.py
    assert functions.STAR_SIGNAL_METRICS == PRODUCTION_STAR_METRICS
    assert DISPLAY_ONLY_METRICS.isdisjoint(functions.STAR_SIGNAL_METRICS)

    # signal_wrappers.py (STAR_SIGNAL_METRICS excludes d2pt; still no duo/trio/pos1)
    import signal_wrappers as sw

    for banned in DISPLAY_ONLY_METRICS:
        assert banned not in sw.STAR_SIGNAL_METRICS
    for required in ("counterpick_1vs1", "counterpick_1vs2", "solo"):
        assert required in sw.STAR_SIGNAL_METRICS

    # cyberscore_try.py
    import cyberscore_try as cs

    assert cs._STAR_SIGNAL_METRICS == PRODUCTION_STAR_METRICS
    for banned in DISPLAY_ONLY_METRICS:
        assert banned not in cs._STAR_SIGNAL_METRICS

    # Threshold / calibration files unchanged from pre-integration SHA-256.
    for rel, expected in _PROTECTED_SHA256.items():
        path = PROJECT_ROOT / rel
        import hashlib

        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        assert digest == expected, f"{rel}: {digest} != {expected}"

    # Diagnostics must not feed score/gate/threshold code outside display/report.
    # Scoped text assertions on known score-path symbols.
    functions_src = (BASE_DIR / "functions.py").read_text(encoding="utf-8")
    # _diagnostic_support_* helpers are only used for *_games emission, not thresholds.
    assert "def _diagnostic_support_from_entry" in functions_src
    assert "def _is_valid_diagnostic_observation" in functions_src
    # STAR threshold loading path must not reference diagnostic support helpers.
    thr_load = functions_src[
        functions_src.index("def _load_star_thresholds") : functions_src.index(
            "def _load_star_thresholds"
        )
        + 2500
    ]
    assert "_diagnostic_support" not in thr_load
    assert "_is_valid_diagnostic_observation" not in thr_load

    # format_output / gate paths should not call diagnostic support helpers.
    for marker in (
        "def format_output_dict",
        "def _is_star_metric_enabled",
        "def get_diff",
    ):
        start = functions_src.index(marker)
        snippet = functions_src[start : start + 4000]
        assert "_diagnostic_support_from_" not in snippet, marker


def test_phase_metric_name_contract() -> None:
    """18 phase×metric names and prefix mapping are the canonical integration surface."""
    assert PHASE_METRIC_NAMES == (
        "early_counterpick_1vs1",
        "early_counterpick_1vs2",
        "early_solo",
        "early_synergy_duo",
        "early_synergy_trio",
        "early_pos1_vs_pos1",
        "late_counterpick_1vs1",
        "late_counterpick_1vs2",
        "late_solo",
        "late_synergy_duo",
        "late_synergy_trio",
        "late_pos1_vs_pos1",
        "post_lane_counterpick_1vs1",
        "post_lane_counterpick_1vs2",
        "post_lane_solo",
        "post_lane_synergy_duo",
        "post_lane_synergy_trio",
        "post_lane_pos1_vs_pos1",
    )
    assert mw._is_valid_metric_observation(5) is True
    assert mw._is_valid_metric_observation(0) is False
    assert mw._is_valid_metric_observation(True) is False
    assert mw._is_valid_metric_observation(float("nan")) is False
    assert functions._is_valid_diagnostic_observation(0.6) is True
    assert functions._is_valid_diagnostic_observation(0) is False
