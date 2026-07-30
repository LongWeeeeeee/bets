from __future__ import annotations

import json
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import metrics_winrate as runtime  # noqa: E402


def test_load_matches_converts_dict_to_list_with_map_id(tmp_path) -> None:
    path = tmp_path / "matches_dict.json"
    path.write_text(
        json.dumps(
            {
                "123": {"id": 123, "foo": 1},
                "456": {"id": 456, "bar": 2},
            }
        ),
        encoding="utf-8",
    )

    matches = runtime.load_matches(str(path))

    assert isinstance(matches, list)
    assert len(matches) == 2
    by_id = {m["id"]: m for m in matches}
    assert by_id[123]["_map_id"] == 123
    assert by_id[456]["_map_id"] == 456


def test_load_matches_list_shaped_unchanged(tmp_path) -> None:
    path = tmp_path / "matches_list.json"
    path.write_text(json.dumps([{"id": 1}, {"id": 2}]), encoding="utf-8")

    matches = runtime.load_matches(str(path))

    assert matches == [{"id": 1}, {"id": 2}]
    assert all("_map_id" not in m for m in matches)


def test_load_matches_streaming_path_handles_large_dict(monkeypatch, tmp_path) -> None:
    path = tmp_path / "matches_large.json"
    path.write_text('{"1":{"id":1},"2":{"id":2}}', encoding="utf-8")

    class _FakeStat:
        st_size = 60 * 1024 * 1024
        st_mode = 0o100644

    # Patch only the concrete Path instance used by load_matches — not global Path.stat.
    real_path_cls = runtime.Path
    target = real_path_cls(str(path))
    original_stat = real_path_cls.stat

    def _stat(self, *args, **kwargs):
        if self == target or str(self) == str(path):
            return _FakeStat()
        return original_stat(self, *args, **kwargs)

    monkeypatch.setattr(runtime.Path, "stat", _stat)

    class _FakeIjson:
        @staticmethod
        def kvitems(_fh, _prefix, use_float=True):
            assert use_float is True
            yield "1", {"id": 1}
            yield "2", {"id": 2}

        @staticmethod
        def items(_fh, _prefix, use_float=True):  # pragma: no cover
            raise AssertionError("list stream should not be used for dict JSON")

    monkeypatch.setattr(runtime, "ijson", _FakeIjson(), raising=False)

    matches = runtime.load_matches(str(path))

    assert [m["_map_id"] for m in matches] == [1, 2]
    assert [m["id"] for m in matches] == [1, 2]

    # Isolation: ordinary Path.stat still works after the test's instance-aware patch.
    assert Path(str(path)).stat().st_size > 0


def test_load_matches_streaming_path_handles_large_list(monkeypatch, tmp_path) -> None:
    path = tmp_path / "matches_large_list.json"
    path.write_text('[{"id":1},{"id":2}]', encoding="utf-8")

    class _FakeStat:
        st_size = 60 * 1024 * 1024
        st_mode = 0o100644

    real_path_cls = runtime.Path
    target = real_path_cls(str(path))
    original_stat = real_path_cls.stat

    def _stat(self, *args, **kwargs):
        if self == target or str(self) == str(path):
            return _FakeStat()
        return original_stat(self, *args, **kwargs)

    monkeypatch.setattr(runtime.Path, "stat", _stat)

    class _FakeIjson:
        @staticmethod
        def items(_fh, _prefix, use_float=True):
            assert _prefix == "item"
            assert use_float is True
            yield {"id": 1}
            yield {"id": 2}

        @staticmethod
        def kvitems(_fh, _prefix, use_float=True):  # pragma: no cover
            raise AssertionError("dict stream should not be used for list JSON")

    monkeypatch.setattr(runtime, "ijson", _FakeIjson(), raising=False)

    matches = runtime.load_matches(str(path))

    assert matches == [{"id": 1}, {"id": 2}]


def test_summary_average_wr_counts_each_map_once():
    """Cumulative threshold bins must not inflate summary average WR denominator."""
    # 10 unique maps, metric_value=5 each -> cumulative bins 1..5 inflate n by 5x.
    # 5 wins + 5 losses => 50% summary WR with n=10 (MIN_MATCHES_FOR_AVG).
    results = {
        "early_solo": {
            idx: {
                "positive": {"wins": 5 if idx <= 5 else 0, "looses": 5 if idx <= 5 else 0},
                "negative": {"wins": 0, "looses": 0},
            }
            for idx in range(1, 51)
        }
    }
    unique = {"early_solo": set(range(1, 11))}
    summary = {"early_solo": {"wins": 5, "n": 10}}

    assert summary["early_solo"]["n"] == len(unique["early_solo"])
    assert summary["early_solo"]["wins"] / summary["early_solo"]["n"] == 0.5

    # Cumulative table would claim n=50 (10 maps * 5 thresholds) — must not be used for avg.
    cumulative_n = sum(
        results["early_solo"][i]["positive"]["wins"] + results["early_solo"][i]["positive"]["looses"]
        for i in range(1, 6)
    )
    assert cumulative_n == 50

    captured = []

    def _fake_print(*args, **kwargs):
        captured.append(" ".join(str(a) for a in args))

    import builtins
    original = builtins.print
    builtins.print = _fake_print
    try:
        runtime.print_results(results, unique, summary)
    finally:
        builtins.print = original

    joined = "\n".join(captured)
    assert "early_solo" in joined
    assert "n=10" in joined
    assert "50.00%" in joined
    # Must not report the inflated cumulative denominator.
    assert "n=50" not in joined


def test_record_metric_summary_outcome_opposite_signs():
    summary = {}
    unique = {}
    # Map A: +3 predicts radiant, radiant wins -> win
    runtime._record_metric_summary_outcome(summary, unique, "late_cp", 1, 3, "radiant")
    # Map B: -4 predicts dire, radiant wins -> loss
    runtime._record_metric_summary_outcome(summary, unique, "late_cp", 2, -4, "radiant")
    # Duplicate map A must not double-count
    runtime._record_metric_summary_outcome(summary, unique, "late_cp", 1, 3, "radiant")

    assert summary["late_cp"] == {"wins": 1, "n": 2}
    assert unique["late_cp"] == {1, 2}


def test_record_metric_summary_outcome_validates_before_dedup():
    """Invalid first observation must not lock the map out of a later valid one."""
    summary = {}
    unique = {}
    runtime._record_metric_summary_outcome(summary, unique, "early_solo", 7, None, "radiant")
    runtime._record_metric_summary_outcome(summary, unique, "early_solo", 7, 0, "radiant")
    runtime._record_metric_summary_outcome(summary, unique, "early_solo", 7, True, "radiant")
    runtime._record_metric_summary_outcome(summary, unique, "early_solo", 7, float("nan"), "radiant")
    runtime._record_metric_summary_outcome(summary, unique, "early_solo", 7, float("inf"), "radiant")
    # First valid observation now counts.
    runtime._record_metric_summary_outcome(summary, unique, "early_solo", 7, 5, "radiant")
    # Later valid duplicate still ignored.
    runtime._record_metric_summary_outcome(summary, unique, "early_solo", 7, -3, "dire")

    assert summary["early_solo"] == {"wins": 1, "n": 1}
    assert unique["early_solo"] == {7}


def test_record_metric_summary_outcome_rejects_bool_zero_nonfinite():
    summary = {}
    unique = {}
    for mid, value in (
        (1, True),
        (2, False),
        (3, 0),
        (4, 0.0),
        (5, float("nan")),
        (6, float("inf")),
        (7, float("-inf")),
        (8, None),
        (9, "5"),
    ):
        runtime._record_metric_summary_outcome(summary, unique, "late_solo", mid, value, "radiant")
    assert "late_solo" not in summary
    assert unique.get("late_solo", set()) == set()

    runtime._record_metric_summary_outcome(summary, unique, "late_solo", 10, -2.7, "dire")
    assert summary["late_solo"] == {"wins": 1, "n": 1}


def test_stable_match_id_prefers_map_id():
    # Explicit precedence: id > match_id > _map_id.
    assert runtime._stable_match_id({"id": "42", "match_id": 99, "_map_id": 7}, fallback_idx=1) == 42
    assert runtime._stable_match_id({"_map_id": "99", "id": None}, fallback_idx=7) == 99
    assert runtime._stable_match_id({"match_id": "55"}, fallback_idx=7) == 55
    assert runtime._stable_match_id({"id": "42"}, fallback_idx=7) == 42
    assert runtime._stable_match_id({}, fallback_idx=7) == 7
    assert runtime._stable_match_id({"id": True, "match_id": 8}, fallback_idx=7) == 8


def test_id_less_maps_keep_stable_unique_fallback_ids_end_to_end(monkeypatch) -> None:
    """Outer map ordinal must not be shadowed by metric init loops (early/late/post).

    50 ID-less maps must each get a unique fallback id so summary n stays 50
    with no collisions across phases that re-init metric result structures.
    """
    matches = [
        {
            "didRadiantWin": True,
            "early_output": {"solo": 5, "counterpick_1vs1": 3},
            "late_output": {"solo": 5, "synergy_duo": 4},
            "post_lane_output": {"solo": 5, "counterpick_1vs2": 6},
        }
        for _ in range(50)
    ]

    monkeypatch.setattr(runtime, "FILTER_MODE", "draft")
    monkeypatch.setattr(runtime, "MIN_START_DATE", 0)
    monkeypatch.setattr(runtime, "USE_CUMULATIVE_INDICES", True)
    monkeypatch.setattr(
        runtime, "is_early_match_strict", lambda match: (True, "radiant")
    )
    monkeypatch.setattr(
        runtime,
        "is_late_match_strict",
        lambda match, early_dominator=None, if_check=True: (True, "radiant"),
    )
    monkeypatch.setattr(runtime, "is_post_lane_match", lambda match: True)

    _results, unique, summary = runtime.process_metrics_winrate(matches)

    metric_names = (
        "early_solo",
        "early_counterpick_1vs1",
        "late_solo",
        "late_synergy_duo",
        "post_lane_solo",
        "post_lane_counterpick_1vs2",
    )
    for metric_name in metric_names:
        ids = unique[metric_name]
        assert len(ids) == 50, f"{metric_name}: expected 50 unique ids, got {len(ids)}: {sorted(ids)}"
        assert summary[metric_name]["n"] == 50, f"{metric_name}: summary n={summary[metric_name]['n']}"
        # Fallback ordinals are 1..50 with no collisions after metric init.
        assert ids == set(range(1, 51)), f"{metric_name}: unexpected ids {sorted(ids)}"


def test_duplicate_explicit_ids_count_once_per_map_phase_metric(monkeypatch) -> None:
    matches = [
        {
            "id": "100",
            "didRadiantWin": True,
            "early_output": {"solo": 5},
            "late_output": {"solo": 5},
            "post_lane_output": {"solo": 5},
        },
        {
            "id": 100,  # str/int equivalent of first map
            "didRadiantWin": False,
            "early_output": {"solo": -5},
            "late_output": {"solo": -5},
            "post_lane_output": {"solo": -5},
        },
        {
            "id": 200,
            "didRadiantWin": True,
            "early_output": {"solo": 4},
            "late_output": {"solo": 4},
            "post_lane_output": {"solo": 4},
        },
    ]
    monkeypatch.setattr(runtime, "FILTER_MODE", "draft")
    monkeypatch.setattr(runtime, "MIN_START_DATE", 0)
    monkeypatch.setattr(runtime, "USE_CUMULATIVE_INDICES", True)
    monkeypatch.setattr(runtime, "is_early_match_strict", lambda match: (True, "radiant"))
    monkeypatch.setattr(
        runtime,
        "is_late_match_strict",
        lambda match, early_dominator=None, if_check=True: (
            True,
            "radiant" if match.get("didRadiantWin") else "dire",
        ),
    )
    monkeypatch.setattr(runtime, "is_post_lane_match", lambda match: True)

    _results, unique, summary = runtime.process_metrics_winrate(matches)
    for metric_name in ("early_solo", "late_solo", "post_lane_solo"):
        assert unique[metric_name] == {100, 200}
        assert summary[metric_name]["n"] == 2


def test_legacy_threshold_truncates_fractional_values(monkeypatch) -> None:
    """Old exact/cumulative mode uses int() truncation before threshold selection."""
    matches = [
        {
            "id": 1,
            "didRadiantWin": True,
            "early_output": {"solo": 3.9},  # truncates toward 3
            "late_output": {},
            "post_lane_output": {},
        },
        {
            "id": 2,
            "didRadiantWin": True,
            "early_output": {"solo": -2.1},  # abs(int(-2.1)) == 2
            "late_output": {},
            "post_lane_output": {},
        },
    ]
    monkeypatch.setattr(runtime, "FILTER_MODE", "draft")
    monkeypatch.setattr(runtime, "MIN_START_DATE", 0)
    monkeypatch.setattr(runtime, "USE_CUMULATIVE_INDICES", True)
    monkeypatch.setattr(runtime, "is_early_match_strict", lambda match: (True, "radiant"))
    monkeypatch.setattr(
        runtime,
        "is_late_match_strict",
        lambda match, early_dominator=None, if_check=True: (False, None),
    )
    monkeypatch.setattr(runtime, "is_post_lane_match", lambda match: False)

    results, _unique, summary = runtime.process_metrics_winrate(matches)
    # Cumulative: value 3.9 contributes to indices 1..3 only (not 4).
    assert results["early_solo"][3]["positive"]["wins"] == 1
    assert results["early_solo"][4]["positive"]["wins"] == 0
    # Negative -2.1: abs(int(-2.1))=2; predicts dire vs early_dominator radiant -> negative loss.
    assert results["early_solo"][2]["negative"]["looses"] == 1
    assert results["early_solo"][3]["negative"]["looses"] == 0
    assert summary["early_solo"]["n"] == 2


def test_bucket_mode_uses_floor_abs(monkeypatch) -> None:
    matches = [
        {"is_early": True, "early_win": "radiant", "early_output": {"solo": 3.9}},
        {"is_early": True, "early_win": "radiant", "early_output": {"solo": -2.1}},
    ]
    results = runtime.process_metrics_winrate_buckets(matches)
    # floor(abs(3.9))=3 win; floor(abs(-2.1))=2 predicts dire vs radiant -> loss
    assert results["early_solo"][3]["wins"] == 1
    assert results["early_solo"][2]["looses"] == 1


def test_phase_semantics_early_late_post_lane(monkeypatch) -> None:
    matches = [
        {
            "id": 1,
            "didRadiantWin": False,  # map winner = dire
            "early_output": {"solo": 5},  # predicts radiant
            "late_output": {"solo": 5},
            "post_lane_output": {"solo": 5},
        }
    ]
    monkeypatch.setattr(runtime, "FILTER_MODE", "draft")
    monkeypatch.setattr(runtime, "MIN_START_DATE", 0)
    monkeypatch.setattr(runtime, "USE_CUMULATIVE_INDICES", True)
    # Early uses early dominator (radiant), not map winner.
    monkeypatch.setattr(runtime, "is_early_match_strict", lambda match: (True, "radiant"))
    # Late / post-lane compare against map winner (dire here).
    monkeypatch.setattr(
        runtime,
        "is_late_match_strict",
        lambda match, early_dominator=None, if_check=True: (True, "dire"),
    )
    monkeypatch.setattr(runtime, "is_post_lane_match", lambda match: True)

    results, unique, summary = runtime.process_metrics_winrate(matches)
    assert "early_solo" in results and "late_solo" in results and "post_lane_solo" in results
    # early: predict radiant, early_dominator radiant -> win
    assert summary["early_solo"] == {"wins": 1, "n": 1}
    # late/post: predict radiant, actual map winner dire -> loss
    assert summary["late_solo"] == {"wins": 0, "n": 1}
    assert summary["post_lane_solo"] == {"wins": 0, "n": 1}


def test_exclude_test_from_train_uses_stable_id_candidates() -> None:
    """CLI/on-the-fly exclude set must use id / match_id / _map_id, not id only.

    Dict-shaped loaded test maps often carry external key as _map_id with no
    embedded id field — those must still enter the exclude set.
    """
    test_matches = [
        {"id": 10, "foo": 1},  # classic embedded id
        {"match_id": "20", "foo": 2},  # alternate field
        {"_map_id": 30, "foo": 3},  # external dict key, no embedded id
        {"id": None, "_map_id": "40", "foo": 4},  # null id, external key wins
        {"foo": 5},  # no usable id candidates -> skipped
        "not-a-dict",  # non-dict -> skipped
    ]

    exclude_ids = runtime._exclude_ids_from_test_matches(test_matches)

    assert exclude_ids == {10, 20, 30, 40}


def _diagnostic_passed_filter_count(captured: str, section_heading: str) -> int:
    """Parse `Прошли фильтр: N` under a diagnostics section heading in stdout."""
    lines = captured.splitlines()
    for idx, line in enumerate(lines):
        if section_heading not in line:
            continue
        for follow in lines[idx + 1 : idx + 6]:
            stripped = follow.strip()
            if stripped.startswith("Прошли фильтр:"):
                return int(stripped.split(":", 1)[1].strip())
        raise AssertionError(
            f"no 'Прошли фильтр' line found under heading {section_heading!r} in:\n{captured}"
        )
    raise AssertionError(f"heading {section_heading!r} not found in:\n{captured}")


def test_presence_diagnostics_use_canonical_valid_observations_for_all_phases(
    monkeypatch, capsys
) -> None:
    """Map-presence diagnostics must use _is_valid_metric_observation for all phases.

    Raw isinstance(int, float) wrongly accepts 0/bool/NaN/±inf; summary already
    accepts only finite nonzero values. With values
    [None, 0, False, True, nan, +inf, -inf, 5] each phase diagnostic must report
    the same count as summary n (1), not 7.
    """
    values = [None, 0, False, True, float("nan"), float("inf"), float("-inf"), 5]
    matches = [
        {
            "id": idx + 1,
            "didRadiantWin": True,
            "early_output": {"solo": value},
            "late_output": {"solo": value},
            "post_lane_output": {"solo": value},
        }
        for idx, value in enumerate(values)
    ]

    monkeypatch.setattr(runtime, "FILTER_MODE", "draft")
    monkeypatch.setattr(runtime, "MIN_START_DATE", 0)
    monkeypatch.setattr(runtime, "USE_CUMULATIVE_INDICES", True)
    monkeypatch.setattr(runtime, "is_early_match_strict", lambda match: (True, "radiant"))
    monkeypatch.setattr(
        runtime,
        "is_late_match_strict",
        lambda match, early_dominator=None, if_check=True: (True, "radiant"),
    )
    monkeypatch.setattr(runtime, "is_post_lane_match", lambda match: True)

    _results, _unique, summary = runtime.process_metrics_winrate(matches)
    captured = capsys.readouterr().out

    # Current equivalent headings for plan's Ранняя/Поздняя/Post-lane sections.
    early_diag = _diagnostic_passed_filter_count(captured, "Early метрики")
    late_diag = _diagnostic_passed_filter_count(captured, "Late метрики")
    post_lane_diag = _diagnostic_passed_filter_count(captured, "Post-lane метрики")

    assert early_diag == 1, f"early diagnostic count={early_diag}, stdout:\n{captured}"
    assert late_diag == 1, f"late diagnostic count={late_diag}, stdout:\n{captured}"
    assert post_lane_diag == 1, (
        f"post_lane diagnostic count={post_lane_diag}, stdout:\n{captured}"
    )
    assert summary["early_solo"]["n"] == 1
    assert summary["late_solo"]["n"] == 1
    assert summary["post_lane_solo"]["n"] == 1
