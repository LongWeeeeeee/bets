"""Regression checks for real production journal duplicate/timing hazards."""
import importlib.util
import json
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[2] / "scripts/ops/audit_prematch_confidence.py"
SPEC = importlib.util.spec_from_file_location("prematch_confidence_audit", MODULE_PATH)
AUDIT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(AUDIT)


def test_poll_suffix_and_map_number_do_not_multiply_one_real_map():
    first = {"match_key": "dltv.org/matches/8992864996.0", "ts": 100, "map_num": 1}
    repeat = dict(first, match_key="dltv.org/matches/8992864996.7", ts=120, map_num=2)
    assert AUDIT.first_per_map([repeat, first]) == {"8992864996": first}


def test_exact_label_side_timing_and_ambiguous_attribution(tmp_path):
    bet = {"match_key": "dltv.org/matches/8992864996.0", "ts": 100,
           "side": "dire", "index": -22.465, "confidence": 0.72465,
           "expected_wr": 0.7057, "model_elo": -95.2,
           "radiant_team": "Yangon Galacticos", "dire_team": "DIREBORN"}
    late = dict(bet, match_key="dltv.org/matches/8968286637.0", ts=400)
    ev = dict(bet, ts=99, branch="full", parts={"elo": -0.6249, "draft": -0.3708})
    (tmp_path / "prematch_model_bet_sent.jsonl").write_text(
        "\n".join(json.dumps(r) for r in [bet, late, dict(bet, ts=110)]))
    # Two matching evaluations must not silently produce a preferred attribution.
    (tmp_path / "prematch_model_eval.jsonl").write_text(
        "\n".join(json.dumps(r) for r in [ev, dict(ev, ts=98)]))
    (tmp_path / "source_labels.json").write_text(json.dumps({
        mid: {"radiant_win": False, "start": 90, "end": 300}
        for mid in ("8992864996", "8968286637")}))
    result = AUDIT.audit(tmp_path)
    assert result["overall"]["n"] == result["overall"]["wins"] == 1
    assert result["duplicate_rows"] == 1
    assert result["attribution_n"] == 0
    assert result["excluded"]["missing_end_or_prediction_after_finish"] == ["8968286637"]
    assert result["account_elo_direction_comparison"]["elo_wins"] == 1
