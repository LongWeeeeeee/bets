import json
from pathlib import Path


THRESHOLDS_PATH = Path(__file__).resolve().parents[2] / "data" / "star_thresholds_by_wr.json"


def _metric_threshold(payload, level, section, metric):
    return dict(payload[str(level)][section]).get(metric)


def test_role_pooled_synergy_trio_uses_strict_oos_supported_wr_levels_only():
    payload = json.loads(THRESHOLDS_PATH.read_text(encoding="utf-8"))

    assert _metric_threshold(payload, 60, "early_output", "synergy_trio") == 25
    assert _metric_threshold(payload, 60, "mid_output", "synergy_trio") is None
    assert _metric_threshold(payload, 60, "all_output", "synergy_trio") == 10
    assert _metric_threshold(payload, 65, "all_output", "synergy_trio") == 19

    for level in (70, 75, 80, 85, 90, 95):
        for section in ("early_output", "mid_output", "all_output"):
            assert _metric_threshold(payload, level, section, "synergy_trio") is None

    for section in ("early_output", "mid_output"):
        assert _metric_threshold(payload, 65, section, "synergy_trio") is None
