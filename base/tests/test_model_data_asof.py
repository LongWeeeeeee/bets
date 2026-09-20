"""Свежесть данных за драфт-артефактом: правила 1-4 + текст приписки."""
from __future__ import annotations

import datetime
import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
ROOT = BASE_DIR.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import model_data_asof as mda  # noqa: E402


def _ts(y, m, d):
    return int(datetime.datetime(y, m, d, tzinfo=datetime.timezone.utc).timestamp())


def test_rule1_accepted_when_shipped_equals_train(tmp_path):
    (tmp_path / "results.json").write_text(json.dumps({
        "counts": {"train": 100, "shipped_fit_rows": 100},
        "split_boundaries": {"train": {"last": {"start_time": _ts(2026, 8, 9)}}},
    }), encoding="utf-8")
    assert mda.data_asof_ts(tmp_path) == _ts(2026, 8, 9)


def test_rule1_rejected_falls_back_to_rule3(tmp_path):
    target = tmp_path / "2026-09-01_public_early_nw"
    target.mkdir()
    (target / "results.json").write_text(json.dumps({
        "counts": {"train": 50, "shipped_fit_rows": 100},
        "split_boundaries": {"train": {"last": {"start_time": _ts(2026, 6, 1)}}},
    }), encoding="utf-8")
    assert mda.data_asof_ts(target) == _ts(2026, 9, 1)


def test_rule2_manifest_key(tmp_path):
    (tmp_path / "manifest.json").write_text(json.dumps({
        "data_asof_ts": "2026-08-20",
    }), encoding="utf-8")
    assert mda.data_asof_ts(tmp_path) == _ts(2026, 8, 20)


def test_rule3_own_name(tmp_path):
    target = tmp_path / "2026-09-05_position_pairs"
    target.mkdir()
    assert mda.data_asof_ts(target) == _ts(2026, 9, 5)


def test_rule3_parent_name(tmp_path):
    parent = tmp_path / "2026-09-05_position_pairs"
    child = parent / "early_win"
    child.mkdir(parents=True)
    assert mda.data_asof_ts(child) == _ts(2026, 9, 5)


def test_rule3_grandparent_name(tmp_path):
    grandparent = tmp_path / "2026-09-05_position_pairs"
    child = grandparent / "mid" / "early_win"
    child.mkdir(parents=True)
    assert mda.data_asof_ts(child) == _ts(2026, 9, 5)


def test_rule4_newest_mtime(tmp_path):
    older = tmp_path / "a.joblib"
    newer = tmp_path / "b.joblib"
    older.write_bytes(b"x")
    newer.write_bytes(b"y")
    import os
    import time
    now = time.time()
    os.utime(older, (now - 200, now - 200))
    os.utime(newer, (now - 10, now - 10))
    assert mda.data_asof_ts(tmp_path) == int(now - 10)


def test_none_when_nothing_matches(tmp_path):
    assert mda.data_asof_ts(tmp_path) is None
    assert mda.data_asof_ts(None) is None


def test_freshness_note_format():
    ts = _ts(2026, 9, 1)
    now = _ts(2026, 9, 20) + 3600
    assert mda.freshness_note(ts, now) == "данные до 01.09 (19 дн.)"
    assert mda.freshness_note(None) == ""
    assert mda.freshness_note(0) == ""


def _skip_if_missing(path: Path):
    import pytest
    if not path.exists():
        pytest.skip(f"прод-каталог отсутствует: {path}")


def test_real_prod_dirs_if_present():
    import pytest
    cases = [
        (ROOT / "data/public_draft_hero10_experiment/2026-08-15_all_public_5m_full",
         "09.08"),
        (ROOT / "data/early_nw_draft/2026-09-01_public_early_nw", "01.09"),
        (ROOT / "data/late_draft_win/2026-08-29_public_5m_late36", "29.08"),
        (ROOT / "data/draft_phase_serving/2026-09-05_position_pairs/early_win",
         "05.09"),
    ]
    ran = False
    for path, expected_ddmm in cases:
        if not path.exists():
            continue
        ran = True
        note = mda.model_dir_note(path)
        assert expected_ddmm in note, (path, note)
    if not ran:
        pytest.skip("ни один прод-каталог не найден")
