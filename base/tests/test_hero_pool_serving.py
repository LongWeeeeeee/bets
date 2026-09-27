"""Captured pro-map inputs at the All panel/dispatch boundary (E-343)."""
import math
import os
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from base import laning_serving as serving
from base import hero_pool_serving as pool
from base import win_model_veto as draft


FIXTURE = Path(__file__).parent / "fixtures/hero_pool_acc_hero_20260928.npz"
COEFS = (0.018089718077978715, 0.03202312233063573,
         0.013768603968361319)


def _teams(data, i):
    return tuple({f"pos{k + 1}": {
        "hero_id": int(data["heroes"][i, side * 5 + k]),
        "account_id": int(data["accounts"][i, side * 5 + k]),
    } for k in range(5)} for side in range(2))


def _expected_index(base_index, radiant, dire, table):
    counts = {(int(row[0]), int(row[1])): int(row[2]) for row in table}
    shift = 0.0
    for pos, coef in enumerate(COEFS, 1):
        r, d = radiant[f"pos{pos}"], dire[f"pos{pos}"]
        if not r["account_id"] or not d["account_id"]:
            continue
        rn = counts.get((r["account_id"], r["hero_id"]), 0)
        dn = counts.get((d["account_id"], d["hero_id"]), 0)
        shift += coef * (math.log1p(rn) - math.log1p(dn))
    p = 0.5 + base_index / 100.0
    corrected = 1.0 / (1.0 + math.exp(-(math.log(p / (1.0 - p)) + shift)))
    return (corrected - 0.5) * 100.0


@pytest.fixture
def boundary(monkeypatch):
    root = Path(os.environ.get("INGAME_ARTIFACT_ROOT", str(serving.ROOT)))
    model_dir = root / "data/draft_phase_serving/20260923_all7m/all"
    if not (model_dir / "radiant_win_model.joblib").exists():
        pytest.skip("captured serving draft model absent; set INGAME_ARTIFACT_ROOT")
    monkeypatch.setattr(draft, "MODEL_DIR", model_dir)
    monkeypatch.setattr(draft, "_state", {"loaded": False, "encoder": None,
                                        "model": None, "error": None})
    monkeypatch.setattr(draft, "_cache", {})
    monkeypatch.setattr(serving, "_SERVICE", SimpleNamespace(predict=lambda *args: None))
    monkeypatch.setenv("PREMATCH_ARTIFACT", str(FIXTURE))
    monkeypatch.setenv("ALL_HERO_POOL_ENABLED", "1")
    monkeypatch.setenv("ML_DISPATCH_MIN_CONF", "0.60")
    with np.load(FIXTURE) as z:
        yield {name: z[name].copy() for name in z.files}


@pytest.mark.parametrize("map_index", [0, 1, 2])
def test_captured_map_all_panel_and_verdict_share_formula(boundary, map_index):
    r, d = _teams(boundary, map_index)
    original = draft.win_index_draft(r, d)
    expected = _expected_index(original, r, d, boundary["acc_hero"])
    got = serving.verdicts(r, d, int(boundary["ts"][map_index]), draft_model=draft)["all"]
    assert got == {"side": "Radiant" if expected >= 0 else "Dire",
                   "confidence": (50 + abs(expected)) / 100.0}
    line = serving.panel_lines(r, d, int(boundary["ts"][map_index]),
                               draft_model=draft)["all_model_line"]
    star = " ★" if got["confidence"] >= 0.60 else ""
    assert line.split(" | ")[0] == (f"🌐 All ML-модель: {got['side']} "
                                     f"{50 + abs(expected):.1f}%{star}")
    assert "пул:" not in line
    assert abs(expected - original) > 0.05


def test_toggle_unknown_identity_and_missing_artifact(boundary, monkeypatch, tmp_path,
                                                      caplog):
    r, d = _teams(boundary, 1)  # captured pos2 off-pool vs experienced opponent
    counts = {(int(row[0]), int(row[1])): int(row[2])
              for row in boundary["acc_hero"]}
    assert counts.get((r["pos2"]["account_id"], r["pos2"]["hero_id"]), 0) <= 1
    assert counts.get((d["pos2"]["account_id"], d["pos2"]["hero_id"]), 0) >= 20
    original = draft.win_index_draft(r, d)
    expected = _expected_index(original, r, d, boundary["acc_hero"])
    assert expected != original
    for off in ("0", "false", "off"):
        monkeypatch.setenv("ALL_HERO_POOL_ENABLED", off)
        got = serving.verdicts(r, d, int(boundary["ts"][1]), draft_model=draft)["all"]
        assert got == {"side": "Radiant" if original >= 0 else "Dire",
                       "confidence": (50 + abs(original)) / 100.0}
    monkeypatch.setenv("ALL_HERO_POOL_ENABLED", "1")
    r_missing = {key: value.copy() for key, value in r.items()}
    r_missing["pos2"]["account_id"] = 0
    partial = _expected_index(original, r_missing, d, boundary["acc_hero"])
    assert partial != expected
    got = serving.verdicts(r_missing, d, int(boundary["ts"][1]),
                           draft_model=draft)["all"]
    assert got["confidence"] == (50 + abs(partial)) / 100.0
    assert got["side"] == ("Radiant" if partial >= 0 else "Dire")
    d_missing = {key: value.copy() for key, value in d.items()}
    d_missing["pos2"]["account_id"] = 0
    partial = _expected_index(original, r, d_missing, boundary["acc_hero"])
    got = serving.verdicts(r, d_missing, int(boundary["ts"][1]),
                           draft_model=draft)["all"]
    assert got["confidence"] == (50 + abs(partial)) / 100.0
    assert got["side"] == ("Radiant" if partial >= 0 else "Dire")
    monkeypatch.setenv("PREMATCH_ARTIFACT", str(tmp_path / "missing.npz"))
    for _ in range(2):
        got = serving.verdicts(r, d, int(boundary["ts"][1]), draft_model=draft)["all"]
        assert got["confidence"] == (50 + abs(original)) / 100.0
        assert got["side"] == ("Radiant" if original >= 0 else "Dire")
    assert sum("All hero-pool artifact unavailable" in record.message
               for record in caplog.records) == 1


def test_replaced_artifact_is_reloaded_after_stat_interval(boundary, monkeypatch, tmp_path):
    r, d = _teams(boundary, 1)
    original = draft.win_index_draft(r, d)
    path = tmp_path / "prematch.npz"
    rows = boundary["acc_hero"]
    np.savez_compressed(path, acc_hero=rows)
    monkeypatch.setenv("PREMATCH_ARTIFACT", str(path))
    monkeypatch.setattr(pool, "_STAT_INTERVAL", 0.0)
    before = pool.correct_index(original, r, d)
    changed = rows.copy()
    key = ((changed[:, 0] == r["pos2"]["account_id"])
           & (changed[:, 1] == r["pos2"]["hero_id"]))
    if key.any():
        changed[key, 2] += 100
    else:
        changed = np.vstack((changed, [r["pos2"]["account_id"],
                                       r["pos2"]["hero_id"], 100, 0, 0]))
    replacement = tmp_path / "replacement.npz"
    np.savez_compressed(replacement, acc_hero=changed)
    os.replace(replacement, path)
    assert pool.correct_index(original, r, d) == _expected_index(original, r, d, changed)
    assert pool.correct_index(original, r, d) != before
    # The account still has other captured heroes, but a missing pair means
    # zero prior games on this hero (a debut), not an unknown identity.
    debut = changed[~((changed[:, 0] == r["pos2"]["account_id"])
                      & (changed[:, 1] == r["pos2"]["hero_id"]))]
    assert np.any(debut[:, 0] == r["pos2"]["account_id"])
    with open(replacement, "wb") as output:
        np.savez_compressed(output, acc_hero=debut)
    os.replace(replacement, path)
    assert pool.correct_index(original, r, d) == _expected_index(original, r, d, debut)
