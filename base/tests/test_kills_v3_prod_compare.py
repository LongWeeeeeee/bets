"""Synthetic tests for base/tools/kills_v3_prod_compare.py (offline only)."""
import json
import shutil
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

TOOL = Path(__file__).resolve().parents[1] / "tools" / "kills_v3_prod_compare.py"
TARGETS = ("side30", "total55", "lead_5_15", "lead_10_20", "lead_15_25", "lead_20_30")
KEYS = {"side30": "rad_30_25", "total55": "total_55_50", "lead_5_15": "w_5_15",
        "lead_10_20": "w_10_20", "lead_15_25": "w_15_25", "lead_20_30": "w_20_30"}
SINCE = 1789171200
NMID = 1000

OWN = [30, 28, 25, 35, 27, 31, 24, 33, 29, 26, 32, 22]
TOT = [55, 52, 48, 60, 53, 57, 50, 58, 54, 51, 59, 49]
L515 = [i + 1 for i in range(12)]                      # all radiant leads
L1020 = [(-1) ** i * (i + 1) for i in range(12)]       # mixed
L1525 = [(-1) ** (i + 1) * (i + 2) for i in range(12)]  # mixed
L2030 = [(-1) ** i * (i + 3) for i in range(12)]        # mixed
V3_CONST = {"side30": 0.6, "total55": 0.6, "lead_5_15": 0.85,
            "lead_10_20": 0.6, "lead_15_25": 0.6, "lead_20_30": 0.6}

_model_src = None


def _model_file(workdir):
    global _model_src
    if _model_src is None:
        from catboost import CatBoostClassifier
        rng = np.random.default_rng(0)
        Xtr = rng.normal(size=(24, 3))
        ytr = (Xtr[:, 0] > 0).astype(int)
        m = CatBoostClassifier(iterations=5, depth=2, learning_rate=0.2,
                               random_seed=0, verbose=False,
                               allow_writing_files=False, thread_count=1)
        m.fit(Xtr, ytr)
        _model_src = Path(workdir) / "seed_model.cbm"
        m.save_model(str(_model_src))
    return _model_src


def _panel_p(target, i, row):
    y1 = {"side30": OWN[i] >= 30, "total55": TOT[i] >= 55,
          "lead_5_15": L515[i] > 0, "lead_10_20": L1020[i] > 0,
          "lead_15_25": L1525[i] > 0, "lead_20_30": L2030[i] > 0}[target]
    if row == 1:
        if target == "lead_5_15":
            return 0.15  # confidently wrong on all-positive labels
        return 0.85 if y1 else 0.15  # confidently right
    if target == "lead_5_15":
        return 0.16
    return 0.5


def make_env(root, extra_rows=()):
    rng = np.random.default_rng(1)
    n = 13
    starts = np.array([SINCE + (i % 3) * 86400 + i * 100 for i in range(n)])
    ends = starts + 3600
    mids = np.array([NMID + i for i in range(n)])
    y = np.zeros((n, 2, 8))
    for i in range(12):
        y[i, 0] = [OWN[i], TOT[i] - OWN[i], TOT[i], L515[i], L1020[i],
                   L1525[i], L2030[i], 2400]
        y[i, 1] = [TOT[i] - OWN[i], OWN[i], TOT[i], -L515[i], -L1020[i],
                   -L1525[i], -L2030[i], 2400]
    y[12] = [30, 25, 55, 2, -2, 3, -3, 2400]
    X = rng.normal(size=(n, 2, 3))
    split = np.array([2] * 12 + [0])
    ds = root / "ds"
    ds.mkdir()
    np.savez_compressed(ds / "dataset.npz", X=X, y=y, mids=mids, starts=starts,
                        ends=ends, series_ids=np.zeros(n, dtype=np.int64),
                        split=split, team_ids=np.zeros((n, 2), dtype=np.int64))
    (ds / "metadata.json").write_text(json.dumps(
        {"feature_names": ["g_a", "g_b", "g_c"],
         "blocks": {"G": ["g_a", "g_b", "g_c"], "H": [], "T": [], "P": []}}))
    out = root / "harness"
    sel = {"targets": {t: [{"id": f"select__{t}__depth4"}] for t in TARGETS}}
    (out / "selection.json").parent.mkdir(parents=True, exist_ok=True)
    (out / "selection.json").write_text(json.dumps(sel))
    (out / "test_started.json").write_text(json.dumps({"started": 1}))
    src = _model_file(str(root))
    for t in TARGETS:
        cdir = out / "candidates" / f"select__{t}__depth4"
        cdir.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, cdir / "model.cbm")
        (cdir / "record.json").write_text(json.dumps(
            {"id": f"select__{t}__depth4", "target": t, "block": "G",
             "kind": "binary", "calibration": {"platt": {"constant": V3_CONST[t]}}}))
    rows = []
    for i in range(12):
        ts1 = int(ends[i] + 10) if i == 0 else int(starts[i] + 50)  # map0: row1 past end
        rows.append({"map_id": str(NMID + i), "ts": ts1,
                     "models": [{"key": KEYS[t], "p": _panel_p(t, i, 1)} for t in TARGETS]})
        if i != 1:  # map1 keeps only its (excluded) row1
            rows.append({"map_id": str(NMID + i), "ts": int(starts[i] + 500),
                         "models": [{"key": KEYS[t], "p": _panel_p(t, i, 2)}
                                    for t in TARGETS]})
    rows[2]["ts"] = SINCE - 1  # map1 row1 is before since -> map1 has no panel row
    rows.append({"map_id": str(NMID + 2), "ts": int(starts[2] + 50),  # exact duplicate: legal
                 "models": [{"key": KEYS[t], "p": _panel_p(t, 2, 1)} for t in TARGETS]})
    rows.append({"map_id": "999999", "ts": int(starts[0] + 50),  # unknown map
                 "models": [{"key": "w_5_15", "p": 0.5}]})
    rows.append({"map_id": str(NMID + 12), "ts": int(starts[12] + 50),  # split 0 map
                 "models": [{"key": "w_5_15", "p": 0.5}]})
    rows.extend(extra_rows)
    jpath = root / "journal.jsonl"
    jpath.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    return {"journal": jpath, "out": out, "dataset": ds / "dataset.npz",
            "starts": starts, "ends": ends}


def run_tool(env, tmp_path, *extra):
    odir = tmp_path / "res"
    cmd = [sys.executable, str(TOOL), "--journal", str(env["journal"]), "--out",
           str(env["out"]), "--dataset", str(env["dataset"]), "--output-dir",
           str(odir), "--since", str(SINCE), "--reps", "100", *extra]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=300), odir


def test_refusal_without_lock(tmp_path):
    env = make_env(tmp_path)
    (env["out"] / "test_started.json").unlink()
    proc, _ = run_tool(env, tmp_path)
    assert proc.returncode != 0
    (env["out"] / "selection.json").unlink()
    (env["out"] / "test_started.json").write_text("{}")
    proc, _ = run_tool(env, tmp_path)
    assert proc.returncode != 0


def test_help():
    proc = subprocess.run([sys.executable, str(TOOL), "--help"],
                          capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0 and "--journal" in proc.stdout


def test_earliest_row_and_end_exclusion(tmp_path):
    env = make_env(tmp_path)
    proc, odir = run_tool(env, tmp_path)
    assert proc.returncode == 0, proc.stderr
    z = np.load(odir / "prod_compare_rows.npz", allow_pickle=False)
    ti = list(TARGETS).index("lead_10_20")
    sel = z["target"] == ti
    by_mid = {int(m): float(p) for m, p in zip(z["mid"][sel], z["p_panel"][sel])}
    assert int(NMID + 1) not in by_mid  # ts < since -> no panel row
    assert by_mid[NMID + 0] == pytest.approx(_panel_p("lead_10_20", 0, 2))  # row1 past end
    assert by_mid[NMID + 2] == pytest.approx(_panel_p("lead_10_20", 2, 1))  # earliest wins
    rep = json.loads((odir / "prod_compare.json").read_text())
    assert rep["exclusions"].get("row_ts_at_or_after_end", 0) >= 1
    assert rep["exclusions"].get("row_ts_before_since", 0) >= 1
    assert rep["exclusions"].get("row_map_unknown", 0) >= 1
    assert rep["exclusions"].get("row_map_not_test", 0) >= 1


def test_excluded_middle_subset(tmp_path):
    env = make_env(tmp_path)
    proc, odir = run_tool(env, tmp_path)
    assert proc.returncode == 0, proc.stderr
    rep = json.loads((odir / "prod_compare.json").read_text())
    assert rep["targets"]["total55"]["subsets"]["all"]["models"]["panel"]["n_maps"] == 11
    assert rep["targets"]["total55"]["subsets"]["cohort"]["models"]["panel"]["n_maps"] == 8
    assert rep["targets"]["side30"]["subsets"]["all"]["models"]["panel"]["n_maps"] == 11
    assert rep["targets"]["side30"]["subsets"]["cohort"]["models"]["panel"]["n_maps"] == 8


def test_orientation_side0(tmp_path):
    env = make_env(tmp_path)
    proc, odir = run_tool(env, tmp_path)
    assert proc.returncode == 0, proc.stderr
    rep = json.loads((odir / "prod_compare.json").read_text())
    ll = rep["targets"]["lead_10_20"]["subsets"]["all"]["models"]["panel"]["ll"]
    assert ll < 0.35  # panel is confidently right on side-0 labels; side-1 would give ~1.9
    z = np.load(odir / "prod_compare_rows.npz", allow_pickle=False)
    ti = list(TARGETS).index("lead_10_20")
    sel = z["target"] == ti
    expect = {(NMID + i): int(L1020[i] > 0) for i in range(12) if i != 1}
    assert {int(m): int(v) for m, v in zip(z["mid"][sel], z["y"][sel])} == expect


def test_bootstrap_delta_sign_and_frozen(tmp_path):
    env = make_env(tmp_path)
    fdir = tmp_path / "fz"
    fdir.mkdir()
    fz = fdir / "dataset.npz"
    shutil.copyfile(env["dataset"], fz)
    shutil.copyfile(Path(env["dataset"]).with_name("metadata.json"),
                    fdir / "metadata.json")
    proc, odir = run_tool(env, tmp_path, "--frozen-dataset", str(fz))
    assert proc.returncode == 0, proc.stderr
    rep = json.loads((odir / "prod_compare.json").read_text())
    boot = rep["targets"]["lead_5_15"]["subsets"]["all"]["bootstrap"]["v3_online_minus_panel"]
    assert boot["delta_ll_mean"] < -1.0 and boot["delta_ll_ci95"][1] < 0
    z = np.load(odir / "prod_compare_rows.npz", allow_pickle=False)
    assert np.allclose(z["p_v3_frozen"], z["p_v3_online"])
    assert rep["targets"]["lead_5_15"]["subsets"]["all"]["models"]["v3_frozen"]["n_maps"] == 11


def test_conflict(tmp_path):
    env = make_env(tmp_path)
    with open(env["journal"], "a") as f:
        f.write(json.dumps({"map_id": str(NMID + 3), "ts": int(env["starts"][3] + 50),
                            "models": [{"key": "w_5_15", "p": 0.99}]}) + "\n")
    proc, _ = run_tool(env, tmp_path)
    assert proc.returncode != 0


def test_frozen_label_check_accepts_identical_nan_labels():
    """Censored window labels are NaN; the frozen/online label equality must treat NaN == NaN."""
    a = np.array([30.0, 25.0, 55.0, np.nan, 2.0, np.nan, -1.0, 1800.0])
    assert not np.array_equal(a, a.copy())  # the pre-fix comparison rejects identical rows
    assert np.array_equal(a, a.copy(), equal_nan=True)
    assert 'np.array_equal(np.asarray(frozen["y"])[fp], y[pos], equal_nan=True)' in TOOL.read_text()
