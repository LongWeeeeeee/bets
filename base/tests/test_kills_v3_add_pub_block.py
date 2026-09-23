"""Fast tests for base/tools/kills_v3_add_pub_block.py (offline, synthetic)."""
import io
import json

import numpy as np
import pytest

from base.tools import kills_v3_add_pub_block as tool
from base import public_kills_block as pkb

RAD = [13, 25, 32, 63, 76]
DIRE = [5, 8, 39, 45, 98]


def test_batch_equals_single_block():
    H = np.array([RAD + DIRE, DIRE + RAD, [1, 2, 3, 4, 5, 6, 7, 9, 10, 11]],
                 dtype=np.float64)
    U = tool.compute_u_float64(H)
    assert U.shape == (3, 9)
    for i in range(3):
        ref = pkb.block([int(v) for v in H[i]])
        assert ref is not None
        want = np.array([ref[c] for c in pkb.COLUMNS])
        assert np.max(np.abs(want - U[i])) < 1e-9


def test_orientation1_equals_block_on_swapped():
    # orientation slice storing own=dire first -> block(dire + radiant)
    H_own_first = np.array([DIRE + RAD], dtype=np.float64)
    U = tool.compute_u_float64(H_own_first)
    ref = pkb.block(DIRE + RAD)
    assert ref is not None
    want = np.array([ref[c] for c in pkb.COLUMNS])
    assert np.max(np.abs(want - U[0])) < 1e-9
    # swapped input must route through block()'s side-swap path, i.e. equal the
    # direct block() call on the same order (no output re-mapping)
    ref_plain = pkb.block(RAD + DIRE)
    assert ref is not None and ref_plain is not None
    U_plain = tool.compute_u_float64(np.array([RAD + DIRE], dtype=np.float64))
    want_plain = np.array([ref_plain[c] for c in pkb.COLUMNS])
    assert np.max(np.abs(want_plain - U_plain[0])) < 1e-9


def test_missing_or_invalid_hero_masked():
    H = np.array([
        RAD + DIRE,
        [np.nan] + RAD[1:] + DIRE,   # missing
        [0] + RAD[1:] + DIRE,        # invalid (< 1)
        RAD + DIRE[:4] + [98.5],   # non-integer
    ], dtype=np.float64)
    mask = tool.valid_mask(H)
    assert mask.tolist() == [True, False, False, False]


def _tiny_input(tmp_path):
    rng = np.random.default_rng(0)
    N, F = 8, 171
    X = rng.normal(size=(N, 2, F)).astype(np.float32)
    fn = [f"f{i}" for i in range(F)]
    hidx = {}
    for k, c in enumerate(tool.HERO_COLS):
        fn[150 + k] = c
        hidx[c] = 150 + k
    for i in range(N):
        X[i, 0, [hidx[c] for c in tool.HERO_COLS]] = np.array(RAD + DIRE, np.float32)
        X[i, 1, [hidx[c] for c in tool.HERO_COLS]] = np.array(DIRE + RAD, np.float32)
    X[3, 0, hidx["hero_own_0"]] = np.nan  # one missing slice
    X[5, 1, hidx["hero_opp_4"]] = 0.0     # one invalid slice
    arrays = {
        "X": X, "y": rng.normal(size=(N, 2, 2)).astype(np.float32),
        "mids": np.arange(N), "orient": np.tile([0, 1], (N, 1)).astype(np.int8),
        "starts": np.arange(N), "ends": np.arange(N),
        "series_ids": np.arange(N), "team_ids": np.zeros((N, 2), np.int64),
        "split": np.zeros(N, np.int8), "source": np.zeros(N, np.int8),
        "feature_names": np.array(fn, dtype="<U51"),
        "label_names": np.array(["a", "b"], dtype="<U10"),
    }
    p_in = tmp_path / "in.npz"
    buf = io.BytesIO()
    np.savez_compressed(buf, **arrays)
    p_in.write_bytes(buf.getvalue())
    meta = {"feature_names": fn, "blocks": {"G": [], "T": [], "P": [],
                                            "H": list(tool.HERO_COLS)}}
    p_meta = tmp_path / "meta.json"
    p_meta.write_text(json.dumps(meta))
    return p_in, p_meta, arrays


def test_build_end_to_end_tiny(tmp_path):
    p_in, p_meta, src = _tiny_input(tmp_path)
    out = tmp_path / "od3u"
    info = tool.build(out, n_check=8, in_npz=p_in, in_meta=p_meta)
    assert info["worst"] < 1e-9
    got = np.load(str(out / "dataset.npz"))
    Xn = got["X"]
    assert Xn.shape == (8, 2, 180)
    # base prefix NaN-aware equal
    assert np.array_equal(np.nan_to_num(src["X"]), np.nan_to_num(Xn[:, :, :171]))
    # missing/invalid slices -> NaN across all 9
    assert np.isnan(Xn[3, 0, 171:]).all()
    assert np.isnan(Xn[5, 1, 171:]).all()
    assert np.isfinite(Xn[0, 0, 171:]).all()
    # orientation rule: o=1 slice equals block(dire+radiant)
    ref = pkb.block(DIRE + RAD)
    assert ref is not None
    want = np.array([ref[c] for c in pkb.COLUMNS], dtype=np.float32)
    assert np.max(np.abs(Xn[0, 1, 171:] - want)) < 1e-6
    # non-X arrays (except extended feature_names) byte-identical
    for k in src:
        if k in ("X", "feature_names"):
            continue
        assert got[k].tobytes() == np.ascontiguousarray(src[k]).tobytes(), k
    assert list(got["feature_names"][:171]) == list(src["feature_names"])
    assert list(got["feature_names"][171:]) == tool.U_NAMES
    meta = json.loads((out / "metadata.json").read_text())
    assert meta["feature_names"][171:] == tool.U_NAMES
    assert meta["blocks"]["H"][-9:] == tool.U_NAMES
    assert "pub_block" in meta and "sha256" in meta["pub_block"]
