#!/usr/bin/env python3
"""Append pub-hero prior block (U) to the kills v3 dataset: od3 -> od3u.

Offline research only. Reads ``data/kills_v3_20260923/od3/{dataset.npz,
metadata.json}`` and writes ``data/kills_v3_20260923/od3u/`` with X extended
from (N,2,171) to (N,2,180) by 9 ``pub_*`` columns. All other arrays are
copied unchanged (npz ``feature_names`` is extended to 180 to stay consistent
with X; every other array is byte-identical).

U columns come from :mod:`base.public_kills_block` ``block(heroes10)`` (9
probabilities from ``ml-models/public_kills``), computed in BATCH with the
same loaded encoders/models instead of one call per map.

Orientation rule (documented choice): X has an orientation axis where
orientation 0 = radiant (own=radiant) and orientation 1 = dire (own=dire),
and each orientation slice already stores own heroes first. U is computed
per orientation slice as ``block(own5 + opp5 as stored)``. Hence for
orientation 0 that is ``block(radiant5 + dire5)`` (radiant perspective, values
as returned), and for orientation 1 it is ``block(dire5 + radiant5)``
(own=dire perspective). In particular the side-specific outputs
``publogit_team27_rad`` / ``publogit_team27_dire`` are NOT swapped back for
orientation 1: they are whatever ``block()`` returns for the dire-first
input. This keeps U a pure function of the stored slice, matching how the
live panel calls ``block()`` on the current side order.

Maps with any missing/invalid hero id (non-finite, non-integer, or < 1) get
NaN for all 9 U columns in that orientation slice.

Look-ahead note: the pub artifacts were trained 2026-03-24..2026-08-09, so
U values for rows before 2026-08-09 carry look-ahead; recorded verbatim in
metadata ``pub_block``.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT / "base") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "base"))

import public_kills_block as pkb  # noqa: E402

IN_NPZ = PROJECT_ROOT / "data/kills_v3_20260923/od3/dataset.npz"
IN_META = PROJECT_ROOT / "data/kills_v3_20260923/od3/metadata.json"
OUT_DIR = PROJECT_ROOT / "data/kills_v3_20260923/od3u"

U_NAMES = ["pub_" + c for c in pkb.COLUMNS]

PUB_TRAIN_PERIOD = "2026-03-24..2026-08-09 (look-ahead for rows before 2026-08-09)"
ORIENTATION_RULE = (
    "orientation 0 (own=radiant): U = block(radiant5 + dire5), values as returned; "
    "orientation 1 (own=dire): U = block(dire5 + radiant5), i.e. block() on the "
    "side-swapped heroes with no output swapping (team27_rad/dire keep block() semantics)."
)

HERO_COLS = [f"hero_own_{i}" for i in range(5)] + [f"hero_opp_{i}" for i in range(5)]

CHUNK = 20_000


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def artifact_files() -> list[Path]:
    files = [pkb.MODEL_DIR / f"encoder_{k}.joblib"
             for k in ("signed", "unsigned", "team_signed", "team_unsigned")]
    files += [pkb.MODEL_DIR / f"{k}.joblib" for k, _ in pkb.SINGLE]
    files += [pkb.MODEL_DIR / "team27.joblib"]
    return files


def valid_mask(H: np.ndarray) -> np.ndarray:
    """(n,10) float heroes -> bool mask. Valid = finite, integer-valued, >= 1."""
    finite = np.isfinite(H)
    integral = np.zeros_like(finite)
    integral[finite] = (H[finite] == np.floor(H[finite]))
    ge1 = np.zeros_like(finite)
    ge1[finite] = (H[finite] >= 1)
    return (finite & integral & ge1).all(axis=1)


def compute_u_float64(H: np.ndarray) -> np.ndarray:
    """Batch U for (M,10) int/float heroes in own-first order. Returns (M,9) float64.

    Uses the encoders/models already loaded by public_kills_block; replicates
    block() exactly (same transforms, same models, same side-swap for team27_dire).
    """
    import scipy.sparse as sp

    st = pkb._load()
    enc, mdl = st["enc"], st["mdl"]
    if enc is None or mdl is None:
        raise RuntimeError(f"pub artifacts not loaded: {st.get('error')}")
    Hi = np.asarray(H, dtype=np.int64)
    M = Hi.shape[0]
    out = np.empty((M, len(pkb.COLUMNS)), dtype=np.float64)
    col_index = {col: j for j, col in enumerate(pkb.COLUMNS)}
    for key, col in pkb.SINGLE:
        spec = mdl[key]
        e = enc["signed"] if spec["signed"] else enc["unsigned"]
        for s in range(0, M, CHUNK):
            Xs = e.transform(Hi[s:s + CHUNK])
            out[s:s + CHUNK, col_index[col]] = spec["model"].predict_proba(Xs)[:, 1]
    team = mdl["team27"]["model"]

    def team_p_batch(Xh: np.ndarray, col: str) -> None:
        for s in range(0, M, CHUNK):
            X = sp.hstack([enc["team_signed"].transform(Xh[s:s + CHUNK]),
                           enc["team_unsigned"].transform(Xh[s:s + CHUNK])],
                          format="csr")
            out[s:s + CHUNK, col_index[col]] = team.predict_proba(X)[:, 1]

    team_p_batch(Hi, "publogit_team27_rad")
    team_p_batch(np.concatenate([Hi[:, 5:], Hi[:, :5]], axis=1),
                 "publogit_team27_dire")
    return out


def check_vs_block(H: np.ndarray, U: np.ndarray, n_sample: int = 200) -> float:
    """Max |batch - block()| over a strided sample of rows. Raises on mismatch."""
    idx = np.linspace(0, H.shape[0] - 1, num=min(n_sample, H.shape[0])).astype(int)
    worst = 0.0
    for i in idx:
        ref = pkb.block([int(v) for v in H[i]])
        assert ref is not None
        got = np.array([ref[c] for c in pkb.COLUMNS], dtype=np.float64)
        worst = max(worst, float(np.max(np.abs(got - U[i]))))
    if not worst < 1e-9:
        raise AssertionError(f"batch vs block() max|diff|={worst:.3e} >= 1e-9")
    return worst


def atomic_write_bytes(path: Path, data: bytes) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, path)


def build(out_dir: Path = OUT_DIR, n_check: int = 200,
          in_npz: Path = IN_NPZ, in_meta: Path = IN_META) -> dict:
    st = pkb._load()
    if st.get("enc") is None or st.get("mdl") is None:
        raise RuntimeError(f"pub artifacts not loaded: {st.get('error')}")
    data = np.load(str(in_npz))
    arrays = {k: data[k] for k in data.files}
    X = arrays["X"]
    fn = [str(v) for v in list(arrays["feature_names"])]
    hidx = [fn.index(c) for c in HERO_COLS]
    N = X.shape[0]
    H = np.stack([X[:, o, :][:, hidx] for o in range(X.shape[1])], axis=1)
    Hf = H.reshape(-1, 10)
    ok = valid_mask(Hf)
    U = np.full((Hf.shape[0], len(pkb.COLUMNS)), np.nan, dtype=np.float64)
    if ok.any():
        U[ok] = compute_u_float64(Hf[ok])
        worst = check_vs_block(Hf[ok], U[ok], n_sample=max(n_check, 200))
    else:
        worst = 0.0
    U = U.reshape(N, X.shape[1], len(pkb.COLUMNS))
    Xnew = np.concatenate([X, U.astype(np.float32)], axis=2)
    arrays["X"] = Xnew
    arrays["feature_names"] = np.array(fn + U_NAMES, dtype="<U51")

    out_dir.mkdir(parents=True, exist_ok=True)
    import io
    buf = io.BytesIO()
    np.savez_compressed(buf, **arrays)
    atomic_write_bytes(out_dir / "dataset.npz", buf.getvalue())

    meta = json.loads(Path(in_meta).read_text())
    meta["feature_names"] = meta["feature_names"] + U_NAMES
    meta["blocks"]["H"] = list(meta["blocks"]["H"]) + U_NAMES
    split = arrays["split"]
    names = {0: "train", 1: "valid", 2: "test", -1: "other"}
    nan_share: dict[str, dict[str, float]] = {}
    for j, name in enumerate(U_NAMES):
        col = Xnew[:, :, X.shape[2] + j]
        per: dict[str, float] = {}
        for code, sname in names.items():
            m = split == code
            if m.any():
                per[sname] = float(np.isnan(col[m]).mean())
        per["all"] = float(np.isnan(col).mean())
        nan_share[name] = per
    meta["pub_block"] = {
        "artifact_dir": str(pkb.MODEL_DIR.relative_to(PROJECT_ROOT)),
        "sha256": {p.name: sha256_of(p) for p in artifact_files()},
        "pub_training_period": PUB_TRAIN_PERIOD,
        "orientation_rule": ORIENTATION_RULE,
        "nan_share": nan_share,
        "batch_vs_block_max_abs_diff": worst,
        "source": "od3",
    }
    payload = json.dumps(meta, indent=2).encode()
    atomic_write_bytes(out_dir / "metadata.json", payload)
    return {"n": N, "worst": worst, "nan_share": nan_share,
            "out": str(out_dir / "dataset.npz")}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=OUT_DIR)
    ap.add_argument("--n-check", type=int, default=200)
    args = ap.parse_args()
    import time
    t0 = time.time()
    info = build(args.out, n_check=args.n_check)
    print(f"wrote {info['out']} N={info['n']} worst={info['worst']:.3e} "
          f"wall={time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
