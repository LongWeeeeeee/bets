#!/usr/bin/env python3
"""Retrospective residual check; chronological fitting cannot fix snapshot leakage."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from research_lane_wait import summary, unique_index


def sigmoid(x):
    return 1 / (1 + np.exp(-np.clip(x, -35, 35)))


def fit(x, y):
    beta = np.zeros(x.shape[1])
    penalty = np.eye(x.shape[1]) * 1e-4
    penalty[0, 0] = 0
    for _ in range(100):
        p = sigmoid(x @ beta)
        step = np.linalg.solve((x.T * (p * (1 - p))) @ x + penalty,
                               x.T @ (y - p) - penalty @ beta)
        beta += step
        if np.max(np.abs(step)) < 1e-8:
            return beta
    raise RuntimeError("Logistic fit did not converge")


def loss(y, p):
    p = np.clip(p, 1e-9, 1 - 1e-9)
    return -(y * np.log(p) + (1 - y) * np.log1p(-p))


def day_interval(values, days):
    unique, inv = np.unique(days, return_inverse=True)
    if len(unique) < 2:
        return [None, None]
    totals = np.bincount(inv, weights=values)
    counts = np.bincount(inv)
    ix = np.random.default_rng(20260914).integers(len(unique), size=(2000, len(unique)))
    return np.quantile(totals[ix].sum(axis=1) / counts[ix].sum(axis=1), [.025, .975]).tolist()


def analyze(d, dictionary):
    lookup = unique_index(dictionary["mid"])
    value = dictionary["lane_adv_dict"][[lookup[int(m)] for m in d["mid"]]]
    side = np.sign(value)
    neutral = ((d["p_radiant"] >= .47) & (d["p_radiant"] <= .53)
               & (d["p_dire"] >= .47) & (d["p_dire"] <= .53))
    groups = {"neutral_47_53": neutral,
              "no_lane_hit": (d["p_radiant"] < .60) & (d["p_dire"] < .60),
              "all": np.ones(len(value), dtype=bool)}
    total = d["p_radiant"] + d["p_dire"]
    if (not np.all(np.isfinite(total)) or np.any(total <= 0)
            or np.any(d["p_radiant"] < 0) or np.any(d["p_dire"] < 0)):
        raise ValueError("Malformed Lane ML probabilities")
    p = d["p_radiant"] / total
    logit = np.log(np.clip(p, 1e-9, 1 - 1e-9) / np.clip(1 - p, 1e-9, 1))
    base = np.column_stack([np.ones(len(p)), logit])
    augmented = np.column_stack([base, value / 20])
    y = (d["nw10"] > 0).astype(float)
    result = {"groups": {}, "method": "Unpenalized intercept; slopes ridge1e-4; chronological fit only; day-block paired CI conditional on fitted coefficients",
              "limitation": "Retrospective snapshots lack verified as-of provenance; no prospective efficacy claim."}
    for name, group in groups.items():
        valid = group & d["valid"] & np.isfinite(value)
        report = {"threshold20": {}}
        for split in ("discover", "confirm"):
            eligible = valid & d[split]
            take = eligible & (np.abs(value) >= 20)
            h = d["nw10"] * side > 0
            r = summary(h, take, eligible)
            r["day_ci95"] = day_interval(h[take], d["ts"][take] // 86400)
            report["threshold20"][split] = r
        # Binary calibration comparison excludes exact NW10 ties from BOTH arms.
        train = valid & d["discover"] & (d["nw10"] != 0)
        test = valid & d["confirm"] & (d["nw10"] != 0)
        b0, b1 = fit(base[train], y[train]), fit(augmented[train], y[train])
        p0, p1 = sigmoid(base[test] @ b0), sigmoid(augmented[test] @ b1)
        l0, l1 = loss(y[test], p0), loss(y[test], p1)
        report["calibration_comparison"] = {
            "train_n": int(train.sum()), "confirm_n": int(test.sum()),
            "base_coefficients": b0.tolist(), "augmented_coefficients": b1.tolist(),
            "raw_ml_logloss": float(loss(y[test], p[test]).mean()),
            "calibrated_ml_logloss": float(l0.mean()), "plus_dictionary_logloss": float(l1.mean()),
            "logloss_improvement": float((l0 - l1).mean()),
            "improvement_day_ci95": day_interval(l0 - l1, d["ts"][test] // 86400),
        }
        result["groups"][name] = report
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--paired", type=Path, required=True)
    ap.add_argument("--dictionary", type=Path, required=True)
    ap.add_argument("--output-dir", type=Path, required=True)
    args = ap.parse_args()
    result = analyze(np.load(args.paired), np.load(args.dictionary))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    tmp = args.output_dir / "residual_check.json.tmp"
    tmp.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    tmp.replace(args.output_dir / "residual_check.json")


if __name__ == "__main__":
    main()
