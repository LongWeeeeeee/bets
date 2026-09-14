#!/usr/bin/env python3
"""Frozen dictionary / Lane ML residual study, using exact source AST functions.

No application imports, credentials, runtime startup, or network calls. The
source dependency closure is retained in the result for auditability.
"""
from __future__ import annotations

import argparse
import ast
import json
import os
import sqlite3
import sys
import symtable
from pathlib import Path

import numpy as np

from research_lane_wait import ROOT, LANE, RICH, unique_index, summary

DICT = "bets_data/analise_pub_matches/lane_dict_raw.sqlite3"
ENV = "runtime/artifacts/star-dispatch/lane_wait_20260914/prod_lane_environment.json"


def apply_frozen_lane_environment():
    settings = json.loads((ROOT / ENV).read_text())["selected_environment"]
    # Only this isolated worker process is changed. Clear incidental caller
    # overrides; capture the actual production settings in every result.
    for key in list(os.environ):
        if key.startswith("LANE_"):
            del os.environ[key]
    os.environ.update({k: v for k, v in settings.items() if k.startswith("LANE_")})
    return settings


def isolated_source(path, roots, supplied=None):
    tree = ast.parse(path.read_text())
    providers = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            providers[node.name] = node
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Name):
                    providers[target.id] = node
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                providers[alias.asname or alias.name.split(".")[0]] = node
    wanted, seen = list(roots), set()
    selected = set()
    while wanted:
        name = wanted.pop()
        if name in seen or (supplied and name in supplied):
            continue
        seen.add(name)
        node = providers.get(name)
        if node is None:
            continue
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            modules = [node.module] if isinstance(node, ast.ImportFrom) else [x.name for x in node.names]
            if any(x.split(".")[0] not in {"math", "os", "re", "logging", "itertools", "typing", "collections"} for x in modules):
                raise ValueError("Unexpected dependency in isolated lane source: " + repr(modules))
        selected.add(node)
        scopes = [symtable.symtable(ast.unparse(node), str(path), "exec")]
        while scopes:
            scope = scopes.pop()
            wanted.extend(s.get_name() for s in scope.get_symbols() if s.is_global() and s.is_referenced())
            scopes.extend(scope.get_children())
    body = [ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)]
    body += [node for node in tree.body if node in selected]
    ns = dict(supplied or {})
    exec(compile(ast.fix_missing_locations(ast.Module(body=body, type_ignores=[])), str(path), "exec"), ns)
    return ns, sorted(seen & providers.keys())


class ReadonlyStats:
    def __init__(self, path):
        self.conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro&immutable=1", uri=True)
        self.columns = [r[1] for r in self.conn.execute("PRAGMA table_info(stats)")]
        self.cache = {}

    def get_many(self, keys):
        keys = list(dict.fromkeys(keys))
        missing = [k for k in keys if k not in self.cache]
        for offset in range(0, len(missing), 900):
            chunk = missing[offset:offset + 900]
            self.cache.update(dict.fromkeys(chunk))
            sql = "SELECT * FROM stats WHERE key IN (" + ",".join("?" for _ in chunk) + ")"
            for row in self.conn.execute(sql, chunk):
                value = dict(zip(self.columns, row))
                self.cache[value.pop("key")] = value
        return {k: self.cache[k] for k in keys if self.cache.get(k) is not None}

    def get(self, key, default=None):
        return self.get_many([key]).get(key, default)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", type=Path, required=True)
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    settings = apply_frozen_lane_environment()
    funcs, names = isolated_source(ROOT / "base/functions.py", ["calculate_lanes", "structure_lane_dict"])
    scalar, snames = isolated_source(ROOT / "base/cyberscore_try.py", ["_lane_dict_adv_value"],
                                    {"_coerce_metric_value": lambda v: float(v) if v is not None else None})
    lane = np.load(ROOT / LANE)
    rich = np.load(ROOT / RICH)
    ri = unique_index(rich["mids"])
    ix = np.array([ri[int(mid)] for mid in lane["mid"]])
    heroes = rich["heroes"][ix]
    wins = rich["wins"][ix]
    backend = ReadonlyStats(ROOT / DICT)
    structured = funcs["structure_lane_dict"](backend)
    values, fixtures = [], []
    for i, row in enumerate(heroes):
        rad = {"pos" + str(p + 1): {"hero_id": int(row[p])} for p in range(5)}
        dire = {"pos" + str(p + 1): {"hero_id": int(row[5 + p])} for p in range(5)}
        top, bot, mid = funcs["calculate_lanes"](rad, dire, structured, core_support_side_lanes=True)
        v = scalar["_lane_dict_adv_value"](top, mid, bot)
        values.append(float(v) if v is not None else float("nan"))
        if len(fixtures) < 12:
            fixtures.append(dict(mid=int(lane["mid"][i]), heroes=row.tolist(), top=top, mid_lane=mid, bot=bot, value=v))
        if i % 5000 == 0:
            print("scored", i, "cached_keys", len(backend.cache), flush=True)
    backend.conn.close()
    value = np.array(values)
    neutral = (lane["p_radiant"] >= .47) & (lane["p_radiant"] <= .53) & (lane["p_dire"] >= .47) & (lane["p_dire"] <= .53)
    nohit = (lane["p_radiant"] < .60) & (lane["p_dire"] < .60)
    side = np.sign(value)
    h = lane["nw10"] * side > 0
    w = np.where(side > 0, wins == 1, wins == 0)
    p = np.where(side > 0, lane["p_radiant"], lane["p_dire"])
    out = dict(n=len(value), effective_environment=settings, source_functions=names, scalar_functions=snames, fixtures=fixtures, groups={})
    # Fixed requested cut: >=20, plus descriptive sensitivity cuts. Snapshot
    # build cutoff is unknown, so no causality claim or trained combiner.
    for name, pop in {"neutral_47_53": neutral, "no_lane_hit": nohit, "all": np.ones(len(value), dtype=bool)}.items():
        report = []
        for threshold in (3, 5, 10, 15, 20, 25):
            take = pop & np.isfinite(value) & (np.abs(value) >= threshold)
            r = summary(h, take, pop, win=w)
            r.update(threshold=threshold, predicted_lane_probability=float(p[take].mean()) if take.any() else None)
            r["lift_vs_ml_pp"] = 100 * (r["rate"] - r["predicted_lane_probability"]) if take.any() else None
            r["by_side"] = {str(s): summary(h, take & (side == s), pop & (side == s)) for s in (-1, 1)}
            r["by_period"] = {label: summary(h, take & mask, pop & mask) for label, mask in
                              (("before_july23", lane["ts"] < 1784768250), ("after_july23", lane["ts"] >= 1784768250))}
            report.append(r)
        out["groups"][name] = report
    np.savez_compressed(args.output_dir / "dictionary.npz", mid=lane["mid"], lane_adv_dict=value)
    (args.output_dir / "dictionary.json").write_text(json.dumps(out, indent=2, allow_nan=False) + "\n")


if __name__ == "__main__":
    main()
