#!/usr/bin/env python3
"""Собирает IDs как get_pros() и get_pubs() из maps_research, пишет два .txt файла."""
import os
import sys
import json
import warnings
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
from pathlib import Path

warnings.filterwarnings("ignore")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASE_DIR = PROJECT_ROOT / "base"
sys.path.insert(0, str(BASE_DIR))

ANALYSE_PUB_DIR = PROJECT_ROOT / "bets_data" / "analise_pub_matches"
PUBS_SOURCE_DIR = ANALYSE_PUB_DIR / "json_parts_split_from_object"
OUT_DIR = PROJECT_ROOT / "runtime"

try:
    import ijson
except ImportError:
    print("FATAL: ijson not installed", file=sys.stderr)
    sys.exit(1)


def collect_pros_ids():
    from id_to_names import tier_one_teams, tier_two_teams
    raw = list(tier_one_teams.values()) + list(tier_two_teams.values())
    ids = set()
    for i in raw:
        if isinstance(i, set):
            ids.update(i)
        else:
            ids.add(i)
    return ids


def _process_json_file(filepath):
    warnings.filterwarnings("ignore")
    ids = set()
    try:
        with open(filepath, "rb") as f:
            parser = ijson.kvitems(f, "", use_float=True)
            for _match_id, match_data in parser:
                players = match_data.get("players")
                if not players:
                    continue
                for player in players:
                    sa = player.get("steamAccount")
                    if sa and not sa.get("isAnonymous", True):
                        pid = sa.get("id")
                        if pid:
                            ids.add(pid)
    except Exception as e:
        print(f"WARN parse {filepath}: {e}", file=sys.stderr)
    return ids


def collect_pubs_ids():
    json_dir = os.getenv("PUBS_IDS_SOURCE_DIR", str(PUBS_SOURCE_DIR))
    if not os.path.isdir(json_dir):
        raise FileNotFoundError(f"PUBS ids source dir not found: {json_dir}")
    files = [
        os.path.join(json_dir, f)
        for f in os.listdir(json_dir)
        if f.startswith("combined") and f.endswith(".json")
    ]
    if not files:
        files = [
            os.path.join(json_dir, f)
            for f in os.listdir(json_dir)
            if f.endswith(".json") and f not in {"processed_ids.txt", "merge_patch_summary.json"}
        ]
    if not files:
        raise RuntimeError(f"No source json files found in PUBS ids source dir: {json_dir}")
    print(f"PUBS dir: {json_dir}", flush=True)
    print(f"PUBS files: {len(files)}", flush=True)
    num_workers = min(multiprocessing.cpu_count(), len(files))
    print(f"workers: {num_workers}", flush=True)
    ids = set()
    with ProcessPoolExecutor(max_workers=num_workers) as ex:
        for i, result in enumerate(ex.map(_process_json_file, files), 1):
            before = len(ids)
            ids.update(result)
            print(f"  [{i}/{len(files)}] +{len(result)} (total {len(ids)})", flush=True)
    return ids


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=== PROS ===", flush=True)
    pros = collect_pros_ids()
    pros_file = OUT_DIR / "pros_ids.txt"
    with open(pros_file, "w") as f:
        for pid in sorted(pros):
            f.write(f"{pid}\n")
    print(f"PROS ids: {len(pros)} -> {pros_file}", flush=True)

    print("=== PUBS ===", flush=True)
    pubs = collect_pubs_ids()
    pubs_file = OUT_DIR / "pubs_ids.txt"
    with open(pubs_file, "w") as f:
        for pid in sorted(pubs):
            f.write(f"{pid}\n")
    print(f"PUBS ids: {len(pubs)} -> {pubs_file}", flush=True)

    print("DONE", flush=True)


if __name__ == "__main__":
    main()
