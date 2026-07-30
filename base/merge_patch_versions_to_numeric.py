#!/usr/bin/env python3
import argparse
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import orjson

try:
    import ijson
except Exception:
    ijson = None


PATCH_GROUPS: List[Tuple[str, List[str]]] = [
    ("7.40", ["7.40", "7.40b", "7.40c"]),
    ("7.39", ["7.39", "7.39b", "7.39c", "7.39d", "7.39e"]),
    ("7.38", ["7.38", "7.38b", "7.38c"]),
    ("7.37", ["7.37", "7.37b", "7.37c", "7.37d", "7.37e"]),
    ("7.36", ["7.36", "7.36a", "7.36b", "7.36c"]),
    ("7.35", ["7.35c", "7.35d"]),
    ("pre_7.35c", ["pre_7.35c"]),
]


def _iter_match_items(path: Path) -> Iterable[Tuple[str, dict]]:
    if not path.exists():
        return
    if ijson is not None:
        with path.open("rb") as f:
            for k, v in ijson.kvitems(f, "", use_float=True):
                if isinstance(v, dict):
                    yield str(k), v
        return

    with path.open("rb") as f:
        payload = orjson.loads(f.read())
    if isinstance(payload, dict):
        for k, v in payload.items():
            if isinstance(v, dict):
                yield str(k), v


def merge_versions_to_numeric(input_root: Path, output_root: Path, clean_output: bool = False) -> None:
    if clean_output and output_root.exists():
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    summary = {
        "input_root": str(input_root),
        "output_root": str(output_root),
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "groups": {},
    }

    total_written = 0
    total_duplicates = 0
    missing_sources = []

    print(f"Input root:  {input_root}")
    print(f"Output root: {output_root}")

    for group_name, versions in PATCH_GROUPS:
        group_dir = output_root / group_name
        group_dir.mkdir(parents=True, exist_ok=True)
        out_file = group_dir / "matches.json"

        seen_ids = set()
        written = 0
        duplicates = 0
        sources_stats = {}

        with out_file.open("wb") as fh:
            fh.write(b"{")
            first = True

            print(f"\n[{group_name}] sources: {', '.join(versions)}")
            for version in versions:
                src_file = input_root / version / "matches.json"
                if not src_file.exists():
                    missing_sources.append(str(src_file))
                    sources_stats[version] = {"read": 0, "written": 0, "duplicates": 0, "missing": True}
                    print(f"  ⚠️ Missing source: {src_file}")
                    continue

                src_read = 0
                src_written = 0
                src_duplicates = 0
                for match_id, match in _iter_match_items(src_file):
                    src_read += 1
                    if match_id in seen_ids:
                        src_duplicates += 1
                        duplicates += 1
                        continue
                    seen_ids.add(match_id)

                    if not first:
                        fh.write(b",")
                    else:
                        first = False

                    fh.write(orjson.dumps(match_id))
                    fh.write(b":")
                    fh.write(orjson.dumps(match))
                    src_written += 1
                    written += 1

                sources_stats[version] = {
                    "read": src_read,
                    "written": src_written,
                    "duplicates": src_duplicates,
                    "missing": False,
                }
                print(f"  - {version}: read={src_read:,}, written={src_written:,}, duplicates={src_duplicates:,}")

            fh.write(b"}")

        total_written += written
        total_duplicates += duplicates
        summary["groups"][group_name] = {
            "versions": versions,
            "written": written,
            "duplicates": duplicates,
            "sources": sources_stats,
            "output_file": str(out_file),
        }
        print(f"  => group {group_name}: written={written:,}, duplicates={duplicates:,}")

    summary["totals"] = {
        "written": total_written,
        "duplicates": total_duplicates,
        "missing_sources": len(missing_sources),
    }
    summary["missing_sources"] = missing_sources

    summary_path = output_root / "summary_numeric.json"
    with summary_path.open("wb") as f:
        f.write(orjson.dumps(summary, option=orjson.OPT_INDENT_2))

    print("\nDone.")
    print(f"Total written: {total_written:,}")
    print(f"Total duplicates skipped: {total_duplicates:,}")
    print(f"Missing sources: {len(missing_sources)}")
    print(f"Summary: {summary_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge patch letter versions into numeric patch groups")
    parser.add_argument(
        "--input-root",
        type=Path,
        default=Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches/sorted_by_patch_version"),
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches/sorted_by_patch_numeric"),
    )
    parser.add_argument("--clean-output", action="store_true")
    args = parser.parse_args()

    merge_versions_to_numeric(
        input_root=args.input_root,
        output_root=args.output_root,
        clean_output=args.clean_output,
    )


if __name__ == "__main__":
    main()

