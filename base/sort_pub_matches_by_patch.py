#!/usr/bin/env python3
import argparse
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
import re

import orjson

try:
    import ijson
except Exception:
    ijson = None

try:
    from base.dota_patch_calendar import PATCH_RELEASES as _CALENDAR_PATCH_RELEASES
except ModuleNotFoundError:  # Direct invocation: python base/sort_pub_matches_by_patch.py
    from dota_patch_calendar import PATCH_RELEASES as _CALENDAR_PATCH_RELEASES


@dataclass(frozen=True)
class PatchRelease:
    version: str
    release_ts: int
    release_date: str
    release_at_utc: str
    source_url: Optional[str]
    time_convention: str


OLDER_BUCKET = "pre_7.35c"

PATCH_RELEASES_RAW: List[Tuple[str, str]] = [
    (release.label, release.release_date) for release in _CALENDAR_PATCH_RELEASES
]
PATCH_RELEASES: List[PatchRelease] = [
    PatchRelease(
        version=release.label,
        release_ts=release.release_ts,
        release_date=release.release_date,
        release_at_utc=release.release_at_utc,
        source_url=release.source_url,
        time_convention=release.time_convention,
    )
    for release in _CALENDAR_PATCH_RELEASES
]


def _classify_patch(start_ts: int) -> str:
    for patch in PATCH_RELEASES:
        if start_ts >= patch.release_ts:
            return patch.version
    return OLDER_BUCKET


def _combined_sort_key(path: Path) -> int:
    m = re.search(r"combined(\d+)\.json$", path.name)
    return int(m.group(1)) if m else 10**9


def _iter_matches(file_path: Path) -> Iterable[Tuple[str, dict]]:
    if ijson is not None:
        with file_path.open("rb") as f:
            for key, value in ijson.kvitems(f, "", use_float=True):
                if isinstance(value, dict):
                    yield str(key), value
        return

    with file_path.open("rb") as f:
        data = orjson.loads(f.read())
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, dict):
                yield str(key), value


def _open_bucket_writers(out_root: Path) -> Dict[str, dict]:
    bucket_names = [p.version for p in PATCH_RELEASES] + [OLDER_BUCKET]
    writers: Dict[str, dict] = {}
    for bucket in bucket_names:
        bucket_dir = out_root / bucket
        bucket_dir.mkdir(parents=True, exist_ok=True)
        out_file = bucket_dir / "matches.json"
        fh = out_file.open("wb")
        fh.write(b"{")
        writers[bucket] = {"fh": fh, "first": True, "path": out_file}
    return writers


def _close_bucket_writers(writers: Dict[str, dict]) -> None:
    for item in writers.values():
        fh = item.get("fh")
        if fh is None:
            continue
        try:
            fh.write(b"}")
            fh.close()
        except Exception:
            pass


def sort_pub_matches_by_patch(input_dir: Path, out_root: Path, clean_output: bool = False) -> None:
    if clean_output and out_root.exists():
        shutil.rmtree(out_root)
    out_root.mkdir(parents=True, exist_ok=True)

    src_files = sorted(input_dir.glob("combined*.json"), key=_combined_sort_key)
    if not src_files:
        raise FileNotFoundError(f"No combined*.json files in {input_dir}")

    print(f"Input dir:  {input_dir}")
    print(f"Output dir: {out_root}")
    print(f"Files:      {len(src_files)}")
    print("Patch buckets:")
    for p in PATCH_RELEASES:
        print(f"  - {p.version}: >= {p.release_at_utc}")
    print(f"  - {OLDER_BUCKET}: older than {PATCH_RELEASES[-1].release_at_utc}")

    writers = _open_bucket_writers(out_root)
    counts: Dict[str, int] = {name: 0 for name in writers.keys()}
    skipped_no_time = 0
    file_parse_errors = 0
    total = 0

    try:
        for idx, file_path in enumerate(src_files, 1):
            file_total = 0
            before_total = total
            try:
                for match_id, match in _iter_matches(file_path):
                    file_total += 1
                    raw_ts = match.get("startDateTime")
                    try:
                        start_ts = int(raw_ts)
                    except Exception:
                        skipped_no_time += 1
                        continue

                    bucket = _classify_patch(start_ts)
                    writer = writers[bucket]
                    fh = writer["fh"]
                    if not writer["first"]:
                        fh.write(b",")
                    else:
                        writer["first"] = False

                    fh.write(orjson.dumps(match_id))
                    fh.write(b":")
                    fh.write(orjson.dumps(match))
                    counts[bucket] += 1
                    total += 1
            except Exception as e:
                file_parse_errors += 1
                print(f"  ⚠️ Parse error in {file_path.name}: {e}")
                continue

            added = total - before_total
            print(
                f"[{idx}/{len(src_files)}] {file_path.name}: "
                f"read={file_total:,}, written={added:,}, total_written={total:,}"
            )
    finally:
        _close_bucket_writers(writers)

    summary = {
        "input_dir": str(input_dir),
        "output_dir": str(out_root),
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_written_matches": total,
        "skipped_no_startDateTime": skipped_no_time,
        "file_parse_errors": file_parse_errors,
        "counts_by_patch": counts,
        "patch_boundaries_utc": {
            p.version: {
                "release_date": p.release_date,
                "release_at_utc": p.release_at_utc,
                "release_ts": p.release_ts,
                "source_url": p.source_url,
                "time_convention": p.time_convention,
            }
            for p in PATCH_RELEASES
        },
        "older_bucket": OLDER_BUCKET,
    }
    with (out_root / "summary.json").open("wb") as f:
        f.write(orjson.dumps(summary, option=orjson.OPT_INDENT_2))

    print("\nDone.")
    print(f"Total written: {total:,}")
    print(f"Skipped (no startDateTime): {skipped_no_time:,}")
    print(f"File parse errors: {file_parse_errors}")
    print("Counts by patch:")
    for bucket, cnt in sorted(counts.items(), key=lambda kv: kv[0], reverse=True):
        print(f"  {bucket}: {cnt:,}")
    print(f"Summary: {out_root / 'summary.json'}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Sort public matches into patch folders by startDateTime")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches/json_parts_split_from_object"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches/sorted_by_patch_version"),
    )
    parser.add_argument("--clean-output", action="store_true")
    args = parser.parse_args()

    sort_pub_matches_by_patch(
        input_dir=args.input_dir,
        out_root=args.output_dir,
        clean_output=args.clean_output,
    )


if __name__ == "__main__":
    main()
