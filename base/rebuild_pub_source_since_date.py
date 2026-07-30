from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

try:
    import ijson
except Exception:
    ijson = None


DEFAULT_SOURCE_DIR = Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches/json_parts_split_from_object")
DEFAULT_OUTPUT_DIR = Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches/json_parts_split_from_object_since_2025_12_15")
DEFAULT_MIN_DATE = "2025-12-15"
DEFAULT_MATCHES_PER_FILE = 50_000


def _parse_min_timestamp(date_text: str) -> int:
    dt = datetime.strptime(date_text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _coerce_start_ts(match: dict[str, Any]) -> int | None:
    raw = match.get("startDateTime")
    if isinstance(raw, str):
        raw = raw.strip()
        if raw.isdigit():
            raw = int(raw)
    if isinstance(raw, (int, float)):
        return int(raw)
    return None


def _iter_matches_from_file(path: Path) -> Iterator[tuple[str, dict[str, Any]]]:
    if ijson is not None:
        with path.open("rb") as fh:
            for match_id, match_data in ijson.kvitems(fh, "", use_float=True):
                if isinstance(match_data, dict):
                    yield str(match_id), match_data
        return

    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if isinstance(payload, dict):
        for match_id, match_data in payload.items():
            if isinstance(match_data, dict):
                yield str(match_id), match_data


def rebuild_pub_source_since_date(
    *,
    source_dir: Path,
    output_dir: Path,
    min_start_ts: int,
    matches_per_file: int,
) -> dict[str, Any]:
    if not source_dir.exists():
        raise FileNotFoundError(f"Source dir not found: {source_dir}")

    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    source_files = sorted(source_dir.glob("combined*.json"))
    if not source_files:
        raise RuntimeError(f"No combined*.json files found in {source_dir}")

    total_scanned = 0
    total_kept = 0
    kept_ids: list[int] = []
    current_chunk: dict[str, dict[str, Any]] = {}
    chunk_index = 1
    output_files: list[str] = []
    first_kept_ts: int | None = None
    last_kept_ts: int | None = None

    def flush_chunk() -> None:
        nonlocal chunk_index, current_chunk
        if not current_chunk:
            return
        target_path = output_dir / f"combined{chunk_index}.json"
        with target_path.open("w", encoding="utf-8") as fh:
            json.dump(current_chunk, fh, ensure_ascii=False)
        output_files.append(str(target_path))
        chunk_index += 1
        current_chunk = {}

    for file_index, source_path in enumerate(source_files, start=1):
        file_scanned = 0
        file_kept = 0
        for match_id, match_data in _iter_matches_from_file(source_path):
            total_scanned += 1
            file_scanned += 1
            start_ts = _coerce_start_ts(match_data)
            if start_ts is None or start_ts < min_start_ts:
                continue
            current_chunk[match_id] = match_data
            total_kept += 1
            file_kept += 1
            try:
                kept_ids.append(int(match_id))
            except ValueError:
                pass
            if first_kept_ts is None or start_ts < first_kept_ts:
                first_kept_ts = start_ts
            if last_kept_ts is None or start_ts > last_kept_ts:
                last_kept_ts = start_ts
            if len(current_chunk) >= matches_per_file:
                flush_chunk()
        print(
            f"[{file_index}/{len(source_files)}] {source_path.name}: "
            f"scanned={file_scanned} kept={file_kept} total_kept={total_kept}"
        )

    flush_chunk()

    processed_ids_path = output_dir / "processed_ids.txt"
    with processed_ids_path.open("w", encoding="utf-8") as fh:
        json.dump(sorted(kept_ids), fh, ensure_ascii=False)

    summary = {
        "source_dir": str(source_dir),
        "output_dir": str(output_dir),
        "min_start_ts": int(min_start_ts),
        "min_start_utc": datetime.fromtimestamp(min_start_ts, tz=timezone.utc).isoformat(),
        "source_files": len(source_files),
        "scanned_matches": total_scanned,
        "kept_matches": total_kept,
        "output_files": len(output_files),
        "matches_per_file": int(matches_per_file),
        "first_kept_utc": (
            datetime.fromtimestamp(first_kept_ts, tz=timezone.utc).isoformat()
            if first_kept_ts is not None
            else None
        ),
        "last_kept_utc": (
            datetime.fromtimestamp(last_kept_ts, tz=timezone.utc).isoformat()
            if last_kept_ts is not None
            else None
        ),
    }
    with (output_dir / "summary.json").open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild a filtered public-match source dir from combined*.json.")
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--min-date", type=str, default=DEFAULT_MIN_DATE, help="UTC date in YYYY-MM-DD")
    parser.add_argument("--matches-per-file", type=int, default=DEFAULT_MATCHES_PER_FILE)
    args = parser.parse_args()

    min_start_ts = _parse_min_timestamp(args.min_date)
    summary = rebuild_pub_source_since_date(
        source_dir=args.source_dir,
        output_dir=args.output_dir,
        min_start_ts=min_start_ts,
        matches_per_file=max(1, int(args.matches_per_file)),
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
