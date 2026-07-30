from __future__ import annotations

import gc
import json
import os
from pathlib import Path

try:
    from explore_database import DEFAULT_JSON_SUBDIR, PATCH_739_RELEASE_TS, _dump_to_file, _iter_matches
    from tempo_analise_database_experiment import MAX_DURATION_SECONDS, process_tempo_pub_match
except ImportError:  # package import for tests
    from base.explore_database import DEFAULT_JSON_SUBDIR, PATCH_739_RELEASE_TS, _dump_to_file, _iter_matches
    from base.tempo_analise_database_experiment import MAX_DURATION_SECONDS, process_tempo_pub_match

DEFAULT_BASE_DIR = Path("/Users/alex/Documents/ingame/bets_data/tempo_pub_experiment")
DEFAULT_JSON_DIR = Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches") / DEFAULT_JSON_SUBDIR


def _env_path(name: str, default: Path) -> Path:
    raw = os.getenv(name)
    if raw is None:
        return Path(default)
    raw = str(raw).strip()
    return Path(raw) if raw else Path(default)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return int(default)
    try:
        return int(float(str(raw).strip()))
    except (TypeError, ValueError):
        return int(default)


def run_tempo_explore_database(
    base_dir: Path | None = None,
    json_dir: Path | None = None,
    min_start_ts: int = PATCH_739_RELEASE_TS,
) -> dict:
    base_dir = Path(base_dir) if base_dir is not None else _env_path("TEMPO_EXPLORE_BASE_DIR", DEFAULT_BASE_DIR)
    json_dir = Path(json_dir) if json_dir is not None else _env_path("TEMPO_EXPLORE_JSON_DIR", DEFAULT_JSON_DIR)
    progress_every = max(0, _env_int("TEMPO_EXPLORE_PROGRESS_EVERY", 10000))
    max_files = max(0, _env_int("TEMPO_EXPLORE_MAX_FILES", 0))

    pub_files = sorted(json_dir.glob("combined*.json"))
    if max_files > 0:
        pub_files = pub_files[:max_files]
    if not pub_files:
        raise RuntimeError(f"Файлы combined*.json не найдены в {json_dir}")

    solo_dict = {}
    synergy_duo_dict = {}
    counterpick_1vs1_dict = {}
    totals = {
        "scanned": 0,
        "accepted": 0,
        "skipped": 0,
        "files": len(pub_files),
    }

    print("=" * 80)
    print("TEMPO EXPLORE DATABASE EXPERIMENT")
    print("=" * 80)
    print(f"Источник пабов: {json_dir}")
    print(f"Выход: {base_dir}")
    print(f"Фильтр даты: startDateTime >= {int(min_start_ts)}")
    print(f"Фильтр длительности: durationSeconds <= {MAX_DURATION_SECONDS}")

    gc.disable()
    try:
        for file_index, file_path in enumerate(pub_files, 1):
            file_scanned = 0
            file_accepted = 0
            print(f"[{file_index}/{len(pub_files)}] {file_path.name}")
            for match_id, match in _iter_matches(file_path):
                totals["scanned"] += 1
                file_scanned += 1
                updated = process_tempo_pub_match(
                    match,
                    solo_dict,
                    synergy_duo_dict,
                    counterpick_1vs1_dict,
                    min_start_ts=min_start_ts,
                    strict_positions=True,
                )
                if updated:
                    totals["accepted"] += 1
                    file_accepted += 1
                else:
                    totals["skipped"] += 1
                if progress_every > 0 and file_scanned % progress_every == 0:
                    print(
                        f"  ... {file_path.name}: scanned={file_scanned} accepted={file_accepted} skipped={file_scanned - file_accepted}",
                        flush=True,
                    )
            print(f"  ✓ scanned={file_scanned} accepted={file_accepted} skipped={file_scanned - file_accepted}")
            gc.collect()
    finally:
        gc.enable()

    base_dir.mkdir(parents=True, exist_ok=True)
    _dump_to_file(base_dir / "tempo_solo_dict_raw.json", solo_dict)
    _dump_to_file(base_dir / "tempo_synergy_duo_dict_raw.json", synergy_duo_dict)
    _dump_to_file(base_dir / "tempo_counterpick_1vs1_dict_raw.json", counterpick_1vs1_dict)

    meta = {
        "min_start_ts": int(min_start_ts),
        "source_json_dir": str(json_dir),
        "files": len(pub_files),
        **totals,
        "solo_keys": len(solo_dict),
        "synergy_duo_keys": len(synergy_duo_dict),
        "counterpick_1vs1_keys": len(counterpick_1vs1_dict),
    }
    _dump_to_file(base_dir / "tempo_build_meta.json", meta)

    print("\nРезультат:")
    print(json.dumps(meta, ensure_ascii=False, indent=2))
    return meta


def main():
    run_tempo_explore_database()


if __name__ == "__main__":
    main()
