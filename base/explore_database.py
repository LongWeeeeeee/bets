"""
Построение статистики с исключением TEST SET для честной валидации.

Оптимизировано для больших json part-файлов:
- входные файлы читаются потоково через ijson, без json.load на 500MB файл;
- внутренние счетчики хранятся компактно как mutable list [wins, draws, games]
  или packed-int при EXPLORE_COUNTER_MODE=packed;
- на диск сохраняется прежний формат {"wins": N, "draws": N, "games": N}.
"""

from __future__ import annotations

import gc
import json
import os
import resource
import sys
import time
from collections import Counter
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
for path in (str(BASE_DIR), str(ROOT_DIR)):
    if path not in sys.path:
        sys.path.insert(0, path)

try:
    import ijson
except Exception:  # pragma: no cover - fallback for machines without ijson
    ijson = None

try:
    import orjson
except Exception:  # pragma: no cover - fallback for machines without orjson
    orjson = None

import analise_database as analise_database_module
from keys import start_date_time_739 as start_date_time
from maps_research import check_match_quality


DEFAULT_JSON_DIR = Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches/json_parts_split_from_object")
DEFAULT_TEST_SET_PATH = Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches/extracted_100k_matches.json")
DEFAULT_STATS_DIR = Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches")
PROGRESS_EVERY = int(os.getenv("EXPLORE_PROGRESS_EVERY", "50000"))
COUNTER_MODE = os.getenv("EXPLORE_COUNTER_MODE", "list").strip().lower()
COUNTER_BITS = 24
COUNTER_MASK = (1 << COUNTER_BITS) - 1


def _rss_mb() -> float:
    """ru_maxrss: bytes on macOS, KB on Linux."""
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return rss / 1024 / 1024
    return rss / 1024


def _load_json_file(path: Path):
    if orjson is not None:
        with path.open("rb") as f:
            return orjson.loads(f.read())
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _iter_json_object_items(path: Path):
    if ijson is not None:
        with path.open("rb") as f:
            yield from ijson.kvitems(f, "", use_float=True)
        return

    data = _load_json_file(path)
    if not isinstance(data, dict):
        raise ValueError(f"root_is_{type(data).__name__}")
    yield from data.items()


def _list_append_to_dict(target_dict, key, value, is_defaultdict=None):
    """Fast mutable counter: [wins, draws, games]."""
    stats = target_dict.get(key)
    if stats is None:
        stats = [0, 0, 0]  # wins, draws, games
        target_dict[key] = stats

    stats[2] += 1
    if value == 1:
        stats[0] += 1
    elif value == 0.5:
        stats[1] += 1


def _packed_append_to_dict(target_dict, key, value, is_defaultdict=None):
    """Lower-memory packed counter, slower than list mode."""
    stats = target_dict.get(key)
    if stats is None:
        wins = 0
        draws = 0
        games = 0
    else:
        wins = stats & COUNTER_MASK
        draws = (stats >> COUNTER_BITS) & COUNTER_MASK
        games = stats >> (COUNTER_BITS * 2)

    games += 1
    if value == 1:
        wins += 1
    elif value == 0.5:
        draws += 1

    target_dict[key] = (games << (COUNTER_BITS * 2)) | (draws << COUNTER_BITS) | wins


def _enable_compact_accumulators() -> None:
    if COUNTER_MODE == "packed":
        analise_database_module._append_to_dict = _packed_append_to_dict
    else:
        analise_database_module._append_to_dict = _list_append_to_dict



def _stats_games(stats) -> int:
    if isinstance(stats, int):
        return int(stats >> (COUNTER_BITS * 2))
    if isinstance(stats, list):
        return int(stats[2]) if len(stats) >= 3 else 0
    if isinstance(stats, dict):
        return int(stats.get("games", 0) or 0)
    return 0


def _stats_values(stats) -> tuple[int, int, int]:
    if isinstance(stats, int):
        wins = stats & COUNTER_MASK
        draws = (stats >> COUNTER_BITS) & COUNTER_MASK
        games = stats >> (COUNTER_BITS * 2)
        return int(wins), int(draws), int(games)
    if isinstance(stats, list):
        wins = int(stats[0]) if len(stats) > 0 else 0
        draws = int(stats[1]) if len(stats) > 1 else 0
        games = int(stats[2]) if len(stats) > 2 else 0
        return wins, draws, games
    if isinstance(stats, dict):
        return (
            int(stats.get("wins", 0) or 0),
            int(stats.get("draws", 0) or 0),
            int(stats.get("games", 0) or 0),
        )
    return 0, 0, 0


def _dump_stats_dict(stats_dict: dict, path: Path) -> None:
    """Пишет прежний JSON-формат без промежуточной полной конвертации в dict."""
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        f.write("{")
        first = True
        for key, stats in stats_dict.items():
            wins, draws, games = _stats_values(stats)
            if not first:
                f.write(",")
            first = False
            f.write(json.dumps(str(key), ensure_ascii=False))
            f.write(f':{{"wins":{wins},"draws":{draws},"games":{games}}}')
        f.write("}")
    tmp_path.replace(path)


def _load_test_match_ids(test_set_path: Path) -> set[str]:
    test_match_ids: set[str] = set()
    if not test_set_path.exists():
        print("  ⚠️  Файл test_set_pub_matches.json не найден")
        print("  → Будут обработаны все матчи (без исключений)")
        return test_match_ids

    try:
        test_set_data = _load_json_file(test_set_path)
        if isinstance(test_set_data, dict):
            test_match_ids = {str(mid) for mid in test_set_data.keys()}
        elif isinstance(test_set_data, list):
            test_match_ids = {
                str(m.get("match_id") or m.get("id"))
                for m in test_set_data
                if isinstance(m, dict) and (m.get("match_id") or m.get("id"))
            }
        print(f"  ✓ Загружено {len(test_match_ids):,} match_id из test_set_pub_matches.json")
        print("  → Эти матчи будут исключены из train set")
    except Exception as e:
        print(f"  ⚠️  Ошибка загрузки test set: {e}")
        print("  → Будут обработаны все матчи (без исключений)")
    return test_match_ids


def _discover_pub_files(json_dir: Path) -> list[Path]:
    pub_files = sorted(json_dir.glob("combined*.json"))
    if not pub_files:
        pub_files = sorted(
            p for p in json_dir.glob("*.json")
            if p.name != "merge_patch_summary.json"
        )

    max_files = int(os.getenv("EXPLORE_MAX_FILES", "0") or "0")
    if max_files > 0:
        pub_files = pub_files[:max_files]
    return pub_files


def _match_is_train_candidate(match_id, match, min_start_ts: int, test_match_ids: set[str]) -> tuple[bool, str | None]:
    if not isinstance(match, dict):
        return False, "not_dict"
    if "startDateTime" not in match:
        return False, "no_startDateTime"
    try:
        if int(match["startDateTime"]) < min_start_ts:
            return False, "old_patch"
    except Exception:
        return False, "bad_startDateTime"
    if "players" not in match or len(match.get("players", [])) != 10:
        return False, "bad_players"
    if str(match_id) in test_match_ids:
        return False, "test_set"
    return True, None


def main() -> int:
    print("=" * 80)
    print("ПОСТРОЕНИЕ СТАТИСТИКИ (ИСКЛЮЧАЯ TEST SET)")
    print("=" * 80)
    print("✓ Train set: все матчи из базы → статистика")
    print("✓ Test set:  исключается из train (используется существующий)")
    print("✓ Strict position quality: включен check_match_quality(strict_lane_positions=True)")
    print("✓ Streaming JSON: ijson" if ijson is not None else "⚠️ Streaming JSON недоступен, fallback json.load")
    print(f"✓ Compact counters: {COUNTER_MODE}")
    print("=" * 80)

    _enable_compact_accumulators()

    json_dir = Path(os.getenv("EXPLORE_JSON_DIR", str(DEFAULT_JSON_DIR)))
    test_set_path = Path(os.getenv("EXPLORE_TEST_SET_PATH", str(DEFAULT_TEST_SET_PATH)))
    stats_dir = Path(os.getenv("EXPLORE_STATS_DIR", str(DEFAULT_STATS_DIR)))
    min_start_ts = int(start_date_time)
    max_matches = int(os.getenv("EXPLORE_MAX_MATCHES", "0") or "0")

    print("\n[ШАГ 1/3] Загрузка test set для исключения...")
    test_match_ids = _load_test_match_ids(test_set_path)

    pub_files = _discover_pub_files(json_dir)
    if not pub_files:
        print(f"Файлы не найдены в {json_dir}!")
        print(f"Текущая директория скрипта: {BASE_DIR}")
        return 1

    print(f"\nНайдено файлов для обработки: {len(pub_files)}")
    print(f"Источник: {json_dir}")
    print(f"start_date_time: {min_start_ts}")

    print("\n[ШАГ 2/3] Построение статистики на train set...")

    lane_dict: dict = {}
    early_dict: dict = {}
    late_dict: dict = {}
    post_lane_dict: dict = {}

    train_processed = 0
    train_total = 0
    test_excluded = 0
    analysis_errors = 0
    skip_reasons = Counter()
    quality_reasons = Counter()
    started_at = time.monotonic()

    for idx, file in enumerate(pub_files, 1):
        file_started_at = time.monotonic()
        print(f"  [{idx}/{len(pub_files)}] Обработка {file.name}...", end=" ", flush=True)
        file_train = 0
        file_excluded = 0

        try:
            for match_id, match in _iter_json_object_items(file):
                ok, reason = _match_is_train_candidate(match_id, match, min_start_ts, test_match_ids)
                if not ok:
                    skip_reasons[reason or "unknown"] += 1
                    if reason == "test_set":
                        file_excluded += 1
                        test_excluded += 1
                    continue

                result, message = check_match_quality(match, strict_lane_positions=True)
                if not result:
                    quality_reasons[message or "quality_unknown"] += 1
                    continue

                try:
                    analise_database_module.analise_database(
                        match,
                        lane_dict,
                        early_dict,
                        late_dict,
                        post_lane_dict=post_lane_dict,
                    )
                    train_processed += 1
                    file_train += 1
                except Exception:
                    analysis_errors += 1
                    continue

                train_total += 1
                if PROGRESS_EVERY > 0 and train_total % PROGRESS_EVERY == 0:
                    elapsed = max(time.monotonic() - started_at, 1)
                    rate = train_total / elapsed
                    print(
                        f"\n    [{train_total:,}] "
                        f"Lane: {len(lane_dict):,}, Early: {len(early_dict):,}, "
                        f"Late: {len(late_dict):,}, PostLane: {len(post_lane_dict):,}, "
                        f"RSS≈{_rss_mb():.0f}MB, {rate:.0f} maps/s",
                        end="",
                        flush=True,
                    )

                if max_matches > 0 and train_total >= max_matches:
                    break

            gc.collect()
            print(
                f" ✓ train:{file_train} excluded:{file_excluded} "
                f"time:{time.monotonic() - file_started_at:.1f}s RSS≈{_rss_mb():.0f}MB"
            )
        except Exception as e:
            print(f"✗ Ошибка: {e}")

        if max_matches > 0 and train_total >= max_matches:
            print(f"  ⚠️ Остановлено по EXPLORE_MAX_MATCHES={max_matches:,}")
            break

    print(f"\n✓ Успешно обработано train матчей: {train_processed:,}")
    print(f"✓ Исключено test матчей: {test_excluded:,}")
    if analysis_errors:
        print(f"⚠️ Ошибок analise_database: {analysis_errors:,}")
    if quality_reasons:
        print("Топ причин отбраковки check_match_quality:")
        for reason, count in quality_reasons.most_common(10):
            print(f"  - {reason}: {count:,}")
    if skip_reasons:
        print("Топ причин пропуска до quality-check:")
        for reason, count in skip_reasons.most_common(10):
            print(f"  - {reason}: {count:,}")

    lane_matches = sum(_stats_games(stats) for stats in lane_dict.values())
    early_matches = sum(_stats_games(stats) for stats in early_dict.values())
    late_matches = sum(_stats_games(stats) for stats in late_dict.values())
    post_lane_matches = sum(_stats_games(stats) for stats in post_lane_dict.values())

    print("\nСтатистика по словарям (train set):")
    print(f"  Lane dict:     {len(lane_dict):>6,} ключей, {lane_matches:>7,} записей")
    print(f"  Early dict:    {len(early_dict):>6,} ключей, {early_matches:>7,} записей")
    print(f"  Late dict:     {len(late_dict):>6,} ключей, {late_matches:>7,} записей")
    print(f"  Post-lane dict:{len(post_lane_dict):>6,} ключей, {post_lane_matches:>7,} записей")

    print("\n[ШАГ 3/3] Сохранение результатов...")
    stats_dir.mkdir(parents=True, exist_ok=True)

    outputs = [
        ("lane_dict_raw.json", lane_dict),
        ("early_dict_raw.json", early_dict),
        ("late_dict_raw.json", late_dict),
        ("post_lane_dict_raw.json", post_lane_dict),
    ]
    for filename, data in outputs:
        output_path = stats_dir / filename
        started = time.monotonic()
        _dump_stats_dict(data, output_path)
        print(f"  ✓ {filename} ({len(data):,} ключей, {time.monotonic() - started:.1f}s)")

    print(f"\n{'=' * 80}")
    print("ЗАВЕРШЕНО!")
    print(f"{'=' * 80}")
    print(f"TRAIN SET: {train_processed:,} обработанных матчей")
    print(f"Test set исключен: {test_excluded:,} матчей")
    print(f"RSS peak≈{_rss_mb():.0f}MB")
    print("Для валидации запустите: python check_metrics.py")
    print(f"{'=' * 80}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
