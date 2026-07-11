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
import shutil
import sqlite3
import sys
import time
import zlib
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
SHARD_PER_FILE = os.getenv("EXPLORE_SHARD_PER_FILE", "1").strip().lower() not in {"0", "false", "no"}
MERGE_PARTITIONS = max(1, int(os.getenv("EXPLORE_MERGE_PARTITIONS", "64") or "64"))
KEEP_SHARDS = os.getenv("EXPLORE_KEEP_SHARDS", "0").strip().lower() in {"1", "true", "yes"}
COUNTER_BITS = 24
COUNTER_MASK = (1 << COUNTER_BITS) - 1
ALL_METRICS = ("lane", "early", "late", "post_lane")
OUTPUTS_BY_METRIC = {
    "lane": "lane_dict_raw.json",
    "early": "early_dict_raw.json",
    "late": "late_dict_raw.json",
    "post_lane": "post_lane_dict_raw.json",
}
LABELS_BY_METRIC = {
    "lane": "Lane",
    "early": "Early",
    "late": "Late",
    "post_lane": "Post-lane",
}


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


def _list_append_lane_entry(target_dict, key, value, kills10_diff=None):
    """Compact lane accumulator including the optional kills@10 target."""
    stats = target_dict.get(key)
    if stats is None:
        # wins, draws, games, leads, kill_draws, kill_games, diff_sum, diff_sq_sum
        stats = [0, 0, 0, 0, 0, 0, 0.0, 0.0]
        target_dict[key] = stats
    stats[2] += 1
    if value == 1:
        stats[0] += 1
    elif value == 0.5:
        stats[1] += 1
    if kills10_diff is None:
        return
    stats[5] += 1
    stats[6] += kills10_diff
    stats[7] += kills10_diff * kills10_diff
    if kills10_diff > 0:
        stats[3] += 1
    elif kills10_diff == 0:
        stats[4] += 1


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
    # Lane entries carry extra kill counters and therefore cannot use the
    # legacy three-counter packed integer representation.
    analise_database_module._append_lane_entry = _list_append_lane_entry



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


def _lane_stats_values(stats) -> tuple[int, int, int, int, int, int, float, float]:
    wins, draws, games = _stats_values(stats)
    if isinstance(stats, list):
        extra = list(stats[3:8]) + [0] * max(0, 5 - len(stats[3:8]))
        return wins, draws, games, int(extra[0]), int(extra[1]), int(extra[2]), float(extra[3]), float(extra[4])
    if isinstance(stats, dict):
        return (
            wins, draws, games,
            int(stats.get("kills10_leads", 0) or 0),
            int(stats.get("kills10_draws", 0) or 0),
            int(stats.get("kills10_games", 0) or 0),
            float(stats.get("kills10_diff_sum", 0.0) or 0.0),
            float(stats.get("kills10_diff_sq_sum", 0.0) or 0.0),
        )
    return wins, draws, games, 0, 0, 0, 0.0, 0.0


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


def _write_stats_entry(f, key, stats) -> None:
    wins, draws, games = _stats_values(stats)
    f.write(json.dumps(str(key), ensure_ascii=False))
    f.write(f':{{"wins":{wins},"draws":{draws},"games":{games}}}')


def _iter_stats_object_items(path: Path):
    if ijson is not None:
        with path.open("rb") as f:
            yield from ijson.kvitems(f, "", use_float=True)
        return

    data = _load_json_file(path)
    if not isinstance(data, dict):
        raise ValueError(f"stats_root_is_{type(data).__name__}")
    yield from data.items()


def _key_partition(key, partitions: int) -> int:
    if partitions <= 1:
        return 0
    return zlib.crc32(str(key).encode("utf-8")) % partitions


def _merge_stats_into(target: dict, key, stats) -> None:
    wins, draws, games = _stats_values(stats)
    current = target.get(key)
    if current is None:
        target[key] = [wins, draws, games]
        return
    current[0] += wins
    current[1] += draws
    current[2] += games


def _open_lane_sqlite(temp_path: Path) -> sqlite3.Connection:
    """Create the direct-build lane database at a fresh temporary path."""
    conn = sqlite3.connect(str(temp_path))
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA page_size=8192")
    conn.execute(
        """CREATE TABLE stats (
            key TEXT PRIMARY KEY,
            wins INTEGER NOT NULL,
            draws INTEGER NOT NULL,
            games INTEGER NOT NULL,
            kills10_leads INTEGER NOT NULL,
            kills10_draws INTEGER NOT NULL,
            kills10_games INTEGER NOT NULL,
            kills10_diff_sum REAL NOT NULL,
            kills10_diff_sq_sum REAL NOT NULL
        ) WITHOUT ROWID"""
    )
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value BLOB) WITHOUT ROWID")
    return conn


def _upsert_lane_stats(conn: sqlite3.Connection, lane_dict: dict) -> None:
    rows = (
        (str(key), *_lane_stats_values(stats))
        for key, stats in lane_dict.items()
    )
    conn.executemany(
        """INSERT INTO stats VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            wins=wins+excluded.wins,
            draws=draws+excluded.draws,
            games=games+excluded.games,
            kills10_leads=kills10_leads+excluded.kills10_leads,
            kills10_draws=kills10_draws+excluded.kills10_draws,
            kills10_games=kills10_games+excluded.kills10_games,
            kills10_diff_sum=kills10_diff_sum+excluded.kills10_diff_sum,
            kills10_diff_sq_sum=kills10_diff_sq_sum+excluded.kills10_diff_sq_sum""",
        rows,
    )
    conn.commit()


def _finalize_lane_sqlite(conn: sqlite3.Connection, temp_path: Path, output_path: Path) -> tuple[int, int]:
    try:
        entries, games = conn.execute("SELECT COUNT(*), COALESCE(SUM(games), 0) FROM stats").fetchone()
        encode = orjson.dumps if orjson is not None else lambda value: json.dumps(value).encode()
        meta = {"format_version": 2, "backend": "sqlite_stats", "entries": int(entries)}
        for key, value in meta.items():
            conn.execute("INSERT INTO meta (key, value) VALUES (?, ?)", (key, sqlite3.Binary(encode(value))))
        conn.commit()
    finally:
        conn.close()
    temp_path.replace(output_path)
    return int(entries), int(games)


def _dump_partitioned_stats_dict(stats_dict: dict, prefix: Path, partitions: int) -> list[Path]:
    """Выгружает словарь в hash-partition shards без создания копий в памяти."""
    prefix.parent.mkdir(parents=True, exist_ok=True)
    paths = [prefix.with_name(f"{prefix.name}.p{part:03d}.json") for part in range(partitions)]
    tmp_paths = [path.with_suffix(path.suffix + ".tmp") for path in paths]
    handles = []
    first = [True] * partitions

    try:
        for tmp_path in tmp_paths:
            f = tmp_path.open("w", encoding="utf-8")
            f.write("{")
            handles.append(f)

        for key, stats in stats_dict.items():
            part = _key_partition(key, partitions)
            f = handles[part]
            if not first[part]:
                f.write(",")
            first[part] = False
            _write_stats_entry(f, key, stats)

        for f in handles:
            f.write("}")
            f.close()
        handles.clear()

        for tmp_path, path in zip(tmp_paths, paths):
            tmp_path.replace(path)
    finally:
        for f in handles:
            try:
                f.close()
            except Exception:
                pass
    return paths


def _merge_partitioned_shards(partition_shards: list[list[Path]], output_path: Path) -> tuple[int, int]:
    """Склеивает partition shards в итоговый raw json, держа в памяти только один partition."""
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    total_keys = 0
    total_games = 0
    first_out = True

    with tmp_path.open("w", encoding="utf-8") as f:
        f.write("{")
        for part, shard_paths in enumerate(partition_shards):
            bucket: dict = {}
            for shard_path in shard_paths:
                if not shard_path.exists():
                    continue
                for key, stats in _iter_stats_object_items(shard_path):
                    _merge_stats_into(bucket, key, stats)

            for key, stats in bucket.items():
                if not first_out:
                    f.write(",")
                first_out = False
                _write_stats_entry(f, key, stats)
                total_keys += 1
                total_games += _stats_games(stats)

            del bucket
            if part % 8 == 7:
                gc.collect()
        f.write("}")

    tmp_path.replace(output_path)
    return total_keys, total_games


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
    pub_files = sorted(
        p for p in json_dir.glob("*.json")
        if p.name != "merge_patch_summary.json"
    )

    max_files = int(os.getenv("EXPLORE_MAX_FILES", "0") or "0")
    if max_files > 0:
        pub_files = pub_files[:max_files]
    return pub_files


def _enabled_metrics() -> tuple[str, ...]:
    raw = os.getenv("EXPLORE_METRICS", "all").strip().lower()
    if raw in {"", "all", "*"}:
        return ALL_METRICS
    aliases = {
        "postlane": "post_lane",
        "post-lane": "post_lane",
        "post_lane": "post_lane",
    }
    metrics: list[str] = []
    for item in raw.replace(";", ",").split(","):
        metric = aliases.get(item.strip(), item.strip())
        if not metric:
            continue
        if metric not in ALL_METRICS:
            raise ValueError(f"Unknown EXPLORE_METRICS item: {item!r}; allowed={ALL_METRICS}")
        if metric not in metrics:
            metrics.append(metric)
    if not metrics:
        raise ValueError("EXPLORE_METRICS resolved to empty set")
    return tuple(metrics)


def _new_metric_dicts(metric_names: tuple[str, ...]) -> tuple[dict | None, dict | None, dict | None, dict | None]:
    return (
        {} if "lane" in metric_names else None,
        {} if "early" in metric_names else None,
        {} if "late" in metric_names else None,
        {} if "post_lane" in metric_names else None,
    )


def _dict_len(data) -> int:
    return len(data) if data is not None else 0


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


def _build_sqlite_dicts(stats_dir: Path, metric_names: tuple) -> None:
    """Convert JSON stats dicts to SQLite3 for fast runtime key-value lookup."""
    for metric in metric_names:
        filename = OUTPUTS_BY_METRIC.get(metric)
        if not filename:
            continue
        json_path = stats_dir / filename
        sqlite_path = json_path.with_suffix(".sqlite3")
        if not json_path.exists():
            print(f"  ⚠️ {filename} не найден, пропускаем sqlite build")
            continue
        print(f"  🧱 Building {sqlite_path.name}...", end=" ", flush=True)
        started = time.monotonic()
        temp_path = sqlite_path.with_suffix(".sqlite3.tmp")
        if temp_path.exists():
            temp_path.unlink()

        conn = sqlite3.connect(str(temp_path))
        try:
            conn.execute("PRAGMA journal_mode=OFF")
            conn.execute("PRAGMA synchronous=OFF")
            conn.execute("PRAGMA page_size=8192")
            conn.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value BLOB)")
            conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value BLOB)")

            entries = 0
            batch = []
            batch_size = 5000
            encode = orjson.dumps if orjson is not None else lambda v: json.dumps(v).encode()

            for key, stats in _iter_stats_object_items(json_path):
                batch.append((key, sqlite3.Binary(encode(stats))))
                entries += 1
                if len(batch) >= batch_size:
                    conn.executemany("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", batch)
                    batch.clear()
            if batch:
                conn.executemany("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", batch)
                batch.clear()

            # Write meta (must match runtime _stats_expected_meta keys)
            source_stat = json_path.stat()
            meta = {
                "format_version": 1,
                "backend": "sqlite_kv",
                "source_name": json_path.name,
                "source_size": source_stat.st_size,
                "source_mtime_ns": source_stat.st_mtime_ns,
                "entries": entries,
            }
            for mk, mv in meta.items():
                conn.execute(
                    "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                    (mk, sqlite3.Binary(encode(mv))),
                )
            conn.commit()
        finally:
            conn.close()

        if sqlite_path.exists():
            sqlite_path.unlink()
        temp_path.rename(sqlite_path)
        print(f"✓ {entries:,} keys, {time.monotonic() - started:.1f}s")


class _OwnedLaneSqliteBuild:
    """Own one unique temporary lane DB and roll it back unless finalized."""

    def __init__(self):
        self.conn: sqlite3.Connection | None = None
        self.temp_path: Path | None = None

    def open(self, temp_path: Path) -> sqlite3.Connection:
        if self.conn is not None:
            raise RuntimeError("lane sqlite build is already open")
        if temp_path.exists():
            raise FileExistsError(f"lane sqlite temp already exists: {temp_path}")
        self.temp_path = temp_path
        self.conn = _open_lane_sqlite(temp_path)
        return self.conn

    def finalize(self, output_path: Path) -> tuple[int, int]:
        if self.conn is None or self.temp_path is None:
            raise RuntimeError("lane sqlite build is not open")
        conn, temp_path = self.conn, self.temp_path
        self.conn = None
        try:
            result = _finalize_lane_sqlite(conn, temp_path, output_path)
        except BaseException:
            # _finalize_lane_sqlite closes the connection in its own finally.
            raise
        self.temp_path = None
        return result

    def rollback(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            finally:
                self.conn = None
        if self.temp_path is not None and self.temp_path.exists():
            self.temp_path.unlink()
        self.temp_path = None


def _main_impl(lane_build: _OwnedLaneSqliteBuild) -> int:
    print("=" * 80)
    print("ПОСТРОЕНИЕ СТАТИСТИКИ (ИСКЛЮЧАЯ TEST SET)")
    print("=" * 80)
    print("✓ Train set: все матчи из базы → статистика")
    print("✓ Test set:  исключается из train (используется существующий)")
    print("✓ Strict position quality: включен check_match_quality(strict_lane_positions=True)")
    print("✓ Streaming JSON: ijson" if ijson is not None else "⚠️ Streaming JSON недоступен, fallback json.load")
    print(f"✓ Compact counters: {COUNTER_MODE}")
    if SHARD_PER_FILE:
        print(f"✓ File shards: включены, merge partitions: {MERGE_PARTITIONS}")
    else:
        print("⚠️ File shards: выключены, словари будут держаться в памяти до конца")
    print("=" * 80)

    _enable_compact_accumulators()

    metric_names = _enabled_metrics()
    json_dir = Path(os.getenv("EXPLORE_JSON_DIR", str(DEFAULT_JSON_DIR)))
    test_set_path = Path(os.getenv("EXPLORE_TEST_SET_PATH", str(DEFAULT_TEST_SET_PATH)))
    stats_dir = Path(os.getenv("EXPLORE_STATS_DIR", str(DEFAULT_STATS_DIR)))
    min_start_ts = int(start_date_time)
    max_matches = int(os.getenv("EXPLORE_MAX_MATCHES", "0") or "0")
    run_id = os.getenv("EXPLORE_RUN_ID", time.strftime("%Y%m%d_%H%M%S"))
    shard_dir = Path(os.getenv("EXPLORE_SHARD_DIR", str(stats_dir / "explore_database_shards" / run_id)))
    metric_shards = {
        metric: [[] for _ in range(MERGE_PARTITIONS)]
        for metric in metric_names if metric != "lane"
    }
    lane_conn = None
    lane_temp_path = None
    lane_output_path = stats_dir / "lane_dict_raw.sqlite3"
    if "lane" in metric_names:
        stats_dir.mkdir(parents=True, exist_ok=True)
        lane_temp_path = stats_dir / (
            f"lane_dict_raw.sqlite3.{run_id}.{os.getpid()}.{time.time_ns()}.tmp"
        )
        lane_conn = lane_build.open(lane_temp_path)

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
    print(f"enabled_metrics: {', '.join(metric_names)}")

    print("\n[ШАГ 2/3] Построение статистики на train set...")

    if SHARD_PER_FILE:
        if shard_dir.exists():
            shutil.rmtree(shard_dir)
        shard_dir.mkdir(parents=True, exist_ok=True)
        print(f"  Shards dir: {shard_dir}")
        lane_dict, early_dict, late_dict, post_lane_dict = _new_metric_dicts(metric_names)
    else:
        lane_dict, early_dict, late_dict, post_lane_dict = _new_metric_dicts(metric_names)

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
        if SHARD_PER_FILE:
            lane_dict, early_dict, late_dict, post_lane_dict = _new_metric_dicts(metric_names)

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
                        f"Lane: {_dict_len(lane_dict):,}, Early: {_dict_len(early_dict):,}, "
                        f"Late: {_dict_len(late_dict):,}, PostLane: {_dict_len(post_lane_dict):,}, "
                        f"RSS≈{_rss_mb():.0f}MB, {rate:.0f} maps/s",
                        end="",
                        flush=True,
                    )

                if max_matches > 0 and train_total >= max_matches:
                    break

            gc.collect()
            if lane_conn is not None and lane_dict:
                _upsert_lane_stats(lane_conn, lane_dict)
                lane_dict.clear()
            if SHARD_PER_FILE and file_train > 0:
                shard_started = time.monotonic()
                prefix = shard_dir / f"{idx:04d}_{file.stem}"
                per_file_data = {
                    "early": early_dict,
                    "late": late_dict,
                    "post_lane": post_lane_dict,
                }
                per_file_data = {
                    metric: data
                    for metric, data in per_file_data.items()
                    if metric in metric_names and data is not None
                }
                key_counts = {metric: len(data) for metric, data in per_file_data.items()}
                for metric, data in per_file_data.items():
                    paths = _dump_partitioned_stats_dict(
                        data,
                        prefix.with_name(f"{prefix.name}.{metric}"),
                        MERGE_PARTITIONS,
                    )
                    for part, path in enumerate(paths):
                        metric_shards[metric][part].append(path)

                for data in per_file_data.values():
                    data.clear()
                gc.collect()
                shard_msg = (
                    f" shards:{time.monotonic() - shard_started:.1f}s "
                    f"keys "
                    + "/".join(f"{metric}:{key_counts.get(metric, 0):,}" for metric in metric_names)
                )
            else:
                shard_msg = ""
            print(
                f" ✓ train:{file_train} excluded:{file_excluded} "
                f"time:{time.monotonic() - file_started_at:.1f}s RSS≈{_rss_mb():.0f}MB"
                f"{shard_msg}"
            )
        except Exception as e:
            # Do not merge a partially streamed file into the direct SQLite
            # accumulator; completed earlier files remain committed.
            if lane_dict is not None:
                lane_dict.clear()
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

    print("\n[ШАГ 3/3] Сохранение результатов...")
    stats_dir.mkdir(parents=True, exist_ok=True)

    if lane_conn is not None and lane_temp_path is not None:
        lane_entries, lane_games = lane_build.finalize(lane_output_path)
        lane_conn = None
        print(
            f"  ✓ {lane_output_path.name} ({lane_entries:,} ключей, "
            f"{lane_games:,} записей; direct SQLite)"
        )

    if SHARD_PER_FILE:
        outputs = [(OUTPUTS_BY_METRIC[metric], metric) for metric in metric_names if metric != "lane"]
        merged_summary = {}
        for filename, metric in outputs:
            output_path = stats_dir / filename
            started = time.monotonic()
            keys_count, games_count = _merge_partitioned_shards(metric_shards[metric], output_path)
            merged_summary[metric] = (keys_count, games_count)
            print(
                f"  ✓ {filename} ({keys_count:,} ключей, {games_count:,} записей, "
                f"{time.monotonic() - started:.1f}s)"
            )

        print("\nСтатистика по словарям (train set):")
        for metric in metric_names:
            label = LABELS_BY_METRIC[metric]
            if metric == "lane":
                continue
            print(
                f"  {label + ' dict:':15s}"
                f"{merged_summary[metric][0]:>6,} ключей, {merged_summary[metric][1]:>7,} записей"
            )
        if KEEP_SHARDS:
            print(f"  Shards сохранены: {shard_dir}")
        else:
            shutil.rmtree(shard_dir, ignore_errors=True)
            print(f"  Shards удалены: {shard_dir}")
    else:
        print("\nСтатистика по словарям (train set):")
        data_by_metric = {
            "lane": lane_dict,
            "early": early_dict,
            "late": late_dict,
            "post_lane": post_lane_dict,
        }
        for metric in metric_names:
            if metric == "lane":
                continue
            data = data_by_metric[metric] or {}
            matches = sum(_stats_games(stats) for stats in data.values())
            label = LABELS_BY_METRIC[metric]
            print(f"  {label + ' dict:':15s}{len(data):>6,} ключей, {matches:>7,} записей")

        outputs = [
            (OUTPUTS_BY_METRIC[metric], data_by_metric[metric] or {})
            for metric in metric_names if metric != "lane"
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

    # Build SQLite3 versions of stats dicts for fast runtime lookup
    print("\n[ШАГ 4/4] Построение SQLite3 версий словарей...")
    _build_sqlite_dicts(stats_dir, tuple(metric for metric in metric_names if metric != "lane"))

    return 0


def main() -> int:
    lane_build = _OwnedLaneSqliteBuild()
    try:
        return _main_impl(lane_build)
    finally:
        # Covers normal exceptions and BaseException subclasses such as
        # KeyboardInterrupt. A successful atomic finalize disarms rollback.
        lane_build.rollback()


if __name__ == "__main__":
    raise SystemExit(main())
