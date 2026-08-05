"""
Построение статистики с исключением TEST SET для честной валидации.

Оптимизировано для больших json part-файлов:
- входные файлы читаются потоково через ijson, без json.load на 500MB файл;
- счетчики ограничены batch-лимитами и UPSERT-ятся в staging SQLite;
- production-файлы не меняются до commit/quick_check всех выбранных метрик;
- публикация каждого готового файла выполняется одним os.replace().
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
from dataclasses import dataclass
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
# Prefer base/ over project root so legacy root-level maps_research.py does not
# shadow base/maps_research.py (broken hero_valid paths → 0 train matches).
for path in (str(ROOT_DIR), str(BASE_DIR)):
    if path in sys.path:
        sys.path.remove(path)
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
KEEP_SHARDS = os.getenv("EXPLORE_KEEP_SHARDS", "0").strip().lower() in {"1", "true", "yes"}
WRITE_JSON = os.getenv("EXPLORE_WRITE_JSON", "0").strip().lower() in {"1", "true", "yes"}
SQLITE_INSERT_BATCH = 5000
# Staging rebuild writers are disposable; prefer RAM page cache + exclusive lock.
SQLITE_CACHE_SIZE_KIB = -max(64 * 1024, int(os.getenv("EXPLORE_SQLITE_CACHE_KIB", str(512 * 1024)) or str(512 * 1024)))
SQLITE_MMAP_SIZE = max(0, int(os.getenv("EXPLORE_SQLITE_MMAP_BYTES", str(512 * 1024 * 1024)) or str(512 * 1024 * 1024)))
FLUSH_MATCH_LIMIT = max(1, int(os.getenv("EXPLORE_FLUSH_MATCHES", "5000") or "5000"))
FLUSH_KEY_LIMIT = max(1, int(os.getenv("EXPLORE_FLUSH_KEYS", "1000000") or "1000000"))
COUNTER_BITS = 24
COUNTER_MASK = (1 << COUNTER_BITS) - 1
ALL_METRICS = ("lane", "early", "early_end", "late", "post_lane", "kills_window")
OUTPUTS_BY_METRIC = {
    "lane": "lane_dict_raw.json",
    "early": "early_dict_raw.json",
    "early_end": "early_end_dict_raw.json",
    "late": "late_dict_raw.json",
    "post_lane": "post_lane_dict_raw.json",
    "kills_window": "kills_window_dict_raw.json",
}
LABELS_BY_METRIC = {
    "lane": "Lane",
    "early": "Early(NW)",
    "early_end": "Early(end)",
    "late": "Late",
    "post_lane": "Post-lane",
    "kills_window": "Kills-window",
}
# Direct-sqlite metrics use columnar stats tables (not kv blob).
DIRECT_SQLITE_METRICS = ("lane", "kills_window")


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


def _kills_window_column_names() -> list[str]:
    labels = list(analise_database_module.KILLS_WINDOW_LABELS)
    cols: list[str] = []
    for label in labels:
        cols.extend(
            [
                f"kills_{label}_leads",
                f"kills_{label}_draws",
                f"kills_{label}_games",
                f"kills_{label}_diff_sum",
                f"kills_{label}_diff_sq_sum",
            ]
        )
    return cols


def _kills_window_stats_values(stats) -> tuple:
    """Normalize list/dict kill-window counters to the fixed column order."""
    labels = list(analise_database_module.KILLS_WINDOW_LABELS)
    expected = len(labels) * 5
    if isinstance(stats, list):
        values = list(stats[:expected]) + [0] * max(0, expected - len(stats))
        out = []
        for index in range(len(labels)):
            base = index * 5
            out.extend(
                [
                    int(values[base] or 0),
                    int(values[base + 1] or 0),
                    int(values[base + 2] or 0),
                    float(values[base + 3] or 0.0),
                    float(values[base + 4] or 0.0),
                ]
            )
        return tuple(out)
    if isinstance(stats, dict):
        out = []
        for label in labels:
            out.extend(
                [
                    int(stats.get(f"kills_{label}_leads", 0) or 0),
                    int(stats.get(f"kills_{label}_draws", 0) or 0),
                    int(stats.get(f"kills_{label}_games", 0) or 0),
                    float(stats.get(f"kills_{label}_diff_sum", 0.0) or 0.0),
                    float(stats.get(f"kills_{label}_diff_sq_sum", 0.0) or 0.0),
                ]
            )
        return tuple(out)
    return tuple([0, 0, 0, 0.0, 0.0] * len(labels))


def _kills_window_stats_games(stats) -> int:
    values = _kills_window_stats_values(stats)
    # max games across windows (one match can fill several windows)
    return max((int(values[i]) for i in range(2, len(values), 5)), default=0)


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


def _sqlite_path_for_metric(stats_dir: Path, metric: str) -> Path:
    """lane/early/late/post_lane → <metric>_dict_raw.sqlite3"""
    json_name = OUTPUTS_BY_METRIC[metric]
    return _sqlite_path_from_json_name(stats_dir, json_name)


def _sqlite_path_from_json_name(stats_dir: Path, json_name: str) -> Path:
    """lane_dict_raw.json → lane_dict_raw.sqlite3 (stem + .sqlite3)."""
    return stats_dir / f"{Path(json_name).stem}.sqlite3"


def _encode_stats_blob(stats) -> bytes:
    wins, draws, games = _stats_values(stats)
    payload = {"wins": wins, "draws": draws, "games": games}
    if orjson is not None:
        return orjson.dumps(payload)
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def _encode_meta_blob(value) -> bytes:
    if orjson is not None:
        return orjson.dumps(value)
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


def _apply_staging_sqlite_pragmas(conn: sqlite3.Connection) -> None:
    """Speed-oriented PRAGMAs for disposable staging rebuild DBs."""
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA locking_mode=EXCLUSIVE")
    conn.execute(f"PRAGMA cache_size={SQLITE_CACHE_SIZE_KIB}")
    if SQLITE_MMAP_SIZE > 0:
        conn.execute(f"PRAGMA mmap_size={SQLITE_MMAP_SIZE}")


def _open_sqlite_stats_writer(tmp_path: Path) -> sqlite3.Connection:
    """Open tmp sqlite, apply PRAGMAs, create kv/meta tables."""
    if tmp_path.exists():
        raise FileExistsError(f"sqlite temp already exists: {tmp_path}")
    conn = sqlite3.connect(str(tmp_path))
    conn.execute("PRAGMA page_size=8192")
    _apply_staging_sqlite_pragmas(conn)
    conn.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value BLOB)")
    conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value BLOB)")
    return conn


def _write_sqlite_meta(
    conn: sqlite3.Connection,
    *,
    source_name: str,
    source_size: int = 0,
    source_mtime_ns: int = 0,
    entries: int,
) -> None:
    meta = {
        "format_version": 1,
        "backend": "sqlite_kv",
        "source_name": source_name,
        "source_size": int(source_size),
        "source_mtime_ns": int(source_mtime_ns),
        "entries": int(entries),
    }
    for mk, mv in meta.items():
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            (mk, sqlite3.Binary(_encode_meta_blob(mv))),
        )


def _finalize_sqlite_db(tmp_path: Path, final_path: Path) -> None:
    """Atomically replace the final database without a delete window."""
    tmp_path.replace(final_path)


def _unique_sqlite_temp_path(sqlite_path: Path) -> Path:
    """Return a fresh temp path or fail rather than overwriting another build."""
    tmp_path = sqlite_path.with_name(
        f"{sqlite_path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    if tmp_path.exists():
        raise FileExistsError(f"sqlite temp already exists: {tmp_path}")
    return tmp_path


def _flush_sqlite_batch(conn: sqlite3.Connection, batch: list) -> None:
    if not batch:
        return
    conn.executemany("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", batch)
    batch.clear()


def _open_kv_accumulator(temp_path: Path) -> sqlite3.Connection:
    """Open a publishable staging DB that sums bounded in-memory batches."""
    if temp_path.exists():
        raise FileExistsError(f"kv sqlite temp already exists: {temp_path}")
    conn = sqlite3.connect(str(temp_path))
    conn.execute("PRAGMA page_size=8192")
    _apply_staging_sqlite_pragmas(conn)
    conn.execute(
        """CREATE TABLE kv (
            key TEXT PRIMARY KEY,
            value BLOB,
            wins INTEGER NOT NULL,
            draws INTEGER NOT NULL,
            games INTEGER NOT NULL
        ) WITHOUT ROWID"""
    )
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value BLOB) WITHOUT ROWID")
    return conn


def _upsert_kv_stats(conn: sqlite3.Connection, stats_dict: dict) -> None:
    # Fast path: rebuild accumulators are almost always compact list counters.
    rows = []
    append = rows.append
    for key, stats in stats_dict.items():
        if isinstance(stats, list) and len(stats) >= 3:
            append((str(key), None, int(stats[0]), int(stats[1]), int(stats[2])))
        else:
            wins, draws, games = _stats_values(stats)
            append((str(key), None, wins, draws, games))
    conn.executemany(
        """INSERT INTO kv VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value=NULL,
            wins=wins+excluded.wins,
            draws=draws+excluded.draws,
            games=games+excluded.games""",
        rows,
    )


@dataclass(frozen=True)
class _PreparedKvBuild:
    metric: str
    entries: int
    games: int
    staging_path: Path
    publish_path: Path


class _OwnedKvSqliteBuild:
    """Accumulate one metric on disk and publish it only after validation."""

    def __init__(self, metric: str):
        self.metric = metric
        self.conn: sqlite3.Connection | None = None
        self.temp_path: Path | None = None
        self.publish_path: Path | None = None
        self.output_path: Path | None = None
        self.prepared: _PreparedKvBuild | None = None

    def open(self, temp_path: Path) -> sqlite3.Connection:
        if self.conn is not None or self.temp_path is not None:
            raise RuntimeError(f"{self.metric} sqlite build is already open")
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        self.temp_path = temp_path
        self.conn = _open_kv_accumulator(temp_path)
        return self.conn

    def upsert(self, stats_dict: dict) -> None:
        if self.conn is None:
            raise RuntimeError(f"{self.metric} sqlite build is not open")
        if stats_dict:
            _upsert_kv_stats(self.conn, stats_dict)

    def prepare(self, output_path: Path) -> _PreparedKvBuild:
        if self.conn is None or self.temp_path is None:
            raise RuntimeError(f"{self.metric} sqlite build is not open")
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        entries, games = self.conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(games), 0) FROM kv"
        ).fetchone()
        if int(entries) <= 0:
            raise RuntimeError(f"refusing to publish empty {self.metric} sqlite")

        # Single-pass rewrite into the runtime-compatible kv(key,value) shape.
        # Avoids the previous UPDATE-all-rows + CREATE/INSERT/DROP copy.
        self.conn.execute(
            "CREATE TABLE kv_final (key TEXT PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID"
        )
        cursor = self.conn.execute("SELECT key, wins, draws, games FROM kv")
        insert_sql = "INSERT INTO kv_final (key, value) VALUES (?, ?)"
        while True:
            rows = cursor.fetchmany(SQLITE_INSERT_BATCH)
            if not rows:
                break
            self.conn.executemany(
                insert_sql,
                (
                    (
                        str(key),
                        sqlite3.Binary(_encode_stats_blob([wins, draws, row_games])),
                    )
                    for key, wins, draws, row_games in rows
                ),
            )
        self.conn.execute("DROP TABLE kv")
        self.conn.execute("ALTER TABLE kv_final RENAME TO kv")
        _write_sqlite_meta(
            self.conn,
            source_name=output_path.name,
            source_size=0,
            source_mtime_ns=0,
            entries=int(entries),
        )
        self.conn.commit()
        integrity = self.conn.execute("PRAGMA quick_check").fetchone()
        if not integrity or integrity[0] != "ok":
            raise RuntimeError(f"{self.metric} staging sqlite failed quick_check: {integrity}")
        self.conn.close()
        self.conn = None

        self.publish_path = self.temp_path
        self.output_path = output_path
        self.prepared = _PreparedKvBuild(
            metric=self.metric,
            entries=int(entries),
            games=int(games),
            staging_path=self.temp_path,
            publish_path=self.temp_path,
        )
        return self.prepared

    def publish(self) -> _PreparedKvBuild:
        if self.prepared is None or self.publish_path is None or self.output_path is None:
            raise RuntimeError(f"{self.metric} sqlite build is not prepared")
        prepared = self.prepared
        os.replace(self.publish_path, self.output_path)
        self.publish_path = None
        prepared.staging_path.unlink(missing_ok=True)
        self.temp_path = None
        self.output_path = None
        self.prepared = None
        return prepared

    def rollback(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            finally:
                self.conn = None
        if self.publish_path is not None:
            self.publish_path.unlink(missing_ok=True)
        if self.temp_path is not None:
            self.temp_path.unlink(missing_ok=True)
        self.publish_path = None
        self.temp_path = None
        self.output_path = None
        self.prepared = None


def _prepare_and_publish_kv_builds(
    builds: list[_OwnedKvSqliteBuild], outputs: dict[str, Path]
) -> dict[str, _PreparedKvBuild]:
    prepared = {}
    for build in builds:
        prepared[build.metric] = build.prepare(outputs[build.metric])
    for build in builds:
        build.publish()
    return prepared


def _prepare_and_publish_enabled_builds(
    *,
    metric_names: tuple[str, ...],
    outputs: dict[str, Path],
    lane_build: _OwnedLaneSqliteBuild,
    kills_window_build: _OwnedKillsWindowSqliteBuild | None,
    kv_builds: dict[str, _OwnedKvSqliteBuild],
) -> dict[str, tuple[int, int]]:
    """Prepare every enabled artifact, then and only then publish any of them."""
    prepared_summary: dict[str, tuple[int, int]] = {}
    ordered_builds = []
    for metric in metric_names:
        if metric == "lane":
            build = lane_build
        elif metric == "kills_window":
            if kills_window_build is None:
                raise RuntimeError("kills_window metric enabled but no owned build provided")
            build = kills_window_build
        else:
            build = kv_builds[metric]
        result = build.prepare(outputs[metric])
        if isinstance(result, _PreparedKvBuild):
            prepared_summary[metric] = (result.entries, result.games)
        else:
            prepared_summary[metric] = (int(result[0]), int(result[1]))
        ordered_builds.append(build)

    for build in ordered_builds:
        build.publish()
    return prepared_summary


def _should_flush_metric_batches(
    metric_dicts: dict[str, dict | None],
    *,
    matches_since_flush: int,
    match_limit: int = FLUSH_MATCH_LIMIT,
    key_limit: int = FLUSH_KEY_LIMIT,
) -> bool:
    if matches_since_flush >= match_limit:
        return True
    total_keys = sum(len(data) for data in metric_dicts.values() if data is not None)
    return total_keys >= key_limit


def _flush_metric_batches(
    *,
    metric_dicts: dict[str, dict | None],
    kv_builds: dict[str, _OwnedKvSqliteBuild],
    lane_conn: sqlite3.Connection | None,
    kills_window_conn: sqlite3.Connection | None,
) -> None:
    """Persist the current bounded batch, then release all accumulator dicts."""
    lane_dict = metric_dicts.get("lane")
    if lane_conn is not None and lane_dict:
        _upsert_lane_stats(lane_conn, lane_dict)
    kills_dict = metric_dicts.get("kills_window")
    if kills_window_conn is not None and kills_dict:
        _upsert_kills_window_stats(kills_window_conn, kills_dict)
    for metric, build in kv_builds.items():
        data = metric_dicts.get(metric)
        if data:
            build.upsert(data)
    for data in metric_dicts.values():
        if data is not None:
            data.clear()
    gc.collect()


def _dump_stats_dict_to_sqlite(stats_dict: dict, sqlite_path: Path) -> tuple[int, int]:
    """Non-shard path: write stats dict directly to sqlite (kv + meta)."""
    sqlite_path = Path(sqlite_path)
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _unique_sqlite_temp_path(sqlite_path)
    conn = _open_sqlite_stats_writer(tmp_path)
    entries = 0
    total_games = 0
    batch: list = []
    try:
        for key, stats in stats_dict.items():
            wins, draws, games = _stats_values(stats)
            batch.append((str(key), sqlite3.Binary(_encode_stats_blob(stats))))
            entries += 1
            total_games += games
            if len(batch) >= SQLITE_INSERT_BATCH:
                _flush_sqlite_batch(conn, batch)
        _flush_sqlite_batch(conn, batch)
        _write_sqlite_meta(
            conn,
            source_name=sqlite_path.name,
            source_size=0,
            source_mtime_ns=0,
            entries=entries,
        )
        conn.commit()
    except Exception:
        conn.close()
        if tmp_path.exists():
            tmp_path.unlink()
        raise
    else:
        conn.close()
        _finalize_sqlite_db(tmp_path, sqlite_path)
    return entries, total_games


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
    conn.execute("PRAGMA page_size=8192")
    _apply_staging_sqlite_pragmas(conn)
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
    rows = []
    append = rows.append
    for key, stats in lane_dict.items():
        if isinstance(stats, list) and len(stats) >= 8:
            append(
                (
                    str(key),
                    int(stats[0]),
                    int(stats[1]),
                    int(stats[2]),
                    int(stats[3]),
                    int(stats[4]),
                    int(stats[5]),
                    float(stats[6]),
                    float(stats[7]),
                )
            )
        else:
            append((str(key), *_lane_stats_values(stats)))
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


def _prepare_lane_sqlite(
    conn: sqlite3.Connection, temp_path: Path, output_path: Path
) -> tuple[int, int, Path]:
    """Validate and close lane staging without touching the production path."""
    entries, games = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(games), 0) FROM stats"
    ).fetchone()
    if int(entries) <= 0:
        raise RuntimeError("refusing to publish empty lane sqlite")
    encode = orjson.dumps if orjson is not None else lambda value: json.dumps(value).encode()
    meta = {"format_version": 2, "backend": "sqlite_stats", "entries": int(entries)}
    for key, value in meta.items():
        conn.execute("INSERT INTO meta (key, value) VALUES (?, ?)", (key, sqlite3.Binary(encode(value))))
    conn.commit()
    check = conn.execute("PRAGMA quick_check").fetchone()
    if not check or check[0] != "ok":
        raise RuntimeError(f"lane staging sqlite failed quick_check: {check}")
    conn.close()
    return int(entries), int(games), Path(output_path)


def _open_kills_window_sqlite(temp_path: Path) -> sqlite3.Connection:
    """Create the direct-build multi-window kills database at a temp path."""
    cols = _kills_window_column_names()
    col_sql = ",\n            ".join(
        f"{name} {'REAL' if name.endswith(('_diff_sum', '_diff_sq_sum')) else 'INTEGER'} NOT NULL"
        for name in cols
    )
    conn = sqlite3.connect(str(temp_path))
    conn.execute("PRAGMA page_size=8192")
    _apply_staging_sqlite_pragmas(conn)
    conn.execute(
        f"""CREATE TABLE stats (
            key TEXT PRIMARY KEY,
            {col_sql}
        ) WITHOUT ROWID"""
    )
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value BLOB) WITHOUT ROWID")
    return conn


def _upsert_kills_window_stats(conn: sqlite3.Connection, kills_window_dict: dict) -> None:
    cols = _kills_window_column_names()
    expected = len(cols)
    placeholders = ", ".join("?" for _ in range(1 + expected))
    updates = ",\n            ".join(f"{name}={name}+excluded.{name}" for name in cols)
    rows = []
    append = rows.append
    for key, stats in kills_window_dict.items():
        if isinstance(stats, list):
            # Hot path: compact multi-window list counters written by analise_database.
            if len(stats) == expected:
                append((str(key), *stats))
            elif len(stats) > expected:
                append((str(key), *stats[:expected]))
            else:
                padded = list(stats) + [0] * (expected - len(stats))
                append((str(key), *padded))
        else:
            append((str(key), *_kills_window_stats_values(stats)))
    conn.executemany(
        f"""INSERT INTO stats VALUES ({placeholders})
        ON CONFLICT(key) DO UPDATE SET
            {updates}""",
        rows,
    )


def _finalize_kills_window_sqlite(conn: sqlite3.Connection, temp_path: Path, output_path: Path) -> tuple[int, int]:
    try:
        cols = _kills_window_column_names()
        game_cols = [name for name in cols if name.endswith("_games")]
        # Approximate match-records as max of window game counters.
        max_expr = "MAX(" + ", ".join(game_cols) + ")" if game_cols else "0"
        entries, games = conn.execute(
            f"SELECT COUNT(*), COALESCE(SUM({max_expr}), 0) FROM stats"
        ).fetchone()
        encode = orjson.dumps if orjson is not None else lambda value: json.dumps(value).encode()
        meta = {
            "format_version": 1,
            "backend": "sqlite_kills_window",
            "windows": list(analise_database_module.KILLS_WINDOW_LABELS),
            "entries": int(entries),
        }
        for key, value in meta.items():
            conn.execute("INSERT INTO meta (key, value) VALUES (?, ?)", (key, sqlite3.Binary(encode(value))))
        conn.commit()
    finally:
        conn.close()
    temp_path.replace(output_path)
    return int(entries), int(games)


def _prepare_kills_window_sqlite(
    conn: sqlite3.Connection, temp_path: Path, output_path: Path
) -> tuple[int, int, Path]:
    """Validate and close kills-window staging without publishing it."""
    cols = _kills_window_column_names()
    game_cols = [name for name in cols if name.endswith("_games")]
    max_expr = "MAX(" + ", ".join(game_cols) + ")" if game_cols else "0"
    entries, games = conn.execute(
        f"SELECT COUNT(*), COALESCE(SUM({max_expr}), 0) FROM stats"
    ).fetchone()
    if int(entries) <= 0:
        raise RuntimeError("refusing to publish empty kills_window sqlite")
    encode = orjson.dumps if orjson is not None else lambda value: json.dumps(value).encode()
    meta = {
        "format_version": 1,
        "backend": "sqlite_kills_window",
        "windows": list(analise_database_module.KILLS_WINDOW_LABELS),
        "entries": int(entries),
    }
    for key, value in meta.items():
        conn.execute("INSERT INTO meta (key, value) VALUES (?, ?)", (key, sqlite3.Binary(encode(value))))
    conn.commit()
    check = conn.execute("PRAGMA quick_check").fetchone()
    if not check or check[0] != "ok":
        raise RuntimeError(f"kills_window staging sqlite failed quick_check: {check}")
    conn.close()
    return int(entries), int(games), Path(output_path)


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


def _merge_partitioned_shards_to_json(
    partition_shards: list[list[Path]], output_path: Path
) -> tuple[int, int]:
    """Legacy: merge partition shards into a raw JSON file (one partition in RAM)."""
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


def _merge_partitioned_shards(
    partition_shards: list[list[Path]], output_sqlite_path: Path
) -> tuple[int, int]:
    """Merge partition shards directly into sqlite (one partition bucket in RAM)."""
    output_sqlite_path = Path(output_sqlite_path)
    output_sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_sqlite_path.with_suffix(output_sqlite_path.suffix + ".tmp")
    conn = _open_sqlite_stats_writer(tmp_path)
    batch: list = []
    total_keys = 0
    total_games = 0
    try:
        for part, shard_paths in enumerate(partition_shards):
            bucket: dict = {}
            for shard_path in shard_paths:
                if not shard_path.exists():
                    continue
                for key, stats in _iter_stats_object_items(shard_path):
                    _merge_stats_into(bucket, key, stats)

            for key, stats in bucket.items():
                wins, draws, games = _stats_values(stats)
                batch.append((str(key), sqlite3.Binary(_encode_stats_blob(stats))))
                total_keys += 1
                total_games += games
                if len(batch) >= SQLITE_INSERT_BATCH:
                    _flush_sqlite_batch(conn, batch)

            del bucket
            if part % 8 == 7:
                gc.collect()

        _flush_sqlite_batch(conn, batch)
        _write_sqlite_meta(
            conn,
            source_name=output_sqlite_path.name,
            source_size=0,
            source_mtime_ns=0,
            entries=total_keys,
        )
        conn.commit()
    except Exception:
        conn.close()
        if tmp_path.exists():
            tmp_path.unlink()
        raise
    else:
        conn.close()
        _finalize_sqlite_db(tmp_path, output_sqlite_path)
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
        "early-end": "early_end",
        "early_winner": "early_end",
        "early-winner": "early_end",
        "early_match_winner": "early_end",
        "kills": "kills_window",
        "kills-window": "kills_window",
        "kills_windows": "kills_window",
        "kill_window": "kills_window",
        "kills_window": "kills_window",
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


def _new_metric_dicts(metric_names: tuple[str, ...]) -> tuple[dict | None, dict | None, dict | None, dict | None, dict | None, dict | None]:
    return (
        {} if "lane" in metric_names else None,
        {} if "early" in metric_names else None,
        {} if "early_end" in metric_names else None,
        {} if "late" in metric_names else None,
        {} if "post_lane" in metric_names else None,
        {} if "kills_window" in metric_names else None,
    )


def _dict_len(data) -> int:
    return len(data) if data is not None else 0


def _reset_players_crawl_cursor(stats_dir: Path) -> None:
    """Сбросить курсор обхода игроков после успешной публикации словарей.

    `maps_research.get_maps_new` вычитает `processed_ids_to_graph.txt` из списка
    игроков (`ids_set - processed_graph_ids`), поэтому опрошенный игрок выпадал
    из всех будущих прогонов навсегда, и новые матчи известных игроков больше не
    собирались. Курсор осмысленно жить ровно один цикл «сбор -> пересборка»:
    словари построены, значит следующий сбор должен снова обойти всех.

    Прежний файл сохраняется рядом с меткой времени — на случай, если прогон
    сбора придётся повторить с того же места.
    """
    cursor = Path(stats_dir) / "processed_ids_to_graph.txt"
    if not cursor.exists():
        return
    try:
        backup = cursor.with_name(f"{cursor.name}.bak_{time.strftime('%Y%m%d_%H%M%S')}")
        shutil.copy2(cursor, backup)
        count = None
        try:
            count = len(json.loads(cursor.read_text(encoding="utf-8")))
        except Exception:
            pass
        cursor.write_text("[]", encoding="utf-8")
        suffix = f" ({count} игроков)" if count is not None else ""
        print(f"\n♻️  Курсор обхода игроков сброшен{suffix}: {cursor.name}")
        print(f"   Копия прежнего: {backup.name}")
    except Exception as exc:
        print(f"\n⚠️  Не удалось сбросить {cursor.name}: {exc}")


def _match_is_train_candidate(
    match_id, match, test_match_ids: set[str], seen_match_ids: set | None = None
) -> tuple[bool, str | None]:
    """Apply only structural/test-set gates; never filter source data by time.

    ``seen_match_ids`` guards against the same map being counted twice: shards
    may overlap after a merge, and a repeated match inflates every key it
    touches. The local corpus carried 466 708 repeats (23.9% of records) over
    the same 1 485 286 unique ids the deduplicated serv1 corpus has.
    """
    if not isinstance(match, dict):
        return False, "not_dict"
    if "players" not in match or len(match.get("players", [])) != 10:
        return False, "bad_players"
    if str(match_id) in test_match_ids:
        return False, "test_set"
    if seen_match_ids is not None:
        try:
            key = int(match_id)
        except (TypeError, ValueError):
            key = str(match_id)
        if key in seen_match_ids:
            return False, "duplicate_match_id"
        seen_match_ids.add(key)
    return True, None


def _build_sqlite_dicts(stats_dir: Path, metric_names: tuple) -> None:
    """Legacy: convert existing JSON stats dicts to SQLite3.

    Default path writes sqlite directly (no JSON). This helper is kept for
    optional JSON→sqlite conversion when JSON already exists and sqlite is missing.
    """
    for metric in metric_names:
        if metric in DIRECT_SQLITE_METRICS:
            # lane / kills_window are written as columnar stats tables directly.
            continue
        filename = OUTPUTS_BY_METRIC.get(metric)
        if not filename:
            continue
        json_path = stats_dir / filename
        sqlite_path = _sqlite_path_from_json_name(stats_dir, filename)
        if not json_path.exists():
            print(f"  ⚠️ {filename} не найден, пропускаем sqlite build")
            continue
        if sqlite_path.exists():
            print(f"  ✓ {sqlite_path.name} уже есть, skip legacy build")
            continue
        print(f"  🧱 Building {sqlite_path.name} from JSON...", end=" ", flush=True)
        started = time.monotonic()
        tmp_path = _unique_sqlite_temp_path(sqlite_path)
        conn = _open_sqlite_stats_writer(tmp_path)
        entries = 0
        batch: list = []
        try:
            for key, stats in _iter_stats_object_items(json_path):
                batch.append((str(key), sqlite3.Binary(_encode_stats_blob(stats))))
                entries += 1
                if len(batch) >= SQLITE_INSERT_BATCH:
                    _flush_sqlite_batch(conn, batch)
            _flush_sqlite_batch(conn, batch)

            source_stat = json_path.stat()
            _write_sqlite_meta(
                conn,
                source_name=json_path.name,
                source_size=source_stat.st_size,
                source_mtime_ns=source_stat.st_mtime_ns,
                entries=entries,
            )
            conn.commit()
        except Exception:
            conn.close()
            if tmp_path.exists():
                tmp_path.unlink()
            raise
        else:
            conn.close()
            _finalize_sqlite_db(tmp_path, sqlite_path)
        print(f"✓ {entries:,} keys, {time.monotonic() - started:.1f}s")


class _OwnedLaneSqliteBuild:
    """Own one unique lane staging DB and publish only after validation."""

    def __init__(self):
        self.conn: sqlite3.Connection | None = None
        self.temp_path: Path | None = None
        self.output_path: Path | None = None
        self.prepared: tuple[int, int] | None = None

    def open(self, temp_path: Path) -> sqlite3.Connection:
        if self.conn is not None or self.temp_path is not None:
            raise RuntimeError("lane sqlite build is already open")
        if temp_path.exists():
            raise FileExistsError(f"lane sqlite temp already exists: {temp_path}")
        self.temp_path = temp_path
        self.conn = _open_lane_sqlite(temp_path)
        return self.conn

    def prepare(self, output_path: Path) -> tuple[int, int]:
        if self.conn is None or self.temp_path is None:
            raise RuntimeError("lane sqlite build is not open")
        entries, games, self.output_path = _prepare_lane_sqlite(
            self.conn, self.temp_path, output_path
        )
        self.conn = None
        self.prepared = (entries, games)
        return self.prepared

    def publish(self) -> tuple[int, int]:
        if self.prepared is None or self.temp_path is None or self.output_path is None:
            raise RuntimeError("lane sqlite build is not prepared")
        os.replace(self.temp_path, self.output_path)
        result = self.prepared
        self.temp_path = None
        self.output_path = None
        self.prepared = None
        return result

    def finalize(self, output_path: Path) -> tuple[int, int]:
        """Backward-compatible prepare+publish for focused callers."""
        self.prepare(output_path)
        return self.publish()

    def rollback(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            finally:
                self.conn = None
        if self.temp_path is not None and self.temp_path.exists():
            self.temp_path.unlink()
        self.temp_path = None
        self.output_path = None
        self.prepared = None


class _OwnedKillsWindowSqliteBuild:
    """Own one kills-window staging DB and publish only after validation."""

    def __init__(self):
        self.conn: sqlite3.Connection | None = None
        self.temp_path: Path | None = None
        self.output_path: Path | None = None
        self.prepared: tuple[int, int] | None = None

    def open(self, temp_path: Path) -> sqlite3.Connection:
        if self.conn is not None or self.temp_path is not None:
            raise RuntimeError("kills_window sqlite build is already open")
        if temp_path.exists():
            raise FileExistsError(f"kills_window sqlite temp already exists: {temp_path}")
        self.temp_path = temp_path
        self.conn = _open_kills_window_sqlite(temp_path)
        return self.conn

    def prepare(self, output_path: Path) -> tuple[int, int]:
        if self.conn is None or self.temp_path is None:
            raise RuntimeError("kills_window sqlite build is not open")
        entries, games, self.output_path = _prepare_kills_window_sqlite(
            self.conn, self.temp_path, output_path
        )
        self.conn = None
        self.prepared = (entries, games)
        return self.prepared

    def publish(self) -> tuple[int, int]:
        if self.prepared is None or self.temp_path is None or self.output_path is None:
            raise RuntimeError("kills_window sqlite build is not prepared")
        os.replace(self.temp_path, self.output_path)
        result = self.prepared
        self.temp_path = None
        self.output_path = None
        self.prepared = None
        return result

    def finalize(self, output_path: Path) -> tuple[int, int]:
        """Backward-compatible prepare+publish for focused callers."""
        self.prepare(output_path)
        return self.publish()

    def rollback(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            finally:
                self.conn = None
        if self.temp_path is not None and self.temp_path.exists():
            self.temp_path.unlink()
        self.temp_path = None
        self.output_path = None
        self.prepared = None


def _main_impl(
    lane_build: _OwnedLaneSqliteBuild,
    kills_window_build: _OwnedKillsWindowSqliteBuild | None = None,
    kv_builds: dict[str, _OwnedKvSqliteBuild] | None = None,
) -> int:
    print("=" * 80)
    print("ПОСТРОЕНИЕ СТАТИСТИКИ (ИСКЛЮЧАЯ TEST SET)")
    print("=" * 80)
    print("✓ Train set: все матчи из базы → статистика")
    print("✓ Test set:  исключается из train (используется существующий)")
    print("✓ Strict position quality: включен check_match_quality(strict_lane_positions=True)")
    print("✓ Streaming JSON: ijson" if ijson is not None else "⚠️ Streaming JSON недоступен, fallback json.load")
    print(f"✓ Compact counters: {COUNTER_MODE}")
    print(f"✓ Output: sqlite-first" + (" + JSON (EXPLORE_WRITE_JSON=1)" if WRITE_JSON else " (JSON off)"))
    print("✓ Bounded memory: per-file batches → staging SQLite UPSERT")
    print("✓ Publication: all enabled SQLite validated before any production replace")
    print("=" * 80)

    _enable_compact_accumulators()

    metric_names = _enabled_metrics()
    json_dir = Path(os.getenv("EXPLORE_JSON_DIR", str(DEFAULT_JSON_DIR)))
    direct_metrics = set(DIRECT_SQLITE_METRICS)
    kv_metrics = tuple(metric for metric in metric_names if metric not in direct_metrics)
    test_set_path = Path(os.getenv("EXPLORE_TEST_SET_PATH", str(DEFAULT_TEST_SET_PATH)))
    stats_dir = Path(os.getenv("EXPLORE_STATS_DIR", str(DEFAULT_STATS_DIR)))
    max_matches = int(os.getenv("EXPLORE_MAX_MATCHES", "0") or "0")
    run_id = os.getenv("EXPLORE_RUN_ID", time.strftime("%Y%m%d_%H%M%S"))
    staging_dir = Path(
        os.getenv("EXPLORE_SHARD_DIR", str(stats_dir / "explore_database_staging" / run_id))
    )
    stats_dir.mkdir(parents=True, exist_ok=True)
    staging_dir.mkdir(parents=True, exist_ok=True)

    if kv_builds is None:
        kv_builds = {metric: _OwnedKvSqliteBuild(metric) for metric in kv_metrics}
    missing_kv = set(kv_metrics) - set(kv_builds)
    if missing_kv:
        raise RuntimeError(f"missing owned kv builds: {sorted(missing_kv)}")
    for metric in kv_metrics:
        kv_builds[metric].open(staging_dir / f"{metric}.staging.sqlite3")

    lane_conn = None
    lane_output_path = stats_dir / "lane_dict_raw.sqlite3"
    if "lane" in metric_names:
        lane_conn = lane_build.open(staging_dir / "lane.staging.sqlite3")
    kills_window_conn = None
    kills_window_output_path = stats_dir / "kills_window_dict_raw.sqlite3"
    if "kills_window" in metric_names:
        if kills_window_build is None:
            raise RuntimeError("kills_window metric enabled but no owned build provided")
        kills_window_conn = kills_window_build.open(
            staging_dir / "kills_window.staging.sqlite3"
        )

    print("\n[ШАГ 1/3] Загрузка test set для исключения...")
    test_match_ids = _load_test_match_ids(test_set_path)

    pub_files = _discover_pub_files(json_dir)
    if not pub_files:
        print(f"Файлы не найдены в {json_dir}!")
        print(f"Текущая директория скрипта: {BASE_DIR}")
        return 1

    print(f"\nНайдено файлов для обработки: {len(pub_files)}")
    print(f"Источник: {json_dir}")
    print("time_filter: disabled (all source matches are candidates)")
    print(f"enabled_metrics: {', '.join(metric_names)}")

    print("\n[ШАГ 2/3] Построение статистики на train set...")
    print(f"  Staging SQLite dir: {staging_dir}")
    lane_dict, early_dict, early_end_dict, late_dict, post_lane_dict, kills_window_dict = _new_metric_dicts(metric_names)

    train_processed = 0
    train_total = 0
    test_excluded = 0
    analysis_errors = 0
    skip_reasons = Counter()
    quality_reasons = Counter()
    # Один match_id учитывается один раз за прогон, даже если он встретился в
    # нескольких шардах (после merge такие пересечения бывают).
    seen_match_ids: set = set()
    started_at = time.monotonic()

    for idx, file in enumerate(pub_files, 1):
        file_started_at = time.monotonic()
        print(f"  [{idx}/{len(pub_files)}] Обработка {file.name}...", end=" ", flush=True)
        file_train = 0
        file_excluded = 0
        matches_since_flush = 0
        flush_count = 0
        lane_dict, early_dict, early_end_dict, late_dict, post_lane_dict, kills_window_dict = _new_metric_dicts(metric_names)
        metric_dicts = {
            "lane": lane_dict,
            "early": early_dict,
            "early_end": early_end_dict,
            "late": late_dict,
            "post_lane": post_lane_dict,
            "kills_window": kills_window_dict,
        }

        try:
            for match_id, match in _iter_json_object_items(file):
                ok, reason = _match_is_train_candidate(
                    match_id, match, test_match_ids, seen_match_ids
                )
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
                        kills_window_dict=kills_window_dict,
                        early_end_dict=early_end_dict,
                    )
                    train_processed += 1
                    file_train += 1
                except Exception:
                    analysis_errors += 1
                    continue

                train_total += 1
                matches_since_flush += 1
                if _should_flush_metric_batches(
                    metric_dicts,
                    matches_since_flush=matches_since_flush,
                ):
                    _flush_metric_batches(
                        metric_dicts=metric_dicts,
                        kv_builds={metric: kv_builds[metric] for metric in kv_metrics},
                        lane_conn=lane_conn,
                        kills_window_conn=kills_window_conn,
                    )
                    matches_since_flush = 0
                    flush_count += 1
                if PROGRESS_EVERY > 0 and train_total % PROGRESS_EVERY == 0:
                    elapsed = max(time.monotonic() - started_at, 1)
                    rate = train_total / elapsed
                    print(
                        f"\n    [{train_total:,}] "
                        f"Lane: {_dict_len(lane_dict):,}, EarlyNW: {_dict_len(early_dict):,}, "
                        f"EarlyEnd: {_dict_len(early_end_dict):,}, "
                        f"Late: {_dict_len(late_dict):,}, PostLane: {_dict_len(post_lane_dict):,}, "
                        f"KillsWin: {_dict_len(kills_window_dict):,}, "
                        f"RSS≈{_rss_mb():.0f}MB, {rate:.0f} maps/s",
                        end="",
                        flush=True,
                    )

                if max_matches > 0 and train_total >= max_matches:
                    break

            key_counts = {
                metric: _dict_len(metric_dicts.get(metric)) for metric in metric_names
            }
            flush_started = time.monotonic()
            _flush_metric_batches(
                metric_dicts=metric_dicts,
                kv_builds={metric: kv_builds[metric] for metric in kv_metrics},
                lane_conn=lane_conn,
                kills_window_conn=kills_window_conn,
            )
            # One source file is the transaction boundary. A parse failure
            # rolls back all metrics for that file; completed files stay staged.
            for conn in (
                lane_conn,
                kills_window_conn,
                *(kv_builds[metric].conn for metric in kv_metrics),
            ):
                if conn is not None:
                    conn.commit()
            flush_msg = (
                f" sqlite_flush:{time.monotonic() - flush_started:.1f}s "
                f"batches:{flush_count + 1} keys "
                + "/".join(
                    f"{metric}:{key_counts.get(metric, 0):,}" for metric in metric_names
                )
            )
            print(
                f" ✓ train:{file_train} excluded:{file_excluded} "
                f"time:{time.monotonic() - file_started_at:.1f}s RSS≈{_rss_mb():.0f}MB"
                f"{flush_msg}"
            )
        except Exception:
            # Roll back every mid-file SQLite flush as one unit. Completed
            # earlier files remain staged; production outputs stay untouched.
            for conn in (
                lane_conn,
                kills_window_conn,
                *(kv_builds[metric].conn for metric in kv_metrics),
            ):
                if conn is not None:
                    conn.rollback()
            for data in (
                lane_dict, early_dict, early_end_dict, late_dict,
                post_lane_dict, kills_window_dict,
            ):
                if data is not None:
                    data.clear()
            raise

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

    print("\n[ШАГ 3/3] Проверка staging и атомарная публикация...")
    outputs = {metric: _sqlite_path_for_metric(stats_dir, metric) for metric in metric_names}
    prepared_summary = _prepare_and_publish_enabled_builds(
        metric_names=metric_names,
        outputs=outputs,
        lane_build=lane_build,
        kills_window_build=kills_window_build,
        kv_builds=kv_builds,
    )
    lane_conn = None
    kills_window_conn = None

    print("\nСтатистика по словарям (train set):")
    for metric in metric_names:
        keys_count, games_count = prepared_summary[metric]
        label = LABELS_BY_METRIC[metric]
        print(
            f"  {label + ' dict:':15s}{keys_count:>6,} ключей, "
            f"{games_count:>7,} записей → {outputs[metric].name}"
        )
    if WRITE_JSON:
        print("  ⚠️ EXPLORE_WRITE_JSON=1 ignored by bounded SQLite rebuild")
    if KEEP_SHARDS:
        print(f"  Staging dir оставлен: {staging_dir}")
    else:
        shutil.rmtree(staging_dir, ignore_errors=True)
        print(f"  Staging dir очищен: {staging_dir}")

    _reset_players_crawl_cursor(stats_dir)

    print(f"\n{'=' * 80}")
    print("ЗАВЕРШЕНО!")
    print(f"{'=' * 80}")
    print(f"TRAIN SET: {train_processed:,} обработанных матчей")
    print(f"Test set исключен: {test_excluded:,} матчей")
    print(f"RSS peak≈{_rss_mb():.0f}MB")
    print(f"Primary output: *.sqlite3" + (" (+ optional JSON)" if WRITE_JSON else ""))
    print("Для валидации запустите: python check_metrics.py")
    print(f"{'=' * 80}\n")

    return 0


def main() -> int:
    lane_build = _OwnedLaneSqliteBuild()
    kills_window_build = _OwnedKillsWindowSqliteBuild()
    kv_builds = {
        metric: _OwnedKvSqliteBuild(metric)
        for metric in ALL_METRICS
        if metric not in DIRECT_SQLITE_METRICS
    }
    try:
        return _main_impl(
            lane_build,
            kills_window_build=kills_window_build,
            kv_builds=kv_builds,
        )
    finally:
        # Covers normal exceptions and BaseException subclasses such as
        # KeyboardInterrupt. Production files are untouched until prepare
        # succeeds for every enabled metric.
        lane_build.rollback()
        kills_window_build.rollback()
        for build in kv_builds.values():
            build.rollback()


if __name__ == "__main__":
    raise SystemExit(main())
