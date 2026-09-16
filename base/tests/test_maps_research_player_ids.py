"""Персистентный набор pub player id и monotonic part numbering.

Контекст: serv1 после 16.09.2026 больше не держит 36 ГБ part-файлов корпуса
(bets_data/analise_pub_matches/json_parts_split_from_object) — только
pub_player_steam_ids.json, processed_ids.txt и part_counters.json. Покрывает:
  * save/load round trip pub_player_steam_ids.json (атомарность, сортировка);
  * get_pubs() режимы PUBS_IDS_MODE (file/auto) и не лезет в корпус, когда
    файл id уже есть;
  * PUBS_MAX_PLAYERS вычитает уже обойдённых игроков перед срезом;
  * monotonic part-нумерация переживает удаление part-файлов на serv1
    (part_counters.json);
  * run_full_recrawl.start_date_time() падает обратно на
    last_crawl_completed_utc, когда part-файлов на машине нет.
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import maps_research  # noqa: E402


def _load_run_full_recrawl_module():
    """Загружает runtime/experiments/pubs-rebuild/run_full_recrawl.py как модуль.

    Файл не лежит в base/ (это раннер-скрипт, не пакет), поэтому обычный
    import недоступен — используем importlib по пути. Модуль сам делает
    `import maps_research as mr`, который переиспользует уже загруженный
    sys.modules['maps_research'] (тот же объект, что и в этом тесте), поэтому
    monkeypatch на `maps_research` виден и внутри раннера.
    """
    project_root = Path(__file__).resolve().parents[2]
    path = project_root / "runtime" / "experiments" / "pubs-rebuild" / "run_full_recrawl.py"
    spec = importlib.util.spec_from_file_location("run_full_recrawl_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# 1) save/load round trip
# ---------------------------------------------------------------------------

def test_save_load_round_trip_sorted_atomic(tmp_path):
    path = tmp_path / "pub_player_steam_ids.json"
    maps_research.save_pub_player_ids({30, 10, 20}, path=path, source={"kind": "test"})

    tmp_sidecar = path.with_suffix(path.suffix + ".tmp")
    assert not tmp_sidecar.exists()
    assert path.exists()

    raw = json.loads(path.read_text(encoding="utf-8"))
    assert raw["schema"] == maps_research.PUB_PLAYER_IDS_SCHEMA
    assert raw["ids"] == [10, 20, 30]
    assert raw["count"] == 3
    assert raw["source"] == {"kind": "test"}

    loaded = maps_research.load_pub_player_ids(path)
    assert loaded is not None
    ids, meta = loaded
    assert ids == {10, 20, 30}
    assert meta["count"] == 3


def test_save_preserves_last_crawl_completed_utc_unless_overridden(tmp_path):
    path = tmp_path / "ids.json"
    maps_research.save_pub_player_ids(
        {1, 2}, path=path, last_crawl_completed_utc="2026-09-01T00:00:00+00:00",
    )
    # Второе сохранение (например, от скана корпуса) не задаёт last_crawl —
    # отметка последнего ЗАВЕРШЁННОГО обхода не должна теряться.
    maps_research.save_pub_player_ids({1, 2, 3}, path=path)

    _ids, meta = maps_research.load_pub_player_ids(path)
    assert meta["last_crawl_completed_utc"] == "2026-09-01T00:00:00+00:00"

    # Явно заданное значение всё же перезаписывает предыдущее.
    maps_research.save_pub_player_ids(
        {1, 2, 3}, path=path, last_crawl_completed_utc="2026-09-15T00:00:00+00:00",
    )
    _ids, meta2 = maps_research.load_pub_player_ids(path)
    assert meta2["last_crawl_completed_utc"] == "2026-09-15T00:00:00+00:00"


def test_load_missing_file_returns_none(tmp_path):
    assert maps_research.load_pub_player_ids(tmp_path / "nope.json") is None


def test_load_corrupted_file_returns_none_not_raise(tmp_path):
    path = tmp_path / "broken.json"
    path.write_text("{not valid json", encoding="utf-8")
    assert maps_research.load_pub_player_ids(path) is None


# ---------------------------------------------------------------------------
# 2) get_pubs mode=file: файл используется, скан корпуса не запускается
# ---------------------------------------------------------------------------

def test_get_pubs_mode_file_never_scans_corpus(tmp_path, monkeypatch):
    ids_path = tmp_path / "ids.json"
    maps_research.save_pub_player_ids({111, 222, 333}, path=ids_path)

    monkeypatch.setattr(maps_research, "PUBS_PLAYER_IDS_FILE", ids_path)
    monkeypatch.setattr(maps_research, "ANALYSE_PUB_DIR", tmp_path)
    monkeypatch.setenv("PUBS_IDS_MODE", "file")
    monkeypatch.setenv("PUBS_MAX_PLAYERS", "0")

    def _must_not_run(*args, **kwargs):
        raise AssertionError("build_pub_player_ids_from_corpus must not run in mode=file")

    monkeypatch.setattr(maps_research, "build_pub_player_ids_from_corpus", _must_not_run)

    captured = {}

    def fake_get_maps_new(**kwargs):
        captured.update(kwargs)
        return "sentinel"

    monkeypatch.setattr(maps_research, "get_maps_new", fake_get_maps_new)
    monkeypatch.setattr(maps_research.asyncio, "run", lambda value: value)

    maps_research.get_pubs()

    assert captured["ids"] == {111, 222, 333}


def test_get_pubs_mode_file_raises_when_file_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(maps_research, "PUBS_PLAYER_IDS_FILE", tmp_path / "missing.json")
    monkeypatch.setenv("PUBS_IDS_MODE", "file")

    def _must_not_run(*args, **kwargs):
        raise AssertionError("corpus scan must not run when mode=file reports missing file")

    monkeypatch.setattr(maps_research, "build_pub_player_ids_from_corpus", _must_not_run)

    import pytest
    with pytest.raises(FileNotFoundError):
        maps_research.get_pubs()


# ---------------------------------------------------------------------------
# 3) get_pubs mode=auto без файла: сканирует корпус и пишет файл
# ---------------------------------------------------------------------------

def test_get_pubs_mode_auto_scans_corpus_and_persists_when_file_missing(tmp_path, monkeypatch):
    ids_path = tmp_path / "ids.json"
    assert not ids_path.exists()

    monkeypatch.setattr(maps_research, "PUBS_PLAYER_IDS_FILE", ids_path)
    monkeypatch.setattr(maps_research, "ANALYSE_PUB_DIR", tmp_path)
    monkeypatch.delenv("PUBS_IDS_MODE", raising=False)
    monkeypatch.setenv("PUBS_MAX_PLAYERS", "0")

    def fake_build(json_dir=None):
        return {5, 6, 7}, {"corpus_dir": "fake-corpus"}

    monkeypatch.setattr(maps_research, "build_pub_player_ids_from_corpus", fake_build)

    captured = {}

    def fake_get_maps_new(**kwargs):
        captured.update(kwargs)
        return "sentinel"

    monkeypatch.setattr(maps_research, "get_maps_new", fake_get_maps_new)
    monkeypatch.setattr(maps_research.asyncio, "run", lambda value: value)

    maps_research.get_pubs()

    assert captured["ids"] == {5, 6, 7}
    assert ids_path.exists()
    loaded = maps_research.load_pub_player_ids(ids_path)
    assert loaded is not None
    saved_ids, meta = loaded
    assert saved_ids == {5, 6, 7}
    assert meta["source"] == {"corpus_dir": "fake-corpus"}


# ---------------------------------------------------------------------------
# 4) PUBS_MAX_PLAYERS исключает уже обойдённых игроков перед срезом
# ---------------------------------------------------------------------------

def test_get_pubs_max_players_cap_excludes_already_graphed(tmp_path, monkeypatch):
    ids_path = tmp_path / "ids.json"
    maps_research.save_pub_player_ids({1, 2, 3, 4, 5}, path=ids_path)

    monkeypatch.setattr(maps_research, "PUBS_PLAYER_IDS_FILE", ids_path)
    monkeypatch.setattr(maps_research, "ANALYSE_PUB_DIR", tmp_path)
    monkeypatch.setenv("PUBS_IDS_MODE", "file")
    monkeypatch.setenv("PUBS_MAX_PLAYERS", "2")

    (tmp_path / "processed_ids_to_graph.txt").write_text(json.dumps([1, 2]), encoding="utf-8")

    captured = {}

    def fake_get_maps_new(**kwargs):
        captured.update(kwargs)
        return "sentinel"

    monkeypatch.setattr(maps_research, "get_maps_new", fake_get_maps_new)
    monkeypatch.setattr(maps_research.asyncio, "run", lambda value: value)

    maps_research.get_pubs()

    # 1 и 2 уже обойдены -> остаток отсортирован [3, 4, 5], первые 2 -> {3, 4}.
    assert captured["ids"] == {3, 4}


# ---------------------------------------------------------------------------
# 5) part_counters.json переживает удаление part-файлов на serv1
# ---------------------------------------------------------------------------

def test_merge_advances_part_number_past_persisted_counter(tmp_path):
    mkdir = tmp_path / "run"
    (mkdir / "temp_files").mkdir(parents=True)
    output_dir = mkdir / "json_parts_split_from_object"
    output_dir.mkdir(parents=True)
    (output_dir / "part_counters.json").write_text(json.dumps({"7.41e": 33}), encoding="utf-8")

    start_ts = None
    for patch_name, patch_start, _patch_end in maps_research.DOTA_PATCH_SPECS:
        if patch_name == "7.41e":
            start_ts = patch_start
            break
    assert start_ts is not None, "7.41e должен быть в DOTA_PATCH_SPECS"
    match_ts = int(start_ts) + 100

    match_id = 9_123_456_789
    (mkdir / "temp_files" / "batch.txt").write_text(
        json.dumps({str(match_id): {"id": match_id, "startDateTime": match_ts}}),
        encoding="utf-8",
    )

    maps_research.merge_temp_files_by_patch_streaming(mkdir=str(mkdir), output_dir=output_dir)

    assert (output_dir / "7.41e_part034.json").exists()
    assert not list(output_dir.glob("7.41e_part033.json"))
    counters = json.loads((output_dir / "part_counters.json").read_text(encoding="utf-8"))
    assert counters["7.41e"] == 34


def test_next_part_numbers_takes_max_of_disk_and_counter(tmp_path):
    # Диск впереди счётчика (счётчик отстал / файл только что появился).
    (tmp_path / "7.41e_part005.json").write_text("{}", encoding="utf-8")
    result = maps_research._next_part_numbers(tmp_path, ["7.41e"], {"7.41e": 2})
    assert result == {"7.41e": 6}

    # Счётчик впереди диска (part-файлы удалены на serv1 после переноса на Mac).
    result2 = maps_research._next_part_numbers(tmp_path, ["7.41e"], {"7.41e": 40})
    assert result2 == {"7.41e": 41}

    # Ни диска, ни счётчика — начинаем с 1.
    result3 = maps_research._next_part_numbers(tmp_path, ["7.40"], {})
    assert result3 == {"7.40": 1}


def test_part_counters_load_save_round_trip(tmp_path):
    maps_research._save_part_counters(tmp_path, {"7.41e": 34, "7.40": 12})
    loaded = maps_research._load_part_counters(tmp_path)
    assert loaded == {"7.41e": 34, "7.40": 12}
    assert not (tmp_path / "part_counters.json.tmp").exists()


# ---------------------------------------------------------------------------
# 6) run_full_recrawl: окно из pub_player_steam_ids.json, когда part-файлов нет
# ---------------------------------------------------------------------------

def test_run_full_recrawl_start_date_time_falls_back_to_last_crawl(tmp_path, monkeypatch):
    module = _load_run_full_recrawl_module()

    ids_path = tmp_path / "ids.json"
    maps_research.save_pub_player_ids(
        {1}, path=ids_path, last_crawl_completed_utc="2026-09-10T00:00:00+00:00",
    )
    monkeypatch.setattr(maps_research, "PUBS_PLAYER_IDS_FILE", ids_path)
    monkeypatch.delenv("START_DATE_TIME", raising=False)

    empty_source_dir = tmp_path / "empty_source"
    empty_source_dir.mkdir()

    value = module.start_date_time(empty_source_dir)

    expected_day = dt.datetime(2026, 9, 9, tzinfo=dt.timezone.utc)
    assert value == int(expected_day.timestamp())


def test_run_full_recrawl_start_date_time_falls_back_to_source_provenance(tmp_path, monkeypatch):
    module = _load_run_full_recrawl_module()

    ids_path = tmp_path / "ids.json"
    maps_research.save_pub_player_ids(
        {1}, path=ids_path,
        source={"newest_source_mtime_utc": "2026-08-20T00:00:00+00:00"},
    )
    monkeypatch.setattr(maps_research, "PUBS_PLAYER_IDS_FILE", ids_path)
    monkeypatch.delenv("START_DATE_TIME", raising=False)

    empty_source_dir = tmp_path / "empty_source"
    empty_source_dir.mkdir()

    value = module.start_date_time(empty_source_dir)

    expected_day = dt.datetime(2026, 8, 19, tzinfo=dt.timezone.utc)
    assert value == int(expected_day.timestamp())


def test_run_full_recrawl_start_date_time_exits_without_any_source(tmp_path, monkeypatch):
    module = _load_run_full_recrawl_module()

    monkeypatch.setattr(maps_research, "PUBS_PLAYER_IDS_FILE", tmp_path / "missing.json")
    monkeypatch.delenv("START_DATE_TIME", raising=False)

    empty_source_dir = tmp_path / "empty_source"
    empty_source_dir.mkdir()

    import pytest
    with pytest.raises(SystemExit):
        module.start_date_time(empty_source_dir)
