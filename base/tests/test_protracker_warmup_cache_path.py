"""Прогрев должен проверять свежесть в ТОМ ЖЕ каталоге, куда пишет парсер.

Копия пути в cyberscore_try разъехалась с `dota2protracker.CACHE_DIR`: она
смотрела в корень репозитория, где остался осиротевший набор кэшей от 25.07.
Итог — `skipped_fresh=0` на прогреве 26.07 при 127 обойдённых героях, то есть
идемпотентность прогрева не работала.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
ROOT = BASE_DIR.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import cyberscore_try as runtime  # noqa: E402


def _write_cache(directory: Path, hero_file: str, timestamp: float) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / hero_file).write_text(
        json.dumps({"timestamp": timestamp, "matchups": {"axe": {}}}), encoding="utf-8"
    )


def test_freshness_reads_the_parser_cache_dir(tmp_path, monkeypatch) -> None:
    cache_dir = tmp_path / "base" / "hero_dota2protracker_data"
    monkeypatch.setattr(runtime._dota2protracker_module, "CACHE_DIR", str(cache_dir))
    _write_cache(cache_dir, "drow_ranger.json", time.time())

    assert runtime._protracker_cache_dir() == cache_dir
    assert runtime._protracker_cache_is_fresh("Drow Ranger") is True


def test_orphan_copy_in_the_repo_root_is_not_consulted(tmp_path, monkeypatch) -> None:
    """Файл в старом месте не должен выдаваться за свежий кэш."""
    cache_dir = tmp_path / "base" / "hero_dota2protracker_data"
    cache_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(runtime._dota2protracker_module, "CACHE_DIR", str(cache_dir))
    monkeypatch.setattr(runtime, "PROJECT_ROOT", tmp_path, raising=False)
    _write_cache(tmp_path / "hero_dota2protracker_data", "drow_ranger.json", time.time())

    assert runtime._protracker_cache_is_fresh("Drow Ranger") is False


def test_yesterdays_cache_is_stale(tmp_path, monkeypatch) -> None:
    cache_dir = tmp_path / "base" / "hero_dota2protracker_data"
    monkeypatch.setattr(runtime._dota2protracker_module, "CACHE_DIR", str(cache_dir))
    _write_cache(cache_dir, "drow_ranger.json", time.time() - 86400)

    assert runtime._protracker_cache_is_fresh("Drow Ranger") is False


def test_missing_file_is_not_fresh(tmp_path, monkeypatch) -> None:
    cache_dir = tmp_path / "base" / "hero_dota2protracker_data"
    cache_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(runtime._dota2protracker_module, "CACHE_DIR", str(cache_dir))

    assert runtime._protracker_cache_is_fresh("No Such Hero") is False
