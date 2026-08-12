"""Снежный ком по командам и разведённые даты сбора.

Две вещи, каждая из которых способна тихо сжечь квоту Stratz или собрать не то:
  * дата: раньше про-ветка перезаписывала общую константу, и правка паблик-даты
    на про не влияла вовсе. Теперь у каждой ветки своя, со старым именем как
    фолбэком — проверяем и приоритет, и фолбэк;
  * посещённые команды: набор обязан переживать перезапуск, иначе каждая волна
    заново опрашивает те же команды.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import maps_research as MR  # noqa: E402


def test_dates_are_separate_for_pro_and_public(monkeypatch):
    import keys

    monkeypatch.setattr(keys, "start_date_time_pro", 111, raising=False)
    monkeypatch.setattr(keys, "start_date_time_publick", 222, raising=False)
    assert MR._start_date_for(pro=True) == 111
    assert MR._start_date_for(pro=False) == 222


def test_dates_fall_back_to_old_names(monkeypatch):
    """Старый keys.py на другой машине не должен ломать сбор."""
    import keys

    monkeypatch.delattr(keys, "start_date_time_pro", raising=False)
    monkeypatch.delattr(keys, "start_date_time_publick", raising=False)
    monkeypatch.setattr(keys, "start_date_time_736", 333, raising=False)
    monkeypatch.setattr(keys, "start_date_time", 444, raising=False)
    assert MR._start_date_for(pro=True) == 333
    assert MR._start_date_for(pro=False) == 444


def test_visited_teams_survive_restart(tmp_path):
    path = tmp_path / "visited_teams.json"
    assert MR._load_visited_teams(str(path)) == set()

    MR._save_visited_teams(str(path), {5, 3, 9})
    assert MR._load_visited_teams(str(path)) == {3, 5, 9}
    # запись атомарная: временного файла после неё не остаётся
    assert not (tmp_path / "visited_teams.json.tmp").exists()

    MR._save_visited_teams(str(path), {3, 5, 9, 12})
    assert MR._load_visited_teams(str(path)) == {3, 5, 9, 12}


def test_visited_teams_tolerate_broken_file(tmp_path):
    path = tmp_path / "visited_teams.json"
    path.write_text("не json", encoding="utf-8")
    # битый сайдкар не должен ронять сбор — начинаем с пустого набора
    assert MR._load_visited_teams(str(path)) == set()


def test_opponents_are_discovered_from_corpus(tmp_path):
    """Соперники из собранных матчей становятся очередью следующей волны."""
    (tmp_path / "7.41_part001.json").write_text(json.dumps({
        "1": {"radiantTeam": {"id": 100}, "direTeam": {"id": 200}},
        "2": {"radiantTeam": {"id": 100}, "direTeam": {"id": 300}},
        "3": {"radiantTeam": {}, "direTeam": {"id": 0}},          # мусор игнорируется
        "4": "не объект",
    }), encoding="utf-8")
    (tmp_path / "merge_patch_summary.json").write_text("{}", encoding="utf-8")

    assert MR._teams_from_corpus(str(tmp_path)) == {100, 200, 300}
    assert MR._teams_from_corpus(str(tmp_path / "нет-такой")) == set()


def test_outside_patch_matches_go_to_bucket_not_trash(tmp_path):
    """Матч вне известных патчей складывается в бакет, а не выбрасывается.

    Историческому сбору патч не важен — важен сам факт матча для рейтингов и
    сыгранности. Прежнее поведение выкинуло 59 649 матчей из 87 293 за одну волну.
    """
    import maps_research

    src = tmp_path / "src" / "temp_files"
    src.mkdir(parents=True)
    out = tmp_path / "parts"
    out.mkdir()
    # 1 000 000 000 = 2001 год, заведомо вне любых спецификаций патчей
    (src / "batch.txt").write_text(json.dumps({
        "9000000001": {"id": 9000000001, "startDateTime": 1_000_000_000},
        "9000000002": {"id": 9000000002, "startDateTime": 1_780_000_100},
    }), encoding="utf-8")

    maps_research.merge_temp_files_by_patch_streaming(
        tmp_path / "src", output_dir=out,
        patch_specs=[("7.41", 1_780_000_000, None)])

    summary = json.loads((out / "merge_patch_summary.json").read_text(encoding="utf-8"))
    assert summary["unique_matches_added"] == 2          # оба матча сохранены
    assert summary["outside_patch_skipped"] == 1         # счётчик всё равно считает
    bucket = list(out.glob(f"{maps_research.OUTSIDE_PATCH_BUCKET}_part*.json"))
    assert len(bucket) == 1
    assert "9000000001" in json.loads(bucket[0].read_text(encoding="utf-8"))


def test_scan_cache_skips_unchanged_files_but_rereads_changed(tmp_path):
    """Кеш скана: неизменившийся файл не перечитывается, изменившийся — читается.

    Полный скан всех part-файлов корректен, но линеен: на 147 частях он стоит
    минуты В КАЖДОМ слиянии. Кеш обязан сохранять корректность — то есть ловить
    и дописанный файл, и новый.
    """
    import maps_research

    src = tmp_path / "src" / "temp_files"
    src.mkdir(parents=True)
    out = tmp_path / "parts"
    out.mkdir()
    (out / "7.41_part001.json").write_text(
        json.dumps({"8000000001": {"id": 8000000001}}), encoding="utf-8")
    (src / "a.txt").write_text(json.dumps(
        {"8000000002": {"id": 8000000002, "startDateTime": 1_780_000_100}}), encoding="utf-8")

    specs = [("7.41", 1_780_000_000, None)]
    maps_research.merge_temp_files_by_patch_streaming(tmp_path / "src", output_dir=out,
                                                      patch_specs=specs)
    manifest = json.loads((out / "scan_manifest.json").read_text(encoding="utf-8"))
    assert "7.41_part001.json" in manifest
    assert 8000000001 in set(manifest["7.41_part001.json"][2])

    # тот же матч во втором заходе обязан быть отброшен как дубль
    (src / "b.txt").write_text(json.dumps(
        {"8000000002": {"id": 8000000002, "startDateTime": 1_780_000_100}}), encoding="utf-8")
    maps_research.merge_temp_files_by_patch_streaming(tmp_path / "src", output_dir=out,
                                                      patch_specs=specs)
    summary = json.loads((out / "merge_patch_summary.json").read_text(encoding="utf-8"))
    assert summary["duplicates_filtered"] >= 1
    assert summary["unique_matches_added"] == 0


def test_scan_cache_survives_broken_manifest(tmp_path):
    """Битый кеш не должен ронять слияние — просто пересканируем."""
    import maps_research

    out = tmp_path / "parts"
    out.mkdir()
    (out / "scan_manifest.json").write_text("не json", encoding="utf-8")
    assert maps_research._load_scan_manifest(out) == {}
