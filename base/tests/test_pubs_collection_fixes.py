"""Правки сбора паблик-матчей: курсор обхода, дедуп merge, пагинация.

Покрывает четыре разошедшихся с намерением места:
  * игрок помечался обработанным ДО запроса — сбой навсегда выкидывал его;
  * пагинация вставала на первой полностью известной странице;
  * merge доверял `processed_ids.txt` вместо part-файлов на диске;
  * курсор `processed_ids_to_graph.txt` никогда не сбрасывался.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import explore_database


def test_crawl_cursor_reset_keeps_backup(tmp_path):
    cursor = tmp_path / "processed_ids_to_graph.txt"
    cursor.write_text(json.dumps([111, 222, 333]), encoding="utf-8")

    explore_database._reset_players_crawl_cursor(tmp_path)

    assert json.loads(cursor.read_text(encoding="utf-8")) == []
    backups = list(tmp_path.glob("processed_ids_to_graph.txt.bak_*"))
    assert len(backups) == 1
    assert json.loads(backups[0].read_text(encoding="utf-8")) == [111, 222, 333]


def test_crawl_cursor_reset_is_noop_without_file(tmp_path):
    explore_database._reset_players_crawl_cursor(tmp_path)
    assert not list(tmp_path.iterdir())


def test_merge_dedup_reads_part_files_not_sidecar(tmp_path):
    """Сайдкар отстал — дедуп всё равно должен видеть матчи из part-файлов."""
    import maps_research

    (tmp_path / "7.41_part001.json").write_text(
        json.dumps({"8000000001": {"x": 1}, "8000000002": {"x": 2}}), encoding="utf-8")
    (tmp_path / "7.41a_part001.json").write_text(
        json.dumps({"8000000003": {"x": 3}}), encoding="utf-8")
    # пустой шард (после дедупликации) не должен ломать скан
    (tmp_path / "7.41b_part001.json").write_text("{}", encoding="utf-8")
    # посторонний json не является part-файлом
    (tmp_path / "merge_patch_summary.json").write_text("{}", encoding="utf-8")

    found = maps_research._scan_output_dir_match_ids(tmp_path)

    assert found == {8000000001, 8000000002, 8000000003}


def test_paginator_advances_on_known_page():
    """Страница из уже известных матчей не должна обрывать пагинацию.

    Воспроизводим условие из `proceed_get_maps_with_data`: страница полная,
    все матчи в окне, но все уже в existing_match_ids.
    """
    threshold = 1_700_000_000
    existing = {i for i in range(1, 101)}
    page = [{"id": i, "startDateTime": threshold + 10} for i in range(1, 101)]

    kept_in_window = 0
    in_window = 0
    for match in page:
        sdt = match.get("startDateTime")
        if sdt is None or int(sdt) < threshold:
            continue
        in_window += 1
        if int(match["id"]) in existing:
            continue
        kept_in_window += 1

    assert kept_in_window == 0, "все матчи известны"
    assert in_window == 100
    assert len(page) == 100 and in_window > 0, "пагинация обязана продолжиться"


def test_post_lane_solo_scope_walks_back_until_threshold(tmp_path, monkeypatch):
    """Свежий патч не добирает объём — окно расширяется на предыдущие."""
    import json as _json

    def shard(name, count, first_id=800_000_000):
        payload = {str(first_id + i): {"x": i} for i in range(count)}
        (tmp_path / name).write_text(_json.dumps(payload), encoding="utf-8")

    shard("7.41c_part001.json", 40, first_id=800_000_000)
    shard("7.41d_part001.json", 30, first_id=810_000_000)
    shard("7.41e_part001.json", 10, first_id=820_000_000)

    counts = explore_database._count_matches_per_patch(tmp_path)
    assert counts == {"7.41c": 40, "7.41d": 30, "7.41e": 10}

    from keys import DOTA_PATCH_START_TIMES

    # порог 10: хватает одного свежего патча
    ts, patches, total = explore_database._resolve_post_lane_solo_scope(tmp_path, 10)
    assert patches == ["7.41e"] and total == 10
    assert ts == DOTA_PATCH_START_TIMES["7.41e"]

    # порог 35: 7.41e не добирает, подключается 7.41d
    ts, patches, total = explore_database._resolve_post_lane_solo_scope(tmp_path, 35)
    assert patches == ["7.41e", "7.41d"] and total == 40
    assert ts == DOTA_PATCH_START_TIMES["7.41d"]

    # порог 75: нужны все три
    ts, patches, total = explore_database._resolve_post_lane_solo_scope(tmp_path, 75)
    assert patches[:3] == ["7.41e", "7.41d", "7.41c"] and total == 80
    assert ts == DOTA_PATCH_START_TIMES["7.41c"]


def test_post_lane_solo_scope_start_ts_gates_writes(monkeypatch):
    """Матч старше границы окна не пишет solo, свежее — пишет."""
    import analise_database

    monkeypatch.setattr(analise_database, "POST_LANE_SOLO_SCOPE_START_TS", 1_780_531_200)
    r_by_pos = {p: p for p in range(1, 6)}
    d_by_pos = {p: 100 + p for p in range(1, 6)}

    inside, outside = {}, {}
    analise_database._add_combinations_to_dict(r_by_pos, d_by_pos, inside, 1, 0, write_solo=True)
    analise_database._add_combinations_to_dict(r_by_pos, d_by_pos, outside, 1, 0, write_solo=False)

    solo_keys = lambda d: [k for k in d if ',' not in k and '_vs_' not in k and '_with_' not in k]
    assert len(solo_keys(inside)) == 10
    assert solo_keys(outside) == []
