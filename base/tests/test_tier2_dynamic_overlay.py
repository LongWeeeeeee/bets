"""Динамический tier2-onboarding: overlay вместо дописывания в исходник.

Находка инспекции 02.09.2026: живой рантайм дописывал каждую новую tier2-команду
Python-блоком прямо в отслеживаемый `base/id_to_names.py` (611 блоков на serv1) —
файл вечно грязный, и любой pull, трогающий его, упирался в конфликт. Теперь
записи уезжают в JSON-overlay (`base/tier_dynamic_overlay.py`), а legacy-блоки
переносятся оттуда инструментом `base/tools/migrate_tier2_onboarding.py`.

conftest изолирует overlay через `TIER2_DYNAMIC_ONBOARDING_PATH` и сбрасывает
флаг однократной загрузки, поэтому эти тесты не трогают боевое состояние.
"""
from __future__ import annotations

import copy
import json
import sys
import types
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import id_to_names  # noqa: E402
import tier_dynamic_overlay as overlay_mod  # noqa: E402
import cyberscore_try as runtime  # noqa: E402


LEGACY_SAMPLE = '''tier_two_teams = {'existing': 1}

# auto-added by cyberscore_try (dynamic tier2 onboarding)
try:
    tier_two_teams['alohasquad'] = 10028056
except Exception:
    pass

# auto-added by cyberscore_try (dynamic tier2 onboarding)
try:
    _key = 'ironwing'
    _team_id = 10150413
    _existing = tier_two_teams.get(_key)
    if isinstance(_existing, set):
        _existing.add(_team_id)
    elif _existing is None:
        tier_two_teams[_key] = _team_id
    elif _existing != _team_id:
        try:
            tier_two_teams[_key] = {int(_existing), _team_id}
        except Exception:
            tier_two_teams[_key] = _team_id
except Exception:
    pass

# auto-added by cyberscore_try (dynamic tier2 onboarding)
try:
    _key = 'ironwing'
    _team_id = 8291895
    _existing = tier_two_teams.get(_key)
    if isinstance(_existing, set):
        _existing.add(_team_id)
except Exception:
    pass
'''


@pytest.fixture
def isolated_tier_dicts(monkeypatch):
    """Тесты мутируют КОПИЮ tier_two_teams; боевой dict остаётся нетронутым."""
    snapshot = copy.deepcopy(id_to_names.tier_two_teams)
    monkeypatch.setattr(id_to_names, "tier_two_teams", snapshot)
    auto_ids_before = set(runtime._auto_added_tier2_ids)
    yield snapshot
    runtime._auto_added_tier2_ids.difference_update(
        set(runtime._auto_added_tier2_ids) - auto_ids_before
    )


def test_harvest_legacy_blocks_reads_both_generations(tmp_path) -> None:
    src = tmp_path / "id_to_names.py"
    src.write_text(LEGACY_SAMPLE, encoding="utf-8")

    harvested = overlay_mod.harvest_legacy_blocks(src)

    assert harvested == {
        "alohasquad": [10028056],
        "ironwing": [10150413, 8291895],
    }


def test_harvest_legacy_blocks_missing_file_is_empty(tmp_path) -> None:
    assert overlay_mod.harvest_legacy_blocks(tmp_path / "нет_такого.py") == {}


def test_migrate_legacy_is_idempotent(tmp_path) -> None:
    src = tmp_path / "id_to_names.py"
    src.write_text(LEGACY_SAMPLE, encoding="utf-8")
    overlay = tmp_path / "overlay.json"

    assert overlay_mod.migrate_legacy(src, overlay) == 3
    assert overlay_mod.migrate_legacy(src, overlay) == 0
    assert overlay_mod.load_entries(overlay) == {
        "alohasquad": [10028056],
        "ironwing": [10150413, 8291895],
    }


def test_apply_entries_merges_like_legacy_blocks() -> None:
    fake = types.SimpleNamespace(
        tier_two_teams={
            "same": 111,
            "renamed": 222,
            "multi": {333, 444},
            "broken": "не число",
        }
    )
    entries = {
        "same": [111],
        "renamed": [555],
        "multi": [666],
        "broken": [777],
        "fresh": [888],
        "freshpair": [999, 1000],
    }

    applied = overlay_mod.apply_entries(fake, entries)

    assert applied == 6
    assert fake.tier_two_teams["same"] == 111
    assert fake.tier_two_teams["renamed"] == {222, 555}
    assert fake.tier_two_teams["multi"] == {333, 444, 666}
    assert fake.tier_two_teams["broken"] == 777
    assert fake.tier_two_teams["fresh"] == 888
    assert fake.tier_two_teams["freshpair"] == {999, 1000}


def test_upsert_entry_roundtrip_and_duplicate(tmp_path) -> None:
    path = tmp_path / "overlay.json"

    assert overlay_mod.upsert_entry(path, "team", 1) is True
    assert overlay_mod.upsert_entry(path, "team", 2) is True
    assert overlay_mod.upsert_entry(path, "team", 2) is False
    assert json.loads(path.read_text(encoding="utf-8")) == {"team": [1, 2]}
    # атомарная запись не оставляет мусорного .tmp
    assert not (tmp_path / "overlay.json.tmp").exists()


def test_load_entries_rejects_corrupt_file(tmp_path) -> None:
    """Битый overlay — ошибка, а не молчаливая потеря записей onboarding'а."""
    path = tmp_path / "overlay.json"
    path.write_text("{не json", encoding="utf-8")

    with pytest.raises(json.JSONDecodeError):
        overlay_mod.load_entries(path)


def test_append_team_to_tier2_writes_overlay_not_source(isolated_tier_dicts) -> None:
    src_path = BASE_DIR / "id_to_names.py"
    src_before = src_path.read_bytes()

    team_name = "Overlay Probe Team 918273"
    team_id = 987654321
    expected_key = runtime._normalize_tier_team_key(team_name, team_id)

    added, key = runtime._append_team_to_tier2_file(team_name, team_id)

    assert added is True
    assert key == expected_key
    overlay_file = overlay_mod.overlay_path(runtime.BASE_DIR)
    assert json.loads(overlay_file.read_text(encoding="utf-8")) == {
        expected_key: [team_id]
    }
    assert isolated_tier_dicts[expected_key] == team_id
    # ГЛАВНЫЙ ассерт находки: отслеживаемый исходник больше не дописывается.
    assert src_path.read_bytes() == src_before

    added2, key2 = runtime._append_team_to_tier2_file(team_name, 111222333)
    assert added2 is True
    assert key2 == expected_key
    assert isolated_tier_dicts[expected_key] == {team_id, 111222333}
    assert json.loads(overlay_file.read_text(encoding="utf-8"))[expected_key] == [
        team_id,
        111222333,
    ]

    added3, reason3 = runtime._append_team_to_tier2_file(team_name, team_id)
    assert added3 is False
    assert reason3 == "already_known"
    assert src_path.read_bytes() == src_before


def test_get_team_tier_sees_overlay_entries(isolated_tier_dicts) -> None:
    probe_id = 987654321
    known: set[int] = set()
    for value in isolated_tier_dicts.values():
        if isinstance(value, set):
            known.update(value)
        else:
            known.add(value)
    for value in id_to_names.tier_one_teams.values():
        if isinstance(value, set):
            known.update(value)
        else:
            known.add(value)
    assert probe_id not in known, "тестовый id уже известен — выберите другой"

    overlay_file = overlay_mod.overlay_path(runtime.BASE_DIR)
    overlay_mod.save_entries(overlay_file, {"overlayprobe": [probe_id]})

    assert runtime._get_team_tier(probe_id) == 2
    assert isolated_tier_dicts["overlayprobe"] == probe_id


def test_find_known_ids_by_name_sees_overlay(isolated_tier_dicts) -> None:
    name_key = runtime._normalize_tier_team_name_only("Overlay Probe Name")
    overlay_file = overlay_mod.overlay_path(runtime.BASE_DIR)
    overlay_mod.save_entries(overlay_file, {name_key: [123456789]})

    assert runtime._find_known_team_ids_by_name("Overlay Probe Name") == {123456789}
