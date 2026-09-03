"""Overlay и дельта ELO: запись поверх массивов и parity с словарной моделью.

Главный тест здесь — `test_parity_dict_model_vs_overlay_model_on_same_map`:
одну и ту же карту применяют две модели, собранные из ОДНОГО состояния, —
словарная (как сегодня в живом пути) и массивная с пишущим слоем. Рейтинги
обязаны совпасть побитово: речь о числах, из которых считается
`hybrid_strength`, а он уходит в предматчевую модель (признак с корреляцией 1.0
к обучению), поэтому расхождение снаружи не видно.

Второе, что закреплено: дельта переживает перезапуск (collect -> save -> load ->
apply даёт те же числа) и отвергается при чужой базе — ровно те же охранники,
что у рантайм-состояния.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ELO import array_model, state_overlay  # noqa: E402
from ELO.domain import LeagueTier, MatchRecord  # noqa: E402
from ELO.models import HybridPlayerRosterEloModel  # noqa: E402

STATE = {
    "config": {"initial_rating": 1500.0, "elo_scale": 400.0, "k_global": 24.0},
    "current_patch_key": "7.41e",
    "side_bias": {"TIER1": 6.8, "TIER2": 3.1, "TIER3": 0.0},
    "roster_tracker": {},
    "player_global": {str(p): 1500.0 + i for i, p in enumerate(
        [11, 12, 13, 14, 15, 21, 22, 23, 24, 25])},
    "player_global_last_seen_ts": {str(p): 1788200000 for p in
                                   [11, 12, 13, 14, 15, 21, 22, 23, 24, 25]},
    "lineup_match_counts": {"org:tundra::lineup:aaa": 2, "org:og::lineup:bbb": 1},
    "player_local": {"TIER1": {str(p): 1500.0 for p in [11, 12, 13, 14, 15,
                                                        21, 22, 23, 24, 25]}},
    "player_local_last_seen_ts": {"TIER1": {"11": 1788200000}},
    "player_role_local": {"TIER1": {"11|POSITION_1": 1500.0, "21|POSITION_1": 1500.0}},
    "player_role_local_last_seen_ts": {"TIER1": {"11|POSITION_1": 1788200000}},
    "roster_ratings": {"TIER1": {"org:tundra::roster:1": 1600.0,
                                 "org:og::roster:2": 1580.0}},
    "roster_last_seen_ts": {"TIER1": {"org:tundra::roster:1": 1788200000}},
    "roster_match_counts": {"TIER1": {"org:tundra::roster:1": 12,
                                      "org:og::roster:2": 9}},
    "player_current_org": {"11": "org:tundra", "21": "org:og"},
    "player_current_org_matches": {"11": {"org:tundra": 30}, "21": {"org:og": 22}},
}

RAD = (11, 12, 13, 14, 15)
DIRE = (21, 22, 23, 24, 25)
POSITIONS = ("POSITION_1", "POSITION_2", "POSITION_3", "POSITION_4", "POSITION_5")


def _match(mid: int = 101) -> MatchRecord:
    return MatchRecord(
        match_id=mid, timestamp=1788400000, radiant_win=True,
        radiant_team_id=8291895, radiant_team_name="Tundra",
        dire_team_id=2586976, dire_team_name="OG",
        radiant_player_ids=RAD, dire_player_ids=DIRE,
        league_id=19944, league_name="Test League", source_league_tier="TIER1",
        series_id=1, series_type="BEST_OF_ONE",
        radiant_player_positions=POSITIONS, dire_player_positions=POSITIONS,
        derived_league_tier=LeagueTier.TIER1,
    )


def _write_snapshot(path: Path, state=None) -> Path:
    payload = {
        "meta": {"reference_timestamp": 1788387568,
                 "model_config_signature": "sig-test",
                 "team_kills_history_schema_version": 2,
                 "team_kills_history_latest_patch": "7.41e"},
        "teams_by_org_key": {
            "org:tundra": {"org_key": "org:tundra", "team_id": 8291895,
                           "team_name": "Tundra", "tier": "TIER1",
                           "current_strength": 1600.0},
            "org:og": {"org_key": "org:og", "team_id": 2586976,
                       "team_name": "OG", "tier": "TIER1",
                       "current_strength": 1580.0},
        },
        "team_kills_history_by_team_id": {},
        "model_state": STATE if state is None else state,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _array_model(path: Path):
    return array_model.build_read_model(path, None)


def _ratings(model) -> dict:
    """Все числа, которые обязана совпасть после применения карты."""
    out: dict = {}
    for p in RAD + DIRE:
        out[f"player_global[{p}]"] = float(model.player_global.get(p, 0.0) or 0.0)
        out[f"player_local[T1][{p}]"] = float(
            model.player_local[LeagueTier.TIER1].get(p, 0.0) or 0.0)
        out[f"seen[{p}]"] = int(model.player_global_last_seen_ts.get(p, 0) or 0)
    for key in ("org:tundra::roster:1", "org:og::roster:2"):
        out[f"roster[{key}]"] = float(
            model.roster_ratings[LeagueTier.TIER1].get(key, 0.0) or 0.0)
        out[f"roster_n[{key}]"] = int(
            model.roster_match_counts[LeagueTier.TIER1].get(key, 0) or 0)
    for key in ("org:tundra::lineup:aaa", "org:og::lineup:bbb"):
        out[f"lineup[{key}]"] = int(model.lineup_match_counts.get(key, 0) or 0)
    out["role[11|POSITION_1]"] = float(
        model.player_role_local[LeagueTier.TIER1].get((11, "POSITION_1"), 0.0) or 0.0)
    out["org[11]"] = str(model.player_current_org.get(11, "") or "")
    out["org_matches[11,tundra]"] = int(
        model.player_current_org_matches.get((11, "org:tundra"), 0) or 0)
    return out


@pytest.fixture(autouse=True)
def _reset():
    array_model._READ_CACHE.clear()
    yield
    array_model._READ_CACHE.clear()


def test_overlay_reads_base_and_keeps_writes_apart(tmp_path) -> None:
    src = _write_snapshot(tmp_path / "snapshot.json")
    model = _array_model(src)
    base_value = float(model.player_global[11])

    overlay = state_overlay.Overlay(model.player_global, "player_global")
    assert float(overlay[11]) == base_value
    assert float(overlay.get(11)) == base_value
    assert 11 in overlay

    overlay[11] = base_value + 25.0
    assert float(overlay[11]) == base_value + 25.0
    # база не тронута — иначе повторное чтение отдавало бы изменённое
    assert float(model.player_global[11]) == base_value
    assert overlay.changes() == {11: base_value + 25.0}


def test_overlay_supports_compound_assignment(tmp_path) -> None:
    src = _write_snapshot(tmp_path / "snapshot.json")
    model = _array_model(src)
    overlay = state_overlay.Overlay(model.lineup_match_counts, "lineup_match_counts")

    before = int(overlay["org:tundra::lineup:aaa"])
    overlay["org:tundra::lineup:aaa"] += 1
    assert int(overlay["org:tundra::lineup:aaa"]) == before + 1
    # отсутствующий ключ — дефолт ноль, как у defaultdict(int) в словарной модели
    assert int(overlay.get("нет:такого", 0)) == 0


def test_overlay_items_merges_and_refuses_hashed(tmp_path) -> None:
    src = _write_snapshot(tmp_path / "snapshot.json")
    model = _array_model(src)

    enumerable = state_overlay.Overlay(model.player_global, "player_global")
    enumerable[99] = 1234.5
    merged = dict(enumerable.items())
    assert merged[99] == 1234.5 and merged[11] == pytest.approx(1500.0)

    hashed = state_overlay.Overlay(model.lineup_match_counts, "lineup_match_counts")
    with pytest.raises(TypeError, match="не перечислимо"):
        list(hashed.items())


def test_wrap_model_covers_every_state_field(tmp_path) -> None:
    src = _write_snapshot(tmp_path / "snapshot.json")
    model = _array_model(src)

    wrappers = state_overlay.wrap_model(model)

    assert set(wrappers) == set(state_overlay.FIELD_SPECS)
    for field, (scope, _k, _v) in state_overlay.FIELD_SPECS.items():
        if scope == "tiered":
            assert isinstance(wrappers[field], dict)
            assert all(isinstance(s, state_overlay.Overlay)
                       for s in wrappers[field].values())
        else:
            assert isinstance(wrappers[field], state_overlay.Overlay)


def test_parity_dict_model_vs_overlay_model_on_same_map(tmp_path) -> None:
    """Главная проверка: одна карта, два представления — числа обязаны совпасть."""
    src = _write_snapshot(tmp_path / "snapshot.json")

    dict_model = HybridPlayerRosterEloModel.from_state(json.loads(json.dumps(STATE)))
    array_wrapped = _array_model(src)
    state_overlay.wrap_model(array_wrapped)

    assert _ratings(dict_model) == _ratings(array_wrapped), \
        "модели расходятся ДО применения карты — проблема в массивном представлении"

    dict_step = dict_model.process_match(_match(101))
    array_step = array_wrapped.process_match(_match(101))

    assert _ratings(dict_model) == _ratings(array_wrapped)
    assert dict_step.metadata == array_step.metadata
    assert float(dict_step.p_radiant) == float(array_step.p_radiant)
    assert float(dict_step.radiant_strength) == float(array_step.radiant_strength)
    assert float(dict_step.dire_strength) == float(array_step.dire_strength)


def test_delta_roundtrip_restores_same_numbers(tmp_path) -> None:
    src = _write_snapshot(tmp_path / "snapshot.json")
    mutated = _array_model(src)
    wrappers = state_overlay.wrap_model(mutated)
    mutated.process_match(_match(101))
    mutated.process_match(_match(102))
    expected = _ratings(mutated)

    changes = state_overlay.collect_changes(wrappers)
    assert changes, "после двух карт дельта обязана быть непустой"
    delta = tmp_path / "live_elo_delta.json"
    state_overlay.save_delta(
        delta, base_reference_timestamp=1788387568,
        base_model_config_signature="sig-test", changes=changes,
        small_parts=state_overlay.collect_small_parts(mutated),
        updated_at=1788400000)
    assert delta.stat().st_size < 64 * 1024, "дельта обязана быть килобайтной"

    restored = _array_model(src)
    state_overlay.wrap_model(restored)
    payload = state_overlay.load_delta(delta, base_reference_timestamp=1788387568,
                                       base_model_config_signature="sig-test")
    assert payload is not None
    state_overlay.restore_small_parts(restored, payload.get("small_parts") or {})
    applied = state_overlay.apply_changes(restored, payload.get("changes") or {})
    assert applied > 0

    assert _ratings(restored) == expected


def test_delta_rejected_on_other_base(tmp_path) -> None:
    delta = tmp_path / "live_elo_delta.json"
    state_overlay.save_delta(
        delta, base_reference_timestamp=1788387568,
        base_model_config_signature="sig-test",
        changes={"player_global": [[11, 1600.0]]}, small_parts={},
        updated_at=1788400000)

    assert state_overlay.load_delta(delta, base_reference_timestamp=1788387568,
                                    base_model_config_signature="sig-test") is not None
    assert state_overlay.load_delta(delta, base_reference_timestamp=1,
                                    base_model_config_signature="sig-test") is None
    assert state_overlay.load_delta(delta, base_reference_timestamp=1788387568,
                                    base_model_config_signature="другая") is None
    assert state_overlay.load_delta(tmp_path / "нет.json",
                                    base_reference_timestamp=1788387568,
                                    base_model_config_signature="sig-test") is None


def test_key_encoding_survives_colons_and_pairs() -> None:
    """Ключи с ':' и '::' и кортежи (игрок, org) обязаны переживать JSON."""
    encoded = state_overlay._encode_key((11, "org:tundra"), "pair")
    assert state_overlay._decode_key(encoded, "pair") == (11, "org:tundra")

    roster_key = "org:tundra::roster:1"
    assert state_overlay._decode_key(
        state_overlay._encode_key(roster_key, "str"), "str") == roster_key

    payload = {"player_current_org_matches": [[[11, "org:tundra"], 31]],
               "roster_ratings": {"TIER1": [["org:tundra::roster:1", 1601.5]]},
               "player_global": [[11, 1613.25]]}
    text = json.loads(json.dumps(payload))

    class _Fake:
        pass

    model = _Fake()
    model.player_current_org_matches = {}
    model.roster_ratings = {LeagueTier.TIER1: {}}
    model.player_global = {}
    applied = state_overlay.apply_changes(model, text)
    assert applied == 3
    assert model.player_current_org_matches[(11, "org:tundra")] == 31
    assert model.roster_ratings[LeagueTier.TIER1]["org:tundra::roster:1"] == 1601.5
    assert model.player_global[11] == 1613.25


def _state_with_old_patch() -> dict:
    state = json.loads(json.dumps(STATE))
    # Патч-ключ заведомо старый: первая же карта обязана triggers сброс тира.
    state["current_patch_key"] = "7.39"
    state["player_local"]["TIER1"] = {str(p): 1700.0 + i for i, p in enumerate(RAD + DIRE)}
    # Игрок ВНЕ состава карты: по нему видно именно сброс, а не обновление карты.
    state["player_local"]["TIER1"]["99"] = 1750.0
    state["player_role_local"]["TIER1"] = {"11|POSITION_1": 1690.0,
                                           "21|POSITION_1": 1680.0}
    return state


def test_patch_reset_is_lazy_and_matches_dict_model(tmp_path) -> None:
    """Смена патча сбрасывает TIER1 local-рейтинги: overlay делает это лениво.

    `patch_local_reset_mode` по умолчанию "exact", `player_local_keep=0.0`,
    `roster_keep=1.0`, `tier1_only=True` (ELO/config.py:62-65), и в боевом
    model_state этих ключей нет — то есть сброс ВКЛЮЧЁН и происходит на первой
    карте после смены патча. Перечислить хешированные хранилища нельзя, поэтому
    сброс ленивый: числа обязаны совпасть со словарной моделью.
    """
    state = _state_with_old_patch()
    src = _write_snapshot(tmp_path / "snapshot.json", state)

    dict_model = HybridPlayerRosterEloModel.from_state(json.loads(json.dumps(state)))
    overlay_model = _array_model(src)
    state_overlay.wrap_model(overlay_model)

    # до сброса local-рейтинги разные и ненулевые
    assert float(dict_model.player_local[LeagueTier.TIER1][11]) == pytest.approx(1700.0)

    dict_model.process_match(_match(201))
    overlay_model.process_match(_match(201))

    initial = float(dict_model.config.initial_rating)
    # Игрок 99 в карте не участвовал: его local-рейтинг обязан остаться ровно
    # initial_rating — это и есть сброс, не замаскированный обновлением карты.
    assert float(dict_model.player_local[LeagueTier.TIER1][99]) == pytest.approx(initial)
    assert float(overlay_model.player_local[LeagueTier.TIER1][99]) == pytest.approx(initial)
    # Участник карты сброшен, затем обновлён самой картой — числа обязаны совпасть.
    assert _ratings(dict_model) == _ratings(overlay_model)


def test_patch_reset_survives_delta_roundtrip(tmp_path) -> None:
    state = _state_with_old_patch()
    src = _write_snapshot(tmp_path / "snapshot.json", state)

    mutated = _array_model(src)
    wrappers = state_overlay.wrap_model(mutated)
    mutated.process_match(_match(201))
    expected = _ratings(mutated)

    resets = state_overlay.collect_resets(wrappers)
    assert resets, "после смены патча дельта обязана нести ленивый сброс"

    delta = tmp_path / "delta.json"
    state_overlay.save_delta(
        delta, base_reference_timestamp=1788387568,
        base_model_config_signature="sig-test",
        changes=state_overlay.collect_changes(wrappers),
        resets=resets,
        small_parts=state_overlay.collect_small_parts(mutated),
        updated_at=1788400000)

    restored = _array_model(src)
    state_overlay.wrap_model(restored)
    payload = state_overlay.load_delta(delta, base_reference_timestamp=1788387568,
                                       base_model_config_signature="sig-test")
    assert payload is not None
    state_overlay.restore_small_parts(restored, payload.get("small_parts") or {})
    assert state_overlay.apply_resets(restored, payload.get("resets") or {}) > 0
    state_overlay.apply_changes(restored, payload.get("changes") or {})

    assert _ratings(restored) == expected


def test_intermediate_keep_refuses_loudly_on_overlay(tmp_path) -> None:
    """Промежуточный keep лениво непредставим — отказ громкий, а не тихий."""
    state = _state_with_old_patch()
    state["config"]["patch_local_reset_player_local_keep"] = 0.5
    src = _write_snapshot(tmp_path / "snapshot.json", state)

    model = _array_model(src)
    state_overlay.wrap_model(model)

    with pytest.raises(TypeError, match="keep=0 и keep=1"):
        model.process_match(_match(301))


def _map_record(mid: int, ts: int, radiant_win: bool = True) -> MatchRecord:
    """Карта серии. `radiant_win` обязан совпадать со сдвигом счёта.

    Живой путь определяет исход по сдвигу счёта серии, а не из записи: при
    `first_team_score` 0 -> 1 и `first_team_is_radiant=True` победителем
    считается радиант. Эталон в тесте обязан быть согласован, иначе расхождение
    выглядит как ошибка чисел, хотя это ошибка фикстуры.
    """
    return MatchRecord(
        match_id=mid, timestamp=ts, radiant_win=radiant_win,
        radiant_team_id=1, radiant_team_name="Elegia",
        dire_team_id=2, dire_team_name="Team Mariachi",
        radiant_player_ids=(1, 2, 3, 4, 5), dire_player_ids=(6, 7, 8, 9, 10),
        league_id=11, league_name="Test League", source_league_tier="TIER2",
        series_id=425663, series_type="3",
        derived_league_tier=LeagueTier.TIER2,
    )


def test_live_maps_write_delta_not_full_state(tmp_path, monkeypatch) -> None:
    """Сквозная проверка живого пути: дельта вместо перезаписи 519 МБ.

    Закрепляет три вещи разом: полное состояние НЕ создаётся вовсе, дельта
    несёт изменения, и числа совпадают со словарной моделью, которая применила
    ту же карту (иначе память сэкономили бы ценой рейтингов).
    """
    import ELO.live_team_strength as lts
    from ELO.config import HybridEloConfig

    lts._SNAPSHOT_CACHE = None
    lts._MODEL_FROM_SNAPSHOT_CACHE.clear()
    lts._RUNTIME_SNAPSHOT_CACHE.update(
        {"base_snapshot_id": None, "runtime_signature": None, "snapshot": None})
    array_model._READ_CACHE.clear()
    array_model._OVERLAY_CACHE.clear()
    array_model._META_CACHE.clear()
    lts._DELTA_MODEL_REPORTED = False

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    snapshot_path = tmp_path / "live_snapshot.json"
    progress_path = tmp_path / "live_progress.json"
    state_path = tmp_path / "live_model_state.json"
    delta_path = tmp_path / "live_delta.json"
    lock_path = tmp_path / "live_state.lock"
    monkeypatch.setenv("LIVE_ELO_DELTA", str(delta_path))

    base_state = HybridPlayerRosterEloModel(HybridEloConfig()).export_state()
    snapshot = {"meta": {"reference_timestamp": 1771153251},
                "teams_by_org_key": {},
                "model_state": base_state}
    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
    signature = lts._model_config_signature(base_state)

    # Пустая дельта = «живое состояние совпадает с базой»: этого достаточно,
    # чтобы живой путь пошёл через overlay, а не через разбор 519 МБ.
    state_overlay.save_delta(delta_path, base_reference_timestamp=1771153251,
                             base_model_config_signature=signature,
                             changes={}, resets={}, small_parts={}, updated_at=1)

    common = dict(
        series_key="425663",
        series_url="dltv.org/matches/425663/elegia-vs-team-mariachi",
        snapshot_path=snapshot_path, data_dir=data_dir, rebuild_if_missing=False,
        progress_path=progress_path, runtime_model_state_path=state_path,
        runtime_lock_path=lock_path,
    )
    first = lts.register_live_map_context(
        map_key="dltv.org/matches/425663/elegia-vs-team-mariachi.0",
        first_team_score=0, second_team_score=0, first_team_is_radiant=True,
        match_record=_map_record(101, 1771153200), **common)
    assert first is not None

    second = lts.register_live_map_context(
        map_key="dltv.org/matches/425663/elegia-vs-team-mariachi.1",
        first_team_score=1, second_team_score=0, first_team_is_radiant=True,
        match_record=_map_record(102, 1771153800), **common)
    assert second is not None
    assert second["applied_update"] is not None, "карта обязана примениться"

    # 1. полное состояние не создавалось вовсе
    assert state_path.exists() is False
    # 2. дельта записана и несёт изменения игроков обеих сторон
    payload = json.loads(delta_path.read_text(encoding="utf-8"))
    assert payload["base_reference_timestamp"] == 1771153251
    changes = payload["changes"]
    assert "player_global" in changes and len(changes["player_global"]) == 10
    assert delta_path.stat().st_size < 64 * 1024

    # 3. числа совпадают со словарной моделью, применившей ту же карту
    reference = HybridPlayerRosterEloModel.from_state(json.loads(json.dumps(base_state)))
    reference.process_match(_map_record(101, 1771153200))

    array_model._OVERLAY_CACHE.clear()
    fresh = array_model.build_overlay_model(snapshot_path, delta_path)
    for player in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10):
        assert float(fresh.player_global[player]) == pytest.approx(
            float(reference.player_global[player])), f"игрок {player} разъехался"
