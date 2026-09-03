"""Sidecar `.npz` для массивов `model_state`: идентичность, протухание, slim-путь.

Зачем эти тесты. Два пути сборки хранилищ (поток из JSON и чтение готовых
массивов) обязаны давать ОДНИ И ТЕ ЖЕ числа: речь о рейтингах, которыми
`hybrid_strength` уходит в предматчевую модель (один из двух признаков с
корреляцией 1.0 к обучению). Расхождение здесь не видно снаружи — оно выглядит
как «модель чуть поехала».

Второе, что закреплено: slim-снимок (E-251) больше не тянет полный
`model_state`. До этой правки `_restore_model_from_snapshot` на базовом снимке
дозагружал 527 МБ (~1.7 ГБ RSS) и строил поверх них словарную модель (~1.3 ГБ),
и случалось это в КАЖДОМ процессе, потому что `get_matchup_summary` зовёт
`build_matchup_summary_from_snapshot` дважды — на базовом снимке и на мерженом.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ELO import array_model  # noqa: E402
from ELO.domain import LeagueTier  # noqa: E402
import ELO.live_team_strength as lts  # noqa: E402


STATE = {
    "config": {"k_global": 24.0, "base_rating": 1500.0},
    "current_patch_key": "7.41e",
    "side_bias": {"TIER1": 6.8, "TIER2": 3.1, "TIER3": 0.0},
    "roster_tracker": {},
    "player_global": {"11": 1612.5, "22": 1488.0, "33": 1500.0},
    "player_global_last_seen_ts": {"11": 1788200000, "22": 1788100000,
                                   "33": 1788000000},
    "lineup_match_counts": {"org:tundra::lineup:abc": 3, "org:og::lineup:xyz": 1},
    "player_local": {"TIER1": {"11": 1590.0}, "TIER2": {"22": 1502.5}},
    "player_local_last_seen_ts": {"TIER1": {"11": 1788200000},
                                  "TIER2": {"22": 1788100000}},
    "player_role_local": {"TIER1": {"11|POSITION_1": 1600.0},
                          "TIER2": {"22|POSITION_5": 1495.0}},
    "player_role_local_last_seen_ts": {"TIER1": {"11|POSITION_1": 1788200000}},
    "roster_ratings": {"TIER1": {"org:tundra::roster:1": 1723.2},
                       "TIER3": {"name:stack::roster:9": 1512.75}},
    "roster_last_seen_ts": {"TIER1": {"org:tundra::roster:1": 1788200000}},
    "roster_match_counts": {"TIER1": {"org:tundra::roster:1": 137},
                            "TIER3": {"name:stack::roster:9": 2}},
    "player_current_org": {"11": "org:tundra", "22": "org:og"},
    "player_current_org_matches": {"11": {"org:tundra": 39}, "22": {"org:og": 5}},
}


def _snapshot_payload(state=None) -> dict:
    return {
        "meta": {
            "reference_timestamp": 1788387568,
            "reference_utc": "2026-09-02T22:19:28+00:00",
            "model_config_signature": "sig-test",
            "team_kills_history_schema_version": 2,
            "team_kills_history_latest_patch": "7.41e",
        },
        "teams_by_org_key": {
            "org:tundra": {"org_key": "org:tundra", "team_id": 8291895,
                           "team_name": "Tundra", "tier": "TIER1",
                           "current_strength": 1723.2},
        },
        "team_kills_history_by_team_id": {
            "8291895": [{"match_id": 1, "kills": 30, "timestamp": 1788200000,
                         "player_ids": [11, 22, 33, 44, 55], "patch": "7.41e"}],
        },
        "model_state": STATE if state is None else state,
    }


def _write_snapshot(path: Path, state=None) -> Path:
    path.write_text(json.dumps(_snapshot_payload(state), ensure_ascii=False),
                    encoding="utf-8")
    return path


def _scalar(value):
    """numpy-скаляр -> python-скаляр, чтобы repr не различал два пути."""
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def _array_sig(arr) -> list:
    kind = getattr(arr, "dtype", None)
    flat = list(arr.ravel()) if hasattr(arr, "ravel") else list(arr)
    if kind is not None and kind.kind in "iu":
        return sorted(int(_scalar(v)) for v in flat)
    if kind is not None and kind.kind == "f":
        return sorted(float(_scalar(v)) for v in flat)
    return sorted(str(_scalar(v)) for v in flat)


def _store_content(store, depth: int = 0):
    """Содержимое хранилища в сравниваемом виде — по внутренностям.

    Публичного `items()` хватает не везде: у `HashedStore` ключи — необратимый
    blake2b, и отдавать их он не должен по контракту; у `PairCounts` и
    `RoleStore` данные лежат в `_inner`. Поэтому сравниваются массивы внутри
    (рекурсивно) — для проверки идентичности двух путей сборки этого
    достаточно: в оба пути попадают одни и те же пары ключ-значение.
    """
    if depth > 3:
        return ("deep", type(store).__name__)
    parts = []
    for attr in ("_keys", "_vals", "_names", "_orgs", "_default"):
        if not hasattr(store, attr):
            continue
        value = getattr(store, attr)
        if hasattr(value, "ravel"):
            parts.append((attr, _array_sig(value)))
        elif isinstance(value, dict):
            parts.append((attr, sorted((str(k), _scalar(v)) for k, v in value.items())))
        elif isinstance(value, (list, tuple)):
            parts.append((attr, [str(_scalar(v)) for v in value]))
        else:
            parts.append((attr, _scalar(value)))
    inner = getattr(store, "_inner", None)
    if inner is not None:
        parts.append(("_inner", _store_content(inner, depth + 1)))
    try:
        size = len(store)
    except Exception:
        size = None
    return ("attrs", parts, size)


def _dump(stores: dict) -> dict:
    out = {}
    for name, value in stores.items():
        if isinstance(value, dict):
            out[name] = {tier: _store_content(store) for tier, store in value.items()}
        else:
            out[name] = _store_content(value)
    return out


@pytest.fixture(autouse=True)
def _reset_caches():
    lts._SNAPSHOT_CACHE = None
    lts._MODEL_FROM_SNAPSHOT_CACHE.clear()
    array_model._READ_CACHE.clear()
    yield
    lts._SNAPSHOT_CACHE = None
    lts._MODEL_FROM_SNAPSHOT_CACHE.clear()
    array_model._READ_CACHE.clear()


def test_sidecar_roundtrip_is_identical(tmp_path) -> None:
    src = _write_snapshot(tmp_path / "snapshot.json")

    streamed = array_model.load_state_arrays(src, "model_state.")
    out = array_model.save_state_arrays(src, "model_state.")
    assert out == array_model.sidecar_path(src)
    assert out.exists()
    cached = array_model.load_state_arrays_cached(src, "model_state.")

    assert _dump(cached) == _dump(streamed)


def test_sidecar_stamp_guards_staleness(tmp_path) -> None:
    src = _write_snapshot(tmp_path / "snapshot.json")
    out = array_model.save_state_arrays(src, "model_state.")
    assert array_model._sidecar_matches(out, src, "model_state.")

    # источник изменился — sidecar обязан перестать подходить
    state = json.loads(json.dumps(STATE))
    state["player_global"]["44"] = 1655.0
    _write_snapshot(src, state)
    assert not array_model._sidecar_matches(out, src, "model_state.")

    fresh = array_model.load_state_arrays_cached(src, "model_state.")
    assert fresh["player_global"][44] == pytest.approx(1655.0)


def test_sidecar_ignores_wrong_prefix(tmp_path) -> None:
    src = _write_snapshot(tmp_path / "snapshot.json")
    out = array_model.save_state_arrays(src, "model_state.")
    assert not array_model._sidecar_matches(out, src, "other_prefix.")


def test_build_read_model_same_with_and_without_sidecar(tmp_path) -> None:
    src = _write_snapshot(tmp_path / "snapshot.json")

    plain = array_model.build_read_model(src, None)
    array_model.save_state_arrays(src, "model_state.")
    array_model._READ_CACHE.clear()
    fast = array_model.build_read_model(src, None)

    assert float(plain.player_global[11]) == float(fast.player_global[11])
    assert float(plain.player_global[22]) == float(fast.player_global[22])
    assert int(plain.lineup_match_counts["org:tundra::lineup:abc"]) == \
        int(fast.lineup_match_counts["org:tundra::lineup:abc"])

    kwargs = dict(team_id=8291895, team_name="Tundra",
                  player_ids=[11, 22, 33, 44, 55], tier=LeagueTier.TIER1,
                  timestamp=1788387569)
    a = plain.preview_team_strength(**kwargs)
    b = fast.preview_team_strength(**kwargs)
    assert a == b, f"preview_team_strength разошёлся: {a} != {b}"


def test_restore_model_from_slim_snapshot_does_not_load_full_state(tmp_path,
                                                                  capsys) -> None:
    """Главная экономия: базовый снимок не тянет 527 МБ model_state."""
    src = _write_snapshot(tmp_path / "snapshot.json")
    array_model.save_state_arrays(src, "model_state.")

    slim = lts._load_snapshot_streaming(src)
    assert slim is not None
    assert lts.SLIM_MODEL_STATE_MARKER in slim["model_state"]

    model = lts._restore_model_from_snapshot(slim)

    assert model is not None
    assert float(model.player_global[11]) == pytest.approx(1612.5)
    # состояние осталось slim: полной догрузки не было
    assert lts.SLIM_MODEL_STATE_MARKER in slim["model_state"]
    assert "player_global" not in slim["model_state"]
    assert "догружаю полный model_state" not in capsys.readouterr().out


def test_restore_model_falls_back_when_array_model_unavailable(tmp_path,
                                                              monkeypatch,
                                                              capsys) -> None:
    """Если массивная модель не поднялась — прежний путь, саммари не пропадает."""
    src = _write_snapshot(tmp_path / "snapshot.json")
    slim = lts._load_snapshot_streaming(src)

    monkeypatch.setattr(lts, "_array_model_for_base_snapshot", lambda _p: None)

    model = lts._restore_model_from_snapshot(slim)

    assert model is not None
    assert float(model.player_global[11]) == pytest.approx(1612.5)
    # slim заменён полным состоянием — это и есть признак запасного пути
    assert lts.SLIM_MODEL_STATE_MARKER not in slim["model_state"]
    assert "догружаю полный model_state" in capsys.readouterr().out
