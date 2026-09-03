"""Память живого ELO-пути: кэш разбора рантайм-состояния и его перебазировка.

Оба механизма — продолжение E-251 (slim-загрузка снимка). Замер 03.09.2026 на
проде: RSS процесса 6.42 ГБ и пик 10.74 ГБ при том, что slim-загрузка снимка
стоит 0.83 ГБ, а json.load всего снимка — 3.69 ГБ. Разницу дали две вещи,
которые эти тесты закрепляют:

* `_load_json_dict` парсил `live_elo_model_state.json` (519 МБ) ЗАНОВО на каждом
  вызове, а `_load_runtime_model_payload` зовётся до трёх раз на завершённую
  карту (`:627`, `:1722`, `:1891`) — два-три разбора по ~2.5-3 ГБ временных
  словарей на карту, и RSS не возвращается (арены glibc фрагментированы);
* после ночной доставки снимка `base_reference_timestamp` расходился, payload
  отклонялся (`:590-603`), и процесс уходил в `full_model_state()` — разбор
  всего model_state из снимка. В логе прода три строки «[ELO] догружаю полный
  model_state», по одной на каждую доставку.

Кэш безопасен, потому что `from_state` КОПИРУЕТ словари состояния
(`models.py:571-579, 657, 662`), а обратная запись строит НОВЫЙ payload через
`export_state()` (`:1918-1922`) — закэшированный dict никто не мутирует.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import ELO.live_team_strength as lts  # noqa: E402
from ELO.rebase_runtime_model_state import main as rebase_main  # noqa: E402


@pytest.fixture(autouse=True)
def _clear_json_cache():
    lts._JSON_DICT_CACHE.clear()
    yield
    lts._JSON_DICT_CACHE.clear()


def _write(path: Path, payload) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _snapshot(tmp_path: Path, reference: int = 1788387568,
              signature: str = "sig-ca41da66", *, with_state: bool = True) -> Path:
    snap = tmp_path / "snapshot.json"
    payload = {
        "meta": {
            "reference_timestamp": reference,
            "model_config_signature": signature,
            "reference_utc": "2026-09-02T22:19:28+00:00",
        },
        "teams_by_org_key": {"org:tundra": {"team_id": 8291895, "tier": "TIER1"}},
        "team_kills_history_by_team_id": {"8291895": [{"match_id": 1, "kills": 30}]},
    }
    if with_state:
        payload["model_state"] = {
            "config": {"k_global": 24.0},
            "player_global": {"11": 1612.5, "22": 1488.0},
        }
    _write(snap, payload)
    return snap


# --------------------------------------------------------------------------- #
# кэш разбора
# --------------------------------------------------------------------------- #

def test_load_json_dict_reuses_parse_until_file_changes(tmp_path) -> None:
    path = tmp_path / "state.json"
    _write(path, {"base_reference_timestamp": 1, "model_state": {"a": 1}})

    first = lts._load_json_dict(path)
    second = lts._load_json_dict(path)
    assert first is second, "повторный вызов обязан отдать кэш, а не парсить заново"

    _write(path, {"base_reference_timestamp": 2, "model_state": {"a": 2, "b": 3}})
    third = lts._load_json_dict(path)
    assert third is not first
    assert third["base_reference_timestamp"] == 2


def test_load_json_dict_survives_same_size_content_change(tmp_path) -> None:
    """Кэш ключуется mtime_ns И размером: равный размер не должен означать «то же»."""
    path = tmp_path / "state.json"
    _write(path, {"v": 1})
    first = lts._load_json_dict(path)

    _write(path, {"v": 2})                      # размер тот же, содержимое другое
    os.utime(path, ns=(1_800_000_000_000_000_000, 1_800_000_000_000_000_000))
    second = lts._load_json_dict(path)

    assert second == {"v": 2}
    assert second is not first


def test_load_json_dict_missing_and_non_dict(tmp_path) -> None:
    assert lts._load_json_dict(tmp_path / "нет.json") is None
    bad = tmp_path / "list.json"
    _write(bad, [1, 2, 3])
    assert lts._load_json_dict(bad) is None
    broken = tmp_path / "broken.json"
    broken.write_text("{не json", encoding="utf-8")
    assert lts._load_json_dict(broken) is None


# --------------------------------------------------------------------------- #
# перебазировка рантайм-состояния
# --------------------------------------------------------------------------- #

def test_rebase_creates_state_when_absent(tmp_path, capsys) -> None:
    snap = _snapshot(tmp_path)
    state = tmp_path / "live_elo_model_state.json"

    assert rebase_main(["--snapshot", str(snap), "--state", str(state)]) == 0

    payload = json.loads(state.read_text(encoding="utf-8"))
    assert payload["base_reference_timestamp"] == 1788387568
    assert payload["base_model_config_signature"] == "sig-ca41da66"
    assert payload["model_state"]["player_global"] == {"11": 1612.5, "22": 1488.0}
    assert "перебазировано" in capsys.readouterr().out


def test_rebase_is_noop_when_base_already_matches(tmp_path, capsys) -> None:
    """Главный guard: иначе перебазировка выбросила бы живые обновления рейтингов."""
    snap = _snapshot(tmp_path)
    state = tmp_path / "live_elo_model_state.json"
    live = {
        "base_reference_timestamp": 1788387568,
        "base_model_config_signature": "sig-ca41da66",
        "updated_at": 1,
        "model_state": {"config": {"k_global": 24.0},
                        "player_global": {"11": 1700.0}},  # живой апдейт рейтинга
    }
    _write(state, live)
    before = state.read_bytes()

    assert rebase_main(["--snapshot", str(snap), "--state", str(state)]) == 0

    assert state.read_bytes() == before, "файл не должен быть тронут"
    assert json.loads(state.read_text())["model_state"]["player_global"]["11"] == 1700.0
    assert "не тронуто" in capsys.readouterr().out


def test_rebase_rewrites_when_base_diverged(tmp_path) -> None:
    snap = _snapshot(tmp_path, reference=1788387568, signature="sig-new")
    state = tmp_path / "live_elo_model_state.json"
    _write(state, {
        "base_reference_timestamp": 1788295911,       # старый срез
        "base_model_config_signature": "sig-old",
        "updated_at": 1,
        "model_state": {"config": {}, "player_global": {"11": 1500.0}},
    })

    assert rebase_main(["--snapshot", str(snap), "--state", str(state)]) == 0

    payload = json.loads(state.read_text(encoding="utf-8"))
    assert payload["base_reference_timestamp"] == 1788387568
    assert payload["base_model_config_signature"] == "sig-new"
    assert payload["model_state"]["player_global"]["11"] == 1612.5


def test_rebase_force_overrides_matching_base(tmp_path) -> None:
    snap = _snapshot(tmp_path)
    state = tmp_path / "live_elo_model_state.json"
    _write(state, {
        "base_reference_timestamp": 1788387568,
        "base_model_config_signature": "sig-ca41da66",
        "updated_at": 1,
        "model_state": {"player_global": {"11": 1700.0}},
    })

    assert rebase_main(["--snapshot", str(snap), "--state", str(state), "--force"]) == 0
    assert json.loads(state.read_text())["model_state"]["player_global"]["11"] == 1612.5


def test_rebase_fails_loudly_without_model_state(tmp_path, capsys) -> None:
    snap = _snapshot(tmp_path, with_state=False)
    state = tmp_path / "live_elo_model_state.json"

    assert rebase_main(["--snapshot", str(snap), "--state", str(state)]) == 1
    assert "нет model_state" in capsys.readouterr().err
    assert not state.exists()
