"""Ночная синхронизация дельты ELO после доставки свежего снимка."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ELO import state_overlay  # noqa: E402
from ELO.convert_state_to_delta import main as convert_main  # noqa: E402


def _write(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _snapshot(tmp_path: Path, *, reference: int = 1788562207,
              signature: str = "sig-fresh") -> Path:
    path = tmp_path / "snapshot.json"
    _write(path, {
        "meta": {
            "reference_timestamp": reference,
            "model_config_signature": signature,
        },
        "model_state": {
            "config": {"k_global": 24.0},
            "player_global": {"11": 1612.5},
        },
    })
    return path


def _state(tmp_path: Path, *, reference: int = 1788562207,
           signature: str = "sig-fresh", rating: float = 1700.0) -> Path:
    path = tmp_path / "state.json"
    _write(path, {
        "base_reference_timestamp": reference,
        "base_model_config_signature": signature,
        "model_state": {
            "config": {"k_global": 24.0},
            "player_global": {"11": rating},
        },
    })
    return path


def _save_delta(path: Path, *, reference: int, signature: str) -> None:
    state_overlay.save_delta(
        path,
        base_reference_timestamp=reference,
        base_model_config_signature=signature,
        changes={"player_global": [[11, 1600.0]]},
        small_parts={},
        updated_at=1,
    )


def test_if_stale_refreshes_delta_and_keeps_live_player_value(tmp_path) -> None:
    snapshot = _snapshot(tmp_path)
    state = _state(tmp_path, rating=1700.0)
    delta = tmp_path / "live_elo_delta.json"
    _save_delta(delta, reference=1788387568, signature="sig-old")

    assert convert_main(["--if-stale", "--snapshot", str(snapshot),
                         "--state", str(state), "--delta", str(delta)]) == 0

    payload = state_overlay.load_delta(
        delta, base_reference_timestamp=1788562207,
        base_model_config_signature="sig-fresh")
    assert payload is not None
    assert payload["changes"]["player_global"] == [[11, 1700.0]]


def test_if_stale_keeps_matching_delta_byte_identical_despite_old_full_state(tmp_path) -> None:
    snapshot = _snapshot(tmp_path)
    state = _state(tmp_path, reference=1788387568, signature="sig-old")
    delta = tmp_path / "live_elo_delta.json"
    _save_delta(delta, reference=1788562207, signature="sig-fresh")
    before = delta.read_bytes()

    assert convert_main(["--if-stale", "--snapshot", str(snapshot),
                         "--state", str(state), "--delta", str(delta)]) == 0

    assert delta.read_bytes() == before


def test_failed_stale_refresh_does_not_replace_existing_delta(tmp_path) -> None:
    snapshot = _snapshot(tmp_path)
    state = _state(tmp_path, rating=1700.0)
    # Удаление записи дельтой не выразить, поэтому конвертер обязан отказаться.
    _write(state, {
        "base_reference_timestamp": 1788562207,
        "base_model_config_signature": "sig-fresh",
        "model_state": {"config": {"k_global": 24.0}, "player_global": {}},
    })
    delta = tmp_path / "live_elo_delta.json"
    _save_delta(delta, reference=1788387568, signature="sig-old")
    before = delta.read_bytes()

    assert convert_main(["--if-stale", "--snapshot", str(snapshot),
                         "--state", str(state), "--delta", str(delta)]) == 1

    assert delta.read_bytes() == before


def test_nightly_script_refreshes_delta_after_rebase_before_restart() -> None:
    script = (ROOT / "scripts/run/rebuild_prematch_snapshot.sh").read_text(
        encoding="utf-8")
    stop = script.index("systemctl stop cyberscore.service")
    rebase = script.index("ELO/rebase_runtime_model_state.py", stop)
    refresh = script.index("ELO/convert_state_to_delta.py --if-stale", rebase)
    arrays = script.index("ELO/build_state_arrays.py", refresh)
    start = script.index("systemctl start cyberscore.service", arrays)

    assert stop < rebase < refresh < arrays < start
    assert "ВНИМАНИЕ: обновление ELO-дельты не удалось" in script[refresh:start]
