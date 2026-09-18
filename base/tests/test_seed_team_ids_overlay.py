"""`_seed_team_ids()` должен включать динамически онбордженные tier2-команды.

Находка: с 02.09.2026 рантайм на serv1 онбордит новые tier2-команды не в
статический `base/id_to_names.py` (заморожен), а в JSON-overlay рядом со
справочником (`base/tier_dynamic_overlay.py`). До этой правки `_seed_team_ids()`
читал только статический словарь, и такие команды (Uralan, клубы WINLINE Star
Series) никогда не становились сидами добора про-корпуса.

`_isolate_tier2_dynamic_overlay` в conftest.py (autouse) уже подменяет
`TIER2_DYNAMIC_ONBOARDING_PATH` на путь во `tmp_path` для каждого теста —
здесь достаточно дописать overlay-файл по этому пути перед вызовом.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

try:
    from maps_research import _seed_team_ids
    _IMPORT_ERROR = None
except Exception as e:  # pragma: no cover - защитный путь
    _seed_team_ids = None
    _IMPORT_ERROR = e


pytestmark = pytest.mark.skipif(
    _seed_team_ids is None,
    reason=f"import maps_research не удался: {_IMPORT_ERROR}",
)


def _overlay_path() -> str:
    path = os.getenv("TIER2_DYNAMIC_ONBOARDING_PATH")
    assert path, "conftest должен был выставить TIER2_DYNAMIC_ONBOARDING_PATH"
    return path


def test_seed_team_ids_includes_overlay_teams():
    overlay = {"uralan": [10261034], "winline_club": [10240261]}
    with open(_overlay_path(), "w", encoding="utf-8") as fh:
        json.dump(overlay, fh)

    ids = _seed_team_ids()

    assert 10261034 in ids
    assert 10240261 in ids
    assert ids == sorted(set(ids))


def test_seed_team_ids_missing_overlay_yields_static_only():
    overlay_path = _overlay_path()
    if os.path.exists(overlay_path):
        os.remove(overlay_path)

    static_ids = _seed_team_ids()

    assert 10261034 not in static_ids
    assert 10240261 not in static_ids
    assert static_ids == sorted(set(static_ids))
    assert len(static_ids) > 0
