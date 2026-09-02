"""`team_mean_rating` в build_prematch_artifact.py считает средний ELO команды.

Раньше это было `float(np.mean([...]) or 1500.0)`: при пустом списке (ни
одного известного аккаунта в пятёрке) `np.mean([])` даёт `nan`, а `nan or X`
возвращает сам `nan` (nan truthy), а не `X`. Итог — снимок вручную собранного
артефакта мог получить `nan` в рейтинге команды вместо дефолта 1500.0.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
MODULE_PATH = ROOT / "runtime/experiments/misc/build_prematch_artifact.py"

pytestmark = pytest.mark.skipif(
    not MODULE_PATH.exists(),
    reason="runtime/ — git-ignored scratch, скрипт есть только на боевой машине")

sys.path.insert(0, str(ROOT / "runtime/experiments/misc"))
import build_prematch_artifact as bpa  # noqa: E402


def test_zero_known_accounts_returns_default_not_nan():
    result = bpa.team_mean_rating({}, [0, 0, 0, 0, 0])
    assert result == 1500.0
    assert result == result, "результат — NaN (NaN != NaN)"


def test_known_accounts_average_correctly():
    rating = {5: 1700.0, 7: 1900.0}
    result = bpa.team_mean_rating(rating, [5, 7, 0, 0, 0])
    assert result == pytest.approx(1800.0)


def test_unknown_account_defaults_to_1500_inside_average():
    rating = {5: 1700.0}
    result = bpa.team_mean_rating(rating, [5, 99, 0, 0, 0])
    assert result == pytest.approx(1600.0)
