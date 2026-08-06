"""Гейт изолированного провала GPM на этапе сборки словарей.

Ключевое различие, ради которого гейт и делался (docs/EXPERIMENTS.md E-21):
абсолютный провал эндогенен — он следствие проигранного драфта, и на таких
картах драфт предсказывает ЛУЧШЕ обычного (худший GPM ниже 40% базы -> AUC
0.6814 против 0.6266). А изолированный провал экзогенен и рвёт связь: при
пороге 0.4 AUC падает до 0.5729.

Поэтому меряется разрыв между худшим игроком и МЕДИАНОЙ ЕГО ЖЕ КОМАНДЫ, а не
отставание от бейзлайна как таковое.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import explore_database as ed  # noqa: E402

# База: у всех героев медиана 500 GPM, чтобы отношения читались как доли.
CELLS = {f"{hero}|POSITION_{pos}": {"gpm": {"med": 500}}
         for hero in range(1, 11) for pos in range(1, 6)}


def _match(radiant_gpm, dire_gpm) -> dict:
    players = []
    for i, gpm in enumerate(radiant_gpm):
        players.append({"heroId": i + 1, "position": f"POSITION_{i + 1}",
                        "isRadiant": True, "goldPerMinute": gpm})
    for i, gpm in enumerate(dire_gpm):
        players.append({"heroId": i + 6, "position": f"POSITION_{i + 1}",
                        "isRadiant": False, "goldPerMinute": gpm})
    return {"players": players}


def test_balanced_match_has_no_collapse() -> None:
    got = ed._isolated_gpm_collapse(_match([500] * 5, [500] * 5), CELLS)

    assert got == 0.0


def test_whole_team_underperforming_is_not_a_collapse() -> None:
    """Просевшая ЦЕЛИКОМ команда — это проигрыш, а не поломка: разрыв внутри
    команды нулевой, и такие матчи обязаны проходить."""
    got = ed._isolated_gpm_collapse(_match([250] * 5, [500] * 5), CELLS)

    assert got == 0.0


def test_single_player_collapse_is_detected() -> None:
    """Один провалился, остальные в норме — разрыв равен отставанию от медианы."""
    got = ed._isolated_gpm_collapse(_match([100, 500, 500, 500, 500], [500] * 5), CELLS)

    assert abs(got - 0.8) < 1e-9


def test_collapse_is_taken_as_max_over_both_teams() -> None:
    got = ed._isolated_gpm_collapse(_match([500] * 5, [500, 500, 500, 500, 150]), CELLS)

    assert abs(got - 0.7) < 1e-9


def test_missing_baselines_disable_the_gate() -> None:
    assert ed._isolated_gpm_collapse(_match([100] + [500] * 4, [500] * 5), {}) is None


def test_team_with_too_few_baseline_hits_is_skipped() -> None:
    """Если бейзлайнов хватило меньше чем на четверых, команда не считается —
    иначе разрыв меряется по двум-трём игрокам и это шум."""
    thin = {"1|POSITION_1": {"gpm": {"med": 500}}, "2|POSITION_2": {"gpm": {"med": 500}}}

    assert ed._isolated_gpm_collapse(_match([100, 500, 500, 500, 500], [500] * 5), thin) is None


def test_gate_is_off_by_default() -> None:
    assert ed.ISO_GPM_MAX == 0.0
    assert ed.SMURF_PAIR_FILTER_ENABLED is False
