"""Таблицы драфтовых ячеек и личных встреч на массивах вместо словарей.

После чистки `PairTable`/`AccTable` в модели остались словари помельче, но их
суммарно 1 452 177 записей при 50 МБ полезных данных: `vs_pos` (374 250), `h2h`
(362 112), `h2h_org` (352 193), `team_merge` (167 094), `syn_pos` (150 525) и две
плоские таблицы героев. Обёртки вокруг данных стоили 304 МБ — замерено на боевом
артефакте: модель держала 933 МБ, стала 629.

Числа модели здесь важнее памяти: `cp_lane`, `syn_pos_mean` и `h2h_resid` входят
в вердикт, по которому идёт ставка. Поэтому проверяется не «работает», а что
таблица неотличима от словаря — включая упаковку ключей, которая при неверной
ширине поля молча склеила бы разные пары в одну ячейку.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import prematch_scorer as ps  # noqa: E402

#: (герой1, позиция1, герой2, позиция2, w, g, t0) — как в `vs_pos` артефакта.
VS_POS = np.array([
    [26, 1, 75, 1, 3.0, 7.0, 1_700_000_000],
    [26, 2, 75, 1, 1.0, 4.0, 1_700_000_100],
    [155, 5, 1, 5, 9.0, 11.0, 1_700_000_200],
    [1, 1, 1, 1, 0.5, 2.0, 1_700_000_300],
], dtype=np.float64)

#: (команда1, команда2, значение) — как в `h2h`.
H2H = np.array([
    [2503063, 2680546, 0.25],
    [10213384, 10219690, -0.5],
    [2, 4, 0.75],
], dtype=np.float64)


def test_lookup_matches_a_plain_dict():
    ref = {(int(r[0]), int(r[1]), int(r[2]), int(r[3])): (r[4], r[5], r[6])
           for r in VS_POS}
    t = ps.PackedTable(VS_POS, (8, 3, 8, 3))
    for key, want in ref.items():
        np.testing.assert_allclose(list(t.get(key)), list(want))


def test_missing_key_returns_default():
    t = ps.PackedTable(VS_POS, (8, 3, 8, 3))
    assert t.get((26, 3, 75, 1)) is None
    assert t.get((99, 1, 99, 1), "нет") == "нет"


def test_membership_and_length():
    t = ps.PackedTable(VS_POS, (8, 3, 8, 3))
    assert (26, 1, 75, 1) in t
    assert (26, 3, 75, 1) not in t
    assert len(t) == len(VS_POS)


def test_neighbouring_keys_do_not_collide():
    """Соседние поля не должны перетекать друг в друга.

    Ровно эта ошибка была бы невидимой: ячейка нашлась бы, но чужая, и вердикт
    посчитался бы по контрпику другой пары героев.
    """
    t = ps.PackedTable(VS_POS, (8, 3, 8, 3))
    assert t.get((26, 1, 75, 1))[0] == 3.0
    assert t.get((26, 2, 75, 1))[0] == 1.0      # отличается только позицией
    assert t.get((155, 5, 1, 5))[0] == 9.0
    assert t.get((1, 1, 1, 1))[0] == 0.5


def test_a_field_that_does_not_fit_is_rejected():
    """Позиция 8 не влезает в три бита — это ошибка, а не тихий сдвиг."""
    bad = np.array([[26, 8, 75, 1, 0.0, 0.0, 0.0]], dtype=np.float64)
    with pytest.raises(ValueError, match="не влезает в упаковку"):
        ps.PackedTable(bad, (8, 3, 8, 3))


def test_packing_wider_than_int64_is_rejected():
    with pytest.raises(ValueError, match="не влезает в int64"):
        ps.PackedTable(np.zeros((1, 4)), (32, 32))


def test_scalar_values_come_back_as_numbers():
    t = ps.PackedTable(H2H, (31, 31), scalar=True)
    assert t.get((2503063, 2680546)) == 0.25
    assert t.get((2, 4)) == 0.75
    assert t.get((1, 1)) is None


def test_large_team_ids_survive_packing():
    """Замеренный максимум id боевого артефакта — 10 219 690."""
    t = ps.PackedTable(H2H, (31, 31), scalar=True)
    assert t.get((10213384, 10219690)) == -0.5


def test_nested_key_is_accepted_for_synergy():
    """`syn_pos` приходит ключом ((герой, позиция), (герой, позиция))."""
    rows = np.array([
        [26, 1, 46, 2, 4.0, 8.0, 1_700_000_000],
        [26, 1, 46, 3, 2.0, 5.0, 1_700_000_100],
    ], dtype=np.float64)
    t = ps.PackedTable(rows, (8, 3, 8, 3), nested=True)
    np.testing.assert_allclose(list(t.get(((26, 1), (46, 2)))), [4.0, 8.0, 1_700_000_000])
    np.testing.assert_allclose(list(t.get(((26, 1), (46, 3)))), [2.0, 5.0, 1_700_000_100])
    assert t.get(((26, 2), (46, 2))) is None


def test_single_field_key_is_accepted_for_team_merge():
    """`team_merge` — один id в один id, и значение обязано остаться целым.

    Float на месте идентификатора поехал бы дальше ключом другой таблицы.
    """
    rows = np.array([[2673339, 8121295], [10182357, 8121295]], dtype=np.float64)
    t = ps.PackedTable(rows, (31,), scalar=True, cast=int)
    v = t.get(2673339)
    assert v == 8121295 and isinstance(v, int)
    assert t.get(999, 999) == 999


def test_values_survive_reordering():
    """Строки внутри таблицы сортируются — значение обязано ехать с ключом."""
    t = ps.PackedTable(VS_POS, (8, 3, 8, 3))
    np.testing.assert_allclose(list(t.get((155, 5, 1, 5))), [9.0, 11.0, 1_700_000_200])


def test_empty_table_is_valid():
    t = ps.PackedTable(np.zeros((0, 7)), (8, 3, 8, 3))
    assert len(t) == 0
    assert t.get((1, 1, 1, 1)) is None
    assert (1, 1, 1, 1) not in t
    assert not t          # пустая таблица ложна, как пустой словарь


def test_truthiness_matches_a_dict():
    """`if self.h2h_org` в скорере полагается на это."""
    assert bool(ps.PackedTable(H2H, (31, 31), scalar=True)) is True
    assert bool(ps.PackedTable(np.zeros((0, 3)), (31, 31), scalar=True)) is False
