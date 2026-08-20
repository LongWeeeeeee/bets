"""Снимок рейтингов на массивах ведёт себя как словарь, но не ест память.

Словарём это состояние стоило 480 МБ при 47 МБ полезных данных: два миллиона
записей, у каждой объект-ключ и список из трёх Python-float, а у позиционного
Glicko ключ ещё и кортеж. Живой путь состояние только читает — `block()` зовёт
`features(mutate=False)`, — поэтому база лежит в массивах, а запись идёт
наложением поверх: офлайн-сборщик снимка работает как прежде.

Числа модели здесь важнее памяти: колонки `rating_0..rating_5` входят в панель,
и сдвиг хоть в одном разряде означал бы другой вердикт. Поэтому проверяется не
«работает», а побитовое совпадение со словарной реализацией.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import team_ratings as T  # noqa: E402

G_KEYS = np.array([500, 100, 900, 42], dtype=np.int64)
G_VALS = np.array([[1500.0, 200.0, 1_700_000_000],
                   [1600.0, 100.0, 1_700_100_000],
                   [1400.0, 350.0, 1_699_000_000],
                   [1550.0, 150.0, 1_700_200_000]], dtype=np.float64)
P_KEYS = np.array([[500, 0], [500, 4], [100, 2], [900, 1]], dtype=np.int64)
P_VALS = np.array([[1510.0, 210.0, 1_700_000_000],
                   [1490.0, 220.0, 1_700_000_500],
                   [1610.0, 110.0, 1_700_100_000],
                   [1390.0, 340.0, 1_699_000_000]], dtype=np.float64)
T_KEYS = np.array([500, 100, 900, 42], dtype=np.int64)
T_VALS = np.array([[25.5, 8.0, 1_700_000_000],
                   [27.0, 7.5, 1_700_100_000],
                   [23.0, 8.2, 1_699_000_000],
                   [25.0, 8.3, 1_700_200_000]], dtype=np.float64)


def dict_state() -> T.RatingState:
    """Прежняя, словарная сборка — эталон для сравнения."""
    st = T.RatingState()
    for k, v in zip(G_KEYS.tolist(), G_VALS.tolist()):
        st.glicko[int(k)] = [v[0], v[1], int(v[2])]
    for k, v in zip(P_KEYS.tolist(), P_VALS.tolist()):
        st.pos[(int(k[0]), int(k[1]))] = [v[0], v[1], int(v[2])]
    for k, v in zip(T_KEYS.tolist(), T_VALS.tolist()):
        a = int(k)
        st.ts_mu[a], st.ts_sig[a], st.ts_last[a] = v[0], v[1], int(v[2])
    return st


def array_state() -> T.RatingState:
    return T.RatingState.from_arrays(G_KEYS, G_VALS, P_KEYS, P_VALS,
                                     T_KEYS, T_VALS)


#: Десять РАЗНЫХ аккаунтов. Повторять игрока в обеих командах нельзя: при
#: `mutate=True` первая сторона записывает раздутый RD обратно, и вторая читает
#: уже изменённое состояние — расхождение было бы артефактом теста, а не кода.
ACCOUNTS = [500, 100, 900, 42, 777, 111, 222, 333, 444, 888]
NOW = 1_700_500_000


def test_features_match_the_dict_implementation_bit_for_bit():
    """Шесть колонок обязаны совпасть РОВНО: они идут в вердикт панели."""
    a = dict_state().features(NOW, ACCOUNTS)
    b = array_state().features(NOW, ACCOUNTS)
    assert a == b, f"колонки разошлись:\n{a}\n{b}"


def test_unknown_accounts_fall_back_the_same_way():
    """Незнакомый игрок обязан давать те же значения по умолчанию."""
    unknown = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    assert dict_state().features(NOW, unknown) == array_state().features(NOW, unknown)


def test_get_returns_the_same_rows():
    d, a = dict_state(), array_state()
    for k in G_KEYS.tolist():
        np.testing.assert_allclose(list(d.glicko[int(k)]), list(a.glicko[int(k)]))
    for k in P_KEYS.tolist():
        key = (int(k[0]), int(k[1]))
        np.testing.assert_allclose(list(d.pos[key]), list(a.pos[key]))
    for k in T_KEYS.tolist():
        assert float(d.ts_mu[int(k)]) == float(a.ts_mu[int(k)])
        assert float(d.ts_sig[int(k)]) == float(a.ts_sig[int(k)])
        assert int(d.ts_last[int(k)]) == int(a.ts_last[int(k)])


def test_missing_key_returns_default():
    a = array_state()
    assert a.glicko.get(123456) is None
    assert a.pos.get((123456, 3)) is None
    assert a.ts_mu.get(123456, T.TS_MU0) == T.TS_MU0


def test_membership_matches_the_keys():
    a = array_state()
    assert 500 in a.glicko and 123456 not in a.glicko
    assert (500, 4) in a.pos and (500, 3) not in a.pos


def test_pair_keys_do_not_collide():
    """Упаковка `аккаунт << 3 | позиция` не смеет склеивать соседние ключи."""
    keys = np.array([[1, 0], [1, 1], [2, 0], [(1 << 28) - 1, 7], [1 << 28, 0]],
                    dtype=np.int64)
    vals = np.arange(15, dtype=np.float64).reshape(5, 3)
    st = T.RatingState.from_arrays(np.zeros(0, np.int64), np.zeros((0, 3)),
                                   keys, vals,
                                   np.zeros(0, np.int64), np.zeros((0, 3)))
    for row, (acc, pos) in zip(vals, keys.tolist()):
        np.testing.assert_allclose(list(st.pos[(acc, pos)]), list(row))
    assert st.pos.get((1, 2)) is None


def test_position_outside_the_packing_is_rejected():
    """Позиция вне 0..7 склеила бы разные пары — это ошибка, а не тихий сдвиг."""
    with pytest.raises(ValueError, match="не влезают в упаковку"):
        T.RatingState.from_arrays(np.zeros(0, np.int64), np.zeros((0, 3)),
                                  np.array([[1, 8]], dtype=np.int64),
                                  np.zeros((1, 3)),
                                  np.zeros(0, np.int64), np.zeros((0, 3)))


def test_writes_land_on_top_of_the_arrays():
    """Офлайн-сборщик пишет в то же состояние: наложение обязано перекрывать базу."""
    a = array_state()
    a.glicko[500] = [9999.0, 1.0, 42]
    assert list(a.glicko[500]) == [9999.0, 1.0, 42]
    a.pos[(500, 0)] = [8888.0, 2.0, 43]
    assert list(a.pos[(500, 0)]) == [8888.0, 2.0, 43]
    # Новый ключ, которого в базе нет вовсе.
    a.glicko[321] = [1234.0, 3.0, 44]
    assert list(a.glicko[321]) == [1234.0, 3.0, 44]
    assert 321 in a.glicko


def test_advance_still_works_on_a_snapshot_state():
    """Накопление поверх снимка не должно падать: на нём строится следующий снимок."""
    a = array_state()
    before = a.features(NOW, ACCOUNTS)
    same = a.advance(NOW, ACCOUNTS, radiant_won=True)
    assert same == before, "advance обязан вернуть то же, что видел живой путь"
    after = a.features(NOW + 3600, ACCOUNTS)
    assert after != before, "состояние не изменилось, хотя карта проведена"


def test_len_counts_base_and_overlay_without_double_counting():
    a = array_state()
    assert len(a.glicko) == len(G_KEYS)
    a.glicko[500] = [1.0, 2.0, 3]          # уже есть в базе
    assert len(a.glicko) == len(G_KEYS)
    a.glicko[999999] = [1.0, 2.0, 3]       # новый
    assert len(a.glicko) == len(G_KEYS) + 1


def test_keys_and_items_survive_for_the_snapshot_writer():
    """`save_snapshot` обходит состояние ключами — иначе снимок не соберётся."""
    a = array_state()
    assert sorted(a.glicko.keys()) == sorted(G_KEYS.tolist())
    assert sorted(a.pos.keys()) == sorted((int(x), int(y)) for x, y in P_KEYS)
    got = {k: list(v) for k, v in a.pos.items()}
    for k, v in zip(P_KEYS.tolist(), P_VALS.tolist()):
        np.testing.assert_allclose(got[(int(k[0]), int(k[1]))], v)


def test_a_snapshot_round_trip_keeps_the_numbers(tmp_path):
    """Записать и прочитать обратно — колонки обязаны совпасть."""
    st = array_state()
    p = tmp_path / "rating_snapshot.npz"
    T.save_snapshot(p, st, built_ts=NOW)
    back = T.load_snapshot(p)
    assert back is not None
    assert back.features(NOW, ACCOUNTS) == st.features(NOW, ACCOUNTS)


def test_empty_snapshot_is_valid():
    st = T.RatingState.from_arrays(np.zeros(0, np.int64), np.zeros((0, 3)),
                                   np.zeros((0, 2), np.int64), np.zeros((0, 3)),
                                   np.zeros(0, np.int64), np.zeros((0, 3)))
    assert len(st.glicko) == 0
    vals = st.features(NOW, ACCOUNTS)
    assert len(vals) == 6
    assert all(isinstance(v, float) for v in vals)
