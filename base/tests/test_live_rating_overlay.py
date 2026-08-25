"""Живые рейтинги: снимок плюс карты, сыгранные после его сборки.

Glicko и TrueSkill накопительные — сыграл карту, сила изменилась. Снимок же
собирается ночью, и живой путь читал его с `mutate=False`, то есть на
четвёртой карте дня показывал состояние игрока ДО первой. Для ELO разрыв
закрыт (живой `live_elo_progress.json`), у рейтингов оставался: 25.08.2026
`team_ratings.block` брал только ночной срез.

Контракт:
- наложение не трогает исходный снимок: он один на процесс, и провести карты
  прямо в нём значит накопить их заново на следующей же оценке;
- карты проводятся по возрастанию времени — рейтинг зависит от порядка;
- карта без пяти+пяти слотов или с анонимом не проводится вовсе;
- граница — срез снимка (`built_ts`); без него накладка не делается, иначе
  карты легли бы поверх состояния, которое их уже содержит.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import prematch_live_delta as D  # noqa: E402
import team_ratings as R  # noqa: E402

ACCS = list(range(101, 111))


def _map(end: int, *, accs=None, rad_won: bool = True, players: int = 10):
    """Карта дельты: пять радиантов и пять дайров с позициями 0..4."""
    accs = list(accs or ACCS)
    rows = []
    for i, a in enumerate(accs[:players]):
        rows.append({"acc": a, "hero": 1 + i, "pos": i % 5,
                     "rad": i < 5, "won": (i < 5) == rad_won,
                     "k": 1, "d": 1, "a": 1})
    return {"end": end, "dur": 2000, "radiant_won": rad_won, "players": rows}


@pytest.fixture
def store(tmp_path, monkeypatch):
    path = tmp_path / "delta.json"
    monkeypatch.setattr(D, "STORE", path, raising=False)
    return path


def _write(path: Path, maps: dict):
    path.write_text(json.dumps({"snapshot_ts": 0, "maps": maps}),
                    encoding="utf-8")


class TestOverlayClone:
    def test_write_does_not_leak_into_source(self):
        st = R.RatingState()
        st.advance(1_000_000, ACCS, True)
        before = st.features(1_000_100, ACCS)

        clone = st.overlay_clone()
        clone.advance(1_000_200, ACCS, True)

        assert st.features(1_000_100, ACCS) == before
        assert clone.features(1_000_300, ACCS) != st.features(1_000_300, ACCS)

    def test_clone_sees_source_values(self):
        st = R.RatingState()
        st.advance(1_000_000, ACCS, True)
        clone = st.overlay_clone()
        # Ничего не проводя, копия обязана отдавать те же числа.
        assert clone.features(1_000_100, ACCS) == st.features(1_000_100, ACCS)


class TestOverlay:
    def test_empty_returns_same_object(self):
        st = R.RatingState()
        assert R.overlay(st, []) is st

    def test_map_moves_rating(self):
        st = R.RatingState()
        st.advance(1_000_000, ACCS, True)
        base = st.features(1_000_500, ACCS)
        got = R.overlay(st, [(1_000_400, ACCS, True)])
        assert got is not st
        assert got.features(1_000_500, ACCS) != base

    def test_order_matters(self):
        # Разный порядок исходов даёт разный рейтинг — значит сортировка в
        # `rating_maps` не косметика.
        a = R.overlay(R.RatingState(), [(1_000, ACCS, True), (2_000, ACCS, False)])
        b = R.overlay(R.RatingState(), [(1_000, ACCS, False), (2_000, ACCS, True)])
        assert a.features(3_000, ACCS) != b.features(3_000, ACCS)


class TestRatingMaps:
    def test_border_excludes_older(self, store):
        _write(store, {"1": _map(500), "2": _map(1_500)})
        got = D.rating_maps(1_000, store_path=store)
        assert [row[0] for row in got] == [1_500]

    def test_sorted_ascending(self, store):
        _write(store, {"1": _map(3_000), "2": _map(1_000), "3": _map(2_000)})
        got = D.rating_maps(0, store_path=store)
        assert [row[0] for row in got] == [1_000, 2_000, 3_000]

    def test_slot_order_is_radiant_then_dire(self, store):
        _write(store, {"1": _map(1_000)})
        (_ts, accounts10, rad_won) = D.rating_maps(0, store_path=store)[0]
        assert accounts10 == ACCS
        assert rad_won is True

    def test_incomplete_side_skipped(self, store):
        _write(store, {"1": _map(1_000, players=9)})
        assert D.rating_maps(0, store_path=store) == []

    def test_anonymous_account_skipped(self, store):
        accs = list(ACCS)
        accs[3] = 0
        _write(store, {"1": _map(1_000, accs=accs)})
        assert D.rating_maps(0, store_path=store) == []

    def test_missing_file_is_empty(self, tmp_path):
        assert D.rating_maps(0, store_path=tmp_path / "нет.json") == []


class TestBlock:
    def test_block_uses_given_state(self):
        st = R.RatingState()
        st.advance(1_000_000, ACCS, True)
        got = R.block(1_000_100, ACCS, snap=st)
        assert got is not None
        assert set(got) == set(R.COLUMNS)
        moved = R.overlay(st, [(1_000_200, ACCS, True)])
        assert R.block(1_000_300, ACCS, snap=moved) != R.block(1_000_300, ACCS, snap=st)
