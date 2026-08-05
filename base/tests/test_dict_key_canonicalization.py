"""Канонический порядок групп в словарях: без дублей и без потери значений.

Билдер писал каждую пару в обоих порядках (`A,B` и `B,A`) с одинаковыми
инкрементами, а трио — в порядке игроков из JSON, из-за чего один набор
растекался по перестановкам. Здесь фиксируется новый контракт:

  * на матч пишется ровно один ключ на неупорядоченную группу;
  * читатели (cp/synergy и kills_window) дают на каноническом словаре те же
    значения, что и на старом двухпорядковом.
"""
from __future__ import annotations

import sys
from itertools import combinations, permutations
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import analise_database
import functions


def _draft(offset=0):
    r_by_pos = {pos: offset + pos for pos in range(1, 6)}
    d_by_pos = {pos: offset + 100 + pos for pos in range(1, 6)}
    return r_by_pos, d_by_pos


def _legacy_add_combinations(r_by_pos, d_by_pos, target, r_value, d_value):
    """Прежний билдер: оба порядка у пар, трио в порядке items()."""
    r_items = list(r_by_pos.items())
    d_items = list(d_by_pos.items())
    for pos_num, hero_id in r_items:
        analise_database._append_to_dict(target, f'{hero_id}pos{pos_num}', r_value)
    for pos_num, hero_id in d_items:
        analise_database._append_to_dict(target, f'{hero_id}pos{pos_num}', d_value)
    for r_pos, r_hero in r_items:
        for d_pos1, d_hero1 in d_items:
            for d_pos2, d_hero2 in d_items:
                if d_hero1 == d_hero2:
                    continue
                analise_database._append_to_dict(
                    target, f'{r_hero}pos{r_pos}_vs_{d_hero1}pos{d_pos1},{d_hero2}pos{d_pos2}', r_value)
    for r_pos1, r_hero1 in r_items:
        for r_pos2, r_hero2 in r_items:
            if r_hero1 == r_hero2:
                continue
            for d_pos, d_hero in d_items:
                analise_database._append_to_dict(
                    target, f'{r_hero1}pos{r_pos1},{r_hero2}pos{r_pos2}_vs_{d_hero}pos{d_pos}', r_value)
    for r_pos, r_hero in r_items:
        for d_pos, d_hero in d_items:
            analise_database._append_to_dict(
                target, f'{r_hero}pos{r_pos}_vs_{d_hero}pos{d_pos}', r_value)
    for r_pos1, r_hero1 in r_items:
        for r_pos2, r_hero2 in r_items:
            if r_hero1 == r_hero2:
                continue
            analise_database._append_to_dict(
                target, f'{r_hero1}pos{r_pos1}_with_{r_hero2}pos{r_pos2}', r_value)
    for d_pos1, d_hero1 in d_items:
        for d_pos2, d_hero2 in d_items:
            if d_hero1 == d_hero2:
                continue
            analise_database._append_to_dict(
                target, f'{d_hero1}pos{d_pos1}_with_{d_hero2}pos{d_pos2}', d_value)
    for trio in combinations(r_items, 3):
        analise_database._append_to_dict(
            target, ",".join(f'{h}pos{p}' for p, h in trio), r_value)
    for trio in combinations(d_items, 3):
        analise_database._append_to_dict(
            target, ",".join(f'{h}pos{p}' for p, h in trio), d_value)


def _group_of(key):
    """Неупорядоченная подпись ключа: группы приводятся к frozenset."""
    if '_vs_' in key:
        left, right = key.split('_vs_', 1)
        return ('vs', frozenset(left.split(',')), frozenset(right.split(',')))
    if '_with_' in key:
        left, right = key.split('_with_', 1)
        return ('with', frozenset([left, right]))
    return ('combo', frozenset(key.split(',')))


def test_builder_writes_one_key_per_unordered_group():
    r_by_pos, d_by_pos = _draft()
    target = {}
    analise_database._add_combinations_to_dict(r_by_pos, d_by_pos, target, 1, 0)

    signatures = {}
    for key in target:
        sig = _group_of(key)
        assert sig not in signatures, f"дубль порядка: {key} и {signatures[sig]}"
        signatures[sig] = key

    # 10 solo + 50 1v2 + 50 2v1 + 25 1v1 + 20 with + 20 trio
    assert len(target) == 175
    assert sum(stats['games'] for stats in target.values()) == 175


def test_kills_builder_writes_all_layers_canonically():
    r_by_pos, d_by_pos = _draft()
    target = {}
    analise_database._add_kills_window_combinations(
        r_by_pos, d_by_pos, target, [1.0, 2.0, 3.0, 4.0])

    signatures = set()
    for key in target:
        sig = _group_of(key)
        assert sig not in signatures, f"дубль порядка: {key}"
        signatures.add(sig)
    # 10 solo + 50 1v2 + 50 2v1 + 25 1v1 + 20 with + 20 trio
    assert len(target) == 175
    assert sum(1 for key in target if key.count(',') == 2 and '_vs_' not in key) == 20


def test_kills_builder_high_order_flag_off_drops_1v2_2v1(monkeypatch):
    monkeypatch.setattr(analise_database, "KILLS_WINDOW_BUILD_HIGH_ORDER", False)
    r_by_pos, d_by_pos = _draft()
    target = {}
    analise_database._add_kills_window_combinations(
        r_by_pos, d_by_pos, target, [1.0, 2.0, 3.0, 4.0])

    assert not any('_vs_' in key and ',' in key for key in target)
    # 10 solo + 25 1v1 + 20 with + 20 trio
    assert len(target) == 75


def test_kills_trio_layer_is_read_back(monkeypatch):
    """Слой trio читается там, где парные слои пусты."""
    monkeypatch.setenv("KILLS_WINDOW_MIN_GAMES", "1")
    monkeypatch.setenv("KILLS_WINDOW_LAYER_POLICY", "first_hit")
    r_by_pos, d_by_pos = _draft()
    full = {}
    analise_database._add_kills_window_combinations(
        r_by_pos, d_by_pos, full, [2.0, 2.0, 2.0, 2.0])

    r_tokens = [f'{h}pos{p}' for p, h in r_by_pos.items()]
    d_tokens = [f'{h}pos{p}' for p, h in d_by_pos.items()]
    trio_only = {k: v for k, v in full.items()
                 if k.count(',') == 2 and '_vs_' not in k}
    assert trio_only

    payload = functions.calculate_kills_window_advantage(
        r_tokens, d_tokens, trio_only, window="10_20")
    assert payload is not None
    assert payload["layer"] == "trio"
    assert payload["expected_diff"] == 2.0


def test_trio_keys_do_not_fragment_across_player_order():
    """Разный порядок игроков в JSON не должен плодить перестановки трио."""
    target = {}
    r_by_pos = {1: 11, 2: 12, 3: 13, 4: 14, 5: 15}
    d_by_pos = {1: 21, 2: 22, 3: 23, 4: 24, 5: 25}
    shuffled_r = {pos: r_by_pos[pos] for pos in (3, 5, 1, 4, 2)}
    shuffled_d = {pos: d_by_pos[pos] for pos in (2, 4, 5, 3, 1)}

    analise_database._add_combinations_to_dict(r_by_pos, d_by_pos, target, 1, 0)
    keys_after_first = set(target)
    analise_database._add_combinations_to_dict(shuffled_r, shuffled_d, target, 1, 0)

    assert set(target) == keys_after_first, "перемешанный порядок игроков создал новые ключи"
    assert all(stats['games'] == 2 for stats in target.values())


def test_readers_match_legacy_two_order_dictionary():
    """cp/synergy-читатели дают на каноническом словаре те же числа."""
    r_by_pos, d_by_pos = _draft()
    canonical = {}
    legacy = {}
    analise_database._add_combinations_to_dict(r_by_pos, d_by_pos, canonical, 1, 0)
    _legacy_add_combinations(r_by_pos, d_by_pos, legacy, 1, 0)

    r_tokens = [f'{h}pos{p}' for p, h in r_by_pos.items()]
    d_tokens = [f'{h}pos{p}' for p, h in d_by_pos.items()]

    for left in r_tokens:
        for right in d_tokens:
            assert (functions._lookup_vs_winrate(canonical, left, right)
                    == functions._lookup_vs_winrate(legacy, left, right))
        for d1, d2 in combinations(d_tokens, 2):
            duo = ",".join([d1, d2])
            assert (functions._lookup_vs_winrate(canonical, left, duo)
                    == functions._lookup_vs_winrate(legacy, left, duo))
    for a, b in combinations(r_tokens, 2):
        assert (functions._lookup_with_winrate(canonical, a, b)
                == functions._lookup_with_winrate(legacy, a, b))
    for trio in combinations(r_tokens, 3):
        for perm in permutations(trio):
            assert (functions._lookup_unordered_combo_winrate(canonical, list(perm))
                    == functions._lookup_unordered_combo_winrate(legacy, list(perm)))


def test_kills_window_reader_matches_legacy_dictionary(monkeypatch):
    """calculate_kills_window_advantage читает канонический словарь как прежний.

    Legacy-словарь строится без trio (их в kills не собирали), поэтому
    канонический тут тоже берётся без trio — сверяются одни и те же слои.
    """
    monkeypatch.setattr(analise_database, "KILLS_WINDOW_BUILD_HIGH_ORDER", True)
    r_by_pos, d_by_pos = _draft()
    canonical = {}
    legacy = {}
    for offset, diffs in enumerate(([3.0, 1.0, -2.0, 0.5], [-1.0, 2.0, 4.0, -3.0])):
        analise_database._add_kills_window_combinations(r_by_pos, d_by_pos, canonical, diffs)
        # прежний билдер: пары в обоих порядках
        r_items, d_items = list(r_by_pos.items()), list(d_by_pos.items())
        for pos_num, hero_id in r_items:
            analise_database._append_kills_window_entry(
                legacy, f'{hero_id}pos{pos_num}', diffs, invert=False)
        for pos_num, hero_id in d_items:
            analise_database._append_kills_window_entry(
                legacy, f'{hero_id}pos{pos_num}', diffs, invert=True)
        for r_pos, r_hero in r_items:
            for d_pos1, d_hero1 in d_items:
                for d_pos2, d_hero2 in d_items:
                    if d_hero1 == d_hero2:
                        continue
                    analise_database._append_kills_window_entry(
                        legacy,
                        f'{r_hero}pos{r_pos}_vs_{d_hero1}pos{d_pos1},{d_hero2}pos{d_pos2}',
                        diffs, invert=False)
        for r_pos1, r_hero1 in r_items:
            for r_pos2, r_hero2 in r_items:
                if r_hero1 == r_hero2:
                    continue
                for d_pos, d_hero in d_items:
                    analise_database._append_kills_window_entry(
                        legacy,
                        f'{r_hero1}pos{r_pos1},{r_hero2}pos{r_pos2}_vs_{d_hero}pos{d_pos}',
                        diffs, invert=False)
        for r_pos, r_hero in r_items:
            for d_pos, d_hero in d_items:
                analise_database._append_kills_window_entry(
                    legacy, f'{r_hero}pos{r_pos}_vs_{d_hero}pos{d_pos}', diffs, invert=False)
        for r_pos1, r_hero1 in r_items:
            for r_pos2, r_hero2 in r_items:
                if r_hero1 == r_hero2:
                    continue
                analise_database._append_kills_window_entry(
                    legacy, f'{r_hero1}pos{r_pos1}_with_{r_hero2}pos{r_pos2}', diffs, invert=False)
        for d_pos1, d_hero1 in d_items:
            for d_pos2, d_hero2 in d_items:
                if d_hero1 == d_hero2:
                    continue
                analise_database._append_kills_window_entry(
                    legacy, f'{d_hero1}pos{d_pos1}_with_{d_hero2}pos{d_pos2}', diffs, invert=True)

    canonical = {k: v for k, v in canonical.items()
                 if not (k.count(',') == 2 and '_vs_' not in k)}

    r_tokens = [f'{h}pos{p}' for p, h in r_by_pos.items()]
    d_tokens = [f'{h}pos{p}' for p, h in d_by_pos.items()]
    for policy in ("core_1v1_with", "first_hit", "blend_all", "best_abs"):
        monkeypatch.setenv("KILLS_WINDOW_LAYER_POLICY", policy)
        monkeypatch.setenv("KILLS_WINDOW_MIN_GAMES", "1")
        got = functions.calculate_kills_window_advantage(r_tokens, d_tokens, canonical, window=None)
        expected = functions.calculate_kills_window_advantage(r_tokens, d_tokens, legacy, window=None)
        assert got == expected, policy
        assert got["10_20"] is not None
