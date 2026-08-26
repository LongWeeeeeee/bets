"""Последняя карта серии обязана применяться к рейтингу.

23.08.2026, серия Team Spirit — TEAM VISION 3:2. Решающая карта `8960991322`
закончилась в 16:13, Stratz отдал её исход, но к 18:20 она всё ещё висела в
очереди неприменённой.

Причина структурная: аварийный подбор доигранных серий определял победителя
ТОЛЬКО по сдвигу счёта серии, а счёт после её конца брать негде — живого фида
уже нет. Для всех карт, кроме последней, счёт приносила регистрация следующей
карты; у последней такой не бывает. Отсюда и наблюдение E-224 «в живой ELO
попадает ровно одна карта на серию».

Контракт: когда счёт недоступен, исход берётся у Stratz по match_id карты, а
счёт достраивается — победа прибавляется тому слоту, за которым карта осталась.
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

SOURCE = (BASE_DIR / "cyberscore_try.py").read_text(encoding="utf-8")


def _sweep_function() -> ast.FunctionDef:
    tree = ast.parse(SOURCE)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_finalize_orphaned_live_elo_series":
            return node
    raise AssertionError("аварийный подбор не найден")


def test_sweep_falls_back_to_the_outcome_lookup() -> None:
    """Без счёта подбор обязан спросить исход, а не молча пропустить серию."""
    fn = _sweep_function()
    names = {n.id for n in ast.walk(fn) if isinstance(n, ast.Name)}
    assert "_live_elo_winner_lookup" in names, (
        "подбор снова полагается только на счёт серии — последняя карта "
        "останется неприменённой")


def test_score_is_reconstructed_from_the_winner() -> None:
    """Счёт достраивается: победа прибавляется слоту победителя."""
    fn = _sweep_function()
    body = ast.dump(fn)
    assert "first_team_is_radiant" in body, "ориентация слота потеряна"
    assert "prev_first" in body and "prev_second" in body, (
        "счёт больше не достраивается от предыдущего")


def test_known_score_still_wins_over_the_lookup() -> None:
    """Когда счёт есть, он и решает — справка нужна только как запасной путь."""
    fn = _sweep_function()
    for node in ast.walk(fn):
        if not isinstance(node, ast.If):
            continue
        test = ast.dump(node.test)
        if "finished_scores" in test and "None" in test:
            fallback = ast.dump(ast.Module(body=node.body, type_ignores=[]))
            primary = ast.dump(ast.Module(body=node.orelse, type_ignores=[]))
            assert "_live_elo_winner_lookup" in fallback, "справка не в запасной ветке"
            assert "_winner_slot_from_series_scores" in primary, (
                "счёт перестал быть основным источником победителя")
            return
    raise AssertionError("развилка «есть счёт / нет счёта» не найдена")
