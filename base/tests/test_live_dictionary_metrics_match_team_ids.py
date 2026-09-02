"""Матч-словарь, который `_run_local_dictionary_metrics()` отдаёт вето-модели,
обязан нести id обеих команд — иначе фикс h2h (см. `test_prematch_h2h_team_id.py`
и `test_win_model_veto.py`) в бою не работает: `_prematch_index` получает
`match` без `radiant_team_id`/`dire_team_id` и держит h2h_resid нулём даже при
доступной истории.

Функция — вложенный замкнутый closure внутри гигантской функции обработки
живой карты, вызвать её напрямую нельзя (десятки внешних переменных из
охватывающей области). Проверяем СТРУКТУРУ AST, как уже делает
`test_live_elo_applied_before_verdict.py` для соседних веток того же файла.
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

SOURCE = (BASE_DIR / "cyberscore_try.py").read_text(encoding="utf-8")


def _dictionary_metrics_function() -> ast.FunctionDef:
    tree = ast.parse(SOURCE)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_run_local_dictionary_metrics":
            return node
    raise AssertionError("_run_local_dictionary_metrics пропала из cyberscore_try.py")


def _match_dict_literal(fn: ast.FunctionDef) -> ast.Dict:
    """Словарь-литерал, переданный как `match=` в вызов `synergy_and_counterpick`."""
    for node in ast.walk(fn):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == "synergy_and_counterpick"):
            continue
        for kw in node.keywords:
            if kw.arg == "match" and isinstance(kw.value, ast.Dict):
                return kw.value
    raise AssertionError("вызов synergy_and_counterpick(match={...}) не найден")


def _dict_keys(node: ast.Dict) -> set[str]:
    keys = set()
    for k in node.keys:
        if isinstance(k, ast.Constant) and isinstance(k.value, str):
            keys.add(k.value)
    return keys


def test_dictionary_metrics_match_dict_carries_team_ids_for_h2h():
    fn = _dictionary_metrics_function()
    match_dict = _match_dict_literal(fn)
    keys = _dict_keys(match_dict)
    assert "radiant_team_id" in keys, (
        f"match={{...}} без radiant_team_id -> h2h_resid в бою всегда 0; ключи: {sorted(keys)}")
    assert "dire_team_id" in keys, (
        f"match={{...}} без dire_team_id -> h2h_resid в бою всегда 0; ключи: {sorted(keys)}")


def test_dictionary_metrics_match_dict_team_ids_reference_outer_scope_variables():
    """Значения — переменные `radiant_team_id`/`dire_team_id`, а не литералы/заглушки."""
    fn = _dictionary_metrics_function()
    match_dict = _match_dict_literal(fn)
    values_by_key = {
        k.value: v for k, v in zip(match_dict.keys, match_dict.values)
        if isinstance(k, ast.Constant) and isinstance(k.value, str)
    }
    for key in ("radiant_team_id", "dire_team_id"):
        value_node = values_by_key.get(key)
        assert isinstance(value_node, ast.Name) and value_node.id == key, (
            f"{key} в match={{...}} обязан быть переменной {key} из внешней области, "
            f"а не {ast.dump(value_node) if value_node else 'отсутствует'}")
