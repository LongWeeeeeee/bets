"""Контракт шкалы боевого предматчевого скорера.

E-166: `imp_recent` без /100 даёт −0.116 AUC на тесте 26 016.
`vs_wr` в обучении — разность двух сторон, не p−0.5.
"""
from __future__ import annotations

import ast
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "prematch_scorer.py"


def test_imp_recent_divides_by_100():
    """Делится ли на 100 — не важно, из какого поля снимка берётся величина.

    Раньше здесь было пришпилено `r["imp30"]`. По E-177 источник заменён на
    `imp_recent10` (окно десять матчей вместо тридцати: два поля из 35 были в
    бою коллинеарны), и тест начал падать на верном коде. Проверяем сам
    контракт масштаба, а не имя поля.
    """
    text = SRC.read_text(encoding="utf-8")
    line = next(ln for ln in text.splitlines() if '"imp_recent":' in ln)
    assert "/ 100.0" in line, line


def _slice_const(node: ast.expr):
    """Значение простого индекса `x["ключ"]` — с оглядкой на ast.Index из <3.9."""
    inner = getattr(node, "value", node) if node.__class__.__name__ == "Index" else node
    return inner.value if isinstance(inner, ast.Constant) else None


def _vs_wr_expression(tree: ast.Module) -> ast.expr:
    """Выражение, которым скорер получает признак `vs_wr`, независимо от написания.

    Понимает оба вида сборки признаков: присваивание в словарь
    (`f["vs_wr"] = ...`) и пару «временная переменная + словарный литерал»
    (`vs_val = ...` ... `"vs_wr": vs_val`).
    """
    named: dict[str, ast.expr] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name):
                named[target.id] = node.value
            elif (isinstance(target, ast.Subscript)
                  and _slice_const(target.slice) == "vs_wr"):
                return node.value
    for node in ast.walk(tree):
        if isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values):
                if isinstance(key, ast.Constant) and key.value == "vs_wr":
                    if isinstance(value, ast.Name) and value.id in named:
                        return named[value.id]
                    return value
    raise AssertionError("в скорере не найдено вычисление признака vs_wr")


def test_vs_wr_is_two_sided_difference():
    """`vs_wr` — разность radiant-vs-dire и dire-vs-radiant, а не p−0.5.

    Двусторонняя форма пришла в скорер дважды и независимо: на main коммитом
    adb1d66 «prematch: двусторонний vs_wr», на serv1 — 4154bc3, где выбор
    подкреплён замером: «односторонняя p-0.5 даёт ровно половину обучающего
    масштаба (sd 0.501 против 1.002)». Поведение в слитой версии сохранено:
    выражение признака и тело `pair_wr` совпадают с main посимвольно после
    разбора.

    Раньше здесь была пришпилена одна конкретная строка исходника —
    `vs_val = pair_wr(radiant_heroes, dire_heroes) - pair_wr(dire_heroes, radiant_heroes)`.
    В 98b127a сборка признаков на serv1 переехала со словарного литерала на
    присваивания по ключам, доступным для ветки: временная `vs_val` исчезла в
    пользу `f["vs_wr"]`, а выражение встало в две строки. Значение то же,
    написание другое — и тест падал на верном коде. По той же причине, что и в
    test_imp_recent_divides_by_100, проверяем контракт, а не написание: разбираем
    AST и требуем зеркальную пару вызовов `pair_wr(A, B) - pair_wr(B, A)`.
    """
    tree = ast.parse(SRC.read_text(encoding="utf-8"))
    expr = _vs_wr_expression(tree)

    assert isinstance(expr, ast.BinOp) and isinstance(expr.op, ast.Sub), (
        f"vs_wr должен быть разностью двух сторон, а не {ast.dump(expr)}")

    for side in (expr.left, expr.right):
        assert (isinstance(side, ast.Call)
                and isinstance(side.func, ast.Name)
                and side.func.id == "pair_wr"
                and len(side.args) == 2), (
            "обе стороны разности должны быть вызовами pair_wr на двух составах, "
            f"а не {ast.dump(side)}")

    first = [ast.dump(arg) for arg in expr.left.args]
    second = [ast.dump(arg) for arg in expr.right.args]
    assert first[0] != first[1], "pair_wr должен сравнивать два разных состава"
    assert second == [first[1], first[0]], (
        "вторая сторона должна быть зеркальной первой — иначе разность "
        "не двусторонняя")

    pair_wr = next(node for node in ast.walk(tree)
                   if isinstance(node, ast.FunctionDef) and node.name == "pair_wr")
    for node in ast.walk(pair_wr):
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Sub):
            assert not (isinstance(node.right, ast.Constant)
                        and node.right.value == 0.5), (
                "односторонний сдвиг p−0.5 вернулся внутрь pair_wr: "
                "половина обучающего масштаба (sd 0.501 против 1.002)")
