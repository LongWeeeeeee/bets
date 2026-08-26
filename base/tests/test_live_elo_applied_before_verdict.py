"""Исходы доигранных карт применяются ДО вердиктов, а не после цикла.

23.08.2026, серия TEAM VISION — Team Spirit. Порядок строк в боевом логе:

    🗺️ Bookmaker map context: карта 2
    LIVE_ELO_NOT_APPLIED series=8960655084 scores={'first': 0, 'second': 1}
    ✅ ВЕРДИКТ: ставка отправлена (side=radiant, index=+31.04, conf=81.0%)
    📈 Live ELO finalized from orphaned finished series: applied_map=…8960577698

Ставка ушла на радианта (TEAM VISION), а карта 1 — их поражение — применилась
66 строк спустя и уронила рейтинг с 2070.5 до 2056.1. Модель считала вердикт по
рейтингу, который на тот момент был уже неверен.

Причина была структурная: аварийный подбор доигранных серий вызывался ПОСЛЕ
цикла обработки матчей, то есть после всех вердиктов и отправок. Ключи серий,
по которым он отличает доигранное от живого, собирались в том же цикле, поэтому
раньше вызвать его было нельзя. Теперь сбор ключей вынесен в отдельный проход.
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

SOURCE = (BASE_DIR / "cyberscore_try.py").read_text(encoding="utf-8")


def _function_with_orphan_sweep() -> ast.FunctionDef:
    """Функция цикла, в которой живут и сбор ключей, и аварийный подбор."""
    tree = ast.parse(SOURCE)
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        body = ast.dump(node)
        if "_sweep_orphaned_live_elo" in body and "check_head" in body:
            return node
    raise AssertionError("не найдена функция с аварийным подбором и check_head")


def _lines_of(node: ast.AST, name: str) -> list:
    return [child.lineno for child in ast.walk(node)
            if isinstance(child, ast.Name) and child.id == name]


def test_orphan_sweep_runs_before_the_first_verdict() -> None:
    """Хотя бы один вызов подбора обязан стоять выше первого `check_head`."""
    fn = _function_with_orphan_sweep()
    sweeps = _lines_of(fn, "_sweep_orphaned_live_elo")
    verdicts = _lines_of(fn, "check_head")
    assert sweeps, "аварийный подбор пропал из цикла"
    assert verdicts, "обработка матчей пропала из цикла"
    assert min(sweeps) < min(verdicts), (
        f"подбор идёт после вердиктов: первый подбор на строке {min(sweeps)}, "
        f"первый check_head на {min(verdicts)}")


def test_series_keys_are_collected_before_the_sweep() -> None:
    """Подбор отличает доигранное от живого по ключам — они нужны раньше него.

    Проверяются только вызовы, которым ключи ПЕРЕДАЮТСЯ: на пустом фиде подбор
    зовётся с пустым множеством намеренно, там живых серий нет по условию.
    """
    fn = _function_with_orphan_sweep()
    sweeps = [call.lineno for call in ast.walk(fn)
              if isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
              and call.func.id == "_sweep_orphaned_live_elo"
              and any(isinstance(a, ast.Name) and a.id == "seen_series_keys"
                      for a in call.args)]
    assert sweeps, "подбор больше не получает ключи серий"
    keys = [n.lineno for n in ast.walk(fn)
            if isinstance(n, ast.Attribute) and n.attr == "add"
            and isinstance(n.value, ast.Name) and n.value.id == "seen_series_keys"]
    assert keys, "сбор ключей серий пропал"
    assert max(keys) < min(sweeps), (
        "ключи собираются после первого подбора — он увидит пустое множество и "
        "посчитает доигранными живые серии")


def test_sweep_still_runs_at_the_end_of_the_cycle() -> None:
    """Серия могла доиграть уже после сбора ключей — второй проход обязателен."""
    fn = _function_with_orphan_sweep()
    sweeps = _lines_of(fn, "_sweep_orphaned_live_elo")
    verdicts = _lines_of(fn, "check_head")
    assert len(sweeps) >= 2, "второй проход в конце цикла пропал"
    assert max(sweeps) > max(verdicts), "последний подбор обязан идти после обработки"


def test_sweep_runs_when_the_feed_is_empty() -> None:
    """Пустой фид — момент, когда серия доиграла.

    23.08.2026 решающая карта `8960991322` провисела в очереди больше двух
    часов: функция выходила на пустом фиде РАНЬШЕ подбора, а следующей карты
    серии, которая принесла бы счёт, уже не будет.
    """
    fn = _function_with_orphan_sweep()
    sweeps = set(_lines_of(fn, "_sweep_orphaned_live_elo"))
    empties = [n.lineno for n in ast.walk(fn)
               if isinstance(n, ast.Constant) and isinstance(n.value, str)
               and "Live matches empty" in n.value]
    assert empties, "ветка пустого фида пропала"
    assert any(abs(s - empties[0]) <= 12 for s in sweeps), (
        "на пустом фиде подбор не зовётся — последняя карта серии останется "
        "неприменённой до следующей серии тех же команд")
