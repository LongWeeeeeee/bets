"""Контракт шкалы боевого предматчевого скорера.

E-166: `imp_recent` без /100 даёт −0.116 AUC на тесте 26 016.
`vs_wr` в обучении — разность двух сторон, не p−0.5.
"""
from __future__ import annotations

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


def test_vs_wr_is_two_sided_difference():
    """`vs_wr` — РАЗНОСТЬ двух сторон, а не `p − 0.5` одной.

    Раньше здесь сверялась точная строка `vs_val = pair_wr(...) - pair_wr(...)`.
    При разнесении расчёта по ключам данных величина стала присваиваться прямо
    в словарь признаков и переехала на две строки, и тест начал падать на
    верном коде. Проверяется сам контракт: обе ориентации есть и вычитаются.
    """
    text = " ".join(SRC.read_text(encoding="utf-8").split())
    assert ("pair_wr(radiant_heroes, dire_heroes) - pair_wr(dire_heroes, radiant_heroes)"
            in text)
    assert "float(np.mean([(w + 5.0) / (g + 10.0) for w, g in vv])) - 0.5" not in text
