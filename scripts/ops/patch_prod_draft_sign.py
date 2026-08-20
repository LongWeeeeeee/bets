#!/usr/bin/env python3
"""Единый знаменатель в строке ML-модели: минус за Dire, плюс за Radiant.

ЗАЧЕМ. В одной строке жили две системы отсчёта, и они выглядели спорящими там,
где спора нет:

    ML-модель: Dire 65.1% | ELO модели: -166 | драфт +6%

`ELO модели` — сырой признак `elo` = (радиант − дайр)/400, то есть ВСЕГДА
ориентация Radiant: −166 значит «Dire сильнее». А `драфт` считался после
переворота вкладов на сторону ставки (`win_model_veto.py`: `if _idx < 0:
_contrib = -_contrib`), то есть +6% значило «за Dire». Обе величины говорили
одно и то же противоположными знаками.

ЧТО ДЕЛАЕТ ПАТЧ. Приводит долю драфта к ориентации Radiant прямо при печати:
умножает её на знак выбранной моделью стороны. Величина (какую долю опоры под
решением даёт драфт) сохраняется, меняется только знак.

    было:  ML-модель: Dire 65.1%    | ELO модели: -166 | драфт +6%
    стало: ML-модель: Dire 65.1%    | ELO модели: -166 | драфт -6%
    было:  ML-модель: Radiant 69.8% | ELO модели: +166 | драфт -7%
    стало: ML-модель: Radiant 69.8% | ELO модели: +166 | драфт -7%   (не меняется)

ВНУТРЕННЮЮ ВЕЛИЧИНУ НЕ ТРОГАЕМ. `_LAST_FILL["draft_share"]` остаётся в
ориентации ставки: на нём стоит `_DRAFT_FIRST_ONLY` (гейт «драфт должен быть
первым среди тянущих в сторону ставки»), и переворот там сменил бы поведение
ставок, а не только текст. Правка живёт ровно на слое печати.

Патч идемпотентен.
"""
from __future__ import annotations

import shutil
import time
from pathlib import Path

P = Path("/root/main/base/cyberscore_try.py")
s = P.read_text(encoding="utf-8")
MARK = "_draft_radiant_oriented"
if MARK in s:
    print("патч уже применён")
    raise SystemExit(0)

old = """        _dr = win_model_veto.last_draft_rank(index)
        if _dr:
            line += f" | драфт {_dr[1]:+.0%}\""""
new = """        _dr = win_model_veto.last_draft_rank(index)
        if _dr:
            # Единый знаменатель со строкой ELO: минус за Dire, плюс за Radiant.
            # `draft_share` считается ПОСЛЕ переворота вкладов на сторону ставки,
            # поэтому его знак означает «за выбранную сторону»; домножаем на знак
            # стороны и получаем ориентацию Radiant, ту же, что у `ELO модели`.
            # Внутреннюю величину не трогаем: на ней стоит гейт _DRAFT_FIRST_ONLY.
            _draft_radiant_oriented = _dr[1] * (1.0 if index > 0 else -1.0)
            line += f" | драфт {_draft_radiant_oriented:+.0%}\""""
if s.count(old) != 1:
    raise SystemExit(f"якорь встретился {s.count(old)} раз")
bak = P.with_suffix(f".py.bak_{time.strftime('%Y%m%d_%H%M%S')}_draft_sign")
shutil.copy2(P, bak)
P.write_text(s.replace(old, new), encoding="utf-8")
import ast
ast.parse(P.read_text(encoding="utf-8"))
print(f"патч применён, резерв {bak.name}")
