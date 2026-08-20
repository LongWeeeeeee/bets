#!/usr/bin/env python3
"""Записывать в журнал оценок ВЕТКУ лестницы и разложение по компонентам.

Без ветки в журнале нельзя ни подтвердить, что лестница работает в бою, ни
разобрать потом, какой веткой посчитана конкретная ставка. Разложение по
компонентам кладётся туда же: оно теперь точное (частичные суммы логита), и
прежний самодельный подсчёт долей в `_LAST_FILL` ему больше не нужен — но его
не трогаем, чтобы правка осталась минимальной.

Патч идемпотентен.
"""
from __future__ import annotations
import shutil, time
from pathlib import Path

P = Path("/root/main/base/win_model_veto.py")
s = P.read_text(encoding="utf-8")
if "branch=str(getattr(res" in s:
    print("патч уже применён")
    raise SystemExit(0)
bak = P.with_suffix(f".py.bak_{time.strftime('%Y%m%d_%H%M%S')}_journal")
shutil.copy2(P, bak)
old = '''                      bet=abs(_idx) >= _PREMATCH_MIN_INDEX,
                      reason="ok")'''
new = '''                      branch=str(getattr(res, "branch", "full") or "full"),
                      parts={k: round(float(v), 4) for k, v in
                             (getattr(res, "parts", None) or {}).items()},
                      wr=(None if _LAST_FILL.get("wr") is None
                          or _LAST_FILL["wr"] != _LAST_FILL["wr"]
                          else round(float(_LAST_FILL["wr"]), 4)),
                      bet=abs(_idx) >= _PREMATCH_MIN_INDEX,
                      reason="ok")'''
assert s.count(old) == 1, s.count(old)
P.write_text(s.replace(old, new), encoding="utf-8")
import ast; ast.parse(P.read_text(encoding="utf-8"))
print(f"патч применён, резерв {bak.name}")
