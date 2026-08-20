#!/usr/bin/env python3
"""Доля драфта печатается с десятыми: `+0%` теряет и знак, и величину.

На карте Falcons — Nigma доля драфта была 0.002, и в сообщении это выглядело как
«драфт +0%». Прочитать это можно двояко: «драфт возражает и его переехали» или
«драфт молчит». Разница существенная — вторая ситуация обычная и, по замеру,
даже более выигрышная (винрейт 0.719 против 0.660 там, где драфт решает).

Одна десятая различает эти случаи и не удлиняет строку.
Патч идемпотентен.
"""
from __future__ import annotations
import shutil, time
from pathlib import Path
P = Path("/root/main/base/cyberscore_try.py")
s = P.read_text(encoding="utf-8")
old = 'line += f" | драфт {_draft_radiant_oriented:+.0%}"'
new = 'line += f" | драфт {_draft_radiant_oriented:+.1%}"'
if new in s:
    print("патч уже применён"); raise SystemExit(0)
if s.count(old) != 1:
    raise SystemExit(f"якорь встретился {s.count(old)} раз")
bak = P.with_suffix(f".py.bak_{time.strftime('%Y%m%d_%H%M%S')}_draft_precision")
shutil.copy2(P, bak)
P.write_text(s.replace(old, new), encoding="utf-8")
import ast; ast.parse(P.read_text(encoding="utf-8"))
print(f"патч применён, резерв {bak.name}")
