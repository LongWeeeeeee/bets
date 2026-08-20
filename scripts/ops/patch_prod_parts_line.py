#!/usr/bin/env python3
"""В строке ML печатается РАСПРЕДЕЛЕНИЕ решения по компонентам, а не одна доля драфта.

ЗАЧЕМ. Раньше строка несла только долю драфта, и по ней нельзя было отличить
«решение держится на ELO» от «решение держится на форме игроков» — а это разные
по качеству ставки: по замеру E-216 винрейт 0.710, когда во главе сила команды,
и 0.661, когда игроки. Человек этого не видел.

ЧТО ПЕЧАТАЕТСЯ. Доля каждого компонента в решении, знак — в ориентации Radiant
(минус за Dire, плюс за Radiant), как и у `ELO модели`. Доли считаются от суммы
МОДУЛЕЙ вкладов, поэтому по модулю дают 100% и видно, кто тянет против.

    вклад: ELO +91% драфт +0% игроки +2% очные +6%

Величины берутся из `ScoreResult.parts` — это ЧАСТИЧНЫЕ СУММЫ ЛОГИТА, а не
оценка: сумма частей сходится с самим логитом до 4.4e-15 (E-213). Прежняя
`draft_share` считалась отдельным самодельным проходом и теперь избыточна, но
она остаётся: на ней стоит гейт WIN_MODEL_DRAFT_FIRST_ONLY.

Артефакт без веток разложения не даёт — тогда печатается прежняя строка с долей
драфта, чтобы на старом артефакте сообщение не обеднело.

Патч идемпотентен.
"""
from __future__ import annotations
import shutil, time
from pathlib import Path

V = Path("/root/main/base/win_model_veto.py")
C = Path("/root/main/base/cyberscore_try.py")
sv, sc = V.read_text(encoding="utf-8"), C.read_text(encoding="utf-8")
if "def last_parts(" in sv:
    print("патч уже применён"); raise SystemExit(0)

stamp = time.strftime("%Y%m%d_%H%M%S")
shutil.copy2(V, V.with_suffix(f".py.bak_{stamp}_parts_line"))
shutil.copy2(C, C.with_suffix(f".py.bak_{stamp}_parts_line"))

old = '''        _LAST_FILL["branch"] = str(getattr(res, "branch", "full") or "full")'''
new = '''        _LAST_FILL["branch"] = str(getattr(res, "branch", "full") or "full")
        # Точное разложение логита по компонентам: сумма частей равна самому
        # логиту, поэтому доли можно печатать как состав решения, а не как оценку.
        _LAST_FILL["parts"] = {str(k): float(v) for k, v in
                               (getattr(res, "parts", None) or {}).items()}'''
assert sv.count(old) == 1
sv = sv.replace(old, new)

old = '''def last_draft_rank(index):'''
new = '''def last_parts(index):
    """Разложение логита по компонентам для ЭТОГО индекса; пусто — если нет."""
    try:
        if _LAST_FILL["index"] is not None and abs(float(index) - float(_LAST_FILL["index"])) < 1e-6:
            return dict(_LAST_FILL.get("parts") or {})
    except (TypeError, ValueError):
        pass
    return {}


def last_draft_rank(index):'''
assert sv.count(old) == 1
sv = sv.replace(old, new)
sv = sv.replace('_LAST_FILL = {"index": None, "fill": None, "elo": None,\n'
                '               "draft_rank": None, "draft_share": None,\n'
                '               "branch": None, "wr": None}',
                '_LAST_FILL = {"index": None, "fill": None, "elo": None,\n'
                '               "draft_rank": None, "draft_share": None,\n'
                '               "branch": None, "wr": None, "parts": None}')

old = '''        _dr = win_model_veto.last_draft_rank(index)
        if _dr:'''
new = '''        _parts = win_model_veto.last_parts(index)
        if _parts:
            # Доли от суммы МОДУЛЕЙ: по модулю дают 100%, и видно, кто тянет
            # против. Знак — в ориентации Radiant, как у `ELO модели`.
            _tot = sum(abs(v) for v in _parts.values()) or 1.0
            _names = (("elo", "ELO"), ("draft", "драфт"),
                      ("players", "игроки"), ("h2h", "очные"))
            _sh = " ".join(f"{_ru} {_parts.get(_k, 0.0)/_tot:+.0%}"
                           for _k, _ru in _names if _k in _parts)
            if _sh:
                line += f" | вклад: {_sh}"
        _dr = win_model_veto.last_draft_rank(index)
        if _dr and not _parts:'''
assert sc.count(old) == 1
sc = sc.replace(old, new)

V.write_text(sv, encoding="utf-8"); C.write_text(sc, encoding="utf-8")
import ast
ast.parse(V.read_text(encoding="utf-8")); ast.parse(C.read_text(encoding="utf-8"))
print(f"патч применён, резервы .bak_{stamp}_parts_line")
