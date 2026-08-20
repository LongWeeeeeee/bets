#!/usr/bin/env python3
"""Хирургический патч боевого `win_model_veto.py`: цена ставки берётся ПО ВЕТКЕ.

ЗАЧЕМ. С лестницей веток модель начинает отдавать вердикт там, где раньше
отказывала: покрытие на сквозном прогоне выросло с 42.5% до 99.5%. Но
`model_bet` берёт винрейт из модульной `lan_expected_wr`, то есть из таблицы
ПОЛНОЙ модели. Если ничего не менять, короткие ветки пошли бы в ставку по чужой
цене: при уверенности 60% полная обещает 0.710, а ветка `no_org` на своей
популяции даёт 0.618 — требуемый кэф выходил бы заниженным, и ставка не
покрывала бы риск.

ЧТО ДЕЛАЕТ ПАТЧ.
  1. Заводит список веток, которым разрешена САМОСТОЯТЕЛЬНАЯ ставка. По
     умолчанию `full,no_org`: порог |индекс| >= 8 проверялся на полной модели
     (E-142, 1 769 карт, 70.0%), а для коротких веток такой проверки нет.
     Остальные ветки дают вердикт и панель, но не ставку. Список меняется через
     env, чтобы расширять его по мере накопления ROI.
  2. Запоминает ветку и её винрейт рядом с индексом.
  3. В `model_bet` берёт цену по ветке. Для `full` — БУКВАЛЬНО прежние вызовы,
     чтобы у уже существующих ставок ничего не сдвинулось. Для остальных —
     винрейт ветки и кэф с округлением ВВЕРХ. Винрейт `nan` (полоса ветки не
     откалибрована) означает отказ от ставки.

Патч идемпотентен: повторный запуск ничего не делает.
"""
from __future__ import annotations

import shutil
import sys
import time
from pathlib import Path

P = Path("/root/main/base/win_model_veto.py")


def main() -> None:
    s = P.read_text(encoding="utf-8")
    if "_PREMATCH_BET_BRANCHES" in s:
        print("патч уже применён — ничего не делаю")
        return
    bak = P.with_suffix(f".py.bak_{time.strftime('%Y%m%d_%H%M%S')}_branches")
    shutil.copy2(P, bak)
    print(f"резерв: {bak.name}")

    def rep(old: str, new: str) -> None:
        nonlocal s
        if s.count(old) != 1:
            raise SystemExit(f"якорь встретился {s.count(old)} раз: {old[:70]!r}")
        s = s.replace(old, new)

    rep('_PREMATCH_MIN_INDEX = float(os.getenv("WIN_MODEL_VETO_PREMATCH_MIN", "8"))',
        '''_PREMATCH_MIN_INDEX = float(os.getenv("WIN_MODEL_VETO_PREMATCH_MIN", "8"))
# Ветки лестницы, которым разрешена САМОСТОЯТЕЛЬНАЯ ставка. Порог 8 проверялся
# на полной модели (E-142: 1 769 карт, 72% потока, винрейт 70.0%); для коротких
# веток такой проверки нет, а покрытие с лестницей растёт с 42.5% до 99.5% — то
# есть поток ставок вырос бы вдвое с лишним на непроверенном. Остальные ветки
# отдают вердикт и панель, но не ставку. Расширять список — по накопленному ROI.
_PREMATCH_BET_BRANCHES = tuple(
    x.strip() for x in os.getenv("WIN_MODEL_VETO_PREMATCH_BRANCHES",
                                 "full,no_org").split(",") if x.strip())''')

    rep('_LAST_FILL = {"index": None, "fill": None, "elo": None,\n'
        '               "draft_rank": None, "draft_share": None}',
        '_LAST_FILL = {"index": None, "fill": None, "elo": None,\n'
        '               "draft_rank": None, "draft_share": None,\n'
        '               "branch": None, "wr": None}')

    rep('        _LAST_FILL["elo"] = (getattr(res, "features", None) or {}).get("elo")',
        '''        _LAST_FILL["elo"] = (getattr(res, "features", None) or {}).get("elo")
        # Ветка лестницы и ЕЁ винрейт. Без этого ставка на короткой ветке
        # оценивалась бы таблицей полной модели, а это разные величины.
        _LAST_FILL["branch"] = str(getattr(res, "branch", "full") or "full")
        try:
            _LAST_FILL["wr"] = float(getattr(res, "lan_winrate", float("nan")))
        except (TypeError, ValueError):              # noqa: BLE001
            _LAST_FILL["wr"] = float("nan")''')

    rep('''    return {
        "side": "radiant" if index > 0 else "dire",
        "index": index,
        "confidence": confidence,
        "min_odds": ps.lan_min_odds(confidence),
        "expected_wr": ps.lan_expected_wr(confidence),
    }''',
        '''    # Какой веткой посчитан именно ЭТОТ индекс. Сверка по значению — тот же
    # приём, что у `_LAST_FILL` в остальных местах модуля.
    branch = "full"
    wr = None
    if _LAST_FILL.get("index") is not None:
        try:
            if abs(float(index) - float(_LAST_FILL["index"])) < 1e-6:
                branch = str(_LAST_FILL.get("branch") or "full")
                _w = _LAST_FILL.get("wr")
                wr = float(_w) if _w is not None else None
        except (TypeError, ValueError):              # noqa: BLE001
            pass
    if branch not in _PREMATCH_BET_BRANCHES:
        return None
    if branch == "full":
        # БУКВАЛЬНО прежнее поведение: у ставок, которые идут и сегодня, цена
        # не имеет права сдвинуться ни на копейку.
        min_odds = ps.lan_min_odds(confidence)
        expected_wr = ps.lan_expected_wr(confidence)
    else:
        if wr is None or wr != wr:                   # nan: полоса не откалибрована
            return None
        expected_wr = wr
        min_odds = _math.ceil(100.0 / max(wr, 1e-9)) / 100.0
    return {
        "side": "radiant" if index > 0 else "dire",
        "index": index,
        "confidence": confidence,
        "branch": branch,
        "min_odds": min_odds,
        "expected_wr": expected_wr,
    }''')

    rep("import os\nimport threading", "import math as _math\nimport os\nimport threading")

    P.write_text(s, encoding="utf-8")
    import ast
    ast.parse(s)
    print("патч применён, синтаксис в порядке")


if __name__ == "__main__":
    main()
