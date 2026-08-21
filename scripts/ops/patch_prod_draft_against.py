#!/usr/bin/env python3
"""Запрет ставки, когда драфт тянет против стороны, на которую ставим.

ЗАЧЕМ. Решение обеих боевых моделей раскладывается по компонентам, и в карточке
уже печатается, кто из них тянет против: у предматчевой модели — `вклад: ELO
±x% драфт ±y% игроки ±z%` (E-213, точное разложение логита), у оконных моделей
панели — та же строка по блокам. Разложение до сих пор только показывалось.
Теперь оно решает: сторона, названная ВОПРЕКИ драфту, ставки не даёт.

ЧТО ДЕЛАЕТ ПАТЧ.

  1. `win_model_veto.model_bet` — самостоятельная ставка предматчевой модели.
     Берётся `parts["draft"]` (ориентация Radiant) и приводится к стороне ставки
     знаком индекса. Минус — ставки нет. Отказ печатается по одному разу на
     индекс: функция зовётся на каждом тике диспетчера.

  2. `ml_panel.evaluate` — вердикты панели. Берётся `draft_share`, он стоит уже
     в ориентации ВЕРДИКТА, поэтому минус читается прямо со знака. Такой вердикт
     не выставляется (🔴), причина печатается в строке и пишется в журнал —
     схема журнала поднята до 4. Доля считается только для окон
     (`ML_PANEL_DRAFT_KEYS` = w_5_15..w_20_30), поэтому запрет ровно на них и
     стоит: у `dur43`, `total_55_50` и `rad_30_25` доли нет и гасить нечем.

ЧТО НЕ СЧИТАЕТСЯ ВОЗРАЖЕНИЕМ. Ноль и отсутствие доли. E-217 §1: карты, где
драфту нечего сказать, дают ЛУЧШИЙ винрейт 0.719 против 0.660 там, где драфт
решает, — резать их нечем. «Против» надо доказать, а не предположить.

ОГОВОРКА. Замера именно этого гейта нет. Соседний, E-217 §3, говорит, что
недооценки драфта у модели нет: на сильном аутдрафте она обещает 0.596 при
факте 0.599. Запрет вводится по решению alex, а не по замеру, и снимается без
деплоя: WIN_MODEL_DRAFT_AGAINST_BLOCK=0 и ML_PANEL_DRAFT_AGAINST_BLOCK=0.

Патч идемпотентен. Каталог задаётся первым аргументом (по умолчанию боевой
`/root/main`) — на копии файлов его можно прогнать вхолостую.
"""
from __future__ import annotations

import ast
import shutil
import sys
import time
from pathlib import Path

VETO_MARK = "_DRAFT_AGAINST_BLOCK"
PANEL_MARK = "DRAFT_AGAINST_BLOCK"

VETO_EDITS: list[tuple[str, str]] = [
    # Выключатель. Рядом с `_DRAFT_FIRST_ONLY`: это два правила про один и тот
    # же драфт, и жить им положено на виду друг у друга.
    ('_DRAFT_FIRST_ONLY = str(os.getenv("WIN_MODEL_DRAFT_FIRST_ONLY", "0")).strip() in ("1", "true", "yes", "on")',
     '_DRAFT_FIRST_ONLY = str(os.getenv("WIN_MODEL_DRAFT_FIRST_ONLY", "0")).strip() in ("1", "true", "yes", "on")\n'
     '# Запрет «драфт против стороны ставки». Включён по умолчанию, снимается\n'
     '# WIN_MODEL_DRAFT_AGAINST_BLOCK=0 без деплоя.\n'
     '_DRAFT_AGAINST_BLOCK = str(os.getenv("WIN_MODEL_DRAFT_AGAINST_BLOCK", "1")).strip() in ("1", "true", "yes", "on")\n'
     '#: Отказ печатается по одному разу на индекс: `model_bet` зовётся на\n'
     '#: каждом тике диспетчера, и строка на вызов залила бы лог.\n'
     '_DRAFT_GATE_SEEN: set = set()\n'
     '_DRAFT_GATE_MUTE = False'),

    # Само правило. Отдельной функцией, потому что у него три исхода, и внутри
    # `model_bet` они бы утонули между порогом и ценой.
    ('def last_fill(index):',
     '''def _draft_agrees(index) -> bool:
    """False, только когда драфт ДОКАЗАНО тянет против стороны ставки.

    `parts` — точное разложение логита по компонентам (E-213): сумма частей
    равна самому логиту, знак стоит в ориентации Radiant, и к стороне ставки
    его приводит знак индекса.

    Ноль и отсутствие ключа `draft` (ветка `pre_draft` считается вовсе без
    драфтовых колонок) — это «драфту нечего сказать», а не возражение. E-217 §1:
    карты, где драфт молчит, дают ЛУЧШИЙ винрейт 0.719, резать их нечем.

    Разложения нет вовсе — запрет стоит в стороне, но не молчит: без лестницы
    веток он не сработал бы ни разу, и узнать об этом было бы неоткуда.
    """
    global _DRAFT_GATE_MUTE
    parts = last_parts(index)
    if not parts:
        if not _DRAFT_GATE_MUTE:
            _DRAFT_GATE_MUTE = True
            print("ВНИМАНИЕ: запрет «драфт против стороны» проверять нечем — "
                  "модель не даёт разложения логита по компонентам", flush=True)
        return True
    try:
        side_sign = 1.0 if float(index) > 0 else -1.0
        toward_bet = float(parts.get("draft", 0.0)) * side_sign
    except (TypeError, ValueError):                  # noqa: BLE001
        return True
    if toward_bet >= 0.0:
        return True
    key = round(float(index), 2)
    if key not in _DRAFT_GATE_SEEN:
        if len(_DRAFT_GATE_SEEN) > 20000:
            _DRAFT_GATE_SEEN.clear()
        _DRAFT_GATE_SEEN.add(key)
        print(f"[win_model] ставка отменена: драфт против стороны "
              f"({'radiant' if index > 0 else 'dire'}, индекс {index:+.2f}, "
              f"вклад драфта {parts['draft']:+.4f} в ориентации Radiant)",
              flush=True)
    return False


def last_fill(index):'''),

    # Оговорка в самой функции: правило отбора обязано быть видно там, где оно
    # применяется, а не только в патче, который его принёс.
    ('''    Драфтовый источник сюда не пускается: у него другая шкала и другой винрейт,
    порог 8 на ней не проверялся.
    """''',
     '''    Драфтовый источник сюда не пускается: у него другая шкала и другой винрейт,
    порог 8 на ней не проверялся.

    Сторона, названная ВОПРЕКИ драфту, ставки не даёт: см. `_draft_agrees`.
    Замера у этого правила нет, оно введено решением; снимается
    WIN_MODEL_DRAFT_AGAINST_BLOCK=0.
    """'''),

    ('''    if _DRAFT_FIRST_ONLY:
        _dr = last_draft_rank(index)
        if not _dr or _dr[0] != 1:
            return None''',
     '''    if _DRAFT_FIRST_ONLY:
        _dr = last_draft_rank(index)
        if not _dr or _dr[0] != 1:
            return None
    if _DRAFT_AGAINST_BLOCK and not _draft_agrees(index):
        return None'''),
]

#: Правки панели те же, что легли в репозиторий. Якоря выбраны из строк, которые
#: у двух версий файла совпадают: типизация `parts` и часть комментариев у них
#: расходятся, и брать их в якоря нельзя.
PANEL_EDITS: list[tuple[str, str]] = [
    ('Формат журнала (JSONL, по строке на карту) заморожен: `schema` = 1. Любое',
     '''ДРАФТ ПРОТИВ. Вердикт не выставляется, когда драфтовая компонента решения
тянет ПРОТИВ названной стороны: `draft_share` стоит в ориентации вердикта, и
минус означает, что модель назвала сторону вопреки драфту. Ноль — это «драфту
нечего сказать», а не возражение (E-217 §1), поэтому режется строго минус.
Доли нет вовсе (модель вне `ML_PANEL_DRAFT_KEYS` или SHAP не посчитался) —
запрет не срабатывает: «против» надо доказать, а не предположить.

Формат журнала (JSONL, по строке на карту) заморожен: `schema` = 1. Любое'''),

    ('JOURNAL_SCHEMA = 3   # +parts: разложение решения по ELO/драфту/игрокам',
     'JOURNAL_SCHEMA = 4   # +blocked: чем именно погашен вердикт'),

    ('MIN_FILL = float(os.getenv("ML_PANEL_MIN_FILL", "0.75"))',
     'MIN_FILL = float(os.getenv("ML_PANEL_MIN_FILL", "0.75"))\n'
     '# Запрет «драфт против названной стороны». Выключается ML_PANEL_DRAFT_AGAINST_BLOCK=0\n'
     '# без деплоя — как и порог заполненности, это правило отбора, а не код.\n'
     'DRAFT_AGAINST_BLOCK = os.getenv(\n'
     '    "ML_PANEL_DRAFT_AGAINST_BLOCK", "1") not in ("0", "false", "False")\n'
     '#: Причина в строке и в журнале. Без неё 🔴 по драфту неотличим от 🔴 по порогу.\n'
     'BLOCK_DRAFT_AGAINST = "драфт против"'),

    ('    odds: float | None = None           # кэф безубытка',
     '    odds: float | None = None           # кэф безубытка\n'
     '    blocked: str | None = None          # чем погашен вердикт; None — не гасили'),

    ('    ok = bool(conf >= spec.threshold and fill >= MIN_FILL)',
     '''    ok = bool(conf >= spec.threshold and fill >= MIN_FILL)
    blocked = None
    if ok and DRAFT_AGAINST_BLOCK and draft_share is not None and draft_share < 0.0:
        # Минус в ориентации вердикта — драфт тянет против той стороны, которую
        # называет модель. Такой вердикт не выставляется вовсе.
        ok = False
        blocked = BLOCK_DRAFT_AGAINST'''),

    ('                        band_hit=None if band is None else band[0],',
     '                        blocked=blocked,\n'
     '                        band_hit=None if band is None else band[0],'),

    ('        tail = " ✅" if v.key in star else ""',
     '        if v.blocked:\n'
     '            bits.append(v.blocked)\n'
     '        tail = " ✅" if v.key in star else ""'),

    ('             "band_hit": v.band_hit, "band_n": v.band_n,',
     '             "blocked": v.blocked,\n'
     '             "band_hit": v.band_hit, "band_n": v.band_n,'),
]


def _patch(path: Path, mark: str, edits: list[tuple[str, str]], stamp: str) -> bool:
    """Одна правка одного файла. False — уже пропатчен, трогать нечего."""
    text = path.read_text(encoding="utf-8")
    if mark in text:
        print(f"{path.name}: патч уже применён")
        return False
    for old, new in edits:
        if text.count(old) != 1:
            raise SystemExit(f"{path.name}: якорь встретился {text.count(old)} "
                             f"раз: {old[:70]!r}")
        text = text.replace(old, new)
    ast.parse(text)                                  # до записи, а не после
    backup = path.with_suffix(f".py.bak_{stamp}_draft_against")
    shutil.copy2(path, backup)
    path.write_text(text, encoding="utf-8")
    print(f"{path.name}: применено, резерв {backup.name}")
    return True


def main(argv: list[str]) -> None:
    root = Path(argv[1] if len(argv) > 1 else "/root/main")
    stamp = time.strftime("%Y%m%d_%H%M%S")
    changed = _patch(root / "base" / "win_model_veto.py", VETO_MARK, VETO_EDITS, stamp)
    changed |= _patch(root / "base" / "ml_panel.py", PANEL_MARK, PANEL_EDITS, stamp)
    print("готово" if changed else "нечего делать")


if __name__ == "__main__":
    main(sys.argv)
