#!/usr/bin/env python3
"""Заводит новую запись эксперимента: файл, фронтматтер, следующий номер.

Смысл — убрать трение. Правило «записывать в том же ходу» держится ровно до тех
пор, пока запись стоит одну команду; как только надо вспоминать номер, придумывать
имя файла и копировать шапку, правило начинают нарушать.

    python3 scripts/ops/new_experiment.py "название записи" --area kills
    python3 scripts/ops/experiments_index.py    # пересобрать реестр

Область (`--area`) нужна для группировки в реестре: draft-cp, kills, filters,
star-dispatch, elo, stratz, odds, ml, misc.
"""
from __future__ import annotations

import argparse
import datetime as dt
import re
import subprocess
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ENTRIES = ROOT / "docs" / "experiments"
TEMPLATE = ENTRIES / "TEMPLATE.md"
AREAS = ("draft-cp", "kills", "filters", "star-dispatch", "elo", "stratz",
         "odds", "ml", "misc")

TRANSLIT = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e", "ж": "zh",
    "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m", "н": "n", "о": "o",
    "п": "p", "р": "r", "с": "s", "т": "t", "у": "u", "ф": "f", "х": "h", "ц": "ts",
    "ч": "ch", "ш": "sh", "щ": "sch", "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu",
    "я": "ya",
}


def slugify(s: str) -> str:
    out = []
    for ch in s.lower():
        if ch in TRANSLIT:
            out.append(TRANSLIT[ch])
        elif ch.isalnum():
            out.append(ch)
        elif ch in " -_/":
            out.append("-")
    s = unicodedata.normalize("NFKD", "".join(out)).encode("ascii", "ignore").decode()
    return re.sub(r"-+", "-", s).strip("-")[:48].strip("-")


def next_number() -> int:
    nums = [int(m.group(1)) for p in ENTRIES.glob("E-*.md")
            if (m := re.match(r"E-(\d+)-", p.name))]
    return max(nums, default=0) + 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("title", help="название записи")
    ap.add_argument("--area", default="misc", choices=AREAS)
    ap.add_argument("--corpus", default="", help="на чём мерилось")
    args = ap.parse_args()

    num = next_number()
    slug = slugify(args.title) or f"e{num}"
    path = ENTRIES / f"E-{num:02d}-{slug}.md"
    if path.exists():
        print(f"уже существует: {path}", file=sys.stderr)
        return 1

    body = TEMPLATE.read_text(encoding="utf-8") if TEMPLATE.exists() else ""
    body = body.split("---\n", 2)[-1] if body.startswith("---") else body
    # У шаблона свой заголовок «# E-NN. Название» — свой мы уже поставили выше,
    # иначе в новой записи окажутся два h1 подряд.
    body = re.sub(r"^\s*#\s+E-NN\..*?\n", "", body, count=1)
    today = dt.date.today().isoformat()
    head = "\n".join([
        "---",
        f"id: E-{num:02d}",
        f'title: "{args.title}"',
        f'date: "{today}"',
        f"area: {args.area}",
        "status: full",
        f'corpus: "{args.corpus}"',
        'verdict: "в работе"',
        'harness: ""',
        "---",
        "",
        f"# E-{num:02d}. {args.title}",
        "",
    ])
    path.write_text(head + body.lstrip("\n"), encoding="utf-8")
    print(f"создано: {path.relative_to(ROOT)}")
    print("не забудь: python3 scripts/ops/experiments_index.py")
    try:
        subprocess.run(["git", "add", str(path)], cwd=ROOT, check=False,
                       capture_output=True)
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
