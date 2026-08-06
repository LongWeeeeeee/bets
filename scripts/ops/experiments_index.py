#!/usr/bin/env python3
"""Пересобирает реестр экспериментов в docs/EXPERIMENTS.md из фронтматтера записей.

Зачем генератор, а не ручная таблица. Реестр — единственное, что читается ВСЕГДА
(правило индекса в AGENTS.md), поэтому расхождение реестра с записями отравляет
всю базу знаний: агент видит строку «отвергнуто», открывает файл и находит другое.
Руками такая таблица разъезжается на третьей записи.

Источник правды — фронтматтер файлов `docs/experiments/E-NN-*.md`.

Режимы:
  python3 scripts/ops/experiments_index.py           # переписать реестр
  python3 scripts/ops/experiments_index.py --check   # только проверить (для хука)
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ENTRIES = ROOT / "docs" / "experiments"
JOURNAL = ROOT / "docs" / "EXPERIMENTS.md"
BEGIN = "<!-- BEGIN GENERATED REGISTRY -->"
END = "<!-- END GENERATED REGISTRY -->"

AREA_TITLES = {
    "draft-cp": "Драфт-метрики и словари",
    "kills": "Kills-окна",
    "filters": "Фильтрация матчей",
    "star-dispatch": "STAR и диспетчеризация",
    "elo": "ELO",
    "stratz": "Сбор данных и Stratz",
    "odds": "Котировки и рынок",
    "ml": "ML-модели",
    "misc": "Прочее",
}


def front(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---"):
        return {}
    end = text.find("\n---", 3)
    if end < 0:
        return {}
    out = {}
    for line in text[3:end].strip().split("\n"):
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        out[k.strip()] = v.strip().strip('"')
    return out


def build() -> str:
    rows = []
    for p in sorted(ENTRIES.glob("E-*.md")):
        fm = front(p)
        if not fm.get("id"):
            print(f"пропущен без фронтматтера: {p.name}", file=sys.stderr)
            continue
        num = int(re.sub(r"\D", "", fm["id"]) or 0)
        rows.append((num, fm, p.name))
    rows.sort()

    out = [BEGIN, ""]
    out.append(f"Записей: **{len(rows)}**. Полный текст — в `docs/experiments/`; "
               "здесь только строка на запись, чтобы реестр можно было читать целиком.")
    out.append("")
    by_area: dict[str, list] = {}
    for num, fm, name in rows:
        by_area.setdefault(fm.get("area", "misc"), []).append((num, fm, name))
    for area in sorted(by_area, key=lambda a: (-len(by_area[a]), a)):
        out.append(f"### {AREA_TITLES.get(area, area)}")
        out.append("")
        out.append("| ID | что проверяли | корпус | вердикт |")
        out.append("|---|---|---|---|")
        for num, fm, name in by_area[area]:
            mark = "" if fm.get("status") == "full" else " ⚠"
            out.append(f"| [{fm['id']}](experiments/{name}){mark} | {fm.get('title', '')} "
                       f"| {fm.get('corpus', '')} | {fm.get('verdict', '')} |")
        out.append("")
    out.append("⚠ — запись зафиксирована только сводкой, полного тела нет.")
    out.append("")
    out.append(END)
    return "\n".join(out)


def main() -> int:
    check = "--check" in sys.argv
    text = JOURNAL.read_text(encoding="utf-8")
    if BEGIN not in text or END not in text:
        print("в docs/EXPERIMENTS.md нет маркеров реестра", file=sys.stderr)
        return 2
    head, rest = text.split(BEGIN, 1)
    _, tail = rest.split(END, 1)
    new = head + build() + tail
    if new == text:
        print("реестр актуален")
        return 0
    if check:
        print("РЕЕСТР УСТАРЕЛ: запусти scripts/ops/experiments_index.py", file=sys.stderr)
        return 1
    JOURNAL.write_text(new, encoding="utf-8")
    print(f"реестр пересобран: {len(list(ENTRIES.glob('E-*.md')))} записей")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
