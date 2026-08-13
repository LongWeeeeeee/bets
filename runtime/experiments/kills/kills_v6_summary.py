#!/usr/bin/env python3
"""Одна таблица по всем прогонам: цель × оценщик × корпус, чтобы не переносить руками.

Собирает все `report_*.json`, которые написали `kills_v3_train.py`,
`kills_v4_gbdt.py` и `kills_v5_skellam.py`, и печатает AUC на тесте в одном месте.
Переписывание чисел глазами — самый дешёвый способ соврать в отчёте, поэтому
итоговая таблица собирается кодом.

Запуск:
    venv_catboost/bin/python3 runtime/experiments/kills/kills_v6_summary.py
"""
from __future__ import annotations

import json
import os
from pathlib import Path

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
OUT_DIR = ROOT / "runtime/artifacts/kills/window_model_v2"
OUT = ROOT / "runtime/artifacts/kills/kills_summary.md"

SOURCES = [
    ("v3 линейная A (драфт)", "report_v3{c}.json", ("targets", "A_draft", "auc")),
    ("v3 линейная B (признаки)", "report_v3{c}.json", ("targets", "B_features", "auc")),
    ("v3 линейная C (связка)", "report_v3{c}.json", ("targets", "C_stacked", "auc")),
    ("v3 перенос паблик→про", "report_v3prot.json", ("targets", "T_transfer", "auc")),
    ("v4 бустинг", "report_v4gbdt_{corpus}.json", ("targets", "G_gbdt", "auc")),
    ("v4 бустинг + драфт", "report_v4gbdt_{corpus}.json", ("targets", "GS_stacked", "auc")),
    ("v4 ПОТОЛОК (с ингеймом)", "report_v4ceil_{corpus}.json", ("targets", "GS_stacked", "auc")),
    ("v5 регрессия разницы", "report_v5skellam_{corpus}.json", ("targets", "R_diff", "auc")),
    ("v5 Скеллам", "report_v5skellam_{corpus}.json", ("targets", "S_skellam", "auc")),
]


def dig(data: dict, target: str, path: tuple) -> float | None:
    node = data.get(path[0], {}).get(target)
    if not isinstance(node, dict):
        return None
    node = node.get(path[1])
    if not isinstance(node, dict):
        return None
    v = node.get(path[2])
    return float(v) if isinstance(v, (int, float)) else None


def main() -> int:
    lines = ["# Килы: все прогоны в одной таблице", "",
             "AUC на тестовом срезе. Пусто — прогон такой цели не делал.", ""]
    for corpus, suffix in (("public", "pub"), ("pro", "pro")):
        loaded: list[tuple[str, dict]] = []
        for label, pattern, path in SOURCES:
            fname = pattern.format(c=suffix, corpus=corpus)
            p = OUT_DIR / fname
            if not p.exists():
                continue
            try:
                loaded.append((label, json.loads(p.read_text(encoding="utf-8")), path))
            except json.JSONDecodeError:
                continue
        if not loaded:
            continue
        targets: list[str] = []
        for _, data, _ in loaded:
            for t in data.get("targets", {}):
                if t not in targets:
                    targets.append(t)
        order = [t for t in ("w_5_15", "w_10_20", "w_15_25", "w_20_30",
                             "tot_5_15", "tot_10_20", "tot_15_25", "tot_20_30",
                             "map", "ge27", "tot51") if t in targets]
        order += [t for t in targets if t not in order]
        lines += [f"## {corpus}", "",
                  "| оценщик | " + " | ".join(order) + " |",
                  "|---|" + "---:|" * len(order)]
        for label, data, path in loaded:
            row = []
            for t in order:
                v = dig(data, t, path)
                row.append("—" if v is None else f"{v:.4f}")
            lines.append(f"| {label} | " + " | ".join(row) + " |")
        base = loaded[0][1]
        n = base.get("split", {}).get("test")
        lines += ["", f"Тестовых карт в срезе: {n:,}." if isinstance(n, int) else "", ""]
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    print("OUT:", OUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
