#!/usr/bin/env python3
"""Пересборка `live_probability_segment_policy.json` из отчёта grid-эксперимента.

Политика переводит разрыв рейтингов в вероятность по ФИКСИРОВАННЫМ границам
бакетов. Границы — это квантили распределения рейтингов на том корпусе, где их
считали (текущий файл: 28.03.2026). Любая пересборка ELO на другом корпусе сдвигает
и разрыв, и средний уровень рейтингов, а вариант `blend_avg_k50` считает
`abs_diff * (1 + 5000/avg_strength)` — то есть зависит от абсолютного уровня явно.
Старые границы после пересборки указывают не туда, и это не падает, а тихо портит
вероятности.

Генератора у файла не было — его собрали руками. Этот скрипт закрывает дыру:
берёт `elo_winrate_grid_report.json` и раскладывает его в формат политики,
выбирая для каждого сегмента лучший из двух режимов по log loss (сетка бакетов
против прямой вероятности из разницы рейтингов).

    python3 -m ELO.build_live_probability_policy \\
        --report ELO/output/elo_winrate_grid_experiment/elo_winrate_grid_report.json \\
        --output ELO/live_probability_segment_policy.json --version 2026-08-12-segment-grid-v2
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ELO.live_team_strength import (  # noqa: E402
    SEGMENT_OVERALL,
    SEGMENT_TIER1_ONLY,
    SEGMENT_TIER1_VS_TIER2,
    SEGMENT_TIER2_ONLY,
)

# Ровно те сегменты, которые умеет вернуть `_known_team_segment`. SEGMENT_OTHER
# намеренно не пишем: на нём политика падает обратно на overall, а отдельная
# сетка для «всё остальное» в мартовском отчёте не выиграла у прямой вероятности.
POLICY_SEGMENTS = (SEGMENT_OVERALL, SEGMENT_TIER1_ONLY, SEGMENT_TIER2_ONLY, SEGMENT_TIER1_VS_TIER2)


def _segment_policy(segment: dict[str, Any]) -> dict[str, Any] | None:
    direct = segment.get("direct_series_prob_baseline") or {}
    grid = segment.get("best_variant_grid")
    variant = segment.get("best_variant_by_log_loss")
    variant_result = (segment.get("variant_results") or {}).get(variant) if variant else None

    direct_ll = direct.get("favorite_log_loss")
    grid_ll = (variant_result or {}).get("favorite_log_loss")

    if not isinstance(grid, dict) or grid_ll is None:
        if direct_ll is None:
            return None
        return {
            "mode": "direct_series_prob",
            "favorite_accuracy": direct.get("favorite_accuracy"),
            "favorite_log_loss": direct_ll,
            "favorite_brier": direct.get("favorite_brier"),
        }

    # сетка берётся только если она реально лучше прямой вероятности по log loss
    if direct_ll is not None and float(direct_ll) <= float(grid_ll):
        return {
            "mode": "direct_series_prob",
            "favorite_accuracy": direct.get("favorite_accuracy"),
            "favorite_log_loss": direct_ll,
            "favorite_brier": direct.get("favorite_brier"),
        }

    probs = grid.get("smoothed_probs")
    if not isinstance(probs, list) or not probs:
        probs = [row.get("smoothed_favorite_wr") for row in (grid.get("buckets") or [])]
    return {
        "mode": "grid",
        "variant": grid.get("variant") or variant,
        "favorite_accuracy": (variant_result or {}).get("favorite_accuracy"),
        "favorite_log_loss": grid_ll,
        "favorite_brier": (variant_result or {}).get("favorite_brier"),
        "edges": [float(x) for x in (grid.get("edges") or [])],
        "bucket_probs": [float(x) for x in probs],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--version", type=str, required=True)
    parser.add_argument("--dry-run", action="store_true", help="только показать, файл не писать")
    args = parser.parse_args()

    report = json.loads(args.report.read_text(encoding="utf-8"))
    segments_in = report.get("segments") or {}
    segments_out: dict[str, Any] = {}
    for name in POLICY_SEGMENTS:
        raw = segments_in.get(name)
        if not isinstance(raw, dict):
            print(f"! сегмента {name} нет в отчёте — пропуск")
            continue
        built = _segment_policy(raw)
        if built is None:
            print(f"! сегмент {name}: ни сетки, ни прямой вероятности — пропуск")
            continue
        segments_out[name] = built
        edges = built.get("edges")
        print(f"{name:16s} mode={built['mode']:<19s} variant={str(built.get('variant')):<14s} "
              f"log_loss={built['favorite_log_loss']:.4f} "
              f"бакетов={len(edges) + 1 if edges else '-'}")

    if not segments_out:
        raise SystemExit("политика пустая — отчёт не подходит")

    policy = {
        "policy_version": args.version,
        "generated_at_utc": datetime.now(tz=timezone.utc).isoformat(),
        "source_report": str(args.report),
        "segments": segments_out,
    }
    if args.dry_run:
        print("\n--dry-run: файл не записан")
        return
    # rebuild-then-replace: сначала .tmp, потом атомарная замена
    tmp = args.output.with_suffix(args.output.suffix + ".tmp")
    tmp.write_text(json.dumps(policy, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(args.output)
    print(f"\nзаписано: {args.output}")


if __name__ == "__main__":
    main()
