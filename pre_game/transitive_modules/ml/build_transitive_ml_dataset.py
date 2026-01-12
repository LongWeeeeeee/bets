#!/usr/bin/env python3
"""Сбор датасета для ML‑модели поверх транзитивного анализатора.

Идея:
- проходим по историческим матчам из TransitiveAnalyzer.matches_data;
- для каждого матча вызываем get_transitiv() так, как в бэктестах
  (as_of_timestamp = startDateTime, чтобы не было data leakage);
- сохраняем ключевые детерминированные фичи из результата + таргет didRadiantWin
  в CSV.

Пример запуска:
    cd transitive_modules
    python3 build_transitive_ml_dataset.py --n-matches 4000 --max-days 30 \
        --out-path transitive_ml_dataset.csv

CSV можно затем использовать в train_transitive_meta_model.py.
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import io
from typing import Dict, Any, List

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from core.transitive_analyzer import TransitiveAnalyzer, get_transitiv


FEATURE_FIELDS: List[str] = [
    # Основные скоры (нормированные/агрегированные)
    "h2h_score",              # нормированный score за серию
    "common_score",           # уже нормированный per-series
    "transitive_score",       # нормированный по количеству цепей
    "total_score",            # финальный score режима, который выбрался внутри get_transitiv
    # Объём информации
    "h2h_series",
    "common_series",
    "transitive_series",
    "total_series",
    "info_units",
    # Elo
    "elo_radiant",
    "elo_dire",
    "elo_diff",
    "elo_score",
    # Мета
    "strength",
    "confidence",            # base_confidence (0..1)
    "period_days",
    
    # =========================================================================
    # НОВЫЕ ФИЧИ
    # =========================================================================
    
    # Form (текущая форма команды)
    "radiant_form",
    "dire_form",
    "form_diff",
    "radiant_form_raw",
    "dire_form_raw",
    "form_diff_raw",
    "radiant_form_games",
    "dire_form_games",
    
    # Streak (серия побед/поражений)
    "radiant_streak",
    "dire_streak",
    "streak_diff",
    "radiant_streak_length",
    "dire_streak_length",
    
    # Momentum (изменение Elo за 14 дней)
    "radiant_momentum",
    "dire_momentum",
    "momentum_diff",
    
    # Activity (дней с последнего матча)
    "radiant_days_since_last",
    "dire_days_since_last",
    "radiant_is_cold",
    "dire_is_cold",
    
    # Consistency (стабильность результатов)
    "radiant_consistency",
    "dire_consistency",
    "consistency_diff",
    "radiant_avg_margin",
    "dire_avg_margin",
    "radiant_clean_sweep_rate",
    "dire_clean_sweep_rate",
    
    # Side stats (винрейт по сторонам)
    "radiant_radiant_wr",
    "radiant_dire_wr",
    "dire_radiant_wr",
    "dire_dire_wr",
    "side_advantage",
    
    # Tier stats (против разных уровней команд)
    "radiant_vs_tier1_wr",
    "radiant_vs_tier2_wr",
    "dire_vs_tier1_wr",
    "dire_vs_tier2_wr",
    "radiant_sos",
    "dire_sos",
    "sos_diff",
    
    # Normalized scores
    "form_score",
    "momentum_score",
    "streak_score",
    "activity_score",
    
    # Signal Agreement
    "signals_agree",
    "signal_conflict",
    "h2h_elo_conflict",
]

CATEGORICAL_FIELDS: List[str] = [
    "decision_mode",          # primary / transitive / unknown
    "confidence_label",       # низкая / средняя / высокая
]

TARGET_FIELD = "radiant_win"


def build_dataset(
    n_matches: int,
    max_days: int,
    min_strength: float,
    out_path: str,
    scenarios: str | None = None,
    skip_transitive: bool = False,
    progress_every: int = 0,
) -> None:
    """Строит CSV датасет поверх get_transitiv.

    n_matches: сколько последних матчей брать из matches_data (по времени).
    max_days: окно в днях, пробрасывается в get_transitiv.
    min_strength: фильтр по strength; слабее считаем шумом и не пишем в датасет.
    out_path: путь к CSV.
    scenarios: зарезервировано на будущее (например, primary_only vs trans_only),
               пока не используется.
    """

    analyzer = TransitiveAnalyzer()
    matches = [m for m in analyzer.matches_data.values() if m.get("startDateTime", 0) > 0]
    matches.sort(key=lambda m: m.get("startDateTime", 0))  # по возрастанию времени

    if n_matches > 0:
        matches = matches[-n_matches:]

    total = len(matches)
    fieldnames = FEATURE_FIELDS + CATEGORICAL_FIELDS + [TARGET_FIELD]

    written = 0
    skipped_no_data = 0
    skipped_weak = 0

    with open(out_path, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()

        for idx, m in enumerate(matches, start=1):
            radiant = m.get("radiantTeam") or {}
            dire = m.get("direTeam") or {}
            rid = radiant.get("id")
            did = dire.get("id")
            rname = radiant.get("name")
            dname = dire.get("name")
            start_ts = m.get("startDateTime", 0)
            radiant_win = m.get("didRadiantWin")

            if not rid or not did or not isinstance(radiant_win, bool) or start_ts <= 0:
                continue

            # Глушим подробный вывод get_transitiv
            with contextlib.redirect_stdout(io.StringIO()):
                res: Dict[str, Any] = get_transitiv(
                    radiant_team_id=rid,
                    dire_team_id=did,
                    radiant_team_name_original=rname,
                    dire_team_name_original=dname,
                    as_of_timestamp=start_ts,
                    analyzer=analyzer,
                    max_days=max_days,
                    use_transitive=not skip_transitive,
                )

            if not res.get("has_data"):
                skipped_no_data += 1
                continue

            strength = float(res.get("strength") or 0.0)
            if strength < min_strength:
                skipped_weak += 1
                continue

            # Приводим prediction к стороне (Radiant/Dire) – но для таргета достаточно факта
            row: Dict[str, Any] = {}
            for f in FEATURE_FIELDS:
                val = res.get(f)
                # безопасно приводим None -> 0.0 / 0
                if isinstance(val, (int, float)):
                    row[f] = val
                elif val is None:
                    row[f] = 0
                else:
                    # на всякий случай: попытка приведения к float
                    try:
                        row[f] = float(val)
                    except Exception:
                        row[f] = 0

            for f in CATEGORICAL_FIELDS:
                v = res.get(f)
                row[f] = str(v) if v is not None else ""

            row[TARGET_FIELD] = 1 if radiant_win else 0

            writer.writerow(row)
            written += 1

            if progress_every > 0 and idx % progress_every == 0:
                print(
                    f"[build_dataset] обработано {idx}/{total} матчей "
                    f"(записано строк: {written}, has_data_skip={skipped_no_data}, strength_skip={skipped_weak})"
                )

    print(f"Датасет сохранён в {out_path}")
    print(f"Строк записано: {written}")
    print(f"Без данных (has_data=False): {skipped_no_data}")
    print(f"Отфильтровано по strength<{min_strength}: {skipped_weak}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Сбор CSV датасета поверх транзитивного анализатора")
    p.add_argument("--n-matches", type=int, default=4000, help="Сколько последних матчей взять (0 = все)")
    p.add_argument("--max-days", type=int, default=30, help="Окно max_days для get_transitiv")
    p.add_argument(
        "--min-strength",
        type=float,
        default=0.0,
        help="Отсечь матчи со strength ниже порога (0.0-1.0)",
    )
    p.add_argument(
        "--out-path",
        type=str,
        default="transitive_ml_dataset.csv",
        help="Путь к выходному CSV",
    )
    p.add_argument(
        "--scenarios",
        type=str,
        default="",
        help="Зарезервировано на будущее (пока не используется)",
    )
    p.add_argument(
        "--skip-transitive",
        action="store_true",
        help="Не считать транзитивные цепочки (use_transitive=False в get_transitiv) — значительно ускоряет сбор",
    )
    p.add_argument(
        "--progress-every",
        type=int,
        default=100,
        help="Печатать прогресс каждые N матчей (0 = без прогресса)",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    build_dataset(
        n_matches=args.n_matches,
        max_days=args.max_days,
        min_strength=args.min_strength,
        out_path=args.out_path,
        scenarios=args.scenarios or None,
        skip_transitive=args.skip_transitive,
        progress_every=args.progress_every,
    )
