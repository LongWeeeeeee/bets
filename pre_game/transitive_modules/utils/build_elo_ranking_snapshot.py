#!/usr/bin/env python3
"""Утилита для построения Elo-ранкинга команд на срезе времени.

Использует:
- ростер-ориентированный Elo (сброс при смене кора <4 игроков);
- веса по тиру турнира (MAJOR/PROFESSIONAL/AMATEUR/QUALIFIER);
- поправку за формат серии (BO3/BO5);
- горизонт по умолчанию ~1 год.

Пример использования из корня репозитория:

    python3 transitive_modules/build_elo_ranking_snapshot.py --top-n 50

Можно указать дату среза и горизонт в днях:

    python3 transitive_modules/build_elo_ranking_snapshot.py \
        --date 2025-11-01 \
        --max-days 365 \
        --top-n 30
"""

import argparse
from datetime import datetime
from typing import Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from core.transitive_analyzer import DEFAULT_ELO_PARAMS, TransitiveAnalyzer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Построение Elo-ранкинга по про-матчам")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Дата среза в формате YYYY-MM-DD (по умолчанию сейчас)",
    )
    parser.add_argument(
        "--timestamp",
        type=int,
        default=None,
        help="UNIX timestamp среза (если указан, переопределяет --date)",
    )
    parser.add_argument(
        "--max-days",
        type=int,
        default=365,
        help="Горизонт по времени в днях для Elo (по умолчанию 365)",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=50,
        help="Сколько команд показать в таблице (по умолчанию 50)",
    )
    return parser.parse_args()


def resolve_as_of_timestamp(date_str: Optional[str], ts_override: Optional[int]) -> int:
    """Определяет timestamp среза.

    Приоритет: --timestamp > --date > сейчас.
    """
    if ts_override is not None:
        return int(ts_override)

    if date_str:
        # Интерпретируем дату как полночь по UTC этого дня
        dt = datetime.strptime(date_str, "%Y-%m-%d")
    else:
        dt = datetime.utcnow()

    return int(dt.timestamp())


def build_snapshot(as_of_ts: int, max_days: int, top_n: int) -> None:
    analyzer = TransitiveAnalyzer()
    ratings = analyzer.compute_elo_ratings_up_to(
        as_of_timestamp=as_of_ts,
        max_days=max_days,
        **DEFAULT_ELO_PARAMS,
    )

    # Сортировка по рейтингу (по убыванию)
    items = sorted(ratings.items(), key=lambda kv: kv[1], reverse=True)

    as_of_dt = datetime.utcfromtimestamp(as_of_ts)
    print("=" * 80)
    print(
        f"Elo-ранкинг команд на {as_of_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC "
        f"(горизонт {max_days} дн.)"
    )
    print("=" * 80)
    print(f"Top-{top_n}\n")

    header = f"{'#':>3}  {'Команда':40s} {'ID':>10s}  {'Elo':>8s}"
    print(header)
    print("-" * len(header))

    for rank, (team_id, rating) in enumerate(items[:top_n], start=1):
        name = analyzer.get_team_name(team_id)
        print(f"{rank:3d}  {name[:40]:40s} {team_id:10d}  {rating:8.1f}")


def main() -> None:
    args = parse_args()
    as_of_ts = resolve_as_of_timestamp(args.date, args.timestamp)
    build_snapshot(as_of_ts=as_of_ts, max_days=args.max_days, top_n=args.top_n)


if __name__ == "__main__":
    main()
