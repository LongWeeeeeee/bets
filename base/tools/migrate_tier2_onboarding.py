#!/usr/bin/env python3
"""Переносит legacy tier2-onboarding блоки из id_to_names.py в JSON-overlay.

ЗАЧЕМ. До 02.09.2026 рантайм дописывал каждую новую tier2-команду Python-блоком
прямо в отслеживаемый `base/id_to_names.py` (611 блоков на serv1). С этой даты
записи уезжают в `id_to_names_dynamic_tier2.json` рядом со справочником. Старые
блоки продолжают исполняться при импорте, поэтому ПОКА файл не чищен — ничего не
теряется. Но любая будущая очистка файла от блоков ОБЯЗАНА идти после запуска
этого инструмента: без overlay записи из удалённых блоков потеряются.

Идемпотентен: повторный запуск ничего не дописывает.

Запуск на машине, где лежит id_to_names.py (по умолчанию — рядом с репозиторием):
    venv_catboost/bin/python3 base/tools/migrate_tier2_onboarding.py
    # или с явными путями:
    venv_catboost/bin/python3 base/tools/migrate_tier2_onboarding.py \
        --names /path/to/id_to_names.py --overlay /path/to/overlay.json
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import tier_dynamic_overlay  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--names",
        type=Path,
        default=tier_dynamic_overlay.BASE_DIR / "id_to_names.py",
        help="путь к id_to_names.py (по умолчанию — в репозитории)",
    )
    parser.add_argument(
        "--overlay",
        type=Path,
        default=None,
        help="путь к overlay JSON (по умолчанию — рядом с --names)",
    )
    args = parser.parse_args(argv)

    names_path = Path(args.names).resolve()
    overlay = (
        Path(args.overlay).resolve()
        if args.overlay
        else tier_dynamic_overlay.overlay_path(names_path.parent)
    )
    if not names_path.exists():
        print(f"ОШИБКА: {names_path} не найден", file=sys.stderr)
        return 1

    harvested = tier_dynamic_overlay.harvest_legacy_blocks(names_path)
    captured = tier_dynamic_overlay.migrate_legacy(names_path, overlay)
    total = tier_dynamic_overlay.load_entries(overlay)
    print(f"источник: {names_path}")
    print(f"overlay:  {overlay}")
    print(f"legacy-блоков распознано: {sum(len(v) for v in harvested.values())} id в {len(harvested)} ключах")
    print(f"новых id записано в overlay: {captured}")
    print(f"всего ключей в overlay: {len(total)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
