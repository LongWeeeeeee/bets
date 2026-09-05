#!/usr/bin/env python3
"""Собрать дельту из полного рантайм-состояния — одноразовый переход на E-255.

ЗАЧЕМ. Живой путь переходит с полного состояния (`live_elo_model_state.json`,
519 МБ, перезапись целиком после каждой карты) на дельту поверх базовых
массивов (`runtime/live_elo_delta.json`). Накопленные обновления терять нельзя,
поэтому перед переключением они перекладываются в дельту: сравниваются
`model_state` рантайм-состояния и снимка, и в дельту попадают только
отличающиеся записи.

КОГДА ЗАПУСКАТЬ. На боевой машине при остановленном проде, ПОСЛЕ доставки
снимка и перебазировки (`ELO/rebase_runtime_model_state.py`): оба инструмента
читают одни и те же файлы. Если база рантайм-состояния не совпадает со снимком —
конвертер отказывается: значит состояние собрано от чужого снимка, и его надо
сначала перебазировать, а не конвертировать.

ЧТО ПРОВЕРЯЕТ. Удаления дельта выразить не может (формат — список изменённых
пар). Живой путь записи только добавляет и обновляет (`del self.` в `models.py`
нет), поэтому найденные удаления означают, что состояние собрано не этим кодом,
и конвертация останавливается с перечнем.

Запуск: venv/bin/python3 ELO/convert_state_to_delta.py [--state PATH] [--delta PATH]
       venv/bin/python3 ELO/convert_state_to_delta.py --if-stale
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ELO import live_team_strength as lts  # noqa: E402
from ELO import state_overlay  # noqa: E402

#: Поля с ключом-кортежем (игрок, позиция) в JSON хранятся строкой "11|POSITION_1".
_PAIR_KEY_FIELDS = {"player_role_local", "player_role_local_last_seen_ts"}
#: Вложенный словарь {игрок: {org: число}} — тоже пары.
_NESTED_PAIR_FIELDS = {"player_current_org_matches"}


def _decode_pair(raw: str) -> tuple[int, str]:
    player, _, rest = str(raw).rpartition("|")
    return int(player), rest


def _diff_flat(field: str, base: dict, live: dict, key_kind: str, val_kind: str,
               removed: list) -> list:
    out = []
    for key, value in live.items():
        decoded = _decode_pair(key) if key_kind == "pair" else (
            int(key) if key_kind == "int" else str(key))
        casted = state_overlay._cast_value(value, val_kind)
        if key not in base:
            out.append([decoded, casted])
            continue
        if state_overlay._cast_value(base[key], val_kind) != casted:
            out.append([decoded, casted])
    for key in base:
        if key not in live:
            removed.append(f"{field}:{key}")
    return out


def _diff_nested_pairs(field: str, base: dict, live: dict, removed: list) -> list:
    """{игрок: {org: число}} -> список пар [(игрок, org), число]."""
    flat_base = {str(p) + "|" + str(o): v for p, orgs in base.items()
                 for o, v in (orgs or {}).items()}
    flat_live = {str(p) + "|" + str(o): v for p, orgs in live.items()
                 for o, v in (orgs or {}).items()}
    return _diff_flat(field, flat_base, flat_live, "pair", "int", removed)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--snapshot", type=Path, default=lts.DEFAULT_SNAPSHOT_PATH)
    parser.add_argument("--state", type=Path, default=lts.DEFAULT_RUNTIME_MODEL_STATE_PATH)
    parser.add_argument("--delta", type=Path, default=lts.DEFAULT_LIVE_DELTA_PATH)
    parser.add_argument("--if-stale", action="store_true",
                        help="собрать дельту, только если текущая не привязана "
                             "к этому снимку")
    parser.add_argument("--force", action="store_true",
                        help="конвертировать даже если база состояния не совпадает со снимком")
    args = parser.parse_args(argv)

    if not args.snapshot.exists():
        print(f"ОШИБКА: снимок не найден: {args.snapshot}", file=sys.stderr)
        return 1

    print(f"читаю снимок {args.snapshot} ...", flush=True)
    with args.snapshot.open(encoding="utf-8") as fh:
        snapshot = json.load(fh)

    reference = lts._snapshot_reference_timestamp(snapshot)
    signature = lts._snapshot_model_config_signature(snapshot)
    if args.if_stale and state_overlay.load_delta(
            args.delta,
            base_reference_timestamp=reference,
            base_model_config_signature=signature,
    ) is not None:
        print(f"дельта уже совпадает со снимком ({reference}), не тронута "
              "— живые обновления сохранены")
        return 0

    if not args.state.exists():
        print(f"ОШИБКА: рантайм-состояние не найдено: {args.state}", file=sys.stderr)
        return 1
    print(f"читаю состояние {args.state} ...", flush=True)
    with args.state.open(encoding="utf-8") as fh:
        state = json.load(fh)
    state_reference = int(state.get("base_reference_timestamp") or 0)
    state_signature = str(state.get("base_model_config_signature") or "")
    if state_reference != reference or state_signature != signature:
        print(f"ОШИБКА: состояние собрано от другой базы "
              f"({state_reference}/{state_signature[:12]}… против "
              f"{reference}/{signature[:12]}…). Сначала "
              f"`ELO/rebase_runtime_model_state.py`.", file=sys.stderr)
        if not args.force:
            return 1
        print("--force: продолжаю, дельта будет привязана к базе снимка", flush=True)

    base_state = snapshot.get("model_state") or {}
    live_state = state.get("model_state") or {}
    if not isinstance(base_state, dict) or not isinstance(live_state, dict):
        print("ОШИБКА: model_state отсутствует в одном из файлов", file=sys.stderr)
        return 1

    changes: dict[str, object] = {}
    removed: list[str] = []
    total = 0
    for field, (scope, key_kind, val_kind) in state_overlay.FIELD_SPECS.items():
        base_field = base_state.get(field) or {}
        live_field = live_state.get(field) or {}
        if scope == "tiered":
            per_tier = {}
            for tier in ("TIER1", "TIER2", "TIER3"):
                rows = _diff_flat(field, base_field.get(tier) or {},
                                  live_field.get(tier) or {}, key_kind, val_kind,
                                  removed)
                if rows:
                    per_tier[tier] = rows
                    total += len(rows)
            if per_tier:
                changes[field] = per_tier
        elif field in _NESTED_PAIR_FIELDS:
            rows = _diff_nested_pairs(field, base_field, live_field, removed)
            if rows:
                changes[field] = rows
                total += len(rows)
        else:
            rows = _diff_flat(field, base_field, live_field, key_kind, val_kind, removed)
            if rows:
                changes[field] = rows
                total += len(rows)

    if removed:
        print(f"ОШИБКА: в состоянии отсутствуют {len(removed)} записей, которые есть "
              f"в базе (примеры: {removed[:5]}). Дельта удаления не выражает — "
              f"конвертация остановлена.", file=sys.stderr)
        return 1

    small_parts = {
        "current_patch_key": live_state.get("current_patch_key"),
        "side_bias": live_state.get("side_bias") or {},
        "roster_tracker": live_state.get("roster_tracker") or {},
    }
    state_overlay.save_delta(
        args.delta,
        base_reference_timestamp=reference,
        base_model_config_signature=signature,
        changes=changes,
        resets={},
        small_parts=small_parts,
        updated_at=int(time.time()),
    )
    print(f"дельта собрана: {args.delta}")
    print(f"  изменённых записей: {total} в {len(changes)} полях")
    print(f"  размер: {args.delta.stat().st_size / 1024:.1f} КБ "
          f"(полное состояние: {args.state.stat().st_size / 1048576:.0f} МБ)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
