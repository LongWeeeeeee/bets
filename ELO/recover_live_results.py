#!/usr/bin/env python3
"""Offline-only, fail-closed recovery of live ELO results.

The tool never reads or writes the default runtime paths.  It builds a staged
full state and progress ledger from an explicit snapshot, progress, delta and
event ledger.  Every event must carry a complete serialised ``MatchRecord``
and an explicit model-event timestamp; duplicates, wrong base signatures and
incomplete legacy entries stop before any output is replaced.
"""
from __future__ import annotations

import argparse
import copy
import gc
import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ELO import live_team_strength as lts  # noqa: E402
from ELO import state_overlay  # noqa: E402
from ELO.domain import MatchRecord  # noqa: E402
from ELO.models import HybridPlayerRosterEloModel  # noqa: E402
from ELO.replay import result_record  # noqa: E402


class RecoveryInputError(ValueError):
    """Inputs do not prove a safe recovery."""


REQUIRED_RECORD_FIELDS = frozenset({
    "match_id", "timestamp", "radiant_team_id", "radiant_team_name",
    "dire_team_id", "dire_team_name", "radiant_player_ids", "dire_player_ids",
    "league_id", "league_name", "source_league_tier", "series_id",
    "series_type", "source_patch", "duration_seconds", "derived_league_tier",
})


def _read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as fh:
            payload = json.load(fh)
    except (OSError, ValueError) as exc:
        raise RecoveryInputError(f"непрочитываемый JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise RecoveryInputError(f"ожидался JSON-объект: {path}")
    return payload


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            try:
                os.remove(tmp_name)
            except FileNotFoundError:
                pass


def _as_int(raw: Any, label: str) -> int:
    if isinstance(raw, bool):
        raise RecoveryInputError(f"{label}: ожидалось целое")
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise RecoveryInputError(f"{label}: ожидалось целое") from exc


def _validate_record(raw: Any, *, radiant_win: bool, label: str) -> MatchRecord:
    if not isinstance(raw, dict):
        raise RecoveryInputError(f"{label}: отсутствует match_record")
    missing = sorted(REQUIRED_RECORD_FIELDS.difference(raw))
    if missing:
        raise RecoveryInputError(f"{label}: неполный match_record ({', '.join(missing)})")
    record = lts._deserialize_match_record(raw, radiant_win=radiant_win)
    if record is None:
        raise RecoveryInputError(f"{label}: неразбираемый match_record")
    if record.timestamp <= 0 or len(record.radiant_player_ids) != 5 or len(record.dire_player_ids) != 5:
        raise RecoveryInputError(f"{label}: нужны timestamp и ровно 5+5 account IDs")
    if len(set(record.radiant_player_ids) | set(record.dire_player_ids)) != 10:
        raise RecoveryInputError(f"{label}: account IDs сторон пересекаются или повторяются")
    return record


def _event_from_entry(map_key: str, entry: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(entry, dict):
        raise RecoveryInputError(f"{label}: applied entry не объект")
    radiant_win = entry.get("radiant_win")
    if not isinstance(radiant_win, bool):
        raise RecoveryInputError(f"{label}: отсутствует radiant_win")
    record = _validate_record(entry.get("match_record"), radiant_win=radiant_win, label=label)
    result_timestamp = _as_int(entry.get("result_timestamp"), f"{label}.result_timestamp")
    if result_timestamp < record.timestamp:
        raise RecoveryInputError(f"{label}: result_timestamp раньше начала карты")
    if _as_int(entry.get("match_id"), f"{label}.match_id") != record.match_id:
        raise RecoveryInputError(f"{label}: match_id не совпадает с match_record")
    return {
        "map_key": str(map_key),
        "entry": copy.deepcopy(entry),
        "record": record,
        "radiant_win": radiant_win,
        "result_timestamp": result_timestamp,
        "provenance": str(entry.get("provenance") or "existing_progress"),
    }


def _event_from_ledger(raw: Any, *, index: int) -> dict[str, Any]:
    label = f"events[{index}]"
    if not isinstance(raw, dict):
        raise RecoveryInputError(f"{label}: event не объект")
    map_key = str(raw.get("map_key") or "").strip()
    if not map_key:
        raise RecoveryInputError(f"{label}: отсутствует map_key")
    radiant_win = raw.get("radiant_win")
    if not isinstance(radiant_win, bool):
        raise RecoveryInputError(f"{label}: отсутствует radiant_win")
    record_raw = raw.get("match_record")
    record = _validate_record(record_raw, radiant_win=radiant_win, label=label)
    result_timestamp = _as_int(raw.get("result_timestamp"), f"{label}.result_timestamp")
    if result_timestamp < record.timestamp:
        raise RecoveryInputError(f"{label}: result_timestamp раньше начала карты")
    entry = {
        "match_id": record.match_id,
        "radiant_win": radiant_win,
        "result_timestamp": result_timestamp,
        "applied_at": result_timestamp,
        "match_record": copy.deepcopy(record_raw),
        "provenance": str(raw.get("provenance") or "explicit_recovery"),
    }
    for key in ("series_key", "series_url", "winner_slot", "original_applied_at_unknown"):
        if key in raw:
            entry[key] = copy.deepcopy(raw[key])
    return {
        "map_key": map_key,
        "entry": entry,
        "record": record,
        "radiant_win": radiant_win,
        "result_timestamp": result_timestamp,
        "repair_existing": raw.get("repair_existing") is True,
        "provenance": entry["provenance"],
    }


def _validate_base(snapshot: dict[str, Any], progress: dict[str, Any], delta: dict[str, Any]) -> tuple[int, str]:
    reference = lts._snapshot_reference_timestamp(snapshot)
    signature = lts._snapshot_model_config_signature(snapshot)
    if reference <= 0 or not signature or not isinstance(snapshot.get("model_state"), dict):
        raise RecoveryInputError("snapshot не содержит base reference/config/model_state")
    for label, payload in (("progress", progress), ("delta", delta)):
        if lts._payload_base(payload) != (reference, signature):
            raise RecoveryInputError(f"{label} не принадлежит переданному snapshot")
    return reference, signature


def _validate_pending(*, pending: dict[str, Any], blocked_map_keys: set[str],
                      blocked_match_ids: set[int]) -> None:
    """Reject ambiguous pending data rather than carrying a future duplicate."""
    seen_map_series: dict[str, str] = {}
    seen_match_series: dict[int, str] = {}
    for raw_series_key, series in pending.items():
        series_key = str(raw_series_key)
        if not isinstance(series, dict):
            raise RecoveryInputError(f"pending_series[{series_key}] не объект")
        queue = series.get("pending_maps")
        mirror = series.get("pending_map")
        if not isinstance(queue, list) or not queue or not isinstance(mirror, dict):
            raise RecoveryInputError(f"pending_series[{series_key}] не содержит полные pending_maps/pending_map")
        if mirror != queue[0]:
            raise RecoveryInputError(f"pending_series[{series_key}]: pending mirror не совпадает с началом очереди")
        entries = [*queue, mirror]
        local_maps: dict[str, int] = {}
        for index, raw in enumerate(entries):
            label = f"pending_series[{series_key}][{index}]"
            if not isinstance(raw, dict):
                raise RecoveryInputError(f"{label} не объект")
            map_key = str(raw.get("map_key") or "").strip()
            if not map_key:
                raise RecoveryInputError(f"{label}: отсутствует map_key")
            record = _validate_record(raw.get("match_record"), radiant_win=False, label=label)
            previous = local_maps.get(map_key)
            if previous is not None and previous != record.match_id:
                raise RecoveryInputError(f"{label}: pending mirror меняет match_id для {map_key}")
            local_maps[map_key] = record.match_id
        for map_key, match_id in local_maps.items():
            if map_key in blocked_map_keys or match_id in blocked_match_ids:
                raise RecoveryInputError(f"pending {map_key} пересекается с applied/recovered map")
            other_series = seen_map_series.setdefault(map_key, series_key)
            if other_series != series_key:
                raise RecoveryInputError(f"pending {map_key} повторяется в series {other_series}/{series_key}")
            other_match_series = seen_match_series.setdefault(match_id, series_key)
            if other_match_series != series_key:
                raise RecoveryInputError(f"pending match_id {match_id} повторяется в series {other_match_series}/{series_key}")


def _lineup_summary(model: HybridPlayerRosterEloModel, record: MatchRecord, *, timestamp: int) -> dict[str, Any]:
    summary = lts._preview_live_matchup_from_model(model=model, match=result_record(record, timestamp))
    return {
        "radiant": round(float(summary["radiant"]["base_rating"]), 9),
        "dire": round(float(summary["dire"]["base_rating"]), 9),
        "difference": round(float(summary["elo_diff"]), 9),
    }


def _apply_results(model: Any, events: list[dict[str, Any]]) -> None:
    for event in sorted(events, key=lambda item: (item["result_timestamp"], item["record"].match_id,
                                                   item["map_key"])):
        model.process_match(result_record(event["record"], event["result_timestamp"]))


def _overlay_parts(model: Any) -> dict[str, Any]:
    wrappers = getattr(model, "_overlay_wrappers", None)
    if not isinstance(wrappers, dict):
        raise RecoveryInputError("overlay model не предоставил изменяемые поля")
    return {
        "small_parts": state_overlay.collect_small_parts(model),
        "resets": state_overlay.collect_resets(wrappers),
        "changes": state_overlay.collect_changes(wrappers),
    }


def _is_overlay_delta(delta: dict[str, Any]) -> bool:
    return "delta_version" in delta


def _verify_current_delta(*, snapshot_path: Path, delta_path: Path, snapshot: dict[str, Any], delta: dict[str, Any],
                          repaired: list[dict[str, Any]], applied_count: int,
                          reference: int, signature: str) -> str:
    """Prove the incomplete current ledger is reconstructed exactly before additions.

    This is intentionally opt-in: recovery of an older, already complete ledger
    may carry more entries than the current delta.  For an incident with a known
    single truncated entry it prevents adding recovered events to an unproven
    baseline.
    """
    if len(repaired) != applied_count:
        raise RecoveryInputError("baseline-проверка требует repair_existing для каждой applied map")
    if _is_overlay_delta(delta):
        if state_overlay.load_delta(delta_path,
                                    base_reference_timestamp=reference,
                                    base_model_config_signature=signature) is None:
            raise RecoveryInputError("delta overlay невалидна для переданного snapshot")
        try:
            from ELO.array_model import build_overlay_model
            model = build_overlay_model(snapshot_path, None)
        except Exception as exc:  # noqa: BLE001
            raise RecoveryInputError("не удалось собрать базовую overlay-модель для baseline-проверки") from exc
        try:
            _apply_results(model, repaired)
            expected = _overlay_parts(model)
            actual = {key: delta.get(key) or {} for key in ("small_parts", "resets", "changes")}
            if expected != actual:
                raise RecoveryInputError("delta не совпадает с replay текущих applied maps; recovery остановлен")
        finally:
            del model
            from ELO import array_model
            array_model._OVERLAY_CACHE.clear()
            array_model._READ_CACHE.clear()
            gc.collect()
        return "overlay"
    raw_delta_state = delta.get("model_state")
    if not isinstance(raw_delta_state, dict):
        raise RecoveryInputError("delta не содержит model_state для baseline-проверки")
    model = HybridPlayerRosterEloModel.from_state(snapshot["model_state"])
    _apply_results(model, repaired)
    if model.export_state() != raw_delta_state:
        raise RecoveryInputError("delta не совпадает с replay текущих applied maps; recovery остановлен")
    return "full_state"


def recover(*, snapshot_path: Path, progress_path: Path, delta_path: Path, events_path: Path,
            output_state: Path, output_progress: Path, output_report: Path,
            verify_current_delta: bool = False) -> dict[str, Any]:
    input_paths = (snapshot_path, progress_path, delta_path, events_path)
    outputs = (output_state, output_progress, output_report)
    if any(out.resolve() in {path.resolve() for path in input_paths} for out in outputs):
        raise RecoveryInputError("output не может совпадать с input")
    snapshot = _read_json(snapshot_path)
    progress = _read_json(progress_path)
    delta = _read_json(delta_path)
    ledger = _read_json(events_path)
    reference, signature = _validate_base(snapshot, progress, delta)
    if "pending_overlay_commit" in progress:
        raise RecoveryInputError("progress содержит pending_overlay_commit; recovery требует завершить или откатить commit")
    raw_events = ledger.get("events")
    if not isinstance(raw_events, list) or not raw_events:
        raise RecoveryInputError("events ledger должен содержать непустой events[]")

    applied = progress.get("applied_maps")
    pending = progress.get("pending_series")
    if not isinstance(applied, dict) or not isinstance(pending, dict):
        raise RecoveryInputError("progress не содержит applied_maps/pending_series")
    events_by_key = {str(raw.get("map_key") or "").strip(): _event_from_ledger(raw, index=index)
                     for index, raw in enumerate(raw_events) if isinstance(raw, dict)}
    if len(events_by_key) != len(raw_events) or "" in events_by_key:
        raise RecoveryInputError("events содержит дублирующийся или пустой map_key")

    replay_events: list[dict[str, Any]] = []
    repaired_entries: dict[str, dict[str, Any]] = {}
    known_ids: set[int] = set()
    for map_key, entry in applied.items():
        key = str(map_key)
        replacement = events_by_key.pop(key, None)
        if replacement is not None:
            if not replacement["repair_existing"]:
                raise RecoveryInputError(f"{key}: replacement требует repair_existing=true")
            if not isinstance(entry, dict) or _as_int(entry.get("match_id"), f"{key}.match_id") != replacement["record"].match_id:
                raise RecoveryInputError(f"{key}: repair_existing не совпадает с existing match_id")
            replay_events.append(replacement)
            repaired_entries[key] = replacement["entry"]
            known_ids.add(replacement["record"].match_id)
            continue
        existing = _event_from_entry(key, entry, label=f"applied_maps[{key}]")
        if existing["record"].match_id in known_ids:
            raise RecoveryInputError(f"duplicate existing match_id {existing['record'].match_id}")
        known_ids.add(existing["record"].match_id)
        replay_events.append(existing)

    new_entries: dict[str, dict[str, Any]] = {}
    for key, event in events_by_key.items():
        if event["repair_existing"]:
            raise RecoveryInputError(f"{key}: repair_existing не найден в progress")
        if event["record"].match_id in known_ids:
            raise RecoveryInputError(f"duplicate recovered match_id {event['record'].match_id}")
        known_ids.add(event["record"].match_id)
        replay_events.append(event)
        new_entries[key] = event["entry"]

    _validate_pending(pending=pending, blocked_map_keys={event["map_key"] for event in replay_events},
                      blocked_match_ids=known_ids)

    delta_format = "overlay" if _is_overlay_delta(delta) else "full_state"
    if verify_current_delta:
        delta_format = _verify_current_delta(
            snapshot_path=snapshot_path,
            delta_path=delta_path,
            snapshot=snapshot,
            delta=delta,
            repaired=[event for event in replay_events if event["map_key"] in repaired_entries],
            applied_count=len(applied),
            reference=reference,
            signature=signature,
        )
    raw_model_state = snapshot.pop("model_state")
    model = HybridPlayerRosterEloModel.from_state(raw_model_state)
    del raw_model_state
    gc.collect()
    diagnostics: list[dict[str, Any]] = []
    for event in sorted(replay_events, key=lambda item: (item["result_timestamp"], item["record"].match_id, item["map_key"])):
        record = event["record"]
        before = _lineup_summary(model, record, timestamp=event["result_timestamp"])
        model.process_match(result_record(record, event["result_timestamp"]))
        after = _lineup_summary(model, record, timestamp=event["result_timestamp"])
        diagnostics.append({
            "map_key": event["map_key"], "match_id": record.match_id,
            "result_timestamp": event["result_timestamp"], "radiant_win": event["radiant_win"],
            "provenance": event["provenance"], "before": before, "after": after,
        })

    output_applied = copy.deepcopy(applied)
    output_applied.update(repaired_entries)
    output_applied.update(new_entries)
    staged_progress = {
        "base_reference_timestamp": reference,
        "base_model_config_signature": signature,
        "pending_series": copy.deepcopy(pending),
        "applied_maps": output_applied,
    }
    staged_state = {
        "base_reference_timestamp": reference,
        "base_model_config_signature": signature,
        "model_state": model.export_state(),
    }
    report = {
        "recovery_version": 1,
        "inputs": {str(path): _sha256(path) for path in input_paths},
        "snapshot_base": {"reference_timestamp": reference, "model_config_signature": signature},
        "replayed_event_count": len(diagnostics),
        "recovered_event_count": len(new_entries),
        "repaired_existing_event_count": len(repaired_entries),
        "current_delta_verified": verify_current_delta,
        "delta_format": delta_format,
        "pending_series_preserved": sorted(pending),
        "applied_map_keys": sorted(output_applied),
        "events": diagnostics,
    }
    _write_json_atomic(output_state, staged_state)
    _write_json_atomic(output_progress, staged_progress)
    _write_json_atomic(output_report, report)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--progress", type=Path, required=True)
    parser.add_argument("--delta", type=Path, required=True)
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--output-state", type=Path, required=True)
    parser.add_argument("--output-progress", type=Path, required=True)
    parser.add_argument("--output-report", type=Path, required=True)
    parser.add_argument("--verify-current-delta", action="store_true",
                        help="требовать, чтобы repaired existing maps точно воспроизводили delta")
    args = parser.parse_args(argv)
    try:
        report = recover(snapshot_path=args.snapshot, progress_path=args.progress, delta_path=args.delta,
                         events_path=args.events, output_state=args.output_state,
                         output_progress=args.output_progress, output_report=args.output_report,
                         verify_current_delta=args.verify_current_delta)
    except RecoveryInputError as exc:
        print(f"ОШИБКА: {exc}", file=sys.stderr)
        return 1
    print(f"staged {report['replayed_event_count']} events; recovered={report['recovered_event_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
