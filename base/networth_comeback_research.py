from __future__ import annotations

import argparse
import glob
import json
import math
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Optional


BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
DEFAULT_MAPS_PATH = ROOT_DIR / "bets_data" / "analise_pub_matches" / "json_parts_split_from_object"
DEFAULT_OUTPUT = ROOT_DIR / "runtime" / "networth_comeback_7.41_300k.json"
DEFAULT_TEXT_OUTPUT = ROOT_DIR / "runtime" / "networth_comeback_7.41_300k.txt"
PATCH_START_TIMES = {
    "7.40": 1765756800,
    "7.41": 1774310400,
}
ALCHEMIST_HERO_ID = 73


try:
    import ijson
except Exception:  # pragma: no cover
    ijson = None

try:
    import orjson
except Exception:  # pragma: no cover
    orjson = None


def _load_json(path: Path) -> Any:
    if orjson is not None:
        return orjson.loads(path.read_bytes())
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _dump_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if orjson is not None:
        path.write_bytes(orjson.dumps(payload, option=orjson.OPT_INDENT_2))
        return
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _iter_json_object_items(path: Path):
    if ijson is not None:
        with path.open("rb") as f:
            yield from ijson.kvitems(f, "", use_float=True)
        return
    data = _load_json(path)
    if isinstance(data, dict):
        yield from data.items()
    else:
        for item in data:
            if isinstance(item, dict):
                yield str(item.get("id", "")), item


def _has_glob_meta(value: str) -> bool:
    return any(ch in value for ch in "*?[]")


def _resolve_maps_paths(maps_path: str | Path, patch: Optional[str] = None) -> list[Path]:
    raw_parts = [part.strip() for part in str(maps_path).split(",") if part.strip()]
    resolved: list[Path] = []
    for raw_part in raw_parts:
        path = Path(raw_part)
        if _has_glob_meta(raw_part):
            matches = sorted(Path(item) for item in glob.glob(raw_part))
        elif path.is_dir():
            pattern = f"{patch}_part*.json" if patch else "*.json"
            matches = sorted(
                item
                for item in path.glob(pattern)
                if item.is_file() and item.name != "merge_patch_summary.json"
            )
        else:
            matches = [path]
        resolved.extend(matches)

    seen: set[Path] = set()
    unique_paths: list[Path] = []
    for path in resolved:
        normalized = path.expanduser()
        key = normalized.resolve() if normalized.exists() else normalized
        if key in seen:
            continue
        seen.add(key)
        unique_paths.append(normalized)

    if not unique_paths:
        suffix = f" for patch {patch}" if patch else ""
        raise FileNotFoundError(f"No map files found{suffix}: {maps_path}")
    missing = [str(path) for path in unique_paths if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Map file does not exist: {missing[0]}")
    return unique_paths


def _winner_from_match(match: dict) -> Optional[str]:
    did_radiant_win = match.get("didRadiantWin")
    if did_radiant_win is None:
        win_rates = match.get("winRates") or []
        if win_rates:
            did_radiant_win = win_rates[-1] > 0.5
    if did_radiant_win is None:
        return None
    return "radiant" if bool(did_radiant_win) else "dire"


def _as_float(value: Any) -> Optional[float]:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value):
        return None
    return value


def _target_label(target: float) -> str:
    return f"{target:.0%}"


def _match_has_hero(match: dict, hero_id: int) -> bool:
    for player in match.get("players") or []:
        if not isinstance(player, dict):
            continue
        try:
            if int(player.get("heroId")) == int(hero_id):
                return True
        except (TypeError, ValueError):
            continue
    return False


def _hero_side_flags(match: dict, hero_id: int) -> tuple[bool, bool]:
    radiant_has = False
    dire_has = False
    for player in match.get("players") or []:
        if not isinstance(player, dict):
            continue
        try:
            if int(player.get("heroId")) != int(hero_id):
                continue
        except (TypeError, ValueError):
            continue
        if bool(player.get("isRadiant")):
            radiant_has = True
        else:
            dire_has = True
    return radiant_has, dire_has


def _empty_minute_bins(start_minute: int, end_minute: int) -> dict[int, dict[int, list[int]]]:
    return {
        minute: defaultdict(lambda: [0, 0]) for minute in range(start_minute, end_minute + 1)
    }


def _iter_valid_matches(
    paths: Iterable[Path],
    *,
    min_start_ts: int,
    max_matches: Optional[int],
    min_minute: int,
):
    scanned = 0
    selected = 0
    skipped = defaultdict(int)
    for path in paths:
        print(f"Reading maps: {path}", flush=True)
        for match_id, match in _iter_json_object_items(path):
            scanned += 1
            if not isinstance(match, dict):
                skipped["not_dict"] += 1
                continue
            if int(match.get("startDateTime") or 0) < int(min_start_ts):
                skipped["too_old"] += 1
                continue
            winner = _winner_from_match(match)
            if winner is None:
                skipped["no_winner"] += 1
                continue
            leads = match.get("radiantNetworthLeads") or []
            if len(leads) < min_minute:
                skipped["too_short"] += 1
                continue
            selected += 1
            yield str(match.get("id") or match_id or ""), match, winner, leads
            if max_matches is not None and selected >= int(max_matches):
                print(
                    f"Selected matches: {selected:,} (scanned={scanned:,}, skipped={dict(skipped)})",
                    flush=True,
                )
                return
            if selected % 25000 == 0:
                print(f"  selected={selected:,} scanned={scanned:,}", flush=True)
    print(f"Selected matches: {selected:,} (scanned={scanned:,}, skipped={dict(skipped)})", flush=True)


def analyze_comebacks(
    maps_paths: list[Path],
    *,
    min_start_ts: int,
    max_matches: int,
    start_minute: int,
    end_minute: int,
    step: int,
    targets: list[float],
    min_samples: int,
    require_end_minute: bool = False,
    split_hero_id: Optional[int] = None,
    split_hero_by_lead_side: bool = False,
) -> dict:
    if split_hero_id is None:
        minute_bins_by_group: dict[str, dict[int, dict[int, list[int]]]] = {
            "all": _empty_minute_bins(start_minute, end_minute)
        }
    elif split_hero_by_lead_side:
        minute_bins_by_group = {
            "alchemist_leading": _empty_minute_bins(start_minute, end_minute),
            "alchemist_trailing": _empty_minute_bins(start_minute, end_minute),
            "no_alchemist": _empty_minute_bins(start_minute, end_minute),
        }
    else:
        minute_bins_by_group = {
            f"with_hero_{split_hero_id}": _empty_minute_bins(start_minute, end_minute),
            f"without_hero_{split_hero_id}": _empty_minute_bins(start_minute, end_minute),
        }
    selected_by_group: dict[str, int] = defaultdict(int)
    match_ids_by_group: dict[str, set[str]] = defaultdict(set)
    observations_by_group: dict[str, int] = defaultdict(int)
    started_at = time.monotonic()
    selected = 0

    for match_id, match, winner, leads in _iter_valid_matches(
        maps_paths,
        min_start_ts=min_start_ts,
        max_matches=max_matches,
        min_minute=end_minute if require_end_minute else start_minute,
    ):
        selected += 1
        fixed_group = None
        radiant_has_split_hero = False
        dire_has_split_hero = False
        if split_hero_id is None:
            fixed_group = "all"
        elif split_hero_by_lead_side:
            radiant_has_split_hero, dire_has_split_hero = _hero_side_flags(match, split_hero_id)
        elif _match_has_hero(match, split_hero_id):
            fixed_group = f"with_hero_{split_hero_id}"
        else:
            fixed_group = f"without_hero_{split_hero_id}"
        if fixed_group is not None:
            selected_by_group[fixed_group] += 1
        for minute in range(start_minute, end_minute + 1):
            if len(leads) < minute:
                continue
            lead = _as_float(leads[minute - 1])
            if lead is None or lead == 0:
                continue
            if fixed_group is not None:
                group = fixed_group
            elif not (radiant_has_split_hero or dire_has_split_hero):
                group = "no_alchemist"
            elif (lead > 0 and radiant_has_split_hero) or (lead < 0 and dire_has_split_hero):
                group = "alchemist_leading"
            else:
                group = "alchemist_trailing"
            abs_lead = abs(lead)
            bucket = int(abs_lead // step) * step
            comeback = (lead > 0 and winner == "dire") or (lead < 0 and winner == "radiant")
            match_ids_by_group[group].add(match_id)
            observations_by_group[group] += 1
            stats = minute_bins_by_group[group][minute][bucket]
            stats[0] += 1
            if comeback:
                stats[1] += 1

    results: dict[str, Any] = {
        "meta": {
            "selected_matches": selected,
            "min_start_ts": int(min_start_ts),
            "start_minute": start_minute,
            "end_minute": end_minute,
            "threshold_step": step,
            "targets": targets,
            "min_samples": min_samples,
            "require_end_minute": require_end_minute,
            "split_hero_id": split_hero_id,
            "split_hero_by_lead_side": split_hero_by_lead_side,
            "paths": [str(path) for path in maps_paths],
            "elapsed_seconds": round(time.monotonic() - started_at, 3),
            "minute_indexing": "minute N uses radiantNetworthLeads[N-1]",
            "eligibility": (
                f"all selected matches have minute {end_minute}"
                if require_end_minute
                else f"selected matches have minute {start_minute}; each minute uses matches that reached that minute"
            ),
        },
    }

    def build_minutes(minute_bins: dict[int, dict[int, list[int]]]) -> dict[str, Any]:
        minutes: dict[str, Any] = {}
        for minute in range(start_minute, end_minute + 1):
            bins = minute_bins[minute]
            exact_bands = []
            for threshold in sorted(bins.keys()):
                count, comebacks = bins[threshold]
                exact_bands.append(
                    {
                        "band_start": threshold,
                        "band_end": threshold + step,
                        "matches": count,
                        "comebacks": comebacks,
                        "comeback_rate": comebacks / count if count else 0.0,
                    }
                )

            exact_targets = {}
            exact_sample_ok = [row for row in exact_bands if row["matches"] >= min_samples]
            for target in targets:
                row = (
                    min(
                        exact_sample_ok,
                        key=lambda item: (
                            abs(item["comeback_rate"] - target),
                            item["band_start"],
                        ),
                    )
                    if exact_sample_ok
                    else None
                )
                exact_targets[_target_label(target)] = {
                    **(
                        row
                        or {
                            "band_start": None,
                            "band_end": None,
                            "matches": 0,
                            "comebacks": 0,
                            "comeback_rate": None,
                        }
                    ),
                    "distance_to_target": (
                        abs(row["comeback_rate"] - target) if row is not None else None
                    ),
                }

            cumulative = []
            total = 0
            comeback_total = 0
            for threshold in sorted(bins.keys(), reverse=True):
                count, comebacks = bins[threshold]
                total += count
                comeback_total += comebacks
                rate = comeback_total / total if total else 0.0
                cumulative.append(
                    {
                        "threshold": threshold,
                        "matches": total,
                        "comebacks": comeback_total,
                        "comeback_rate": rate,
                    }
                )
            cumulative = list(reversed(cumulative))

            by_target = {}
            for target in targets:
                eligible = [
                    row
                    for row in cumulative
                    if row["matches"] >= min_samples and row["comeback_rate"] <= target
                ]
                if eligible:
                    row = min(eligible, key=lambda item: item["threshold"])
                    reached = True
                else:
                    sample_ok = [row for row in cumulative if row["matches"] >= min_samples]
                    row = (
                        min(sample_ok, key=lambda item: abs(item["comeback_rate"] - target))
                        if sample_ok
                        else None
                    )
                    reached = False
                by_target[_target_label(target)] = {
                    "reached": reached,
                    **(
                        row
                        or {
                            "threshold": None,
                            "matches": 0,
                            "comebacks": 0,
                            "comeback_rate": None,
                        }
                    ),
                }

            minutes[str(minute)] = {
                "targets": by_target,
                "thresholds": cumulative,
                "exact_targets": exact_targets,
                "exact_bands": exact_bands,
            }
        return minutes

    if split_hero_id is None:
        results["minutes"] = build_minutes(minute_bins_by_group["all"])
        return results

    results["groups"] = {}
    for group_name, minute_bins in minute_bins_by_group.items():
        results["groups"][group_name] = {
            "selected_matches": selected_by_group[group_name] or len(match_ids_by_group[group_name]),
            "observations": observations_by_group[group_name],
            "minutes": build_minutes(minute_bins),
        }
    return results

def format_text(results: dict) -> str:
    lines = []
    meta = results["meta"]
    lines.append("NETWORTH COMEBACK THRESHOLDS")
    lines.append("=" * 80)
    lines.append(
        f"matches={meta['selected_matches']:,}, minutes={meta['start_minute']}-{meta['end_minute']}, "
        f"step={meta['threshold_step']}, min_samples={meta['min_samples']}"
    )
    lines.append("Minute N uses radiantNetworthLeads[N-1]. Comeback = trailing side at minute N wins map.")
    lines.append("")
    groups = results.get("groups")
    if groups:
        for group_name, group_data in groups.items():
            observations = int(group_data.get("observations") or 0)
            observations_text = f", observations={observations:,}" if observations else ""
            lines.append(
                f"[{group_name}] matches={int(group_data.get('selected_matches') or 0):,}"
                f"{observations_text}"
            )
            for minute in range(meta["start_minute"], meta["end_minute"] + 1):
                minute_data = group_data["minutes"][str(minute)]["exact_targets"]
                parts = []
                for target in [_target_label(target) for target in meta["targets"]]:
                    row = minute_data.get(target)
                    if not row or row.get("band_start") is None:
                        parts.append(f"{target}: N/A")
                        continue
                    rate = row["comeback_rate"] * 100 if row["comeback_rate"] is not None else 0.0
                    parts.append(
                        f"{target}: {int(row['band_start'])}-{int(row['band_end'])} "
                        f"-> {rate:.2f}% comeback (n={int(row['matches'])})"
                    )
                lines.append(f"{minute} min: " + "; ".join(parts))
            lines.append("")
        return "\n".join(lines).rstrip() + "\n"

    for minute in range(meta["start_minute"], meta["end_minute"] + 1):
        minute_data = results["minutes"][str(minute)]["targets"]
        parts = []
        for target in [_target_label(target) for target in meta["targets"]]:
            row = minute_data.get(target)
            if not row or row.get("threshold") is None:
                parts.append(f"{target}: N/A")
                continue
            marker = "" if row.get("reached") else "~"
            rate = row["comeback_rate"] * 100 if row["comeback_rate"] is not None else 0.0
            parts.append(
                f"{target}: {marker}>=abs({int(row['threshold'])}) -> {rate:.2f}% comeback (n={int(row['matches'])})"
            )
        lines.append(f"{minute} min: " + "; ".join(parts))
    return "\n".join(lines) + "\n"


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Public networth comeback threshold research.")
    parser.add_argument("--maps-path", default=str(DEFAULT_MAPS_PATH))
    parser.add_argument("--patch", default="7.41")
    parser.add_argument("--start-date-time", type=int, default=None)
    parser.add_argument("--max-matches", type=int, default=300000)
    parser.add_argument("--start-minute", type=int, default=20)
    parser.add_argument("--end-minute", type=int, default=34)
    parser.add_argument("--step", type=int, default=500)
    parser.add_argument("--targets", default="0.15,0.10,0.05,0.01")
    parser.add_argument("--min-samples", type=int, default=500)
    parser.add_argument("--require-end-minute", action="store_true", default=False)
    parser.add_argument(
        "--split-alchemist",
        action="store_true",
        default=False,
        help="Split results into matches with and without Alchemist.",
    )
    parser.add_argument(
        "--split-alchemist-by-lead-side",
        action="store_true",
        default=False,
        help="Split each minute into Alchemist leading, Alchemist trailing, and no Alchemist.",
    )
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--text-output", default=str(DEFAULT_TEXT_OUTPUT))
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    targets = [float(part.strip()) for part in str(args.targets).split(",") if part.strip()]
    min_start_ts = args.start_date_time
    if min_start_ts is None:
        min_start_ts = PATCH_START_TIMES.get(str(args.patch), 0)
    paths = _resolve_maps_paths(args.maps_path, patch=args.patch)
    results = analyze_comebacks(
        paths,
        min_start_ts=int(min_start_ts),
        max_matches=int(args.max_matches),
        start_minute=int(args.start_minute),
        end_minute=int(args.end_minute),
        step=int(args.step),
        targets=targets,
        min_samples=int(args.min_samples),
        require_end_minute=bool(args.require_end_minute),
        split_hero_id=(
            ALCHEMIST_HERO_ID
            if args.split_alchemist or args.split_alchemist_by_lead_side
            else None
        ),
        split_hero_by_lead_side=bool(args.split_alchemist_by_lead_side),
    )
    output_path = Path(args.output)
    text_output_path = Path(args.text_output)
    _dump_json(output_path, results)
    text_output_path.parent.mkdir(parents=True, exist_ok=True)
    text_output_path.write_text(format_text(results), encoding="utf-8")
    print(f"Saved JSON: {output_path}")
    print(f"Saved text: {text_output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
