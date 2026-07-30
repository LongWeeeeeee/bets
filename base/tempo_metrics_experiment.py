from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Optional

try:
    from explore_database import _dump_to_file, _iter_matches
    from functions import check_bad_map, format_output_dict, synergy_and_counterpick
    from tempo_analise_database_experiment import (
        PATCH_739_RELEASE_TS,
        TEMPO_INDEX_SCALES,
        TEMPO_RATE_FIELDS,
        build_tempo_draft_metrics,
        compute_match_total_kills_per_min,
        draft_to_named_payload,
        load_hero_name_map,
        load_tempo_dicts,
    )
except ImportError:  # package import for tests
    from base.explore_database import _dump_to_file, _iter_matches
    from base.functions import check_bad_map, format_output_dict, synergy_and_counterpick
    from base.tempo_analise_database_experiment import (
        PATCH_739_RELEASE_TS,
        TEMPO_INDEX_SCALES,
        TEMPO_RATE_FIELDS,
        build_tempo_draft_metrics,
        compute_match_total_kills_per_min,
        draft_to_named_payload,
        load_hero_name_map,
        load_tempo_dicts,
    )

PRO_DEFAULT_DIR = Path("/Users/alex/Documents/ingame/pro_heroes_data/json_parts_split_from_object")
PUB_STATS_DEFAULT_DIR = Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches")
TEMPO_STATS_DEFAULT_DIR = Path("/Users/alex/Documents/ingame/bets_data/tempo_pub_experiment")
REPORT_DEFAULT_DIR = Path("/Users/alex/Documents/ingame/runtime/tempo_experiment")


def _env_path(name: str, default: Path) -> Path:
    raw = os.getenv(name)
    if raw is None:
        return Path(default)
    raw = str(raw).strip()
    return Path(raw) if raw else Path(default)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return int(default)
    try:
        return int(float(str(raw).strip()))
    except (TypeError, ValueError):
        return int(default)


def _star_count(metrics: dict) -> int:
    count = 0
    for value in (metrics or {}).values():
        if isinstance(value, str) and value.endswith("*"):
            count += 1
    return count


def _format_star_blocks(raw_output: dict, target_wr: int) -> tuple[dict, dict]:
    early_output = dict((raw_output or {}).get("early_output", {}) or {})
    late_output = dict((raw_output or {}).get("mid_output", {}) or {})
    format_output_dict(
        {"early_output": early_output, "mid_output": late_output},
        target_wr=target_wr,
    )
    return early_output, late_output


def _pearson(points: list[tuple[float, float]]) -> Optional[float]:
    n = len(points)
    if n < 2:
        return None
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    cov = sum((x - mean_x) * (y - mean_y) for x, y in points)
    var_x = sum((x - mean_x) ** 2 for x in xs)
    var_y = sum((y - mean_y) ** 2 for y in ys)
    if var_x <= 0 or var_y <= 0:
        return None
    return cov / math.sqrt(var_x * var_y)



def _ensure_bucket(container: dict, index: int) -> dict:
    key = str(index)
    bucket = container.get(key)
    if bucket is None:
        bucket = {
            "matches": 0,
            "predicted_total_pm_sum": 0.0,
            "actual_kills_pm_sum": 0.0,
            "match_ids": [],
        }
        container[key] = bucket
    return bucket


def _build_summary(metric_reports: dict) -> dict:
    summary = {}
    for metric_key, payload in metric_reports.items():
        exact = payload["exact_indices"]
        exact_ge10 = {
            idx: {
                "matches": bucket["matches"],
                "avg_predicted_total_pm": bucket["predicted_total_pm_sum"] / bucket["matches"],
                "avg_actual_kills_pm": bucket["actual_kills_pm_sum"] / bucket["matches"],
            }
            for idx, bucket in exact.items()
            if bucket["matches"] >= 10
        }
        summary[metric_key] = {
            "matches": payload["matches"],
            "pearson_predicted_vs_actual_kills_pm": payload["pearson_predicted_vs_actual_kills_pm"],
            "exact_indices_ge10": exact_ge10,
        }
    return summary


def run_tempo_metrics_experiment(
    pro_json_dir: Path | None = None,
    pub_stats_dir: Path | None = None,
    tempo_stats_dir: Path | None = None,
    report_dir: Path | None = None,
    min_start_ts: int = PATCH_739_RELEASE_TS,
    star_wr: int = 60,
) -> dict:
    pro_json_dir = Path(pro_json_dir) if pro_json_dir is not None else _env_path("TEMPO_PRO_JSON_DIR", PRO_DEFAULT_DIR)
    pub_stats_dir = Path(pub_stats_dir) if pub_stats_dir is not None else _env_path("TEMPO_PUB_STATS_DIR", PUB_STATS_DEFAULT_DIR)
    tempo_stats_dir = Path(tempo_stats_dir) if tempo_stats_dir is not None else _env_path("TEMPO_STATS_DIR", TEMPO_STATS_DEFAULT_DIR)
    report_dir = Path(report_dir) if report_dir is not None else _env_path("TEMPO_REPORT_DIR", REPORT_DEFAULT_DIR)
    max_files = max(0, _env_int("TEMPO_PRO_MAX_FILES", 0))
    progress_every = max(0, _env_int("TEMPO_PRO_PROGRESS_EVERY", 500))

    pro_files = sorted(pro_json_dir.glob("combined*.json"))
    if max_files > 0:
        pro_files = pro_files[:max_files]
    if not pro_files:
        raise RuntimeError(f"Файлы combined*.json не найдены в {pro_json_dir}")

    early_dict = json.loads((pub_stats_dir / "early_dict_raw.json").read_text(encoding="utf-8"))
    late_dict = json.loads((pub_stats_dir / "late_dict_raw.json").read_text(encoding="utf-8"))
    tempo_solo_dict, tempo_duo_dict, tempo_cp1v1_dict = load_tempo_dicts(tempo_stats_dir)
    hero_name_by_id = load_hero_name_map()

    metric_reports: dict[str, dict] = {}
    per_match = {}
    selected_matches = 0
    scanned_matches = 0
    skip_reasons = {
        "too_old": 0,
        "bad_map": 0,
        "missing_lead10": 0,
        "networth10_outside_gate": 0,
        "has_early_star": 0,
        "has_late_star": 0,
        "missing_actual_kills_pm": 0,
    }

    print("=" * 80)
    print("TEMPO METRICS EXPERIMENT")
    print("=" * 80)
    print(f"Источник pro матчей: {pro_json_dir}")
    print(f"Обычные draft словари: {pub_stats_dir}")
    print(f"Tempo словари: {tempo_stats_dir}")
    print(f"Выход: {report_dir}")
    print(f"Фильтр даты: startDateTime >= {int(min_start_ts)}")
    print(f"Фильтр no-star: WR{int(star_wr)}")

    for file_index, file_path in enumerate(pro_files, 1):
        print(f"[{file_index}/{len(pro_files)}] {file_path.name}")
        file_scanned = 0
        for match_id, match in _iter_matches(file_path):
            scanned_matches += 1
            file_scanned += 1
            start_ts = match.get("startDateTime")
            try:
                start_ts = int(start_ts)
            except (TypeError, ValueError):
                skip_reasons["too_old"] += 1
                continue
            if start_ts < int(min_start_ts):
                skip_reasons["too_old"] += 1
                continue

            draft = check_bad_map(match=match, start_date_time=min_start_ts)
            if draft is None:
                skip_reasons["bad_map"] += 1
                continue
            radiant_heroes_and_pos, dire_heroes_and_pos = draft

            leads = match.get("radiantNetworthLeads", [])
            if not isinstance(leads, list) or len(leads) <= 10:
                skip_reasons["missing_lead10"] += 1
                continue
            lead10 = leads[10]
            if abs(float(lead10)) > 1500:
                skip_reasons["networth10_outside_gate"] += 1
                continue

            raw_output = synergy_and_counterpick(
                radiant_heroes_and_pos=radiant_heroes_and_pos,
                dire_heroes_and_pos=dire_heroes_and_pos,
                early_dict=early_dict,
                mid_dict=late_dict,
            ) or {}
            early_output, late_output = _format_star_blocks(raw_output, target_wr=star_wr)
            if _star_count(early_output) > 0:
                skip_reasons["has_early_star"] += 1
                continue
            if _star_count(late_output) > 0:
                skip_reasons["has_late_star"] += 1
                continue

            actual_kills_pm = compute_match_total_kills_per_min(match)
            if actual_kills_pm is None:
                skip_reasons["missing_actual_kills_pm"] += 1
                continue

            tempo_metrics = build_tempo_draft_metrics(
                radiant_heroes_and_pos,
                dire_heroes_and_pos,
                tempo_solo_dict,
                tempo_duo_dict,
                tempo_cp1v1_dict,
            )
            match_id_int = int(match.get("id") or match_id)
            per_match[str(match_id_int)] = {
                "match_id": match_id_int,
                "startDateTime": start_ts,
                "lead10": float(lead10),
                "durationSeconds": match.get("durationSeconds"),
                "actual_total_kills_pm": actual_kills_pm,
                "draft": draft_to_named_payload(
                    radiant_heroes_and_pos,
                    dire_heroes_and_pos,
                    hero_name_by_id=hero_name_by_id,
                ),
                "base_no_star_filter": {
                    "star_wr": int(star_wr),
                    "early_output": early_output,
                    "late_output": late_output,
                },
                "tempo_metrics": tempo_metrics,
            }
            selected_matches += 1

            for family_name, family_payload in tempo_metrics.items():
                if not family_payload.get("complete"):
                    continue
                for stat_name in TEMPO_RATE_FIELDS:
                    stat_payload = family_payload.get(stat_name) or {}
                    predicted_total_pm = stat_payload.get("predicted_total_pm")
                    index = stat_payload.get("index")
                    if predicted_total_pm is None or index is None:
                        continue
                    metric_key = f"{family_name}_{stat_name}"
                    report = metric_reports.setdefault(
                        metric_key,
                        {
                            "family": family_name,
                            "stat": stat_name,
                            "index_scale": TEMPO_INDEX_SCALES[stat_name],
                            "matches": 0,
                            "pairs": [],
                            "exact_indices": {},
                        },
                    )
                    report["matches"] += 1
                    report["pairs"].append((float(predicted_total_pm), float(actual_kills_pm)))
                    bucket = _ensure_bucket(report["exact_indices"], int(index))
                    bucket["matches"] += 1
                    bucket["predicted_total_pm_sum"] += float(predicted_total_pm)
                    bucket["actual_kills_pm_sum"] += float(actual_kills_pm)
                    if len(bucket["match_ids"]) < 50:
                        bucket["match_ids"].append(match_id_int)

            if progress_every > 0 and file_scanned % progress_every == 0:
                print(f"  ... {file_path.name}: scanned={file_scanned} selected={selected_matches}", flush=True)
        print(f"  ✓ scanned={file_scanned} cumulative_selected={selected_matches}")

    for metric_key, payload in metric_reports.items():
        payload["pearson_predicted_vs_actual_kills_pm"] = _pearson(payload.pop("pairs"))

    report_dir.mkdir(parents=True, exist_ok=True)
    meta = {
        "scanned_matches": scanned_matches,
        "selected_matches": selected_matches,
        "skip_reasons": skip_reasons,
        "min_start_ts": int(min_start_ts),
        "star_wr": int(star_wr),
        "pro_json_dir": str(pro_json_dir),
        "pub_stats_dir": str(pub_stats_dir),
        "tempo_stats_dir": str(tempo_stats_dir),
    }
    aggregate_report = {
        "meta": meta,
        "metric_reports": metric_reports,
        "summary": _build_summary(metric_reports),
    }
    _dump_to_file(report_dir / "tempo_match_reports.json", per_match)
    _dump_to_file(report_dir / "tempo_metric_report.json", aggregate_report)
    _dump_to_file(report_dir / "tempo_metric_report_meta.json", meta)

    print("\nИтог:")
    print(json.dumps(meta, ensure_ascii=False, indent=2))
    print("\nТоп summary:")
    for metric_key, payload in sorted(metric_reports.items()):
        print(
            f"  {metric_key}: matches={payload['matches']} pearson={payload['pearson_predicted_vs_actual_kills_pm']}"
        )
    return aggregate_report


def main():
    run_tempo_metrics_experiment()


if __name__ == "__main__":
    main()
