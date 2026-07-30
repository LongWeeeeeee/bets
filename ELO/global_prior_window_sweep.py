from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass, is_dataclass
from pathlib import Path
from typing import Any

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ELO.config import EvaluationConfig, HybridEloConfig
from ELO.data_loader import load_matches
from ELO.domain import LeagueTier, SeriesBundle
from ELO.models import HybridPlayerRosterEloModel
from ELO.series_data import build_series_bundles
from ELO.series_evaluation import run_series_online_evaluation
from ELO.tiering import attach_league_tiers, classify_leagues

SECONDS_PER_DAY = 24 * 60 * 60


def _to_json_ready(value: Any) -> Any:
    if isinstance(value, LeagueTier):
        return value.value
    if is_dataclass(value):
        return _to_json_ready(asdict(value))
    if isinstance(value, dict):
        return {
            str(key.value if isinstance(key, LeagueTier) else key): _to_json_ready(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_to_json_ready(item) for item in value]
    return value


def _default_data_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "pro_heroes_data" / "json_parts_split_from_object"


def _default_output_path() -> Path:
    return Path(__file__).resolve().parent / "output" / "global_prior_window_sweep.json"


@dataclass(frozen=True)
class WindowCfg:
    days: int


def _series_split(
    series_bundles: list[SeriesBundle],
    cutoff_ts: int,
) -> tuple[list[SeriesBundle], list[SeriesBundle]]:
    warmup_bundles: list[SeriesBundle] = []
    recent_bundles: list[SeriesBundle] = []
    for bundle in series_bundles:
        if bundle.series.start_timestamp < cutoff_ts:
            warmup_bundles.append(bundle)
        else:
            recent_bundles.append(bundle)
    return warmup_bundles, recent_bundles


def _process_bundles(model: HybridPlayerRosterEloModel, bundles: list[SeriesBundle]) -> None:
    for bundle in bundles:
        for match in bundle.all_maps:
            model.process_match(match)


def _global_only_warmup_config(base: HybridEloConfig) -> HybridEloConfig:
    zero_k = {tier: 0.0 for tier in LeagueTier}
    return HybridEloConfig(
        initial_rating=base.initial_rating,
        elo_scale=base.elo_scale,
        bo3_sweep_bonus_weight=base.bo3_sweep_bonus_weight,
        bo3_sweep_bonus_error_basis=base.bo3_sweep_bonus_error_basis,
        player_global_weight=1.0,
        player_tier_weight=0.0,
        max_roster_weight=0.0,
        roster_full_weight_matches=base.roster_full_weight_matches,
        lineup_uncertainty_boost_max=0.0,
        lineup_uncertainty_boost_matches=base.lineup_uncertainty_boost_matches,
        lineup_uncertainty_boost_global=False,
        lineup_uncertainty_boost_local=False,
        lineup_uncertainty_boost_roster=False,
        lineup_uncertainty_tier1_enabled=False,
        player_org_uncertainty_boost_max=0.0,
        player_org_uncertainty_boost_matches=base.player_org_uncertainty_boost_matches,
        player_org_uncertainty_boost_global=False,
        player_org_uncertainty_boost_local=False,
        player_org_uncertainty_tier1_enabled=False,
        patch_local_reset_mode="none",
        patch_local_reset_player_local_keep=1.0,
        patch_local_reset_roster_keep=1.0,
        patch_local_reset_tier1_only=base.patch_local_reset_tier1_only,
        side_bias_k=base.side_bias_k,
        player_global_decay_half_life_days=base.player_global_decay_half_life_days,
        player_local_decay_half_life_days=0.0,
        roster_decay_half_life_days=0.0,
        org_prior_rating_by_tier=dict(base.org_prior_rating_by_tier),
        cold_start_org_prior_weight=0.0,
        org_prior_fade_matches=base.org_prior_fade_matches,
        k_global_by_tier=dict(base.k_global_by_tier),
        k_local_by_tier=zero_k,
        k_roster_by_tier=zero_k,
    )


def _seed_global_prior(
    base_config: HybridEloConfig,
    warmup_bundles: list[SeriesBundle],
) -> HybridPlayerRosterEloModel:
    warmup_model = HybridPlayerRosterEloModel(_global_only_warmup_config(base_config))
    _process_bundles(warmup_model, warmup_bundles)

    seeded_model = HybridPlayerRosterEloModel(base_config)
    seeded_model.player_global = dict(warmup_model.player_global)
    seeded_model.player_global_last_seen_ts = dict(warmup_model.player_global_last_seen_ts)
    return seeded_model


def _run_recent_eval(
    model: HybridPlayerRosterEloModel,
    recent_bundles: list[SeriesBundle],
) -> dict[str, Any]:
    return run_series_online_evaluation(
        model=model,
        series_bundles=recent_bundles,
        config=EvaluationConfig(evaluation_fraction=1.0, min_train_series=0),
    )


def _summarize_report(name: str, report: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": name,
        "accuracy": float(report["accuracy"]),
        "log_loss": float(report["log_loss"]),
        "brier": float(report["brier"]),
        "by_tier": _to_json_ready(report.get("by_tier", {})),
        "matches": int(report.get("matches", 0)),
    }


def main() -> None:
    data_dir = _default_data_dir()
    output_path = _default_output_path()

    matches, load_summary = load_matches(data_dir)
    if not matches:
        raise SystemExit("No valid matches were loaded.")

    league_info, league_summary = classify_leagues(matches)
    attach_league_tiers(matches, league_info)
    series_bundles, series_summary = build_series_bundles(matches)

    last_match_ts = matches[-1].timestamp
    base_config = HybridEloConfig()
    windows = [WindowCfg(days=180), WindowCfg(days=270), WindowCfg(days=365), WindowCfg(days=540)]
    results: list[dict[str, Any]] = []

    for window in windows:
        cutoff_ts = last_match_ts - window.days * SECONDS_PER_DAY
        warmup_bundles, recent_bundles = _series_split(series_bundles, cutoff_ts)
        recent_eligible = [bundle for bundle in recent_bundles if bundle.series.eligible_for_winner_target]
        if not recent_eligible:
            continue

        no_warmup_report = _run_recent_eval(HybridPlayerRosterEloModel(base_config), recent_bundles)

        full_warmup_model = HybridPlayerRosterEloModel(base_config)
        _process_bundles(full_warmup_model, warmup_bundles)
        full_warmup_report = _run_recent_eval(full_warmup_model, recent_bundles)

        global_prior_model = _seed_global_prior(base_config, warmup_bundles)
        global_prior_report = _run_recent_eval(global_prior_model, recent_bundles)

        results.append(
            {
                "window_days": window.days,
                "cutoff_ts": cutoff_ts,
                "warmup_series": len(warmup_bundles),
                "recent_series": len(recent_bundles),
                "recent_eligible_series": len(recent_eligible),
                "no_warmup": _summarize_report("no_warmup", no_warmup_report),
                "full_warmup": _summarize_report("full_warmup", full_warmup_report),
                "global_prior_only": _summarize_report("global_prior_only", global_prior_report),
                "delta_global_prior_vs_full": {
                    "accuracy": float(global_prior_report["accuracy"]) - float(full_warmup_report["accuracy"]),
                    "log_loss": float(global_prior_report["log_loss"]) - float(full_warmup_report["log_loss"]),
                    "brier": float(global_prior_report["brier"]) - float(full_warmup_report["brier"]),
                    "tier1_accuracy": float(global_prior_report["by_tier"]["TIER1"]["accuracy"])
                    - float(full_warmup_report["by_tier"]["TIER1"]["accuracy"]),
                    "tier1_log_loss": float(global_prior_report["by_tier"]["TIER1"]["log_loss"])
                    - float(full_warmup_report["by_tier"]["TIER1"]["log_loss"]),
                    "tier1_brier": float(global_prior_report["by_tier"]["TIER1"]["brier"])
                    - float(full_warmup_report["by_tier"]["TIER1"]["brier"]),
                },
            }
        )

    best_by_tier1_accuracy = max(
        results,
        key=lambda row: (
            row["global_prior_only"]["by_tier"]["TIER1"]["accuracy"],
            -row["global_prior_only"]["by_tier"]["TIER1"]["log_loss"],
        ),
    )
    best_by_tier1_vs_full = max(
        results,
        key=lambda row: (
            row["delta_global_prior_vs_full"]["tier1_accuracy"],
            -row["delta_global_prior_vs_full"]["tier1_log_loss"],
        ),
    )
    best_by_overall_vs_full = min(
        results,
        key=lambda row: (
            row["delta_global_prior_vs_full"]["log_loss"],
            row["delta_global_prior_vs_full"]["brier"],
            -row["delta_global_prior_vs_full"]["accuracy"],
        ),
    )

    output = {
        "data_dir": str(data_dir),
        "dataset_summary": load_summary,
        "league_summary": league_summary,
        "series_summary": series_summary,
        "current_config": _to_json_ready(base_config),
        "results": results,
        "best_by_tier1_accuracy": best_by_tier1_accuracy,
        "best_by_tier1_vs_full": best_by_tier1_vs_full,
        "best_by_overall_vs_full": best_by_overall_vs_full,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(_to_json_ready(output), fh, ensure_ascii=False, indent=2)

    print(f"Saved global-prior window sweep to {output_path}")
    print(
        "Best global-prior TIER1 accuracy: "
        f"{best_by_tier1_accuracy['window_days']}d "
        f"acc={best_by_tier1_accuracy['global_prior_only']['by_tier']['TIER1']['accuracy']:.4f} "
        f"ll={best_by_tier1_accuracy['global_prior_only']['by_tier']['TIER1']['log_loss']:.4f}"
    )
    print(
        "Best delta vs full warmup for TIER1 accuracy: "
        f"{best_by_tier1_vs_full['window_days']}d "
        f"d_acc={best_by_tier1_vs_full['delta_global_prior_vs_full']['tier1_accuracy']:.4f} "
        f"d_ll={best_by_tier1_vs_full['delta_global_prior_vs_full']['tier1_log_loss']:.4f}"
    )
    print(
        "Best delta vs full warmup overall log_loss: "
        f"{best_by_overall_vs_full['window_days']}d "
        f"d_acc={best_by_overall_vs_full['delta_global_prior_vs_full']['accuracy']:.4f} "
        f"d_ll={best_by_overall_vs_full['delta_global_prior_vs_full']['log_loss']:.4f}"
    )


if __name__ == "__main__":
    main()
