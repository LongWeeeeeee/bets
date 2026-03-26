"""Phase-specific signal wrappers for draft metrics (early/late)."""

from __future__ import annotations

import json
import logging
import os
import pickle
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Tuple


DEFAULT_HERO_FEATURES_PATH = Path("/Users/alex/Documents/ingame/data/hero_features_processed.json")
DEFAULT_WRAPPER_CONFIG_PATH = Path("/Users/alex/Documents/ingame/data/hero_signal_wrappers.json")
DEFAULT_ML_MODEL_SET_DIR = Path("/Users/alex/Documents/ingame/ml-models/phase_models_early66_latebase")
DEFAULT_ML_EARLY_MODEL_PATH = DEFAULT_ML_MODEL_SET_DIR / "phase_signal_wrapper_early.pkl"
DEFAULT_ML_LATE_MODEL_PATH = DEFAULT_ML_MODEL_SET_DIR / "phase_signal_wrapper_late.pkl"
LEGACY_ML_EARLY_MODEL_PATH = Path("/Users/alex/Documents/ingame/ml-models/phase_signal_wrapper_early.pkl")
LEGACY_ML_LATE_MODEL_PATH = Path("/Users/alex/Documents/ingame/ml-models/phase_signal_wrapper_late.pkl")
DEFAULT_REQUIRED_SKLEARN_VERSION = "1.6.1"
DEFAULT_RUNTIME_REQUIREMENTS_PATH = Path("/Users/alex/Documents/ingame/data/ml_wrapper_runtime_requirements.json")
STAR_THRESHOLDS_PATH = Path(
    os.getenv("STAR_THRESHOLDS_PATH", "/Users/alex/Documents/ingame/data/star_thresholds_by_wr.json")
)
_SKLEARN_VERSION_WARNED: set[str] = set()
logger = logging.getLogger(__name__)

TARGET_METRICS = (
    "counterpick_1vs1",
    "pos1_vs_pos1",
    "counterpick_1vs2",
    "solo",
    "synergy_duo",
    "synergy_trio",
)
CORE_POSITIONS = (1, 2, 3)
ROLE_KEYS = ("hard_carry", "has_initiator", "save_count", "has_control")

DEFAULT_HARD_CARRY_IDS = (
    1,    # Anti-Mage
    10,   # Morphling
    12,   # Phantom Lancer
    35,   # Sniper
    41,   # Faceless Void
    44,   # Phantom Assassin
    48,   # Luna
    67,   # Spectre
    72,   # Gyrocopter
    80,   # Lone Druid
    89,   # Naga Siren
    94,   # Medusa
    95,   # Troll Warlord
    109,  # Terrorblade
    114,  # Monkey King
)
_STAR_THRESHOLDS_FALLBACK = {
    60: {
        "early_output": [
            ("counterpick_1vs1", 4),
            ("pos1_vs_pos1", 20),
            ("counterpick_1vs2", 7),
            ("synergy_duo", 7),
            ("solo", 3),
            ("synergy_trio", 7),
        ],
        "mid_output": [
            ("counterpick_1vs1", 5),
            ("pos1_vs_pos1", 20),
            ("counterpick_1vs2", 8),
            ("synergy_duo", 8),
            ("synergy_trio", 6),
            ("solo", 3),
        ],
    },
}

DEFAULT_CONFIG: Dict[str, Any] = {
    "guardrails": {
        # Conservative mode: wrapper cannot introduce new weak bets from zero
        # and cannot invert existing signal direction by default.
        "allow_zero_to_nonzero": False,
        "allow_nonzero_to_zero": False,
        "allow_sign_flip": False,
    },
    "normalization_scales": {
        "escape_lock": 24.0,
        "channel_interrupt": 18.0,
        "control_pressure": 16.0,
        "save_resilience": 12.0,
        "big_ult_tradeoff": 22.0,
    },
    "early": {
        "max_shift": 0.0,
        "min_abs_metric_to_adjust": 2.0,
        "min_phase_shift_for_new_bet": 3.0,
        "index_keep_gate": {
            "enabled": False,
            "min_abs_default": 1.0,
            "max_abs_default": 0.0,
            "min_abs_by_metric": {},
            "max_abs_by_metric": {},
            "metrics": list(TARGET_METRICS),
        },
        "feature_balance_gate": {
            "enabled": False,
            "max_abs_metric_to_zero": 99.0,
            "escape_reliance_diff_min": 1.0,
            "enemy_lock_adv_min": 1.5,
            "save_deficit_min": 1.0,
            "enemy_control_adv_min": 2.0,
            "melee_imbalance_min": 2.0,
            "enemy_control_adv_for_melee_min": 2.0,
            "metrics": list(TARGET_METRICS),
        },
        "support_gap_gate": {
            "enabled": False,
            "max_abs_metric_to_zero": 99.0,
            "save_deficit_min": 1.0,
            "escape_deficit_min": 1.0,
            "metrics": list(TARGET_METRICS),
        },
        "big_ult_burden_gate": {
            "enabled": True,
            "burden_diff_min": 2,
            "max_abs_metric_to_zero": 2.0,
            "metrics": list(TARGET_METRICS),
        },
        "escape_vulnerability_gate": {
            "enabled": False,
            "escape_diff_min": 1.0,
            "enemy_lock_adv_min": 1.5,
            "max_abs_metric_to_zero": 2.0,
            "metrics": list(TARGET_METRICS),
        },
        "edge_requirements_gate": {
            "enabled": False,
            "max_abs_metric_to_zero": 99.0,
            "metrics": list(TARGET_METRICS),
            "min_escape_edge": None,
            "min_save_edge": None,
            "min_bkb_edge": None,
            "min_control_edge": None,
            "min_lock_edge": None,
            "min_initiation_edge": None,
            "max_big_ult_edge": None,
            "max_big_ult_hard_edge": None,
            "max_melee_edge": None,
        },
        "weights": {
            "escape_lock": 0.34,
            "channel_interrupt": 0.26,
            "control_pressure": 0.22,
            "save_resilience": 0.10,
            "big_ult_tradeoff": 0.08,
        },
        "metric_gains": {
            "counterpick_1vs1": 1.00,
            "pos1_vs_pos1": 0.80,
            "counterpick_1vs2": 0.95,
            "solo": 0.65,
            "synergy_duo": 0.55,
            "synergy_trio": 0.45,
        },
    },
    "late": {
        "max_shift": 10.0,
        "min_abs_metric_to_adjust": 2.0,
        "min_phase_shift_for_new_bet": 3.0,
        "index_keep_gate": {
            "enabled": False,
            "min_abs_default": 1.0,
            "max_abs_default": 0.0,
            "min_abs_by_metric": {},
            "max_abs_by_metric": {},
            "metrics": list(TARGET_METRICS),
        },
        "role_balance_gate": {
            "enabled": False,
            "max_abs_metric_to_zero": 99.0,
            "score_gap_min": 1.0,
            "bkb_deficit_min": 1.0,
            "carry_deficit_min": 1.0,
            "control_deficit_min": 2.0,
            "save_deficit_min": 1.0,
            "min_initiation_tools": 2.0,
            "min_save_resilience": 1.0,
            "min_bkb_control": 1.0,
            "min_control_pressure": 8.0,
            "metrics": list(TARGET_METRICS),
        },
        "support_gap_gate": {
            "enabled": False,
            "max_abs_metric_to_zero": 99.0,
            "save_deficit_min": 1.0,
            "escape_deficit_min": 1.0,
            "carry_deficit_min": 1.0,
            "bkb_deficit_min": 1.0,
            "metrics": list(TARGET_METRICS),
        },
        "hard_carry_gate": {
            "enabled": True,
            "deficit_min": 1,
            "max_abs_metric_to_zero": 2.0,
            "metrics": list(TARGET_METRICS),
            "hard_carry_ids": list(DEFAULT_HARD_CARRY_IDS),
        },
        "control_stability_gate": {
            "enabled": False,
            "control_deficit_min": 2.0,
            "save_deficit_min": 1.0,
            "max_abs_metric_to_zero": 2.0,
            "metrics": list(TARGET_METRICS),
        },
        "edge_requirements_gate": {
            "enabled": False,
            "max_abs_metric_to_zero": 99.0,
            "metrics": list(TARGET_METRICS),
            "min_escape_edge": None,
            "min_save_edge": None,
            "min_bkb_edge": None,
            "min_control_edge": None,
            "min_lock_edge": None,
            "min_initiation_edge": None,
            "max_big_ult_edge": None,
            "max_big_ult_hard_edge": None,
            "max_melee_edge": None,
        },
        "weights": {
            "escape_lock": 0.18,
            "channel_interrupt": 0.24,
            "control_pressure": 0.16,
            "save_resilience": 0.20,
            "big_ult_tradeoff": 0.22,
        },
        "metric_gains": {
            "counterpick_1vs1": 0.80,
            "pos1_vs_pos1": 0.80,
            "counterpick_1vs2": 0.90,
            "solo": 0.50,
            "synergy_duo": 0.85,
            "synergy_trio": 1.00,
        },
    },
}


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
    return default


def _is_enabled(env_name: str, default: bool = True) -> bool:
    value = os.getenv(env_name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "off", "no"}


def _runtime_sklearn_version() -> str:
    try:
        import sklearn  # type: ignore

        return str(getattr(sklearn, "__version__", "") or "").strip()
    except Exception:
        return ""


@lru_cache(maxsize=1)
def _required_sklearn_version_default() -> str:
    path = DEFAULT_RUNTIME_REQUIREMENTS_PATH
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            raw = payload.get("required_sklearn_version")
            if raw is not None:
                val = str(raw).strip()
                if val:
                    return val
    except Exception:
        pass
    return DEFAULT_REQUIRED_SKLEARN_VERSION


def _warn_sklearn_mismatch(path: Path, required: str, artifact: str) -> None:
    req = str(required or "").strip()
    if not req:
        return
    runtime = _runtime_sklearn_version()
    if not runtime or runtime == req:
        return
    key = f"{path}|{req}|{runtime}|{artifact}"
    if key in _SKLEARN_VERSION_WARNED:
        return
    _SKLEARN_VERSION_WARNED.add(key)
    art = str(artifact or "n/a")
    print(
        "⚠️ SIGNAL_WRAPPER sklearn mismatch: "
        f"runtime={runtime}, required={req}, artifact={art}, model={path}"
    )


@lru_cache(maxsize=1)
def _load_star_thresholds() -> Dict[int, Dict[str, list[tuple[str, int]]]]:
    if STAR_THRESHOLDS_PATH.exists():
        try:
            data = json.loads(STAR_THRESHOLDS_PATH.read_text(encoding="utf-8"))
            parsed: Dict[int, Dict[str, list[tuple[str, int]]]] = {}
            if isinstance(data, dict):
                for raw_wr, payload in data.items():
                    try:
                        wr = int(raw_wr)
                    except (TypeError, ValueError):
                        continue
                    if not isinstance(payload, dict):
                        continue
                    block: Dict[str, list[tuple[str, int]]] = {}
                    for section in ("early_output", "mid_output"):
                        items = payload.get(section) or []
                        rows: list[tuple[str, int]] = []
                        if isinstance(items, list):
                            for row in items:
                                if not isinstance(row, (list, tuple)) or len(row) != 2:
                                    continue
                                metric = str(row[0]).strip()
                                try:
                                    thr = int(row[1])
                                except (TypeError, ValueError):
                                    continue
                                if metric:
                                    rows.append((metric, thr))
                        block[section] = rows
                    parsed[wr] = block
            if parsed:
                hydrated: Dict[int, Dict[str, list[tuple[str, int]]]] = {}
                fallback60 = {
                    section: list(_STAR_THRESHOLDS_FALLBACK[60].get(section, []))
                    for section in ("early_output", "mid_output")
                }
                for wr, block in parsed.items():
                    out_block: Dict[str, list[tuple[str, int]]] = {}
                    for section in ("early_output", "mid_output"):
                        section_rows = list(block.get(section) or [])
                        if not section_rows and int(wr) == 60:
                            logger.warning(
                                "SIGNAL_WRAPPER thresholds missing WR60 section=%s in %s; using hardcoded fallback60",
                                section,
                                STAR_THRESHOLDS_PATH,
                            )
                            section_rows = list(fallback60.get(section, []))
                        elif not section_rows:
                            logger.warning(
                                "SIGNAL_WRAPPER thresholds missing WR%s section=%s in %s; section disabled",
                                wr,
                                section,
                                STAR_THRESHOLDS_PATH,
                            )
                        out_block[section] = section_rows
                    hydrated[int(wr)] = out_block
                if 60 not in hydrated:
                    logger.warning(
                        "SIGNAL_WRAPPER thresholds missing WR60 in %s; using hardcoded fallback60",
                        STAR_THRESHOLDS_PATH,
                    )
                    hydrated[60] = fallback60
                return hydrated
            raise RuntimeError(
                f"SIGNAL_WRAPPER thresholds file {STAR_THRESHOLDS_PATH} contains no valid WR entries"
            )
        except Exception as exc:
            logger.exception("Failed to load SIGNAL_WRAPPER thresholds from %s", STAR_THRESHOLDS_PATH)
            raise RuntimeError(
                f"Failed to load SIGNAL_WRAPPER thresholds from {STAR_THRESHOLDS_PATH}"
            ) from exc
    return {int(k): dict(v) for k, v in _STAR_THRESHOLDS_FALLBACK.items()}


def _star_target_wr() -> int:
    raw = os.getenv("STAR_THRESHOLD_WR", "60")
    try:
        wr = int(str(raw).strip())
    except (TypeError, ValueError):
        wr = 60
    return wr


def _coerce_metric_value(raw: Any) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None
        if text.endswith("*"):
            text = text[:-1]
        try:
            return float(text)
        except ValueError:
            return None
    if isinstance(raw, (int, float)):
        return float(raw)
    return None


def _metric_sign(raw: Any) -> int | None:
    value = _coerce_metric_value(raw)
    if value is None or value == 0.0:
        return None
    return 1 if value > 0.0 else -1


def _block_star_sign(block: Dict[str, Any], metric_thresholds: list[tuple[str, int]]) -> int | None:
    if not isinstance(block, dict) or not metric_thresholds:
        return None
    block_star_count = 0
    block_sign: int | None = None
    block_conflict = False
    for metric, threshold in metric_thresholds:
        value = _coerce_metric_value(block.get(metric))
        if value is None:
            continue
        if abs(value) < float(threshold):
            continue
        block_star_count += 1
        if value == 0.0:
            continue
        sign = 1 if value > 0.0 else -1
        if block_sign is None:
            block_sign = sign
        elif block_sign != sign:
            block_conflict = True
    if block_star_count > 0 and not block_conflict and block_sign is not None:
        for metric, _ in metric_thresholds:
            sign = _metric_sign(block.get(metric))
            if sign is not None and sign != block_sign:
                block_conflict = True
                break
    if block_star_count == 0 or block_conflict or block_sign is None:
        return None
    return int(block_sign)


def _runtime_pro_star_features(
    phase: str,
    phase_bucket: Dict[str, Any],
    phase_context: Dict[str, Any] | None,
) -> Dict[str, float]:
    context = phase_context if isinstance(phase_context, dict) else {}
    early_block = context.get("early_output")
    late_block = context.get("mid_output")
    if not isinstance(early_block, dict):
        early_block = phase_bucket if phase == "early" else {}
    if not isinstance(late_block, dict):
        late_block = phase_bucket if phase == "late" else {}

    all_thresholds = _load_star_thresholds()
    target_wr = _star_target_wr()
    threshold_set = all_thresholds.get(target_wr)
    if not isinstance(threshold_set, dict):
        threshold_set = all_thresholds.get(60) if target_wr == 60 else {}
    early_thresholds = threshold_set.get("early_output") or []
    late_thresholds = threshold_set.get("mid_output") or []

    early_sign = _block_star_sign(early_block, early_thresholds)
    late_sign = _block_star_sign(late_block, late_thresholds)
    early_features = {
        "pro_early_has_star": 1.0 if early_sign in (-1, 1) else 0.0,
        "pro_early_star_sign": float(early_sign or 0),
    }
    late_features = {
        "pro_late_has_star": 1.0 if late_sign in (-1, 1) else 0.0,
        "pro_late_star_sign": float(late_sign or 0),
    }
    if _is_enabled("SIGNAL_WRAPPER_CROSS_PHASE_STAR_FEATURES", default=False):
        # Compatibility mode for legacy artifacts trained with cross-phase star features.
        return {
            **early_features,
            **late_features,
            "pro_star_same_sign": (
                1.0 if early_sign in (-1, 1) and late_sign in (-1, 1) and early_sign == late_sign else 0.0
            ),
        }
    if phase == "early":
        return early_features
    if phase == "late":
        return late_features
    return {
        **early_features,
        **late_features,
        "pro_star_same_sign": (
            1.0 if early_sign in (-1, 1) and late_sign in (-1, 1) and early_sign == late_sign else 0.0
        ),
    }


def _softsign(value: float, scale: float) -> float:
    if scale <= 0:
        scale = 1.0
    return value / (abs(value) + scale)


@lru_cache(maxsize=1)
def _load_hero_features() -> Dict[str, Dict[str, Any]]:
    path = Path(os.getenv("HERO_FEATURES_PATH", str(DEFAULT_HERO_FEATURES_PATH)))
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for key, value in payload.items():
        if isinstance(value, dict):
            out[str(key)] = value
    return out


@lru_cache(maxsize=1)
def _load_wrapper_config() -> Dict[str, Any]:
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    path = Path(os.getenv("SIGNAL_WRAPPER_CONFIG_PATH", str(DEFAULT_WRAPPER_CONFIG_PATH)))
    if not path.exists():
        return cfg
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return cfg
    if not isinstance(loaded, dict):
        return cfg

    def _merge(dst: Dict[str, Any], src: Dict[str, Any]) -> None:
        for key, value in src.items():
            if isinstance(value, dict) and isinstance(dst.get(key), dict):
                _merge(dst[key], value)
            else:
                dst[key] = value

    _merge(cfg, loaded)
    return cfg


def clear_wrapper_caches() -> None:
    _load_hero_features.cache_clear()
    _load_wrapper_config.cache_clear()
    _load_ml_wrapper_artifact.cache_clear()


def _wrapper_mode() -> str:
    return os.getenv("SIGNAL_WRAPPER_MODE", "ml").strip().lower()


def _wrapper_debug_enabled() -> bool:
    return _is_enabled("SIGNAL_WRAPPER_DEBUG", default=False)


def _ml_model_path_for_phase(phase: str) -> Path:
    if phase == "early":
        env_path = os.getenv("SIGNAL_WRAPPER_ML_EARLY_PATH")
        if env_path:
            return Path(env_path)
        if DEFAULT_ML_EARLY_MODEL_PATH.exists():
            return DEFAULT_ML_EARLY_MODEL_PATH
        return LEGACY_ML_EARLY_MODEL_PATH
    env_path = os.getenv("SIGNAL_WRAPPER_ML_LATE_PATH")
    if env_path:
        return Path(env_path)
    if DEFAULT_ML_LATE_MODEL_PATH.exists():
        return DEFAULT_ML_LATE_MODEL_PATH
    return LEGACY_ML_LATE_MODEL_PATH


@lru_cache(maxsize=2)
def _load_ml_wrapper_artifact(phase: str) -> Dict[str, Any]:
    path = _ml_model_path_for_phase(phase)
    if not path.exists():
        return {}
    try:
        with path.open("rb") as f:
            payload = pickle.load(f)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    model = payload.get("model")
    feature_names = payload.get("feature_names")
    target_metrics = payload.get("target_metrics")
    if model is None or not isinstance(feature_names, list) or not feature_names:
        return {}
    if not isinstance(target_metrics, list):
        target_metrics = list(TARGET_METRICS)
    else:
        target_metrics = [str(x) for x in target_metrics if str(x) in TARGET_METRICS]
    model_type = str(payload.get("model_type") or "logreg").strip().lower()
    ensemble_models = payload.get("ensemble_models")
    if not isinstance(ensemble_models, dict):
        ensemble_models = {}
    ensemble_weights_raw = payload.get("ensemble_weights")
    if isinstance(ensemble_weights_raw, dict):
        ensemble_weights = {str(k): _as_float(v, 0.0) for k, v in ensemble_weights_raw.items()}
    else:
        ensemble_weights = {}
    required_sklearn_version = (
        str(payload.get("required_sklearn_version") or "").strip()
        or str(os.getenv("SIGNAL_WRAPPER_REQUIRED_SKLEARN_VERSION") or "").strip()
        or _required_sklearn_version_default()
    )
    artifact_sklearn_version = str(payload.get("sklearn_version") or "").strip()
    _warn_sklearn_mismatch(path, required_sklearn_version, artifact_sklearn_version)
    out: Dict[str, Any] = {
        "phase": payload.get("phase", phase),
        "model": model,
        "model_type": model_type,
        "ensemble_models": ensemble_models,
        "ensemble_weights": ensemble_weights,
        "feature_names": [str(x) for x in feature_names],
        "target_metrics": list(target_metrics),
        "edge_feature_keys": [str(x) for x in (payload.get("edge_feature_keys") or [])],
        "threshold": _as_float(payload.get("threshold"), 0.5),
        "threshold_mode": str(payload.get("threshold_mode", "phase_metric")),
        "thresholds_by_metric": {
            str(k): _as_float(v, 0.5) for k, v in (payload.get("thresholds_by_metric") or {}).items()
        },
        "boost_strength": _as_float(payload.get("boost_strength"), 0.0),
        "include_hero_id_features": _as_bool(payload.get("include_hero_id_features"), False),
        "sklearn_version": artifact_sklearn_version,
        "required_sklearn_version": required_sklearn_version,
        "path": str(path),
    }
    return out


def _extract_side_pos_map(side: Dict[str, Any]) -> Dict[int, int]:
    out: Dict[int, int] = {}
    if not isinstance(side, dict):
        return out
    for pos in CORE_POSITIONS + (4, 5):
        node = side.get(f"pos{pos}") or {}
        hero_id = node.get("hero_id")
        try:
            hero_int = int(hero_id)
        except (TypeError, ValueError):
            continue
        if hero_int > 0:
            out[pos] = hero_int
    return out


def _phase_bucket_value(phase_bucket: Dict[str, Any], metric: str) -> float:
    value = phase_bucket.get(metric, 0.0)
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("*"):
            text = text[:-1]
        try:
            return float(text)
        except ValueError:
            return 0.0
    return _as_float(value, 0.0)


def _team_feature_sums(
    side_pos: Dict[int, int],
    feature_rows: Dict[str, Dict[str, Any]],
    edge_keys: Tuple[str, ...],
) -> Tuple[Dict[str, float], Dict[str, int]]:
    sums = {key: 0.0 for key in edge_keys if key != "role_coverage_4"}
    role_presence = {key: 0 for key in ROLE_KEYS}
    for pos, hero_id in side_pos.items():
        row = feature_rows.get(str(hero_id))
        if not row:
            continue
        for key in sums:
            if key == "hard_carry" and pos not in CORE_POSITIONS:
                continue
            sums[key] += _as_float(row.get(key))
        if pos in CORE_POSITIONS and _as_bool(row.get("hard_carry")):
            role_presence["hard_carry"] = 1
        if _as_bool(row.get("has_initiator")):
            role_presence["has_initiator"] = 1
        if _as_bool(row.get("save_count")):
            role_presence["save_count"] = 1
        if _as_bool(row.get("has_control")):
            role_presence["has_control"] = 1
    return sums, role_presence


def _interaction_specs_for_phase(phase: str) -> Tuple[Tuple[str, str, str], ...]:
    # Keep runtime interaction construction aligned with tools/train_ml_phase_wrappers.py
    if phase == "early":
        return (
            ("inter_escape_x_channeling", "escape_count", "channeling_spell_count"),
            ("inter_escape_x_interruptible", "escape_count", "interruptible_channel_count"),
            ("inter_pusher_x_melee", "has_pusher", "is_melee"),
            ("inter_latepusher_x_escape", "has_pusher_late", "escape_count"),
        )
    if phase == "late":
        return (
            ("inter_save_x_hard_disable", "save_count", "has_hard_disable"),
            ("inter_escape_x_control", "has_escape", "has_control"),
            ("inter_hex_x_hex_count", "has_hex", "hex_count"),
            ("inter_root_x_leash", "has_root", "has_leash"),
            ("inter_pusherlate_x_hg", "has_pusher_late", "hg_defence"),
            ("inter_hardcarry_x_save", "hard_carry", "save_count"),
        )
    return tuple()


def _edge_diff_for_model(
    artifact: Dict[str, Any],
    feature_rows: Dict[str, Dict[str, Any]],
    radiant_heroes_and_pos: Dict[str, Any],
    dire_heroes_and_pos: Dict[str, Any],
) -> Dict[str, float]:
    edge_keys = tuple(str(x) for x in (artifact.get("edge_feature_keys") or []))
    if not edge_keys:
        return {}
    r_pos = _extract_side_pos_map(radiant_heroes_and_pos)
    d_pos = _extract_side_pos_map(dire_heroes_and_pos)
    r_sum, r_role = _team_feature_sums(r_pos, feature_rows, edge_keys)
    d_sum, d_role = _team_feature_sums(d_pos, feature_rows, edge_keys)
    out: Dict[str, float] = {}
    for key in edge_keys:
        if key == "role_coverage_4":
            out[key] = float(sum(r_role.values()) - sum(d_role.values()))
        else:
            out[key] = float(r_sum.get(key, 0.0) - d_sum.get(key, 0.0))
    return out


def _boost_abs_index(abs_idx: int, prob: float, threshold: float, boost_strength: float) -> int:
    abs_idx = max(1, min(int(abs_idx), 99))
    if prob <= threshold or boost_strength <= 0.0:
        return abs_idx
    denom = max(1e-6, 1.0 - threshold)
    confidence = max(0.0, min((prob - threshold) / denom, 1.0))
    multiplier = 1.0 + (boost_strength * confidence)
    if abs_idx <= 3:
        multiplier += 0.5 * boost_strength * confidence
    boosted = int(round(abs_idx * multiplier))
    return max(1, min(boosted, 99))


def _build_ml_feature_vector(
    artifact: Dict[str, Any],
    phase_bucket: Dict[str, Any],
    metric: str,
    metric_value: float,
    edge_diff: Dict[str, float],
    runtime_pro_features: Dict[str, float] | None = None,
) -> list[float]:
    sign = 1 if metric_value > 0 else -1
    abs_idx = max(1, min(int(round(abs(metric_value))), 99))
    targets = [str(x) for x in (artifact.get("target_metrics") or TARGET_METRICS)]
    feat: Dict[str, float] = {}
    for m in targets:
        feat[f"metric_{m}"] = 1.0 if m == metric else 0.0
    feat["abs_idx"] = float(abs_idx)
    feat["abs_idx_sq"] = float(abs_idx * abs_idx)
    feat["weak_idx"] = 1.0 if abs_idx <= 3 else 0.0
    feat["strong_idx"] = 1.0 if abs_idx >= 8 else 0.0

    aligned_vals = []
    agree = 0
    disagree = 0
    zero = 0
    for m in targets:
        ctx_val = _phase_bucket_value(phase_bucket, m)
        aligned = float(sign) * float(ctx_val)
        feat[f"context_aligned_{m}"] = aligned
        aligned_vals.append(aligned)
        if aligned > 0:
            agree += 1
        elif aligned < 0:
            disagree += 1
        else:
            zero += 1
    feat["context_agree_count"] = float(agree)
    feat["context_disagree_count"] = float(disagree)
    feat["context_zero_count"] = float(zero)
    feat["context_aligned_abs_sum"] = float(sum(abs(v) for v in aligned_vals))
    feat["context_aligned_abs_max"] = float(max((abs(v) for v in aligned_vals), default=0.0))

    signed_edges: Dict[str, float] = {}
    for key, raw in edge_diff.items():
        signed = float(sign) * float(raw)
        signed_edges[key] = signed
        feat[f"edge_{key}"] = signed
        feat[f"edge_abs_{key}"] = abs(signed)

    phase = str(artifact.get("phase") or "").strip().lower()
    interaction_map = {
        name: (key_a, key_b)
        for name, key_a, key_b in _interaction_specs_for_phase(phase)
    }
    for name in artifact.get("feature_names", []):
        if not name.startswith("inter_"):
            continue
        pair = interaction_map.get(name)
        if pair is not None:
            left, right = pair
            feat[name] = float(signed_edges.get(left, 0.0) * signed_edges.get(right, 0.0))
            continue
        # Fallback for unknown interaction names that directly match edge keys.
        body = name[len("inter_") :]
        if "_x_" not in body:
            continue
        left, right = body.split("_x_", 1)
        feat[name] = float(signed_edges.get(left, 0.0) * signed_edges.get(right, 0.0))

    # Neutral priors at inference time (runtime does not carry train split priors).
    feat.setdefault("prior_wr", 0.5)
    feat.setdefault("prior_logit", 0.0)
    feat.setdefault("prior_conf", 0.0)
    if runtime_pro_features:
        for key, value in runtime_pro_features.items():
            feat[str(key)] = float(value)

    return [float(feat.get(name, 0.0)) for name in artifact.get("feature_names", [])]


def _predict_single_model_prob(model: Any, vec: list[float]) -> float | None:
    if model is None:
        return None
    try:
        pred = model.predict_proba([vec])
        return float(pred[0][1])
    except Exception:
        return None


def _predict_artifact_prob(artifact: Dict[str, Any], vec: list[float]) -> float | None:
    model_type = str(artifact.get("model_type") or "logreg").strip().lower()
    if model_type == "ensemble":
        models = artifact.get("ensemble_models")
        weights = artifact.get("ensemble_weights")
        if isinstance(models, dict) and isinstance(weights, dict):
            p_sum = 0.0
            w_sum = 0.0
            for name, model in models.items():
                w = _as_float(weights.get(name), 0.0)
                if w <= 0.0:
                    continue
                p = _predict_single_model_prob(model, vec)
                if p is None:
                    continue
                p_sum += w * p
                w_sum += w
            if w_sum > 0.0:
                return p_sum / w_sum
    return _predict_single_model_prob(artifact.get("model"), vec)


def _apply_phase_ml_wrapper(
    phase: str,
    phase_bucket: Dict[str, Any],
    radiant_heroes_and_pos: Dict[str, Any],
    dire_heroes_and_pos: Dict[str, Any],
    phase_context: Dict[str, Any] | None = None,
) -> Dict[str, Any] | None:
    artifact = _load_ml_wrapper_artifact(phase)
    if not artifact:
        return None
    feature_rows = _load_hero_features()
    if not feature_rows:
        return None
    edge_diff = _edge_diff_for_model(artifact, feature_rows, radiant_heroes_and_pos, dire_heroes_and_pos)
    runtime_pro_features = _runtime_pro_star_features(
        phase=phase,
        phase_bucket=phase_bucket,
        phase_context=phase_context,
    )
    model_pro_features = [
        str(name) for name in (artifact.get("feature_names") or []) if str(name).startswith("pro_")
    ]
    unsupported_pro = [name for name in model_pro_features if name not in runtime_pro_features]
    if artifact.get("model") is None and not artifact.get("ensemble_models"):
        return None

    updates = 0
    zeroed = 0
    boosted = 0
    probs: Dict[str, float] = {}
    thresholds_by_metric = artifact.get("thresholds_by_metric", {})
    base_thr = _as_float(artifact.get("threshold"), 0.5)
    boost_strength = _as_float(artifact.get("boost_strength"), 0.0)
    target_metrics = [m for m in artifact.get("target_metrics", TARGET_METRICS) if m in TARGET_METRICS]

    for metric in target_metrics:
        value = _phase_bucket_value(phase_bucket, metric)
        if value == 0.0:
            continue
        vec = _build_ml_feature_vector(
            artifact,
            phase_bucket,
            metric,
            value,
            edge_diff,
            runtime_pro_features=runtime_pro_features,
        )
        prob = _predict_artifact_prob(artifact, vec)
        if prob is None:
            continue
        probs[metric] = prob
        threshold = _as_float(thresholds_by_metric.get(metric), base_thr)
        sign = 1 if value > 0 else -1
        abs_idx = max(1, min(int(round(abs(value))), 99))
        if prob < threshold:
            phase_bucket[metric] = 0
            updates += 1
            zeroed += 1
            continue
        boosted_abs = _boost_abs_index(abs_idx, prob, threshold, boost_strength)
        phase_bucket[metric] = int(sign * boosted_abs)
        updates += 1
        if boosted_abs != abs_idx:
            boosted += 1

    return {
        "mode": "ml",
        "model_path": artifact.get("path"),
        "model_type": artifact.get("model_type"),
        "updated": updates,
        "zeroed": zeroed,
        "boosted": boosted,
        "edge_diff": edge_diff,
        "probs": probs,
        "runtime_pro_features": runtime_pro_features,
        "unsupported_pro_feature_count": len(unsupported_pro),
        "unsupported_pro_features": unsupported_pro[:10],
    }


def _team_hero_ids(side: Dict[str, Any]) -> Tuple[int, ...]:
    hero_ids = []
    if not isinstance(side, dict):
        return tuple()
    for pos in ("pos1", "pos2", "pos3", "pos4", "pos5"):
        hero_id = side.get(pos, {}).get("hero_id")
        try:
            hero_id = int(hero_id)
        except (TypeError, ValueError):
            continue
        if hero_id > 0:
            hero_ids.append(hero_id)
    return tuple(hero_ids)


def _team_stats(hero_ids: Tuple[int, ...], features: Dict[str, Dict[str, Any]]) -> Dict[str, float]:
    stats = {
        "escape": 0.0,
        "melee_count": 0.0,
        "lock_tools": 0.0,
        "interrupt_tools": 0.0,
        "channel_vuln": 0.0,
        "control_pressure": 0.0,
        "save_resilience": 0.0,
        "bkb_control": 0.0,
        "initiation_tools": 0.0,
        "big_ult": 0.0,
        "big_ult_hard": 0.0,
    }
    for hero_id in hero_ids:
        row = features.get(str(hero_id))
        if not row:
            continue
        stats["escape"] += _as_float(row.get("has_escape", row.get("escape_count", 0.0)))
        stats["melee_count"] += _as_float(row.get("is_melee"))
        lock_count = (
            _as_float(row.get("has_stun"))
            + _as_float(row.get("has_silence"))
            + _as_float(row.get("has_hex"))
            + _as_float(row.get("has_leash"))
            + _as_float(row.get("has_root"))
            + _as_float(row.get("has_sleep"))
            + _as_float(row.get("has_fear"))
            + _as_float(row.get("has_taunt"))
            + 0.7 * _as_float(row.get("has_forced_movement"))
            + 0.6 * _as_float(row.get("has_banish"))
        )
        interrupt_count = (
            _as_float(row.get("has_stun"))
            + _as_float(row.get("has_silence"))
            + _as_float(row.get("has_hex"))
            + _as_float(row.get("has_sleep"))
            + _as_float(row.get("has_fear"))
            + _as_float(row.get("has_taunt"))
            + 0.5 * _as_float(row.get("has_forced_movement"))
            + 0.6 * _as_float(row.get("has_banish"))
        )
        control_pressure = (
            _as_float(row.get("stun_count"))
            + _as_float(row.get("root_count"))
            + _as_float(row.get("silence_count"))
            + _as_float(row.get("hex_count"))
            + _as_float(row.get("fear_count"))
            + _as_float(row.get("taunt_count"))
            + _as_float(row.get("sleep_count"))
            + _as_float(row.get("leash_count"))
            + 0.6 * _as_float(row.get("forced_movement_count"))
            + 0.8 * _as_float(row.get("banish_count"))
        )
        save_resilience = (
            _as_float(row.get("save_count"))
            + _as_float(row.get("strong_dispel_count"))
            + _as_float(row.get("has_uninterruptible_cast"))
        )
        initiation_tools = (
            _as_float(row.get("has_stun"))
            + _as_float(row.get("has_forced_movement"))
            + _as_float(row.get("has_taunt"))
            + _as_float(row.get("has_sleep"))
            + 0.5 * _as_float(row.get("has_fear"))
            + 0.5 * _as_float(row.get("has_uninterruptible_cast"))
        )
        bkb_control = _as_float(row.get("bkb_pierce_ability_count"))
        stats["lock_tools"] += lock_count
        stats["interrupt_tools"] += interrupt_count
        stats["channel_vuln"] += _as_float(row.get("interruptible_channel_count"))
        stats["control_pressure"] += control_pressure
        stats["save_resilience"] += save_resilience
        stats["initiation_tools"] += initiation_tools
        stats["bkb_control"] += bkb_control
        stats["big_ult"] += _as_float(row.get("big_ult_80s_lvl3"))
        stats["big_ult_hard"] += _as_float(row.get("big_ult_100s_lvl3"))
    return stats


def _edge_features(rad: Dict[str, float], dire: Dict[str, float]) -> Dict[str, float]:
    return {
        # Чем выше, тем больше у Radiant инструментов наказывать enemy-escape.
        "escape_lock": (rad["lock_tools"] * dire["escape"]) - (dire["lock_tools"] * rad["escape"]),
        # Наказание channeling-героев.
        "channel_interrupt": (rad["interrupt_tools"] * dire["channel_vuln"]) - (dire["interrupt_tools"] * rad["channel_vuln"]),
        # Общий pressure от дизейблов.
        "control_pressure": rad["control_pressure"] - dire["control_pressure"],
        # Сейв/диспел устойчивость.
        "save_resilience": rad["save_resilience"] - dire["save_resilience"],
        # Late-темп на длинных ультах.
        "big_ult_tradeoff": (
            (dire["big_ult"] * rad["control_pressure"] + dire["big_ult_hard"] * rad["interrupt_tools"])
            - (rad["big_ult"] * dire["control_pressure"] + rad["big_ult_hard"] * dire["interrupt_tools"])
        ),
    }


def _phase_adjustment(phase: str, edges: Dict[str, float], cfg: Dict[str, Any]) -> Tuple[int, float, Dict[str, float]]:
    phase_cfg = cfg.get(phase, {})
    phase_weights = phase_cfg.get("weights", {})
    scales = cfg.get("normalization_scales", {})
    normalized: Dict[str, float] = {}
    raw_score = 0.0
    for key in ("escape_lock", "channel_interrupt", "control_pressure", "save_resilience", "big_ult_tradeoff"):
        value = _as_float(edges.get(key))
        scale = _as_float(scales.get(key), 1.0)
        norm = _softsign(value, scale)
        normalized[key] = norm
        raw_score += _as_float(phase_weights.get(key)) * norm
    max_shift = _as_float(phase_cfg.get("max_shift"), 0.0)
    phase_shift = int(round(raw_score * max_shift))
    return phase_shift, raw_score, normalized


def _apply_metric_shift(
    phase_bucket: Dict[str, Any],
    phase_shift: int,
    metric_gains: Dict[str, Any],
    phase_cfg: Dict[str, Any],
    global_guardrails: Dict[str, Any],
) -> None:
    if phase_shift == 0:
        return
    min_abs_metric_to_adjust = _as_float(phase_cfg.get("min_abs_metric_to_adjust"), 0.0)
    min_phase_shift_for_new_bet = _as_float(phase_cfg.get("min_phase_shift_for_new_bet"), 0.0)
    allow_zero_to_nonzero = _as_bool(
        phase_cfg.get("allow_zero_to_nonzero"),
        _as_bool(global_guardrails.get("allow_zero_to_nonzero"), False),
    )
    allow_nonzero_to_zero = _as_bool(
        phase_cfg.get("allow_nonzero_to_zero"),
        _as_bool(global_guardrails.get("allow_nonzero_to_zero"), False),
    )
    allow_sign_flip = _as_bool(
        phase_cfg.get("allow_sign_flip"),
        _as_bool(global_guardrails.get("allow_sign_flip"), False),
    )
    for metric in TARGET_METRICS:
        current = phase_bucket.get(metric)
        if not isinstance(current, (int, float)):
            continue
        current_value = float(current)
        if current_value == 0.0:
            if not allow_zero_to_nonzero:
                continue
            if abs(phase_shift) < min_phase_shift_for_new_bet:
                continue
        elif abs(current_value) < min_abs_metric_to_adjust:
            continue
        gain = _as_float(metric_gains.get(metric), 1.0)
        delta = int(round(phase_shift * gain))
        if delta == 0:
            continue
        updated = int(round(current_value + delta))
        if current_value > 0.0:
            if not allow_sign_flip and updated <= 0:
                updated = 1
            elif not allow_nonzero_to_zero and updated == 0:
                updated = 1
        elif current_value < 0.0:
            if not allow_sign_flip and updated >= 0:
                updated = -1
            elif not allow_nonzero_to_zero and updated == 0:
                updated = -1
        if updated > 99:
            updated = 99
        if updated < -99:
            updated = -99
        phase_bucket[metric] = updated


def _hard_carry_id_set(gate_cfg: Dict[str, Any]) -> set[int]:
    raw = gate_cfg.get("hard_carry_ids")
    if not isinstance(raw, list):
        return set(DEFAULT_HARD_CARRY_IDS)
    out: set[int] = set()
    for value in raw:
        try:
            hero_id = int(value)
        except (TypeError, ValueError):
            continue
        if hero_id > 0:
            out.add(hero_id)
    if not out:
        return set(DEFAULT_HARD_CARRY_IDS)
    return out


def _apply_late_hard_carry_gate(
    phase: str,
    phase_bucket: Dict[str, Any],
    rad_ids: Tuple[int, ...],
    dire_ids: Tuple[int, ...],
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    info = {
        "enabled": False,
        "radiant_hard_carries": 0,
        "dire_hard_carries": 0,
        "changed_metrics": 0,
    }
    if phase != "late":
        return info
    late_cfg = cfg.get("late", {})
    gate_cfg = late_cfg.get("hard_carry_gate", {})
    if not isinstance(gate_cfg, dict):
        return info
    if not _as_bool(gate_cfg.get("enabled"), False):
        return info
    info["enabled"] = True

    hard_carry_ids = _hard_carry_id_set(gate_cfg)
    rad_hc = sum(1 for hero_id in rad_ids if hero_id in hard_carry_ids)
    dire_hc = sum(1 for hero_id in dire_ids if hero_id in hard_carry_ids)
    info["radiant_hard_carries"] = rad_hc
    info["dire_hard_carries"] = dire_hc

    deficit_min = int(_as_float(gate_cfg.get("deficit_min"), 1.0))
    carry_diff = dire_hc - rad_hc
    if abs(carry_diff) < max(deficit_min, 1):
        return info

    max_abs_metric_to_zero = _as_float(gate_cfg.get("max_abs_metric_to_zero"), 2.0)
    metrics = gate_cfg.get("metrics")
    if not isinstance(metrics, list) or not metrics:
        metrics = list(TARGET_METRICS)

    changed = 0
    for metric in metrics:
        current = phase_bucket.get(metric)
        if not isinstance(current, (int, float)):
            continue
        value = float(current)
        if value == 0.0:
            continue
        if abs(value) > max_abs_metric_to_zero:
            continue
        # Zero weak late bets that favor the side with hard-carry deficit.
        if carry_diff > 0 and value > 0:
            phase_bucket[metric] = 0
            changed += 1
        elif carry_diff < 0 and value < 0:
            phase_bucket[metric] = 0
            changed += 1
    info["changed_metrics"] = changed
    return info


def _apply_early_big_ult_burden_gate(
    phase: str,
    phase_bucket: Dict[str, Any],
    rad_stats: Dict[str, float],
    dire_stats: Dict[str, float],
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    info = {
        "enabled": False,
        "radiant_big_ults": 0,
        "dire_big_ults": 0,
        "changed_metrics": 0,
    }
    if phase != "early":
        return info
    early_cfg = cfg.get("early", {})
    gate_cfg = early_cfg.get("big_ult_burden_gate", {})
    if not isinstance(gate_cfg, dict):
        return info
    if not _as_bool(gate_cfg.get("enabled"), False):
        return info
    info["enabled"] = True

    rad_big = int(round(_as_float(rad_stats.get("big_ult"), 0.0)))
    dire_big = int(round(_as_float(dire_stats.get("big_ult"), 0.0)))
    info["radiant_big_ults"] = rad_big
    info["dire_big_ults"] = dire_big

    burden_diff_min = int(_as_float(gate_cfg.get("burden_diff_min"), 2.0))
    burden_diff_min = max(burden_diff_min, 1)
    max_abs_metric_to_zero = _as_float(gate_cfg.get("max_abs_metric_to_zero"), 2.0)
    metrics = gate_cfg.get("metrics")
    if not isinstance(metrics, list) or not metrics:
        metrics = list(TARGET_METRICS)

    changed = 0
    for metric in metrics:
        current = phase_bucket.get(metric)
        if not isinstance(current, (int, float)):
            continue
        value = float(current)
        if value == 0.0:
            continue
        if abs(value) > max_abs_metric_to_zero:
            continue
        # Zero weak early bets that favor side with more long-cooldown ult reliance.
        if value > 0 and (rad_big - dire_big) >= burden_diff_min:
            phase_bucket[metric] = 0
            changed += 1
        elif value < 0 and (dire_big - rad_big) >= burden_diff_min:
            phase_bucket[metric] = 0
            changed += 1
    info["changed_metrics"] = changed
    return info


def _metric_threshold_value(
    mapping: Any,
    metric: str,
    default_value: float,
) -> float:
    if isinstance(mapping, dict):
        return _as_float(mapping.get(metric), default_value)
    return default_value


def _apply_index_keep_gate(
    phase: str,
    phase_bucket: Dict[str, Any],
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    info = {
        "enabled": False,
        "changed_metrics": 0,
    }
    phase_cfg = cfg.get(phase, {})
    gate_cfg = phase_cfg.get("index_keep_gate", {})
    if not isinstance(gate_cfg, dict):
        return info
    if not _as_bool(gate_cfg.get("enabled"), False):
        return info
    info["enabled"] = True

    min_abs_default = _as_float(gate_cfg.get("min_abs_default"), 0.0)
    max_abs_default = _as_float(gate_cfg.get("max_abs_default"), 0.0)
    min_abs_by_metric = gate_cfg.get("min_abs_by_metric")
    max_abs_by_metric = gate_cfg.get("max_abs_by_metric")
    metrics = gate_cfg.get("metrics")
    if not isinstance(metrics, list) or not metrics:
        metrics = list(TARGET_METRICS)

    changed = 0
    for metric in metrics:
        current = phase_bucket.get(metric)
        if not isinstance(current, (int, float)):
            continue
        value = float(current)
        if value == 0.0:
            continue
        abs_value = abs(value)
        min_abs = _metric_threshold_value(min_abs_by_metric, metric, min_abs_default)
        max_abs = _metric_threshold_value(max_abs_by_metric, metric, max_abs_default)
        if min_abs > 0.0 and abs_value < min_abs:
            phase_bucket[metric] = 0
            changed += 1
            continue
        if max_abs > 0.0 and abs_value > max_abs:
            phase_bucket[metric] = 0
            changed += 1
            continue
    info["changed_metrics"] = changed
    return info


def _apply_early_feature_balance_gate(
    phase: str,
    phase_bucket: Dict[str, Any],
    rad_stats: Dict[str, float],
    dire_stats: Dict[str, float],
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    info = {
        "enabled": False,
        "changed_metrics": 0,
    }
    if phase != "early":
        return info
    early_cfg = cfg.get("early", {})
    gate_cfg = early_cfg.get("feature_balance_gate", {})
    if not isinstance(gate_cfg, dict):
        return info
    if not _as_bool(gate_cfg.get("enabled"), False):
        return info
    info["enabled"] = True

    max_abs_metric_to_zero = _as_float(gate_cfg.get("max_abs_metric_to_zero"), 99.0)
    escape_reliance_diff_min = _as_float(gate_cfg.get("escape_reliance_diff_min"), 1.0)
    enemy_lock_adv_min = _as_float(gate_cfg.get("enemy_lock_adv_min"), 1.5)
    save_deficit_min = _as_float(gate_cfg.get("save_deficit_min"), 1.0)
    enemy_control_adv_min = _as_float(gate_cfg.get("enemy_control_adv_min"), 2.0)
    melee_imbalance_min = _as_float(gate_cfg.get("melee_imbalance_min"), 2.0)
    enemy_control_adv_for_melee_min = _as_float(gate_cfg.get("enemy_control_adv_for_melee_min"), 2.0)

    metrics = gate_cfg.get("metrics")
    if not isinstance(metrics, list) or not metrics:
        metrics = list(TARGET_METRICS)

    def _favored_is_bad(
        fav_escape: float,
        opp_escape: float,
        fav_lock: float,
        opp_lock: float,
        fav_save: float,
        opp_save: float,
        fav_control: float,
        opp_control: float,
        fav_melee: float,
        opp_melee: float,
    ) -> bool:
        bad_escape_trade = (fav_escape - opp_escape) >= escape_reliance_diff_min and (opp_lock - fav_lock) >= enemy_lock_adv_min
        bad_save_trade = (opp_save - fav_save) >= save_deficit_min and (opp_control - fav_control) >= enemy_control_adv_min
        bad_melee_shape = (
            (fav_melee - opp_melee) >= melee_imbalance_min
            and (opp_control - fav_control) >= enemy_control_adv_for_melee_min
        )
        return bad_escape_trade or bad_save_trade or bad_melee_shape

    changed = 0
    for metric in metrics:
        current = phase_bucket.get(metric)
        if not isinstance(current, (int, float)):
            continue
        value = float(current)
        if value == 0.0 or abs(value) > max_abs_metric_to_zero:
            continue
        if value > 0.0:
            bad = _favored_is_bad(
                rad_stats["escape"],
                dire_stats["escape"],
                rad_stats["lock_tools"],
                dire_stats["lock_tools"],
                rad_stats["save_resilience"],
                dire_stats["save_resilience"],
                rad_stats["control_pressure"],
                dire_stats["control_pressure"],
                rad_stats["melee_count"],
                dire_stats["melee_count"],
            )
        else:
            bad = _favored_is_bad(
                dire_stats["escape"],
                rad_stats["escape"],
                dire_stats["lock_tools"],
                rad_stats["lock_tools"],
                dire_stats["save_resilience"],
                rad_stats["save_resilience"],
                dire_stats["control_pressure"],
                rad_stats["control_pressure"],
                dire_stats["melee_count"],
                rad_stats["melee_count"],
            )
        if bad:
            phase_bucket[metric] = 0
            changed += 1
    info["changed_metrics"] = changed
    return info


def _apply_early_support_gap_gate(
    phase: str,
    phase_bucket: Dict[str, Any],
    rad_stats: Dict[str, float],
    dire_stats: Dict[str, float],
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    info = {
        "enabled": False,
        "changed_metrics": 0,
    }
    if phase != "early":
        return info
    early_cfg = cfg.get("early", {})
    gate_cfg = early_cfg.get("support_gap_gate", {})
    if not isinstance(gate_cfg, dict):
        return info
    if not _as_bool(gate_cfg.get("enabled"), False):
        return info
    info["enabled"] = True

    max_abs_metric_to_zero = _as_float(gate_cfg.get("max_abs_metric_to_zero"), 99.0)
    save_deficit_min = _as_float(gate_cfg.get("save_deficit_min"), 1.0)
    escape_deficit_min = _as_float(gate_cfg.get("escape_deficit_min"), 1.0)
    metrics = gate_cfg.get("metrics")
    if not isinstance(metrics, list) or not metrics:
        metrics = list(TARGET_METRICS)

    def _favored_is_bad(fav_save: float, opp_save: float, fav_escape: float, opp_escape: float) -> bool:
        return (opp_save - fav_save) >= save_deficit_min or (opp_escape - fav_escape) >= escape_deficit_min

    changed = 0
    for metric in metrics:
        current = phase_bucket.get(metric)
        if not isinstance(current, (int, float)):
            continue
        value = float(current)
        if value == 0.0 or abs(value) > max_abs_metric_to_zero:
            continue
        if value > 0.0:
            bad = _favored_is_bad(rad_stats["save_resilience"], dire_stats["save_resilience"], rad_stats["escape"], dire_stats["escape"])
        else:
            bad = _favored_is_bad(dire_stats["save_resilience"], rad_stats["save_resilience"], dire_stats["escape"], rad_stats["escape"])
        if bad:
            phase_bucket[metric] = 0
            changed += 1
    info["changed_metrics"] = changed
    return info


def _apply_early_escape_vulnerability_gate(
    phase: str,
    phase_bucket: Dict[str, Any],
    rad_stats: Dict[str, float],
    dire_stats: Dict[str, float],
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    info = {
        "enabled": False,
        "radiant_escape": 0.0,
        "dire_escape": 0.0,
        "radiant_lock_tools": 0.0,
        "dire_lock_tools": 0.0,
        "changed_metrics": 0,
    }
    if phase != "early":
        return info
    early_cfg = cfg.get("early", {})
    gate_cfg = early_cfg.get("escape_vulnerability_gate", {})
    if not isinstance(gate_cfg, dict):
        return info
    if not _as_bool(gate_cfg.get("enabled"), False):
        return info
    info["enabled"] = True

    rad_escape = _as_float(rad_stats.get("escape"), 0.0)
    dire_escape = _as_float(dire_stats.get("escape"), 0.0)
    rad_lock = _as_float(rad_stats.get("lock_tools"), 0.0)
    dire_lock = _as_float(dire_stats.get("lock_tools"), 0.0)
    info["radiant_escape"] = round(rad_escape, 3)
    info["dire_escape"] = round(dire_escape, 3)
    info["radiant_lock_tools"] = round(rad_lock, 3)
    info["dire_lock_tools"] = round(dire_lock, 3)

    escape_diff_min = _as_float(gate_cfg.get("escape_diff_min"), 1.0)
    enemy_lock_adv_min = _as_float(gate_cfg.get("enemy_lock_adv_min"), 1.5)
    max_abs_metric_to_zero = _as_float(gate_cfg.get("max_abs_metric_to_zero"), 2.0)
    metrics = gate_cfg.get("metrics")
    if not isinstance(metrics, list) or not metrics:
        metrics = list(TARGET_METRICS)

    changed = 0
    for metric in metrics:
        current = phase_bucket.get(metric)
        if not isinstance(current, (int, float)):
            continue
        value = float(current)
        if value == 0.0:
            continue
        if abs(value) > max_abs_metric_to_zero:
            continue
        if value > 0:
            if (rad_escape - dire_escape) >= escape_diff_min and (dire_lock - rad_lock) >= enemy_lock_adv_min:
                phase_bucket[metric] = 0
                changed += 1
        else:
            if (dire_escape - rad_escape) >= escape_diff_min and (rad_lock - dire_lock) >= enemy_lock_adv_min:
                phase_bucket[metric] = 0
                changed += 1
    info["changed_metrics"] = changed
    return info


def _apply_late_support_gap_gate(
    phase: str,
    phase_bucket: Dict[str, Any],
    rad_stats: Dict[str, float],
    dire_stats: Dict[str, float],
    rad_ids: Tuple[int, ...],
    dire_ids: Tuple[int, ...],
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    info = {
        "enabled": False,
        "changed_metrics": 0,
    }
    if phase != "late":
        return info
    late_cfg = cfg.get("late", {})
    gate_cfg = late_cfg.get("support_gap_gate", {})
    if not isinstance(gate_cfg, dict):
        return info
    if not _as_bool(gate_cfg.get("enabled"), False):
        return info
    info["enabled"] = True

    max_abs_metric_to_zero = _as_float(gate_cfg.get("max_abs_metric_to_zero"), 99.0)
    save_deficit_min = _as_float(gate_cfg.get("save_deficit_min"), 1.0)
    escape_deficit_min = _as_float(gate_cfg.get("escape_deficit_min"), 1.0)
    carry_deficit_min = _as_float(gate_cfg.get("carry_deficit_min"), 1.0)
    bkb_deficit_min = _as_float(gate_cfg.get("bkb_deficit_min"), 1.0)
    metrics = gate_cfg.get("metrics")
    if not isinstance(metrics, list) or not metrics:
        metrics = list(TARGET_METRICS)

    hard_carry_ids = _hard_carry_id_set(late_cfg.get("hard_carry_gate", {}))
    rad_carry = float(sum(1 for hero_id in rad_ids if hero_id in hard_carry_ids))
    dire_carry = float(sum(1 for hero_id in dire_ids if hero_id in hard_carry_ids))

    def _favored_is_bad(
        fav_save: float,
        opp_save: float,
        fav_escape: float,
        opp_escape: float,
        fav_carry: float,
        opp_carry: float,
        fav_bkb: float,
        opp_bkb: float,
    ) -> bool:
        bad_support = (opp_save - fav_save) >= save_deficit_min or (opp_escape - fav_escape) >= escape_deficit_min
        bad_late_tools = (opp_carry - fav_carry) >= carry_deficit_min or (opp_bkb - fav_bkb) >= bkb_deficit_min
        return bad_support or bad_late_tools

    changed = 0
    for metric in metrics:
        current = phase_bucket.get(metric)
        if not isinstance(current, (int, float)):
            continue
        value = float(current)
        if value == 0.0 or abs(value) > max_abs_metric_to_zero:
            continue
        if value > 0.0:
            bad = _favored_is_bad(
                rad_stats["save_resilience"],
                dire_stats["save_resilience"],
                rad_stats["escape"],
                dire_stats["escape"],
                rad_carry,
                dire_carry,
                rad_stats["bkb_control"],
                dire_stats["bkb_control"],
            )
        else:
            bad = _favored_is_bad(
                dire_stats["save_resilience"],
                rad_stats["save_resilience"],
                dire_stats["escape"],
                rad_stats["escape"],
                dire_carry,
                rad_carry,
                dire_stats["bkb_control"],
                rad_stats["bkb_control"],
            )
        if bad:
            phase_bucket[metric] = 0
            changed += 1
    info["changed_metrics"] = changed
    return info


def _apply_late_control_stability_gate(
    phase: str,
    phase_bucket: Dict[str, Any],
    rad_stats: Dict[str, float],
    dire_stats: Dict[str, float],
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    info = {
        "enabled": False,
        "radiant_control_pressure": 0.0,
        "dire_control_pressure": 0.0,
        "radiant_save_resilience": 0.0,
        "dire_save_resilience": 0.0,
        "changed_metrics": 0,
    }
    if phase != "late":
        return info
    late_cfg = cfg.get("late", {})
    gate_cfg = late_cfg.get("control_stability_gate", {})
    if not isinstance(gate_cfg, dict):
        return info
    if not _as_bool(gate_cfg.get("enabled"), False):
        return info
    info["enabled"] = True

    rad_control = _as_float(rad_stats.get("control_pressure"), 0.0)
    dire_control = _as_float(dire_stats.get("control_pressure"), 0.0)
    rad_save = _as_float(rad_stats.get("save_resilience"), 0.0)
    dire_save = _as_float(dire_stats.get("save_resilience"), 0.0)
    info["radiant_control_pressure"] = round(rad_control, 3)
    info["dire_control_pressure"] = round(dire_control, 3)
    info["radiant_save_resilience"] = round(rad_save, 3)
    info["dire_save_resilience"] = round(dire_save, 3)

    control_deficit_min = _as_float(gate_cfg.get("control_deficit_min"), 2.0)
    save_deficit_min = _as_float(gate_cfg.get("save_deficit_min"), 1.0)
    max_abs_metric_to_zero = _as_float(gate_cfg.get("max_abs_metric_to_zero"), 2.0)
    metrics = gate_cfg.get("metrics")
    if not isinstance(metrics, list) or not metrics:
        metrics = list(TARGET_METRICS)

    changed = 0
    for metric in metrics:
        current = phase_bucket.get(metric)
        if not isinstance(current, (int, float)):
            continue
        value = float(current)
        if value == 0.0:
            continue
        if abs(value) > max_abs_metric_to_zero:
            continue
        if value > 0:
            if (dire_control - rad_control) >= control_deficit_min and (dire_save - rad_save) >= save_deficit_min:
                phase_bucket[metric] = 0
                changed += 1
        else:
            if (rad_control - dire_control) >= control_deficit_min and (rad_save - dire_save) >= save_deficit_min:
                phase_bucket[metric] = 0
                changed += 1
    info["changed_metrics"] = changed
    return info


def _apply_late_role_balance_gate(
    phase: str,
    phase_bucket: Dict[str, Any],
    rad_stats: Dict[str, float],
    dire_stats: Dict[str, float],
    rad_ids: Tuple[int, ...],
    dire_ids: Tuple[int, ...],
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    info = {
        "enabled": False,
        "changed_metrics": 0,
        "radiant_role_score": 0.0,
        "dire_role_score": 0.0,
    }
    if phase != "late":
        return info
    late_cfg = cfg.get("late", {})
    gate_cfg = late_cfg.get("role_balance_gate", {})
    if not isinstance(gate_cfg, dict):
        return info
    if not _as_bool(gate_cfg.get("enabled"), False):
        return info
    info["enabled"] = True

    max_abs_metric_to_zero = _as_float(gate_cfg.get("max_abs_metric_to_zero"), 99.0)
    score_gap_min = _as_float(gate_cfg.get("score_gap_min"), 1.0)
    bkb_deficit_min = _as_float(gate_cfg.get("bkb_deficit_min"), 1.0)
    carry_deficit_min = _as_float(gate_cfg.get("carry_deficit_min"), 1.0)
    control_deficit_min = _as_float(gate_cfg.get("control_deficit_min"), 2.0)
    save_deficit_min = _as_float(gate_cfg.get("save_deficit_min"), 1.0)
    min_initiation_tools = _as_float(gate_cfg.get("min_initiation_tools"), 2.0)
    min_save_resilience = _as_float(gate_cfg.get("min_save_resilience"), 1.0)
    min_bkb_control = _as_float(gate_cfg.get("min_bkb_control"), 1.0)
    min_control_pressure = _as_float(gate_cfg.get("min_control_pressure"), 8.0)

    metrics = gate_cfg.get("metrics")
    if not isinstance(metrics, list) or not metrics:
        metrics = list(TARGET_METRICS)

    hard_carry_ids = _hard_carry_id_set(late_cfg.get("hard_carry_gate", {}))
    rad_carry = float(sum(1 for hero_id in rad_ids if hero_id in hard_carry_ids))
    dire_carry = float(sum(1 for hero_id in dire_ids if hero_id in hard_carry_ids))

    def _role_score(carry: float, initiation: float, save: float, bkb: float, control: float) -> float:
        score = 0.0
        if carry >= 1.0:
            score += 1.0
        if initiation >= min_initiation_tools:
            score += 1.0
        if save >= min_save_resilience:
            score += 1.0
        if bkb >= min_bkb_control:
            score += 1.0
        if control >= min_control_pressure:
            score += 1.0
        return score

    rad_score = _role_score(
        rad_carry, rad_stats["initiation_tools"], rad_stats["save_resilience"], rad_stats["bkb_control"], rad_stats["control_pressure"]
    )
    dire_score = _role_score(
        dire_carry, dire_stats["initiation_tools"], dire_stats["save_resilience"], dire_stats["bkb_control"], dire_stats["control_pressure"]
    )
    info["radiant_role_score"] = rad_score
    info["dire_role_score"] = dire_score

    def _favored_is_bad(
        fav_score: float,
        opp_score: float,
        fav_bkb: float,
        opp_bkb: float,
        fav_carry: float,
        opp_carry: float,
        fav_control: float,
        opp_control: float,
        fav_save: float,
        opp_save: float,
    ) -> bool:
        if (opp_score - fav_score) < score_gap_min:
            return False
        bad_bkb = (opp_bkb - fav_bkb) >= bkb_deficit_min
        bad_carry = (opp_carry - fav_carry) >= carry_deficit_min
        bad_control_save = (opp_control - fav_control) >= control_deficit_min and (opp_save - fav_save) >= save_deficit_min
        return bad_bkb or bad_carry or bad_control_save

    changed = 0
    for metric in metrics:
        current = phase_bucket.get(metric)
        if not isinstance(current, (int, float)):
            continue
        value = float(current)
        if value == 0.0 or abs(value) > max_abs_metric_to_zero:
            continue
        if value > 0.0:
            bad = _favored_is_bad(
                rad_score,
                dire_score,
                rad_stats["bkb_control"],
                dire_stats["bkb_control"],
                rad_carry,
                dire_carry,
                rad_stats["control_pressure"],
                dire_stats["control_pressure"],
                rad_stats["save_resilience"],
                dire_stats["save_resilience"],
            )
        else:
            bad = _favored_is_bad(
                dire_score,
                rad_score,
                dire_stats["bkb_control"],
                rad_stats["bkb_control"],
                dire_carry,
                rad_carry,
                dire_stats["control_pressure"],
                rad_stats["control_pressure"],
                dire_stats["save_resilience"],
                rad_stats["save_resilience"],
            )
        if bad:
            phase_bucket[metric] = 0
            changed += 1
    info["changed_metrics"] = changed
    return info


def _gate_threshold(cfg: Dict[str, Any], key: str) -> float | None:
    if key not in cfg:
        return None
    value = cfg.get(key)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _apply_edge_requirements_gate(
    phase: str,
    phase_bucket: Dict[str, Any],
    rad_stats: Dict[str, float],
    dire_stats: Dict[str, float],
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    info: Dict[str, Any] = {
        "enabled": False,
        "changed_metrics": 0,
    }
    phase_cfg = cfg.get(phase, {})
    gate_cfg = phase_cfg.get("edge_requirements_gate", {})
    if not isinstance(gate_cfg, dict):
        return info
    if not _as_bool(gate_cfg.get("enabled"), False):
        return info
    info["enabled"] = True

    metrics = gate_cfg.get("metrics")
    if not isinstance(metrics, list) or not metrics:
        metrics = list(TARGET_METRICS)
    max_abs_metric_to_zero = _as_float(gate_cfg.get("max_abs_metric_to_zero"), 99.0)

    min_escape_edge = _gate_threshold(gate_cfg, "min_escape_edge")
    min_save_edge = _gate_threshold(gate_cfg, "min_save_edge")
    min_bkb_edge = _gate_threshold(gate_cfg, "min_bkb_edge")
    min_control_edge = _gate_threshold(gate_cfg, "min_control_edge")
    min_lock_edge = _gate_threshold(gate_cfg, "min_lock_edge")
    min_initiation_edge = _gate_threshold(gate_cfg, "min_initiation_edge")
    max_big_ult_edge = _gate_threshold(gate_cfg, "max_big_ult_edge")
    max_big_ult_hard_edge = _gate_threshold(gate_cfg, "max_big_ult_hard_edge")
    max_melee_edge = _gate_threshold(gate_cfg, "max_melee_edge")

    def _favored_is_good(fav: Dict[str, float], opp: Dict[str, float]) -> bool:
        escape_edge = fav["escape"] - opp["escape"]
        save_edge = fav["save_resilience"] - opp["save_resilience"]
        bkb_edge = fav["bkb_control"] - opp["bkb_control"]
        control_edge = fav["control_pressure"] - opp["control_pressure"]
        lock_edge = fav["lock_tools"] - opp["lock_tools"]
        initiation_edge = fav["initiation_tools"] - opp["initiation_tools"]
        big_ult_edge = fav["big_ult"] - opp["big_ult"]
        big_ult_hard_edge = fav["big_ult_hard"] - opp["big_ult_hard"]
        melee_edge = fav["melee_count"] - opp["melee_count"]
        if min_escape_edge is not None and escape_edge < min_escape_edge:
            return False
        if min_save_edge is not None and save_edge < min_save_edge:
            return False
        if min_bkb_edge is not None and bkb_edge < min_bkb_edge:
            return False
        if min_control_edge is not None and control_edge < min_control_edge:
            return False
        if min_lock_edge is not None and lock_edge < min_lock_edge:
            return False
        if min_initiation_edge is not None and initiation_edge < min_initiation_edge:
            return False
        if max_big_ult_edge is not None and big_ult_edge > max_big_ult_edge:
            return False
        if max_big_ult_hard_edge is not None and big_ult_hard_edge > max_big_ult_hard_edge:
            return False
        if max_melee_edge is not None and melee_edge > max_melee_edge:
            return False
        return True

    changed = 0
    for metric in metrics:
        current = phase_bucket.get(metric)
        if not isinstance(current, (int, float)):
            continue
        value = float(current)
        if value == 0.0:
            continue
        if abs(value) > max_abs_metric_to_zero:
            continue
        if value > 0.0:
            favored_good = _favored_is_good(rad_stats, dire_stats)
        else:
            favored_good = _favored_is_good(dire_stats, rad_stats)
        if not favored_good:
            phase_bucket[metric] = 0
            changed += 1
    info["changed_metrics"] = changed
    return info


def _apply_phase_wrapper(
    phase: str,
    phase_bucket: Dict[str, Any],
    radiant_heroes_and_pos: Dict[str, Any],
    dire_heroes_and_pos: Dict[str, Any],
    enabled_env_name: str,
    phase_context: Dict[str, Any] | None = None,
) -> None:
    if not _is_enabled("SIGNAL_WRAPPER_ENABLED", default=False):
        return
    if not _is_enabled(enabled_env_name, default=True):
        return
    if not isinstance(phase_bucket, dict):
        return

    mode = _wrapper_mode()
    if mode in {"ml", "auto"}:
        ml_info = _apply_phase_ml_wrapper(
            phase=phase,
            phase_bucket=phase_bucket,
            radiant_heroes_and_pos=radiant_heroes_and_pos,
            dire_heroes_and_pos=dire_heroes_and_pos,
            phase_context=phase_context,
        )
        if ml_info is not None:
            # Keep compact ML metadata available for runtime decision logic.
            phase_bucket.setdefault("_ml_meta", {})[phase] = {
                "probs": dict(ml_info.get("probs") or {}),
                "model_path": ml_info.get("model_path"),
            }
            if _wrapper_debug_enabled():
                phase_bucket.setdefault("_wrapper_meta", {})[phase] = {
                    "mode": "ml",
                    **ml_info,
                }
            return

    features = _load_hero_features()
    if not features:
        return
    cfg = _load_wrapper_config()

    rad_ids = _team_hero_ids(radiant_heroes_and_pos)
    dire_ids = _team_hero_ids(dire_heroes_and_pos)
    if len(rad_ids) < 5 or len(dire_ids) < 5:
        return

    rad_stats = _team_stats(rad_ids, features)
    dire_stats = _team_stats(dire_ids, features)
    edges = _edge_features(rad_stats, dire_stats)
    phase_shift, raw_score, normalized = _phase_adjustment(phase, edges, cfg)

    phase_cfg = cfg.get(phase, {})
    metric_gains = phase_cfg.get("metric_gains", {})
    global_guardrails = cfg.get("guardrails", {})
    _apply_metric_shift(phase_bucket, phase_shift, metric_gains, phase_cfg, global_guardrails)
    index_keep_gate_info = _apply_index_keep_gate(phase, phase_bucket, cfg)
    early_feature_balance_gate_info = _apply_early_feature_balance_gate(
        phase, phase_bucket, rad_stats, dire_stats, cfg
    )
    early_support_gap_gate_info = _apply_early_support_gap_gate(
        phase, phase_bucket, rad_stats, dire_stats, cfg
    )
    early_big_ult_gate_info = _apply_early_big_ult_burden_gate(phase, phase_bucket, rad_stats, dire_stats, cfg)
    early_escape_vulnerability_gate_info = _apply_early_escape_vulnerability_gate(
        phase, phase_bucket, rad_stats, dire_stats, cfg
    )
    late_role_balance_gate_info = _apply_late_role_balance_gate(
        phase, phase_bucket, rad_stats, dire_stats, rad_ids, dire_ids, cfg
    )
    late_support_gap_gate_info = _apply_late_support_gap_gate(
        phase, phase_bucket, rad_stats, dire_stats, rad_ids, dire_ids, cfg
    )
    late_control_stability_gate_info = _apply_late_control_stability_gate(
        phase, phase_bucket, rad_stats, dire_stats, cfg
    )
    edge_requirements_gate_info = _apply_edge_requirements_gate(
        phase, phase_bucket, rad_stats, dire_stats, cfg
    )
    hard_carry_gate_info = _apply_late_hard_carry_gate(phase, phase_bucket, rad_ids, dire_ids, cfg)

    if _wrapper_debug_enabled():
        phase_bucket.setdefault("_wrapper_meta", {})[phase] = {
            "mode": "heuristic",
            "phase_shift": phase_shift,
            "raw_score": round(raw_score, 6),
            "edges": {k: round(v, 6) for k, v in edges.items()},
            "normalized": {k: round(v, 6) for k, v in normalized.items()},
            "index_keep_gate": index_keep_gate_info,
            "early_feature_balance_gate": early_feature_balance_gate_info,
            "early_support_gap_gate": early_support_gap_gate_info,
            "early_big_ult_burden_gate": early_big_ult_gate_info,
            "early_escape_vulnerability_gate": early_escape_vulnerability_gate_info,
            "late_role_balance_gate": late_role_balance_gate_info,
            "late_support_gap_gate": late_support_gap_gate_info,
            "late_control_stability_gate": late_control_stability_gate_info,
            "edge_requirements_gate": edge_requirements_gate_info,
            "late_hard_carry_gate": hard_carry_gate_info,
        }


def apply_early_signal_wrapper(
    phase_bucket: Dict[str, Any],
    radiant_heroes_and_pos: Dict[str, Any],
    dire_heroes_and_pos: Dict[str, Any],
    phase_context: Dict[str, Any] | None = None,
) -> None:
    _apply_phase_wrapper(
        phase="early",
        phase_bucket=phase_bucket,
        radiant_heroes_and_pos=radiant_heroes_and_pos,
        dire_heroes_and_pos=dire_heroes_and_pos,
        enabled_env_name="SIGNAL_WRAPPER_EARLY_ENABLED",
        phase_context=phase_context,
    )


def apply_late_signal_wrapper(
    phase_bucket: Dict[str, Any],
    radiant_heroes_and_pos: Dict[str, Any],
    dire_heroes_and_pos: Dict[str, Any],
    phase_context: Dict[str, Any] | None = None,
) -> None:
    _apply_phase_wrapper(
        phase="late",
        phase_bucket=phase_bucket,
        radiant_heroes_and_pos=radiant_heroes_and_pos,
        dire_heroes_and_pos=dire_heroes_and_pos,
        enabled_env_name="SIGNAL_WRAPPER_LATE_ENABLED",
        phase_context=phase_context,
    )
