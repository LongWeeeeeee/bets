from __future__ import annotations

import json
import math
from itertools import combinations
from pathlib import Path
from typing import Iterable, Optional

try:
    from analise_database import is_pro_match
    from explore_database import PATCH_739_RELEASE_TS
    from maps_research import check_match_quality
except ImportError:  # package import for tests
    from base.analise_database import is_pro_match
    from base.explore_database import PATCH_739_RELEASE_TS
    from base.maps_research import check_match_quality

MAX_DURATION_SECONDS = 34 * 60
TEMPO_RATE_FIELDS = ("kills_pm", "deaths_pm", "assists_pm", "hero_damage_pm")
TEMPO_INDEX_SCALES = {
    "kills_pm": 10.0,
    "deaths_pm": 10.0,
    "assists_pm": 10.0,
    "hero_damage_pm": 0.01,
}
TEMPO_FAMILY_FACTORS = {
    "solo": 10.0,
    "synergy_duo": 5.0,
    "counterpick_1vs1": 5.0,
}
CORE_POSITIONS = ("pos1", "pos2", "pos3")
ALL_POSITIONS = ("pos1", "pos2", "pos3", "pos4", "pos5")
HERO_FEATURES_CANDIDATES = (
    Path("/Users/alex/Documents/ingame/data/hero_features_processed.json"),
    Path("/Users/alex/Documents/ingame/base/hero_features_processed.json"),
    Path(__file__).resolve().parent / "hero_features_processed.json",
)


def _normalize_position(value) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, int):
        if 1 <= value <= 5:
            return f"pos{value}"
        return None
    value = str(value).strip()
    if not value:
        return None
    if value.startswith("POSITION_"):
        value = value.split("_", 1)[1]
    if value.startswith("pos"):
        suffix = value[3:]
    else:
        suffix = value
    if suffix in {"1", "2", "3", "4", "5"}:
        return f"pos{suffix}"
    return None


def _hero_pos_key(hero_id: int, position: str) -> str:
    return f"{int(hero_id)}{position}"


def _pair_key(left: str, right: str, sep: str) -> str:
    a, b = sorted((left, right))
    return f"{a}{sep}{b}"


def _empty_record() -> dict:
    return {
        "games": 0,
        "kills_pm_sum": 0.0,
        "deaths_pm_sum": 0.0,
        "assists_pm_sum": 0.0,
        "hero_damage_pm_sum": 0.0,
    }


def _append_metrics(target_dict: dict, key: str, metric_values: dict[str, float]) -> None:
    record = target_dict.setdefault(key, _empty_record())
    record["games"] += 1
    for metric_name, metric_value in metric_values.items():
        record[f"{metric_name}_sum"] += float(metric_value)


def _record_mean(record: dict, metric_name: str) -> Optional[float]:
    if not isinstance(record, dict):
        return None
    games = int(record.get("games", 0) or 0)
    if games <= 0:
        return None
    total = record.get(f"{metric_name}_sum")
    if total is None:
        return None
    return float(total) / games


def _minutes_played(match: dict) -> Optional[float]:
    duration_seconds = match.get("durationSeconds")
    try:
        duration_seconds = int(duration_seconds)
    except (TypeError, ValueError):
        return None
    if duration_seconds <= 0 or duration_seconds > MAX_DURATION_SECONDS:
        return None
    return duration_seconds / 60.0


def _player_pm_metrics(player: dict, minutes_played: float) -> dict[str, float]:
    return {
        "kills_pm": float(player.get("kills", 0) or 0) / minutes_played,
        "deaths_pm": float(player.get("deaths", 0) or 0) / minutes_played,
        "assists_pm": float(player.get("assists", 0) or 0) / minutes_played,
        "hero_damage_pm": float(player.get("heroDamage", 0) or 0) / minutes_played,
    }


def _sum_metric_values(left: dict[str, float], right: dict[str, float]) -> dict[str, float]:
    return {metric_name: left[metric_name] + right[metric_name] for metric_name in TEMPO_RATE_FIELDS}


def extract_players_by_position(match: dict) -> tuple[Optional[dict[str, dict]], Optional[dict[str, dict]]]:
    radiant_by_pos: dict[str, dict] = {}
    dire_by_pos: dict[str, dict] = {}
    for player in match.get("players", []):
        hero_id = player.get("heroId")
        try:
            hero_id = int(hero_id)
        except (TypeError, ValueError):
            return None, None
        pos = _normalize_position(player.get("position"))
        if pos is None:
            return None, None
        if player.get("isRadiant"):
            radiant_by_pos[pos] = player
        else:
            dire_by_pos[pos] = player
    if tuple(sorted(radiant_by_pos)) != ALL_POSITIONS or tuple(sorted(dire_by_pos)) != ALL_POSITIONS:
        return None, None
    return radiant_by_pos, dire_by_pos


def _iter_side_keys(side_by_pos: dict[str, dict]) -> Iterable[tuple[str, int, dict, dict[str, float]]]:
    for pos in ALL_POSITIONS:
        player = side_by_pos[pos]
        hero_id = int(player["heroId"])
        yield pos, hero_id, player, player["__tempo_metrics"]


def process_tempo_pub_match(
    match: dict,
    solo_dict: dict,
    synergy_duo_dict: dict,
    counterpick_1vs1_dict: dict,
    *,
    min_start_ts: int = PATCH_739_RELEASE_TS,
    strict_positions: bool = True,
) -> bool:
    if not isinstance(match, dict):
        return False
    if is_pro_match(match):
        return False
    start_ts = match.get("startDateTime")
    try:
        start_ts = int(start_ts)
    except (TypeError, ValueError):
        return False
    if start_ts < int(min_start_ts):
        return False
    minutes_played = _minutes_played(match)
    if minutes_played is None:
        return False
    quality_ok, _ = check_match_quality(match, strict_lane_positions=strict_positions)
    if not quality_ok:
        return False

    radiant_by_pos, dire_by_pos = extract_players_by_position(match)
    if radiant_by_pos is None or dire_by_pos is None:
        return False

    for player in match.get("players", []):
        player["__tempo_metrics"] = _player_pm_metrics(player, minutes_played)

    for side_by_pos in (radiant_by_pos, dire_by_pos):
        side_items = list(_iter_side_keys(side_by_pos))
        for pos, hero_id, _player, metric_values in side_items:
            _append_metrics(solo_dict, _hero_pos_key(hero_id, pos), metric_values)
        for left, right in combinations(side_items, 2):
            left_pos, left_hero_id, _left_player, left_metrics = left
            right_pos, right_hero_id, _right_player, right_metrics = right
            duo_key = _pair_key(
                _hero_pos_key(left_hero_id, left_pos),
                _hero_pos_key(right_hero_id, right_pos),
                "_with_",
            )
            _append_metrics(synergy_duo_dict, duo_key, _sum_metric_values(left_metrics, right_metrics))

    radiant_items = list(_iter_side_keys(radiant_by_pos))
    dire_items = list(_iter_side_keys(dire_by_pos))
    for r_item in radiant_items:
        r_pos, r_hero_id, _r_player, r_metrics = r_item
        r_key = _hero_pos_key(r_hero_id, r_pos)
        for d_item in dire_items:
            d_pos, d_hero_id, _d_player, d_metrics = d_item
            d_key = _hero_pos_key(d_hero_id, d_pos)
            cp_key = _pair_key(r_key, d_key, "_vs_")
            _append_metrics(counterpick_1vs1_dict, cp_key, _sum_metric_values(r_metrics, d_metrics))

    for player in match.get("players", []):
        player.pop("__tempo_metrics", None)
    return True


def load_hero_name_map() -> dict[int, str]:
    for path in HERO_FEATURES_CANDIDATES:
        if not path.exists():
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        hero_name_by_id: dict[int, str] = {}
        if isinstance(data, dict):
            for hero_id, payload in data.items():
                try:
                    hero_id_int = int(payload.get("hero_id", hero_id))
                except Exception:
                    continue
                hero_name = payload.get("hero_name") or payload.get("hero_slug") or f"hero_{hero_id_int}"
                hero_name_by_id[hero_id_int] = str(hero_name)
        if hero_name_by_id:
            return hero_name_by_id
    return {}


def draft_to_named_payload(
    radiant_heroes_and_pos: dict,
    dire_heroes_and_pos: dict,
    hero_name_by_id: Optional[dict[int, str]] = None,
) -> dict:
    hero_name_by_id = hero_name_by_id or {}

    def _side_payload(side: dict) -> dict[str, dict[str, object]]:
        payload = {}
        for pos in ALL_POSITIONS:
            hero_id = int(side[pos]["hero_id"])
            payload[pos] = {
                "hero_id": hero_id,
                "hero_name": hero_name_by_id.get(hero_id, f"hero_{hero_id}"),
            }
        return payload

    return {
        "radiant": _side_payload(radiant_heroes_and_pos),
        "dire": _side_payload(dire_heroes_and_pos),
    }


def _values_for_keys(source_dict: dict, keys: Iterable[str], metric_name: str) -> list[float]:
    values = []
    for key in keys:
        value = _record_mean(source_dict.get(key), metric_name)
        if value is not None:
            values.append(value)
    return values


def _match_level_metric(values: list[float], family: str) -> Optional[float]:
    if not values:
        return None
    return (sum(values) / len(values)) * TEMPO_FAMILY_FACTORS[family]


def get_tempo_index(metric_name: str, predicted_value: Optional[float]) -> Optional[int]:
    if predicted_value is None:
        return None
    scale = TEMPO_INDEX_SCALES[metric_name]
    scaled = predicted_value * scale
    if not math.isfinite(scaled):
        return None
    return int(round(scaled))


def build_tempo_draft_metrics(
    radiant_heroes_and_pos: dict,
    dire_heroes_and_pos: dict,
    solo_dict: dict,
    synergy_duo_dict: dict,
    counterpick_1vs1_dict: dict,
) -> dict:
    radiant_keys = [_hero_pos_key(int(radiant_heroes_and_pos[pos]["hero_id"]), pos) for pos in ALL_POSITIONS]
    dire_keys = [_hero_pos_key(int(dire_heroes_and_pos[pos]["hero_id"]), pos) for pos in ALL_POSITIONS]
    all_solo_keys = radiant_keys + dire_keys
    all_duo_keys = [
        _pair_key(left, right, "_with_")
        for team_keys in (radiant_keys, dire_keys)
        for left, right in combinations(team_keys, 2)
    ]
    all_cp1v1_keys = [_pair_key(r_key, d_key, "_vs_") for r_key in radiant_keys for d_key in dire_keys]

    family_specs = {
        "solo": (solo_dict, all_solo_keys, len(all_solo_keys)),
        "synergy_duo": (synergy_duo_dict, all_duo_keys, len(all_duo_keys)),
        "counterpick_1vs1": (counterpick_1vs1_dict, all_cp1v1_keys, len(all_cp1v1_keys)),
    }
    output = {}
    for family_name, (source_dict, keys, required_count) in family_specs.items():
        family_payload = {
            "required": required_count,
            "found": 0,
            "complete": False,
        }
        for metric_name in TEMPO_RATE_FIELDS:
            values = _values_for_keys(source_dict, keys, metric_name)
            predicted_total_pm = _match_level_metric(values, family_name)
            family_payload[metric_name] = {
                "predicted_total_pm": predicted_total_pm,
                "index": get_tempo_index(metric_name, predicted_total_pm),
                "found": len(values),
                "required": required_count,
            }
        found_set = max(family_payload[metric_name]["found"] for metric_name in TEMPO_RATE_FIELDS)
        family_payload["found"] = found_set
        family_payload["complete"] = found_set == required_count
        output[family_name] = family_payload
    return output


def compute_match_total_kills_per_min(match: dict) -> Optional[float]:
    minutes_played = match.get("durationSeconds")
    try:
        minutes_played = float(minutes_played) / 60.0
    except (TypeError, ValueError):
        return None
    if minutes_played <= 0:
        return None
    total_kills = 0.0
    for player in match.get("players", []):
        total_kills += float(player.get("kills", 0) or 0)
    return total_kills / minutes_played


def load_tempo_dicts(base_dir: Path) -> tuple[dict, dict, dict]:
    base_dir = Path(base_dir)
    solo = json.loads((base_dir / "tempo_solo_dict_raw.json").read_text(encoding="utf-8"))
    duo = json.loads((base_dir / "tempo_synergy_duo_dict_raw.json").read_text(encoding="utf-8"))
    cp1v1 = json.loads((base_dir / "tempo_counterpick_1vs1_dict_raw.json").read_text(encoding="utf-8"))
    return solo, duo, cp1v1
