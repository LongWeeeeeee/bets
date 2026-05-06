from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List
from zoneinfo import ZoneInfo
from datetime import datetime

import orjson
import pytest
from bs4 import BeautifulSoup


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402


def test_live_entrypoint_defaults_keep_signal_gates_enabled(monkeypatch) -> None:
    for env_name in (
        "DOTA2PROTRACKER_ENABLED",
        "PIPELINE_DISABLE_SIGNAL_GATES",
        "PIPELINE_SEND_EVERY_PARSED_MATCH",
        "PIPELINE_BYPASS_BOOKMAKER_GATE",
        "PIPELINE_BYPASS_TIER_GATE",
        "PIPELINE_BYPASS_LEAGUE_DENYLIST_GATE",
        "PIPELINE_BYPASS_PROTRACKER_GATE",
        "PIPELINE_SKIP_BOOKMAKER_PREPARE_ON_SEND",
    ):
        monkeypatch.delenv(env_name, raising=False)
    monkeypatch.setattr(runtime, "DOTA2PROTRACKER_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "PIPELINE_DISABLE_SIGNAL_GATES", False, raising=False)
    monkeypatch.setattr(runtime, "PIPELINE_SEND_EVERY_PARSED_MATCH", False, raising=False)
    monkeypatch.setattr(runtime, "PIPELINE_BYPASS_BOOKMAKER_GATE", False, raising=False)
    monkeypatch.setattr(runtime, "PIPELINE_BYPASS_TIER_GATE", False, raising=False)
    monkeypatch.setattr(runtime, "PIPELINE_BYPASS_LEAGUE_DENYLIST_GATE", False, raising=False)
    monkeypatch.setattr(runtime, "PIPELINE_BYPASS_PROTRACKER_GATE", False, raising=False)
    monkeypatch.setattr(runtime, "PIPELINE_SKIP_BOOKMAKER_PREPARE_ON_SEND", False, raising=False)

    runtime._apply_live_entrypoint_pipeline_defaults()

    assert runtime.DOTA2PROTRACKER_ENABLED is False
    assert runtime.PIPELINE_DISABLE_SIGNAL_GATES is False
    assert runtime.PIPELINE_SEND_EVERY_PARSED_MATCH is False
    assert runtime.PIPELINE_BYPASS_BOOKMAKER_GATE is False
    assert runtime.PIPELINE_BYPASS_TIER_GATE is False
    assert runtime.PIPELINE_BYPASS_LEAGUE_DENYLIST_GATE is False
    assert runtime.PIPELINE_BYPASS_PROTRACKER_GATE is False
    assert runtime.PIPELINE_SKIP_BOOKMAKER_PREPARE_ON_SEND is False


def test_recommend_odds_for_block_uses_all_output_thresholds(monkeypatch) -> None:
    monkeypatch.setattr(
        runtime,
        "STAR_THRESHOLDS_BY_WR",
        {
            60: {"all_output": [["counterpick_1vs2", 3]]},
            65: {"all_output": [["counterpick_1vs2", 4]]},
            70: {"all_output": [["counterpick_1vs2", 5]]},
            75: {"all_output": [["counterpick_1vs2", 6]]},
            80: {"all_output": [["counterpick_1vs2", 7]]},
            85: {"all_output": [["counterpick_1vs2", 9]]},
        },
        raising=False,
    )
    monkeypatch.setattr(runtime, "STAR_ODDS_USE_CALIBRATION", False, raising=False)

    rec = runtime._recommend_odds_for_block({"counterpick_1vs2": "7*"}, "all")

    assert rec is not None
    assert rec["level"] == 80
    assert rec["wr_pct"] == pytest.approx(80.0)
    assert rec["min_odds"] == pytest.approx(1.25)


def test_recommend_odds_for_block_averages_star_metric_levels(monkeypatch) -> None:
    thresholds = {}
    for level, threshold in (
        (60, 4),
        (65, 5),
        (70, 6),
        (75, 7),
        (80, 8),
        (85, 9),
        (90, 10),
    ):
        thresholds[level] = {
            "early_output": [
                ["solo", threshold],
                ["counterpick_1vs1", threshold],
                ["counterpick_1vs2", threshold],
            ],
        }
    monkeypatch.setattr(runtime, "STAR_THRESHOLDS_BY_WR", thresholds, raising=False)
    monkeypatch.setattr(runtime, "STAR_ODDS_USE_CALIBRATION", False, raising=False)

    rec = runtime._recommend_odds_for_block(
        {
            "solo": "4*",
            "counterpick_1vs1": "5*",
            "counterpick_1vs2": "9*",
        },
        "early",
    )

    assert rec is not None
    assert rec["level"] == 70
    assert rec["wr_pct"] == pytest.approx(70.0)
    assert rec["min_odds"] == pytest.approx(1.43)


def test_recommend_odds_for_block_averages_single_table_dynamic_levels(monkeypatch) -> None:
    monkeypatch.setattr(
        runtime,
        "STAR_THRESHOLDS_BY_WR",
        {
            60: {
                "early_output": [
                    ["solo", 4],
                    ["counterpick_1vs1", 4],
                    ["counterpick_1vs2", 4],
                ],
            },
        },
        raising=False,
    )
    monkeypatch.setattr(runtime, "STAR_ODDS_USE_CALIBRATION", False, raising=False)

    rec = runtime._recommend_odds_for_block(
        {
            "solo": "4*",
            "counterpick_1vs1": "8*",
            "counterpick_1vs2": "10*",
        },
        "early",
    )

    assert rec is not None
    assert rec["level"] == 75
    assert rec["wr_pct"] == pytest.approx(75.0)
    assert rec["min_odds"] == pytest.approx(1.33)


def test_recommend_odds_for_block_ignores_non_star_metrics(monkeypatch) -> None:
    monkeypatch.setattr(
        runtime,
        "STAR_THRESHOLDS_BY_WR",
        {
            60: {
                "mid_output": [["synergy_trio", 3], ["counterpick_1vs1", 4]],
            },
        },
        raising=False,
    )

    assert runtime._recommend_odds_for_block({"synergy_trio": "99*"}, "late") is None
    rec = runtime._recommend_odds_for_block({"counterpick_1vs1": "4*"}, "late")
    assert rec is not None
    assert rec["level"] == 60


def test_compose_star_metric_blocks_for_message_uses_early_late_all_order() -> None:
    message = runtime._compose_star_metric_blocks_for_message(
        "Early 20-28:\nE\n",
        "Late: (28-60 min):\nL\n",
        "All:\nA\n",
    )

    assert message == "Early 20-28:\nE\nLate: (28-60 min):\nL\nAll:\nA\n"
    assert message.index("Early 20-28:") < message.index("Late: (28-60 min):") < message.index("All:")


def test_format_wr_estimate_line_includes_team_wr_and_min_odds() -> None:
    line = runtime._format_wr_estimate_line(
        "All",
        "Inner Circle",
        65.0,
        {"level": 65, "min_odds": 1.54, "wr_pct": 65.0},
    )

    assert line == "All: Inner Circle WR≈65.0% от кэфа 1.54"


class _FakeTextResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code


class _FakeJsonResponse:
    def __init__(self, payload: Dict[str, Any], status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = "{}"

    def json(self) -> Dict[str, Any]:
        return self._payload


def _build_heads_and_bodies():
    html = """
    <div class="head">
      <div class="event__info-info__time">live</div>
    </div>
    <div class="body">
      <div class="match__item-team__score">0</div>
      <div class="match__item-team__score">0</div>
      <a href="https://dltv.org/matches/test-integrity"></a>
    </div>
    """
    soup = BeautifulSoup(html, "lxml")
    head = soup.find("div", class_="head")
    body = soup.find("div", class_="body")
    assert head is not None and body is not None
    return [head], [body]


def _build_v2_live_cards():
    html = """
    <div class="match live" data-series-id="425633" data-match="8740039655">
      <div class="match__head">
        <div class="match__head-event"><span>ESL One Birmingham 2026</span></div>
      </div>
      <div class="match__body">
        <div class="match__body-details">
          <div class="match__body-details__team">
            <div class="team"><div class="team__title"><span>Virtus.pro</span></div></div>
          </div>
          <div class="match__body-details__score">
            <div class="score"><strong class="text-red">12</strong><small>(0)</small></div>
            <div class="duration">
              <div class="duration__time"><strong>draft...</strong></div>
            </div>
            <div class="score"><strong class="text-red">18</strong><small>(1)</small></div>
          </div>
          <div class="match__body-details__team">
            <div class="team"><div class="team__title"><span>Nigma Galaxy</span></div></div>
          </div>
        </div>
      </div>
    </div>
    """
    soup = BeautifulSoup(html, "lxml")
    card = soup.find("div", class_="match")
    assert card is not None
    return [card], [card]


def _build_cyberscore_card():
    html = """
    <a class="item matches-item online" href="/en/matches/172835/">
      LIVE MAP 3 BO3 1:1 +5.4k 1WIN Team 13 : 48 9 - 3 SA Rejects
    </a>
    """
    soup = BeautifulSoup(html, "lxml")
    card = soup.find("a", class_="matches-item")
    assert card is not None
    card["data-source"] = "cyberscore"
    card["data-cyberscore-href"] = "https://cyberscore.live/en/matches/172835/"
    card["data-cyberscore-match-id"] = "172835"
    return [card], [card]


def _build_cyberscore_item() -> Dict[str, Any]:
    picks = []
    radiant_heroes = [1, 2, 3, 4, 5]
    dire_heroes = [6, 7, 8, 9, 10]
    for role, hero_id in enumerate(radiant_heroes, start=1):
        picks.append(
            {
                "team": "radiant",
                "player": {"id": 1000 + role, "role": role, "game_name": f"r{role}"},
                "hero": {"id": 2600 + role, "id_steam": hero_id, "name": f"R{role}"},
            }
        )
    for role, hero_id in enumerate(dire_heroes, start=1):
        picks.append(
            {
                "team": "dire",
                "player": {"id": 2000 + role, "role": role, "game_name": f"d{role}"},
                "hero": {"id": 2700 + role, "id_steam": hero_id, "name": f"D{role}"},
            }
        )
    return {
        "id": 172835,
        "id_series": "series-1",
        "title": "1WIN Team vs South American Rejects",
        "status": "online",
        "best_of": 3,
        "game_time": 845,
        "game_map_number": 3,
        "score_team_radiant": 9,
        "score_team_dire": 3,
        "best_of_score": [1, 1],
        "team_radiant_id": 44348,
        "team_dire_id": 45011,
        "team_radiant": {"id": 44348, "name": "1WIN Team"},
        "team_dire": {"id": 45011, "name": "South American Rejects"},
        "tournament_id": 46035,
        "tournament": {"id": 46035, "title": "DreamLeague Division 2 Season 4"},
        "picks": picks,
        "networth": [
            {"team": "radiant", "time": 120, "value": 500},
            {"team": "dire", "time": 240, "value": 1200},
        ],
    }


def _valid_heroes(seed: int, positions: int = 5) -> Dict[str, Dict[str, int]]:
    pos_order = ["pos1", "pos2", "pos3", "pos4", "pos5"][:positions]
    return {
        pos: {"hero_id": seed + idx + 1, "account_id": seed + idx + 101}
        for idx, pos in enumerate(pos_order)
    }


def _write_sharded_stats(shard_dir: Path, stats: Dict[str, Any]) -> None:
    shard_dir.mkdir(parents=True, exist_ok=True)
    grouped: Dict[str, List[tuple[str, Any]]] = {}
    for key, value in stats.items():
        shard_id = runtime._stats_key_leading_hero_id(key)
        grouped.setdefault(shard_id, []).append((key, value))
    for shard_id, rows in grouped.items():
        payload = b"".join(orjson.dumps([key, value]) + b"\n" for key, value in rows)
        (shard_dir / f"{shard_id}.jsonl").write_bytes(payload)


def _write_complete_sharded_stats(source_path: Path, stats: Dict[str, Any]) -> Path:
    source_path.write_bytes(orjson.dumps(stats))
    shard_dir = source_path.parent / f"{source_path.stem}.shards"
    _write_sharded_stats(shard_dir, stats)
    expected_meta = runtime._stats_expected_meta(source_path)
    expected_meta["entries"] = len(stats)
    (shard_dir / "_meta.json").write_text(json.dumps(expected_meta), encoding="utf-8")
    (shard_dir / "_complete").write_text("ok\n", encoding="utf-8")
    return shard_dir


def test_sharded_stats_get_many_returns_only_requested_keys_without_full_cache(tmp_path) -> None:
    shard_dir = tmp_path / "stats.shards"
    stats = {
        "1pos1": {"wins": 12, "games": 20},
        "1pos1_with_2pos2": {"wins": 11, "games": 20},
        "1pos1_vs_6pos1": {"wins": 14, "games": 20},
        "1pos1_with_999pos1": {"wins": 1, "games": 20},
        "6pos1_vs_1pos1": {"wins": 6, "games": 20},
    }
    _write_sharded_stats(shard_dir, stats)
    lookup = runtime._ShardedStatsLookup(shard_dir, label="unit", max_cached_shards=0, max_cached_keys=2)

    result = lookup.get_many({"1pos1", "1pos1_vs_6pos1", "6pos1_vs_1pos1", "missing"})

    assert result == {
        "1pos1": stats["1pos1"],
        "1pos1_vs_6pos1": stats["1pos1_vs_6pos1"],
        "6pos1_vs_1pos1": stats["6pos1_vs_1pos1"],
    }
    assert lookup._shards == {}
    assert len(lookup._key_cache) == 2
    assert set(lookup._key_cache) <= set(result)
    assert lookup.get_many({"1pos1_vs_6pos1"}) == {"1pos1_vs_6pos1": stats["1pos1_vs_6pos1"]}
    assert lookup.get("1pos1_with_2pos2") == stats["1pos1_with_2pos2"]
    assert len(lookup._key_cache) == 2
    assert "1pos1_with_2pos2" in lookup._key_cache


def test_sqlite_stats_lookup_builds_from_complete_shards_and_batches_keys(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(runtime, "STATS_SQLITE_BUILD_FROM_SHARDS", True, raising=False)
    monkeypatch.setattr(runtime, "STATS_SQLITE_BUILD_BATCH_SIZE", 2, raising=False)
    monkeypatch.setattr(runtime, "STATS_SQLITE_BUILD_PROGRESS_EVERY", 0, raising=False)
    monkeypatch.setattr(runtime, "STATS_SQLITE_QUERY_CHUNK_SIZE", 2, raising=False)
    monkeypatch.setattr(runtime, "STATS_SHARD_KEY_CACHE_MAX", 2, raising=False)
    source_path = tmp_path / "early_dict_raw.json"
    stats = {
        "1pos1": {"wins": 12, "games": 20},
        "1pos1_with_2pos2": {"wins": 11, "games": 20},
        "1pos1_vs_6pos1": {"wins": 14, "games": 20},
        "6pos1_vs_1pos1": {"wins": 6, "games": 20},
    }
    _write_complete_sharded_stats(source_path, stats)

    lookup = runtime._prepare_sqlite_stats_lookup(str(source_path), "early")

    assert isinstance(lookup, runtime._SqliteStatsLookup)
    assert runtime._stats_sqlite_db_path(source_path).exists()
    assert lookup.get_many(["1pos1", "1pos1_vs_6pos1", "missing"]) == {
        "1pos1": stats["1pos1"],
        "1pos1_vs_6pos1": stats["1pos1_vs_6pos1"],
    }
    assert lookup.get("1pos1_with_2pos2") == stats["1pos1_with_2pos2"]
    assert lookup.get("missing", {"fallback": True}) == {"fallback": True}
    assert len(lookup._key_cache) <= 2
    assert runtime._sqlite_stats_meta_matches(
        runtime._stats_sqlite_db_path(source_path),
        runtime._stats_expected_meta(source_path),
    )


def test_indexed_stats_lookup_auto_uses_existing_sqlite_without_autobuild(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(runtime, "STATS_LOOKUP_BACKEND", "auto", raising=False)
    monkeypatch.setattr(runtime, "STATS_SQLITE_AUTOBUILD", False, raising=False)
    monkeypatch.setattr(runtime, "STATS_SQLITE_BUILD_FROM_SHARDS", True, raising=False)
    monkeypatch.setattr(runtime, "STATS_SQLITE_BUILD_PROGRESS_EVERY", 0, raising=False)
    monkeypatch.setattr(runtime, "STATS_SHARD_KEY_CACHE_MAX", 5, raising=False)
    source_path = tmp_path / "late_dict_raw.json"
    stats = {"1pos1": {"wins": 12, "games": 20}}
    _write_complete_sharded_stats(source_path, stats)
    runtime._prepare_sqlite_stats_lookup(str(source_path), "late").close()

    lookup = runtime._prepare_indexed_stats_lookup(str(source_path), "late")

    assert isinstance(lookup, runtime._SqliteStatsLookup)
    assert lookup.get("1pos1") == stats["1pos1"]


def test_sqlite_stats_meta_accepts_same_sized_source_with_different_mtime(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(runtime, "STATS_SQLITE_BUILD_FROM_SHARDS", True, raising=False)
    monkeypatch.setattr(runtime, "STATS_SQLITE_BUILD_PROGRESS_EVERY", 0, raising=False)
    source_path = tmp_path / "post_lane_dict_raw.json"
    _write_complete_sharded_stats(source_path, {"1pos1": {"wins": 12, "games": 20}})
    lookup = runtime._prepare_sqlite_stats_lookup(str(source_path), "post_lane")
    lookup.close()

    expected = runtime._stats_expected_meta(source_path)
    expected["source_mtime_ns"] += 1
    assert runtime._sqlite_stats_meta_matches(runtime._stats_sqlite_db_path(source_path), expected)

    expected["source_size"] += 1
    assert not runtime._sqlite_stats_meta_matches(runtime._stats_sqlite_db_path(source_path), expected)


def test_explicit_stats_lookup_backend_enables_indexed_lookup_without_sharded_gate(monkeypatch) -> None:
    monkeypatch.delenv("STATS_EARLY_LOOKUP_BACKEND", raising=False)
    monkeypatch.delenv("STATS_EARLY_SHARDED_LOOKUP_MODE", raising=False)
    monkeypatch.setattr(runtime, "STATS_SHARDED_LOOKUP_MODE", "never", raising=False)

    monkeypatch.setattr(runtime, "STATS_LOOKUP_BACKEND", "auto", raising=False)
    assert runtime._stats_indexed_lookup_enabled("early") is False

    monkeypatch.setattr(runtime, "STATS_LOOKUP_BACKEND", "sqlite", raising=False)
    assert runtime._stats_indexed_lookup_enabled("early") is True

    monkeypatch.setenv("STATS_EARLY_LOOKUP_BACKEND", "jsonl")
    assert runtime._stats_indexed_lookup_enabled("early") is True


def test_draft_stats_lookup_keys_cover_synergy_accesses() -> None:
    radiant = _valid_heroes(0)
    dire = _valid_heroes(5)
    observed_keys = set()

    class _TracingStats(dict):
        def __bool__(self) -> bool:
            return True

        def get(self, key: Any, default=None):
            observed_keys.add(str(key))
            return super().get(key, default)

    tracing_stats = _TracingStats()

    runtime.synergy_and_counterpick(
        radiant,
        dire,
        tracing_stats,
        tracing_stats,
        post_lane_dict=tracing_stats,
    )

    assert observed_keys
    assert observed_keys <= runtime._draft_stats_lookup_keys(radiant, dire)


def test_draft_scoped_stats_lookup_preserves_synergy_results(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(runtime, "STATS_DRAFT_SCOPED_LOOKUP_ENABLED", True, raising=False)
    radiant = _valid_heroes(0)
    dire = _valid_heroes(5)
    required_keys = runtime._draft_stats_lookup_keys(radiant, dire)
    stats = {key: {"wins": 14, "games": 20} for key in required_keys}
    stats["1pos1_with_999pos1"] = {"wins": 1, "games": 20}
    shard_dir = tmp_path / "stats.shards"
    _write_sharded_stats(shard_dir, stats)
    lookup = runtime._ShardedStatsLookup(shard_dir, label="unit", max_cached_shards=0, max_cached_keys=50)

    scoped = runtime._prepare_draft_scoped_stats_lookup(lookup, radiant, dire)

    assert isinstance(scoped, dict)
    assert scoped
    assert "1pos1_with_999pos1" not in scoped
    assert lookup._shards == {}
    assert len(lookup._key_cache) <= 50
    full_stats = {key: stats[key] for key in required_keys}
    full_result = runtime.synergy_and_counterpick(
        radiant,
        dire,
        full_stats,
        full_stats,
        post_lane_dict=full_stats,
    )
    scoped_result = runtime.synergy_and_counterpick(
        radiant,
        dire,
        scoped,
        scoped,
        post_lane_dict=scoped,
    )
    assert full_result == scoped_result


def test_draft_scoped_stats_lookup_supports_sqlite_backend(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(runtime, "STATS_DRAFT_SCOPED_LOOKUP_ENABLED", True, raising=False)
    monkeypatch.setattr(runtime, "STATS_SQLITE_BUILD_FROM_SHARDS", True, raising=False)
    monkeypatch.setattr(runtime, "STATS_SQLITE_BUILD_PROGRESS_EVERY", 0, raising=False)
    monkeypatch.setattr(runtime, "STATS_SQLITE_QUERY_CHUNK_SIZE", 3, raising=False)
    monkeypatch.setattr(runtime, "STATS_SHARD_KEY_CACHE_MAX", 100, raising=False)
    radiant = _valid_heroes(0)
    dire = _valid_heroes(5)
    required_keys = runtime._draft_stats_lookup_keys(radiant, dire)
    stats = {key: {"wins": 14, "games": 20} for key in required_keys}
    stats["1pos1_with_999pos1"] = {"wins": 1, "games": 20}
    source_path = tmp_path / "late_dict_raw.json"
    _write_complete_sharded_stats(source_path, stats)
    lookup = runtime._prepare_sqlite_stats_lookup(str(source_path), "late")

    scoped = runtime._prepare_draft_scoped_stats_lookup(lookup, radiant, dire)

    assert isinstance(scoped, dict)
    assert scoped
    assert "1pos1_with_999pos1" not in scoped
    full_stats = {key: stats[key] for key in required_keys}
    assert runtime.synergy_and_counterpick(
        radiant,
        dire,
        full_stats,
        full_stats,
        post_lane_dict=full_stats,
    ) == runtime.synergy_and_counterpick(
        radiant,
        dire,
        scoped,
        scoped,
        post_lane_dict=scoped,
    )


def test_extract_live_listing_context_supports_cyberscore_cards() -> None:
    heads, bodies = _build_cyberscore_card()
    context = runtime._extract_live_listing_context(heads[0], bodies[0])

    assert context["source"] == "cyberscore"
    assert context["layout"] == "cyberscore_match_card"
    assert context["status"] == "live"
    assert context["score"] == "1 : 1"
    assert context["uniq_score"] == 2
    assert context["live_match_id"] == "172835"
    assert context["href"] == "https://cyberscore.live/en/matches/172835/"


def test_cyberscore_next_payload_extracts_draft_time_and_networth() -> None:
    item = _build_cyberscore_item()
    flight_chunk = f'prefix "item":{json.dumps(item, separators=(",", ":"))}, suffix'
    html = f"<script>self.__next_f.push([1,{json.dumps(flight_chunk)}])</script>"

    parsed_item = runtime._extract_cyberscore_match_item_from_html(html, match_id=172835)
    assert parsed_item is not None
    payload = runtime._cyberscore_item_to_runtime_payload(parsed_item)

    assert payload["game_time"] == 845
    assert payload["radiant_lead"] == -1200
    assert payload["radiant_score"] == 9
    assert payload["dire_score"] == 3
    assert payload["db"]["first_team"]["title"] == "1WIN Team"
    assert payload["live_league_data"]["radiant_series_wins"] == 1
    assert payload["_cyberscore_draft_error"] is None
    assert payload["_cyberscore_heroes_and_pos"]["radiant"]["pos1"]["hero_id"] == 1
    assert payload["_cyberscore_heroes_and_pos"]["radiant"]["pos5"]["hero_id"] == 5
    assert payload["_cyberscore_heroes_and_pos"]["dire"]["pos1"]["hero_id"] == 6
    assert payload["_cyberscore_heroes_and_pos"]["dire"]["pos5"]["hero_id"] == 10


def test_delayed_match_state_reads_cyberscore_html_instead_of_json(monkeypatch) -> None:
    item = _build_cyberscore_item()
    item["game_time"] = 1234
    item["networth"] = [{"team": "radiant", "time": 1234, "value": 2100}]
    flight_chunk = f'prefix "item":{json.dumps(item, separators=(",", ":"))}, suffix'
    html = f"<script>self.__next_f.push([1,{json.dumps(flight_chunk)}])</script>"
    monkeypatch.setattr(runtime, "CYBERSCORE_LIVE_WATCHER_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "_get_cyberscore_delayed_html_via_camoufox", lambda _url: html)
    monkeypatch.setattr(runtime, "CYBERSCORE_LISTING_ITEM_CACHE", {}, raising=False)

    state = runtime._fetch_delayed_match_state("https://cyberscore.live/en/matches/172835/")

    assert state == {"game_time": 1234.0, "radiant_lead": 2100.0}


def test_delayed_match_state_prefers_newer_cyberscore_listing_cache(monkeypatch) -> None:
    detail_item = _build_cyberscore_item()
    detail_item["game_time"] = 1000
    detail_item["networth"] = [{"team": "radiant", "time": 1000, "value": 500}]
    listing_item = _build_cyberscore_item()
    listing_item["game_time"] = 1300
    listing_item["networth"] = [{"team": "dire", "time": 1300, "value": 2200}]
    flight_chunk = f'prefix "item":{json.dumps(detail_item, separators=(",", ":"))}, suffix'
    html = f"<script>self.__next_f.push([1,{json.dumps(flight_chunk)}])</script>"
    monkeypatch.setattr(runtime, "CYBERSCORE_LIVE_WATCHER_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "_get_cyberscore_delayed_html_via_camoufox", lambda _url: html)
    monkeypatch.setattr(runtime, "CYBERSCORE_LISTING_ITEM_CACHE", {"172835": listing_item}, raising=False)

    state = runtime._fetch_delayed_match_state("https://cyberscore.live/en/matches/172835/")

    assert state == {"game_time": 1300.0, "radiant_lead": -2200.0}


def test_cyberscore_delayed_fetch_bypasses_long_page_cache(monkeypatch) -> None:
    one_shot_urls: List[str] = []
    closed_pages: List[tuple[str, str]] = []

    monkeypatch.setattr(runtime, "CAMOUFOX_AVAILABLE", True, raising=False)
    monkeypatch.setattr(runtime, "CYBERSCORE_CAMOUFOX_FETCH_ATTEMPTS", 1, raising=False)
    monkeypatch.setattr(
        runtime,
        "_get_cyberscore_html_via_one_shot",
        lambda url: one_shot_urls.append(url) or "<html>fresh</html>",
    )
    monkeypatch.setattr(
        runtime,
        "_cyberscore_close_cached_long_page",
        lambda key, reason="": closed_pages.append((key, reason)),
    )

    html = runtime._get_cyberscore_delayed_html_via_camoufox(
        "https://cyberscore.live/en/matches/173102/"
    )

    assert html == "<html>fresh</html>"
    assert one_shot_urls
    assert one_shot_urls[0].startswith("https://cyberscore.live/en/matches/173102/?")
    assert "_delayed_ts=" in one_shot_urls[0]
    assert closed_pages == [("match:173102", "delayed fresh fetch")]


def test_delayed_match_state_prefers_live_watcher_snapshot(monkeypatch) -> None:
    fallback_calls: List[str] = []

    monkeypatch.setattr(runtime, "CYBERSCORE_LIVE_WATCHER_ENABLED", True, raising=False)
    monkeypatch.setattr(runtime, "CAMOUFOX_AVAILABLE", True, raising=False)
    monkeypatch.setattr(
        runtime,
        "_fetch_cyberscore_live_watcher_state",
        lambda _url: {"game_time": 1680.0, "radiant_lead": -3200.0},
    )
    monkeypatch.setattr(
        runtime,
        "_get_cyberscore_delayed_html_via_camoufox",
        lambda url: fallback_calls.append(url) or None,
    )

    state = runtime._fetch_delayed_match_state("https://cyberscore.live/en/matches/173102/")

    assert state == {"game_time": 1680.0, "radiant_lead": -3200.0}
    assert fallback_calls == []


def test_cyberscore_live_watcher_keeps_latest_progress() -> None:
    entry: Dict[str, Any] = {}
    stale_item = _build_cyberscore_item()
    stale_item["game_time"] = 1284
    stale_item["networth"] = [{"team": "dire", "time": 1284, "value": 4092}]
    fresh_item = _build_cyberscore_item()
    fresh_item["game_time"] = 1680
    fresh_item["networth"] = [{"team": "dire", "time": 1680, "value": 3200}]

    assert runtime._cyberscore_live_watcher_update_item(entry, stale_item, source="response") is True
    assert entry["latest_state"]["game_time"] == pytest.approx(1284.0)
    assert runtime._cyberscore_live_watcher_update_item(entry, fresh_item, source="response") is True
    assert entry["latest_state"]["game_time"] == pytest.approx(1680.0)
    assert entry["latest_state"]["radiant_lead"] == pytest.approx(-3200.0)
    assert runtime._cyberscore_live_watcher_update_item(entry, stale_item, source="response") is False
    assert entry["latest_state"]["game_time"] == pytest.approx(1680.0)


def test_cyberscore_empty_live_draft_forces_fresh_fetch(monkeypatch) -> None:
    calls: List[str] = []
    fresh_item = {"id": 173627, "game_time": 576}
    fresh_data = {
        "game_time": 576,
        "fast_picks": {
            "first_team": [{"hero_id": 73}],
            "second_team": [{"hero_id": 63}],
        },
    }

    monkeypatch.setattr(
        runtime,
        "_get_cyberscore_delayed_html_via_camoufox",
        lambda url: calls.append(url) or "fresh-html",
    )
    monkeypatch.setattr(
        runtime,
        "_extract_cyberscore_match_item_from_html",
        lambda text, match_id=None: fresh_item
        if text == "fresh-html" and str(match_id) == "173627"
        else None,
    )
    monkeypatch.setattr(
        runtime,
        "_cyberscore_item_to_runtime_payload",
        lambda item: fresh_data if item is fresh_item else {},
    )

    data, item, refreshed = runtime._maybe_refresh_cyberscore_empty_live_draft_payload(
        match_url="https://cyberscore.live/en/matches/173627/",
        match_id="173627",
        data={"game_time": 457, "fast_picks": {"first_team": [], "second_team": []}},
    )

    assert refreshed is True
    assert item is fresh_item
    assert data is fresh_data
    assert calls == ["https://cyberscore.live/en/matches/173627/"]


def test_cyberscore_empty_draft_does_not_refetch_before_game_time(monkeypatch) -> None:
    calls: List[str] = []
    monkeypatch.setattr(
        runtime,
        "_get_cyberscore_delayed_html_via_camoufox",
        lambda url: calls.append(url) or "fresh-html",
    )

    data, item, refreshed = runtime._maybe_refresh_cyberscore_empty_live_draft_payload(
        match_url="https://cyberscore.live/en/matches/173627/",
        match_id="173627",
        data={"game_time": 0, "fast_picks": {"first_team": [], "second_team": []}},
    )

    assert refreshed is False
    assert item is None
    assert data["game_time"] == 0
    assert calls == []


def test_cyberscore_empty_online_draft_forces_fresh_fetch_even_at_zero_time(monkeypatch) -> None:
    calls: List[str] = []
    fresh_item = {"id": 173630, "status": "online", "game_time": 641}
    fresh_data = {
        "game_time": 641,
        "fast_picks": {
            "first_team": [{"hero_id": 131}],
            "second_team": [{"hero_id": 145}],
        },
    }

    monkeypatch.setattr(
        runtime,
        "_get_cyberscore_delayed_html_via_camoufox",
        lambda url: calls.append(url) or "fresh-html",
    )
    monkeypatch.setattr(
        runtime,
        "_extract_cyberscore_match_item_from_html",
        lambda text, match_id=None: fresh_item
        if text == "fresh-html" and str(match_id) == "173630"
        else None,
    )
    monkeypatch.setattr(
        runtime,
        "_cyberscore_item_to_runtime_payload",
        lambda item: fresh_data if item is fresh_item else {},
    )

    data, item, refreshed = runtime._maybe_refresh_cyberscore_empty_live_draft_payload(
        match_url="https://cyberscore.live/en/matches/173630/",
        match_id="173630",
        data={"game_time": 0, "fast_picks": {"first_team": [], "second_team": []}},
        cyber_item={"id": 173630, "status": "online", "game_time": 0},
    )

    assert refreshed is True
    assert item is fresh_item
    assert data is fresh_data
    assert calls == ["https://cyberscore.live/en/matches/173630/"]


def test_late_pub_table_releases_acceptable_deficit_and_target_lead(monkeypatch) -> None:
    monkeypatch.setattr(
        runtime,
        "late_pub_comeback_table_thresholds_by_wr",
        {60: {20: -3569.63, 28: -6257.58, 38: -8788.17}},
        raising=False,
    )

    ready_deficit = runtime._late_star_pub_table_decision(
        wr_level=60,
        game_time_seconds=28 * 60,
        target_networth_diff=-3200.0,
    )
    too_deep = runtime._late_star_pub_table_decision(
        wr_level=60,
        game_time_seconds=28 * 60,
        target_networth_diff=-7000.0,
    )
    target_lead_at_start = runtime._late_star_pub_table_decision(
        wr_level=60,
        game_time_seconds=float(runtime.LATE_PUB_COMEBACK_TABLE_START_SECONDS),
        target_networth_diff=1200.0,
    )
    target_lead_late = runtime._late_star_pub_table_decision(
        wr_level=60,
        game_time_seconds=(38 * 60) + 24,
        target_networth_diff=9064.0,
    )

    assert ready_deficit["ready"] is True
    assert ready_deficit["source_minute"] == 28
    assert too_deep["ready"] is False
    assert target_lead_at_start["ready"] is True
    assert target_lead_at_start["source_minute"] == 20
    assert target_lead_late["ready"] is True


def test_delayed_cyberscore_stale_state_requests_browser_reset(tmp_path, monkeypatch) -> None:
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    reset_calls: List[str] = []

    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)
    monkeypatch.setattr(runtime, "CYBERSCORE_DELAYED_STALE_REFRESH_SECONDS", 30, raising=False)
    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_000.0)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(
        runtime,
        "_fetch_delayed_match_state",
        lambda _json_url: {"game_time": 1171.0, "radiant_lead": -368.0},
    )
    monkeypatch.setattr(runtime._shared_camoufox_session, "request_reset", lambda: reset_calls.append("reset"))
    monkeypatch.setattr(runtime, "CYBERSCORE_LISTING_ITEM_CACHE", {"173593": {"game_time": 1171}}, raising=False)

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
    runtime._set_delayed_match(
        "cyberscore.live/en/matches/173593.map2",
        {
            "message": "payload",
            "reason": "late_only_opposite_signs",
            "json_url": "https://cyberscore.live/en/matches/173593/",
            "target_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "queued_at": 1_699_999_000.0,
            "queued_game_time": 1171.0,
            "last_game_time": 1171.0,
            "last_progress_at": 1_699_999_900.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {"target_side": "dire"},
            "fallback_send_status_label": runtime.NETWORTH_STATUS_LATE_FALLBACK_20_20_SEND,
            "send_on_target_game_time": True,
            "allow_live_recheck": False,
            "networth_target_side": "dire",
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
        },
    )

    runtime._drain_due_delayed_signals_once()

    assert reset_calls == ["reset"]
    assert "173593" not in runtime.CYBERSCORE_LISTING_ITEM_CACHE
    with runtime.monitored_matches_lock:
        payload = dict(runtime.monitored_matches["cyberscore.live/en/matches/173593.map2"])
    assert payload["last_game_time"] == pytest.approx(1171.0)
    assert payload["target_game_time"] == pytest.approx(float(runtime.LATE_PUB_COMEBACK_TABLE_START_SECONDS))
    assert payload["last_cyberscore_stale_refresh_at"] == pytest.approx(1_700_000_000.0)
    assert payload["cyberscore_stale_refresh_count"] == 1
    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()


def test_delayed_pub_table_wait_refreshes_stale_cyberscore_after_target(tmp_path, monkeypatch) -> None:
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    reset_calls: List[str] = []

    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)
    monkeypatch.setattr(runtime, "CYBERSCORE_DELAYED_STALE_REFRESH_SECONDS", 30, raising=False)
    monkeypatch.setattr(runtime, "late_pub_comeback_table_thresholds_by_wr", {60: {21: -3809.48}}, raising=False)
    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_000.0)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(
        runtime,
        "_fetch_delayed_match_state",
        lambda _json_url: {"game_time": 1284.0, "radiant_lead": -4092.0},
    )
    monkeypatch.setattr(runtime._shared_camoufox_session, "request_reset", lambda: reset_calls.append("reset"))
    monkeypatch.setattr(runtime, "CYBERSCORE_LISTING_ITEM_CACHE", {"173102": {"game_time": 1284}}, raising=False)

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
    runtime._set_delayed_match(
        "cyberscore.live/en/matches/173102.map1",
        {
            "message": "payload",
            "reason": "late_star_pub_comeback_table_monitor",
            "json_url": "https://cyberscore.live/en/matches/173102/",
            "target_game_time": float(runtime.LATE_PUB_COMEBACK_TABLE_START_SECONDS),
            "queued_at": 1_699_999_000.0,
            "queued_game_time": 19.0,
            "last_game_time": 1284.0,
            "last_progress_at": 1_699_999_900.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {"target_side": "radiant"},
            "fallback_send_status_label": runtime.NETWORTH_STATUS_LATE_PUB_TABLE_WAIT,
            "send_on_target_game_time": False,
            "allow_live_recheck": False,
            "late_pub_comeback_table_active": True,
            "late_pub_comeback_table_wr_level": 60,
            "networth_target_side": "radiant",
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
        },
    )

    runtime._drain_due_delayed_signals_once()

    assert reset_calls == ["reset"]
    assert "173102" not in runtime.CYBERSCORE_LISTING_ITEM_CACHE
    with runtime.monitored_matches_lock:
        payload = dict(runtime.monitored_matches["cyberscore.live/en/matches/173102.map1"])
    assert payload["dispatch_status_label"] == runtime.NETWORTH_STATUS_LATE_PUB_TABLE_WAIT
    assert payload["last_cyberscore_stale_refresh_at"] == pytest.approx(1_700_000_000.0)
    assert payload["cyberscore_stale_refresh_count"] == 1
    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()


def test_check_head_skips_existing_cyberscore_delayed_payload_after_map_key(monkeypatch, capsys) -> None:
    heads, bodies = _build_cyberscore_card()
    item = _build_cyberscore_item()
    item["game_time"] = 1600
    item["game_map_number"] = 3
    item["networth"] = [{"team": "dire", "time": 1600, "value": 1200}]
    flight_chunk = f'prefix "item":{json.dumps(item, separators=(",", ":"))}, suffix'
    html = f"<script>self.__next_f.push([1,{json.dumps(flight_chunk)}])</script>"

    monkeypatch.setattr(runtime, "_get_cyberscore_html_via_camoufox", lambda _url: html)
    monkeypatch.setattr(runtime, "_dispatch_block_reason", lambda _url: None)
    monkeypatch.setattr(runtime, "SIGNAL_MINIMAL_ODDS_ONLY_MODE", False, raising=False)

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
        runtime.monitored_matches["cyberscore.live/en/matches/172835.map3"] = {
            "message": "payload",
            "reason": "late_star_pub_comeback_table_monitor",
            "json_url": "https://cyberscore.live/en/matches/172835/",
            "target_game_time": float(runtime.LATE_PUB_COMEBACK_TABLE_START_SECONDS),
            "queued_game_time": 845.0,
            "last_game_time": 1230.0,
            "dispatch_status_label": runtime.NETWORTH_STATUS_LATE_PUB_TABLE_WAIT,
            "allow_live_recheck": False,
        }

    runtime.check_head(heads, bodies, 0, set())

    output = capsys.readouterr().out
    assert "пропускаем повторный расчет" in output
    with runtime.monitored_matches_lock:
        payload = dict(runtime.monitored_matches["cyberscore.live/en/matches/172835.map3"])
    assert payload["reason"] == "late_star_pub_comeback_table_monitor"
    assert payload["last_game_time"] == pytest.approx(1230.0)
    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()


def test_cyberscore_listing_cache_survives_empty_transition_html(monkeypatch) -> None:
    item = _build_cyberscore_item()
    monkeypatch.setattr(runtime, "CYBERSCORE_LISTING_ITEM_CACHE", {"172835": item}, raising=False)

    heads, bodies = runtime._extract_cyberscore_live_cards_from_html("<main>No live matches</main>")

    assert heads == []
    assert bodies == []
    assert runtime.CYBERSCORE_LISTING_ITEM_CACHE["172835"] is item


def test_add_url_creates_json_array_and_deduplicates(tmp_path, monkeypatch) -> None:
    target_path = tmp_path / "map_id_check.json"
    monkeypatch.setattr(runtime, "MAP_ID_CHECK_PATH", str(target_path), raising=False)
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)

    with runtime.processed_urls_lock:
        runtime.processed_urls_cache.clear()

    runtime.add_url("dltv.org/matches/test-integrity.0", reason="unit_test")
    runtime.add_url("dltv.org/matches/test-integrity.0", reason="unit_test_repeat")

    assert target_path.exists()
    assert orjson.loads(target_path.read_bytes()) == ["dltv.org/matches/test-integrity.0"]


def test_add_url_recovers_corrupt_map_id_check_and_persists_url(tmp_path, monkeypatch) -> None:
    target_path = tmp_path / "map_id_check.json"
    target_path.write_text("{broken", encoding="utf-8")
    monkeypatch.setattr(runtime, "MAP_ID_CHECK_PATH", str(target_path), raising=False)
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)

    with runtime.processed_urls_lock:
        runtime.processed_urls_cache.clear()

    runtime.add_url("dltv.org/matches/test-recover.0", reason="unit_test_recover")

    assert orjson.loads(target_path.read_bytes()) == ["dltv.org/matches/test-recover.0"]
    assert list(tmp_path.glob("map_id_check.json.corrupt.*"))


def test_verbose_match_log_cache_is_bounded(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "VERBOSE_MATCH_LOG_CACHE_MAX_SIZE", 2, raising=False)

    with runtime.verbose_match_log_lock:
        runtime.verbose_match_log_cache.clear()

    runtime._mark_verbose_match_log_done("match-a")
    runtime._mark_verbose_match_log_done("match-b")
    runtime._mark_verbose_match_log_done("match-c")

    with runtime.verbose_match_log_lock:
        keys = list(runtime.verbose_match_log_cache.keys())

    assert keys == ["match-b", "match-c"]


def test_build_series_score_line_with_fallback_uses_live_score_when_series_missing() -> None:
    assert runtime._build_series_score_line_with_fallback({}, "0 : 0") == "0-0\n"


def test_build_minimal_odds_only_message_contains_only_teams_and_score() -> None:
    message = runtime._build_minimal_odds_only_message(
        radiant_team_name="Team Lynx",
        dire_team_name="Nemiga Gaming",
        live_league={},
        fallback_score_text="0 : 0",
    )
    assert message == "Team Lynx VS Nemiga Gaming\n0-0\n"


def test_prepare_minimal_odds_only_message_requires_at_least_one_numeric_bookmaker(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", True, raising=False)
    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_USE_SUBPROCESS", False, raising=False)
    monkeypatch.setattr(runtime, "_bookmaker_refresh_cached_match_tabs_for_dispatch", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        runtime,
        "_bookmaker_format_odds_block",
        lambda *_args, **_kwargs: ("", False, "no_numeric_odds"),
    )

    message, ready, reason = runtime._prepare_minimal_odds_only_message_for_delivery(
        "dltv.org/matches/test.0",
        "Team Lynx VS Nemiga Gaming\n0-0\n",
    )

    assert ready is False
    assert reason == "no_numeric_odds"
    assert message == "Team Lynx VS Nemiga Gaming\n0-0\n"


def test_build_runtime_memory_snapshot_reports_cache_sizes(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "_get_current_rss_mb", lambda: 321.5)

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
        runtime.monitored_matches["dltv.org/matches/test-memory.0"] = {"queued": True}
    with runtime.processed_urls_lock:
        runtime.processed_urls_cache.clear()
        runtime.processed_urls_cache.update({"a", "b"})
    with runtime.verbose_match_log_lock:
        runtime.verbose_match_log_cache.clear()
        runtime.verbose_match_log_cache["verbose-a"] = None
    with runtime.uncertain_delivery_urls_lock:
        runtime.uncertain_delivery_urls_cache.clear()
        runtime.uncertain_delivery_urls_cache.add("uncertain-a")
    with runtime.signal_send_guard_lock:
        runtime.signal_send_guard.clear()
        runtime.signal_send_guard.add("guard-a")

    snapshot = runtime._build_runtime_memory_snapshot()

    assert snapshot["rss_mb"] == 321.5
    assert snapshot["monitored_matches"] == 1
    assert snapshot["processed_urls_cache"] == 2
    assert snapshot["verbose_match_log_cache"] == 1
    assert snapshot["uncertain_delivery_urls_cache"] == 1
    assert snapshot["signal_send_guard"] == 1
    assert isinstance(snapshot["gc_count"], str)

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
    with runtime.processed_urls_lock:
        runtime.processed_urls_cache.clear()
    with runtime.verbose_match_log_lock:
        runtime.verbose_match_log_cache.clear()
    with runtime.uncertain_delivery_urls_lock:
        runtime.uncertain_delivery_urls_cache.clear()
    with runtime.signal_send_guard_lock:
        runtime.signal_send_guard.clear()


def test_build_runtime_object_snapshot_reports_large_runtime_objects(monkeypatch) -> None:
    with runtime.bookmaker_prefetch_lock:
        runtime.bookmaker_prefetch_queue.clear()
        runtime.bookmaker_prefetch_queue.append({"match": "queued"})
        runtime.bookmaker_prefetch_results.clear()
        runtime.bookmaker_prefetch_results["match-a"] = {"status": "done"}
    runtime.match_history.clear()
    runtime.match_history["match-a"] = {"times": [0], "leads": [100]}

    monkeypatch.setattr(runtime, "lane_data", {"lane": 1})
    monkeypatch.setattr(runtime, "early_dict", {"early": 1})
    monkeypatch.setattr(runtime, "late_dict", {"late": 1})
    monkeypatch.setattr(runtime, "post_lane_dict", {"post_lane": 1})
    monkeypatch.setattr(runtime, "late_pub_comeback_table_thresholds_by_wr", {"21": 123})
    monkeypatch.setattr(runtime, "KILLS_MODELS", {"model": 1})
    monkeypatch.setattr(runtime, "TEAM_PREDICTABILITY_CACHE", {"team": 1})
    monkeypatch.setattr(runtime, "tempo_solo_dict", {"solo": 1})
    monkeypatch.setattr(runtime, "tempo_duo_dict", {"duo": 1})
    monkeypatch.setattr(runtime, "tempo_cp1v1_dict", {"cp": 1})

    snapshot = runtime._build_runtime_object_snapshot()

    assert snapshot["lane_data"] == "dict(len=1)"
    assert snapshot["early_dict"] == "dict(len=1)"
    assert snapshot["late_dict"] == "dict(len=1)"
    assert snapshot["post_lane_dict"] == "dict(len=1)"
    assert snapshot["late_pub_comeback_table_thresholds"] == "dict(len=1)"
    assert snapshot["match_history"] == "dict(len=1)"
    assert snapshot["bookmaker_prefetch_queue"] == "deque(len=1)"
    assert snapshot["bookmaker_prefetch_results"] == "dict(len=1)"
    assert snapshot["kills_models_loaded"] == "yes"
    assert snapshot["team_predictability_cache"] == "dict(len=1)"
    assert snapshot["tempo_solo_dict"] == "dict(len=1)"
    assert snapshot["tempo_duo_dict"] == "dict(len=1)"
    assert snapshot["tempo_cp1v1_dict"] == "dict(len=1)"

    with runtime.bookmaker_prefetch_lock:
        runtime.bookmaker_prefetch_queue.clear()
        runtime.bookmaker_prefetch_results.clear()
    runtime.match_history.clear()
    monkeypatch.setattr(runtime, "lane_data", None)
    monkeypatch.setattr(runtime, "early_dict", None)
    monkeypatch.setattr(runtime, "late_dict", None)
    monkeypatch.setattr(runtime, "late_comeback_ceiling_thresholds", {})
    monkeypatch.setattr(runtime, "KILLS_MODELS", None)
    monkeypatch.setattr(runtime, "TEAM_PREDICTABILITY_CACHE", None)
    monkeypatch.setattr(runtime, "tempo_solo_dict", None)
    monkeypatch.setattr(runtime, "tempo_duo_dict", None)
    monkeypatch.setattr(runtime, "tempo_cp1v1_dict", None)


def test_load_stats_dicts_loads_late_pub_comeback_table(tmp_path, monkeypatch) -> None:
    table_path = tmp_path / "pub_late_star_comeback_table_piecewise.json"
    table_path.write_text(
        json.dumps(
            {
                "table_rows": [
                    {"wr_level": 85, "minute": 36, "avg_target_networth_diff": -9598.96},
                    {"wr_level": 85, "minute": 37, "avg_target_networth_diff": -9853.70},
                ]
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("STATS_LATE_PUB_COMEBACK_TABLE_PATH", str(table_path))
    monkeypatch.setattr(runtime, "LIVE_LANE_ANALYSIS_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "early_dict", {}, raising=False)
    monkeypatch.setattr(runtime, "late_dict", {}, raising=False)
    monkeypatch.setattr(runtime, "post_lane_dict", {}, raising=False)
    monkeypatch.setattr(runtime, "late_pub_comeback_table_data", None, raising=False)
    monkeypatch.setattr(runtime, "late_pub_comeback_table_thresholds_by_wr", {}, raising=False)
    monkeypatch.setattr(runtime, "late_pub_comeback_table_max_minute_by_wr", {}, raising=False)
    monkeypatch.setattr(runtime, "late_pub_comeback_table_global_max_minute", None, raising=False)

    assert runtime._load_stats_dicts() is True

    assert runtime.late_pub_comeback_table_thresholds_by_wr[85][36] == pytest.approx(-9598.96)
    assert runtime.late_pub_comeback_table_max_minute_by_wr[85] == 37
    assert runtime.late_pub_comeback_table_global_max_minute == 37


def test_build_dota2protracker_block_marks_invalid_metrics() -> None:
    payload = {
        "pro_cp1vs1_late": 0.0,
        "pro_duo_synergy_late": 1.25,
        "pro_cp1vs1_valid": False,
        "pro_duo_synergy_valid": True,
        "pro_lane_mid_cp1vs1": -0.18,
        "pro_lane_mid_cp1vs1_valid": True,
        "pro_lane_advantage": 0.42,
    }

    block = runtime._build_dota2protracker_block(payload)

    assert block.startswith("\ndota2protracker:")
    assert "cp1vs1: invalid" in block
    assert "synergy_duo: +1.25" in block
    assert "mid_cp1vs1" not in block
    assert runtime._build_dota2protracker_lane_adv_line(payload) == "lane_adv_protracker: +0.42\n"


def test_format_dota2protracker_output_value_uses_two_decimals() -> None:
    assert runtime._format_dota2protracker_output_value(1.0938095238095238) == "+1.09"
    assert runtime._format_dota2protracker_output_value("-3*") == "-3.00*"
    assert runtime._format_dota2protracker_output_value("N/A") == "N/A"


def test_build_dota2protracker_lane_adv_line_accepts_legacy_payload_without_flags() -> None:
    assert (
        runtime._build_dota2protracker_lane_adv_line({"pro_lane_advantage": -2.36})
        == "lane_adv_protracker: -2.36\n"
    )
    assert (
        runtime._build_dota2protracker_lane_adv_line(
            {
                "pro_lane_advantage": -2.36,
                "pro_lane_mid_cp1vs1_valid": False,
                "pro_lane_top_cp1vs1_valid": False,
                "pro_lane_bot_cp1vs1_valid": False,
                "pro_lane_top_duo_valid": False,
                "pro_lane_bot_duo_valid": False,
            }
        )
        == "lane_adv_protracker: -2.36\n"
    )


def test_build_dota2protracker_lane_adv_line_emits_none_when_unavailable() -> None:
    assert runtime._build_dota2protracker_lane_adv_line(None) == "lane_adv_protracker: None\n"
    assert runtime._build_dota2protracker_lane_adv_line({}) == "lane_adv_protracker: None\n"
    assert (
        runtime._build_dota2protracker_lane_adv_line({"pro_lane_advantage": "bad"})
        == "lane_adv_protracker: None\n"
    )


def test_has_valid_dota2protracker_signal_requires_at_least_one_valid_metric() -> None:
    assert runtime._has_valid_dota2protracker_signal(
        {
            "pro_cp1vs1_valid": False,
            "pro_duo_synergy_valid": False,
        }
    ) is False
    assert runtime._has_valid_dota2protracker_signal(
        {
            "pro_cp1vs1_valid": True,
            "pro_duo_synergy_valid": False,
        }
    ) is True


def test_has_dispatchable_dota2protracker_signal_respects_thresholds() -> None:
    assert runtime._has_dispatchable_dota2protracker_signal(
        {
            "pro_cp1vs1_valid": True,
            "pro_cp1vs1_late": 3.01,
            "pro_duo_synergy_valid": False,
            "pro_duo_synergy_late": 0.0,
        }
    ) is True
    assert runtime._has_dispatchable_dota2protracker_signal(
        {
            "pro_cp1vs1_valid": True,
            "pro_cp1vs1_late": 2.99,
            "pro_duo_synergy_valid": False,
            "pro_duo_synergy_late": 0.0,
        }
    ) is False
    assert runtime._has_dispatchable_dota2protracker_signal(
        {
            "pro_cp1vs1_valid": False,
            "pro_cp1vs1_late": 0.0,
            "pro_duo_synergy_valid": True,
            "pro_duo_synergy_late": -7.0,
        }
    ) is True


def test_pipeline_probe_phase_block_hides_solo_games() -> None:
    block = runtime._format_pipeline_probe_phase_block(
        "Early",
        {
            "solo": 3,
            "solo_games": 10346,
            "counterpick_1vs1": 4,
            "counterpick_1vs1_games": 1328,
        },
    )

    assert "solo: +3.00 (10346 games)" not in block
    assert "solo: +3.00" in block
    assert "counterpick_1vs1: +4.00 (1328 games)" in block


def test_build_dota2protracker_gate_summary_reports_threshold_pass_state() -> None:
    summary = runtime._build_dota2protracker_gate_summary(
        {
            "pro_cp1vs1_valid": True,
            "pro_cp1vs1_late": 3.5,
            "pro_duo_synergy_valid": True,
            "pro_duo_synergy_late": 4.0,
        }
    )

    assert "cp(abs>=3" in summary
    assert "pass=True" in summary
    assert "duo(abs>=7" in summary
    assert "pass=False" in summary


def test_build_dota2protracker_debug_summary_includes_invalid_reasons() -> None:
    summary = runtime._build_dota2protracker_debug_summary(
        {
            "pro_cp1vs1_valid": False,
            "pro_duo_synergy_valid": False,
            "pro_cp1vs1_reason": "insufficient_core_heroes",
            "pro_duo_synergy_reason": "insufficient_core_heroes",
            "pro_cp1vs1_diagnostics": {"radiant_core_count": 2, "dire_core_count": 3},
            "pro_duo_synergy_diagnostics": {"radiant_core_count": 2, "dire_core_count": 3},
        }
    )

    assert "cp1vs1=invalid" in summary
    assert "reason=insufficient_core_heroes" in summary
    assert "radiant_core_count" in summary


def test_build_dota2protracker_log_lines_include_games_and_reasons() -> None:
    lines = runtime._build_dota2protracker_log_lines(
        {
            "pro_cp1vs1_valid": True,
            "pro_cp1vs1_late": -3.25,
            "pro_cp1vs1_late_games": 84,
            "pro_cp1vs1_reason": "ok",
            "pro_duo_synergy_valid": False,
            "pro_duo_synergy_late": 0.0,
            "pro_duo_synergy_late_games": 0,
            "pro_duo_synergy_reason": "insufficient_duo_core_coverage",
        }
    )

    joined = "\n".join(lines)
    assert "Dota2ProTracker" in joined
    assert "cp1vs1: -3.25" in joined
    assert "games=84" in joined
    assert "synergy_duo: invalid" in joined
    assert "insufficient_duo_core_coverage" in joined


def test_build_lane_block_omits_section_when_lanes_missing() -> None:
    assert runtime._build_lane_block("", "", "") == ""
    assert runtime._build_lane_block(None, "Mid: win 52%", "") == "Lanes:\nMid: win 52%\n\n"
    assert (
        runtime._build_lane_block(
            "Top: lose 75%",
            "Mid: lose 47%",
            "Bot: win 40%",
            lane_adv_line="lane_adv_protracker: +0.42\n",
            lane_adv_dict_line="lane_adv_dict: -16.00\n",
        )
        == "Lanes:\nTop: lose 75%\nMid: lose 47%\nBot: win 40%\nlane_adv_dict: -16.00\nlane_adv_protracker: +0.42\n\n"
    )


def test_build_lane_dict_adv_line_averages_available_lane_edges() -> None:
    assert (
        runtime._build_lane_dict_adv_line(
            "Top: lose 53%",
            "Mid: win 39%",
            "Bot: win 53%",
        )
        == "lane_adv_dict: +0.00\n"
    )
    assert (
        runtime._build_lane_dict_adv_line(
            "Top: lose 60%",
            "Mid: win 55%",
            "Bot: win 70%",
        )
        == "lane_adv_dict: +8.67\n"
    )
    assert runtime._build_lane_dict_adv_line("Top: None", "", None) == ""


def test_pipeline_probe_message_places_protracker_lane_adv_under_dict() -> None:
    message = runtime._build_pipeline_probe_message(
        radiant_team_name="Radiant Team",
        dire_team_name="Dire Team",
        live_league={"radiant_series_wins": 0, "dire_series_wins": 0},
        fallback_score_text="",
        game_time_seconds=600,
        radiant_lead=0,
        radiant_heroes_and_pos={"pos1": {"hero_id": 1}},
        dire_heroes_and_pos={"pos1": {"hero_id": 2}},
        metrics_payload={
            "top": "Top: lose 60%",
            "mid": "Mid: win 55%",
            "bot": "Bot: win 70%",
            "early_output": {},
            "mid_output": {},
            "post_lane_output": {},
        },
        protracker_payload={
            "pro_lane_advantage": 4.2,
            "pro_cp1vs1_late": 1.09,
            "pro_cp1vs1_valid": True,
            "pro_duo_synergy_late": -0.68,
            "pro_duo_synergy_valid": True,
        },
    )

    assert "lane_adv_dict: +8.67\nlane_adv_protracker: +4.20" in message
    assert "\ndota2protracker:\n" not in message
    assert "cp1vs1: +1.09" not in message
    assert "synergy_duo: -0.68" not in message


def test_refresh_stake_multiplier_message_strips_legacy_protracker_block() -> None:
    message = (
        "СТАВКА НА Vici Gaming x1\n"
        "Yakult Brothers VS Vici Gaming\n"
        "Late: (28-60 min):\n"
        "Synergy_trio: +1.00\n"
        "\n"
        "dota2protracker:\n"
        "cp1vs1: +1.09\n"
        "synergy_duo: -0.68\n"
        "Time: 32:00\n"
        "Networth: Yakult Brothers +1000\n"
    )

    updated = runtime._refresh_stake_multiplier_message(
        message,
        stake_multiplier_context={
            "stake_team_name": "Vici Gaming",
            "target_side": "dire",
            "selected_late_sign": -1,
            "has_selected_late_star": True,
            "late_wr_pct": 85.0,
            "radiant_team_name": "Yakult Brothers",
            "dire_team_name": "Vici Gaming",
        },
        game_time_seconds=(36 * 60) + 30,
        radiant_lead=33641,
    )

    assert "\ndota2protracker:\n" not in updated
    assert "cp1vs1: +1.09" not in updated
    assert "synergy_duo: -0.68" not in updated
    assert "Time: 36:30" in updated
    assert "Networth: Yakult Brothers +33641" in updated


def test_refresh_stake_multiplier_message_keeps_dota2protracker_cp1vs1_inside_all_block() -> None:
    message = (
        "СТАВКА НА PlayTime x1\n"
        "nemiga VS playtime\n"
        "All:\n"
        "Counterpick_1vs1: None\n"
        "Counterpick_1vs2: None\n"
        "Synergy_duo: None\n"
        "Synergy_trio: None\n"
        "Dota2ProTracker_cp1vs1: -1.88\n"
        "Time: 11:00\n"
        "Networth: PlayTime +1000\n"
    )

    updated = runtime._refresh_stake_multiplier_message(
        message,
        stake_multiplier_context={
            "stake_team_name": "PlayTime",
            "target_side": "dire",
            "selected_early_sign": -1,
            "has_selected_early_star": True,
            "early_wr_pct": 60.0,
            "radiant_team_name": "Nemiga",
            "dire_team_name": "PlayTime",
        },
        game_time_seconds=(11 * 60) + 58,
        radiant_lead=-1752,
    )

    assert (
        "Synergy_trio: None\n"
        "Dota2ProTracker_cp1vs1: -1.88\n"
        "Time: 11:58\n"
        "Networth: PlayTime +1752\n"
    ) in updated


def test_calculate_lanes_preserves_legacy_return_and_can_return_sources(monkeypatch) -> None:
    monkeypatch.delenv("LANE_SOURCE_CONFIDENCE_DELTA_2V2", raising=False)
    radiant = {
        "pos1": {"hero_id": 1},
        "pos2": {"hero_id": 2},
        "pos3": {"hero_id": 3},
        "pos4": {"hero_id": 4},
        "pos5": {"hero_id": 5},
    }
    dire = {
        "pos1": {"hero_id": 11},
        "pos2": {"hero_id": 12},
        "pos3": {"hero_id": 13},
        "pos4": {"hero_id": 14},
        "pos5": {"hero_id": 15},
    }
    lane_data = {
        "2v2_lanes": {
            "3pos3,4pos4_vs_11pos1,15pos5": {
                "games": 10,
                "wins": 8,
                "draws": 1,
            },
        },
        "2v1_lanes": {},
        "1v1_lanes": {},
        "1_with_1_lanes": {},
        "solo_lanes": {},
    }

    legacy = runtime.calculate_lanes(radiant, dire, lane_data)
    with_sources = runtime.calculate_lanes(radiant, dire, lane_data, return_sources=True)

    assert len(legacy) == 3
    assert legacy[0] == "Top: win 68%\n"
    assert len(with_sources) == 4
    assert with_sources[:3] == legacy
    assert with_sources[3] == {"top": "2v2", "bot": None, "mid": None}


def test_load_map_id_check_urls_migrates_legacy_repo_file(tmp_path, monkeypatch) -> None:
    target_path = tmp_path / ".local" / "state" / "ingame" / "map_id_check.txt"
    legacy_path = tmp_path / "repo" / "map_id_check.txt"
    expected = ["dltv.org/matches/test-migrate.0"]
    legacy_path.parent.mkdir(parents=True, exist_ok=True)
    legacy_path.write_bytes(orjson.dumps(expected))
    monkeypatch.setattr(runtime, "DEFAULT_MAP_ID_CHECK_PATH", target_path, raising=False)
    monkeypatch.setattr(runtime, "DEFAULT_MAP_ID_CHECK_PATH_ODDS", tmp_path / ".local" / "state" / "ingame" / "map_id_check_test.txt", raising=False)
    monkeypatch.setattr(runtime, "LEGACY_MAP_ID_CHECK_PATH", legacy_path, raising=False)
    monkeypatch.setattr(runtime, "LEGACY_MAP_ID_CHECK_PATH_ODDS", tmp_path / "repo" / "map_id_check_test.txt", raising=False)
    monkeypatch.setattr(runtime, "MAP_ID_CHECK_PATH", str(target_path), raising=False)

    urls = runtime._load_map_id_check_urls(recover=False)

    assert urls == expected
    assert target_path.exists()
    assert orjson.loads(target_path.read_bytes()) == expected


def test_compute_moscow_quiet_hours_sleep_seconds() -> None:
    tz = ZoneInfo("Europe/Moscow")

    assert runtime._compute_moscow_quiet_hours_sleep_seconds(
        datetime(2026, 3, 28, 2, 59, tzinfo=tz)
    ) == 0.0
    assert runtime._compute_moscow_quiet_hours_sleep_seconds(
        datetime(2026, 3, 28, 7, 0, tzinfo=tz)
    ) == 0.0

    sleep_seconds = runtime._compute_moscow_quiet_hours_sleep_seconds(
        datetime(2026, 3, 28, 3, 30, tzinfo=tz)
    )
    assert sleep_seconds == 3.5 * 60 * 60


def test_compute_cyberscore_quiet_hours_sleep_seconds() -> None:
    tz = ZoneInfo("Europe/Moscow")

    assert runtime._compute_cyberscore_quiet_hours_sleep_seconds(
        datetime(2026, 3, 28, 23, 59, tzinfo=tz)
    ) == 0.0
    assert runtime._compute_cyberscore_quiet_hours_sleep_seconds(
        datetime(2026, 3, 28, 7, 0, tzinfo=tz)
    ) == 0.0

    sleep_seconds = runtime._compute_cyberscore_quiet_hours_sleep_seconds(
        datetime(2026, 3, 28, 0, 15, tzinfo=tz)
    )
    assert sleep_seconds == 6.75 * 60 * 60


def test_compute_schedule_recheck_sleep_seconds() -> None:
    assert runtime._compute_schedule_recheck_sleep_seconds(-1) == 3 * 60
    assert runtime._compute_schedule_recheck_sleep_seconds(30) == 30
    assert runtime._compute_schedule_recheck_sleep_seconds(5 * 60) == 60
    assert runtime._compute_schedule_recheck_sleep_seconds(29 * 60) == 60
    assert runtime._compute_schedule_recheck_sleep_seconds(30 * 60) == 60
    assert runtime._compute_schedule_recheck_sleep_seconds(45 * 60) == 15 * 60
    assert runtime._compute_schedule_recheck_sleep_seconds(4 * 60 * 60) == 30 * 60


def test_extract_nearest_cyberscore_scheduled_match_info_from_card() -> None:
    now_utc = datetime(2026, 4, 1, 9, 0, tzinfo=ZoneInfo("UTC"))
    html = """
    <html>
      <a class="matches-item" href="/en/matches/111/late-match" data-start-time="2026-04-01T12:00:00+00:00">
        <span class="team-name">Late A</span><span class="team-name">Late B</span>
      </a>
      <a class="matches-item" href="/en/matches/222/next-match" data-start-time="2026-04-01T09:20:00+00:00">
        <span class="team-name">Team A</span><span class="team-name">Team B</span>
      </a>
    </html>
    """

    info = runtime._extract_nearest_cyberscore_scheduled_match_info(html, now_utc=now_utc)

    assert info is not None
    assert info["matchup"] == "Team A vs Team B"
    assert info["href"] == "https://cyberscore.live/en/matches/222/next-match"
    assert info["sleep_seconds_raw"] == 20 * 60
    assert info["sleep_seconds"] == 60
    assert info["source"] == "cyberscore"


def test_cyberscore_schedule_ignores_nested_tournament_dates() -> None:
    now_utc = datetime(2026, 4, 29, 8, 30, tzinfo=ZoneInfo("UTC"))  # 11:30 MSK
    today_item = {
        "id": 222,
        "tournament": {
            "title": "European Pro League 37",
            "date_start": "2026-04-01T00:00:00+00:00",
        },
    }
    tomorrow_item = {
        "id": 333,
        "date_start": "2026-04-30T06:00:00+00:00",
        "tournament": {"title": "European Pro League 37"},
    }
    flight_chunk = (
        f'prefix "item":{json.dumps(today_item, separators=(",", ":"))},'
        f'mid "item":{json.dumps(tomorrow_item, separators=(",", ":"))}, suffix'
    )
    html = f"""
    <html>
      <a class="matches-item" href="/en/matches/222/today-match">
        Today Today at 12:00 BO 3 0:0
        <span class="team-name">Team Stels</span><span class="team-name">Inner Circle</span>
      </a>
      <a class="matches-item" href="/en/matches/333/tomorrow-match">
        Tomorrow Tomorrow at 09:00 BO 3 0:0
        <span class="team-name">MODUS Minus Modus</span><span class="team-name">DOGSENT</span>
      </a>
      <script>self.__next_f.push([1,{json.dumps(flight_chunk)}])</script>
    </html>
    """

    info = runtime._extract_nearest_cyberscore_scheduled_match_info(html, now_utc=now_utc)

    assert info is not None
    assert info["matchup"] == "Team Stels vs Inner Circle"
    assert info["href"] == "https://cyberscore.live/en/matches/222/today-match"
    assert int(info["sleep_seconds_raw"]) == 30 * 60
    assert info["sleep_seconds"] == 60


def test_cyberscore_recent_live_empty_caps_idle_sleep(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "CYBERSCORE_RECENT_LIVE_EMPTY_GRACE_SECONDS", 45 * 60, raising=False)
    monkeypatch.setattr(runtime, "CYBERSCORE_RECENT_LIVE_EMPTY_RECHECK_SECONDS", 60, raising=False)
    monkeypatch.setattr(runtime, "LAST_CYBERSCORE_LIVE_SEEN_MONOTONIC", 1000.0, raising=False)
    schedule_info = {
        "sleep_seconds": 30 * 60,
        "sleep_seconds_raw": 30 * 60,
        "matchup": "no tier1/2 upcoming match",
        "source": "cyberscore_no_upcoming",
    }

    capped = runtime._cap_cyberscore_empty_schedule_after_recent_live(
        schedule_info,
        now_monotonic=1120.0,
    )

    assert capped is True
    assert schedule_info["sleep_seconds"] == 60
    assert schedule_info["sleep_seconds_raw"] == 30 * 60
    assert schedule_info["recent_live_empty"] is True


def test_cyberscore_recent_live_empty_cap_expires(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "CYBERSCORE_RECENT_LIVE_EMPTY_GRACE_SECONDS", 45 * 60, raising=False)
    monkeypatch.setattr(runtime, "CYBERSCORE_RECENT_LIVE_EMPTY_RECHECK_SECONDS", 60, raising=False)
    monkeypatch.setattr(runtime, "LAST_CYBERSCORE_LIVE_SEEN_MONOTONIC", 1000.0, raising=False)
    schedule_info = {
        "sleep_seconds": 30 * 60,
        "sleep_seconds_raw": 30 * 60,
        "matchup": "no tier1/2 upcoming match",
        "source": "cyberscore_no_upcoming",
    }

    capped = runtime._cap_cyberscore_empty_schedule_after_recent_live(
        schedule_info,
        now_monotonic=1000.0 + 60 * 60,
    )

    assert capped is False
    assert schedule_info["sleep_seconds"] == 30 * 60
    assert "recent_live_empty" not in schedule_info


def test_cyberscore_schedule_sleep_polls_near_midnight_quiet_window() -> None:
    now_utc = datetime(2026, 4, 1, 20, 50, tzinfo=ZoneInfo("UTC"))  # 23:50 MSK
    html = """
    <html>
      <a class="matches-item" href="/en/matches/333/after-midnight" data-start-time="2026-04-01T21:10:00+00:00">
        <span class="team-name">Night A</span><span class="team-name">Night B</span>
      </a>
    </html>
    """

    info = runtime._extract_nearest_cyberscore_scheduled_match_info(html, now_utc=now_utc)

    assert info is not None
    assert info["sleep_seconds_raw"] == 20 * 60
    assert info["sleep_seconds"] == 60


def test_cyberscore_schedule_before_quiet_end_keeps_runtime_awake() -> None:
    tz = ZoneInfo("Europe/Moscow")
    now = datetime(2026, 4, 2, 0, 5, tzinfo=tz)
    during_quiet = {
        "scheduled_at_msk": datetime(2026, 4, 2, 1, 30, tzinfo=tz),
        "matchup": "Lynx vs Sa Reject",
    }
    after_quiet = {
        "scheduled_at_msk": datetime(2026, 4, 2, 7, 30, tzinfo=tz),
        "matchup": "Late Match",
    }

    assert runtime._cyberscore_schedule_before_quiet_end(during_quiet, now=now) is True
    assert runtime._cyberscore_schedule_before_quiet_end(after_quiet, now=now) is False


def test_cyberscore_proxy_required_blocks_direct(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "CYBERSCORE_CAMOUFOX_REQUIRE_PROXY", True, raising=False)
    monkeypatch.setattr(runtime, "CYBERSCORE_CAMOUFOX_PROXY_URL", "", raising=False)
    monkeypatch.setattr(runtime, "CURRENT_PROXY", None, raising=False)
    monkeypatch.setattr(runtime, "DLTV_PROXY_POOL", [], raising=False)
    monkeypatch.setattr(runtime, "PROXY_LIST", [], raising=False)

    with pytest.raises(RuntimeError):
        runtime._cyberscore_camoufox_proxy_kwargs()


def test_cyberscore_proxy_parser_accepts_host_first_credentials() -> None:
    proxy_kwargs = runtime._camoufox_proxy_kwargs_from_url("proxy.example:12345@login:password")

    assert proxy_kwargs == {
        "proxy": {
            "server": "http://proxy.example:12345",
            "username": "login",
            "password": "password",
        }
    }


def test_init_proxy_pool_prunes_dead_live_proxies_and_keeps_alive(monkeypatch) -> None:
    messages: List[str] = []
    validation_calls: List[str] = []

    def _fake_validate(proxy, **_kwargs):
        validation_calls.append(proxy)
        return {
            "ok": proxy == "http://live.example:2000",
            "attempts": 3,
            "reason": "http 200" if proxy == "http://live.example:2000" else "connection refused",
        }

    monkeypatch.setattr(runtime, "PROXY_LIST", ["http://dead.example:1000", "http://live.example:2000"], raising=False)
    monkeypatch.setattr(runtime, "CURRENT_PROXY_INDEX", 0, raising=False)
    monkeypatch.setattr(runtime, "CURRENT_PROXY", None, raising=False)
    monkeypatch.setattr(runtime, "PROXIES", {}, raising=False)
    monkeypatch.setattr(runtime, "USE_PROXY", None, raising=False)
    monkeypatch.setattr(runtime, "CYBERSCORE_CAMOUFOX_PROXY_URL", "", raising=False)
    monkeypatch.setattr(runtime, "LIVE_PROXY_PREFLIGHT_ENABLED", True, raising=False)
    monkeypatch.setattr(runtime, "LIVE_PROXY_PREFLIGHT_ATTEMPTS", 3, raising=False)
    monkeypatch.setattr(runtime, "LIVE_PROXY_EMPTY_POOL_FATAL", True, raising=False)
    monkeypatch.setattr(runtime, "_validate_live_proxy", _fake_validate)
    monkeypatch.setattr(runtime, "send_message", lambda text, **_kwargs: messages.append(text))

    runtime._init_proxy_pool(True)

    assert validation_calls == ["http://dead.example:1000", "http://live.example:2000"]
    assert runtime.PROXY_LIST == ["http://live.example:2000"]
    assert runtime.CURRENT_PROXY == "http://live.example:2000"
    assert runtime.PROXIES == {
        "http": "http://live.example:2000",
        "https": "http://live.example:2000",
    }
    assert messages == []


def test_init_proxy_pool_exits_and_alerts_when_all_live_proxies_dead(monkeypatch) -> None:
    messages: List[str] = []

    monkeypatch.setattr(
        runtime,
        "PROXY_LIST",
        [
            "http://user:secret@dead-one.example:1000",
            "http://user:secret@dead-two.example:2000",
        ],
        raising=False,
    )
    monkeypatch.setattr(runtime, "CURRENT_PROXY_INDEX", 0, raising=False)
    monkeypatch.setattr(runtime, "CURRENT_PROXY", None, raising=False)
    monkeypatch.setattr(runtime, "PROXIES", {}, raising=False)
    monkeypatch.setattr(runtime, "USE_PROXY", None, raising=False)
    monkeypatch.setattr(runtime, "CYBERSCORE_CAMOUFOX_PROXY_URL", "", raising=False)
    monkeypatch.setattr(runtime, "LIVE_PROXY_PREFLIGHT_ENABLED", True, raising=False)
    monkeypatch.setattr(runtime, "LIVE_PROXY_PREFLIGHT_ATTEMPTS", 3, raising=False)
    monkeypatch.setattr(runtime, "LIVE_PROXY_EMPTY_POOL_FATAL", True, raising=False)
    monkeypatch.setattr(runtime, "LIVE_PROXY_EMPTY_POOL_ALERT_SENT", False, raising=False)
    monkeypatch.setattr(
        runtime,
        "_validate_live_proxy",
        lambda proxy, **_kwargs: {"ok": False, "attempts": 3, "reason": "connection refused"},
    )
    monkeypatch.setattr(runtime, "send_message", lambda text, **_kwargs: messages.append(text))

    with pytest.raises(SystemExit) as exc_info:
        runtime._init_proxy_pool(True)

    assert exc_info.value.code == 2
    assert runtime.PROXY_LIST == []
    assert messages
    assert "Все live proxies мертвы" in messages[0]
    assert "secret" not in messages[0]


def test_cyberscore_proxy_kwargs_uses_pruned_live_pool_not_raw_dltv_pool(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "CYBERSCORE_CAMOUFOX_REQUIRE_PROXY", True, raising=False)
    monkeypatch.setattr(runtime, "CYBERSCORE_CAMOUFOX_PROXY_URL", "", raising=False)
    monkeypatch.setattr(runtime, "USE_PROXY", True, raising=False)
    monkeypatch.setattr(runtime, "CURRENT_PROXY", None, raising=False)
    monkeypatch.setattr(runtime, "DLTV_PROXY_POOL", ["http://user:secret@dead.example:1000"], raising=False)
    monkeypatch.setattr(runtime, "PROXY_LIST", ["http://user:secret@live.example:2000"], raising=False)

    proxy_kwargs = runtime._cyberscore_camoufox_proxy_kwargs()

    assert proxy_kwargs["proxy"]["server"] == "http://live.example:2000"


def test_runtime_dead_proxy_error_prunes_current_live_proxy(monkeypatch) -> None:
    reset_calls: List[str] = []

    monkeypatch.setattr(runtime, "PROXY_LIST", ["http://dead.example:1000", "http://live.example:2000"], raising=False)
    monkeypatch.setattr(runtime, "CURRENT_PROXY_INDEX", 0, raising=False)
    monkeypatch.setattr(runtime, "CURRENT_PROXY", "http://dead.example:1000", raising=False)
    monkeypatch.setattr(
        runtime,
        "PROXIES",
        {"http": "http://dead.example:1000", "https": "http://dead.example:1000"},
        raising=False,
    )
    monkeypatch.setattr(runtime, "USE_PROXY", True, raising=False)
    monkeypatch.setattr(runtime, "LIVE_PROXY_RUNTIME_PRUNE_ENABLED", True, raising=False)
    monkeypatch.setattr(runtime, "LIVE_PROXY_PREFLIGHT_ATTEMPTS", 3, raising=False)
    monkeypatch.setattr(runtime, "LIVE_PROXY_EMPTY_POOL_FATAL", True, raising=False)
    monkeypatch.setattr(
        runtime,
        "_validate_live_proxy",
        lambda proxy, **_kwargs: {"ok": False, "attempts": 3, "reason": "connection refused"},
    )
    monkeypatch.setattr(runtime._shared_camoufox_session, "request_reset", lambda: reset_calls.append("reset"))

    pruned = runtime._prune_current_live_proxy_if_dead(
        "Page.goto: NS_ERROR_PROXY_CONNECTION_REFUSED",
        source_label="Camoufox",
        target_url="https://cyberscore.live/en/matches/",
    )

    assert pruned is True
    assert runtime.PROXY_LIST == ["http://live.example:2000"]
    assert runtime.CURRENT_PROXY == "http://live.example:2000"
    assert runtime.PROXIES == {
        "http": "http://live.example:2000",
        "https": "http://live.example:2000",
    }
    assert reset_calls == ["reset"]


def test_cyberscore_long_page_job_does_not_reset_shared_browser(monkeypatch) -> None:
    calls: List[Dict[str, Any]] = []

    def _fake_run_shared_camoufox_job(label, callback, **kwargs):
        calls.append({"label": label, "kwargs": dict(kwargs)})
        return "<html></html>"

    monkeypatch.setattr(runtime, "_run_shared_camoufox_job", _fake_run_shared_camoufox_job)

    assert runtime._get_cyberscore_html_via_long_page("https://cyberscore.live/en/matches/173557/") == "<html></html>"
    assert calls == [
        {
            "label": "cyberscore-long:https://cyberscore.live/en/matches/173557/",
            "kwargs": {"timeout": 90, "retry": False, "reset_on_error": False},
        }
    ]


def test_cyberscore_one_shot_job_does_not_reset_shared_browser(monkeypatch) -> None:
    calls: List[Dict[str, Any]] = []

    def _fake_run_shared_camoufox_job(label, callback, **kwargs):
        calls.append({"label": label, "kwargs": dict(kwargs)})
        return "<html></html>"

    monkeypatch.setattr(runtime, "CAMOUFOX_AVAILABLE", True, raising=False)
    monkeypatch.setattr(runtime, "_cyberscore_long_page_enabled_for_url", lambda _url: False)
    monkeypatch.setattr(runtime, "_run_shared_camoufox_job", _fake_run_shared_camoufox_job)

    assert runtime._get_cyberscore_html_via_camoufox("https://cyberscore.live/en/matches/173557/") == "<html></html>"
    assert calls == [
        {
            "label": "cyberscore:https://cyberscore.live/en/matches/173557/",
            "kwargs": {"timeout": 90, "retry": False, "reset_on_error": False},
        }
    ]


def test_cyberscore_camoufox_retries_after_empty_fetch(monkeypatch) -> None:
    calls: List[str] = []

    def _fake_one_shot(_url):
        calls.append("one-shot")
        return None if len(calls) == 1 else "<html></html>"

    monkeypatch.setattr(runtime, "CAMOUFOX_AVAILABLE", True, raising=False)
    monkeypatch.setattr(runtime, "CYBERSCORE_CAMOUFOX_FETCH_ATTEMPTS", 2, raising=False)
    monkeypatch.setattr(runtime, "_cyberscore_long_page_enabled_for_url", lambda _url: False)
    monkeypatch.setattr(runtime, "_get_cyberscore_html_via_one_shot", _fake_one_shot)

    assert runtime._get_cyberscore_html_via_camoufox("https://cyberscore.live/en/matches/173557/") == "<html></html>"
    assert calls == ["one-shot", "one-shot"]


@pytest.mark.parametrize(
    "error_message",
    [
        "NS_ERROR_NET_RESET",
        "Page.goto: NS_ERROR_NET_INTERRUPT",
    ],
)
def test_cyberscore_transient_fetch_refreshes_network_path(monkeypatch, error_message: str) -> None:
    rotate_calls: List[str] = []
    reset_calls: List[str] = []

    def _fake_run_shared_camoufox_job(*_args, **_kwargs):
        raise RuntimeError(error_message)

    monkeypatch.setattr(runtime, "_run_shared_camoufox_job", _fake_run_shared_camoufox_job)
    monkeypatch.setattr(runtime, "CYBERSCORE_CAMOUFOX_PROXY_URL", "", raising=False)
    monkeypatch.setattr(runtime, "USE_PROXY", True, raising=False)
    monkeypatch.setattr(runtime, "PROXY_LIST", ["proxy-a", "proxy-b"], raising=False)
    monkeypatch.setattr(runtime, "rotate_proxy", lambda: rotate_calls.append("rotate"), raising=False)
    monkeypatch.setattr(
        runtime._shared_camoufox_session,
        "request_reset",
        lambda: reset_calls.append("reset"),
    )

    assert runtime._get_cyberscore_html_via_long_page("https://cyberscore.live/en/matches/173557/") is None
    assert rotate_calls == ["rotate"]
    assert reset_calls == ["reset"]


def test_shared_camoufox_job_can_fail_without_reset(monkeypatch) -> None:
    close_events: List[str] = []

    class _FakeBrowser:
        def close(self) -> None:
            close_events.append("browser")

    fake_browser = _FakeBrowser()

    class _FakeCamoufox:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def __enter__(self):
            return fake_browser

        def __exit__(self, *_args) -> None:
            close_events.append("context")

    class _FakeCamoufoxModule:
        Camoufox = _FakeCamoufox

    def _fail(_browser):
        raise RuntimeError("tab failed")

    session = runtime._SharedCamoufoxSession()
    monkeypatch.setattr(runtime, "CAMOUFOX_AVAILABLE", True, raising=False)
    monkeypatch.setattr(runtime, "camoufox", _FakeCamoufoxModule(), raising=False)
    monkeypatch.setattr(runtime, "_cyberscore_camoufox_proxy_kwargs", lambda: {}, raising=False)
    try:
        with pytest.raises(RuntimeError):
            session.submit("dota2protracker:test", _fail, timeout=2, reset_on_error=False)
        assert session.submit("after-error", lambda browser: browser is fake_browser, timeout=2) is True
        assert close_events == []
    finally:
        session.close()


def test_shared_camoufox_session_applies_requested_reset_before_next_job(monkeypatch) -> None:
    close_events: List[str] = []
    created_browsers: List[Any] = []

    class _FakeBrowser:
        def __init__(self, index: int) -> None:
            self.index = index

        def close(self) -> None:
            close_events.append(f"browser:{self.index}")

    class _FakeCamoufox:
        def __init__(self, *args, **kwargs) -> None:
            self.browser = _FakeBrowser(len(created_browsers))

        def __enter__(self):
            created_browsers.append(self.browser)
            return self.browser

        def __exit__(self, *_args) -> None:
            close_events.append(f"context:{self.browser.index}")

    class _FakeCamoufoxModule:
        Camoufox = _FakeCamoufox

    session = runtime._SharedCamoufoxSession()
    monkeypatch.setattr(runtime, "CAMOUFOX_AVAILABLE", True, raising=False)
    monkeypatch.setattr(runtime, "camoufox", _FakeCamoufoxModule(), raising=False)
    monkeypatch.setattr(runtime, "_cyberscore_camoufox_proxy_kwargs", lambda: {}, raising=False)
    try:
        assert session.submit("first", lambda browser: browser.index, timeout=2) == 0
        session.request_reset()
        assert session.submit("second", lambda browser: browser.index, timeout=2) == 1
        assert close_events == ["browser:0", "context:0"]
    finally:
        session.close()


def test_extract_nearest_scheduled_match_info() -> None:
    html = """
    <div class="live__matches"></div>
    <a class="event" href="/events/test-1">
      <div class="event__name"><div>ESL One Birmingham 2026</div></div>
      <div class="event__info">
        <div class="event__info-info">
          <div class="event__info-info__time" data-moment="HH:mm">2026-03-28 12:00:00</div>
        </div>
      </div>
    </a>
    <div class="match__item">
      <div class="match__item-team__name">Aurora</div>
      <div class="match__item-team__name">PARIVISION</div>
    </div>
    <a class="event" href="/events/test-2">
      <div class="event__name"><div>ESL One Birmingham 2026</div></div>
      <div class="event__info">
        <div class="event__info-info">
          <div class="event__info-info__time" data-moment="HH:mm">2026-03-28 15:30:00</div>
        </div>
      </div>
    </a>
    <div class="match__item">
      <div class="match__item-team__name">Team Yandex</div>
      <div class="match__item-team__name">Tundra Esports</div>
    </div>
    """
    soup = BeautifulSoup(html, "lxml")
    schedule = runtime._extract_nearest_scheduled_match_info(
        soup,
        now_utc=datetime(2026, 3, 28, 8, 46, tzinfo=ZoneInfo("UTC")),
    )

    assert schedule is not None
    assert schedule["matchup"] == "Aurora vs PARIVISION"
    assert int(schedule["sleep_seconds_raw"]) == (3 * 60 * 60 + 14 * 60)
    assert int(schedule["sleep_seconds"]) == (30 * 60)


def test_extract_nearest_scheduled_match_info_skips_denied_leagues() -> None:
    html = """
    <div class="live__matches"></div>
    <a class="event" href="/events/test-skip">
      <div class="event__name"><div>BLAST Slam VII: China Open Qualifier 2</div></div>
      <div class="event__info">
        <div class="event__info-info">
          <div class="event__info-info__time" data-moment="HH:mm">2026-03-28 12:00:00</div>
        </div>
      </div>
    </a>
    <div class="match__item">
      <div class="match__item-team__name">Skip Team A</div>
      <div class="match__item-team__name">Skip Team B</div>
    </div>
    <a class="event" href="/events/test-keep">
      <div class="event__name"><div>ESL One Birmingham 2026</div></div>
      <div class="event__info">
        <div class="event__info-info">
          <div class="event__info-info__time" data-moment="HH:mm">2026-03-28 15:30:00</div>
        </div>
      </div>
    </a>
    <div class="match__item">
      <div class="match__item-team__name">Team Yandex</div>
      <div class="match__item-team__name">Tundra Esports</div>
    </div>
    """
    soup = BeautifulSoup(html, "lxml")
    schedule = runtime._extract_nearest_scheduled_match_info(
        soup,
        now_utc=datetime(2026, 3, 28, 8, 46, tzinfo=ZoneInfo("UTC")),
    )

    assert schedule is not None
    assert schedule["matchup"] == "Team Yandex vs Tundra Esports"
    assert schedule["league_title"] == "ESL One Birmingham 2026"


def test_extract_nearest_scheduled_match_info_supports_new_match_card_layout() -> None:
    html = """
    <div class="match upcoming" data-matches-odd="2026-04-01 11:00:00" data-series-id="425790">
      <div class="match__head">
        <div class="match__head-event"><span>Fonbet Media Eleague Season 4</span></div>
      </div>
      <div class="match__body">
        <div class="match__body-details">
          <div class="match__body-details__team">
            <div class="team"><div class="team__title"><span>Team RostikFaceKid</span></div></div>
            <div class="team"><div class="team__title"><span>Team Lens</span></div></div>
          </div>
        </div>
      </div>
    </div>
    """
    soup = BeautifulSoup(html, "lxml")
    schedule = runtime._extract_nearest_scheduled_match_info(
        soup,
        now_utc=datetime(2026, 4, 1, 8, 0, tzinfo=ZoneInfo("UTC")),
    )

    assert schedule is not None
    assert schedule["league_title"] == "Fonbet Media Eleague Season 4"
    assert schedule["matchup"] == "Team RostikFaceKid vs Team Lens"
    assert schedule["scheduled_at_msk"].strftime("%Y-%m-%d %H:%M:%S") == "2026-04-01 14:00:00"


def test_extract_nearest_scheduled_match_info_skips_denied_new_layout_by_href() -> None:
    html = """
    <div class="match upcoming tbd" data-matches-odd="2026-03-31 07:00:00" data-series-id="425836">
      <div class="match__body">
        <div class="match__body-details">
          <a href="https://dltv.org/matches/425836/cloud-rising-vs-tbd-blast-slam-vii-china-open-qualifier-2"></a>
          <div class="match__body-details__team">
            <div class="team"><div class="team__title"><span>Cloud Rising</span></div></div>
            <div class="team"><div class="team__title"><span>TBD</span></div></div>
          </div>
        </div>
      </div>
    </div>
    <div class="match upcoming" data-matches-odd="2026-04-01 11:00:00" data-series-id="425790">
      <div class="match__head">
        <div class="match__head-event"><span>Fonbet Media Eleague Season 4</span></div>
      </div>
      <div class="match__body">
        <div class="match__body-details">
          <a href="https://dltv.org/matches/425790/team-rostikfacekid-vs-team-lens-fonbet-media-eleague-season-4"></a>
          <div class="match__body-details__team">
            <div class="team"><div class="team__title"><span>Team RostikFaceKid</span></div></div>
            <div class="team"><div class="team__title"><span>Team Lens</span></div></div>
          </div>
        </div>
      </div>
    </div>
    """
    soup = BeautifulSoup(html, "lxml")
    schedule = runtime._extract_nearest_scheduled_match_info(
        soup,
        now_utc=datetime(2026, 3, 31, 6, 0, tzinfo=ZoneInfo("UTC")),
    )

    assert schedule is not None
    assert schedule["league_title"] == "Fonbet Media Eleague Season 4"
    assert schedule["matchup"] == "Team RostikFaceKid vs Team Lens"


def test_should_poll_for_scheduled_live_target_after_match_start() -> None:
    runtime.SCHEDULE_LIVE_WAIT_TARGET = {
        "matchup": "Team Yandex vs Xtreme Gaming",
        "scheduled_at_utc": datetime(2026, 3, 29, 11, 0, tzinfo=ZoneInfo("UTC")),
    }

    assert runtime._should_poll_for_scheduled_live_target(
        datetime(2026, 3, 29, 10, 59, tzinfo=ZoneInfo("UTC"))
    ) is False
    assert runtime._should_poll_for_scheduled_live_target(
        datetime(2026, 3, 29, 11, 0, 1, tzinfo=ZoneInfo("UTC"))
    ) is True

    runtime.SCHEDULE_LIVE_WAIT_TARGET = None


def test_emit_pending_schedule_wake_audit_logs_schedule_shift(capsys, monkeypatch) -> None:
    monkeypatch.setattr(runtime, "_get_current_proxy_marker", lambda: "proxy:test")
    runtime.PENDING_SCHEDULE_WAKE_AUDIT = {
        "matchup": "Aurora vs PARIVISION",
        "scheduled_at_msk": datetime(2026, 3, 28, 15, 0, tzinfo=ZoneInfo("Europe/Moscow")),
        "woke_at_msk": datetime(2026, 3, 28, 15, 0, 5, tzinfo=ZoneInfo("Europe/Moscow")),
    }

    runtime._emit_pending_schedule_wake_audit(
        heads_count=0,
        bodies_count=0,
        next_schedule_info={
            "matchup": "Team Yandex vs Tundra Esports",
            "scheduled_at_msk": datetime(2026, 3, 28, 18, 30, tzinfo=ZoneInfo("Europe/Moscow")),
        },
    )

    captured = capsys.readouterr()
    assert "Aurora vs PARIVISION" in captured.out
    assert "Team Yandex vs Tundra Esports" in captured.out
    assert runtime.PENDING_SCHEDULE_WAKE_AUDIT is None


def test_build_recent_match_summaries_text_for_rejected_match(tmp_path, monkeypatch) -> None:
    log_path = tmp_path / "log.txt"
    log_path.write_text(
        "\n".join(
            [
                "🔍 DEBUG: Начало обработки матча #0",
                "   Статус: 0:52",
                "   URL: dltv.org/matches/425881/virtuspro-vs-team-stels-blast-slam-vii-europe-open-qualifier-1.0",
                "   Score: 0 : 0",
                "   ✅ Драфт успешно распарсен",
                "   🛣️ Lanes:",
                "      Top: lose 52%",
                "      Mid: lose 53%",
                "      Bot: win 58%",
                "   📊 Team ELO attached: source=elo_live_lineup_snapshot raw Virtus.pro=1586 vs Team Stels=1374 (raw_wr=77.3%/22.7%, adj_wr=87.6%/12.4%)",
                "   ⚠️ Early star invalidated by ELO block guard (raw_wr=90.0%, penalty=37.6, adj=52.4%)",
                "   ⚠️ Late star invalidated by ELO block guard (raw_wr=60.0%, penalty=37.6, adj=22.4%)",
                "   ⚠️ ВЕРДИКТ: ОТКАЗ (нет late star-сигнала) - матч пропущен",
                "   📉 Star checks: WR60: early=ok, late=ok, match=send_now_same_sign | ELO60: early=elo_wr_below_min60(adj=52.4,penalty=37.6), late=elo_wr_below_min60(adj=22.4,penalty=37.6)",
                "   ✅ map_id_check.txt обновлен: add_url после отказа no-late-star",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(runtime, "PROJECT_ROOT", tmp_path, raising=False)

    payload = runtime._build_recent_match_summaries_text(limit=10)

    assert "[1]" in payload
    assert "Статус: 0:52" in payload
    assert "URL: dltv.org/matches/425881/virtuspro-vs-team-stels-blast-slam-vii-europe-open-qualifier-1" in payload
    assert "Top: lose 52%" in payload
    assert "Late star invalidated by ELO block guard" in payload
    assert "ВЕРДИКТ: ОТКАЗ" in payload
    assert "map_id_check.txt обновлен: add_url после отказа no-late-star" in payload


def test_build_recent_match_summaries_text_appends_delayed_outcome(tmp_path, monkeypatch) -> None:
    log_path = tmp_path / "log.txt"
    log_path.write_text(
        "\n".join(
            [
                "🔍 DEBUG: Начало обработки матча #0",
                "   Статус: draft...",
                "   URL: dltv.org/matches/425690/winter-bear-vs-nemiga-gaming-european-pro-league-season-35.1",
                "   Score: 0 : 1",
                "   ✅ Драфт успешно распарсен",
                "   🛣️ Lanes:",
                "      Top: draw 50%",
                "      Mid: win 52%",
                "      Bot: lose 47%",
                "   📊 Team ELO attached: source=elo_live_lineup_snapshot raw Nemiga Gaming=1456 vs Winter Bear=1462 (raw_wr=49.1%/50.9%, adj_wr=49.1%/50.9%)",
                "   ⏳ Ожидание dispatch: late_only_opposite_signs (target_side=dire)",
                "   📉 Star checks: WR60: early=ok, late=ok, match=delay_late_only_opposite_signs | ELO60: early=ok, late=ok",
                "   ✅ ВЕРДИКТ: Сигнал добавлен в delayed-очередь (reason=late_only_opposite_signs)",
                "⏱️ Отложенный сигнал отправлен по comeback ceiling: dltv.org/matches/425690/winter-bear-vs-nemiga-gaming-european-pro-league-season-35.1 (game_time=1233, target_networth_diff=-569, minute=20, ceiling=13500)",
                "🔍 DEBUG: Начало обработки матча #1",
                "   Статус: finished",
                "   URL: dltv.org/matches/425690/winter-bear-vs-nemiga-gaming-european-pro-league-season-35.1",
                "   Score: 0 : 1",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(runtime, "PROJECT_ROOT", tmp_path, raising=False)

    payload = runtime._build_recent_match_summaries_text(limit=10)

    assert "URL: dltv.org/matches/425690/winter-bear-vs-nemiga-gaming-european-pro-league-season-35" in payload
    assert "ВЕРДИКТ: Сигнал добавлен в delayed-очередь" in payload
    assert "Отложенный сигнал отправлен по comeback ceiling" in payload
    assert payload.count("winter-bear-vs-nemiga-gaming-european-pro-league-season-35") == 2


def test_build_recent_match_summaries_text_orders_from_older_to_newer(tmp_path, monkeypatch) -> None:
    log_path = tmp_path / "log.txt"
    log_path.write_text(
        "\n".join(
            [
                "🔍 DEBUG: Начало обработки матча #0",
                "   Статус: draft...",
                "   URL: dltv.org/matches/1/older-match.0",
                "   Score: 0 : 0",
                "   ✅ Драфт успешно распарсен",
                "   ⚠️ ВЕРДИКТ: ОТКАЗ (нет late star-сигнала) - матч пропущен",
                "🔍 DEBUG: Начало обработки матча #1",
                "   Статус: draft...",
                "   URL: dltv.org/matches/2/newer-match.0",
                "   Score: 0 : 0",
                "   ✅ Драфт успешно распарсен",
                "   ⚠️ ВЕРДИКТ: ОТКАЗ (нет late star-сигнала) - матч пропущен",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(runtime, "PROJECT_ROOT", tmp_path, raising=False)

    payload = runtime._build_recent_match_summaries_text(limit=10)

    assert payload.index("dltv.org/matches/1/older-match") < payload.index("dltv.org/matches/2/newer-match")


def test_read_log_tail_lines_returns_recent_lines_without_full_scan(tmp_path) -> None:
    log_path = tmp_path / "log.txt"
    log_path.write_text(
        "\n".join(f"line-{idx}" for idx in range(1, 501)),
        encoding="utf-8",
    )

    lines = runtime._read_log_tail_lines(log_path, max_lines=5, chunk_size=32, max_bytes=256)

    assert lines == ["line-496", "line-497", "line-498", "line-499", "line-500"]


def test_parse_draft_and_positions_uses_live_league_players_for_account_ids() -> None:
    html = """
    <div class="lineups__team">
      <span class="title">xtreme</span>
      <div class="player__name-name">ame</div><div class="player__role-item">Core</div>
      <div class="player__name-name">nothingtosay</div><div class="player__role-item">Mid</div>
      <div class="player__name-name">xxs</div><div class="player__role-item">Offlane</div>
      <div class="player__name-name">fy</div><div class="player__role-item">Support</div>
      <div class="player__name-name">xnova</div><div class="player__role-item">Full Support</div>
    </div>
    <div class="lineups__team">
      <span class="title">yandex</span>
      <div class="player__name-name">watson</div><div class="player__role-item">Core</div>
      <div class="player__name-name">chira_junior</div><div class="player__role-item">Mid</div>
      <div class="player__name-name">noticed</div><div class="player__role-item">Offlane</div>
      <div class="player__name-name">saksa</div><div class="player__role-item">Support</div>
      <div class="player__name-name">malady</div><div class="player__role-item">Full Support</div>
    </div>
    """
    soup = BeautifulSoup(html, "lxml")
    data = {
        "fast_picks": {
            "first_team": [
                {"player": {"title": "chira_junior"}, "hero_id": 106},
                {"player": {"title": "noticed"}, "hero_id": 29},
                {"player": {"title": "watson"}, "hero_id": 46},
                {"player": {"title": "saksa"}, "hero_id": 65},
                {"player": {"title": "malady"}, "hero_id": 110},
            ],
            "second_team": [
                {"player": {"title": "fy"}, "hero_id": 100},
                {"player": {"title": "nothingtosay"}, "hero_id": 52},
                {"player": {"title": "xnova"}, "hero_id": 111},
                {"player": {"title": "ame"}, "hero_id": 114},
                {"player": {"title": "xxs"}, "hero_id": 129},
            ],
        },
        "live_league_data": {
            "players": [
                {"hero_id": 106, "account_id": 10106},
                {"hero_id": 29, "account_id": 10029},
                {"hero_id": 46, "account_id": 10046},
                {"hero_id": 65, "account_id": 10065},
                {"hero_id": 110, "account_id": 10110},
                {"hero_id": 100, "account_id": 10100},
                {"hero_id": 52, "account_id": 10052},
                {"hero_id": 111, "account_id": 10111},
                {"hero_id": 114, "account_id": 10114},
                {"hero_id": 129, "account_id": 10129},
            ]
        },
    }

    radiant, dire, error, _summary, _candidates = runtime.parse_draft_and_positions(
        soup,
        data,
        "xtreme",
        "yandex",
    )

    assert error is None
    assert radiant["pos1"]["account_id"] == 10114
    assert radiant["pos2"]["account_id"] == 10052
    assert dire["pos1"]["account_id"] == 10046
    assert dire["pos5"]["account_id"] == 10110


def test_zero_players_proxy_ban_diagnostics_requires_fast_picks_and_zero_counts(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "USE_PROXY", True, raising=False)
    monkeypatch.setattr(runtime, "CURRENT_PROXY", "http://proxy.local", raising=False)

    diag = runtime._zero_players_proxy_ban_diagnostics(
        "Слишком мало игроков: radiant=0, dire=0",
        {"fast_picks": [1, 2, 3]},
    )

    assert diag is not None
    assert diag["radiant"] == 0
    assert diag["dire"] == 0
    assert diag["proxy_in_use"] is True
    assert diag["proxy_marker"] == "http://proxy.local"

    assert runtime._zero_players_proxy_ban_diagnostics(
        "Слишком мало игроков: radiant=1, dire=4",
        {"fast_picks": [1]},
    ) is None
    assert runtime._zero_players_proxy_ban_diagnostics(
        "Слишком мало игроков: radiant=0, dire=0",
        {},
    ) is None


def test_functions_star_thresholds_require_real_file(tmp_path, monkeypatch) -> None:
    import functions

    missing = tmp_path / "missing_star_thresholds.json"
    monkeypatch.setattr(functions, "STAR_THRESHOLDS_PATH", missing)

    with pytest.raises(FileNotFoundError):
        functions._load_star_thresholds()


def test_signal_wrappers_star_thresholds_require_real_file(tmp_path, monkeypatch) -> None:
    import signal_wrappers

    missing = tmp_path / "missing_star_thresholds.json"
    signal_wrappers._load_star_thresholds.cache_clear()
    monkeypatch.setattr(signal_wrappers, "STAR_THRESHOLDS_PATH", missing)

    with pytest.raises(FileNotFoundError):
        signal_wrappers._load_star_thresholds()

    signal_wrappers._load_star_thresholds.cache_clear()


def test_send_message_requires_delivery_confirmation(monkeypatch) -> None:
    import functions

    class _RejectedResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> Dict[str, Any]:
            return {"ok": False, "description": "bot was blocked by the user"}

    monkeypatch.setattr(functions.requests, "post", lambda *_args, **_kwargs: _RejectedResponse())

    with pytest.raises(RuntimeError):
        functions.send_message("test message", require_delivery=True)


def test_send_message_uses_curl_fallback_on_ssl_connection_error(tmp_path, monkeypatch) -> None:
    import functions
    monkeypatch.setattr(functions, "TELEGRAM_UPDATES_FETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(functions, "TELEGRAM_SUBSCRIBERS_STATE_PATH", tmp_path / "telegram_subscribers_state.json", raising=False)
    monkeypatch.setattr(functions, "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH", tmp_path / "legacy_telegram_subscribers_state.json", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_id", "100", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_ids", [], raising=False)
    monkeypatch.setattr(functions, "_vk_is_enabled", lambda: False)

    class _CurlResult:
        returncode = 0
        stdout = '{"ok": true, "result": {"message_id": 1}}'
        stderr = ""

    monkeypatch.setattr(
        functions.requests,
        "post",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            functions.requests.exceptions.ConnectionError(
                "SSLEOFError(8, 'EOF occurred in violation of protocol')"
            )
        ),
    )
    monkeypatch.setattr(functions.shutil, "which", lambda _name: "/usr/bin/curl")

    calls: List[Dict[str, Any]] = []

    def _fake_run(command, **kwargs):
        calls.append({"command": list(command), **kwargs})
        return _CurlResult()

    monkeypatch.setattr(functions.subprocess, "run", _fake_run)

    assert functions.send_message("fallback message", require_delivery=True) is True
    assert len(calls) == 1
    assert "text@-" in calls[0]["command"]
    assert calls[0]["input"] == "fallback message"


def test_auto_add_to_tier2_does_not_send_telegram_message(monkeypatch) -> None:
    sent_messages: List[str] = []

    monkeypatch.setattr(runtime, "_find_known_team_ids_by_name", lambda *_args, **_kwargs: set())
    monkeypatch.setattr(runtime, "_get_team_tier", lambda *_args, **_kwargs: 3)
    monkeypatch.setattr(runtime, "_append_team_to_tier2_file", lambda *_args, **_kwargs: (True, "astini+5"))
    monkeypatch.setattr(runtime, "send_message", lambda message, **_kwargs: sent_messages.append(str(message)))

    ok, resolved_team_id = runtime._ensure_known_team_or_add_to_tier2(
        team_ids=[10081431],
        team_name="Astini+5",
        match_url="dltv.org/matches/test-auto-tier2",
    )

    assert ok is True
    assert resolved_team_id == 10081431
    assert sent_messages == []


def test_send_message_uses_proxy_fallback_before_curl(tmp_path, monkeypatch) -> None:
    import functions
    monkeypatch.setattr(functions, "TELEGRAM_UPDATES_FETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(functions, "TELEGRAM_SUBSCRIBERS_STATE_PATH", tmp_path / "telegram_subscribers_state.json", raising=False)
    monkeypatch.setattr(functions, "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH", tmp_path / "legacy_telegram_subscribers_state.json", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_id", "100", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_ids", [], raising=False)
    monkeypatch.setattr(functions, "_vk_is_enabled", lambda: False)

    class _ProxyResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> Dict[str, Any]:
            return {"ok": True, "result": {"message_id": 2}}

    post_calls: List[Dict[str, Any]] = []

    def _fake_post(*_args, **kwargs):
        post_calls.append(dict(kwargs))
        if kwargs.get("proxies"):
            return _ProxyResponse()
        raise functions.requests.exceptions.ConnectionError(
            "SSLEOFError(8, 'EOF occurred in violation of protocol')"
        )

    monkeypatch.setattr(functions.requests, "post", _fake_post)
    monkeypatch.setattr(
        functions.keys,
        "TELEGRAM_PROXIES",
        {"http": "http://proxy.example:8080", "https": "http://proxy.example:8080"},
        raising=False,
    )
    monkeypatch.setattr(functions.subprocess, "run", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("curl should not be used")))

    assert functions.send_message("proxy message", require_delivery=True) is True
    assert len(post_calls) == 2
    assert post_calls[0].get("proxies") in (None, {})
    assert post_calls[1].get("proxies") == {
        "http": "http://proxy.example:8080",
        "https": "http://proxy.example:8080",
    }


def test_send_message_broadcasts_to_discovered_subscribers(tmp_path, monkeypatch) -> None:
    import functions

    state_path = tmp_path / "telegram_subscribers_state.json"
    legacy_path = tmp_path / "legacy_telegram_subscribers_state.json"
    monkeypatch.setattr(functions, "TELEGRAM_SUBSCRIBERS_STATE_PATH", state_path, raising=False)
    monkeypatch.setattr(functions, "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH", legacy_path, raising=False)
    monkeypatch.setattr(functions.keys, "Chat_id", "100", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_ids", [], raising=False)

    class _Response:
        def __init__(self, payload: Dict[str, Any]) -> None:
            self._payload = payload
            self.status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> Dict[str, Any]:
            return self._payload

    delivered: List[str] = []

    def _fake_post(url, **kwargs):
        if url.endswith("/getUpdates"):
            return _Response(
                {
                    "ok": True,
                    "result": [
                        {
                            "update_id": 10,
                            "message": {
                                "chat": {"id": 200},
                                "from": {"id": 200},
                            },
                        }
                    ],
                }
            )
        delivered.append(str(kwargs["json"]["chat_id"]))
        return _Response({"ok": True, "result": {"message_id": 1}})

    monkeypatch.setattr(functions.requests, "post", _fake_post)

    assert functions.send_message("broadcast", require_delivery=True) is True
    assert delivered == ["100", "200"]
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["chat_ids"] == ["100", "200"]
    assert state["last_update_id"] == 10


def test_send_message_mirrors_to_vk_once_per_broadcast(tmp_path, monkeypatch) -> None:
    import functions

    state_path = tmp_path / "telegram_subscribers_state.json"
    legacy_path = tmp_path / "legacy_telegram_subscribers_state.json"
    monkeypatch.setattr(functions, "TELEGRAM_SUBSCRIBERS_STATE_PATH", state_path, raising=False)
    monkeypatch.setattr(functions, "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH", legacy_path, raising=False)
    monkeypatch.setattr(functions.keys, "Chat_id", "100", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_ids", [], raising=False)
    monkeypatch.setattr(functions.keys, "VK_GROUP_TOKEN", "vk-token", raising=False)
    monkeypatch.setattr(functions.keys, "VK_GROUP_ID", "237301744", raising=False)
    monkeypatch.setattr(functions.keys, "VK_PEER_IDS", ["717099073"], raising=False)
    monkeypatch.setattr(functions.keys, "VK_PEER_ID", "717099073", raising=False)
    monkeypatch.setattr(functions.keys, "VK_API_VERSION", "5.199", raising=False)

    class _Response:
        def __init__(self, payload: Dict[str, Any]) -> None:
            self._payload = payload
            self.status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> Dict[str, Any]:
            return self._payload

    delivered_telegram: List[str] = []
    delivered_vk: List[Dict[str, Any]] = []

    def _fake_post(url, **kwargs):
        if url.endswith("/getUpdates"):
            return _Response(
                {
                    "ok": True,
                    "result": [
                        {
                            "update_id": 10,
                            "message": {
                                "chat": {"id": 200},
                                "from": {"id": 200},
                            },
                        }
                    ],
                }
            )
        if "api.vk.com/method/messages.send" in url:
            delivered_vk.append(dict(kwargs["data"]))
            return _Response({"response": 42})
        delivered_telegram.append(str(kwargs["json"]["chat_id"]))
        return _Response({"ok": True, "result": {"message_id": 1}})

    monkeypatch.setattr(functions.requests, "post", _fake_post)

    assert functions.send_message("broadcast", require_delivery=True) is True
    assert delivered_telegram == ["100", "200"]
    assert len(delivered_vk) == 1
    assert delivered_vk[0]["peer_id"] == "717099073"
    assert delivered_vk[0]["message"] == "broadcast"


def test_send_message_mirrors_to_all_vk_peer_ids(tmp_path, monkeypatch) -> None:
    import functions

    state_path = tmp_path / "telegram_subscribers_state.json"
    legacy_path = tmp_path / "legacy_telegram_subscribers_state.json"
    monkeypatch.setattr(functions, "TELEGRAM_SUBSCRIBERS_STATE_PATH", state_path, raising=False)
    monkeypatch.setattr(functions, "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH", legacy_path, raising=False)
    monkeypatch.setattr(functions.keys, "Chat_id", "100", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_ids", [], raising=False)
    monkeypatch.setattr(functions.keys, "VK_GROUP_TOKEN", "vk-token", raising=False)
    monkeypatch.setattr(functions.keys, "VK_GROUP_ID", "237301744", raising=False)
    monkeypatch.setattr(functions.keys, "VK_PEER_IDS", ["717099073", "64086675"], raising=False)
    monkeypatch.setattr(functions.keys, "VK_PEER_ID", "717099073", raising=False)
    monkeypatch.setattr(functions.keys, "VK_API_VERSION", "5.199", raising=False)

    class _Response:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> Dict[str, Any]:
            return {"response": 42}

    vk_peer_ids: List[str] = []

    def _fake_post(url, **kwargs):
        if "api.vk.com/method/messages.send" in url:
            vk_peer_ids.append(str(kwargs["data"]["peer_id"]))
            return _Response()
        return _Response()

    monkeypatch.setattr(functions.requests, "post", _fake_post)
    monkeypatch.setattr(functions, "_refresh_telegram_subscribers", lambda: [])

    assert functions.send_message("СТАВКА НА test x1\nmatch body", require_delivery=True) is True
    assert vk_peer_ids == ["717099073", "64086675"]


def test_send_message_non_bet_mirrors_only_to_primary_vk_peer(tmp_path, monkeypatch) -> None:
    import functions

    state_path = tmp_path / "telegram_subscribers_state.json"
    legacy_path = tmp_path / "legacy_telegram_subscribers_state.json"
    monkeypatch.setattr(functions, "TELEGRAM_SUBSCRIBERS_STATE_PATH", state_path, raising=False)
    monkeypatch.setattr(functions, "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH", legacy_path, raising=False)
    monkeypatch.setattr(functions.keys, "Chat_id", "100", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_ids", [], raising=False)
    monkeypatch.setattr(functions.keys, "VK_GROUP_TOKEN", "vk-token", raising=False)
    monkeypatch.setattr(functions.keys, "VK_GROUP_ID", "237301744", raising=False)
    monkeypatch.setattr(functions.keys, "VK_PEER_IDS", ["717099073", "64086675"], raising=False)
    monkeypatch.setattr(functions.keys, "VK_PEER_ID", "717099073", raising=False)
    monkeypatch.setattr(functions.keys, "VK_API_VERSION", "5.199", raising=False)

    class _Response:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> Dict[str, Any]:
            return {"response": 42}

    vk_peer_ids: List[str] = []

    def _fake_post(url, **kwargs):
        if "api.vk.com/method/messages.send" in url:
            vk_peer_ids.append(str(kwargs["data"]["peer_id"]))
            return _Response()
        return _Response()

    monkeypatch.setattr(functions.requests, "post", _fake_post)
    monkeypatch.setattr(functions, "_refresh_telegram_subscribers", lambda: [])

    assert functions.send_message("service notice", require_delivery=True) is True
    assert vk_peer_ids == ["717099073"]


def test_send_message_admin_only_mirrors_only_to_primary_vk_peer(tmp_path, monkeypatch) -> None:
    import functions

    state_path = tmp_path / "telegram_subscribers_state.json"
    legacy_path = tmp_path / "legacy_telegram_subscribers_state.json"
    monkeypatch.setattr(functions, "TELEGRAM_SUBSCRIBERS_STATE_PATH", state_path, raising=False)
    monkeypatch.setattr(functions, "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH", legacy_path, raising=False)
    monkeypatch.setattr(functions.keys, "Chat_id", "100", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_ids", [], raising=False)
    monkeypatch.setattr(functions.keys, "VK_GROUP_TOKEN", "vk-token", raising=False)
    monkeypatch.setattr(functions.keys, "VK_GROUP_ID", "237301744", raising=False)
    monkeypatch.setattr(functions.keys, "VK_PEER_IDS", ["717099073", "64086675"], raising=False)
    monkeypatch.setattr(functions.keys, "VK_PEER_ID", "717099073", raising=False)
    monkeypatch.setattr(functions.keys, "VK_API_VERSION", "5.199", raising=False)

    class _Response:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> Dict[str, Any]:
            return {"response": 42}

    vk_peer_ids: List[str] = []

    def _fake_post(url, **kwargs):
        if "api.vk.com/method/messages.send" in url:
            vk_peer_ids.append(str(kwargs["data"]["peer_id"]))
            return _Response()
        return _Response()

    monkeypatch.setattr(functions.requests, "post", _fake_post)

    assert functions.send_message("admin message", require_delivery=True, admin_only=True) is True
    assert vk_peer_ids == ["717099073"]


def test_send_message_can_skip_vk_mirror_for_admin_reply(tmp_path, monkeypatch) -> None:
    import functions

    state_path = tmp_path / "telegram_subscribers_state.json"
    legacy_path = tmp_path / "legacy_telegram_subscribers_state.json"
    state_path.write_text(
        json.dumps({"chat_ids": ["100"], "last_update_id": 0}),
        encoding="utf-8",
    )
    monkeypatch.setattr(functions, "TELEGRAM_SUBSCRIBERS_STATE_PATH", state_path, raising=False)
    monkeypatch.setattr(functions, "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH", legacy_path, raising=False)
    monkeypatch.setattr(functions.keys, "Chat_id", "100", raising=False)
    monkeypatch.setattr(functions.keys, "VK_GROUP_TOKEN", "vk-token", raising=False)
    monkeypatch.setattr(functions.keys, "VK_GROUP_ID", "237301744", raising=False)
    monkeypatch.setattr(functions.keys, "VK_PEER_IDS", ["717099073", "64086675"], raising=False)
    monkeypatch.setattr(functions.keys, "VK_PEER_ID", "717099073", raising=False)
    monkeypatch.setattr(functions.keys, "VK_API_VERSION", "5.199", raising=False)

    telegram_calls: List[Dict[str, Any]] = []
    vk_calls: List[Dict[str, Any]] = []

    class _Response:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> Dict[str, Any]:
            return {"ok": True, "result": {"message_id": 1}, "response": 1}

    def _fake_post(url, **kwargs):
        if "api.telegram.org" in url:
            telegram_calls.append(dict(kwargs))
            return _Response()
        if "api.vk.com/method/messages.send" in url:
            vk_calls.append(dict(kwargs))
            return _Response()
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(functions.requests, "post", _fake_post)

    assert functions.send_message("admin tail reply", admin_only=True, mirror_to_vk=False) is True
    assert len(telegram_calls) == 1
    assert vk_calls == []


def test_send_message_can_succeed_via_vk_when_telegram_fails(tmp_path, monkeypatch) -> None:
    import functions

    state_path = tmp_path / "telegram_subscribers_state.json"
    legacy_path = tmp_path / "legacy_telegram_subscribers_state.json"
    monkeypatch.setattr(functions, "TELEGRAM_SUBSCRIBERS_STATE_PATH", state_path, raising=False)
    monkeypatch.setattr(functions, "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH", legacy_path, raising=False)
    monkeypatch.setattr(functions.keys, "Chat_id", "100", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_ids", [], raising=False)
    monkeypatch.setattr(functions.keys, "VK_GROUP_TOKEN", "vk-token", raising=False)
    monkeypatch.setattr(functions.keys, "VK_GROUP_ID", "237301744", raising=False)
    monkeypatch.setattr(functions.keys, "VK_PEER_ID", "717099073", raising=False)

    class _Response:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> Dict[str, Any]:
            return {"response": 99}

    def _fake_post(url, **kwargs):
        if "api.vk.com/method/messages.send" in url:
            return _Response()
        raise functions.requests.exceptions.ConnectionError("telegram down")

    monkeypatch.setattr(functions.requests, "post", _fake_post)

    assert functions.send_message("vk fallback", require_delivery=True) is True


def test_send_message_admin_only_targets_primary_chat_only(tmp_path, monkeypatch) -> None:
    import functions

    state_path = tmp_path / "telegram_subscribers_state.json"
    legacy_path = tmp_path / "legacy_telegram_subscribers_state.json"
    state_path.write_text(
        json.dumps({"chat_ids": ["100", "200", "300"], "last_update_id": 0}),
        encoding="utf-8",
    )
    monkeypatch.setattr(functions, "TELEGRAM_SUBSCRIBERS_STATE_PATH", state_path, raising=False)
    monkeypatch.setattr(functions, "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH", legacy_path, raising=False)
    monkeypatch.setattr(functions.keys, "Chat_id", "100", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_ids", ["200", "300"], raising=False)

    class _Response:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> Dict[str, Any]:
            return {"ok": True, "result": {"message_id": 1}}

    delivered: List[str] = []
    reply_markups: List[dict] = []

    def _fake_post(_url, **kwargs):
        delivered.append(str(kwargs["json"]["chat_id"]))
        reply_markup = kwargs["json"].get("reply_markup")
        if isinstance(reply_markup, dict):
            reply_markups.append(reply_markup)
        return _Response()

    monkeypatch.setattr(functions.requests, "post", _fake_post)

    assert functions.send_message("admin message", require_delivery=True, admin_only=True) is True
    assert delivered == ["100"]
    assert len(reply_markups) == 1
    assert reply_markups[0]["keyboard"][0][0]["text"] == "tail_log"
    assert reply_markups[0]["keyboard"][0][1]["text"] == "reboot"


def test_telegram_proxy_fallback_does_not_reuse_bookmaker_proxy(monkeypatch) -> None:
    import functions

    monkeypatch.setattr(functions, "TELEGRAM_SEND_PROXY_FALLBACK_ENABLED", True, raising=False)
    monkeypatch.delattr(functions.keys, "TELEGRAM_PROXIES", raising=False)
    monkeypatch.setattr(
        functions.keys,
        "BOOKMAKER_PROXIES",
        {"http": "http://bookmaker-proxy", "https": "http://bookmaker-proxy"},
        raising=False,
    )

    assert functions._get_telegram_proxy_fallback() == {}


def test_drain_telegram_admin_commands_extracts_restart_command(tmp_path, monkeypatch) -> None:
    import functions

    state_path = tmp_path / "telegram_subscribers_state.json"
    legacy_path = tmp_path / "legacy_telegram_subscribers_state.json"
    monkeypatch.setattr(functions, "TELEGRAM_SUBSCRIBERS_STATE_PATH", state_path, raising=False)
    monkeypatch.setattr(functions, "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH", legacy_path, raising=False)
    monkeypatch.setattr(functions.keys, "Chat_id", "100", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_ids", [], raising=False)

    class _Response:
        status_code = 200

        def __init__(self, payload: Dict[str, Any]) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> Dict[str, Any]:
            return self._payload

    def _fake_post(url, **_kwargs):
        if url.endswith("/getUpdates"):
            return _Response(
                {
                    "ok": True,
                    "result": [
                        {
                            "update_id": 101,
                            "message": {
                                "chat": {"id": 100},
                                "from": {"id": 100},
                                "text": "/restart_bot",
                            },
                        }
                    ],
                }
            )
        return _Response({"ok": True, "result": {"message_id": 1}})

    with functions.TELEGRAM_ADMIN_COMMANDS_LOCK:
        functions.TELEGRAM_PENDING_ADMIN_COMMANDS.clear()

    monkeypatch.setattr(functions.requests, "post", _fake_post)

    commands = functions.drain_telegram_admin_commands(refresh=True)

    assert len(commands) == 1
    assert commands[0]["command"] == "restart_bot"
    assert commands[0]["chat_id"] == "100"


def test_drain_telegram_admin_commands_extracts_literal_tail_command(tmp_path, monkeypatch) -> None:
    import functions

    state_path = tmp_path / "telegram_subscribers_state.json"
    legacy_path = tmp_path / "legacy_telegram_subscribers_state.json"
    monkeypatch.setattr(functions, "TELEGRAM_SUBSCRIBERS_STATE_PATH", state_path, raising=False)
    monkeypatch.setattr(functions, "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH", legacy_path, raising=False)
    monkeypatch.setattr(functions.keys, "Chat_id", "100", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_ids", [], raising=False)

    class _Response:
        status_code = 200

        def __init__(self, payload: Dict[str, Any]) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> Dict[str, Any]:
            return self._payload

    def _fake_post(url, **_kwargs):
        if url.endswith("/getUpdates"):
            return _Response(
                {
                    "ok": True,
                    "result": [
                        {
                            "update_id": 102,
                            "message": {
                                "chat": {"id": 100},
                                "from": {"id": 100},
                                "text": "tail -n 100 log.txt",
                            },
                        }
                    ],
                }
            )
        return _Response({"ok": True, "result": {"message_id": 1}})

    with functions.TELEGRAM_ADMIN_COMMANDS_LOCK:
        functions.TELEGRAM_PENDING_ADMIN_COMMANDS.clear()

    monkeypatch.setattr(functions.requests, "post", _fake_post)

    commands = functions.drain_telegram_admin_commands(refresh=True)

    assert len(commands) == 1
    assert commands[0]["command"] == "tail_log_100"
    assert commands[0]["raw_text"] == "tail -n 100 log.txt"


def test_drain_telegram_admin_commands_extracts_plain_reboot_command(tmp_path, monkeypatch) -> None:
    import functions

    state_path = tmp_path / "telegram_subscribers_state.json"
    legacy_path = tmp_path / "legacy_telegram_subscribers_state.json"
    monkeypatch.setattr(functions, "TELEGRAM_SUBSCRIBERS_STATE_PATH", state_path, raising=False)
    monkeypatch.setattr(functions, "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH", legacy_path, raising=False)
    monkeypatch.setattr(functions.keys, "Chat_id", "100", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_ids", [], raising=False)

    class _Response:
        status_code = 200

        def __init__(self, payload: Dict[str, Any]) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> Dict[str, Any]:
            return self._payload

    def _fake_post(url, **_kwargs):
        if url.endswith("/getUpdates"):
            return _Response(
                {
                    "ok": True,
                    "result": [
                        {
                            "update_id": 103,
                            "message": {
                                "chat": {"id": 100},
                                "from": {"id": 100},
                                "text": "reboot",
                            },
                        }
                    ],
                }
            )
        return _Response({"ok": True, "result": {"message_id": 1}})

    with functions.TELEGRAM_ADMIN_COMMANDS_LOCK:
        functions.TELEGRAM_PENDING_ADMIN_COMMANDS.clear()

    monkeypatch.setattr(functions.requests, "post", _fake_post)

    commands = functions.drain_telegram_admin_commands(refresh=True)

    assert len(commands) == 1
    assert commands[0]["command"] == "restart_bot"


def test_build_recent_match_summaries_text_normalizes_map_suffix_in_urls(tmp_path, monkeypatch) -> None:
    log_path = tmp_path / "log.txt"
    log_path.write_text(
        "\n".join(
            [
                "🔍 DEBUG: Начало обработки матча #0",
                "   Статус: draft...",
                "   URL: dltv.org/matches/425878/1win-team-vs-enjoy-boys-blast-slam-vii-europe-open-qualifier-1.0",
                "   Score: 0 : 0",
                "   ✅ Драфт успешно распарсен",
                "   ⚠️ ВЕРДИКТ: ОТКАЗ (нет late star-сигнала) - матч пропущен",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(runtime, "PROJECT_ROOT", tmp_path, raising=False)

    payload = runtime._build_recent_match_summaries_text(limit=10)

    assert "dltv.org/matches/425878/1win-team-vs-enjoy-boys-blast-slam-vii-europe-open-qualifier-1.0" not in payload
    assert "dltv.org/matches/425878/1win-team-vs-enjoy-boys-blast-slam-vii-europe-open-qualifier-1" in payload


def _tail_entry(url: str, *lines: str, line_no: int = 1) -> Dict[str, Any]:
    return {
        "url": url,
        "lines": list(lines),
        "line_no": line_no,
    }


def test_send_admin_log_tail_sends_one_message_per_match(monkeypatch) -> None:
    entries = [
        _tail_entry(
            "dltv.org/matches/1/older-match.0",
            "   Статус: draft...",
            "   URL: dltv.org/matches/1/older-match",
            line_no=10,
        ),
        _tail_entry(
            "dltv.org/matches/2/newer-match.0",
            "   Статус: draft...",
            "   URL: dltv.org/matches/2/newer-match",
            line_no=20,
        ),
    ]
    sent_messages: List[Dict[str, Any]] = []
    requested_limits: List[int] = []
    saved_seen_urls: List[List[str]] = []

    def _fake_build_recent_match_summaries_entries(limit=10, scan_lines=12000):
        requested_limits.append(int(limit))
        return entries

    def _fake_send_message(message, **kwargs):
        sent_messages.append({"message": str(message), "kwargs": dict(kwargs)})

    monkeypatch.setattr(runtime, "_build_recent_match_summaries_entries", _fake_build_recent_match_summaries_entries)
    monkeypatch.setattr(runtime, "_admin_tail_current_live_map_urls", lambda: set())
    monkeypatch.setattr(runtime, "send_message", _fake_send_message)
    monkeypatch.setattr(runtime, "_load_admin_tail_log_seen_urls", lambda **_kwargs: [])
    monkeypatch.setattr(
        runtime,
        "_save_admin_tail_log_seen_urls",
        lambda urls, **_kwargs: saved_seen_urls.append(list(urls)),
    )

    runtime._send_admin_log_tail(line_count=100, raw_odds=False)

    assert requested_limits == [runtime._ADMIN_TAIL_LOG_RECENT_MATCH_SCAN_LIMIT]
    assert len(sent_messages) == 2
    assert "dltv.org/matches/2/newer-match" in sent_messages[0]["message"]
    assert "dltv.org/matches/1/older-match" in sent_messages[1]["message"]
    assert sent_messages[0]["kwargs"]["admin_only"] is True
    assert sent_messages[0]["kwargs"]["mirror_to_vk"] is False
    assert saved_seen_urls == [["dltv.org/matches/2/newer-match.0", "dltv.org/matches/1/older-match.0"]]


def test_send_admin_log_tail_skips_seen_matches_and_only_sends_new(monkeypatch) -> None:
    entries = [
        _tail_entry(
            "dltv.org/matches/1/already-seen.0",
            "   Статус: draft...",
            "   URL: dltv.org/matches/1/already-seen",
            line_no=10,
        ),
        _tail_entry(
            "dltv.org/matches/2/new-match.0",
            "   Статус: draft...",
            "   URL: dltv.org/matches/2/new-match",
            line_no=20,
        ),
    ]
    sent_messages: List[Dict[str, Any]] = []
    saved_seen_urls: List[List[str]] = []

    monkeypatch.setattr(
        runtime,
        "_build_recent_match_summaries_entries",
        lambda **_kwargs: entries,
    )
    monkeypatch.setattr(runtime, "_admin_tail_current_live_map_urls", lambda: set())
    monkeypatch.setattr(
        runtime,
        "_load_admin_tail_log_seen_urls",
        lambda **_kwargs: ["dltv.org/matches/1/already-seen.0"],
    )
    monkeypatch.setattr(
        runtime,
        "_save_admin_tail_log_seen_urls",
        lambda urls, **_kwargs: saved_seen_urls.append(list(urls)),
    )
    monkeypatch.setattr(
        runtime,
        "send_message",
        lambda message, **kwargs: sent_messages.append({"message": str(message), "kwargs": dict(kwargs)}),
    )

    runtime._send_admin_log_tail(line_count=100, raw_odds=False)

    assert len(sent_messages) == 1
    assert "dltv.org/matches/2/new-match" in sent_messages[0]["message"]
    assert "dltv.org/matches/1/already-seen" not in sent_messages[0]["message"]
    assert saved_seen_urls == [["dltv.org/matches/1/already-seen.0", "dltv.org/matches/2/new-match.0"]]


def test_send_admin_log_tail_reports_no_new_matches(monkeypatch) -> None:
    entries = [
        _tail_entry(
            "dltv.org/matches/1/already-seen.0",
            "   Статус: draft...",
            "   URL: dltv.org/matches/1/already-seen",
            line_no=10,
        )
    ]
    sent_messages: List[Dict[str, Any]] = []

    monkeypatch.setattr(
        runtime,
        "_build_recent_match_summaries_entries",
        lambda **_kwargs: entries,
    )
    monkeypatch.setattr(runtime, "_admin_tail_current_live_map_urls", lambda: set())
    monkeypatch.setattr(
        runtime,
        "_load_admin_tail_log_seen_urls",
        lambda **_kwargs: ["dltv.org/matches/1/already-seen.0"],
    )
    monkeypatch.setattr(
        runtime,
        "_save_admin_tail_log_seen_urls",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("seen-state must not be rewritten")),
    )
    monkeypatch.setattr(
        runtime,
        "send_message",
        lambda message, **kwargs: sent_messages.append({"message": str(message), "kwargs": dict(kwargs)}),
    )

    runtime._send_admin_log_tail(line_count=100, raw_odds=False)

    assert sent_messages == [
        {
            "message": "tail_log: новых ставок нет",
            "kwargs": {"admin_only": True, "mirror_to_vk": False},
        }
    ]


def test_send_admin_log_tail_prefers_send_limit_freshest_unseen_matches(monkeypatch) -> None:
    entries = [
        _tail_entry(f"dltv.org/matches/{idx}/match-{idx}.0", "   Статус: draft...", f"   URL: dltv.org/matches/{idx}/match-{idx}", line_no=idx)
        for idx in range(1, 6)
    ]
    sent_messages: List[Dict[str, Any]] = []
    saved_seen_urls: List[List[str]] = []

    monkeypatch.setattr(
        runtime,
        "_build_recent_match_summaries_entries",
        lambda **_kwargs: entries,
    )
    monkeypatch.setattr(runtime, "_admin_tail_current_live_map_urls", lambda: set())
    monkeypatch.setattr(runtime, "_load_admin_tail_log_seen_urls", lambda **_kwargs: [])
    monkeypatch.setattr(
        runtime,
        "_save_admin_tail_log_seen_urls",
        lambda urls, **_kwargs: saved_seen_urls.append(list(urls)),
    )
    monkeypatch.setattr(
        runtime,
        "send_message",
        lambda message, **kwargs: sent_messages.append({"message": str(message), "kwargs": dict(kwargs)}),
    )

    runtime._send_admin_log_tail(line_count=100, raw_odds=False)

    assert len(sent_messages) == runtime._ADMIN_TAIL_LOG_SEND_LIMIT
    assert "dltv.org/matches/5/match-5" in sent_messages[0]["message"]
    assert "dltv.org/matches/4/match-4" in sent_messages[1]["message"]
    assert "dltv.org/matches/3/match-3" in sent_messages[2]["message"]
    assert "dltv.org/matches/2/match-2" in sent_messages[3]["message"]


def test_send_admin_log_tail_expands_window_until_three_unseen_found(monkeypatch) -> None:
    entries_small = [
        _tail_entry("dltv.org/matches/10/seen-a.0", "   Статус: draft...", "   URL: dltv.org/matches/10/seen-a", line_no=10),
        _tail_entry("dltv.org/matches/11/seen-b.0", "   Статус: draft...", "   URL: dltv.org/matches/11/seen-b", line_no=11),
        _tail_entry("dltv.org/matches/12/new-c.0", "   Статус: draft...", "   URL: dltv.org/matches/12/new-c", line_no=12),
    ]
    entries_large = [
        _tail_entry("dltv.org/matches/8/new-a.0", "   Статус: draft...", "   URL: dltv.org/matches/8/new-a", line_no=8),
        _tail_entry("dltv.org/matches/9/new-b.0", "   Статус: draft...", "   URL: dltv.org/matches/9/new-b", line_no=9),
        _tail_entry("dltv.org/matches/10/seen-a.0", "   Статус: draft...", "   URL: dltv.org/matches/10/seen-a", line_no=10),
        _tail_entry("dltv.org/matches/11/seen-b.0", "   Статус: draft...", "   URL: dltv.org/matches/11/seen-b", line_no=11),
        _tail_entry("dltv.org/matches/12/new-c.0", "   Статус: draft...", "   URL: dltv.org/matches/12/new-c", line_no=12),
    ]
    requested_limits: List[int] = []
    sent_messages: List[Dict[str, Any]] = []
    saved_seen_urls: List[List[str]] = []

    def _fake_build_recent_match_summaries_entries(limit=10, scan_lines=12000):
        requested_limits.append(int(limit))
        if int(limit) <= runtime._ADMIN_TAIL_LOG_RECENT_MATCH_SCAN_LIMIT:
            return entries_small
        return entries_large

    monkeypatch.setattr(runtime, "_build_recent_match_summaries_entries", _fake_build_recent_match_summaries_entries)
    monkeypatch.setattr(runtime, "_admin_tail_current_live_map_urls", lambda: set())
    monkeypatch.setattr(
        runtime,
        "_load_admin_tail_log_seen_urls",
        lambda **_kwargs: ["dltv.org/matches/10/seen-a.0", "dltv.org/matches/11/seen-b.0"],
    )
    monkeypatch.setattr(
        runtime,
        "_save_admin_tail_log_seen_urls",
        lambda urls, **_kwargs: saved_seen_urls.append(list(urls)),
    )
    monkeypatch.setattr(
        runtime,
        "send_message",
        lambda message, **kwargs: sent_messages.append({"message": str(message), "kwargs": dict(kwargs)}),
    )

    runtime._send_admin_log_tail(line_count=100, raw_odds=False)

    assert requested_limits[:2] == [runtime._ADMIN_TAIL_LOG_RECENT_MATCH_SCAN_LIMIT, runtime._ADMIN_TAIL_LOG_RECENT_MATCH_SCAN_LIMIT * 2]
    assert len(sent_messages) == 3
    assert "dltv.org/matches/12/new-c" in sent_messages[0]["message"]
    assert "dltv.org/matches/9/new-b" in sent_messages[1]["message"]
    assert "dltv.org/matches/8/new-a" in sent_messages[2]["message"]
    assert saved_seen_urls == [[
        "dltv.org/matches/10/seen-a.0",
        "dltv.org/matches/11/seen-b.0",
        "dltv.org/matches/12/new-c.0",
        "dltv.org/matches/9/new-b.0",
        "dltv.org/matches/8/new-a.0",
    ]]


def test_send_admin_log_tail_keeps_distinct_series_maps_unseen(monkeypatch) -> None:
    entries = [
        _tail_entry(
            "dltv.org/matches/425854/pipsqueak4-vs-virtuspro-premier-series.0",
            "   Статус: finished",
            "   URL: dltv.org/matches/425854/pipsqueak4-vs-virtuspro-premier-series",
            "   Score: 1 : 0",
            line_no=10,
        ),
        _tail_entry(
            "dltv.org/matches/425854/pipsqueak4-vs-virtuspro-premier-series.1",
            "   Статус: finished",
            "   URL: dltv.org/matches/425854/pipsqueak4-vs-virtuspro-premier-series",
            "   Score: 1 : 1",
            line_no=20,
        ),
        _tail_entry(
            "dltv.org/matches/425854/pipsqueak4-vs-virtuspro-premier-series.2",
            "   Статус: finished",
            "   URL: dltv.org/matches/425854/pipsqueak4-vs-virtuspro-premier-series",
            "   Score: 2 : 1",
            line_no=30,
        ),
    ]
    sent_messages: List[Dict[str, Any]] = []
    saved_seen_urls: List[List[str]] = []

    monkeypatch.setattr(runtime, "_build_recent_match_summaries_entries", lambda **_kwargs: entries)
    monkeypatch.setattr(runtime, "_admin_tail_current_live_map_urls", lambda: set())
    monkeypatch.setattr(
        runtime,
        "_load_admin_tail_log_seen_urls",
        lambda **_kwargs: ["dltv.org/matches/425854/pipsqueak4-vs-virtuspro-premier-series.0"],
    )
    monkeypatch.setattr(
        runtime,
        "_save_admin_tail_log_seen_urls",
        lambda urls, **_kwargs: saved_seen_urls.append(list(urls)),
    )
    monkeypatch.setattr(
        runtime,
        "send_message",
        lambda message, **kwargs: sent_messages.append({"message": str(message), "kwargs": dict(kwargs)}),
    )

    runtime._send_admin_log_tail(line_count=100, raw_odds=False)

    assert len(sent_messages) == 2
    assert "Score: 2 : 1" in sent_messages[0]["message"]
    assert "Score: 1 : 1" in sent_messages[1]["message"]
    assert saved_seen_urls == [[
        "dltv.org/matches/425854/pipsqueak4-vs-virtuspro-premier-series.0",
        "dltv.org/matches/425854/pipsqueak4-vs-virtuspro-premier-series.2",
        "dltv.org/matches/425854/pipsqueak4-vs-virtuspro-premier-series.1",
    ]]


def test_load_telegram_subscribers_state_merges_primary_and_legacy(tmp_path, monkeypatch) -> None:
    import functions

    state_path = tmp_path / "telegram_subscribers_state.json"
    legacy_path = tmp_path / "legacy_telegram_subscribers_state.json"
    state_path.write_text(
        json.dumps({"chat_ids": ["100"], "last_update_id": 10}),
        encoding="utf-8",
    )
    legacy_path.write_text(
        json.dumps({"chat_ids": ["200", "100"], "last_update_id": 7}),
        encoding="utf-8",
    )
    monkeypatch.setattr(functions, "TELEGRAM_SUBSCRIBERS_STATE_PATH", state_path, raising=False)
    monkeypatch.setattr(functions, "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH", legacy_path, raising=False)
    monkeypatch.setattr(functions.keys, "Chat_id", "100", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_ids", [], raising=False)

    state = functions._load_telegram_subscribers_state()

    assert state["chat_ids"] == ["100", "200"]
    assert state["last_update_id"] == 10
    assert state["_needs_persist"] is True

    functions._save_telegram_subscribers_state(state)

    saved_primary = json.loads(state_path.read_text(encoding="utf-8"))
    saved_legacy = json.loads(legacy_path.read_text(encoding="utf-8"))
    assert saved_primary == {"chat_ids": ["100", "200"], "last_update_id": 10}
    assert saved_legacy == saved_primary


def test_send_message_removes_blocked_subscriber(tmp_path, monkeypatch) -> None:
    import functions

    state_path = tmp_path / "telegram_subscribers_state.json"
    legacy_path = tmp_path / "legacy_telegram_subscribers_state.json"
    state_path.write_text(
        json.dumps({"chat_ids": ["100", "200"], "last_update_id": 0}),
        encoding="utf-8",
    )
    monkeypatch.setattr(functions, "TELEGRAM_SUBSCRIBERS_STATE_PATH", state_path, raising=False)
    monkeypatch.setattr(functions, "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH", legacy_path, raising=False)
    monkeypatch.setattr(functions.keys, "Chat_id", "100", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_ids", [], raising=False)

    class _Response:
        def __init__(self, payload: Dict[str, Any]) -> None:
            self._payload = payload
            self.status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> Dict[str, Any]:
            return self._payload

    def _fake_post(url, **kwargs):
        if url.endswith("/getUpdates"):
            return _Response({"ok": True, "result": []})
        chat_id = str(kwargs["json"]["chat_id"])
        if chat_id == "200":
            return _Response({"ok": False, "description": "bot was blocked by the user"})
        return _Response({"ok": True, "result": {"message_id": 1}})

    monkeypatch.setattr(functions.requests, "post", _fake_post)

    assert functions.send_message("broadcast", require_delivery=True) is True
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["chat_ids"] == ["100"]


def test_get_id_to_names_path_uses_runtime_base_dir(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(runtime, "BASE_DIR", tmp_path, raising=False)
    assert runtime._get_id_to_names_path() == tmp_path / "id_to_names.py"


def test_send_message_keeps_uncertain_when_curl_fallback_fails(monkeypatch) -> None:
    import functions

    class _CurlResult:
        returncode = 35
        stdout = ""
        stderr = "OpenSSL SSL_connect: SSL_ERROR_SYSCALL"

    monkeypatch.setattr(
        functions.requests,
        "post",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            functions.requests.exceptions.ConnectionError(
                "SSLEOFError(8, 'EOF occurred in violation of protocol')"
            )
        ),
    )
    monkeypatch.setattr(functions.shutil, "which", lambda _name: "/usr/bin/curl")
    monkeypatch.setattr(functions.subprocess, "run", lambda *_args, **_kwargs: _CurlResult())

    with pytest.raises(functions.TelegramSendError) as exc_info:
        functions.send_message("fallback message", require_delivery=True)

    assert exc_info.value.delivery_uncertain is True


def test_deliver_and_persist_signal_does_not_persist_when_send_fails(tmp_path, monkeypatch) -> None:
    journal_path = tmp_path / "sent_signal_recovery.jsonl"
    add_url_calls: List[Dict[str, Any]] = []

    monkeypatch.setattr(runtime, "SENT_SIGNAL_JOURNAL_PATH", str(journal_path), raising=False)
    monkeypatch.setattr(
        runtime,
        "send_message",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("telegram down")),
    )

    def _record_add_url(url: str, reason: str = "unspecified", details: Any = None) -> None:
        add_url_calls.append({"url": url, "reason": reason, "details": details})

    monkeypatch.setattr(runtime, "add_url", _record_add_url)

    with pytest.raises(RuntimeError):
        runtime._deliver_and_persist_signal(
            "dltv.org/matches/test-send-fail.0",
            "message",
            add_url_reason="unit_test_send_fail",
            add_url_details={"status": "draft..."},
        )

    assert add_url_calls == []
    assert not journal_path.exists()


def test_deliver_and_persist_signal_journals_after_persist_failure(tmp_path, monkeypatch) -> None:
    journal_path = tmp_path / "sent_signal_recovery.jsonl"
    monkeypatch.setattr(runtime, "SENT_SIGNAL_JOURNAL_PATH", str(journal_path), raising=False)
    monkeypatch.setattr(runtime, "send_message", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "add_url", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("disk full")))
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)

    with runtime.processed_urls_lock:
        runtime.processed_urls_cache.clear()

    runtime._deliver_and_persist_signal(
        "dltv.org/matches/test-journal.0",
        "message",
        add_url_reason="unit_test_journal",
        add_url_details={"status": "draft..."},
    )

    assert journal_path.exists()
    journal_lines = [line for line in journal_path.read_bytes().splitlines() if line.strip()]
    assert len(journal_lines) == 1
    payload = orjson.loads(journal_lines[0])
    assert payload["url"] == "dltv.org/matches/test-journal.0"
    assert payload["reason"] == "unit_test_journal"
    assert payload["details"]["persist_error"] == "disk full"
    with runtime.processed_urls_lock:
        assert "dltv.org/matches/test-journal.0" in runtime.processed_urls_cache


def test_deliver_and_persist_signal_uses_fallback_journal_when_primary_unavailable(tmp_path, monkeypatch) -> None:
    primary_path = tmp_path / "sent_signal_recovery.jsonl"
    fallback_path = tmp_path / "sent_signal_recovery_fallback.jsonl"
    monkeypatch.setattr(runtime, "SENT_SIGNAL_JOURNAL_PATH", str(primary_path), raising=False)
    monkeypatch.setattr(runtime, "SENT_SIGNAL_JOURNAL_FALLBACK_PATH", str(fallback_path), raising=False)
    monkeypatch.setattr(runtime, "send_message", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "add_url", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("disk full")))
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)

    original_append = runtime._append_journal_entry_to_path

    def _append_with_primary_failure(path: Path, entry: Dict[str, Any]) -> None:
        if path == primary_path:
            raise OSError("primary journal unavailable")
        original_append(path, entry)

    monkeypatch.setattr(runtime, "_append_journal_entry_to_path", _append_with_primary_failure)

    runtime._deliver_and_persist_signal(
        "dltv.org/matches/test-fallback-journal.0",
        "message",
        add_url_reason="unit_test_fallback_journal",
        add_url_details={"status": "draft..."},
    )

    assert not primary_path.exists()
    fallback_lines = [line for line in fallback_path.read_bytes().splitlines() if line.strip()]
    assert len(fallback_lines) == 1
    payload = orjson.loads(fallback_lines[0])
    assert payload["url"] == "dltv.org/matches/test-fallback-journal.0"


def test_safe_flush_sent_signal_journal_into_map_id_check_swallows_exception(monkeypatch) -> None:
    monkeypatch.setattr(
        runtime,
        "_flush_sent_signal_journal_into_map_id_check",
        lambda: (_ for _ in ()).throw(RuntimeError("flush broken")),
    )

    assert runtime._safe_flush_sent_signal_journal_into_map_id_check() == 0


def test_try_acquire_runtime_instance_lock_rejects_busy_lock(tmp_path, monkeypatch) -> None:
    class _BusyFcntl:
        LOCK_EX = 1
        LOCK_NB = 2
        LOCK_UN = 8

        @staticmethod
        def flock(_fd: int, _mode: int) -> None:
            raise OSError("lock busy")

    monkeypatch.setattr(runtime, "RUNTIME_INSTANCE_LOCK_PATH", str(tmp_path / "runtime.lock"), raising=False)
    monkeypatch.setattr(runtime, "fcntl", _BusyFcntl(), raising=False)
    monkeypatch.setattr(runtime, "runtime_instance_lock_handle", None, raising=False)

    assert runtime._try_acquire_runtime_instance_lock(mode_label="no_odds") is False


def test_runtime_instance_lock_path_is_split_by_mode(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(runtime, "RUNTIME_INSTANCE_LOCK_PATH", str(tmp_path / "runtime.lock"), raising=False)

    no_odds_lock = runtime._runtime_instance_lock_path_for_mode("no_odds")
    odds_lock = runtime._runtime_instance_lock_path_for_mode("odds")

    assert no_odds_lock.name == "runtime.no_odds.lock"
    assert odds_lock.name == "runtime.odds.lock"
    assert no_odds_lock != odds_lock


def test_delayed_queue_path_is_split_by_mode(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(tmp_path / "delayed_signal_queue.json"), raising=False)

    no_odds_queue = runtime._delayed_queue_path_for_mode("no_odds")
    odds_queue = runtime._delayed_queue_path_for_mode("odds")

    assert no_odds_queue.name == "delayed_signal_queue.no_odds.json"
    assert odds_queue.name == "delayed_signal_queue.odds.json"
    assert no_odds_queue != odds_queue


def test_set_delayed_match_persists_and_restores_queue(tmp_path, monkeypatch) -> None:
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()

    runtime._set_delayed_match(
        "dltv.org/matches/test-delayed.0",
        {
            "message": "payload",
            "reason": "late_only",
            "json_url": "https://dltv.org/live/test.json",
            "target_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "queued_at": 100.0,
            "queued_game_time": 700.0,
            "last_game_time": 700.0,
            "last_progress_at": 100.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {},
            "fallback_send_status_label": "late_fallback_20_20_send",
            "allow_live_recheck": False,
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
        },
    )

    restored = runtime._load_delayed_queue_state(recover=True)

    assert delayed_queue_path.exists()
    assert "dltv.org/matches/test-delayed.0" in restored
    assert restored["dltv.org/matches/test-delayed.0"]["target_game_time"] == float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME)


def test_deliver_and_persist_signal_records_uncertain_delivery_and_blocks_retries(tmp_path, monkeypatch) -> None:
    uncertain_path = tmp_path / "uncertain_signal_delivery.jsonl"
    fallback_path = tmp_path / "uncertain_signal_delivery_fallback.jsonl"
    monkeypatch.setattr(runtime, "UNCERTAIN_SIGNAL_DELIVERY_PATH", str(uncertain_path), raising=False)
    monkeypatch.setattr(runtime, "UNCERTAIN_SIGNAL_DELIVERY_FALLBACK_PATH", str(fallback_path), raising=False)
    monkeypatch.setattr(
        runtime,
        "send_message",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            runtime.TelegramSendError("read timeout", delivery_uncertain=True)
        ),
    )

    add_url_calls: List[str] = []
    monkeypatch.setattr(runtime, "add_url", lambda url, **_kwargs: add_url_calls.append(url))

    with runtime.uncertain_delivery_urls_lock:
        runtime.uncertain_delivery_urls_cache.clear()
    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
        runtime.monitored_matches["dltv.org/matches/test-uncertain.0"] = {"message": "queued"}

    delivered = runtime._deliver_and_persist_signal(
        "dltv.org/matches/test-uncertain.0",
        "message",
        add_url_reason="unit_test_uncertain",
        add_url_details={"status": "draft..."},
    )

    assert delivered is False
    assert add_url_calls == []
    assert uncertain_path.exists()
    lines = [line for line in uncertain_path.read_bytes().splitlines() if line.strip()]
    assert len(lines) == 1
    payload = orjson.loads(lines[0])
    assert payload["url"] == "dltv.org/matches/test-uncertain.0"
    assert runtime._is_url_uncertain_delivery("dltv.org/matches/test-uncertain.0") is True
    with runtime.monitored_matches_lock:
        assert "dltv.org/matches/test-uncertain.0" not in runtime.monitored_matches


def test_get_heads_sets_missing_live_matches_reason_without_telegram(monkeypatch) -> None:
    send_calls: List[str] = []

    monkeypatch.setattr(runtime, "USE_PROXY", True, raising=False)
    monkeypatch.setattr(runtime, "PROXY_LIST", ["proxy-a", "proxy-b"], raising=False)
    monkeypatch.setattr(runtime, "CURRENT_PROXY_INDEX", 0, raising=False)
    monkeypatch.setattr(runtime, "CURRENT_PROXY", "proxy-a", raising=False)
    monkeypatch.setattr(runtime, "PROXIES", {"http": "proxy-a", "https": "proxy-a"}, raising=False)
    monkeypatch.setattr(runtime.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "send_message", lambda message, **_kwargs: send_calls.append(str(message)))

    def _fake_retry(*_args, **_kwargs):
        return _FakeTextResponse("")

    monkeypatch.setattr(runtime, "make_request_with_retry", _fake_retry)
    monkeypatch.setattr(runtime.requests, "get", lambda *_args, **_kwargs: _FakeTextResponse(""))

    heads, bodies = runtime.get_heads(
        response=_FakeTextResponse("")
    )

    assert heads is None and bodies is None
    assert send_calls == [
        "⚠️ Все прокси для live matches исчерпаны после 3 кругов. Переключаюсь на direct fallback."
    ]
    assert (
        runtime.GET_HEADS_LAST_FAILURE_REASON
        == runtime.GET_HEADS_FAILURE_REASON_LIVE_MATCHES_MISSING_ALL_PROXIES
    )


def test_get_heads_uses_direct_fallback_after_proxy_pool_exhaustion(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "USE_PROXY", True, raising=False)
    monkeypatch.setattr(runtime, "PROXY_LIST", ["proxy-a", "proxy-b"], raising=False)
    monkeypatch.setattr(runtime, "CURRENT_PROXY_INDEX", 0, raising=False)
    monkeypatch.setattr(runtime, "CURRENT_PROXY", "proxy-a", raising=False)
    monkeypatch.setattr(runtime, "PROXIES", {"http": "proxy-a", "https": "proxy-a"}, raising=False)
    monkeypatch.setattr(runtime, "PROXY_POOL_ROTATION_ROUNDS", 3, raising=False)
    monkeypatch.setattr(runtime, "PROXY_POOL_DIRECT_FALLBACK_ALERT_ACTIVE", False, raising=False)
    monkeypatch.setattr(runtime.time, "sleep", lambda *_args, **_kwargs: None)

    retry_calls = {"count": 0}

    def _fake_retry(*_args, **_kwargs):
        retry_calls["count"] += 1
        return _FakeTextResponse("")

    monkeypatch.setattr(
        runtime,
        "make_request_with_retry",
        _fake_retry,
    )

    direct_calls: List[Dict[str, Any]] = []
    send_calls: List[str] = []
    monkeypatch.setattr(runtime, "send_message", lambda message, **_kwargs: send_calls.append(str(message)))

    def _direct_get(*_args, **kwargs):
        direct_calls.append(dict(kwargs))
        return _FakeTextResponse(
            """
            <html>
              <div class="live__matches">
                <div class="live__matches-item__head">
                  <div class="event__name"><div>ESL One Birmingham 2026</div></div>
                </div>
                <div class="live__matches-item__body">
                  <a href="/matches/test-direct-fallback"></a>
                </div>
              </div>
            </html>
            """,
            status_code=200,
        )

    monkeypatch.setattr(runtime, "_perform_http_get", _direct_get)

    heads, bodies = runtime.get_heads(
        response=_FakeTextResponse("")
    )

    assert heads is not None and len(heads) == 1
    assert bodies is not None and len(bodies) == 1
    assert retry_calls["count"] == 5
    assert len(direct_calls) >= 1
    assert any("proxies" not in call for call in direct_calls)
    assert send_calls == [
        "⚠️ Все прокси для live matches исчерпаны после 3 кругов. Переключаюсь на direct fallback."
    ]


def test_get_heads_switches_to_schedule_mode_when_live_block_missing(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "USE_PROXY", True, raising=False)
    monkeypatch.setattr(runtime, "PROXY_LIST", ["proxy-a", "proxy-b"], raising=False)
    monkeypatch.setattr(runtime, "CURRENT_PROXY_INDEX", 0, raising=False)
    monkeypatch.setattr(runtime, "CURRENT_PROXY", "proxy-a", raising=False)
    monkeypatch.setattr(runtime, "PROXIES", {"http": "proxy-a", "https": "proxy-a"}, raising=False)
    monkeypatch.setattr(runtime, "PROXY_POOL_ROTATION_ROUNDS", 3, raising=False)
    monkeypatch.setattr(runtime, "PROXY_POOL_DIRECT_FALLBACK_ALERT_ACTIVE", False, raising=False)
    monkeypatch.setattr(runtime.time, "sleep", lambda *_args, **_kwargs: None)

    retry_calls: List[str] = []
    monkeypatch.setattr(
        runtime,
        "make_request_with_retry",
        lambda *_args, **_kwargs: retry_calls.append("retry") or _FakeTextResponse(""),
    )

    send_calls: List[str] = []
    monkeypatch.setattr(runtime, "send_message", lambda message, **_kwargs: send_calls.append(str(message)))
    direct_calls: List[str] = []
    monkeypatch.setattr(
        runtime.requests,
        "get",
        lambda *_args, **_kwargs: direct_calls.append("direct") or _FakeTextResponse(""),
    )

    heads, bodies = runtime.get_heads(
        response=_FakeTextResponse(
            """
            <html>
              <head><title>Dota 2 Matches & livescore – DLTV</title></head>
              <div class="match upcoming" data-matches-odd="2026-04-01 11:00:00" data-series-id="425790">
                <div class="match__head">
                  <div class="match__head-event"><span>Fonbet Media Eleague Season 4</span></div>
                </div>
                <div class="match__body">
                  <div class="match__body-details">
                    <a href="https://dltv.org/matches/425790/team-rostikfacekid-vs-team-lens-fonbet-media-eleague-season-4"></a>
                    <div class="match__body-details__team">
                      <div class="team"><div class="team__title"><span>Team RostikFaceKid</span></div></div>
                      <div class="team"><div class="team__title"><span>Team Lens</span></div></div>
                    </div>
                  </div>
                </div>
              </div>
            </html>
            """
        )
    )

    assert heads == []
    assert bodies == []
    assert runtime.GET_HEADS_LAST_FAILURE_REASON is None
    assert runtime.NEXT_SCHEDULE_MATCH_INFO is not None
    assert runtime.NEXT_SCHEDULE_MATCH_INFO["league_title"] == "Fonbet Media Eleague Season 4"
    assert runtime.NEXT_SCHEDULE_MATCH_INFO["matchup"] == "Team RostikFaceKid vs Team Lens"
    assert send_calls == []
    assert retry_calls == []
    assert direct_calls == []


def test_get_heads_supports_new_live_match_card_layout(monkeypatch) -> None:
    html = """
    <html>
      <head><title>Dota 2 Matches & livescore – DLTV</title></head>
      <div class="match live" data-series-id="425877" data-match="8751684122">
        <div class="match__head">
          <div class="match__head-event"><span>ESL One Birmingham 2026</span></div>
        </div>
        <div class="match__body">
          <div class="match__body-details">
            <div class="match__body-details__team">
              <div class="team"><div class="team__title"><span>Xtreme Gaming</span></div></div>
            </div>
            <div class="match__body-details__score">
              <div class="score"><strong class="text-red">10</strong><small>(0)</small></div>
              <div class="duration"><div class="duration__time"><strong>draft...</strong></div></div>
              <div class="score"><strong class="text-red">12</strong><small>(0)</small></div>
            </div>
            <div class="match__body-details__team">
              <div class="team"><div class="team__title"><span>Team Yandex</span></div></div>
            </div>
          </div>
        </div>
      </div>
    </html>
    """

    heads, bodies = runtime.get_heads(response=_FakeTextResponse(html))

    assert heads is not None and len(heads) == 1
    assert bodies is not None and len(bodies) == 1
    listing = runtime._extract_live_listing_context(heads[0], bodies[0])
    assert listing["layout"] == "match_card_v2"
    assert listing["status"] == "draft..."
    assert listing["score"] == "0 : 0"
    assert listing["series_id"] == "425877"
    assert listing["live_match_id"] == "8751684122"


def test_get_heads_falls_back_to_v2_cards_inside_live_matches_wrapper(monkeypatch) -> None:
    html = """
    <html>
      <head><title>Dota 2 Matches & livescore – DLTV</title></head>
      <div class="live__matches">
        <div class="match live" data-series-id="425877" data-match="8751684122">
          <div class="match__head">
            <div class="match__head-event"><span>ESL One Birmingham 2026</span></div>
          </div>
          <div class="match__body">
            <div class="match__body-details">
              <div class="match__body-details__team">
                <div class="team"><div class="team__title"><span>Xtreme Gaming</span></div></div>
              </div>
              <div class="match__body-details__score">
                <div class="score"><strong class="text-red">10</strong><small>(0)</small></div>
                <div class="duration"><div class="duration__time"><strong>50:47</strong></div></div>
                <div class="score"><strong class="text-red">12</strong><small>(0)</small></div>
              </div>
              <div class="match__body-details__team">
                <div class="team"><div class="team__title"><span>Team Yandex</span></div></div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </html>
    """

    heads, bodies = runtime.get_heads(response=_FakeTextResponse(html))

    assert heads is not None and len(heads) == 1
    assert bodies is not None and len(bodies) == 1
    listing = runtime._extract_live_listing_context(heads[0], bodies[0])
    assert listing["layout"] == "match_card_v2"
    assert listing["status"] == "50:47"
    assert listing["score"] == "0 : 0"
    assert listing["series_id"] == "425877"
    assert listing["live_match_id"] == "8751684122"


def test_get_heads_replaces_tbd_only_proxy_live_snapshot_with_direct_live_cards(monkeypatch) -> None:
    proxy_html = """
    <html>
      <head><title>Dota 2 Matches & livescore – DLTV</title></head>
      <div class="card mobile-none" data-matches-live>
        <div class="card__body">
          <div class="matches__v2-items">
            <div class="match live tbd" data-series-id="426241" data-match="0">
              <div class="match__head">
                <div class="match__head-event"><span>DreamLeague Division 2 Season 4</span></div>
              </div>
              <div class="match__body">
                <div class="match__body-details">
                  <a href="https://dltv.org/matches/426241/btc-gaming-vs-alis-ventorus-dreamleague-division-2-season-4"></a>
                  <div class="match__body-details__team">
                    <div class="team"><div class="team__title"><span>TBD</span></div></div>
                  </div>
                  <div class="match__body-details__score">
                    <div class="score"><strong class="text-red">-</strong><small>(0)</small></div>
                    <div class="duration"><strong class="duration__live"><span>Live</span></strong></div>
                    <div class="score"><strong class="text-red">-</strong><small>(0)</small></div>
                  </div>
                  <div class="match__body-details__team">
                    <div class="team"><div class="team__title"><span>TBD</span></div></div>
                  </div>
                </div>
              </div>
            </div>
            <div class="match live tbd" data-series-id="426263" data-match="0">
              <div class="match__head">
                <div class="match__head-event"><span>DreamLeague Division 2 Season 4</span></div>
              </div>
              <div class="match__body">
                <div class="match__body-details">
                  <a href="https://dltv.org/matches/426263/south-america-rejects-vs-l1ga-team-dreamleague-division-2-season-4"></a>
                  <div class="match__body-details__team">
                    <div class="team"><div class="team__title"><span>TBD</span></div></div>
                  </div>
                  <div class="match__body-details__score">
                    <div class="score"><strong class="text-red">-</strong><small>(0)</small></div>
                    <div class="duration"><strong class="duration__live"><span>Live</span></strong></div>
                    <div class="score"><strong class="text-red">-</strong><small>(0)</small></div>
                  </div>
                  <div class="match__body-details__team">
                    <div class="team"><div class="team__title"><span>TBD</span></div></div>
                  </div>
                </div>
              </div>
            </div>
            <div class="match upcoming" data-series-id="426228">
              <div class="match__body">
                <div class="match__body-details">
                  <a href="https://dltv.org/matches/426228/rune-eaters-vs-modus-dreamleague-division-2-season-4"></a>
                </div>
              </div>
            </div>
            <div class="match upcoming tbd" data-series-id="426205">
              <div class="match__body">
                <div class="match__body-details">
                  <a href="https://dltv.org/matches/426205/tbd-vs-tbd-pgl-wallachia-season-8"></a>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </html>
    """
    direct_html = """
    <html>
      <head><title>Dota 2 Matches & livescore – DLTV</title></head>
      <div class="match live" data-series-id="426228" data-match="0">
        <div class="match__head">
          <div class="match__head-event"><span>DreamLeague Division 2 Season 4</span></div>
        </div>
        <div class="match__body">
          <div class="match__body-details">
            <a href="https://dltv.org/matches/426228/rune-eaters-vs-modus-dreamleague-division-2-season-4"></a>
            <div class="match__body-details__team">
              <div class="team"><div class="team__title"><span>Rune Eaters</span></div></div>
            </div>
            <div class="match__body-details__score">
              <div class="score"><strong class="text-red">-</strong><small>(0)</small></div>
              <div class="duration"><strong class="duration__live"><span>Live</span></strong></div>
              <div class="score"><strong class="text-red">-</strong><small>(1)</small></div>
            </div>
            <div class="match__body-details__team">
              <div class="team"><div class="team__title"><span>Modus</span></div></div>
            </div>
          </div>
        </div>
      </div>
      <div class="match live" data-series-id="426205" data-match="8781484100">
        <div class="match__head">
          <div class="match__head-event"><span>PGL Wallachia Season 8</span></div>
        </div>
        <div class="match__body">
          <div class="match__body-details">
            <a href="https://dltv.org/matches/426205/south-america-rejects-vs-mouz-pgl-wallachia-season-8"></a>
            <div class="match__body-details__team">
              <div class="team"><div class="team__title"><span>SAR</span></div></div>
            </div>
            <div class="match__body-details__score">
              <div class="score"><strong class="text-red">4</strong><small>(1)</small></div>
              <div class="duration"><strong class="duration__live"><span>Live</span></strong></div>
              <div class="score"><strong class="text-red">4</strong><small>(0)</small></div>
            </div>
            <div class="match__body-details__team">
              <div class="team"><div class="team__title"><span>MOUZ</span></div></div>
            </div>
          </div>
        </div>
      </div>
    </html>
    """

    monkeypatch.setattr(runtime, "USE_PROXY", True, raising=False)
    monkeypatch.setattr(runtime, "PROXY_LIST", ["proxy-a"], raising=False)
    monkeypatch.setattr(runtime, "CURRENT_PROXY_INDEX", 0, raising=False)
    monkeypatch.setattr(runtime, "CURRENT_PROXY", "proxy-a", raising=False)
    monkeypatch.setattr(runtime, "PROXIES", {"http": "proxy-a", "https": "proxy-a"}, raising=False)
    monkeypatch.setattr(runtime.time, "sleep", lambda *_args, **_kwargs: None)

    direct_calls: List[str] = []
    monkeypatch.setattr(
        runtime,
        "_perform_http_get",
        lambda *_args, **_kwargs: direct_calls.append("direct") or _FakeTextResponse(direct_html),
    )
    send_calls: List[str] = []
    monkeypatch.setattr(runtime, "send_message", lambda message, **_kwargs: send_calls.append(str(message)))

    heads, bodies = runtime.get_heads(response=_FakeTextResponse(proxy_html))

    assert heads is not None and len(heads) == 2
    assert bodies is not None and len(bodies) == 2
    hrefs = [runtime._extract_live_listing_context(heads[i], bodies[i])["href"] for i in range(len(heads))]
    assert hrefs == [
        "https://dltv.org/matches/426228/rune-eaters-vs-modus-dreamleague-division-2-season-4",
        "https://dltv.org/matches/426205/south-america-rejects-vs-mouz-pgl-wallachia-season-8",
    ]
    assert direct_calls == ["direct"]
    assert send_calls == []


def test_perform_http_get_prefers_curl_cffi_for_matches(monkeypatch) -> None:
    curl_calls: List[Dict[str, Any]] = []

    class _FakeCurlRequests:
        @staticmethod
        def get(url: str, **kwargs):
            curl_calls.append({"url": url, **kwargs})
            return _FakeTextResponse("<html></html>", status_code=200)

    def _requests_get(*_args, **_kwargs):
        raise AssertionError("requests.get should not be used for /matches when curl_cffi is available")

    monkeypatch.setattr(runtime, "CURL_CFFI_AVAILABLE", True, raising=False)
    monkeypatch.setattr(runtime, "curl_cffi_requests", _FakeCurlRequests, raising=False)
    monkeypatch.setattr(runtime.requests, "get", _requests_get)

    response = runtime._perform_http_get(
        "https://46.229.214.49/matches",
        headers={
            "User-Agent": "Mozilla/5.0",
            "X-Requested-With": "XMLHttpRequest",
        },
        verify=False,
        timeout=10,
        proxies={"http": "proxy-a", "https": "proxy-a"},
    )

    assert response.status_code == 200
    assert len(curl_calls) == 1
    assert curl_calls[0]["url"] == "https://46.229.214.49/matches"
    assert curl_calls[0]["impersonate"] == "chrome136"
    assert "X-Requested-With" not in curl_calls[0]["headers"]
    assert "text/html" in curl_calls[0]["headers"]["Accept"]


def test_perform_http_get_uses_requests_for_non_matches(monkeypatch) -> None:
    request_calls: List[Dict[str, Any]] = []

    def _requests_get(url: str, **kwargs):
        request_calls.append({"url": url, **kwargs})
        return _FakeTextResponse("{}", status_code=200)

    monkeypatch.setattr(runtime, "CURL_CFFI_AVAILABLE", True, raising=False)
    monkeypatch.setattr(runtime.requests, "get", _requests_get)

    response = runtime._perform_http_get(
        "https://dltv.org/live/test.json",
        headers={"User-Agent": "Mozilla/5.0", "X-Requested-With": "XMLHttpRequest"},
        verify=False,
        timeout=10,
        proxies=None,
    )

    assert response.status_code == 200
    assert len(request_calls) == 1
    assert request_calls[0]["url"] == "https://dltv.org/live/test.json"
    assert request_calls[0]["headers"]["X-Requested-With"] == "XMLHttpRequest"


def test_render_live_series_json_cards_builds_v2_cards() -> None:
    payload = {
        "live": {"8751684122": 425877},
        "upcoming": {
            "425877": {
                "id": 425877,
                "slug": "xtreme-gaming-vs-team-yandex-esl-one-birmingham-2026",
                "event": {"title": "ESL One Birmingham 2026"},
                "first_team": {"title": "Xtreme Gaming"},
                "second_team": {"title": "Team Yandex"},
                "series_scores": {"first_team": 1, "second_team": 0},
            }
        },
    }

    cards = runtime._render_live_series_json_cards(payload)

    assert len(cards) == 1
    listing = runtime._extract_live_listing_context(cards[0], cards[0])
    assert listing["layout"] == "match_card_v2"
    assert listing["series_id"] == "425877"
    assert listing["live_match_id"] == "8751684122"
    assert listing["href"].endswith("/matches/425877/xtreme-gaming-vs-team-yandex-esl-one-birmingham-2026")
    assert listing["score"] == "1 : 0"


def test_get_heads_uses_live_series_json_when_matches_html_is_template(monkeypatch) -> None:
    html = """
    <html>
      <head><title>Dota 2 Matches & livescore – DLTV</title></head>
      <script>
        for (const [match_id, series_id] of Object.entries(result.live)) { console.log(match_id, series_id); }
      </script>
    </html>
    """

    monkeypatch.setattr(
        runtime,
        "_fetch_live_series_json_cards",
        lambda **_kwargs: _build_v2_live_cards()[0],
    )

    heads, bodies = runtime.get_heads(response=_FakeTextResponse(html))

    assert heads is not None and len(heads) == 1
    assert bodies is not None and len(bodies) == 1
    listing = runtime._extract_live_listing_context(heads[0], bodies[0])
    assert listing["layout"] == "match_card_v2"


def test_general_notifies_live_matches_missing_only_after_all_proxies(monkeypatch) -> None:
    send_calls: List[str] = []

    monkeypatch.setattr(runtime, "_load_stats_dicts", lambda: None)
    monkeypatch.setattr(runtime, "_safe_flush_sent_signal_journal_into_map_id_check", lambda: 0)
    monkeypatch.setattr(runtime, "_load_map_id_check_urls", lambda recover=True: [])
    monkeypatch.setattr(runtime, "_load_delayed_queue_state", lambda recover=True: {})
    monkeypatch.setattr(runtime, "_replace_monitored_matches_from_snapshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_sync_processed_urls_cache", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_load_uncertain_delivery_urls", lambda: [])
    monkeypatch.setattr(runtime, "_sync_uncertain_delivery_urls_cache", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_ensure_delayed_sender_started", lambda: None)
    monkeypatch.setattr(runtime, "_ensure_bookmaker_prefetch_started", lambda: None)
    monkeypatch.setattr(runtime, "_stop_bookmaker_prefetch_worker", lambda: None)
    monkeypatch.setattr(runtime, "_init_proxy_pool", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "send_message", lambda message, **_kwargs: send_calls.append(str(message)))
    monkeypatch.setattr(runtime, "LIVE_MATCHES_MISSING_ALERT_ACTIVE", False, raising=False)
    monkeypatch.setattr(runtime, "PROXY_POOL_DIRECT_FALLBACK_ALERT_ACTIVE", False, raising=False)

    def _heads_request_failed():
        runtime.GET_HEADS_LAST_FAILURE_REASON = runtime.GET_HEADS_FAILURE_REASON_REQUEST_FAILED
        return None, None

    monkeypatch.setattr(runtime, "get_heads", _heads_request_failed)
    assert runtime.general(use_proxy=False, odds=False) is None
    assert send_calls == []

    def _heads_missing_after_all_proxies():
        runtime.GET_HEADS_LAST_FAILURE_REASON = runtime.GET_HEADS_FAILURE_REASON_LIVE_MATCHES_MISSING_ALL_PROXIES
        return None, None

    monkeypatch.setattr(runtime, "get_heads", _heads_missing_after_all_proxies)
    assert runtime.general(use_proxy=False, odds=False) is None
    assert send_calls == ["❌ Не найден элемент live__matches в HTML"]
    assert runtime.general(use_proxy=False, odds=False) is None
    assert send_calls == ["❌ Не найден элемент live__matches в HTML"]


def test_delayed_send_failure_schedules_backoff(tmp_path, monkeypatch) -> None:
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)
    monkeypatch.setattr(runtime, "DELAYED_SIGNAL_RETRY_BACKOFF_BASE_SECONDS", 30, raising=False)
    monkeypatch.setattr(runtime, "DELAYED_SIGNAL_RETRY_BACKOFF_MAX_SECONDS", 120, raising=False)
    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_000.0)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        runtime,
        "_fetch_delayed_match_state",
        lambda _json_url: {"game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME), "radiant_lead": 0.0},
    )
    monkeypatch.setattr(
        runtime,
        "_deliver_and_persist_signal",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("telegram down")),
    )

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
    runtime._set_delayed_match(
        "dltv.org/matches/test-retry.0",
        {
            "message": "payload",
            "reason": "late_only",
            "json_url": "https://dltv.org/live/test.json",
            "target_game_time": 1200.0,
            "queued_at": 1_699_999_000.0,
            "queued_game_time": 1100.0,
            "last_game_time": 1100.0,
            "last_progress_at": 1_699_999_000.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {},
            "fallback_send_status_label": "late_fallback_20_20_send",
            "allow_live_recheck": False,
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
        },
    )

    runtime._drain_due_delayed_signals_once()

    with runtime.monitored_matches_lock:
        payload = dict(runtime.monitored_matches["dltv.org/matches/test-retry.0"])

    assert payload["retry_attempt_count"] == 1
    assert payload["last_send_error"] == "telegram down"
    assert payload["next_retry_at"] == 1_700_000_030.0


def test_delayed_early_core_timeout_transitions_to_wr_grid(tmp_path, monkeypatch) -> None:
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)
    monkeypatch.setattr(runtime, "late_pub_comeback_table_thresholds_by_wr", {60: {20: -1000.0}}, raising=False)
    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_000.0)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        runtime,
        "_fetch_delayed_match_state",
        lambda _json_url: {"game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME), "radiant_lead": 0.0},
    )

    send_calls: List[str] = []
    deliver_calls: List[Dict[str, Any]] = []
    add_url_calls: List[Dict[str, Any]] = []
    def _deliver(*args, **kwargs):
        send_calls.append("send")
        deliver_calls.append({"args": args, "kwargs": kwargs})
        return True
    monkeypatch.setattr(runtime, "_deliver_and_persist_signal", _deliver)
    monkeypatch.setattr(
        runtime,
        "add_url",
        lambda url, **kwargs: add_url_calls.append({"url": url, **kwargs}),
    )

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
    runtime._set_delayed_match(
        "dltv.org/matches/test-early-core-timeout.0",
        {
            "message": "payload",
            "reason": "early_star_late_core_wait_1500",
            "json_url": "https://dltv.org/live/test.json",
            "target_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "queued_at": 1_699_999_000.0,
            "queued_game_time": 1100.0,
            "last_game_time": 1100.0,
            "last_progress_at": 1_699_999_000.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {"status": "draft..."},
            "fallback_send_status_label": "early_core_fallback_20_20_send",
            "send_on_target_game_time": False,
            "timeout_add_url_reason": "star_signal_rejected_early_core_monitor_timeout",
            "timeout_status_label": "early_core_timeout_no_send",
            "allow_live_recheck": True,
            "networth_monitor_threshold": 1500.0,
            "networth_monitor_deadline_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "networth_target_side": "radiant",
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
        },
    )

    runtime._drain_due_delayed_signals_once()

    assert send_calls == []
    assert add_url_calls == []
    with runtime.monitored_matches_lock:
        payload = dict(runtime.monitored_matches["dltv.org/matches/test-early-core-timeout.0"])
    assert payload["reason"] == "post_20_30_wr_grid_monitor"
    assert payload["dispatch_status_label"] == runtime.NETWORTH_STATUS_LATE_PUB_TABLE_WAIT
    assert payload["send_on_target_game_time"] is False
    assert payload["late_pub_comeback_table_active"] is True
    assert payload["late_pub_comeback_table_wr_level"] == 60
    assert payload["target_game_time"] == pytest.approx(float(runtime.LATE_PUB_COMEBACK_TABLE_START_SECONDS))


def test_delayed_wr_grid_sends_after_20_30_when_threshold_reached(tmp_path, monkeypatch) -> None:
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)
    monkeypatch.setattr(runtime, "late_pub_comeback_table_thresholds_by_wr", {60: {20: -1000.0}}, raising=False)
    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_000.0)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        runtime,
        "_fetch_delayed_match_state",
        lambda _json_url: {
            "game_time": float(runtime.LATE_PUB_COMEBACK_TABLE_START_SECONDS),
            "radiant_lead": -500.0,
        },
    )

    deliver_calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(
        runtime,
        "_deliver_and_persist_signal",
        lambda *args, **kwargs: (deliver_calls.append({"args": args, "kwargs": kwargs}) or True),
    )
    monkeypatch.setattr(runtime, "add_url", lambda *_args, **_kwargs: None)

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
    runtime._set_delayed_match(
        "dltv.org/matches/test-wr-grid-send.0",
        {
            "message": "payload",
            "reason": "early_star_late_core_wait_1500",
            "json_url": "https://dltv.org/live/test.json",
            "target_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "queued_at": 1_699_999_000.0,
            "queued_game_time": 1100.0,
            "last_game_time": 1100.0,
            "last_progress_at": 1_699_999_000.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {"status": "draft...", "target_side": "radiant"},
            "fallback_send_status_label": "early_core_fallback_20_20_send",
            "send_on_target_game_time": False,
            "timeout_add_url_reason": "star_signal_rejected_early_core_monitor_timeout",
            "timeout_status_label": "early_core_timeout_no_send",
            "allow_live_recheck": True,
            "networth_monitor_threshold": 1500.0,
            "networth_monitor_deadline_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "networth_target_side": "radiant",
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
        },
    )

    runtime._drain_due_delayed_signals_once()

    assert deliver_calls
    details = deliver_calls[-1]["kwargs"]["add_url_details"]
    assert details["dispatch_status_label"] == runtime.NETWORTH_STATUS_LATE_PUB_TABLE_SEND
    assert details["late_pub_comeback_table_reached"] is True
    assert details["late_pub_comeback_table_wr_level"] == 60
    assert details["late_pub_comeback_table_threshold"] == pytest.approx(-1000.0)
    assert details["target_networth_diff"] == pytest.approx(-500.0)


def test_delayed_late_wr85_after_20_30_waits_for_wr_grid_threshold(tmp_path, monkeypatch) -> None:
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)
    monkeypatch.setattr(runtime, "late_pub_comeback_table_thresholds_by_wr", {85: {36: -9598.96}}, raising=False)
    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_000.0)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        runtime,
        "_fetch_delayed_match_state",
        lambda _json_url: {"game_time": (36 * 60) + 30.0, "radiant_lead": 33641.0},
    )

    deliver_calls: List[Dict[str, Any]] = []
    add_url_calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(
        runtime,
        "_deliver_and_persist_signal",
        lambda *args, **kwargs: (deliver_calls.append({"args": args, "kwargs": kwargs}) or True),
    )
    monkeypatch.setattr(
        runtime,
        "add_url",
        lambda url, **kwargs: add_url_calls.append({"url": url, **kwargs}),
    )

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
    runtime._set_delayed_match(
        "cyberscore.live/en/matches/173556.map2",
        {
            "message": "payload",
            "reason": "late_only_no_early_star_wait_2000",
            "json_url": "https://cyberscore.live/api/test.json",
            "target_game_time": float(runtime.LATE_PUB_COMEBACK_TABLE_START_SECONDS),
            "queued_at": 1_699_999_000.0,
            "queued_game_time": 900.0,
            "last_game_time": 900.0,
            "last_progress_at": 1_699_999_000.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {
                "status": "live",
                "target_side": "dire",
                "late_wr_pct": 85.0,
            },
            "stake_multiplier_context": {
                "target_side": "dire",
                "selected_late_sign": -1,
                "has_selected_late_star": True,
                "late_wr_pct": 85.0,
            },
            "fallback_send_status_label": runtime.NETWORTH_STATUS_LATE_FALLBACK_20_20_SEND,
            "send_on_target_game_time": True,
            "allow_live_recheck": False,
            "networth_target_side": "dire",
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
        },
    )

    runtime._drain_due_delayed_signals_once()

    assert deliver_calls == []
    assert add_url_calls == []
    with runtime.monitored_matches_lock:
        payload = dict(runtime.monitored_matches["cyberscore.live/en/matches/173556.map2"])
    assert payload["reason"] == "post_20_30_wr_grid_monitor"
    assert payload["dispatch_status_label"] == runtime.NETWORTH_STATUS_LATE_PUB_TABLE_WAIT
    assert payload["send_on_target_game_time"] is False
    assert payload["late_pub_comeback_table_active"] is True
    assert payload["late_pub_comeback_table_wr_level"] == 85
    details = payload["add_url_details"]
    assert details["target_networth_diff"] == pytest.approx(-33641.0)
    assert details["late_pub_comeback_table_threshold"] == pytest.approx(-9598.96)


def test_delayed_opposite_fallback_waits_until_wr_grid_start(tmp_path, monkeypatch) -> None:
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)
    monkeypatch.setattr(runtime, "late_pub_comeback_table_thresholds_by_wr", {60: {20: -1000.0}}, raising=False)
    monkeypatch.setattr(runtime, "late_comeback_ceiling_thresholds", {"20": 13500.0, "21": 13698.0}, raising=False)
    monkeypatch.setattr(runtime, "late_comeback_ceiling_max_minute", 21, raising=False)
    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_000.0)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        runtime,
        "_fetch_delayed_match_state",
        lambda _json_url: {"game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME), "radiant_lead": -3501.0},
    )

    send_calls: List[str] = []
    deliver_calls: List[Dict[str, Any]] = []
    add_url_calls: List[Dict[str, Any]] = []

    def _deliver(*args, **kwargs):
        send_calls.append("send")
        deliver_calls.append({"args": args, "kwargs": kwargs})
        return True

    monkeypatch.setattr(runtime, "_deliver_and_persist_signal", _deliver)
    monkeypatch.setattr(
        runtime,
        "add_url",
        lambda url, **kwargs: add_url_calls.append({"url": url, **kwargs}),
    )

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
    runtime._set_delayed_match(
        "dltv.org/matches/test-late-fallback-guard.0",
        {
            "message": "payload",
            "reason": "late_only_opposite_signs",
            "json_url": "https://dltv.org/live/test.json",
            "target_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "queued_at": 1_699_999_000.0,
            "queued_game_time": 900.0,
            "last_game_time": 900.0,
            "last_progress_at": 1_699_999_000.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {"status": "draft...", "target_side": "radiant"},
            "fallback_send_status_label": "late_fallback_20_20_send",
            "fallback_max_deficit_abs": 3000.0,
            "send_on_target_game_time": True,
            "allow_live_recheck": False,
            "networth_target_side": "radiant",
            "late_comeback_monitor_candidate": True,
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
        },
    )

    runtime._drain_due_delayed_signals_once()

    assert send_calls == []
    assert add_url_calls == []
    with runtime.monitored_matches_lock:
        payload = dict(runtime.monitored_matches["dltv.org/matches/test-late-fallback-guard.0"])
    assert payload["reason"] == "late_only_opposite_signs"
    assert payload["target_game_time"] == pytest.approx(float(runtime.LATE_PUB_COMEBACK_TABLE_START_SECONDS))
    assert payload["send_on_target_game_time"] is True


def test_delayed_late_core_monitor_uses_post_target_comeback_ceiling(tmp_path, monkeypatch) -> None:
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)
    monkeypatch.setattr(runtime, "late_comeback_ceiling_thresholds", {"20": 13500.0, "21": 13698.0}, raising=False)
    monkeypatch.setattr(runtime, "late_comeback_ceiling_max_minute", 21, raising=False)
    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_000.0)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        runtime,
        "_fetch_delayed_match_state",
        lambda _json_url: {"game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME), "radiant_lead": -5000.0},
    )

    send_calls: List[str] = []
    deliver_calls: List[Dict[str, Any]] = []
    add_url_calls: List[Dict[str, Any]] = []

    def _deliver(*args, **kwargs):
        send_calls.append("send")
        deliver_calls.append({"args": args, "kwargs": kwargs})
        return True

    monkeypatch.setattr(runtime, "_deliver_and_persist_signal", _deliver)
    monkeypatch.setattr(
        runtime,
        "add_url",
        lambda url, **kwargs: add_url_calls.append({"url": url, **kwargs}),
    )

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
    runtime._set_delayed_match(
        "dltv.org/matches/test-late-core-post-target.0",
        {
            "message": "payload",
            "reason": "late_star_early_core_wait_800",
            "json_url": "https://dltv.org/live/test.json",
            "target_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "queued_at": 1_699_999_000.0,
            "queued_game_time": 900.0,
            "last_game_time": 900.0,
            "last_progress_at": 1_699_999_000.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {"status": "draft...", "target_side": "radiant"},
            "send_on_target_game_time": False,
            "allow_live_recheck": True,
            "networth_monitor_threshold": 800.0,
            "networth_monitor_deadline_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "networth_target_side": "radiant",
            "late_comeback_monitor_candidate": True,
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
        },
    )

    runtime._drain_due_delayed_signals_once()

    assert send_calls == ["send"]
    assert add_url_calls == []
    assert deliver_calls
    add_url_details = deliver_calls[-1]["kwargs"]["add_url_details"]
    assert add_url_details["late_comeback_monitor_reached"] is True
    assert add_url_details["target_networth_diff"] == pytest.approx(-5000.0)
    assert add_url_details["late_comeback_monitor_minute"] == 20
    assert add_url_details["late_comeback_monitor_threshold"] == pytest.approx(13500.0)


def test_delayed_fallback_stays_in_wr_grid_after_20_30_until_threshold(tmp_path, monkeypatch) -> None:
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)
    monkeypatch.setattr(runtime, "late_pub_comeback_table_thresholds_by_wr", {60: {20: -1000.0}}, raising=False)
    monkeypatch.setattr(runtime, "late_comeback_ceiling_thresholds", {"20": 13500.0, "21": 13698.0}, raising=False)
    monkeypatch.setattr(runtime, "late_comeback_ceiling_max_minute", 21, raising=False)
    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_000.0)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        runtime,
        "_fetch_delayed_match_state",
        lambda _json_url: {"game_time": float(runtime.LATE_PUB_COMEBACK_TABLE_START_SECONDS), "radiant_lead": -14000.0},
    )

    send_calls: List[str] = []
    add_url_calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(runtime, "_deliver_and_persist_signal", lambda *_args, **_kwargs: send_calls.append("send"))
    monkeypatch.setattr(
        runtime,
        "add_url",
        lambda url, **kwargs: add_url_calls.append({"url": url, **kwargs}),
    )

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
    runtime._set_delayed_match(
        "dltv.org/matches/test-post-target-comeback-monitor.0",
        {
            "message": "payload",
            "reason": "late_only_opposite_signs",
            "json_url": "https://dltv.org/live/test.json",
            "target_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "queued_at": 1_699_999_000.0,
            "queued_game_time": 900.0,
            "last_game_time": 900.0,
            "last_progress_at": 1_699_999_000.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {"status": "draft...", "target_side": "radiant"},
            "fallback_send_status_label": "late_fallback_20_20_send",
            "fallback_max_deficit_abs": 3000.0,
            "send_on_target_game_time": True,
            "allow_live_recheck": False,
            "networth_target_side": "radiant",
            "late_comeback_monitor_candidate": True,
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
        },
    )

    runtime._drain_due_delayed_signals_once()

    assert send_calls == []
    assert add_url_calls == []
    with runtime.monitored_matches_lock:
        payload = dict(runtime.monitored_matches["dltv.org/matches/test-post-target-comeback-monitor.0"])
    assert payload["reason"] == "post_20_30_wr_grid_monitor"
    assert payload["dispatch_status_label"] == runtime.NETWORTH_STATUS_LATE_PUB_TABLE_WAIT
    assert payload["send_on_target_game_time"] is False
    assert payload["late_pub_comeback_table_active"] is True
    assert payload["late_pub_comeback_table_wr_level"] == 60
    assert payload["target_game_time"] == pytest.approx(float(runtime.LATE_PUB_COMEBACK_TABLE_START_SECONDS))


def test_legacy_functions_add_url_is_disabled() -> None:
    import functions

    with pytest.raises(RuntimeError):
        functions.add_url("dltv.org/matches/legacy.0")


def test_general_recovers_corrupt_map_id_and_isolates_match_errors(tmp_path, monkeypatch) -> None:
    target_path = tmp_path / "map_id_check.json"
    target_path.write_text("{broken", encoding="utf-8")
    journal_path = tmp_path / "sent_signal_recovery.jsonl"
    monkeypatch.setattr(runtime, "MAP_ID_CHECK_PATH", str(target_path), raising=False)
    monkeypatch.setattr(runtime, "SENT_SIGNAL_JOURNAL_PATH", str(journal_path), raising=False)
    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "USE_PROXY", False, raising=False)
    monkeypatch.setattr(runtime, "_init_proxy_pool", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_load_stats_dicts", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_ensure_delayed_sender_started", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_stop_bookmaker_prefetch_worker", lambda *_args, **_kwargs: None)

    heads, bodies = _build_heads_and_bodies()
    monkeypatch.setattr(runtime, "get_heads", lambda *_args, **_kwargs: (heads * 2, bodies * 2))

    processed_indexes: List[int] = []

    def _fake_check_head(_heads, _bodies, i, _maps_data, return_status=None):
        if i == 0:
            raise RuntimeError("boom")
        processed_indexes.append(i)
        return "draft..."

    monkeypatch.setattr(runtime, "check_head", _fake_check_head)

    status = runtime.general(use_proxy=False, odds=False)

    assert status == "draft..."
    assert processed_indexes == [1]
    assert orjson.loads(target_path.read_bytes()) == []
    assert list(tmp_path.glob("map_id_check.json.corrupt.*"))


def test_check_head_skips_invalid_draft_before_synergy(monkeypatch) -> None:
    heads, bodies = _build_heads_and_bodies()
    sent_messages: List[str] = []
    add_url_calls: List[Dict[str, Any]] = []
    synergy_called = {"value": False}

    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "FORCE_ODDS_SIGNAL_TEST", False, raising=False)
    monkeypatch.setattr(runtime, "_ensure_delayed_sender_started", lambda: None)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_mark_url_processed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_log_bookmaker_source_snapshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "send_message", lambda message, **_kwargs: sent_messages.append(str(message)))

    def _record_add_url(url: str, reason: str = "unspecified", details: Any = None):
        add_url_calls.append(
            {
                "url": url,
                "reason": reason,
                "details": dict(details) if isinstance(details, dict) else details,
            }
        )

    monkeypatch.setattr(runtime, "add_url", _record_add_url)

    page_html = "<html><script>$.get('/live/test-integrity.json')</script></html>"
    monkeypatch.setattr(
        runtime,
        "make_request_with_retry",
        lambda *_args, **_kwargs: _FakeTextResponse(page_html, status_code=200),
    )

    live_data = {
        "fast_picks": [1],
        "db": {
            "first_team": {"is_radiant": True, "title": "Radiant Team", "team_id": 1001, "id": 1001},
            "second_team": {"title": "Dire Team", "team_id": 2002, "id": 2002},
        },
        "live_league_data": {
            "match": {},
            "radiant_team": {"team_id": 1001},
            "dire_team": {"team_id": 2002},
        },
        "radiant_lead": 0.0,
        "game_time": float(10 * 60),
    }
    monkeypatch.setattr(
        runtime.requests,
        "get",
        lambda *_args, **_kwargs: _FakeJsonResponse(live_data, status_code=200),
    )

    team_id_calls = {"count": 0}

    def _extract_candidate_team_ids(*_args, **_kwargs):
        team_id_calls["count"] += 1
        return [1001] if team_id_calls["count"] == 1 else [2002]

    monkeypatch.setattr(runtime, "_extract_candidate_team_ids", _extract_candidate_team_ids)
    monkeypatch.setattr(
        runtime,
        "_ensure_known_team_or_add_to_tier2",
        lambda team_ids, _team_name, _match_key: (True, int(team_ids[0])),
    )
    monkeypatch.setattr(runtime, "_determine_star_signal_match_tier", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(
        runtime,
        "parse_draft_and_positions",
        lambda *_args, **_kwargs: (
            _valid_heroes(0, positions=5),
            _valid_heroes(100, positions=4),
            None,
            "",
            [],
        ),
    )

    def _should_not_run(*_args, **_kwargs):
        synergy_called["value"] = True
        raise AssertionError("synergy_and_counterpick must not run for invalid draft")

    monkeypatch.setattr(runtime, "synergy_and_counterpick", _should_not_run)

    runtime.check_head(
        heads=heads,
        bodies=bodies,
        i=0,
        maps_data=set(),
        return_status=None,
    )

    assert synergy_called["value"] is False
    assert sent_messages == []
    assert add_url_calls == []


@pytest.mark.parametrize(
    ("league_title", "league_name"),
    [
        ("BLAST Slam VII: China Open Qualifier 2", "BLAST Slam VII: China Open Qualifier 2"),
        ("BLAST Slam 7: Southeast Asia Open Qualifier 2", "BLAST Slam 7: Southeast Asia Open Qualifier 2"),
    ],
)
def test_check_head_skips_denied_league_title_before_draft(
    monkeypatch, league_title: str, league_name: str
) -> None:
    heads, bodies = _build_heads_and_bodies()
    add_url_calls: List[Dict[str, Any]] = []
    parse_called = {"value": False}

    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "_ensure_delayed_sender_started", lambda: None)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)

    def _record_add_url(url: str, reason: str = "unspecified", details: Any = None):
        add_url_calls.append(
            {
                "url": url,
                "reason": reason,
                "details": dict(details) if isinstance(details, dict) else details,
            }
        )

    monkeypatch.setattr(runtime, "add_url", _record_add_url)

    page_html = "<html><script>$.get('/live/test-denied-league.json')</script></html>"
    monkeypatch.setattr(
        runtime,
        "make_request_with_retry",
        lambda *_args, **_kwargs: _FakeTextResponse(page_html, status_code=200),
    )

    live_data = {
        "fast_picks": [1],
        "db": {
            "first_team": {"is_radiant": True, "title": "Radiant Team", "team_id": 1001, "id": 1001},
            "second_team": {"title": "Dire Team", "team_id": 2002, "id": 2002},
            "league": {"title": league_title},
        },
        "live_league_data": {
            "match": {},
            "radiant_team": {"team_id": 1001},
            "dire_team": {"team_id": 2002},
            "league_name": league_name,
        },
        "radiant_lead": 0.0,
        "game_time": -90.0,
    }
    monkeypatch.setattr(
        runtime.requests,
        "get",
        lambda *_args, **_kwargs: _FakeJsonResponse(live_data, status_code=200),
    )

    team_id_calls = {"count": 0}

    def _extract_candidate_team_ids(*_args, **_kwargs):
        team_id_calls["count"] += 1
        return [1001] if team_id_calls["count"] == 1 else [2002]

    monkeypatch.setattr(runtime, "_extract_candidate_team_ids", _extract_candidate_team_ids)

    def _should_not_parse(*_args, **_kwargs):
        parse_called["value"] = True
        raise AssertionError("parse_draft_and_positions must not run for denied leagues")

    monkeypatch.setattr(runtime, "parse_draft_and_positions", _should_not_parse)

    status = runtime.check_head(heads, bodies, 0, set(), return_status="draft...")

    assert status == "draft..."
    assert parse_called["value"] is False
    assert add_url_calls
    assert add_url_calls[-1]["reason"] == "skip_league_title_denylist"
    assert add_url_calls[-1]["details"]["league_name"] == league_name


def test_find_skipped_player_account_ids_scans_both_sides() -> None:
    hits = runtime._find_skipped_player_account_ids(
        [21270361, 111],
        [860145568, 222],
    )

    assert hits["radiant"] == [21270361]
    assert hits["dire"] == [860145568]


def test_target_side_skipped_player_hits_filters_to_stake_side() -> None:
    hits = {
        "radiant": [21270361],
        "dire": [860145568],
    }

    assert runtime._target_side_skipped_player_hits(hits, "radiant") == [21270361]
    assert runtime._target_side_skipped_player_hits(hits, "dire") == [860145568]
    assert runtime._target_side_skipped_player_hits(hits, None) == []


def test_check_head_retries_direct_after_zero_player_proxy_parse_error(monkeypatch, capsys) -> None:
    heads, bodies = _build_heads_and_bodies()

    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "_ensure_delayed_sender_started", lambda: None)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "USE_PROXY", True, raising=False)
    monkeypatch.setattr(runtime, "CURRENT_PROXY", "http://proxy.local", raising=False)

    page_html = "<html><script>$.get('/live/test-zero-player-retry.json')</script></html>"
    monkeypatch.setattr(
        runtime,
        "make_request_with_retry",
        lambda *_args, **_kwargs: _FakeTextResponse(page_html, status_code=200),
    )

    live_data = {
        "fast_picks": [1],
        "db": {
            "first_team": {"is_radiant": True, "title": "Radiant Team", "team_id": 1001, "id": 1001},
            "second_team": {"title": "Dire Team", "team_id": 2002, "id": 2002},
        },
        "live_league_data": {
            "match": {},
            "radiant_team": {"team_id": 1001},
            "dire_team": {"team_id": 2002},
        },
        "radiant_lead": 0.0,
        "game_time": 420.0,
    }
    monkeypatch.setattr(
        runtime.requests,
        "get",
        lambda *_args, **_kwargs: _FakeJsonResponse(live_data, status_code=200),
    )

    team_id_calls = {"count": 0}

    def _extract_candidate_team_ids(*_args, **_kwargs):
        team_id_calls["count"] += 1
        return [1001] if team_id_calls["count"] == 1 else [2002]

    monkeypatch.setattr(runtime, "_extract_candidate_team_ids", _extract_candidate_team_ids)
    monkeypatch.setattr(
        runtime,
        "_ensure_known_team_or_add_to_tier2",
        lambda team_ids, _team_name, _match_key: (True, int(team_ids[0])),
    )
    monkeypatch.setattr(runtime, "_determine_star_signal_match_tier", lambda *_args, **_kwargs: 1)

    parse_calls = {"count": 0}

    def _parse(*_args, **_kwargs):
        parse_calls["count"] += 1
        if parse_calls["count"] == 1:
            return None, None, "Слишком мало игроков: radiant=0, dire=0", "", []
        return _valid_heroes(0, positions=5), _valid_heroes(100, positions=5), None, "", []

    monkeypatch.setattr(runtime, "parse_draft_and_positions", _parse)

    direct_http_calls: List[Dict[str, Any]] = []

    def _direct_http_get(_url: str, **kwargs):
        direct_http_calls.append(dict(kwargs))
        return _FakeTextResponse("<html><body>direct retry page</body></html>", status_code=200)

    monkeypatch.setattr(runtime, "_perform_http_get", _direct_http_get)
    monkeypatch.setattr(
        runtime,
        "validate_heroes_data",
        lambda *_args, **_kwargs: (False, "forced stop after direct retry"),
    )

    status = runtime.check_head(heads, bodies, 0, set(), return_status="draft...")

    assert status == "draft..."
    assert parse_calls["count"] == 2
    assert direct_http_calls
    assert direct_http_calls[-1].get("proxies") is None

    output = capsys.readouterr().out
    assert "Подозрение на забаненный/битый прокси" in output
    assert "Direct retry восстановил HTML lineups" in output


def test_problem_candidates_are_shown_without_odds(monkeypatch) -> None:
    heads, bodies = _build_heads_and_bodies()
    sent_messages: List[str] = []

    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "FORCE_ODDS_SIGNAL_TEST", False, raising=False)
    monkeypatch.setattr(runtime, "_ensure_delayed_sender_started", lambda: None)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_mark_url_processed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_log_bookmaker_source_snapshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "send_message", lambda message, **_kwargs: sent_messages.append(str(message)))
    monkeypatch.setattr(runtime, "add_url", lambda *_args, **_kwargs: None)

    page_html = "<html><script>$.get('/live/test-problems.json')</script></html>"
    monkeypatch.setattr(
        runtime,
        "make_request_with_retry",
        lambda *_args, **_kwargs: _FakeTextResponse(page_html, status_code=200),
    )

    live_data = {
        "fast_picks": [1],
        "db": {
            "first_team": {"is_radiant": True, "title": "Radiant Team", "team_id": 1001, "id": 1001},
            "second_team": {"title": "Dire Team", "team_id": 2002, "id": 2002},
        },
        "live_league_data": {
            "match": {},
            "radiant_team": {"team_id": 1001},
            "dire_team": {"team_id": 2002},
        },
        "radiant_lead": 0.0,
        "game_time": float(10 * 60),
    }
    monkeypatch.setattr(
        runtime.requests,
        "get",
        lambda *_args, **_kwargs: _FakeJsonResponse(live_data, status_code=200),
    )

    team_id_calls = {"count": 0}

    def _extract_candidate_team_ids(*_args, **_kwargs):
        team_id_calls["count"] += 1
        return [1001] if team_id_calls["count"] == 1 else [2002]

    monkeypatch.setattr(runtime, "_extract_candidate_team_ids", _extract_candidate_team_ids)
    monkeypatch.setattr(
        runtime,
        "_ensure_known_team_or_add_to_tier2",
        lambda team_ids, _team_name, _match_key: (True, int(team_ids[0])),
    )
    monkeypatch.setattr(runtime, "_determine_star_signal_match_tier", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(
        runtime,
        "parse_draft_and_positions",
        lambda *_args, **_kwargs: (
            _valid_heroes(0, positions=5),
            _valid_heroes(100, positions=5),
            None,
            "",
            [
                {"team_key": "radiant", "position": "pos1", "hero_id": 1, "hero_name": "Anti-Mage", "score": 10},
                {"team_key": "dire", "position": "pos5", "hero_id": 50, "hero_name": "Dazzle", "score": 20},
            ],
        ),
    )
    monkeypatch.setattr(
        runtime,
        "synergy_and_counterpick",
        lambda *_args, **_kwargs: {"early_output": {"solo": 3}, "mid_output": {"solo": 3}},
    )
    monkeypatch.setattr(runtime, "calculate_lanes", lambda *_args, **_kwargs: ("", "", ""))
    monkeypatch.setattr(runtime, "format_output_dict", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        runtime,
        "_star_block_diagnostics",
        lambda *, raw_block, target_wr, section: {
            "valid": True,
            "status": "ok",
            "sign": 1,
            "hit_metrics": ["solo"],
            "conflict_metric": None,
        },
    )
    monkeypatch.setattr(
        runtime,
        "_block_signs_same_or_zero",
        lambda *_args, **_kwargs: {"valid": True, "status": "ok"},
    )
    monkeypatch.setattr(runtime, "_format_raw_star_block_metrics", lambda *_args, **_kwargs: "none")
    monkeypatch.setattr(runtime, "_decorate_star_block_for_display", lambda raw_block, **_kwargs: dict(raw_block or {}))
    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_000.0)

    runtime.check_head(
        heads=heads,
        bodies=bodies,
        i=0,
        maps_data=set(),
        return_status=None,
    )

    assert sent_messages, "Expected a sent message"
    assert "problem_positions_top2" in sent_messages[0]
    

def test_team_elo_block_is_shown_in_telegram_message(monkeypatch) -> None:
    heads, bodies = _build_heads_and_bodies()
    sent_messages: List[str] = []

    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "FORCE_ODDS_SIGNAL_TEST", False, raising=False)
    monkeypatch.setattr(runtime, "_ensure_delayed_sender_started", lambda: None)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_mark_url_processed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_log_bookmaker_source_snapshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "send_message", lambda message, **_kwargs: sent_messages.append(str(message)))
    monkeypatch.setattr(runtime, "add_url", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        runtime,
        "_build_team_elo_matchup_summary",
        lambda *_args, **_kwargs: {
            "radiant": {"rating": 1655.0, "base_rating": 1655.0, "games": 42},
            "dire": {"rating": 1570.0, "base_rating": 1570.0, "games": 37},
            "radiant_win_prob": 0.619,
            "dire_win_prob": 0.381,
            "elo_diff": 85.0,
        },
    )

    page_html = "<html><script>$.get('/live/test-elo.json')</script></html>"
    monkeypatch.setattr(
        runtime,
        "make_request_with_retry",
        lambda *_args, **_kwargs: _FakeTextResponse(page_html, status_code=200),
    )

    live_data = {
        "fast_picks": [1],
        "db": {
            "first_team": {"is_radiant": True, "title": "Radiant Team", "team_id": 1001, "id": 1001},
            "second_team": {"title": "Dire Team", "team_id": 2002, "id": 2002},
        },
        "live_league_data": {
            "match": {},
            "radiant_team": {"team_id": 1001},
            "dire_team": {"team_id": 2002},
        },
        "radiant_lead": 0.0,
        "game_time": float(10 * 60),
    }
    monkeypatch.setattr(
        runtime.requests,
        "get",
        lambda *_args, **_kwargs: _FakeJsonResponse(live_data, status_code=200),
    )

    team_id_calls = {"count": 0}

    def _extract_candidate_team_ids(*_args, **_kwargs):
        team_id_calls["count"] += 1
        return [1001] if team_id_calls["count"] == 1 else [2002]

    monkeypatch.setattr(runtime, "_extract_candidate_team_ids", _extract_candidate_team_ids)
    monkeypatch.setattr(
        runtime,
        "_ensure_known_team_or_add_to_tier2",
        lambda team_ids, _team_name, _match_key: (True, int(team_ids[0])),
    )
    monkeypatch.setattr(runtime, "_determine_star_signal_match_tier", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(
        runtime,
        "parse_draft_and_positions",
        lambda *_args, **_kwargs: (
            _valid_heroes(0, positions=5),
            _valid_heroes(100, positions=5),
            None,
            "",
            [],
        ),
    )
    monkeypatch.setattr(
        runtime,
        "synergy_and_counterpick",
        lambda *_args, **_kwargs: {"early_output": {"solo": 3}, "mid_output": {"solo": 3}},
    )
    monkeypatch.setattr(runtime, "calculate_lanes", lambda *_args, **_kwargs: ("", "", ""))
    monkeypatch.setattr(runtime, "format_output_dict", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        runtime,
        "_star_block_diagnostics",
        lambda *, raw_block, target_wr, section: {
            "valid": True,
            "status": "ok",
            "sign": 1,
            "hit_metrics": ["solo"],
            "conflict_metric": None,
        },
    )
    monkeypatch.setattr(
        runtime,
        "_block_signs_same_or_zero",
        lambda *_args, **_kwargs: {"valid": True, "status": "ok"},
    )
    monkeypatch.setattr(runtime, "_format_raw_star_block_metrics", lambda *_args, **_kwargs: "none")
    monkeypatch.setattr(runtime, "_decorate_star_block_for_display", lambda raw_block, **_kwargs: dict(raw_block or {}))
    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_000.0)

    runtime.check_head(
        heads=heads,
        bodies=bodies,
        i=0,
        maps_data=set(),
        return_status=None,
    )

    assert sent_messages, "Expected a sent message"
    assert "Командный ELO:" in sent_messages[0]
    assert "Radiant Team: 1655" in sent_messages[0]
    assert "Dire Team: 1570" in sent_messages[0]
    assert "ELO WR≈61.9% / 38.1% (ΔELO +85)" in sent_messages[0]


def test_team_elo_block_separates_raw_team_elo_from_tier_adjusted_matchup(monkeypatch) -> None:
    block, _meta = runtime._format_team_elo_block(
        {
            "radiant": {"rating": 1593.3, "base_rating": 1511.4},
            "dire": {"rating": 1485.4, "base_rating": 1567.3},
            "radiant_win_prob": 0.6505,
            "dire_win_prob": 0.3495,
            "elo_diff": 107.9,
            "tier_gap_bonus": 163.9,
            "tier_gap_key": "TIER1_vs_TIER2",
        },
        radiant_team_name="L1GA TEAM",
        dire_team_name="Pipsqueak+4",
    )

    assert "L1GA TEAM: 1511" in block
    assert "Pipsqueak+4: 1567" in block
    assert "Raw WR≈42.0% / 58.0% (ΔELO -56)" in block
    assert "Adj WR≈65.0% / 34.9% (ΔELO +108, tier bonus +164 TIER1_vs_TIER2)" in block


def test_team_elo_block_marks_current_lineup_source() -> None:
    block, meta = runtime._format_team_elo_block(
        {
            "source": "elo_live_lineup_snapshot",
            "radiant": {"rating": 1600.0, "base_rating": 1600.0, "lineup_used": True},
            "dire": {"rating": 1500.0, "base_rating": 1500.0, "lineup_used": False},
            "radiant_win_prob": 0.6401,
            "dire_win_prob": 0.3599,
            "elo_diff": 100.0,
        },
        radiant_team_name="L1GA TEAM",
        dire_team_name="Astini+5",
    )

    assert "Командный ELO (текущий состав):" in block
    assert "L1GA TEAM: 1600" in block
    assert "Astini+5: 1500" in block
    assert isinstance(meta, dict)
    assert meta["lineup_used"] is True
    assert meta["source"] == "elo_live_lineup_snapshot"


def test_team_elo_block_shows_live_delta_vs_snapshot() -> None:
    block, meta = runtime._format_team_elo_block(
        {
            "source": "elo_live_lineup_snapshot",
            "radiant": {
                "rating": 1608.0,
                "base_rating": 1608.0,
                "snapshot_base_rating": 1600.0,
                "live_base_delta": 8.0,
                "lineup_used": True,
            },
            "dire": {
                "rating": 1491.0,
                "base_rating": 1491.0,
                "snapshot_base_rating": 1500.0,
                "live_base_delta": -9.0,
                "lineup_used": True,
            },
            "radiant_win_prob": 0.6621,
            "dire_win_prob": 0.3379,
            "elo_diff": 117.0,
        },
        radiant_team_name="Nemiga Gaming",
        dire_team_name="Spirit Academy",
    )

    assert "Δ live vs snapshot: +8 / -9" in block
    assert isinstance(meta, dict)
    assert meta["radiant_live_base_delta"] == pytest.approx(8.0)
    assert meta["dire_live_base_delta"] == pytest.approx(-9.0)


def test_bookmaker_odds_block_shows_match_fallback_row(monkeypatch) -> None:
    snapshot = {
        "status": "done",
        "mode": "live",
        "map_num": 2,
        "sites": {
            "betboom": {"odds": [], "match_odds": [], "market_closed": False},
            "pari": {"odds": [1.58, 2.25], "match_odds": [], "market_closed": False},
            "winline": {"odds": [], "match_odds": [1.30, 3.15], "market_closed": False},
        },
    }
    monkeypatch.setattr(runtime, "_bookmaker_prefetch_lookup", lambda *_args, **_kwargs: snapshot)

    block, ready, reason = runtime._bookmaker_format_odds_block("https://example.com/match")

    assert ready is True
    assert reason == "ok"
    assert "БК (live, карта 2):" in block
    assert "Pari 1.58/2.25" in block
    assert "Winline (матч) 1.30/3.15" in block


@pytest.mark.parametrize("module_name", ["functions", "signal_wrappers"])
def test_partial_star_threshold_sections_do_not_fallback_to_wr60(tmp_path, monkeypatch, module_name) -> None:
    module = __import__(module_name)
    thresholds_path = tmp_path / f"{module_name}_thresholds.json"
    thresholds_path.write_text(
        """
        {
          "60": {
            "early_output": [["solo", 3]],
            "mid_output": [["solo", 3]]
          },
          "65": {
            "early_output": [["solo", 5]],
            "mid_output": []
          }
        }
        """.strip(),
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "STAR_THRESHOLDS_PATH", thresholds_path, raising=False)
    if hasattr(module._load_star_thresholds, "cache_clear"):
        module._load_star_thresholds.cache_clear()

    loaded = module._load_star_thresholds()

    assert loaded[65]["early_output"] == [("solo", 5)]
    assert loaded[65]["mid_output"] == []
    if hasattr(module._load_star_thresholds, "cache_clear"):
        module._load_star_thresholds.cache_clear()


@pytest.mark.parametrize("module_name", ["functions", "signal_wrappers"])
def test_malformed_star_threshold_file_raises(tmp_path, monkeypatch, module_name) -> None:
    module = __import__(module_name)
    thresholds_path = tmp_path / f"{module_name}_thresholds_invalid.json"
    thresholds_path.write_text("{broken", encoding="utf-8")
    monkeypatch.setattr(module, "STAR_THRESHOLDS_PATH", thresholds_path, raising=False)
    if hasattr(module._load_star_thresholds, "cache_clear"):
        module._load_star_thresholds.cache_clear()

    with pytest.raises(RuntimeError):
        module._load_star_thresholds()

    if hasattr(module._load_star_thresholds, "cache_clear"):
        module._load_star_thresholds.cache_clear()


def test_format_output_dict_does_not_fallback_to_wr60_when_target_missing(monkeypatch) -> None:
    import functions

    monkeypatch.setattr(
        functions,
        "STAR_THRESHOLDS_BY_WR",
        {
            60: {
                "early_output": [("solo", 3)],
                "mid_output": [("solo", 3)],
            }
        },
        raising=False,
    )

    has_star = functions.format_output_dict(
        {"early_output": {"solo": 3}, "mid_output": {"solo": 3}},
        target_wr=65,
        late_signal_gate_enabled=False,
    )

    assert has_star is False


def test_format_output_dict_ignores_synergy_and_unknown_metrics(monkeypatch) -> None:
    import functions

    monkeypatch.setattr(
        functions,
        "STAR_THRESHOLDS_BY_WR",
        {
            60: {
                "early_output": [("synergy_duo", 7), ("synergy_trio", 6), ("pos1_vs_pos1", 1)],
                "mid_output": [("synergy_duo", 9), ("synergy_trio", 6), ("pos1_vs_pos1", 1)],
            }
        },
        raising=False,
    )
    payload = {
        "early_output": {"synergy_duo": 99, "synergy_trio": 99, "pos1_vs_pos1": 99},
        "mid_output": {"synergy_duo": 99, "synergy_trio": 99, "pos1_vs_pos1": 99},
    }

    has_star = functions.format_output_dict(payload, target_wr=60, late_signal_gate_enabled=False)

    assert has_star is False
    assert payload["early_output"]["synergy_duo"] == 99
    assert payload["early_output"]["synergy_trio"] == 99
    assert payload["early_output"]["pos1_vs_pos1"] == 99
    assert payload["mid_output"]["synergy_duo"] == 99
    assert payload["mid_output"]["synergy_trio"] == 99
    assert payload["mid_output"]["pos1_vs_pos1"] == 99


def test_runtime_star_thresholds_keep_only_signal_metrics(monkeypatch) -> None:
    monkeypatch.setattr(
        runtime,
        "STAR_THRESHOLDS_BY_WR",
        {
            60: {
                "early_output": [("solo", 3), ("synergy_duo", 7), ("synergy_trio", 6)],
                "mid_output": [("counterpick_1vs1", 5), ("synergy_duo", 9), ("synergy_trio", 6)],
                "all_output": [
                    ("dota2protracker_cp1vs1", 3),
                    ("synergy_duo", 5),
                    ("synergy_trio", 3),
                ],
            }
        },
        raising=False,
    )

    early_thresholds = runtime._star_thresholds_for_wr(60, "early_output")
    late_thresholds = runtime._star_thresholds_for_wr(60, "mid_output")
    all_thresholds = runtime._star_thresholds_for_wr(60, "all_output")

    assert early_thresholds == {"solo": 3}
    assert late_thresholds == {"counterpick_1vs1": 5}
    assert all_thresholds == {"dota2protracker_cp1vs1": 3}


def test_runtime_star_block_valid_with_single_non_conflicting_hit(monkeypatch) -> None:
    monkeypatch.setattr(
        runtime,
        "STAR_THRESHOLDS_BY_WR",
        {
            60: {
                "early_output": [("solo", 3), ("counterpick_1vs1", 4)],
                "mid_output": [],
                "all_output": [],
            }
        },
        raising=False,
    )

    diag = runtime._star_block_diagnostics(
        raw_block={"solo": 3, "counterpick_1vs1": 0},
        target_wr=60,
        section="early_output",
    )

    assert diag["valid"] is True
    assert diag["hit_metrics"] == ["solo"]
    assert diag["hit_count"] == 1


def test_runtime_all_star_block_valid_from_dota2protracker_cp1vs1(monkeypatch) -> None:
    monkeypatch.setattr(
        runtime,
        "STAR_THRESHOLDS_BY_WR",
        {
            60: {
                "early_output": [],
                "mid_output": [],
                "all_output": [
                    ("counterpick_1vs1", 4),
                    ("counterpick_1vs2", 3),
                    ("synergy_duo", 5),
                    ("synergy_trio", 3),
                    ("dota2protracker_cp1vs1", 3),
                ],
            }
        },
        raising=False,
    )

    diag = runtime._star_block_diagnostics(
        raw_block={
            "counterpick_1vs1": 3,
            "counterpick_1vs2": None,
            "synergy_duo": 3,
            "synergy_trio": 3,
            "dota2protracker_cp1vs1": 5.97,
        },
        target_wr=60,
        section="all_output",
    )

    assert diag["valid"] is True
    assert diag["sign"] == 1
    assert diag["hit_metrics"] == ["dota2protracker_cp1vs1"]


def test_star_signal_dispatch_flags_match_new_gate_policy() -> None:
    same_sign = runtime._star_signal_dispatch_flags(
        has_early_star=True,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        has_all_star=False,
    )
    assert same_sign["send_now_immediate"] is True
    assert same_sign["late_star_wait_pub_table"] is False

    late_only = runtime._star_signal_dispatch_flags(
        has_early_star=False,
        early_sign=None,
        has_late_star=True,
        late_sign=-1,
        has_all_star=False,
    )
    assert late_only["send_now_immediate"] is False
    assert late_only["late_star_wait_pub_table"] is True

    opposite_sign = runtime._star_signal_dispatch_flags(
        has_early_star=True,
        early_sign=1,
        has_late_star=True,
        late_sign=-1,
        has_all_star=False,
    )
    assert opposite_sign["send_now_immediate"] is False
    assert opposite_sign["late_star_wait_pub_table"] is True

    early_or_all_without_late = runtime._star_signal_dispatch_flags(
        has_early_star=False,
        early_sign=None,
        has_late_star=False,
        late_sign=None,
        has_all_star=True,
    )
    assert early_or_all_without_late["send_now_immediate"] is True
    assert early_or_all_without_late["late_star_wait_pub_table"] is False

    no_late_early_all_opposite = runtime._star_signal_dispatch_flags(
        has_early_star=True,
        early_sign=1,
        has_late_star=False,
        late_sign=None,
        has_all_star=True,
        all_sign=-1,
    )
    assert no_late_early_all_opposite["send_now_immediate"] is False
    assert no_late_early_all_opposite["send_now_no_late_early_or_all"] is False
    assert no_late_early_all_opposite["no_late_early_all_opposite_signs"] is True
    assert no_late_early_all_opposite["late_star_wait_pub_table"] is False

    late_all_same_sign = runtime._star_signal_dispatch_flags(
        has_early_star=False,
        early_sign=None,
        has_late_star=True,
        late_sign=-1,
        has_all_star=True,
        all_sign=-1,
    )
    assert late_all_same_sign["send_now_immediate"] is True
    assert late_all_same_sign["send_now_late_all_same_sign"] is True
    assert late_all_same_sign["late_star_wait_pub_table"] is False


def test_format_output_dict_all_section_stars_d2pt_and_ignores_synergy(monkeypatch) -> None:
    import functions

    monkeypatch.setattr(
        functions,
        "STAR_THRESHOLDS_BY_WR",
        {
            60: {
                "early_output": [],
                "mid_output": [],
                "all_output": [("solo", 1), ("synergy_trio", 3), ("dota2protracker_cp1vs1", 3)],
            }
        },
        raising=False,
    )
    payload = {
        "all_output": {"solo": 99, "synergy_trio": 3, "dota2protracker_cp1vs1": 3},
    }

    has_star = functions.format_output_dict(payload, target_wr=60, late_signal_gate_enabled=False)

    assert has_star is True
    assert str(payload["all_output"]["solo"]).endswith("*")
    assert payload["all_output"]["synergy_trio"] == 3
    assert str(payload["all_output"]["dota2protracker_cp1vs1"]).endswith("*")


def test_format_output_dict_all_section_ignores_synergy_conflict_against_d2pt(monkeypatch) -> None:
    import functions

    monkeypatch.setattr(
        functions,
        "STAR_THRESHOLDS_BY_WR",
        {
            60: {
                "early_output": [],
                "mid_output": [],
                "all_output": [("synergy_trio", 3), ("dota2protracker_cp1vs1", 3)],
            }
        },
        raising=False,
    )
    payload = {
        "all_output": {"synergy_trio": 3, "dota2protracker_cp1vs1": -3},
    }

    has_star = functions.format_output_dict(payload, target_wr=60, late_signal_gate_enabled=False)

    assert has_star is True
    assert payload["all_output"]["synergy_trio"] == 3
    assert str(payload["all_output"]["dota2protracker_cp1vs1"]).endswith("*")


def test_finalize_orphaned_live_elo_series_uses_finished_page_score(tmp_path, monkeypatch) -> None:
    progress_path = tmp_path / "live_elo_progress.json"
    progress_path.write_text(
        json.dumps(
            {
                "pending_series": {
                    "425561": {
                        "series_key": "425561",
                        "series_url": "dltv.org/matches/425561/pipsqueak4-vs-l1ga-team-premier-series-play-in",
                        "last_scores": {"first": 0, "second": 1},
                        "pending_map": {
                            "map_key": "dltv.org/matches/425561/pipsqueak4-vs-l1ga-team-premier-series-play-in.1",
                            "registered_at": 0,
                        },
                        "updated_at": 0,
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(runtime, "ELO_LIVE_SNAPSHOT_AVAILABLE", True, raising=False)
    monkeypatch.setattr(runtime, "_elo_live_finalize_series_from_scores", object(), raising=False)
    monkeypatch.setattr(runtime, "_elo_live_default_progress_path", progress_path, raising=False)
    monkeypatch.setattr(runtime, "LIVE_ELO_ORPHAN_PENDING_MIN_AGE_SECONDS", 0, raising=False)
    monkeypatch.setattr(
        runtime,
        "make_request_with_retry",
        lambda *_args, **_kwargs: _FakeTextResponse(
            "<html><head><title>Pipsqueak+4 0-2 L1ga Team (Mar. 21, 2026) Final Score - DLTV</title></head></html>"
        ),
        raising=False,
    )

    finalize_calls: List[Dict[str, Any]] = []

    def _fake_finalize_finished_live_series_for_elo(**kwargs):
        finalize_calls.append(dict(kwargs))
        return {
            "applied_update": {
                "map_key": "dltv.org/matches/425561/pipsqueak4-vs-l1ga-team-premier-series-play-in.1"
            }
        }

    dropped: List[Dict[str, Any]] = []
    monkeypatch.setattr(
        runtime,
        "_finalize_finished_live_series_for_elo",
        _fake_finalize_finished_live_series_for_elo,
        raising=False,
    )
    monkeypatch.setattr(
        runtime,
        "_drop_delayed_match",
        lambda match_key, reason="": dropped.append({"match_key": match_key, "reason": reason}) or True,
        raising=False,
    )

    updates = runtime._finalize_orphaned_live_elo_series(set())

    assert len(updates) == 1
    assert finalize_calls == [
        {
            "series_key": "425561",
            "series_url": "dltv.org/matches/425561/pipsqueak4-vs-l1ga-team-premier-series-play-in",
            "first_team_score": 0,
            "second_team_score": 2,
        }
    ]
    assert dropped == [
        {
            "match_key": "dltv.org/matches/425561/pipsqueak4-vs-l1ga-team-premier-series-play-in.1",
            "reason": "orphan_series_finished_live_elo_applied",
        }
    ]


def test_stale_duplicate_live_map_payload_is_not_added_to_map_id_check(tmp_path, monkeypatch) -> None:
    html = """
    <div class="head">
      <div class="event__info-info__time">draft...</div>
    </div>
    <div class="body">
      <div class="match__item-team__score">0</div>
      <div class="match__item-team__score">1</div>
      <a href="https://dltv.org/matches/425633/virtuspro-vs-nigma-esl-one-birmingham-2026"></a>
    </div>
    """
    soup = BeautifulSoup(html, "lxml")
    head = soup.find("div", class_="head")
    body = soup.find("div", class_="body")
    assert head is not None and body is not None

    progress_path = tmp_path / "live_elo_progress.json"
    progress_path.write_text(
        json.dumps(
            {
                "pending_series": {},
                "applied_maps": {
                    "dltv.org/matches/425633/virtuspro-vs-nigma-esl-one-birmingham-2026.0": {
                        "series_key": "425633",
                        "series_url": "dltv.org/matches/425633/virtuspro-vs-nigma-esl-one-birmingham-2026",
                        "winner_slot": "second",
                        "radiant_win": False,
                        "applied_at": 1774215612,
                        "match_id": 8740039655,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(runtime, "_elo_live_default_progress_path", progress_path, raising=False)
    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "FORCE_ODDS_SIGNAL_TEST", False, raising=False)
    monkeypatch.setattr(runtime, "_ensure_delayed_sender_started", lambda: None)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_mark_url_processed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_log_bookmaker_source_snapshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "send_message", lambda *_args, **_kwargs: None)

    add_url_calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(
        runtime,
        "add_url",
        lambda url, reason="unspecified", details=None: add_url_calls.append(
            {"url": url, "reason": reason, "details": details}
        ),
    )

    page_html = "<html><script>$.get('/live/test-stale.json')</script></html>"
    monkeypatch.setattr(
        runtime,
        "make_request_with_retry",
        lambda *_args, **_kwargs: _FakeTextResponse(page_html, status_code=200),
    )

    live_data = {
        "match_id": 8740039655,
        "fast_picks": [1],
        "db": {
            "first_team": {"is_radiant": True, "title": "Virtus.pro", "team_id": 2, "id": 2},
            "second_team": {"title": "Nigma Galaxy", "team_id": 5124, "id": 5124},
            "series": {"id": 425633},
        },
        "live_league_data": {
            "match": {},
            "radiant_team": {"team_id": 2},
            "dire_team": {"team_id": 5124},
            "radiant_series_wins": 0,
            "dire_series_wins": 1,
            "league_id": 19422,
        },
        "radiant_lead": -41020.0,
        "game_time": 2219.0,
    }
    monkeypatch.setattr(
        runtime.requests,
        "get",
        lambda *_args, **_kwargs: _FakeJsonResponse(live_data, status_code=200),
    )

    team_id_calls = {"count": 0}

    def _extract_candidate_team_ids(*_args, **_kwargs):
        team_id_calls["count"] += 1
        return [2] if team_id_calls["count"] == 1 else [5124]

    monkeypatch.setattr(runtime, "_extract_candidate_team_ids", _extract_candidate_team_ids)
    monkeypatch.setattr(
        runtime,
        "_ensure_known_team_or_add_to_tier2",
        lambda team_ids, _team_name, _match_key: (True, int(team_ids[0])),
    )
    monkeypatch.setattr(runtime, "_determine_star_signal_match_tier", lambda *_args, **_kwargs: 1)

    parse_called = {"value": False}

    def _should_not_parse(*_args, **_kwargs):
        parse_called["value"] = True
        raise AssertionError("parse_draft_and_positions must not run for stale duplicate payload")

    monkeypatch.setattr(runtime, "parse_draft_and_positions", _should_not_parse)

    runtime.check_head(
        heads=[head],
        bodies=[body],
        i=0,
        maps_data=set(),
        return_status=None,
    )

    assert parse_called["value"] is False
    assert add_url_calls == []


def test_v2_live_card_duplicate_is_skipped_before_match_page_fetch(monkeypatch) -> None:
    heads, bodies = _build_v2_live_cards()

    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "FORCE_ODDS_SIGNAL_TEST", False, raising=False)
    monkeypatch.setattr(runtime, "_ensure_delayed_sender_started", lambda: None)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_mark_url_processed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_log_bookmaker_source_snapshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "send_message", lambda *_args, **_kwargs: None)

    final_url = "dltv.org/matches/425633/virtuspro-vs-nigma-esl-one-birmingham-2026.1"
    maps_data = {final_url}

    page_fetch_calls: List[str] = []
    monkeypatch.setattr(
        runtime,
        "make_request_with_retry",
        lambda *_args, **_kwargs: page_fetch_calls.append("page") or _FakeTextResponse("", status_code=404),
    )

    live_data = {
        "match_id": 8740039655,
        "db": {
            "series": {
                "id": 425633,
                "slug": "virtuspro-vs-nigma-esl-one-birmingham-2026",
            }
        },
    }
    monkeypatch.setattr(
        runtime.requests,
        "get",
        lambda *_args, **_kwargs: _FakeJsonResponse(live_data, status_code=200),
    )

    parse_called = {"value": False}
    monkeypatch.setattr(
        runtime,
        "parse_draft_and_positions",
        lambda *_args, **_kwargs: parse_called.__setitem__("value", True),
    )

    runtime.check_head(
        heads=heads,
        bodies=bodies,
        i=0,
        maps_data=maps_data,
        return_status=None,
    )

    assert page_fetch_calls == []
    assert parse_called["value"] is False


def test_stale_duplicate_live_map_payload_is_not_added_to_map_id_check_for_later_bo5_map(tmp_path, monkeypatch) -> None:
    html = """
    <div class="head">
      <div class="event__info-info__time">draft...</div>
    </div>
    <div class="body">
      <div class="match__item-team__score">2</div>
      <div class="match__item-team__score">1</div>
      <a href="https://dltv.org/matches/425999/team-a-vs-team-b-bo5-final"></a>
    </div>
    """
    soup = BeautifulSoup(html, "lxml")
    head = soup.find("div", class_="head")
    body = soup.find("div", class_="body")
    assert head is not None and body is not None

    progress_path = tmp_path / "live_elo_progress.json"
    progress_path.write_text(
        json.dumps(
            {
                "pending_series": {},
                "applied_maps": {
                    "dltv.org/matches/425999/team-a-vs-team-b-bo5-final.2": {
                        "series_key": "425999",
                        "series_url": "dltv.org/matches/425999/team-a-vs-team-b-bo5-final",
                        "winner_slot": "second",
                        "radiant_win": False,
                        "applied_at": 1774215612,
                        "match_id": 999000333,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(runtime, "_elo_live_default_progress_path", progress_path, raising=False)
    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "FORCE_ODDS_SIGNAL_TEST", False, raising=False)
    monkeypatch.setattr(runtime, "_ensure_delayed_sender_started", lambda: None)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_mark_url_processed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_log_bookmaker_source_snapshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "send_message", lambda *_args, **_kwargs: None)

    add_url_calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(
        runtime,
        "add_url",
        lambda url, reason="unspecified", details=None: add_url_calls.append(
            {"url": url, "reason": reason, "details": details}
        ),
    )

    page_html = "<html><script>$.get('/live/test-stale-bo5.json')</script></html>"
    monkeypatch.setattr(
        runtime,
        "make_request_with_retry",
        lambda *_args, **_kwargs: _FakeTextResponse(page_html, status_code=200),
    )

    live_data = {
        "match_id": 999000333,
        "fast_picks": [1],
        "db": {
            "first_team": {"is_radiant": True, "title": "Team A", "team_id": 1001, "id": 1001},
            "second_team": {"title": "Team B", "team_id": 2002, "id": 2002},
            "series": {"id": 425999},
            "scores": {"first_team": 2, "second_team": 1},
        },
        "live_league_data": {
            "match": {},
            "radiant_team": {"team_id": 1001},
            "dire_team": {"team_id": 2002},
            "radiant_series_wins": 2,
            "dire_series_wins": 1,
            "league_id": 19422,
        },
        "radiant_lead": -12000.0,
        "game_time": 1800.0,
    }
    monkeypatch.setattr(
        runtime.requests,
        "get",
        lambda *_args, **_kwargs: _FakeJsonResponse(live_data, status_code=200),
    )

    team_id_calls = {"count": 0}

    def _extract_candidate_team_ids(*_args, **_kwargs):
        team_id_calls["count"] += 1
        return [1001] if team_id_calls["count"] == 1 else [2002]

    monkeypatch.setattr(runtime, "_extract_candidate_team_ids", _extract_candidate_team_ids)
    monkeypatch.setattr(
        runtime,
        "_ensure_known_team_or_add_to_tier2",
        lambda team_ids, _team_name, _match_key: (True, int(team_ids[0])),
    )
    monkeypatch.setattr(runtime, "_determine_star_signal_match_tier", lambda *_args, **_kwargs: 1)

    parse_called = {"value": False}

    def _should_not_parse(*_args, **_kwargs):
        parse_called["value"] = True
        raise AssertionError("parse_draft_and_positions must not run for stale duplicate payload on later BO5 map")

    monkeypatch.setattr(runtime, "parse_draft_and_positions", _should_not_parse)

    runtime.check_head(
        heads=[head],
        bodies=[body],
        i=0,
        maps_data=set(),
        return_status=None,
    )

    assert parse_called["value"] is False
    assert add_url_calls == []
