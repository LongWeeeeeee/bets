"""dota2protracker_solo: попозиционное сравнение базовых винрейтов героев.

Метрика отдаёт ОДНО знаковое число (pp): плюс — radiant, минус — dire.
Проверяем не только счёт, но и три границы, на которых такая метрика обычно
врёт: подстановка общего WR вместо попозиционного, односторонние данные и
несвежий кэш сводки. Плюс — что она осталась display-only и не влезла в
STAR-решения.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
ROOT = BASE_DIR.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import cyberscore_try as runtime  # noqa: E402
import dota2protracker as protracker  # noqa: E402
import check_old_maps  # noqa: E402

FULL_POSITIONS = ("pos1", "pos2", "pos3", "pos4", "pos5")


def _stats(**heroes: dict) -> dict:
    """{'hero name': {pos: (matches, wr)}} -> форма кэша /api/heroes/list."""
    out = {}
    for name, by_pos in heroes.items():
        key = protracker._normalize_hero_key(name.replace("__", " "))
        out[key] = {
            "hero": name,
            "matches": sum(m for m, _wr in by_pos.values()),
            "wr": 50.0,
            "by_pos": {
                str(pos): {"matches": matches, "wr": wr}
                for pos, (matches, wr) in by_pos.items()
            },
        }
    return out


def _with_overall(stats: dict, matches: int = 5000, **winrates: float) -> dict:
    """Проставить общий WR героя отдельно от попозиционного."""
    for hero, winrate in winrates.items():
        stats[hero]["wr"] = winrate
        stats[hero]["matches"] = matches
    return stats


def _team(prefix: str) -> list:
    return [(pos, f"{prefix}{index}") for index, pos in enumerate(FULL_POSITIONS, start=1)]


def _flat_stats(radiant_wr: float, dire_wr: float, matches: int = 500) -> dict:
    heroes = {}
    for index in range(1, 6):
        heroes[f"r{index}"] = {index: (matches, radiant_wr)}
        heroes[f"d{index}"] = {index: (matches, dire_wr)}
    return _stats(**heroes)


def test_score_is_the_mean_of_per_position_winrate_deltas() -> None:
    stats = _stats(
        r1={1: (500, 55.0)}, d1={1: (500, 50.0)},   # +5
        r2={2: (500, 48.0)}, d2={2: (500, 52.0)},   # -4
        r3={3: (500, 51.0)}, d3={3: (500, 50.0)},   # +1
        r4={4: (500, 50.0)}, d4={4: (500, 48.0)},   # +2
        r5={5: (500, 49.0)}, d5={5: (500, 50.0)},   # -1
    )
    valid, data = protracker.calculate_solo_winrate_advantage(
        _team("r"), _team("d"), hero_stats=stats
    )
    assert valid is True
    assert data["reason"] == "ok"
    assert data["count"] == 5
    # (5 - 4 + 1 + 2 - 1) / 5 = 0.6 -> плюс, значит преимущество radiant
    assert abs(data["score"] - 0.6) < 1e-9
    assert data["pairs"]["pos2"]["delta"] == -4.0
    assert data["games"] == 5000  # 500 матчей на каждый из десяти срезов


def test_swapping_sides_only_flips_the_sign() -> None:
    stats = _flat_stats(radiant_wr=53.0, dire_wr=49.0)
    _valid, direct = protracker.calculate_solo_winrate_advantage(
        _team("r"), _team("d"), hero_stats=stats
    )
    _valid_mirror, mirrored = protracker.calculate_solo_winrate_advantage(
        _team("d"), _team("r"), hero_stats=stats
    )
    assert abs(direct["score"] - 4.0) < 1e-9
    assert abs(mirrored["score"] + direct["score"]) < 1e-9


def test_thin_position_slice_is_skipped_and_can_invalidate_the_metric() -> None:
    """Срез на 10 матчей — не сигнал. Без четырёх позиций метрика невалидна."""
    stats = _flat_stats(radiant_wr=53.0, dire_wr=49.0)
    thin = json.loads(json.dumps(stats))
    for hero, pos in (("r1", "1"), ("r2", "2")):
        thin[hero]["by_pos"][pos]["matches"] = 10

    valid, data = protracker.calculate_solo_winrate_advantage(
        _team("r"), _team("d"), hero_stats=thin
    )
    assert valid is False
    assert data["reason"] == "insufficient_position_coverage"
    assert data["count"] == 3
    assert "pos1" in data["skipped"] and "r1" in data["skipped"]["pos1"]
    # Число всё равно посчитано и отдано в диагностику — но невалидным.
    assert abs(data["score"] - 4.0) < 1e-9


def test_overall_winrate_is_never_substituted_for_a_missing_position() -> None:
    """Общий WR героя и WR на позиции — разные шкалы; подмена = ложный сигнал."""
    stats = _flat_stats(radiant_wr=53.0, dire_wr=49.0)
    stats["r3"]["by_pos"] = {}          # позиции нет вовсе
    stats["r3"]["wr"] = 99.0            # но общий WR соблазнительно высок

    _valid, data = protracker.calculate_solo_winrate_advantage(
        _team("r"), _team("d"), hero_stats=stats
    )
    assert "pos3" not in data["pairs"]
    assert data["count"] == 4
    assert abs(data["score"] - 4.0) < 1e-9  # 99% в счёт не попал


def test_one_sided_position_does_not_shift_the_mean() -> None:
    """Позиция участвует только если WR известен у обоих героев."""
    stats = _flat_stats(radiant_wr=60.0, dire_wr=50.0)
    del stats["d4"]

    _valid, data = protracker.calculate_solo_winrate_advantage(
        _team("r"), _team("d"), hero_stats=stats
    )
    assert data["skipped"]["pos4"].startswith("thin_D:")
    assert data["count"] == 4
    assert abs(data["score"] - 10.0) < 1e-9
    assert data["radiant_wr"] == 60.0 and data["dire_wr"] == 50.0


def test_missing_hero_list_is_invalid_not_zero() -> None:
    valid, data = protracker.calculate_solo_winrate_advantage(
        _team("r"), _team("d"), hero_stats={}
    )
    assert valid is False
    assert data["reason"] == "hero_list_unavailable"
    assert data["score"] == 0.0
    assert data["pairs"] == {}


def _write_cache(tmp_path: Path, timestamp: float) -> None:
    (tmp_path / protracker.HERO_LIST_CACHE_NAME).write_text(
        json.dumps({"timestamp": timestamp, "heroes": _flat_stats(53.0, 49.0)}),
        encoding="utf-8",
    )


def test_fresh_cache_is_read_without_network(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(protracker, "CACHE_DIR", str(tmp_path))
    monkeypatch.setattr(
        protracker, "PROTRACKER_HERO_LIST_FETCHER", None, raising=False
    )
    _write_cache(tmp_path, time.time() - 3600)

    heroes, age_days = protracker.read_hero_overall_stats_cache()
    assert len(heroes) == 10
    assert 0.0 <= age_days <= 0.1


def test_stale_cache_is_refused_but_its_age_is_reported(tmp_path, monkeypatch) -> None:
    """Возраст наружу нужен, чтобы «метрики нет» не выглядело как «нули»."""
    monkeypatch.setattr(protracker, "CACHE_DIR", str(tmp_path))
    _write_cache(tmp_path, time.time() - 10 * 86400)

    heroes, age_days = protracker.read_hero_overall_stats_cache()
    assert heroes == {}
    assert age_days is not None and age_days > 9.0

    valid, data = protracker.calculate_solo_winrate_advantage(_team("r"), _team("d"))
    assert valid is False
    assert data["reason"] == "hero_list_unavailable"
    assert data["hero_list_age_days"] is not None


def test_live_draft_position_format_resolves_the_slice(tmp_path, monkeypatch) -> None:
    """Живой драфт даёт 'pos1'; раньше срез молча не находился."""
    monkeypatch.setattr(protracker, "CACHE_DIR", str(tmp_path))
    _write_cache(tmp_path, time.time())

    for position in ("pos1", "pos 1", 1, "1"):
        assert protracker.get_hero_overall_stats("r1", position=position)["wr"] == 53.0


def test_enrich_exposes_the_solo_metric_in_the_payload(monkeypatch) -> None:
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)
    monkeypatch.setattr(protracker, "parse_hero_matchups", lambda *_a, **_k: {})
    monkeypatch.setattr(
        protracker,
        "_calculate_cp1vs1_all_positions",
        lambda *_a, **_k: (False, {"count": 0, "games": 0, "radiant_core_coverage": {},
                                   "dire_core_coverage": {}, "radiant_core_vs_core_coverage": {},
                                   "dire_core_vs_core_coverage": {}, "required_core_vs_core": 2}),
    )
    monkeypatch.setattr(
        protracker,
        "_calculate_duo_synergy_all_positions",
        lambda *_a, **_k: (False, {"count": 0, "games": 0, "scores": [],
                                   "core_coverage": {}, "required_per_core": 1}),
    )
    monkeypatch.setattr(
        protracker,
        "calculate_lane_advantage",
        lambda *_a, **_k: {
            lane: {"cp1vs1": 0.0, "cp1vs1_valid": False, "cp1vs1_games": 0,
                   "duo": 0.0, "duo_valid": False, "duo_games": 0,
                   "duo_lane": 0.0, "duo_lane_valid": False, "duo_lane_games": 0}
            for lane in ("mid", "top", "bot")
        } | {"lane_advantage": 0.0, "cp1vs1_valid": False, "duo_valid": False,
             "duo_lane_valid": False, "lane_metric": "lane_adv", "duo_metric": "match_wr"},
    )
    stats = _flat_stats(radiant_wr=53.0, dire_wr=49.0)
    _with_overall(stats, **{f"r{index}": 51.0 for index in range(1, 6)})
    _with_overall(stats, **{f"d{index}": 50.0 for index in range(1, 6)})
    monkeypatch.setattr(
        protracker, "read_hero_overall_stats_cache", lambda **_k: (stats, 0.5)
    )

    radiant = {pos: {"hero_name": f"r{index}"}
               for index, pos in enumerate(FULL_POSITIONS, start=1)}
    dire = {pos: {"hero_name": f"d{index}"}
            for index, pos in enumerate(FULL_POSITIONS, start=1)}

    out = protracker.enrich_with_pro_tracker(radiant, dire, {}, min_games=10)

    assert out["pro_solo_wr_valid"] is True
    assert out["pro_solo_wr_reason"] == "ok"
    assert abs(float(out["pro_solo_wr_late"]) - 4.0) < 1e-9
    assert out["pro_solo_wr_late"] == out["pro_solo_wr_early"]
    assert out["pro_solo_wr_metric"] == "position_baseline_wr"
    assert out["pro_solo_wr_diagnostics"]["count"] == 5

    # Вторая метрика считается из тех же данных, но по общему WR героя.
    assert out["pro_solo_wr_overall_valid"] is True
    assert abs(float(out["pro_solo_wr_overall_late"]) - 1.0) < 1e-9
    assert out["pro_solo_wr_overall_metric"] == "overall_hero_wr"
    assert out["pro_solo_wr_overall_diagnostics"]["scope"] == "overall"

    # cp1vs1/duo невалидны — обе solo-метрики считаются независимо от них.
    assert out["pro_cp1vs1_valid"] is False


def test_offline_metrics_export_includes_both_protracker_solo_values() -> None:
    """Large public backtests must preserve the same solo fields as live."""
    stats = _flat_stats(radiant_wr=53.0, dire_wr=49.0)
    _with_overall(stats, **{f"r{index}": 51.0 for index in range(1, 6)})
    _with_overall(stats, **{f"d{index}": 50.0 for index in range(1, 6)})
    radiant = {
        pos: {"hero_name": f"r{index}"}
        for index, pos in enumerate(FULL_POSITIONS, start=1)
    }
    dire = {
        pos: {"hero_name": f"d{index}"}
        for index, pos in enumerate(FULL_POSITIONS, start=1)
    }

    out = check_old_maps._protracker_metrics_for_match(
        radiant,
        dire,
        hero_data={},
        min_games=10,
        hero_stats=stats,
    )

    assert out["pro_solo_wr_valid"] is True
    assert out["pro_solo_wr_late"] == 4.0
    assert out["pro_solo_wr_overall_valid"] is True
    assert out["pro_solo_wr_overall_late"] == 1.0


def test_incomplete_draft_leaves_solo_invalid(monkeypatch) -> None:
    """При <3 core-героях enrich выходит раньше — solo обязан быть невалиден."""
    monkeypatch.setattr(
        protracker,
        "read_hero_overall_stats_cache",
        lambda **_k: (_flat_stats(radiant_wr=53.0, dire_wr=49.0), 0.5),
    )
    radiant = {"pos1": {"hero_name": "r1"}}
    dire = {"pos1": {"hero_name": "d1"}}

    out = protracker.enrich_with_pro_tracker(radiant, dire, {}, min_games=10)

    assert out["pro_solo_wr_valid"] is False
    assert out["pro_solo_wr_reason"] == "insufficient_position_coverage"
    assert out["pro_cp1vs1_reason"] == "insufficient_core_heroes"


def test_solo_metric_failure_does_not_break_the_rest_of_enrichment(monkeypatch) -> None:
    def boom(**_kwargs):
        raise RuntimeError("hero list on fire")

    monkeypatch.setattr(protracker, "read_hero_overall_stats_cache", boom)
    out = protracker.enrich_with_pro_tracker(
        {"pos1": {"hero_name": "r1"}}, {"pos1": {"hero_name": "d1"}}, {}, min_games=10
    )
    assert out["pro_solo_wr_valid"] is False
    assert out["pro_solo_wr_reason"] == "error:RuntimeError"


# ---------------------------------------------------------------------------
# Метрика 2: общий WR героя по всем позициям (dota2protracker_solo_overall)
# ---------------------------------------------------------------------------


def test_overall_metric_reads_hero_wide_winrate_not_the_position_slice() -> None:
    """Две метрики обязаны расходиться: у них разный источник винрейта.

    Средний разрыв общего и попозиционного WR на боевых данных — 2.88pp, у
    bounty_hunter pos2 — 24.79pp. Здесь разводим их до противоположных знаков.
    """
    stats = _flat_stats(radiant_wr=60.0, dire_wr=50.0)   # по позициям radiant +10
    _with_overall(stats, **{f"r{i}": 45.0 for i in range(1, 6)})
    _with_overall(stats, **{f"d{i}": 55.0 for i in range(1, 6)})  # в целом dire +10

    _pos_valid, by_position = protracker.calculate_solo_winrate_advantage(
        _team("r"), _team("d"), hero_stats=stats
    )
    overall_valid, overall = protracker.calculate_overall_winrate_advantage(
        _team("r"), _team("d"), hero_stats=stats
    )
    assert overall_valid is True
    assert abs(by_position["score"] - 10.0) < 1e-9
    assert abs(overall["score"] + 10.0) < 1e-9
    assert by_position["scope"] == "position" and overall["scope"] == "overall"


def test_overall_metric_does_not_depend_on_which_position_a_hero_took() -> None:
    """Общий WR позиция-агностик: перестановка героев по слотам его не меняет."""
    stats = _flat_stats(radiant_wr=53.0, dire_wr=49.0)
    _with_overall(stats, r1=58.0, r2=52.0, r3=50.0, r4=48.0, r5=47.0)
    _with_overall(stats, d1=50.0, d2=50.0, d3=50.0, d4=50.0, d5=50.0)

    straight = [(pos, f"r{i}") for i, pos in enumerate(FULL_POSITIONS, start=1)]
    shuffled = [(pos, f"r{i}") for i, pos in zip((5, 3, 1, 4, 2), FULL_POSITIONS)]

    _v1, first = protracker.calculate_overall_winrate_advantage(
        straight, _team("d"), hero_stats=stats
    )
    _v2, second = protracker.calculate_overall_winrate_advantage(
        shuffled, _team("d"), hero_stats=stats
    )
    assert abs(first["score"] - second["score"]) < 1e-9
    assert abs(first["score"] - 1.0) < 1e-9  # (58+52+50+48+47)/5 - 50


def test_overall_metric_skips_heroes_below_the_matches_floor() -> None:
    stats = _flat_stats(radiant_wr=53.0, dire_wr=49.0)
    _with_overall(stats, **{f"r{i}": 55.0 for i in range(1, 6)})
    _with_overall(stats, **{f"d{i}": 50.0 for i in range(1, 6)})
    stats["r2"]["matches"] = 30  # герой только появился — общий WR ещё шум

    _valid, data = protracker.calculate_overall_winrate_advantage(
        _team("r"), _team("d"), hero_stats=stats
    )
    assert "pos2" not in data["pairs"]
    assert data["skipped"]["pos2"].startswith("thin_R:")
    assert data["count"] == 4
    assert abs(data["score"] - 5.0) < 1e-9


PAYLOAD = {
    "pro_cp1vs1_late": 2.5,
    "pro_cp1vs1_valid": True,
    "pro_duo_synergy_late": 4.0,
    "pro_duo_synergy_valid": True,
    "pro_solo_wr_late": -1.75,
    "pro_solo_wr_valid": True,
    "pro_solo_wr_overall_late": 0.85,
    "pro_solo_wr_overall_valid": True,
    "pro_lane_advantage": 3.0,
}


def test_all_block_carries_both_solo_metrics() -> None:
    out = runtime._build_dota2protracker_star_output(PAYLOAD)
    assert out["dota2protracker_solo"] == -1.75
    assert out["dota2protracker_solo_overall"] == 0.85

    all_out = runtime._build_all_star_output(
        post_lane_output={"counterpick_1vs1": 1.0, "solo": 0.5},
        protracker_payload=PAYLOAD,
    )
    assert all_out["dota2protracker_solo"] == -1.75
    assert all_out["dota2protracker_solo_overall"] == 0.85
    # Локальный dict-solo и pro-solo — разные метрики, не путать.
    assert all_out["solo"] == 0.5


def test_invalid_solo_never_reaches_the_all_block() -> None:
    payload = dict(PAYLOAD, pro_solo_wr_valid=False, pro_solo_wr_overall_valid=False)
    out = runtime._build_dota2protracker_star_output(payload)
    assert "dota2protracker_solo" not in out
    assert "dota2protracker_solo_overall" not in out


def test_one_invalid_solo_metric_does_not_hide_the_other() -> None:
    out = runtime._build_dota2protracker_star_output(
        dict(PAYLOAD, pro_solo_wr_valid=False)
    )
    assert "dota2protracker_solo" not in out
    assert out["dota2protracker_solo_overall"] == 0.85


def test_telegram_block_shows_both_solo_lines() -> None:
    block = runtime._build_dota2protracker_block(PAYLOAD)
    assert [line for line in block.splitlines() if line.startswith("Protracker_solo")] == [
        "Protracker_solo: -1.75",
        "Protracker_solo_overall: +0.85",
    ]

    only = runtime._build_dota2protracker_only_message(
        radiant_team_name="R",
        dire_team_name="D",
        live_league={},
        protracker_payload=PAYLOAD,
    )
    assert "Protracker_solo: -1.75" in only
    assert "Protracker_solo_overall: +0.85" in only

    invalid = runtime._build_dota2protracker_block(
        dict(PAYLOAD, pro_solo_wr_valid=False, pro_solo_wr_overall_valid=False)
    )
    assert "Protracker_solo: invalid" in invalid
    assert "Protracker_solo_overall: invalid" in invalid


def test_strip_dota2protracker_block_removes_both_solo_lines() -> None:
    lines = [
        "СТАВКА НА R x1",
        "",
        "dota2protracker:",
        "Protracker_1vs1: +2.50",
        "Protracker_duo: +4.00",
        "Protracker_solo: -1.75",
        "Protracker_solo_overall: +0.85",
        "Time: 30:00",
    ]
    assert runtime._strip_dota2protracker_message_block_lines(lines) == [
        "СТАВКА НА R x1",
        "Time: 30:00",
    ]


def test_live_state_block_is_inserted_after_the_last_protracker_line() -> None:
    """Привязка live-блока — последняя строка ProTracker, иначе метрики режутся."""
    message = (
        "СТАВКА НА PlayTime x1\n"
        "nemiga VS playtime\n"
        "All:\n"
        "Counterpick_1vs1: None\n"
        "Protracker_1vs1: -1.88\n"
        "Protracker_duo: -3.00\n"
        "Protracker_solo: -1.75\n"
        "Protracker_solo_overall: +0.85\n"
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
        "Protracker_solo: -1.75\n"
        "Protracker_solo_overall: +0.85\n"
        "Time: 11:58\n"
    ) in updated


def test_both_solo_metrics_are_display_only_and_never_star_metrics() -> None:
    """Влиять на рассылку метрики не должны: на про они ещё не проверены."""
    import functions
    import signal_wrappers

    for metric in ("dota2protracker_solo", "dota2protracker_solo_overall"):
        for metric_set in (
            runtime._STAR_SIGNAL_METRICS,
            functions.STAR_SIGNAL_METRICS,
            signal_wrappers.STAR_SIGNAL_METRICS,
        ):
            assert metric not in metric_set

        for section in ("early_output", "mid_output", "all_output"):
            assert runtime._star_metric_enabled_for_section(metric, section) is False

    # Звёздочка в блоке — тоже про STAR, её быть не должно даже при большом |v|.
    decorated = runtime._decorate_star_block_for_display(
        {
            "counterpick_1vs1": 9.0,
            "dota2protracker_solo": 99.0,
            "dota2protracker_solo_overall": 99.0,
        },
        section="all_output",
        target_wr=60,
    )
    assert not str(decorated["dota2protracker_solo"]).endswith("*")
    assert not str(decorated["dota2protracker_solo_overall"]).endswith("*")
