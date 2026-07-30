"""Solo lane-adv: агрегат в кэше, лейн-метрика и fallback для Lane_adv_protracker.

`lane_adv` — прокси перевеса по нетворсу на 10-й минуте. Solo-значение героя
проверено по источнику: у Drow pos1 взвешенное среднее +6.81 против +6.7% на
странице героя.

Отдельно закреплено, что подстановка solo в Lane_adv_protracker по умолчанию
ВЫКЛЮЧЕНА: это число участвует в гейте рассылки (_same_sign_lane_adv_guard),
и включать его можно только осознанно.
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
ROOT = BASE_DIR.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import cyberscore_try as runtime  # noqa: E402
import dota2protracker as protracker  # noqa: E402

FULL_POSITIONS = ("pos1", "pos2", "pos3", "pos4", "pos5")


def _lane_rows(rows) -> dict:
    """[(own_pos, other_hero, other_pos, lane_adv, games)] -> форма кэша."""
    out: dict = {}
    for own_pos, other_hero, other_pos, lane_adv, games in rows:
        out.setdefault(other_hero, {}).setdefault(str(other_pos), {})[str(own_pos)] = {
            "lane_adv": lane_adv,
            "games": games,
            "wr": 50.0 + lane_adv,
            "diff": lane_adv,
            "metric": "lane_adv",
        }
    return out


def _hero(rows, precomputed: bool = False) -> dict:
    payload = {"_matchups_lane_by_hero_pos": _lane_rows(rows)}
    if precomputed:
        payload["_lane_adv_solo_by_pos"] = protracker._aggregate_solo_lane_adv(
            payload["_matchups_lane_by_hero_pos"]
        )
    return payload


def test_solo_lane_adv_is_weighted_by_games_not_a_plain_mean() -> None:
    """Простое среднее дало бы +7.64 там, где взвешенное даёт +6.81 (Drow)."""
    hero = _hero([
        ("1", "opponent_a", "3", 6.0, 900),
        ("1", "opponent_b", "3", 30.0, 100),
    ])
    aggregate = protracker._aggregate_solo_lane_adv(hero["_matchups_lane_by_hero_pos"])
    assert aggregate["1"]["lane_adv"] == 8.4          # (6*900 + 30*100) / 1000
    assert aggregate["1"]["games"] == 1000
    assert aggregate["1"]["rows"] == 2
    plain = (6.0 + 30.0) / 2
    assert aggregate["1"]["lane_adv"] != plain


def test_thin_matchup_rows_do_not_reach_the_hero_baseline() -> None:
    hero = _hero([
        ("1", "opponent_a", "3", 5.0, 500),
        ("1", "opponent_b", "3", 99.0, 3),   # три игры — не база, а шум
    ])
    aggregate = protracker._aggregate_solo_lane_adv(hero["_matchups_lane_by_hero_pos"])
    assert aggregate["1"]["lane_adv"] == 5.0
    assert aggregate["1"]["rows"] == 1


def test_cache_field_is_used_and_missing_field_is_computed_on_the_fly() -> None:
    """Схему кэша не поднимали: старые файлы обязаны работать без переобхода."""
    rows = [("1", "opponent_a", "3", 4.0, 500)]
    hero_data = {"fresh": _hero(rows, precomputed=True), "legacy": _hero(rows)}

    assert "_lane_adv_solo_by_pos" in hero_data["fresh"]
    assert "_lane_adv_solo_by_pos" not in hero_data["legacy"]
    assert protracker.get_hero_solo_lane_adv(hero_data, "fresh", "pos1") == (4.0, 500)
    assert protracker.get_hero_solo_lane_adv(hero_data, "legacy", "pos1") == (4.0, 500)


def test_hero_without_enough_games_has_no_baseline() -> None:
    hero_data = {"rookie": _hero([("1", "opponent_a", "3", 8.0, 40)])}
    value, games = protracker.get_hero_solo_lane_adv(hero_data, "rookie", "pos1")
    assert value is None
    assert games == 40


def _draft_hero_data(**lane_adv_by_hero: float) -> dict:
    """Герой с одинаковой lane-базой на всех позициях — удобно для арифметики."""
    return {
        hero: _hero([(pos, "opponent", "3", value, 1000) for pos in ("1", "2", "3", "4", "5")])
        for hero, value in lane_adv_by_hero.items()
    }


def _lane_draft():
    radiant = [(pos, f"r{index}") for index, pos in enumerate(FULL_POSITIONS, start=1)]
    dire = [(pos, f"d{index}") for index, pos in enumerate(FULL_POSITIONS, start=1)]
    return radiant, dire


def test_lane_sides_are_paired_by_lane_not_by_position_number() -> None:
    """Drow (pos1) стоит против dire pos3/pos4, а не против dire pos1."""
    radiant, dire = _lane_draft()
    hero_data = _draft_hero_data(
        r1=6.0, r2=4.0, r3=-4.0, r4=0.0, r5=2.0,
        d1=1.0, d2=0.0, d3=-1.0, d4=-3.0, d5=-1.0,
    )
    solo = protracker.calculate_solo_lane_advantage(radiant, dire, hero_data)

    # bot: radiant pos1+pos5 = (6-2)/... = +4 против dire pos3+pos4 = -2
    assert solo["lanes"]["bot"]["radiant"] == 4.0
    assert solo["lanes"]["bot"]["dire"] == -2.0
    assert solo["lanes"]["bot"]["value"] == 6.0
    # top — зеркало: radiant pos3+pos4 = -2 против dire pos1+pos5 = 0
    assert solo["lanes"]["top"]["radiant"] == -2.0
    assert solo["lanes"]["top"]["dire"] == 0.0
    assert solo["lanes"]["top"]["value"] == -2.0
    # Номера позиций сами по себе не сравниваются: pos1 vs pos1 дало бы +5.
    assert solo["lanes"]["bot"]["value"] != 5.0
    assert solo["valid"] is True
    assert solo["covered_lanes"] == 2


def test_mid_is_not_part_of_the_solo_lane_metric() -> None:
    """Мид измеряется парным pos2 vs pos2 напрямую — solo там лишний."""
    assert set(protracker.LANE_SOLO_SIDES) == {"top", "bot"}

    radiant, dire = _lane_draft()
    hero_data = _draft_hero_data(r1=0.0, r2=99.0, r3=0.0, r4=0.0, r5=0.0,
                                 d1=0.0, d2=-99.0, d3=0.0, d4=0.0, d5=0.0)
    solo = protracker.calculate_solo_lane_advantage(radiant, dire, hero_data)

    assert "mid" not in solo["lanes"]
    # Мидеры разведены на 198pp и всё равно не влияют на итог.
    assert solo["lane_advantage"] == 0.0
    assert solo["valid"] is True


def test_solo_lane_advantage_is_invalid_when_a_lane_side_has_no_baseline() -> None:
    radiant, dire = _lane_draft()
    hero_data = _draft_hero_data(r1=6.0, r2=1.0, r3=0.0, r4=0.0, r5=2.0,
                                 d1=0.0, d2=1.0, d3=-1.0, d4=-3.0, d5=0.0)
    del hero_data["d3"]
    del hero_data["d4"]  # у бота не осталось ни одного героя со стороны dire

    solo = protracker.calculate_solo_lane_advantage(radiant, dire, hero_data)
    assert solo["lanes"]["bot"]["valid"] is False
    assert solo["valid"] is False
    assert solo["covered_lanes"] == 1


def test_solo_fallback_is_off_by_default_because_lane_adv_gates_dispatch() -> None:
    assert protracker.PRO_LANE_SOLO_FALLBACK is False

    radiant, dire = _lane_draft()
    hero_data = _draft_hero_data(r1=6.0, r2=4.0, r3=0.0, r4=0.0, r5=2.0,
                                 d1=0.0, d2=0.0, d3=-1.0, d4=-3.0, d5=0.0)
    # Парных матчапов нет вовсе: hero_data содержит только lane-базу.
    without = protracker.calculate_lane_advantage(radiant, dire, hero_data, min_games=10)
    assert without["lane_advantage"] == 0.0
    assert without["cp1vs1_valid"] is False
    assert without["solo_lane_fallback_used"] is False
    assert [without[lane]["cp1vs1_source"] for lane in ("mid", "top", "bot")] == [None] * 3


def test_solo_fallback_fills_only_lanes_without_pairwise_data() -> None:
    radiant, dire = _lane_draft()
    hero_data = _draft_hero_data(r1=6.0, r2=4.0, r3=0.0, r4=0.0, r5=2.0,
                                 d1=0.0, d2=0.0, d3=-1.0, d4=-3.0, d5=0.0)
    with_fallback = protracker.calculate_lane_advantage(
        radiant, dire, hero_data, min_games=10, solo_lane_fallback=True
    )
    assert with_fallback["solo_lane_fallback_used"] is True
    assert [with_fallback[lane]["cp1vs1_source"] for lane in ("mid", "top", "bot")] == [
        None, "solo", "solo",
    ]
    # Мид приором не закрывается, поэтому общая валидность по трём лейнам
    # так и не наступает — fallback поднимает покрытие, а не выдаёт его за полное.
    assert with_fallback["cp1vs1_valid"] is False
    # bot: radiant pos1+pos5 = +4 против dire pos3+pos4 = -2
    assert with_fallback["bot"]["cp1vs1"] == 6.0


def test_pairwise_data_always_wins_over_the_solo_prior(monkeypatch) -> None:
    """Приор подставляется только вместо отсутствующего, не поверх имеющегося."""
    radiant, dire = _lane_draft()
    hero_data = _draft_hero_data(r1=6.0, r2=4.0, r3=0.0, r4=0.0, r5=2.0,
                                 d1=0.0, d2=0.0, d3=-1.0, d4=-3.0, d5=0.0)
    monkeypatch.setattr(protracker, "_get_matchup_1v1", lambda *_a, **_k: (1.25, 300))

    result = protracker.calculate_lane_advantage(
        radiant, dire, hero_data, min_games=10, solo_lane_fallback=True
    )
    assert [result[lane]["cp1vs1_source"] for lane in ("mid", "top", "bot")] == ["matchups"] * 3
    assert result["mid"]["cp1vs1"] == 1.25
    assert result["solo_lane_fallback_used"] is False


def test_enrich_exposes_lane_solo_payload(monkeypatch) -> None:
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)
    monkeypatch.setattr(protracker, "parse_hero_matchups", lambda *_a, **_k: {})
    monkeypatch.setattr(
        protracker,
        "calculate_lane_advantage",
        lambda *_a, **_k: {
            lane: {"cp1vs1": 0.0, "cp1vs1_valid": False, "cp1vs1_games": 0,
                   "cp1vs1_source": None, "duo": 0.0, "duo_valid": False, "duo_games": 0,
                   "duo_lane": 0.0, "duo_lane_valid": False, "duo_lane_games": 0}
            for lane in ("mid", "top", "bot")
        } | {
            "lane_advantage": 0.0, "cp1vs1_valid": False, "duo_valid": False,
            "duo_lane_valid": False, "lane_metric": "lane_adv", "duo_metric": "match_wr",
            "solo_lane_fallback_used": False,
            "solo_lane": {
                "lanes": {
                    "top": {"value": -2.0, "valid": True, "games": 4000,
                            "radiant": -1.0, "dire": 1.0},
                    "bot": {"value": 6.0, "valid": True, "games": 4000,
                            "radiant": 4.0, "dire": -2.0},
                },
                "lane_advantage": 2.0,
                "valid": True,
                "covered_lanes": 2,
                "metric": "lane_adv_solo",
            },
        },
    )
    heroes = {pos: {"hero_name": f"h{index}"}
              for index, pos in enumerate(FULL_POSITIONS, start=1)}

    out = protracker.enrich_with_pro_tracker(heroes, heroes, {}, min_games=10)

    assert out["pro_lane_solo_valid"] is True
    assert abs(float(out["pro_lane_solo"]) - 2.0) < 0.01
    assert out["pro_lane_solo_covered_lanes"] == 2
    assert out["pro_lane_top_solo"] == -2.0
    assert out["pro_lane_bot_solo_games"] == 4000
    # Ключей мида у solo нет вовсе — иначе пустое значение читалось бы как ноль.
    assert "pro_lane_mid_solo" not in out
    assert "pro_lane_mid_cp1vs1_source" in out
    assert out["pro_lane_solo_fallback_used"] is False
    assert out["pro_lane_solo_metric"] == "lane_adv_solo"


LANE_PAYLOAD = {
    "pro_lane_advantage": 0.89,
    "pro_lane_solo": 2.53,
    "pro_lane_solo_valid": True,
    "pro_lane_solo_covered_lanes": 3,
}


def test_message_shows_both_lane_numbers() -> None:
    """Пока solo не проверен, он идёт рядом с основным, а не вместо него."""
    line = runtime._build_dota2protracker_lane_adv_line(LANE_PAYLOAD)
    assert line.splitlines() == ["Lane_adv_protracker: +0.89", "Lane_adv_solo: +2.53"]

    only = runtime._build_dota2protracker_only_message(
        radiant_team_name="R", dire_team_name="D", live_league={},
        protracker_payload=LANE_PAYLOAD,
    )
    assert "Lane_adv_protracker: +0.89" in only
    assert "Lane_adv_solo: +2.53" in only


def test_fallback_origin_is_visible_in_the_message() -> None:
    line = runtime._build_dota2protracker_lane_adv_line(
        dict(LANE_PAYLOAD, pro_lane_solo_fallback_used=True)
    )
    assert line.splitlines()[1] == "Lane_adv_solo: +2.53 (fallback)"


def test_invalid_lane_solo_adds_no_line() -> None:
    line = runtime._build_dota2protracker_lane_adv_line(
        dict(LANE_PAYLOAD, pro_lane_solo_valid=False)
    )
    assert line == "Lane_adv_protracker: +0.89\n"


def test_lane_solo_does_not_change_the_dispatch_guard() -> None:
    """Гейт по-прежнему смотрит на парный Lane_adv_protracker, а не на solo."""
    guard = runtime._same_sign_lane_adv_guard(
        star_sign=-1,
        lane_adv_dict_value=-9.0,
        lane_adv_protracker_value=runtime._dota2protracker_lane_adv_value(LANE_PAYLOAD),
    )
    assert guard["lane_adv_protracker"] == 0.89
    assert runtime._dota2protracker_lane_solo_value(LANE_PAYLOAD) == 2.53
