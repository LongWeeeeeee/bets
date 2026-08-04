from __future__ import annotations

import sys
from pathlib import Path

import pytest


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime


def _fake_gate(ed_by_label):
    def _gate(*, radiant_heroes_and_pos, dire_heroes_and_pos, target_sign, min_abs_ed=0.0):
        return {
            "valid": True,
            "status": "ok",
            "matching_windows": [],
            "ed_by_label": dict(ed_by_label),
            "min_abs_ed": float(min_abs_ed),
            "target_sign": int(target_sign),
        }

    return _gate


def _select(monkeypatch, *, gt, ed_by_label, lane_kills=None, lead=None, required=None):
    # 5_15 теперь тоже проходит NW-гейт (>=0): в кейсах про lane_kills лид
    # задаём явно, иначе сработает "нет данных о нетворсе".
    monkeypatch.setattr(
        runtime, "_kills_window_direction_gate_for_target", _fake_gate(ed_by_label)
    )
    if lead is None and gt is not None and float(gt) < 180.0:
        lead = 300.0
    return runtime._kills_window_policy_select(
        game_time_seconds=gt,
        radiant_heroes_and_pos={"pos1": {"hero_id": 1}},
        dire_heroes_and_pos={"pos1": {"hero_id": 2}},
        lane_kills_adv=lane_kills,
        radiant_lead=lead,
        required_target_sign=required,
    )


def test_515_combo_passes_at_draft(monkeypatch):
    result = _select(
        monkeypatch,
        gt=150.0,
        ed_by_label={"5_15": 0.45, "10_20": 0.1},
        lane_kills={"expected_diff": 0.35, "coverage": 12},
    )
    assert result["valid"] is True
    assert result["window_label"] == "5_15"
    assert result["target_side"] == "radiant"
    assert result["target_sign"] == 1


def test_515_blocked_when_lane_kills_opposite(monkeypatch):
    result = _select(
        monkeypatch,
        gt=150.0,
        ed_by_label={"5_15": 0.45},
        lane_kills={"expected_diff": -0.35, "coverage": 12},
    )
    assert result["valid"] is False
    assert result["status"] == "lane_kills_gate_failed"


def test_515_blocked_when_lane_kills_weak(monkeypatch):
    result = _select(
        monkeypatch,
        gt=150.0,
        ed_by_label={"5_15": 0.45},
        lane_kills={"expected_diff": 0.15, "coverage": 12},
    )
    assert result["valid"] is False
    assert result["status"] == "lane_kills_gate_failed"


def test_515_blocked_when_ed_below_0_3(monkeypatch):
    result = _select(
        monkeypatch,
        gt=150.0,
        ed_by_label={"5_15": 0.25},
        lane_kills={"expected_diff": 0.35, "coverage": 12},
    )
    assert result["valid"] is False
    assert result["status"] == "ed_below_min"


def test_1020_band_requires_nw_500(monkeypatch):
    passed = _select(
        monkeypatch, gt=420.0, ed_by_label={"10_20": 0.2}, lead=600.0
    )
    assert passed["valid"] is True
    assert passed["window_label"] == "10_20"
    assert passed["target_side"] == "radiant"
    blocked = _select(
        monkeypatch, gt=420.0, ed_by_label={"10_20": 0.2}, lead=100.0
    )
    assert blocked["valid"] is False
    assert blocked["status"] == "nw_lead_below_min"


# ── Одна ставка на карту: окно с максимальным |ed| в сторону таргета ────────
def test_strongest_window_wins_over_current_band(monkeypatch):
    # Кейс из прода (BoomBoys vs OG): на 03:36 полоса 10_20, но сильнейшее
    # окно карты — 15_25 (-0.38). В полосе 10_20 ставки быть не должно.
    ed = {"5_15": -0.22, "10_20": -0.31, "15_25": -0.38, "20_30": -0.16}
    blocked = _select(monkeypatch, gt=7 * 60.0, ed_by_label=ed, lead=-800.0)
    assert blocked["valid"] is False
    assert blocked["status"] == "not_strongest_window"
    assert blocked["window_label"] == "15_25"
    assert blocked["current_window"] == "10_20"
    # ... и уходит только в полосе 15_25, когда сходится её NW-гейт (>= +1000).
    passed = _select(monkeypatch, gt=12 * 60.0, ed_by_label=ed, lead=-1200.0)
    assert passed["valid"] is True
    assert passed["window_label"] == "15_25"
    assert passed["target_side"] == "dire"
    assert passed["expected_diff"] == pytest.approx(-0.38)


def test_strongest_window_ignores_blocks_below_their_min_ed(monkeypatch):
    # 5_15 сильнее по модулю, но не проходит свой min_ed=0.3 -> выбирается
    # 10_20 (min_ed=0.2), и ставка идёт в полосе 10_20.
    result = _select(
        monkeypatch,
        gt=420.0,
        ed_by_label={"5_15": 0.28, "10_20": 0.25},
        lead=800.0,
    )
    assert result["valid"] is True
    assert result["window_label"] == "10_20"


def test_strongest_window_tie_keeps_earliest(monkeypatch):
    ed = {"10_20": 0.4, "15_25": 0.4}
    result = _select(monkeypatch, gt=420.0, ed_by_label=ed, lead=800.0)
    assert result["valid"] is True
    assert result["window_label"] == "10_20"


def test_strongest_window_respects_required_sign(monkeypatch):
    # Сильнейшее по модулю окно смотрит не на ту сторону -> берём сильнейшее
    # среди окон нужной стороны.
    result = _select(
        monkeypatch,
        gt=12 * 60.0,
        ed_by_label={"10_20": -0.9, "15_25": 0.4},
        lead=1200.0,
        required=1,
    )
    assert result["valid"] is True
    assert result["window_label"] == "15_25"
    assert result["target_side"] == "radiant"


# ── Фейковый networth: пустой фид больше не проходит гейт ──────────────────
def test_zero_lead_after_two_minutes_is_unknown_not_even(monkeypatch):
    blocked = _select(
        monkeypatch, gt=420.0, ed_by_label={"10_20": 0.3}, lead=0.0
    )
    assert blocked["valid"] is False
    assert blocked["status"] == "networth_unknown"


def test_missing_lead_blocks_nw_gate(monkeypatch):
    blocked = _select(
        monkeypatch, gt=420.0, ed_by_label={"10_20": 0.3}, lead=None
    )
    assert blocked["valid"] is False
    assert blocked["status"] == "networth_unknown"


def test_515_needs_both_lane_kills_and_live_networth(monkeypatch):
    # У связки 5_15 два вторых гейта: lane_kills и NW-лид (>=0).
    ok = _select(
        monkeypatch,
        gt=150.0,
        ed_by_label={"5_15": 0.45},
        lane_kills={"expected_diff": 0.35, "coverage": 12},
        lead=200.0,
    )
    assert ok["valid"] is True
    assert ok["window_label"] == "5_15"
    # пустой фид на 02:30 -> данных нет, ставки нет
    empty = _select(
        monkeypatch,
        gt=150.0,
        ed_by_label={"5_15": 0.45},
        lane_kills={"expected_diff": 0.35, "coverage": 12},
        lead=0.0,
    )
    assert empty["valid"] is False
    assert empty["status"] == "networth_unknown"


def test_1020_dire_target_uses_inverted_lead(monkeypatch):
    result = _select(
        monkeypatch, gt=420.0, ed_by_label={"10_20": -0.2}, lead=-800.0
    )
    assert result["valid"] is True
    assert result["target_side"] == "dire"
    assert result["target_networth_diff"] == pytest.approx(800.0)


def test_1525_band_requires_nw_plus_1000(monkeypatch):
    blocked = _select(
        monkeypatch, gt=12 * 60.0, ed_by_label={"15_25": 0.4}, lead=900.0
    )
    assert blocked["valid"] is False
    assert blocked["status"] == "nw_lead_below_min"
    passed = _select(
        monkeypatch, gt=12 * 60.0, ed_by_label={"15_25": 0.4}, lead=1200.0
    )
    assert passed["valid"] is True
    assert passed["window_label"] == "15_25"


def test_2030_band_requires_nw_plus_1500(monkeypatch):
    blocked = _select(
        monkeypatch, gt=17 * 60.0, ed_by_label={"20_30": 0.2}, lead=1400.0
    )
    assert blocked["valid"] is False
    assert blocked["status"] == "nw_lead_below_min"
    result = _select(
        monkeypatch, gt=17 * 60.0, ed_by_label={"20_30": 0.2}, lead=1600.0
    )
    assert result["valid"] is True
    assert result["window_label"] == "20_30"


@pytest.mark.parametrize(
    "gt",
    [30.0, 200.0, 5 * 60, 9 * 60, 14 * 60, 20 * 60],
)
def test_outside_policy_bands_never_fire(monkeypatch, gt):
    result = _select(
        monkeypatch,
        gt=gt,
        ed_by_label={
            "5_15": 3.0,
            "10_20": 3.0,
            "15_25": 3.0,
            "20_30": 3.0,
        },
        lane_kills={"expected_diff": 3.0, "coverage": 99},
        lead=10000.0,
    )
    assert result["valid"] is False
    assert result["status"] == "outside_policy_band"


def test_required_target_sign_filters_wrong_side(monkeypatch):
    result = _select(
        monkeypatch,
        gt=420.0,
        ed_by_label={"10_20": 0.3},
        lead=900.0,
        required=-1,
    )
    assert result["valid"] is False
    assert result["status"] == "sign_mismatch"


def test_band_boundaries_half_open(monkeypatch):
    # 10-20 полоса: [360, 480) — старт включён, конец (дедлайн 08:00) исключён,
    # чтобы ставка ушла ДО закрытия рынка.
    on_start = _select(monkeypatch, gt=360.0, ed_by_label={"10_20": 0.3}, lead=800.0)
    assert on_start["valid"] is True
    on_end = _select(monkeypatch, gt=480.0, ed_by_label={"10_20": 0.3}, lead=800.0)
    assert on_end["valid"] is False
    assert on_end["status"] == "outside_policy_band"


def test_bands_close_before_market_deadline():
    # Рынок окна закрывается за 2 минуты до его старта — полоса обязана
    # заканчиваться не позже дедлайна.
    starts = {"5_15": 5, "10_20": 10, "15_25": 15, "20_30": 20}
    for combo in runtime.KILLS_WINDOW_POLICY:
        w = str(combo["window"])
        deadline = (starts[w] - 2) * 60
        assert combo["band_end"] <= deadline, w
        assert combo["band_start"] < combo["band_end"], w


def test_target_diff_helper_hides_empty_feed_when_game_time_given():
    # Без game_time — прежнее поведение (совместимость со старыми вызовами).
    assert runtime._target_networth_diff_from_radiant_lead(0, "radiant") == 0.0
    # С game_time ровный 0 после порога = «данных нет» для ЛЮБОГО гейта.
    assert runtime._target_networth_diff_from_radiant_lead(0, "radiant", 200.0) is None
    assert runtime._target_networth_diff_from_radiant_lead(0, "dire", 200.0) is None
    # До порога 0 — законное значение, живые лиды не трогаем.
    assert runtime._target_networth_diff_from_radiant_lead(0, "radiant", 30.0) == 0.0
    assert runtime._target_networth_diff_from_radiant_lead(900, "dire", 1200.0) == -900.0


def test_networth_unknown_helper_thresholds():
    # До порога ровный 0 — законное значение, после — «данных нет».
    assert runtime._networth_lead_is_unknown(0, 30.0) is False
    assert runtime._networth_lead_is_unknown(0, 200.0) is True
    assert runtime._networth_lead_is_unknown(647, 200.0) is False
    assert runtime._networth_lead_is_unknown(None, 10.0) is True
    assert runtime._networth_lead_is_unknown("нет", 10.0) is True


def test_message_body_shows_no_data_instead_of_fake_zero():
    text = runtime._format_live_message_state_block(
        game_time_seconds=216,
        radiant_lead=0,
        radiant_team_name="BetBoom Team",
        dire_team_name="OG",
    )
    assert "Networth: н/д (нет данных)" in text
    assert "Networth: 0" not in text


def test_second_kills_dispatch_on_same_map_is_refused(monkeypatch):
    # Прод 04.08, матч 8928835463: на карту ушли ЧЕТЫРЕ ставки на килы (5_15,
    # 10_20, 15_25, 20_30) — гард был ключёван по URL с суммой килов, и каждое
    # убийство давало новый ключ. Ставка уже ушла под .0 -> под .18 отказ.
    monkeypatch.setattr(runtime, "_match_has_tier1_team", lambda *_a, **_k: True)
    monkeypatch.setattr(
        runtime,
        "_kills_window_policy_select",
        lambda **_kwargs: {
            "valid": True,
            "status": "ok",
            "window_label": "10_20",
            "target_sign": 1,
            "target_side": "radiant",
            "expected_diff": 0.83,
        },
    )

    def _fail_if_called(*_a, **_k):
        raise AssertionError("вторая ставка на килы на ту же карту не должна отправляться")

    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", _fail_if_called)

    sent_key = runtime._kills_bet_dedup_key("dltv.org/matches/8928835463.0")
    with runtime._kills_pre_pass_sent_lock:
        runtime._kills_pre_pass_sent_urls.add(sent_key)
    try:
        refused = runtime._try_dispatch_lane_adv_standalone_kills(
            match_key="dltv.org/matches/8928835463.18",
            status="live",
            radiant_team_name="RE ARISE",
            dire_team_name="FTS",
            live_league=None,
            top=None,
            mid=None,
            bot=None,
            protracker_payload=None,
            team_elo_block="",
            game_time_seconds=801,
            radiant_lead=3051,
            lane_adv_dict_value=None,
            radiant_team_id=1,
            dire_team_id=2,
        )
    finally:
        with runtime._kills_pre_pass_sent_lock:
            runtime._kills_pre_pass_sent_urls.discard(sent_key)
    assert refused is False


def test_kills_dedup_key_is_per_map_not_per_kill():
    # check_uniq_url меняется после каждого убийства — гард обязан их склеить.
    assert runtime._kills_bet_dedup_key("dltv.org/matches/8928714146.4") == (
        runtime._kills_bet_dedup_key("dltv.org/matches/8928714146.12")
    )
    # ...но карты внутри серии остаются разными ключами.
    assert runtime._kills_bet_dedup_key(
        "cyberscore.live/en/matches/123.map1"
    ) != runtime._kills_bet_dedup_key("cyberscore.live/en/matches/123.map2")


def test_kills_header_contains_window_label():
    header = runtime._format_signal_header(
        stake_team_name="Vici Gaming",
        stake_multiplier=1.0,
        special_header_mode="early_kills",
        kills_window_label="10_20",
    )
    assert header == "СТАВКА НА Ранние килы 10-20 Vici Gaming"
