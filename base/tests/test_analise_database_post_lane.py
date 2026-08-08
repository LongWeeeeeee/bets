from __future__ import annotations

import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import analise_database as stats  # noqa: E402


def _player(hero_id: int, position: int, is_radiant: bool, *, imp: int = 0) -> dict:
    return {
        "heroId": hero_id,
        "position": f"POSITION_{position}",
        "isRadiant": is_radiant,
        "intentionalFeeding": False,
        "imp": imp,
    }


def _match(
    *,
    match_id: str = "101",
    duration: int = 25,
    minute_10_lead: int = 0,
    radiant_win: bool = True,
    imp: int = 0,
    start_date_time: int = None,
) -> dict:
    # По умолчанию матч на последнем патче, чтобы post_lane-solo записывался
    # (Option C: post_lane-solo собирается только на последнем version-патче).
    if start_date_time is None:
        start_date_time = stats.LATEST_PATCH_START_TS
    leads = [0 for _ in range(duration)]
    if duration >= stats.POST_LANE_GATE_MINUTE:
        leads[stats.POST_LANE_GATE_MINUTE - 1] = minute_10_lead
    return {
        "id": match_id,
        "startDateTime": start_date_time,
        "didRadiantWin": radiant_win,
        "radiantNetworthLeads": leads,
        "winRates": [0.6 if radiant_win else 0.4],
        "topLaneOutcome": "RADIANT_WIN",
        "midLaneOutcome": "TIE",
        "bottomLaneOutcome": "DIRE_WIN",
        "players": [
            _player(1, 1, True, imp=imp),
            _player(2, 2, True, imp=imp),
            _player(3, 3, True, imp=imp),
            _player(4, 4, True, imp=imp),
            _player(5, 5, True, imp=imp),
            _player(6, 1, False, imp=imp),
            _player(7, 2, False, imp=imp),
            _player(8, 3, False, imp=imp),
            _player(9, 4, False, imp=imp),
            _player(10, 5, False, imp=imp),
        ],
    }


def test_post_lane_dict_records_winner_for_full_metric_set() -> None:
    lane_dict = {}
    early_dict = {}
    late_dict = {}
    post_lane_dict = {}

    stats.analise_database(
        _match(duration=95, minute_10_lead=1500, radiant_win=True),
        lane_dict,
        early_dict,
        late_dict,
        post_lane_dict=post_lane_dict,
    )

    assert post_lane_dict["1pos1"]["wins"] == 1
    assert post_lane_dict["6pos1"]["wins"] == 0
    assert post_lane_dict["1pos1_vs_6pos1"]["wins"] == 1
    assert post_lane_dict["1pos1_with_2pos2"]["wins"] == 1
    assert post_lane_dict["1pos1,2pos2,3pos3"]["wins"] == 1


def test_post_lane_solo_scoped_to_latest_patch() -> None:
    # Option C: post_lane-solo пишется ТОЛЬКО для матчей последнего version-патча,
    # а cp/synergy post_lane — для любого матча, прошедшего post-lane gate.
    old = {}
    stats.analise_database(
        _match(duration=95, minute_10_lead=1500, radiant_win=True,
               start_date_time=stats.LATEST_PATCH_START_TS - 1),
        {}, {}, {}, post_lane_dict=old,
    )
    assert "1pos1" not in old          # solo НЕ записан на старом патче
    assert "6pos1" not in old
    assert old["1pos1_vs_6pos1"]["wins"] == 1     # cp/synergy записаны (широко)
    assert old["1pos1_with_2pos2"]["wins"] == 1

    latest = {}
    stats.analise_database(
        _match(duration=95, minute_10_lead=1500, radiant_win=True,
               start_date_time=stats.LATEST_PATCH_START_TS),
        {}, {}, {}, post_lane_dict=latest,
    )
    assert latest["1pos1"]["wins"] == 1           # solo записан на последнем патче
    assert latest["6pos1"]["wins"] == 0


def test_post_lane_dict_requires_min_duration() -> None:
    post_lane_dict = {}
    stats.analise_database(
        _match(duration=19, minute_10_lead=0),
        {},
        {},
        {},
        post_lane_dict=post_lane_dict,
    )
    assert post_lane_dict == {}


def test_post_lane_minute_10_gate_applies_only_when_enabled(monkeypatch) -> None:
    """Гейт минуты 10 для блока All выключен по умолчанию и включается флагом.

    Карта с большим NW-перевесом на 10-й минуте: при выключенном гейте она
    попадает в словарь, при включённом — отсекается.
    """
    # длина 30: с 2026-08-07 POST_LANE_MIN_DURATION = 28 (E-52), карта на 25 минут
    # больше не проходит порог допуска и до гейта минуты 10 просто не доходит.
    match = _match(duration=30, minute_10_lead=2500)

    monkeypatch.setattr(stats, "ANALISE_POST_LANE_MINUTE10_GATE_ENABLED", False)
    default_dict = {}
    stats.analise_database(match, {}, {}, {}, post_lane_dict=default_dict)
    assert default_dict != {}

    monkeypatch.setattr(stats, "ANALISE_POST_LANE_MINUTE10_GATE_ENABLED", True)
    gated_dict = {}
    stats.analise_database(match, {}, {}, {}, post_lane_dict=gated_dict)
    assert gated_dict == {}


def test_lane_dict_ignores_imp_field() -> None:
    lane_dict = {}

    stats.analise_database(
        _match(duration=25, minute_10_lead=2500, imp=99),
        lane_dict,
        {},
        {},
    )

    assert lane_dict["3pos3"]["games"] == 1
    assert lane_dict["3pos3,4pos4_vs_6pos1,10pos5"]["games"] == 1


def test_early_filter_uses_networth_dominator_not_match_winner() -> None:
    match = _match(duration=45, radiant_win=False)
    match["radiantNetworthLeads"][19] = 6100

    ok, dominator = stats.is_early_match(match)

    assert ok is True
    assert dominator == "radiant"


def test_early_gate_reads_index_9_not_10() -> None:
    # Minute-10 STAR gate must use zero-based index 9, not 10 (minute 11).
    match = _match(duration=45, radiant_win=True)
    match["radiantNetworthLeads"][9] = 5000
    match["radiantNetworthLeads"][10] = 100
    match["radiantNetworthLeads"][19] = 7000

    ok, dominator = stats.is_early_match(match)

    assert ok is False
    assert dominator is None


def test_early_gate_allows_when_index_9_within_max() -> None:
    # Adjacent mapping: idx9 decides gate; idx10 must not decide allow/reject.
    match = _match(duration=45, radiant_win=True)
    match["radiantNetworthLeads"][9] = 100
    match["radiantNetworthLeads"][10] = 5000
    match["radiantNetworthLeads"][19] = 7000

    ok, dominator = stats.is_early_match(match)

    assert ok is True
    assert dominator == "radiant"


def test_early_gate_index_is_minute_10_zero_based() -> None:
    # Parent contract: human minute 10 -> zero-based index 9.
    assert stats.EARLY_GATE_INDEX == 9


def test_early_gate_missing_gate_value_rejects() -> None:
    # Missing/None at the gate index rejects; threshold window must not override.
    match = _match(duration=45, radiant_win=True)
    match["radiantNetworthLeads"][9] = None
    match["radiantNetworthLeads"][10] = 100
    match["radiantNetworthLeads"][19] = 7000

    ok, dominator = stats.is_early_match(match)

    assert ok is False
    assert dominator is None


def test_early_filter_fast_finish_uses_winner_and_bypasses_gate() -> None:
    match = _match(duration=31, minute_10_lead=5000, radiant_win=False)

    ok, dominator = stats.is_early_match(match)

    assert ok is True
    assert dominator == "dire"


def test_early_filter_threshold_window_ends_at_28() -> None:
    match = _match(duration=45, radiant_win=True)
    match["radiantNetworthLeads"][28] = 20000

    ok, dominator = stats.is_early_match(match)

    assert ok is False
    assert dominator is None


def test_early_filter_uses_alchemist_leading_thresholds() -> None:
    match = _match(duration=45, radiant_win=True)
    match["players"][0]["heroId"] = stats.ALCHEMIST_HERO_ID
    match["radiantNetworthLeads"][23] = 7500

    ok, dominator = stats.is_early_match(match)

    assert ok is False
    assert dominator is None


def test_early_filter_uses_alchemist_trailing_thresholds() -> None:
    match = _match(duration=45, radiant_win=True)
    match["players"][5]["heroId"] = stats.ALCHEMIST_HERO_ID
    match["radiantNetworthLeads"][23] = 6600

    ok, dominator = stats.is_early_match(match)

    assert ok is True
    assert dominator == "radiant"


def test_late_filter_takes_long_match_by_duration_alone() -> None:
    """Дефолт с 2026-08-07: длина >= 36, условие равенства выключено (E-49).

    Разъехавшаяся карта нужной длины теперь ПРИНИМАЕТСЯ: на honest holdout
    отбор по равенству ухудшал solo на 5-7 п.п., а фильтр по длине давал +6.
    """
    match = _match(duration=40, radiant_win=False)
    for idx in range(20, 40):
        match["radiantNetworthLeads"][idx] = 20000

    ok, winner = stats.is_late_match(match, if_check=True)

    assert ok is True
    assert winner == "dire"


def test_late_filter_rejects_match_shorter_than_min_duration() -> None:
    match = _match(duration=34, radiant_win=True)
    match["radiantNetworthLeads"][20] = 3500

    ok, winner = stats.is_late_match(match, if_check=True)

    assert ok is False
    assert winner is None


def test_late_filter_equal_moment_still_works_when_enabled(monkeypatch) -> None:
    """Механика равенства осталась опцией и продолжает отсекать разъехавшиеся карты."""
    monkeypatch.setattr(stats, "LATE_REQUIRE_EQUAL_MOMENT", True)
    monkeypatch.setattr(stats, "LATE_MIN_DURATION", 34)

    close = _match(duration=34, radiant_win=True)
    close["radiantNetworthLeads"][20] = 3500
    ok, winner = stats.is_late_match(close, if_check=True)
    assert ok is True
    assert winner == "radiant"

    blown_out = _match(duration=40, radiant_win=False)
    for idx in range(20, 40):
        blown_out["radiantNetworthLeads"][idx] = 20000
    ok, winner = stats.is_late_match(blown_out, if_check=True)
    assert ok is False
    assert winner is None
