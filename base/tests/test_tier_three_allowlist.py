"""Tier-3 allowlist: матч открытой квалификации проходит гейт, tier2 не пухнет.

26.08.2026. Карта Kinetix — Interactive Philippines (квал BLAST Slam, тикет 10877)
доезжала до прода и отбрасывалась на тир-гейте: у филиппинской стороны Valve не
отдаёт ни `team_id`, ни названия (`radiant=[0]`, имя подменяется заглушкой
'Radiant'), а гейт умеет только «обе команды известны как tier1/tier2, иначе
допишем неизвестную в tier2 навсегда».
"""

import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402
import tier_three_teams  # noqa: E402


KINETIX_ID = 10232572


def test_kinetix_is_recognised_by_id_and_by_name() -> None:
    assert runtime._is_tier_three_team([KINETIX_ID], "Team Kinetix") is True
    # Имя приходит по-разному, id может не приехать вовсе.
    assert runtime._is_tier_three_team([], "Kinetix") is True
    assert runtime._is_tier_three_team([], "kinetix") is True
    assert runtime._is_tier_three_team([KINETIX_ID], "") is True
    # Чужая команда в список не попадает.
    assert runtime._is_tier_three_team([10102111], "Satan666") is False
    assert runtime._is_tier_three_team([], "") is False


def test_missing_identity_is_told_apart_from_a_rare_team_name() -> None:
    """«Команды нет» и «команда с редким названием» — разные вещи."""
    assert runtime._team_identity_missing([0], "Radiant") is True
    assert runtime._team_identity_missing([], "") is True
    assert runtime._team_identity_missing(None, "Dire") is True
    # Название есть — значит команду надо заводить, а не пускать безымянной.
    assert runtime._team_identity_missing([0], "Interactive Philippines") is False
    assert runtime._team_identity_missing([10102111], "Satan666") is False


def test_anonymous_side_is_admitted_next_to_an_allowlisted_team() -> None:
    """Боевой случай: `radiant=[0]` без названия против Kinetix."""
    sides = runtime._classify_tier_three_sides([0], "Radiant", [KINETIX_ID], "Team Kinetix")
    assert sides == (0, KINETIX_ID)
    # И в обратном порядке сторон.
    assert runtime._classify_tier_three_sides(
        [KINETIX_ID], "Team Kinetix", [0], "Radiant"
    ) == (KINETIX_ID, 0)


def test_anonymous_side_alone_does_not_open_the_gate() -> None:
    """Без явно разрешённой команды рядом безымянная сторона матч не пускает."""
    assert runtime._classify_tier_three_sides([0], "Radiant", [10102111], "Satan666") is None
    assert runtime._classify_tier_three_sides([0], "Radiant", [0], "Dire") is None


def test_unknown_named_team_still_goes_the_old_way(monkeypatch: pytest.MonkeyPatch) -> None:
    """Правило не про этот матч -> None, и работает прежний путь с авто-tier2.

    Иначе tier-3 список молча менял бы поведение обычных матчей. Тир команд
    подменяем: на боевой машине словарь живой (Satan666 уехал в tier2 26.08.2026
    авто-добавлением), и без подмены тест проверял бы состояние сервера.
    """
    monkeypatch.setattr(runtime, "_get_team_tier", lambda tid: 3)
    assert runtime._classify_tier_three_sides(
        [KINETIX_ID], "Team Kinetix", [10102111], "Satan666"
    ) is None
    assert runtime._classify_tier_three_sides(
        [10232327], "BPM ESPORTS", [10102111], "Satan666"
    ) is None


def test_allowlisted_team_next_to_a_known_tier_team(monkeypatch: pytest.MonkeyPatch) -> None:
    """Рядом с обычной tier1/tier2-командой разрешённая команда тоже проходит."""
    monkeypatch.setattr(runtime, "_get_team_tier", lambda tid: 2 if int(tid or 0) == 777 else 3)
    assert runtime._classify_tier_three_sides(
        [KINETIX_ID], "Team Kinetix", [777], "Известная команда"
    ) == (KINETIX_ID, 777)


def test_tier_three_is_judged_by_tier_two_rules_not_softer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """У tier 3 нет послаблений: требование одинакового знака применяется.

    Ветка `else` в выборе порога — это tier 1, и без явной правки матч
    безымянного состава уехал бы судиться по правилам ПЕРВОГО тира. Флаг включаем
    принудительно: в боевой среде он выключен, и тест иначе ничего не проверял бы.
    """
    monkeypatch.setattr(runtime, "STAR_REQUIRE_TIER2_SAME_SIGN", True)
    early = {"valid": True, "sign": 1}
    late = {"valid": True, "sign": -1}
    assert runtime._star_match_status_from_diags(early, late, 2) == "skip_tier2_same_sign_required"
    assert runtime._star_match_status_from_diags(early, late, 3) == "skip_tier2_same_sign_required"
    # Совпал знак — статус тот же, что у tier 2.
    same = {"valid": True, "sign": -1}
    assert runtime._star_match_status_from_diags(same, late, 3) == "send_now_same_sign"


def test_registry_holds_the_qualifier_pair() -> None:
    keys = set(tier_three_teams.TIER_THREE_TEAMS)
    assert "kinetix" in keys
    assert "interactive philippines" in keys
    assert KINETIX_ID in tier_three_teams.TIER_THREE_TEAMS["kinetix"]
    assert "radiant" in tier_three_teams.ANONYMOUS_TEAM_NAMES


def test_admitted_league_opens_the_gate_without_listing_every_team(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Лига впущена руками -> перечислять её команды не нужно.

    26.08.2026 на тикете 10877 одновременно шли одиннадцать карт открытой
    квалификации BLAST Slam ('RES Unchained 5: BLAST Slam VIII EU OQ#1'), и у
    половины сторон Valve не отдавал ни id, ни названия: Hryvna — WhiteSails
    приехала как `? vs WhiteSails`. Список команд там пришлось бы дописывать
    каждый круг.
    """
    monkeypatch.setattr(runtime, "_get_team_tier", lambda tid: 3)
    # Имя намеренно не словарное: `_resolve_known_team_id_without_side_effects`
    # сперва ищет id ПО ИМЕНИ, и живая команда с боевой машины (WhiteSails уже
    # уехала в tier2 авто-добавлением) подменила бы придуманный id.
    assert runtime._classify_tier_three_sides(
        [0], "Radiant", [9111222], "Стек Без Словаря", league_id=10877
    ) == (0, 9111222)
    # Обе стороны без опознания — тоже пускаем: лигу впустили сознательно.
    assert runtime._classify_tier_three_sides(
        [0], "Radiant", [0], "Dire", league_id=10877
    ) == (0, 0)
    # Лига НЕ впущена — прежнее поведение.
    assert runtime._classify_tier_three_sides(
        [0], "Radiant", [9111222], "Стек Без Словаря", league_id=19944
    ) is None
    assert runtime._classify_tier_three_sides(
        [0], "Radiant", [9111222], "Стек Без Словаря"
    ) is None


def test_known_teams_keep_their_own_tier_even_in_an_admitted_league(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Обе команды известны -> тир считается как раньше, правило не вмешивается.

    Иначе допуск лиги по id (19722 Asgard) молча переводил бы её настоящие
    матчи из tier1/tier2 в tier 3.
    """
    monkeypatch.setattr(runtime, "_get_team_tier", lambda tid: 1 if int(tid or 0) else 3)
    assert runtime._classify_tier_three_sides(
        [111], "A", [222], "B", league_id=10877
    ) is None
