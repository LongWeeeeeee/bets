"""Winline: пара, доказанная ОДНИМ именем, берёт цену единственной живой карточки.

Winline переименовывает команды (1win -> 1W, ЯЧЁ123 -> YACHE123, Blasterbl ->
BLASTERBI), и каждое переименование стоило серии карт без цены, пока человек не
дописывал алиас. Правило: когда пара не доказана по ДВУМ именам, берём единственную
живую карточку (не проп-дуэль, не линию), на которой доказано ровно одно наше имя
прежним сопоставителем; второе имя - это второе имя той карточки, как бы Winline его
ни написал. Цена - строка нашей карты, сторона - по совпавшей команде, в результате
метка `one_side_pair matched=... other=...`. Откат: WINLINE_ONE_SIDE_PAIR=0.

Входы - снимки прода (base/tests/fixtures/*.provenance.json):
- 08.10.2026 18:34: живая `TEAM AURORA 1W` (панель события + закреплённая карточка,
  «Победитель 2 карта 1.61 2.22») и три карточки «Дуэль игроков»;
- 05.10.2026 18:35: живая `LEGION BLASTERBI` в ленте, рядом линия `TEAM AURORA 1W`.
Негативные случаи собраны из этих же снимков: блок настоящей карточки копируется
или переименовывается в разобранном HTML (каждый такой тест говорит об этом сам).
Справочник написаний отключается так же, как в контрольных тестах соседнего файла.
"""
from __future__ import annotations

import copy
import json
import re
import sys
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[2]
for path in (str(BASE_DIR), str(REPO_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

import bookmaker_selenium_odds as bk  # noqa: E402
from services.winline.winline_current_map_odds_poller import _odds_accepted  # noqa: E402

FIXTURES = Path(__file__).resolve().parent / "fixtures"
PRE_ALIAS_FINGERPRINT = "promotion=not_decider"


def _page(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _aurora() -> dict:
    return _page("winline_overview_snapshot_20261008_blast_duel_cards.json")


def _legion() -> dict:
    return _page("winline_overview_legion_blasterbi_20261005.json")


def _extract(page: dict, team1: str, team2: str, map_num: int, **kwargs):
    return bk._extract_winline_current_map_winner(
        page["text"], team1, team2, forced_map_num=map_num, html=page["html"], **kwargs
    )


@pytest.fixture()
def no_aliases(monkeypatch):
    """Справочника написаний нет: ровно положение до записи алиаса."""
    monkeypatch.setattr(bk, "_alias_spellings", lambda _name: [])
    monkeypatch.delenv("WINLINE_ONE_SIDE_PAIR", raising=False)
    getattr(bk, "_ONE_SIDE_PAIR_LOGGED", set()).clear()


# ---------------------------------------------------------------- положительные


def test_aurora_1win_is_priced_from_the_featured_live_card(no_aliases):
    """Наше `1win` на странице `1W`, алиаса нет: цена 2-й карты с меткой."""
    extract = _extract(_aurora(), "Aurora Gaming", "1win", 2)

    assert list(extract.odds) == [1.61, 2.22]
    assert extract.map_num == 2
    assert extract.market_kind == "current_map_winner"
    assert (extract.p1_team, extract.p2_team) == ("team1", "team2")
    assert extract.miss_fingerprint == (
        "one_side_pair matched=Aurora Gaming->TEAM AURORA other=1win->1W"
    )
    # Порядок и цены карточки так, как их написал Winline (F4): имена Winline, не наши.
    assert extract.card_team_order == "Aurora Gaming|1win"
    assert list(extract.card_odds) == [1.61, 2.22]


def test_aurora_request_order_is_mirrored_by_the_matched_team(no_aliases):
    direct = _extract(_aurora(), "Aurora Gaming", "1win", 2)
    mirrored = _extract(_aurora(), "1win", "Aurora Gaming", 2)

    assert list(direct.odds) == [1.61, 2.22]
    assert list(mirrored.odds) == [2.22, 1.61]
    # Размеченное имя то же; порядок карточки - Winline, а не запроса.
    assert mirrored.miss_fingerprint == direct.miss_fingerprint
    assert mirrored.card_team_order == "Aurora Gaming|1win"
    assert list(mirrored.card_odds) == [1.61, 2.22]


def test_matched_team_may_be_the_second_requested_name(no_aliases):
    """Совпавшая команда - вторая в запросе (дословно `TEAM AURORA`), запрос в обоих порядках."""
    extract = _extract(_aurora(), "1win", "TEAM AURORA", 2)
    mirrored = _extract(_aurora(), "TEAM AURORA", "1win", 2)

    assert list(extract.odds) == [2.22, 1.61]
    assert list(mirrored.odds) == [1.61, 2.22]
    assert extract.miss_fingerprint == (
        "one_side_pair matched=TEAM AURORA->TEAM AURORA other=1win->1W"
    )
    assert extract.card_team_order == "TEAM AURORA|1win"
    assert list(extract.card_odds) == [1.61, 2.22]


def test_blasterbl_legion_is_priced_without_the_alias(no_aliases):
    odds = _extract(_legion(), "Blasterbl", "LEGION", 2)
    mirrored = _extract(_legion(), "LEGION", "Blasterbl", 2)

    assert list(odds.odds) == [1.70, 2.02]
    assert list(mirrored.odds) == [2.02, 1.70]
    assert odds.miss_fingerprint == "one_side_pair matched=LEGION->LEGION other=Blasterbl->BLASTERBI"
    assert odds.card_team_order == "LEGION|Blasterbl"
    assert list(odds.card_odds) == [2.02, 1.70]
    assert mirrored.card_team_order == "LEGION|Blasterbl"


def test_both_name_proof_is_untouched_and_unmarked():
    """Справочник на месте: берётся прежний путь, метки нет, цены те же."""
    odds = _extract(_aurora(), "Aurora Gaming", "1win", 2)
    legion = _extract(_legion(), "Blasterbl", "LEGION", 2)

    assert list(odds.odds) == [1.61, 2.22]
    assert not odds.miss_fingerprint
    assert list(legion.odds) == [1.70, 2.02]
    assert not legion.miss_fingerprint


# ----------------------------------------------------------------- откат / нет цены


def test_rollback_flag_restores_the_old_miss(no_aliases, monkeypatch):
    monkeypatch.setenv("WINLINE_ONE_SIDE_PAIR", "0")

    extract = _extract(_aurora(), "Aurora Gaming", "1win", 2)

    assert list(extract.odds) == []
    assert extract.miss_fingerprint == PRE_ALIAS_FINGERPRINT


def test_map_3_has_no_row_and_duel_prices_are_never_used(no_aliases):
    """На карточке есть только 2-я карта; дуэли 1.72/2.00/1.85/1.70/1.80/1.90 не цена."""
    extract = _extract(_aurora(), "Aurora Gaming", "1win", 3)

    assert list(extract.odds) == []
    assert extract.market_closed is False
    assert extract.map_num == 3
    assert PRE_ALIAS_FINGERPRINT in (extract.miss_fingerprint or "")
    assert not (extract.miss_fingerprint or "").startswith("one_side_pair matched")


def test_map_3_duel_rows_stay_unused_when_the_old_prop_filter_is_off(no_aliases, monkeypatch):
    """Флаг отката старого фильтра дуэлей не открывает их новому правилу.

    С WINLINE_CARD_SWEEP_SKIP_PROPS=0 разбор по именам карточки `TEAM AURORA` / `1W`
    видел бы «3 карта 1.72 2.00» дуэлей (в именах игроков те же названия команд).
    """
    monkeypatch.setenv("WINLINE_CARD_SWEEP_SKIP_PROPS", "0")

    extract = _extract(_aurora(), "Aurora Gaming", "1win", 3)

    assert list(extract.odds) == []
    assert extract.market_closed is False
    assert not (extract.miss_fingerprint or "").startswith("one_side_pair matched")


def test_locked_winner_buttons_are_no_price(no_aliases):
    """Кнопки 2-й карты заморожены (классы `coefficient-button_locked`, правка снимка 05.10).

    Строка карты на карточке есть, но разбор по именам карточки видит закрытый рынок:
    цены нет, и метка успеха не пишется.
    """
    soup = BeautifulSoup(_legion()["html"], "html.parser")
    card = soup.find(id="eventId-16858346")
    locked = 0
    for node in card.select(".coefficient-button"):
        if node.get_text(strip=True) in {"2.02", "1.70"}:
            node["class"] = list(node["class"]) + ["coefficient-button_locked"]
            locked += 1
    assert locked == 2
    page = {"text": " ".join(soup.get_text(" ", strip=True).split()), "html": str(soup)}

    extract = _extract(page, "Blasterbl", "LEGION", 2)

    assert list(extract.odds) == []
    assert "one_side_pair matched" not in (extract.miss_fingerprint or "")
    assert "one_side_pair=refused:no_priced_row" in (extract.miss_fingerprint or "")


def test_prematch_line_card_is_never_a_price_source(no_aliases):
    """`TEAM AURORA 1W` в ленте 05.10 - линия на завтра с рядом `1 карта 1.84 1.98`."""
    extract = _extract(_legion(), "Aurora Gaming", "1win", 1)

    assert list(extract.odds) == []
    assert not (extract.miss_fingerprint or "").startswith("one_side_pair matched")


# ---------------------------------------------------------------- проп-дуэли


def _without(page: dict, selectors) -> dict:
    soup = BeautifulSoup(page["html"], "html.parser")
    for selector in selectors:
        for node in soup.select(selector):
            node.decompose()
    return {"text": " ".join(soup.get_text(" ", strip=True).split()), "html": str(soup)}


def test_duel_card_alone_is_never_a_candidate_even_with_the_prop_flag_off(no_aliases, monkeypatch):
    """Страница 08.10 без настоящей карточки (панель и закреплённая вырезаны).

    Остаются три живые дуэли `WS (TEAM AURORA)` / `33 (1W)` с рядом «3 карта».
    Фильтр проп-карточек старого пути выключен флагом отката - у нового правила
    своя, не отключаемая защита.
    """
    monkeypatch.setenv("WINLINE_CARD_SWEEP_SKIP_PROPS", "0")
    page = _without(
        _aurora(),
        ["ww-feature-event-live-center-dsk", "section.event-live-center", "ww-pinned-card"],
    )

    extract = _extract(page, "Aurora Gaming", "1win", 3)

    assert list(extract.odds) == []
    assert not (extract.miss_fingerprint or "").startswith("one_side_pair matched")


# ---------------------------------------------------------------- неоднозначность


def _event_cards(soup):
    return [node for node in soup.find_all(id=True) if str(node["id"]).startswith("eventId-")]


def _duplicate_card(page: dict, event_id: str, new_id: str, rename: dict = None) -> dict:
    """Копия НАСТОЯЩЕГО блока карточки из снимка (опционально с другими именами)."""
    soup = BeautifulSoup(page["html"], "html.parser")
    source = soup.find(id=event_id)
    clone = copy.copy(source)
    clone["id"] = new_id
    for text_node in list(clone.find_all(string=True)):
        value = str(text_node)
        for old, new in (rename or {}).items():
            if value.strip() == old:
                text_node.replace_with(value.replace(old, new))
    source.insert_after(clone)
    return {"text": " ".join(soup.get_text(" ", strip=True).split()), "html": str(soup)}


def test_matched_name_on_two_live_cards_is_ambiguous(no_aliases):
    """Живая карточка LEGION-BLASTERBI продублирована (копия блока 05.10): две живые."""
    page = _duplicate_card(_legion(), "eventId-16858346", "eventId-99999991",
                           {"BLASTERBI": "SOMEONE ELSE"})

    extract = _extract(page, "Blasterbl", "LEGION", 2)

    assert list(extract.odds) == []
    assert "one_side_pair=refused:multi_card" in (extract.miss_fingerprint or "")


def test_our_two_names_on_different_live_cards_is_ambiguous(no_aliases):
    """LEGION - на настоящей живой карточке, Nemiga - на копии её блока, переименованной.

    Без защиты цена карточки LEGION ушла бы паре «LEGION - Nemiga Gaming».
    """
    page = _duplicate_card(
        _legion(), "eventId-16858346", "eventId-99999992",
        {"LEGION": "MOUZ", "BLASTERBI": "NEMIGA GAMING"},
    )

    extract = _extract(page, "LEGION", "Nemiga Gaming", 2)

    assert list(extract.odds) == []
    assert "one_side_pair=refused:split_cards" in (extract.miss_fingerprint or "")


def test_pair_listed_by_both_names_never_falls_back(no_aliases):
    """Обе наши команды названы на живой карточке: это обычный путь, без метки."""
    extract = _extract(_legion(), "LEGION", "BLASTERBI", 2)

    assert list(extract.odds) == [2.02, 1.70]
    assert not extract.miss_fingerprint


def test_pair_named_on_a_line_card_blocks_the_fallback(no_aliases):
    """Обе наши команды названы на линии (завтра), одна ещё и на живой карточке.

    Снимок 05.10: `TEAM AURORA 1W` - линия на завтра; блок живой LEGION-карточки
    скопирован и переименован в `TEAM AURORA` / `1WIN` с рядом 2-й карты. Второе имя
    `1WIN` похоже на `1W` (G2 пропустил бы), так что отказ даёт именно «пара на линии».
    Пара листингом известна - чужую живую карточку ей подставлять нельзя.
    """
    page = _duplicate_card(_legion(), "eventId-16858346", "eventId-99999994",
                           {"LEGION": "TEAM AURORA", "BLASTERBI": "1WIN"})

    extract = _extract(page, "TEAM AURORA", "1W", 2)

    assert list(extract.odds) == []
    assert not (extract.miss_fingerprint or "").startswith("one_side_pair matched")


def test_roster_qualifier_keeps_academy_and_main_roster_apart(no_aliases):
    """Карточка `TEAM SPIRIT ACADEMY` (переименован блок снимка) не цена для `Team Spirit`."""
    page = _duplicate_card(_legion(), "eventId-16858346", "eventId-99999993",
                           {"LEGION": "TEAM SPIRIT ACADEMY", "BLASTERBI": "XTREME"})
    soup = BeautifulSoup(page["html"], "html.parser")
    soup.find(id="eventId-16858346").decompose()  # остаётся только переименованный блок
    page = {"text": " ".join(soup.get_text(" ", strip=True).split()), "html": str(soup)}

    # `Xtrem` (не `XTREME`): второе имя пары не доказано прежним путём, но похоже (G2).
    main = _extract(page, "Team Spirit", "Xtrem", 2)
    academy = _extract(page, "Team Spirit Academy", "Xtrem", 2)

    assert list(main.odds) == []
    assert not (main.miss_fingerprint or "").startswith("one_side_pair matched")
    # Положительный контроль фикстуры: своё написание второго состава находится.
    assert list(academy.odds) == [2.02, 1.70]
    assert academy.miss_fingerprint.startswith("one_side_pair matched=Team Spirit Academy->")


# ------------------------------------------------ раунд 2: совпавшая сторона - ПОЛНОЕ имя (G1)
#
# Раунд 1 «доказывал» сторону одним словом из запасных форм поиска и брал цену
# чужого матча. Каждый случай ниже - реальный снимок и реальная пара (или явно
# переименованная копия настоящего блока, где снимка нет).


def _stray(page: dict, *, event_id: str, new_id: str, rename: dict) -> dict:
    """Страница, на которой настоящий блок `event_id` заменён переименованной копией."""
    page = _duplicate_card(page, event_id, new_id, rename)
    soup = BeautifulSoup(page["html"], "html.parser")
    soup.find(id=event_id).decompose()
    return {"text": " ".join(soup.get_text(" ", strip=True).split()), "html": str(soup)}


def _refused(extract, reason: str) -> bool:
    fingerprint = extract.miss_fingerprint or ""
    return (
        list(extract.odds) == []
        and f"one_side_pair=refused:{reason}" in fingerprint
        and "one_side_pair matched" not in fingerprint
    )


@pytest.mark.parametrize("ours, map_num", [
    ("Team Zero", 1),     # team 9080405; на странице ZERO TENACITY (team 9600141)
    ("Kings", 1),         # DEVIL KINGS
    ("Red Bulls", 1),     # DAWN BULLS
    ("Nova", 1),          # CYBER NOVA
])
def test_g1_single_word_of_a_longer_card_name_is_not_the_team_0910(no_aliases, ours, map_num):
    """Снимок 10.09: слово из запасных форм поиска не доказывает команду."""
    extract = _extract(_page("winline_overview_snapshot_20260910.json"), ours, "Nemiga Gaming", map_num)

    assert _refused(extract, "matched_not_full"), (extract.odds, extract.miss_fingerprint)


def test_g1_gamerlegion_is_not_legion_0510(no_aliases):
    """Снимок 05.10: `GamerLegion` (9964962) - не `LEGION`; цена 2.02/1.70 чужая."""
    extract = _extract(_legion(), "GamerLegion", "Team Liquid", 2)

    assert _refused(extract, "matched_not_full"), (extract.odds, extract.miss_fingerprint)


def test_g1_nigma_galaxy_is_not_galaxy_racer(no_aliases):
    """`Nigma Galaxy - Team Liquid` получала `GALAXY RACER - TEAM SECRET` через слово galaxy.

    Снимка с такой карточкой нет: настоящий блок живой LEGION-карточки (05.10)
    переименован в GALAXY RACER / TEAM SECRET.
    """
    page = _stray(_legion(), event_id="eventId-16858346", new_id="eventId-99999995",
                  rename={"LEGION": "GALAXY RACER", "BLASTERBI": "TEAM SECRET"})

    extract = _extract(page, "Nigma Galaxy", "Team Liquid", 2)

    assert list(extract.odds) == []
    assert "one_side_pair matched" not in (extract.miss_fingerprint or "")


def test_g1_roster_qualifier_is_part_of_the_core(no_aliases):
    """`Aurora Academy` - другой состав: карточка `TEAM AURORA` ему не цена."""
    extract = _extract(_aurora(), "Aurora Academy", "1win", 2)

    assert list(extract.odds) == []
    assert "one_side_pair matched" not in (extract.miss_fingerprint or "")


@pytest.mark.parametrize("ours, card, expected", [
    ("Aurora Gaming", "TEAM AURORA", True),
    ("Level UP esports", "LEVEL UP", True),
    ("Team Nemesis", "NEMESIS", True),
    ("TEAM TPABOMAH", "ТРАВОМАН", True),        # смешение алфавитов: свёртка двойников
    ("Team Zero", "ZERO TENACITY", False),
    ("Nova", "CYBER NOVA", False),
    ("GamerLegion", "LEGION", False),
    ("Aurora Academy", "TEAM AURORA", False),
    ("Team Spirit", "TEAM SPIRIT ACADEMY", False),
])
def test_g1_full_spelling_helper(no_aliases, ours, card, expected):
    assert bk._winline_matched_side_is_full(ours, card) is expected


def test_g1_known_alias_spelling_counts_as_full(monkeypatch, no_aliases):
    monkeypatch.setattr(bk, "_alias_spellings",
                        lambda name: ["BB TEAM"] if name == "BoomBoys" else [])

    assert bk._winline_matched_side_is_full("BoomBoys", "BB TEAM") is True
    assert bk._winline_matched_side_is_full("BoomBoys", "BB STREAMERS") is False


def test_g1_name_proof_keeps_roster_qualifiers_apart(no_aliases):
    """Охрана метки состава самого сопоставителя (отдельно от ядра G1)."""
    snapshot = bk._WinlineDOMSnapshot(_legion()["html"])

    assert bk._winline_name_proven_on(snapshot, "Team Spirit", "TEAM SPIRIT ACADEMY") is False
    assert bk._winline_name_proven_on(snapshot, "Team Spirit Academy", "TEAM SPIRIT") is False
    assert bk._winline_name_proven_on(snapshot, "LEGION", "LEGION") is True


# ---------------------------------------- раунд 2: второе имя похоже на наше (G2)


@pytest.mark.parametrize("ours, card", [
    ("1win", "1W"),                      # приставка
    ("ЯЧЁ123", "YACHE123"),              # транслит
    ("Blasterbl", "BLASTERBI"),          # одна буква (l -> I)
    ("L1GA TEAM", "L1GA"),
    ("Level UP esports", "LEVEL UP"),
    ("Aurora Gaming", "TEAM AURORA"),
    ("Team Nemesis", "NEMESIS"),
    ("Xipto", "XIPTO ESPORTS"),
    ("Tundra Esports", "TUNDRA"),
])
def test_g2_respelling_of_our_other_name_is_accepted(ours, card):
    assert bk._winline_other_side_similar(ours, card) is True


@pytest.mark.parametrize("ours, card", [
    ("Team Liquid", "BLASTERBI"),
    ("Team Liquid", "TEAM SECRET"),
    ("Nigma Galaxy", "GALAXY RACER"),
    ("Team Spirit", "TEAM SPIRIT ACADEMY"),   # остаток 7
    ("OG", "OG.LATAM"),                       # остаток 5
    ("Team Kinetix", "IVORY"),
    ("DIREBORN", "NEMESIS"),
    ("Natus Vincere", "NAVI JUNIOR"),
    ("BetBoom Team", "BB STREAMERS"),
    ("Team Synapse", "TEAM SYNTAX"),          # полное переименование - только ручным алиасом
])
def test_g2_a_different_team_is_refused(ours, card):
    assert bk._winline_other_side_similar(ours, card) is False


def test_g2_legion_vs_team_liquid_does_not_take_the_legion_blasterbi_price(no_aliases):
    """Снимок 05.10: две разные команды с коротким именем LEGION играют одновременно."""
    extract = _extract(_legion(), "LEGION", "Team Liquid", 2)
    mirrored = _extract(_legion(), "Team Liquid", "LEGION", 2)

    assert _refused(extract, "other_not_similar"), (extract.odds, extract.miss_fingerprint)
    assert _refused(mirrored, "other_not_similar"), (mirrored.odds, mirrored.miss_fingerprint)


def test_g2_unknown_opponent_on_the_aurora_card_is_refused(no_aliases):
    extract = _extract(_aurora(), "Aurora Gaming", "Some Unknown Squad", 2)

    assert _refused(extract, "other_not_similar"), (extract.odds, extract.miss_fingerprint)


def test_g2_yache123_pair_names_only(no_aliases):
    """ЯЧЁ123 / YACHE123 (EPL): снимка страницы нет - проверены только имена."""
    assert bk._winline_matched_side_is_full("Cloud Dawning", "CLOUD DAWNING") is True
    assert bk._winline_other_side_similar("ЯЧЁ123", "YACHE123") is True
    assert bk._winline_other_side_similar("ЯЧЁ123", "YACHT CLUB") is False


# ---------------------------------------- раунд 2: копия карточки с другим id (G4)


def test_g4_copy_of_a_card_with_another_event_id_is_a_second_card(no_aliases):
    """Копия настоящего блока LEGION-BLASTERBI (05.10) с другим `eventId-*` и теми же именами."""
    page = _duplicate_card(_legion(), "eventId-16858346", "eventId-99999996")

    extract = _extract(page, "Blasterbl", "LEGION", 2)

    assert list(extract.odds) == []
    assert "one_side_pair=refused:multi_card" in (extract.miss_fingerprint or "")


def test_g4_non_live_twin_with_the_same_names_is_ambiguous(no_aliases):
    """Копия настоящего блока LEGION-BLASTERBI, переведённая в не-live (класс `card--live` снят).

    Разбор цены ищет карточку по именам по всей странице и двойника не отличит,
    поэтому пара с двумя событиями неоднозначна, даже когда живое только одно.
    """
    soup = BeautifulSoup(_legion()["html"], "html.parser")
    clone = copy.copy(soup.find(id="eventId-16858346"))
    clone["id"] = "eventId-99999997"
    for node in clone.select(".card--live"):
        node["class"] = [c for c in node["class"] if c != "card--live"]
    soup.find(id="eventId-16858346").insert_after(clone)
    page = {"text": " ".join(soup.get_text(" ", strip=True).split()), "html": str(soup)}

    extract = _extract(page, "Blasterbl", "LEGION", 2)

    assert list(extract.odds) == []
    assert "one_side_pair=refused:multi_card" in (extract.miss_fingerprint or "")


def _aurora_with_feed_copies(count: int) -> dict:
    """Страница 08.10 (закреплённая `TEAM AURORA 1W` + live-центр) и `count` ленточных
    карточек `TEAM AURORA 1W` с разными id - копии настоящего блока PARIVISION-TEAM YANDEX."""
    page = _aurora()
    for index in range(count):
        page = _duplicate_card(page, "eventId-16889364", f"eventId-9999980{index}",
                               {"PARIVISION": "TEAM AURORA", "TEAM YANDEX": "1W"})
    return page


def _pair_events(page: dict) -> list:
    snapshot = bk._WinlineDOMSnapshot(page["html"])
    return [e for e in bk._winline_one_side_events(snapshot)
            if e["names"] == ("TEAM AURORA", "1W") and not e["prop"]]


def _live_feed_copies(page: dict) -> dict:
    """Ленточные копии пары помечены живыми (`card` -> `card card--live`, как у LEGION 05.10)."""
    soup = BeautifulSoup(page["html"], "html.parser")
    for node in soup.find_all(id=lambda value: bool(value) and str(value).startswith("eventId-999998")):
        card = node.select_one(".card")
        card["class"] = list(card["class"]) + ["card--live"]
    return {"text": " ".join(soup.get_text(" ", strip=True).split()), "html": str(soup)}


def _aurora_with_live_feed_card(event_id: str) -> dict:
    """Страница 08.10 + ЖИВАЯ ленточная карточка `TEAM AURORA 1W` с id `event_id` (копия блока
    PARIVISION-TEAM YANDEX, `card` -> `card card--live`). У настоящего события id 16855095 - он же в
    ссылках логотипов закреплённой карточки и live-центра (`/api/cls/event/1/16855095`)."""
    page = _duplicate_card(_aurora(), "eventId-16889364", event_id,
                           {"PARIVISION": "TEAM AURORA", "TEAM YANDEX": "1W"})
    soup = BeautifulSoup(page["html"], "html.parser")
    card = soup.find(id=event_id).select_one(".card")
    card["class"] = list(card["class"]) + ["card--live"]
    return {"text": " ".join(soup.get_text(" ", strip=True).split()), "html": str(soup)}


def test_g4_pinned_block_merges_into_exactly_one_live_feed_card_with_the_pair():
    """Одна ЖИВАЯ ленточная карточка пары с тем же id события: блоки вливаются в неё."""
    events = _pair_events(_aurora_with_live_feed_card("eventId-16855095"))

    assert len(events) == 1
    assert events[0]["live"] is True
    assert events[0]["ids"] == {"16855095"} and not events[0]["conflict"]
    # лента + закреплённая + live-центр (внешний компонент и вложенная секция)
    assert any(node.get("id") == "eventId-16855095" for node in events[0]["nodes"])
    assert any(node.name == "ww-pinned-card" for node in events[0]["nodes"])


def test_g4_block_with_another_event_id_does_not_merge_into_the_live_feed_card():
    """Живая ленточная карточка пары с ДРУГИМ id (99999800), чем в ссылках логотипов блоков
    (16855095): одинаковые имена - не доказательство одного события (ревью astra, раунд 4)."""
    events = _pair_events(_aurora_with_live_feed_card("eventId-99999800"))

    assert len(events) == 2
    assert sorted(sorted(e["ids"]) for e in events) == [["16855095"], ["99999800"]]


def _relogo(page: dict, selector: str, new_id: str) -> dict:
    """В блоке `selector` id события в ссылках логотипов заменён на `new_id`."""
    soup = BeautifulSoup(page["html"], "html.parser")
    hits = 0
    for root in soup.select(selector):
        for node in root.find_all(True):
            for attr, value in list(node.attrs.items()):
                if isinstance(value, str) and "/api/cls/event/" in value and "16855095" in value:
                    node[attr] = value.replace("16855095", new_id)
                    hits += 1
    assert hits >= 1
    return {"text": " ".join(soup.get_text(" ", strip=True).split()), "html": str(soup)}


def test_pinned_and_live_center_of_different_events_are_not_one_event(no_aliases):
    """Сценарий astra (раунд 4): у live-центра в ссылках логотипов id другого события
    (99999777). Закреплённая карточка и live-центр - два события пары -> цены нет."""
    page = _relogo(_aurora(), "ww-feature-event-live-center-dsk", "99999777")

    events = _pair_events(page)
    extract = _extract(page, "Aurora Gaming", "1win", 2)

    assert len(events) == 2
    assert list(extract.odds) == []
    assert "one_side_pair matched" not in (extract.miss_fingerprint or "")


def test_block_with_two_event_ids_is_an_inconsistent_snapshot(no_aliases):
    """В одной закреплённой карточке логотипы двух разных событий: снимок несогласован."""
    soup = BeautifulSoup(_aurora()["html"], "html.parser")
    pinned = soup.select_one("ww-pinned-card")
    changed = 0
    for node in pinned.find_all(True):
        for attr, value in list(node.attrs.items()):
            if isinstance(value, str) and "/api/cls/event/2/16855095" in value:
                node[attr] = value.replace("/api/cls/event/2/16855095", "/api/cls/event/2/99999778")
                changed += 1
    assert changed >= 1
    page = {"text": " ".join(soup.get_text(" ", strip=True).split()), "html": str(soup)}
    for node in soup.select("ww-feature-event-live-center-dsk, .event-live-center"):
        node.decompose()
    page = {"text": " ".join(soup.get_text(" ", strip=True).split()), "html": str(soup)}

    extract = _extract(page, "Aurora Gaming", "1win", 2)

    assert list(extract.odds) == []
    assert "one_side_pair=refused:id_conflict" in (extract.miss_fingerprint or "")


def test_g4_live_block_does_not_merge_into_a_non_live_feed_card_of_the_pair():
    """Ленточная карточка пары НЕ живая (другой, будущий матч той же пары): живой блок без id
    в неё не вливается - иначе её узлы попадают во фрагмент цены (ревью astra 08.10, раунд 3)."""
    events = _pair_events(_aurora_with_feed_copies(1))

    assert len(events) == 2
    assert sorted(e["live"] for e in events) == [False, True]


def test_price_never_comes_from_a_future_match_of_the_same_pair(no_aliases):
    """Сценарий astra (раунд 3): ленточная карточка пары - будущий матч, её ряд переписан на
    `2 карта` с открытыми кнопками, а кнопки live-центра закрыты. Цены быть не должно."""
    page = _aurora_with_feed_copies(1)
    soup = BeautifulSoup(page["html"], "html.parser")
    feed = soup.find(id="eventId-99999800")
    relabelled = 0
    for node in feed.find_all(string=True):
        if str(node).strip() == "1 карта":
            node.replace_with(str(node).replace("1 карта", "2 карта"))
            relabelled += 1
    assert relabelled >= 1
    locked = 0
    for root in soup.select("ww-feature-event-live-center-dsk, .event-live-center"):
        for button in root.select(".odd-btn"):
            if "coef-btn--locked" not in (button.get("class") or []):
                button["class"] = list(button.get("class") or []) + ["coef-btn--locked"]
                locked += 1
    assert locked >= 2
    page = {"text": " ".join(soup.get_text(" ", strip=True).split()), "html": str(soup)}

    extract = _extract(page, "Aurora Gaming", "1win", 2)

    assert list(extract.odds) == []
    assert "one_side_pair matched" not in (extract.miss_fingerprint or "")


def test_g4_pinned_block_does_not_merge_when_two_feed_cards_have_the_pair():
    """Две ленточные карточки пары: закреплённая не вливается ни в одну (третье событие)."""
    events = _pair_events(_aurora_with_feed_copies(2))

    assert len(events) == 3
    assert sum(1 for e in events if e["live"]) == 1


def test_g4_pinned_card_and_live_center_alone_are_one_event(no_aliases):
    events = _pair_events(_aurora())

    assert len(events) == 1 and events[0]["live"] is True


# ---------------------------------------- раунд 2: метка карты (G3)


def _relabelled(page: dict, selector: str, old: str, new: str) -> dict:
    soup = BeautifulSoup(page["html"], "html.parser")
    hit = 0
    for node in soup.select(selector):
        for text_node in list(node.find_all(string=True)):
            if old in str(text_node):
                text_node.replace_with(str(text_node).replace(old, new))
                hit += 1
    assert hit, "label not found in the captured page"
    return {"text": " ".join(soup.get_text(" ", strip=True).split()), "html": str(soup)}


def test_g3_feed_card_live_on_another_map_is_refused(no_aliases):
    """Шапка живой карточки LEGION-BLASTERBI: `2карта` -> `3карта` (правка снимка 05.10)."""
    page = _relabelled(_legion(), "#eventId-16858346 .header-left__time", "2карта", "3карта")

    extract = _extract(page, "Blasterbl", "LEGION", 2)

    assert _refused(extract, "live_map_mismatch"), (extract.odds, extract.miss_fingerprint)


def test_g3_pinned_card_live_on_another_map_is_refused(no_aliases):
    page = _relabelled(_aurora(), "ww-pinned-card", "2карта", "1карта")

    extract = _extract(page, "Aurora Gaming", "1win", 2)

    assert _refused(extract, "live_map_mismatch"), (extract.odds, extract.miss_fingerprint)


def test_g3_matching_label_prices(no_aliases):
    assert list(_extract(_legion(), "Blasterbl", "LEGION", 2).odds) == [1.70, 2.02]
    assert list(_extract(_aurora(), "Aurora Gaming", "1win", 2).odds) == [1.61, 2.22]


def test_g3_card_without_a_label_is_not_judged_by_it(no_aliases):
    """Метка есть не везде (на странице 10.09 у CYBER NOVA её нет): без неё проверки нет."""
    page = _relabelled(_legion(), "#eventId-16858346 .header-left__time", "2карта", "")

    extract = _extract(page, "Blasterbl", "LEGION", 2)

    assert list(extract.odds) == [1.70, 2.02]


# ---------------------------------------------------------------- приёмка и логи


def test_poller_accepts_a_fallback_result_but_not_a_foreign_map(no_aliases):
    extract = _extract(_aurora(), "Aurora Gaming", "1win", 2)
    identity = {"series": "s", "map_num": 2, "team1": "Aurora Gaming", "team2": "1win"}
    result = {
        "market_status": "open",
        "source": "winline_current_map_winner",
        "p1_odds": extract.odds[0],
        "p2_odds": extract.odds[1],
        "map_num": extract.map_num,
        "page_valid": True,
        "team1": "Aurora Gaming",
        "team2": "1win",
    }
    assert _odds_accepted(result, identity=identity) is True
    assert _odds_accepted({**result, "map_num": 3}, identity=identity) is False


def test_log_line_is_written_once_per_pair_and_map(no_aliases, caplog):
    caplog.set_level("INFO")
    for _ in range(3):
        _extract(_aurora(), "Aurora Gaming", "1win", 2)

    lines = [r for r in caplog.records if "one_side_pair" in r.getMessage()]
    assert len(lines) == 1, [r.getMessage() for r in lines]


# ------------------------------------------------- история и прод-путь (быстрый разбор)


class _Recorder:
    """Достаточно того, что читает `_record_history` (как в test_winline_odds_history)."""

    def __init__(self):
        self._identity = {"url": "dltv.org/matches/x", "map_num": 2}
        self._canonical_key = "x|map2"
        self._history_last_key = None


def _history_rows(monkeypatch, tmp_path, attempt: dict) -> list:
    from services.winline import winline_current_map_odds_poller as pm

    path = tmp_path / "hist.jsonl"
    monkeypatch.setattr(pm, "WINLINE_ODDS_HISTORY_PATH", str(path))
    pm.WinlineCurrentMapOddsPoller._record_history(_Recorder(), attempt)
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def _open_attempt(extract) -> dict:
    return {
        "wall": 1000.0,
        "attempt_index": 1,
        "p1_odds": extract.odds[0],
        "p2_odds": extract.odds[1],
        "card_odds": list(extract.card_odds),
        "card_team_order": extract.card_team_order,
        "market_status": "open",
        "selected_side": "radiant",
        "accepted": True,
        "source": "winline_current_map_winner",
        "miss_fingerprint": extract.miss_fingerprint or None,
        "match_found": True,
        "page_valid": True,
    }


def test_marker_reaches_the_odds_history_row_of_an_open_market(no_aliases, monkeypatch, tmp_path):
    extract = _extract(_aurora(), "Aurora Gaming", "1win", 2)

    rows = _history_rows(monkeypatch, tmp_path, _open_attempt(extract))

    assert len(rows) == 1
    assert rows[0]["p1_odds"] == 1.61 and rows[0]["p2_odds"] == 2.22
    assert rows[0]["miss_fingerprint"] == (
        "one_side_pair matched=Aurora Gaming->TEAM AURORA other=1win->1W"
    )
    # Только метка: прочие поля причины промаха у открытого рынка по-прежнему не пишутся.
    assert "match_found" not in rows[0] and "parser_failure_reasons" not in rows[0]


def test_both_name_open_market_row_stays_lean(monkeypatch, tmp_path):
    extract = _extract(_aurora(), "Aurora Gaming", "1win", 2)  # справочник на месте

    rows = _history_rows(monkeypatch, tmp_path, _open_attempt(extract))

    assert "miss_fingerprint" not in rows[0]


@pytest.fixture(autouse=True)
def _service_line_sink(monkeypatch):
    """Служебный чат в тестах — список, а не сеть; память «уже писали» — с нуля."""
    import cyberscore_try as cs

    sent = []
    monkeypatch.setattr(cs, "send_winline_odds_message",
                        lambda message, **kwargs: sent.append(message) or True)
    monkeypatch.setattr(cs, "_winline_one_side_pair_noted", set(), raising=False)
    monkeypatch.setattr(cs, "_winline_one_side_pair_attempts", {}, raising=False)
    monkeypatch.setattr(cs, "_winline_one_side_pair_inflight", set(), raising=False)
    # Журнал отправленных строк пишет в runtime/ — в тестах не трогаем.
    monkeypatch.setattr(cs, "_winline_journal_sent_message", lambda **kwargs: None)
    return sent


def _fast_collect(team1: str, team2: str, map_num: int = 2) -> dict:
    import cyberscore_try as cs

    page = _aurora()
    return cs._winline_fast_collect_from_payload(
        {"html": page["html"], "url": "https://winline.ru/stavki/sport/kibersport/dota_2"},
        series="sourcetv:league:19102|id:9255039|id:9467224",
        map_num=map_num,
        team1=team1,
        team2=team2,
        expected_url="https://winline.ru/stavki/sport/kibersport/dota_2",
    )


def test_prod_fast_path_delivers_oriented_odds_and_the_poller_accepts_them(no_aliases):
    """Прод-путь `_winline_fast_collect_from_payload` на снимке 08.10 без алиаса `1win`."""
    out = _fast_collect("Aurora Gaming", "1win")
    mirrored = _fast_collect("1win", "Aurora Gaming")

    assert out is not None and out["market_status"] == "open"
    assert (out["p1_odds"], out["p2_odds"]) == (1.61, 2.22)
    assert (mirrored["p1_odds"], mirrored["p2_odds"]) == (2.22, 1.61)
    assert out["card_team_order"] == "Aurora Gaming|1win"
    assert out["card_odds"] == [1.61, 2.22]
    assert out["map_num"] == 2
    identity = {"series": "s", "map_num": 2, "team1": "Aurora Gaming", "team2": "1win"}
    assert _odds_accepted(out, identity=identity) is True


def test_prod_fast_path_carries_the_marker_into_the_collector_dict(no_aliases):
    out = _fast_collect("Aurora Gaming", "1win")

    assert str(out.get("miss_fingerprint") or "").startswith("one_side_pair matched=Aurora Gaming->")


def test_one_side_price_sends_one_service_line_per_pair_and_map(no_aliases, _service_line_sink):
    """Цена по одной команде -> ровно одна строка в служебный чат на пару+карту."""
    _fast_collect("Aurora Gaming", "1win")
    _fast_collect("1win", "Aurora Gaming")  # та же пара в обратном порядке

    assert len(_service_line_sink) == 1
    line = _service_line_sink[0]
    assert "TEAM AURORA" in line and "1W" in line and "team_name_aliases" in line


def test_both_name_price_sends_no_service_line(_service_line_sink):
    out = _fast_collect("Aurora Gaming", "1win")  # справочник на месте: 1win -> 1W

    assert (out["p1_odds"], out["p2_odds"]) == (1.61, 2.22)
    assert not str(out.get("miss_fingerprint") or "").startswith("one_side_pair")
    assert _service_line_sink == []


def test_service_line_rollback_flag(no_aliases, _service_line_sink, monkeypatch):
    monkeypatch.setenv("WINLINE_ONE_SIDE_PAIR_TG", "0")

    out = _fast_collect("Aurora Gaming", "1win")

    assert (out["p1_odds"], out["p2_odds"]) == (1.61, 2.22)
    assert _service_line_sink == []


def test_failed_service_send_is_retried_at_most_three_times(no_aliases, monkeypatch):
    """Сбой отправки не глушит строку навсегда: повтор до 3 попыток, затем тишина."""
    import cyberscore_try as cs

    calls = []
    monkeypatch.setattr(cs, "send_winline_odds_message",
                        lambda message, **kwargs: (calls.append(message), False)[1])
    for _ in range(5):
        _fast_collect("Aurora Gaming", "1win")
    assert len(calls) == 3

    monkeypatch.setattr(cs, "_winline_one_side_pair_attempts", {})
    monkeypatch.setattr(cs, "send_winline_odds_message",
                        lambda message, **kwargs: calls.append(message) or True)
    _fast_collect("Aurora Gaming", "1win")
    _fast_collect("Aurora Gaming", "1win")
    assert len(calls) == 4  # доставлено один раз, дальше ключ «доставлено»


def test_one_side_price_keeps_card_provenance_orientation(no_aliases):
    """card_team_order = НАШИ имена в порядке Winline (контракт пути двух имён):
    по нему стабилизатор доказывает ориентацию, а не включает временную эвристику
    переворота пары (round 2 F4 сломал это, положив имена Winline)."""
    import cyberscore_try as cs

    key = "sourcetv:league:19102|id:9255039|id:9467224|map2|Aurora Gaming|1win"
    out = cs._winline_stabilize_odds_orientation(dict(_fast_collect("Aurora Gaming", "1win")), key)
    assert out["odds_orientation_source"] == "card_provenance"
    assert (out["p1_odds"], out["p2_odds"]) == (1.61, 2.22)


# ------------------------------------------- раунд 3: цена только из DOM выбранного события


def _spirit_live_locked_and_academy_line_open() -> dict:
    """Снимок 05.10, правка: живая карточка `TEAM SPIRIT 1W` с ЗАКРЫТЫМИ кнопками 2-й карты
    (настоящий блок LEGION-BLASTERBI переименован, кнопки 2.02/1.70 помечены `_locked`) и
    рядом её копия `TEAM SPIRIT ACADEMY 1W` - НЕ живая (класс `card--live` снят), с теми же
    открытыми кнопками. Ревью astra 08.10 (раунд 2): повторный разбор по именам карточки шёл
    по всей странице и отдавал паре Team Spirit-1win цену линии академии [2.02, 1.70].
    """
    page = _duplicate_card(_legion(), "eventId-16858346", "eventId-99999995",
                           {"LEGION": "TEAM SPIRIT ACADEMY", "BLASTERBI": "1W"})
    soup = BeautifulSoup(page["html"], "html.parser")
    live = soup.find(id="eventId-16858346")
    for text_node in list(live.find_all(string=True)):
        value = str(text_node)
        if value.strip() == "LEGION":
            text_node.replace_with(value.replace("LEGION", "TEAM SPIRIT"))
        elif value.strip() == "BLASTERBI":
            text_node.replace_with(value.replace("BLASTERBI", "1W"))
    locked = 0
    for node in live.select(".coefficient-button"):
        if node.get_text(strip=True) in {"2.02", "1.70"}:
            node["class"] = list(node["class"]) + ["coefficient-button_locked"]
            locked += 1
    assert locked == 2
    line = soup.find(id="eventId-99999995")
    unlived = 0
    for node in [line] + list(line.find_all(True)):
        classes = list(node.get("class") or [])
        if "card--live" in classes:
            node["class"] = [c for c in classes if c != "card--live"]
            unlived += 1
    assert unlived >= 1
    return {"text": " ".join(soup.get_text(" ", strip=True).split()), "html": str(soup)}


def test_price_comes_only_from_the_chosen_live_card(no_aliases):
    page = _spirit_live_locked_and_academy_line_open()
    events = bk._winline_one_side_events(bk._WinlineDOMSnapshot(page["html"]))
    by_names = {event["names"]: event["live"] for event in events}
    # Фикстура такая, как задумано: живая пара закрыта, линия академии не живая.
    assert by_names.get(("TEAM SPIRIT", "1W")) is True
    assert by_names.get(("TEAM SPIRIT ACADEMY", "1W")) is False

    extract = _extract(page, "Team Spirit", "1win", 2)

    assert list(extract.odds) == []
    assert "one_side_pair matched" not in (extract.miss_fingerprint or "")
    assert "one_side_pair=refused:no_priced_row" in (extract.miss_fingerprint or "")


def test_open_live_card_still_prices_next_to_an_academy_line(no_aliases):
    """Положительный контроль той же правки: у живой карточки кнопки открыты."""
    page = _duplicate_card(_legion(), "eventId-16858346", "eventId-99999996",
                           {"LEGION": "TEAM SPIRIT ACADEMY", "BLASTERBI": "1W"})
    soup = BeautifulSoup(page["html"], "html.parser")
    live = soup.find(id="eventId-16858346")
    for text_node in list(live.find_all(string=True)):
        value = str(text_node)
        if value.strip() == "LEGION":
            text_node.replace_with(value.replace("LEGION", "TEAM SPIRIT"))
        elif value.strip() == "BLASTERBI":
            text_node.replace_with(value.replace("BLASTERBI", "1W"))
    line = soup.find(id="eventId-99999996")
    for node in [line] + list(line.find_all(True)):
        classes = list(node.get("class") or [])
        if "card--live" in classes:
            node["class"] = [c for c in classes if c != "card--live"]
    page = {"text": " ".join(soup.get_text(" ", strip=True).split()), "html": str(soup)}

    extract = _extract(page, "Team Spirit", "1win", 2)

    assert list(extract.odds) == [2.02, 1.70]
    assert extract.miss_fingerprint.startswith("one_side_pair matched=Team Spirit->TEAM SPIRIT ")


def test_concurrent_service_lines_for_one_pair_and_map_send_once(no_aliases, monkeypatch):
    """Второй поток опроса той же пары+карты, пока первый ещё шлёт: строка уходит один раз.

    Ревью astra 08.10: замок отпускался до отправки, оба потока слали и оба получали True.
    Без опоры на тайминги: поток B запускается, когда A уже внутри отправки, и B должен
    закончиться САМ, пока A держится; при дефекте B застревает в той же отправке.
    """
    import threading

    import cyberscore_try as cs

    monkeypatch.setattr(cs, "_winline_one_side_pair_noted", set())
    monkeypatch.setattr(cs, "_winline_one_side_pair_attempts", {})
    monkeypatch.setattr(cs, "_winline_one_side_pair_inflight", set(), raising=False)
    monkeypatch.setattr(cs, "_winline_journal_sent_message", lambda *a, **k: None, raising=False)
    inside = threading.Event()
    release = threading.Event()
    calls = []

    def _slow_send(message, **kwargs):
        calls.append(message)
        inside.set()
        release.wait(10)
        return True

    monkeypatch.setattr(cs, "send_winline_odds_message", _slow_send)
    results = {}

    def _note(name):
        results[name] = cs._winline_note_one_side_pair("Aurora Gaming", "1win", 2, "one_side_pair matched=x")

    first = threading.Thread(target=_note, args=("A",))
    first.start()
    assert inside.wait(10)
    second = threading.Thread(target=_note, args=("B",))
    second.start()
    second.join(5)
    b_finished_alone = not second.is_alive()
    release.set()
    first.join(10)
    second.join(10)

    assert b_finished_alone
    assert len(calls) == 1
    assert results == {"A": True, "B": False}


def test_failed_send_releases_the_in_flight_mark(no_aliases, monkeypatch):
    """Отправка упала исключением: пара+карта не остаётся «в полёте», следующая попытка шлёт."""
    import cyberscore_try as cs

    monkeypatch.setattr(cs, "_winline_one_side_pair_noted", set())
    monkeypatch.setattr(cs, "_winline_one_side_pair_attempts", {})
    monkeypatch.setattr(cs, "_winline_one_side_pair_inflight", set(), raising=False)
    outcomes = iter([RuntimeError("telegram down"), True])
    sent = []

    def _send(message, send_fn, **kwargs):
        outcome = next(outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        sent.append(message)
        return outcome

    monkeypatch.setattr(cs, "_winline_send_lifecycle_message", _send)

    assert cs._winline_note_one_side_pair("Aurora Gaming", "1win", 2, "m") is False
    assert not cs._winline_one_side_pair_inflight
    assert cs._winline_note_one_side_pair("Aurora Gaming", "1win", 2, "m") is True
    assert len(sent) == 1


def test_block_with_a_start_time_never_merges_into_the_live_feed_card():
    """Живая ленточная карточка пары и блок live-центра с датой начала (`Сегодня 13:00`, правка
    снимка 08.10): блок показывает будущий матч той же пары и в живое событие не вливается."""
    page = _live_feed_copies(_aurora_with_feed_copies(1))
    soup = BeautifulSoup(page["html"], "html.parser")
    for node in soup.select("ww-pinned-card"):
        node.decompose()
    header = soup.select_one(".event-live-center__header")
    header.append(soup.new_string(" Сегодня 13:00 "))
    page = {"text": " ".join(soup.get_text(" ", strip=True).split()), "html": str(soup)}

    events = _pair_events(page)

    assert len(events) == 2
    live = [e for e in events if e["live"]]
    # Живое событие - только ленточная карточка (копия несёт дату PARIVISION в тексте, но
    # живость ленты решает класс `card--live`); блок с датой остался отдельным событием.
    assert len(live) == 1
    assert all(node.get("id") == "eventId-99999800" for node in live[0]["nodes"])


def test_merged_event_prices_from_any_of_its_nodes(no_aliases):
    """Слитое событие (живая лента id 16855095 + закреплённая + live-центр, правка снимка 08.10):
    кнопки live-центра закрыты, ряд `2 карта` с ценой есть только в ленточной карточке (ряд копии
    PARIVISION переписан на `2 карта`). Цена берётся из неё - фрагмент несёт все узлы события."""
    page = _aurora_with_live_feed_card("eventId-16855095")
    soup = BeautifulSoup(page["html"], "html.parser")
    feed = soup.find(id="eventId-16855095")
    relabelled = 0
    for node in feed.find_all(string=True):
        if str(node).strip() == "1 карта":
            node.replace_with(str(node).replace("1 карта", "2 карта"))
            relabelled += 1
    assert relabelled >= 1
    for root in soup.select("ww-feature-event-live-center-dsk, .event-live-center"):
        for button in root.select(".odd-btn"):
            if "coef-btn--locked" not in (button.get("class") or []):
                button["class"] = list(button.get("class") or []) + ["coef-btn--locked"]
    page = {"text": " ".join(soup.get_text(" ", strip=True).split()), "html": str(soup)}

    events = _pair_events(page)
    extract = _extract(page, "Aurora Gaming", "1win", 2)

    assert len(events) == 1
    assert len(list(extract.odds)) == 2
    assert extract.miss_fingerprint.startswith("one_side_pair matched=Aurora Gaming->TEAM AURORA ")


def test_failing_print_does_not_leave_the_pair_in_flight(no_aliases, monkeypatch):
    """LOW из проверки Opus (раунд 3): print между отметкой «в полёте» и try. Теперь try открыт
    сразу после отметки - упавший print отметку не оставляет, следующая попытка шлёт."""
    import builtins

    import cyberscore_try as cs

    monkeypatch.setattr(cs, "_winline_one_side_pair_noted", set())
    monkeypatch.setattr(cs, "_winline_one_side_pair_attempts", {})
    monkeypatch.setattr(cs, "_winline_one_side_pair_inflight", set(), raising=False)
    monkeypatch.setattr(cs, "_winline_send_lifecycle_message", lambda message, send_fn, **kw: True)
    real_print = builtins.print

    def _broken_print(*args, **kwargs):
        raise OSError("stdout closed")

    monkeypatch.setattr(builtins, "print", _broken_print)
    assert cs._winline_note_one_side_pair("Aurora Gaming", "1win", 2, "m") is False
    monkeypatch.setattr(builtins, "print", real_print)

    assert not cs._winline_one_side_pair_inflight
    assert cs._winline_note_one_side_pair("Aurora Gaming", "1win", 2, "m") is True


# ------------- раунд 5 (проверка Opus): защиты слияния по живости/дате - без помощи id логотипов


def _without_block_ids(page: dict) -> dict:
    """Ссылки логотипов закреплённой карточки и live-центра без id события (`/api/cls/event/N/<id>`
    -> `/api/cls/logo/N`): проверка id молчит, решают только живость ленты и дата начала."""
    soup = BeautifulSoup(page["html"], "html.parser")
    hits = 0
    for root in soup.select("ww-pinned-card, ww-feature-event-live-center-dsk"):
        for node in root.find_all(True):
            for attr, value in list(node.attrs.items()):
                if isinstance(value, str) and "/api/cls/event/" in value:
                    node[attr] = re.sub(r"/api/cls/event/(\d+)/\d+", r"/api/cls/logo/\1", value)
                    hits += 1
    assert hits >= 2
    return {"text": " ".join(soup.get_text(" ", strip=True).split()), "html": str(soup)}


def test_idless_live_block_does_not_merge_into_a_non_live_feed_card():
    events = _pair_events(_without_block_ids(_aurora_with_feed_copies(1)))

    assert all(not e["ids"] or e["ids"] == {"99999800"} for e in events)
    assert len(events) == 2
    assert sorted(e["live"] for e in events) == [False, True]


def test_idless_future_match_price_is_never_taken(no_aliases):
    page = _aurora_with_feed_copies(1)
    soup = BeautifulSoup(page["html"], "html.parser")
    feed = soup.find(id="eventId-99999800")
    for node in feed.find_all(string=True):
        if str(node).strip() == "1 карта":
            node.replace_with(str(node).replace("1 карта", "2 карта"))
    for root in soup.select("ww-feature-event-live-center-dsk, .event-live-center"):
        for button in root.select(".odd-btn"):
            if "coef-btn--locked" not in (button.get("class") or []):
                button["class"] = list(button.get("class") or []) + ["coef-btn--locked"]
    page = _without_block_ids({"text": "", "html": str(soup)})

    extract = _extract(page, "Aurora Gaming", "1win", 2)

    assert list(extract.odds) == []
    assert "one_side_pair matched" not in (extract.miss_fingerprint or "")


def test_idless_block_with_a_start_time_does_not_merge_into_the_live_feed_card():
    page = _live_feed_copies(_aurora_with_feed_copies(1))
    soup = BeautifulSoup(page["html"], "html.parser")
    for node in soup.select("ww-pinned-card"):
        node.decompose()
    soup.select_one(".event-live-center__header").append(soup.new_string(" Сегодня 13:00 "))
    page = _without_block_ids({"text": "", "html": str(soup)})

    events = _pair_events(page)

    live = [e for e in events if e["live"]]
    assert len(events) == 2 and len(live) == 1
    assert all(node.get("id") == "eventId-99999800" for node in live[0]["nodes"])


def test_idless_dated_block_next_to_the_live_feed_card_gives_no_price(no_aliases):
    """Тот же снимок на уровне цены (Opus r6, LOW): живая карточка ленты и live-центр с датой начала -
    два события одной пары, цены нет. Если дату не проверять при слиянии (мутант X1), блок
    прилипает к ленте и цена ленты [1.61, 2.22] уходит в мост как цена одной стороны."""
    page = _live_feed_copies(_aurora_with_feed_copies(1))
    soup = BeautifulSoup(page["html"], "html.parser")
    for node in soup.select("ww-pinned-card"):
        node.decompose()
    soup.select_one(".event-live-center__header").append(soup.new_string(" Сегодня 13:00 "))
    page = _without_block_ids({"text": "", "html": str(soup)})

    extract = _extract(page, "Aurora Gaming", "1win", 2)

    assert list(extract.odds) == []
    assert "one_side_pair=refused:multi_card" in (extract.miss_fingerprint or "")


def test_idless_pinned_card_and_a_dated_live_center_are_two_events():
    """Закреплённая (живая, без даты) и live-центр с датой начала, оба без id: разные события."""
    soup = BeautifulSoup(_aurora()["html"], "html.parser")
    soup.select_one(".event-live-center__header").append(soup.new_string(" Завтра 15:00 "))
    page = _without_block_ids({"text": "", "html": str(soup)})

    events = _pair_events(page)

    assert len(events) == 2
    assert sorted(e["future"] for e in events) == [False, True]


def test_live_and_future_copies_of_one_feed_id_are_an_id_conflict(no_aliases):
    """Карточка LEGION-BLASTERBI (05.10) дважды с ОДНИМ id: живая и копия без `card--live` с датой."""
    page = _duplicate_card(_legion(), "eventId-16858346", "eventId-16858346")
    soup = BeautifulSoup(page["html"], "html.parser")
    copy_node = soup.find_all(id="eventId-16858346")[1]
    card = copy_node.select_one(".card")
    card["class"] = [c for c in card["class"] if c != "card--live"]
    copy_node.select_one(".header-left__time").append(soup.new_string(" Сегодня 23:00 "))
    page = {"text": " ".join(soup.get_text(" ", strip=True).split()), "html": str(soup)}

    extract = _extract(page, "Blasterbl", "LEGION", 2)

    assert list(extract.odds) == []
    assert "one_side_pair=refused:id_conflict" in (extract.miss_fingerprint or "")
