"""Границы карточки Winline: свои кэфы не теряются, чужие не подмешиваются.

Обе регрессии найдены 2026-07-25 замером на живой странице (дамп 563 КБ, десяток
матчей одновременно), а не рассуждением:

1. Пара команд встречалась в 39 элементах, а сужение брало ОДИН — минимальный по
   длине текста. Первым из четырёх одинаковых оказывался элемент верхнего
   закреплённого бара, подъём от которого приходит в контейнер с навигацией и
   витриной, где предохранители обязаны отказать. Итог: у матча FOKUS-MOUZ рынок
   2-й карты на странице был ('2 карта 4.90 1.18'), а функция отдавала пустую
   карточку в 10 символов. Кэфы терялись в ОБОИХ путях — и в быстром съёме, и в
   полном разборе, то есть молчание вместо цены.

2. Как только перебор кандидатов включили, вылезла обратная беда: 'Counter-Strike |'
   — заголовок ТУРНИРА, общий для нескольких матчей подряд, поэтому узел с тремя
   матчами проходил как одна карточка и отдавал 'Победитель 3 карта 1.87 1.83' —
   цены ТРЕТЬЕГО матча в серии строк. Это уже не молчание, а ставка по чужим кэфам.

Фикстура первого случая — вырезка из настоящей страницы (закреплённый бар, витрина
и строка списка), потому что рукописная вёрстка дефект не воспроизводила: она
отдавала кэфы и на коде до правки, то есть тест ничего бы не охранял. Проверено:
на бэкапе модуля вырезка даёт [] и 10-символьную карточку, на текущем — [4.90, 1.18].
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import bookmaker_selenium_odds as odds  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")

with open(
    os.path.join(FIXTURES, "winline_pinned_bar_shadows_card.html"), encoding="utf-8"
) as handle:
    # Живая вёрстка: тот же матч и строкой в закреплённом баре сверху (минимальный
    # элемент с обеими командами), и настоящей карточкой в списке ниже.
    CARD_SHADOWED_BY_PINNED_BAR = handle.read()

# Три матча подряд под ОДНИМ заголовком турнира — каждый своей строкой, как на
# живой странице. Строка рынка 3-й карты принадлежит последнему из них.
# Проверено, что фикстура не пустая: с выключенным счётом строк матч-рынка она
# отдаёт [1.87, 1.83] первому матчу, то есть кэфы соседа.
THREE_MATCHES_UNDER_ONE_HEADER = """
<html><body>
  <div><div>Counter-Strike | CCT EU 3
    <div><span>VP.PRODIGY 163ON</span> 3карта 1 1 13 15 3К 16 12 2К 10 13 1К
      <span>Матч - - - - - - - -</span></div>
    <div><span>ROUNDSGG PRIVATEER</span> Пер.2 1 1 8 13 2К 13 10 1К
      <span>Матч 2.40 1.50 - - - - - -</span></div>
    <div><span>EX-RUSTEC RUNE EATERS</span> Пер. +8 1 1 13 10 2К 6 13 1К
      <span>Матч 1.87 1.83 - - - - - - 3 карта 1.87 1.83 1.57 + 2.5 - 2.25 1.76 21.5 1.95</span>
    </div>
  </div></div>
</body></html>
"""


def _odds_for(html, team1, team2, map_num):
    card = odds._winline_matched_card_context("", team1, team2, html=html, map_num=map_num)
    extracted = odds._extract_winline_current_map_winner(
        card or "", team1, team2, forced_map_num=map_num
    )
    return card, list(extracted.odds or []), bool(extracted.market_closed)


def test_pinned_bar_does_not_hide_real_card():
    """Кэфы обязаны найтись, хотя минимальный кандидат — строка закреплённого бара."""
    card, found, _closed = _odds_for(CARD_SHADOWED_BY_PINNED_BAR, "FOKUS", "MOUZ", 2)
    assert found == [4.90, 1.18], f"рынок на странице есть, а сужение дало: {card!r}"


def test_neighbour_match_odds_are_never_borrowed():
    """Строка '3 карта 1.87 1.83' принадлежит третьему матчу — первому её нельзя."""
    card, found, _closed = _odds_for(THREE_MATCHES_UNDER_ONE_HEADER, "VP.PRODIGY", "163ON", 3)
    assert found == [], f"взяли чужие кэфы из склеенного узла: {card!r}"


def test_true_owner_of_the_market_row_still_gets_it():
    """Обратная сторона того же инварианта: у владельца строки кэфы забирать нельзя."""
    _card, found, _closed = _odds_for(
        THREE_MATCHES_UNDER_ONE_HEADER, "EX-RUSTEC", "RUNE EATERS", 3
    )
    assert found == [1.87, 1.83]


def test_absent_map_market_stays_empty():
    """Карты, которой нет в серии, рынок отдавать не должен — это и есть отсутствие."""
    _card, found, _closed = _odds_for(CARD_SHADOWED_BY_PINNED_BAR, "FOKUS", "MOUZ", 5)
    assert found == []


def test_absence_is_decided_over_the_whole_page():
    """Отсутствие доказывается по всей странице: ни один кандидат не дал рынок.

    Иначе «рынка нет» означало бы всего лишь «мне дали слишком узкий кусок DOM».
    """
    card, found, _closed = _odds_for(CARD_SHADOWED_BY_PINNED_BAR, "FOKUS", "MOUZ", 4)
    assert found == []
    # Карточка всё же найдена — значит отсутствие относится к рынку, а не к матчу.
    assert card and "FOKUS" in card and "MOUZ" in card


def test_dashes_mean_closed_market_not_missing_card():
    """Прочерки вместо цен — закрытый рынок при найденной карточке, а не отсутствие."""
    html = """
    <html><body><div><div>Counter-Strike | StarLadder
      <div><span>NINJAS IN PYJAMAS HEROIC</span> 3карта +24 1 1 6 4 3К 13 8 2К 12 16 1К
      <span>Матч 1.49 2.65 - - - - - - 3 карта - - - 1.20 + 2.5 - 3.67 1.79 22.5 1.91</span>
      </div></div></div></body></html>
    """
    card, found, closed = _odds_for(html, "NINJAS IN PYJAMAS", "HEROIC", 3)
    assert found == []
    assert closed is True, f"закрытый рынок принят за отсутствующий: {card!r}"


def test_match_row_label_counts_as_card_boundary():
    """Сам предохранитель: две строки матч-рынка = уже не одна карточка."""
    assert odds._winline_single_card_scope("TEAM A TEAM B Матч 1.50 2.50") is True
    assert (
        odds._winline_single_card_scope(
            "TEAM A TEAM B Матч 1.50 2.50 TEAM C TEAM D Матч 1.87 1.83"
        )
        is False
    )


def test_lowercase_market_phrases_are_not_boundaries():
    """Витринные формулировки со строчной 'матч' границами карточки не являются."""
    text = (
        "FOKUS MOUZ Популярные на матч Все маркеты Тотал матч 1.14 4.90 "
        "Тотал раундов матч 1.77 1.94 Победитель 2 карта 4.90 1.18"
    )
    assert odds._winline_single_card_scope(text) is True


def test_short_team_name_does_not_match_inside_another_word():
    """DOWN — команда, а не суффикс countdown/showdown на странице."""
    assert not odds._text_matches_teams(
        "Team Lynx countdown до начала рынка",
        "Team Lynx",
        "DOWN",
    )
    assert not odds._text_matches_teams(
        "Team Lynx showdown market",
        "Team Lynx",
        "DOWN",
    )
    assert odds._text_matches_teams(
        "Team Lynx DOWN Победитель 1 карта 1.70 2.10",
        "Team Lynx",
        "DOWN",
    )


def test_real_card_after_many_shadow_candidates_is_not_skipped():
    """Наличие >40 теней не должно превращать существующий рынок в missing."""
    shadows = "".join(
        "<div><span>Team Lynx DOWN</span><span>Матч - -</span></div>"
        for _ in range(45)
    )
    real_card = """
      <div>
        <span>Team Lynx DOWN</span>
        <span>Матч - - Победитель 1 карта 1.70 2.10</span>
      </div>
    """
    html = f"<html><body>{shadows}{real_card}</body></html>"

    card, found, closed = _odds_for(html, "Team Lynx", "DOWN", 1)

    assert found == [1.70, 2.10], f"рынок после DOM-теней потерян: {card!r}"
    assert closed is False
