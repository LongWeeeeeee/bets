"""Быстрый съём Winline: отсутствие рынка — вывод, а не догадка.

Отрицательный исход самый частый: у второй карты рынок обычно ещё не выставлен.
Раньше он стоил полного разбора страницы (замер: колбэк 25.12 c при подготовке
браузера 5.10 c), потому что доказать отсутствие по суженному поддереву было
нельзя, а эвристика на блок рынков внутри него на живой карте не срабатывала.

Теперь вход быстрого пути — вся страница, и границы карточки считает та же
функция, что в полном разборе. Отсюда два инварианта, каждый со своей ценой
ошибки:

* по полной странице отсутствие УТВЕРЖДАЕТСЯ — иначе мы навсегда останемся в
  откате и опрос кэфов будет отставать от их изменений;
* по обрывку страницы отсутствие утверждать НЕЛЬЗЯ — иначе «рынка нет» означает
  всего лишь «мне дали узкий кусок DOM», и мы молча пропустим живой рынок.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as cs  # noqa: E402

URL = "https://winline.ru/stavki/sport/kibersport/live"
FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")

with open(
    os.path.join(FIXTURES, "winline_pinned_bar_shadows_card.html"), encoding="utf-8"
) as handle:
    REAL_PAGE_SLICE = handle.read()


def _whole_page(body: str = REAL_PAGE_SLICE) -> str:
    """Вырезка живой страницы, добитая до порога «страница загружена целиком».

    Порог существует ровно для того, чтобы отсутствие не утверждалось по огрызку
    DOM; в тесте его добираем наполнителем, а не ослаблением порога.
    """
    padding = "<!-- " + ("наполнитель " * 6000) + " -->"
    return f"<html><body>Киберспорт Live {body}{padding}</body></html>"


class FakePage:
    def __init__(self, html, url=URL):
        self.html = html
        self.url = url
        self.calls = 0

    def evaluate(self, script, args=None):
        self.calls += 1
        return {"html": self.html, "url": self.url}


def _collect(page, *, map_num, team1, team2):
    return cs._winline_fast_collect(
        page,
        series="dltv.org/matches/8912519540",
        map_num=map_num,
        team1=team1,
        team2=team2,
        expected_url=URL,
    )


def test_absent_market_is_asserted_on_a_whole_page():
    """Карточка есть, рынка 4-й карты нет — отвечаем сразу, без полного разбора."""
    page = FakePage(_whole_page())
    out = _collect(page, map_num=4, team1="FOKUS", team2="MOUZ")

    assert out is not None, "по полной странице отсутствие обязано доказываться"
    assert out["p1_odds"] is None and out["p2_odds"] is None
    assert out["market_status"] == "missing"
    assert out["page_valid"] is True
    assert out["acquisition_mode_echo"] == "dynamic_dom_fast"
    assert page.calls == 1


def test_existing_market_is_still_returned_from_the_same_page():
    """Обратная сторона: на той же странице рынок 2-й карты обязан находиться."""
    out = _collect(FakePage(_whole_page()), map_num=2, team1="FOKUS", team2="MOUZ")

    assert out is not None
    assert (out["p1_odds"], out["p2_odds"]) == (4.90, 1.18)
    assert out["market_status"] == "open"


def test_match_not_offered_is_a_miss_not_a_browser_failure():
    """Матча на странице нет вовсе — это честное отсутствие, а не сбой браузера."""
    out = _collect(
        FakePage(_whole_page()), map_num=2, team1="Nemiga Gaming", team2="Team Lynx"
    )

    assert out is not None
    assert out["market_status"] == "missing"
    assert out["page_valid"] is True
    assert out["p1_odds"] is None and out["p2_odds"] is None


def test_partial_page_never_claims_absence():
    """Обрывок DOM отсутствие не доказывает — только откат на полный разбор."""
    fragment = "<html><body>Киберспорт <div><a>FOKUS MOUZ</a></div></body></html>"
    assert _collect(FakePage(fragment), map_num=4, team1="FOKUS", team2="MOUZ") is None


def test_foreign_page_never_claims_absence():
    """Большая, но не winline страница тоже не даёт права утверждать отсутствие."""
    foreign = "<html><body>" + ("посторонний текст " * 6000) + "</body></html>"
    assert _collect(FakePage(foreign), map_num=4, team1="FOKUS", team2="MOUZ") is None


def test_wrong_url_never_claims_absence():
    """Не тот URL — сначала вернуться на нужный, а не рапортовать отсутствие."""
    page = FakePage(_whole_page(), url="https://winline.ru/stavki/sport/futbol")
    assert _collect(page, map_num=4, team1="FOKUS", team2="MOUZ") is None
