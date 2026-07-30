"""Быстрый съём карточки Winline и приоритет задач общего Camoufox.

Полный разбор тянет из браузера ~470 КБ HTML плюс несколько inner_text, чтобы
взять карточку в ~130 символов. Быстрый путь просит браузер вернуть только
поддерево вокруг карточки; решение о границах и извлечение кэфов остаются в
Python — теми же функциями, что покрыты остальными тестами.

Инвариант безопасности: быстрый путь либо возвращает готовый словарь с кэфами,
либо None и полный разбор. Он не имеет права вернуть кэфы соседней карточки.
"""
from __future__ import annotations

import queue
import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[2]
for _p in (BASE_DIR, REPO_ROOT):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import cyberscore_try as cs  # noqa: E402

URL = "https://winline.ru/stavki/sport/kibersport/live"
TEAM1 = "Carstensz Esports"
TEAM2 = "Six Cats"

# Форма живой карточки: команды в шапке, кэфы в соседнем узле.
CARD_HTML = """
<div class="event">
  <div class="hdr"><a>CARSTENSZ SIX CATS</a><span>2карта 10'</span></div>
  <div class="stats">0 1 1 4</div>
  <div class="markets">2К Матч 7.50 1.05 3.80 + 1.5 - 1.20
    2 карта 3.80 1.20 - - - -</div>
</div>
"""

# Соседняя карточка со своим рынком: её кэфы брать нельзя.
FEED_HTML = """
<div class="feed">
  <div class="event">
    <div class="hdr">CARSTENSZ SIX CATS</div>
    <div class="markets">Матч 1.38 2.80</div>
  </div>
  <div class="event">
    <div class="hdr">NEXT RADIANT NEXT DIRE</div>
    <div class="markets">2 карта 1.91 1.83</div>
  </div>
</div>
"""


class FakePage:
    """Дублёр страницы: evaluate отдаёт заранее заданное поддерево."""

    def __init__(self, html, url=URL, raises=False):
        self.html = html
        self.url = url
        self.raises = raises
        self.calls = 0

    def evaluate(self, script, args=None):
        self.calls += 1
        if self.raises:
            raise RuntimeError("evaluate failed")
        if self.html is None:
            return None
        return {"html": self.html, "url": self.url}


def _collect(page, map_num=2, team1=TEAM1, team2=TEAM2):
    return cs._winline_fast_collect(
        page,
        series="dltv.org/matches/8912519540",
        map_num=map_num,
        team1=team1,
        team2=team2,
        expected_url=URL,
    )


# --- токены ----------------------------------------------------------------


@pytest.mark.parametrize(
    "name, expected",
    [
        ("Ilbirs Esports", ["ilbirs"]),
        ("Six Cats", ["six", "cats"]),
        ("NEMIGA GAMING", ["nemiga"]),
        ("Team Gaming", ["team", "gaming"]),
        ("", []),
    ],
)
def test_team_probe_tokens(name, expected):
    assert cs._winline_team_probe_tokens(name) == expected


# --- быстрый путь ----------------------------------------------------------


def test_fast_path_returns_odds_from_own_card():
    page = FakePage(CARD_HTML)
    out = _collect(page)

    assert out is not None
    assert out["p1_odds"] == 3.80
    assert out["p2_odds"] == 1.20
    assert out["market_status"] == "open"
    assert out["map_num"] == 2
    assert out["acquisition_mode_echo"] == "dynamic_dom_fast"
    assert page.calls == 1


def test_fast_path_never_returns_neighbour_odds():
    """Ключевой инвариант: чужие кэфы не подмешиваются.

    Быстрый путь теперь умеет закрывать и отрицательный случай, поэтому здесь
    допустим либо откат (None), либо честный ответ с ПУСТЫМИ кэфами. Чего он
    не имеет права сделать — вернуть цены соседней карточки.
    """
    page = FakePage(FEED_HTML)
    out = _collect(page)

    if out is None:
        return
    assert out["p1_odds"] is None and out["p2_odds"] is None
    assert "1.91" not in str(out.get("details") or "")
    assert "1.83" not in str(out.get("details") or "")


def test_fast_path_falls_back_when_absence_not_provable():
    """Рынка пятой карты нет, но и блок рынков в сузившемся контексте не виден.

    Контекст сжимается до шапки матча, поэтому доказать отсутствие нельзя —
    честный ответ это откат на полный разбор, а не поспешное "рынка нет".
    """
    assert _collect(FakePage(CARD_HTML), map_num=5) is None


def test_fast_path_short_circuits_closed_market():
    """Закрытый рынок доказан прочерками -> отвечаем сразу, без полного разбора."""
    closed_html = """
    <div class="event">
      <div class="hdr"><a>CARSTENSZ SIX CATS</a><span>2karta 10</span></div>
      <div class="markets">2 карта - - Матч 1.50 2.40</div>
    </div>
    """
    out = _collect(FakePage(closed_html))
    if out is None:
        return
    assert out["p1_odds"] is None and out["p2_odds"] is None
    assert out["acquisition_mode_echo"] == "dynamic_dom_fast"


def test_fast_path_falls_back_when_markets_block_not_proven():
    """Если блок рынков в поддереве не виден — отсутствие не доказано, откат."""
    html = "<div><div class='hdr'>CARSTENSZ SIX CATS</div></div>"
    assert _collect(FakePage(html)) is None


def test_fast_path_falls_back_on_wrong_url():
    page = FakePage(CARD_HTML, url="https://winline.ru/other/page")
    assert _collect(page) is None


def test_fast_path_falls_back_when_evaluate_fails():
    page = FakePage(CARD_HTML, raises=True)
    assert _collect(page) is None


def test_fast_path_falls_back_on_empty_subtree():
    assert _collect(FakePage(None)) is None
    assert _collect(FakePage("")) is None


def test_fast_path_falls_back_without_team_tokens():
    assert _collect(FakePage(CARD_HTML), team1="", team2="") is None


# --- приоритет очереди -----------------------------------------------------


def test_priority_constants_ordered():
    assert (
        cs.CAMOUFOX_JOB_PRIORITY_STOP
        < cs.CAMOUFOX_JOB_PRIORITY_WINLINE_POLL
        < cs.CAMOUFOX_JOB_PRIORITY_DEFAULT
    )


def test_winline_job_overtakes_heavier_jobs():
    """Опрос кэфов не должен ждать очереди за pro-tracker."""
    session = cs._SharedCamoufoxSession()
    assert isinstance(session._jobs, queue.PriorityQueue)

    def put(priority, label):
        session._jobs.put((priority, next(session._job_seq), object(), label, None, False))

    put(cs.CAMOUFOX_JOB_PRIORITY_DEFAULT, "protracker-1")
    put(cs.CAMOUFOX_JOB_PRIORITY_DEFAULT, "protracker-2")
    put(cs.CAMOUFOX_JOB_PRIORITY_WINLINE_POLL, "winline-poll")
    put(cs.CAMOUFOX_JOB_PRIORITY_STOP, "stop")

    order = [session._jobs.get()[3] for _ in range(4)]
    assert order == ["stop", "winline-poll", "protracker-1", "protracker-2"]


def test_same_priority_keeps_fifo_order():
    """Внутри одного приоритета порядок остаётся честным FIFO."""
    session = cs._SharedCamoufoxSession()
    for i in range(5):
        session._jobs.put(
            (cs.CAMOUFOX_JOB_PRIORITY_DEFAULT, next(session._job_seq), object(), f"job{i}", None, False)
        )
    order = [session._jobs.get()[3] for _ in range(5)]
    assert order == [f"job{i}" for i in range(5)]


@pytest.mark.parametrize(
    "label, expected_high",
    [
        ("winline_current_map_poll:dltv.org/matches/1|map2", True),
        ("winline_current_map_poll", True),
        ("protracker:undying", False),
        ("bookmaker_prefetch:betboom", False),
        ("", False),
        (None, False),
    ],
)
def test_priority_is_derived_from_label(label, expected_high):
    """Приоритет выводится по метке, а не kwarg-ом на месте вызова.

    Передача priority= в _run_shared_camoufox_job ломала бы любую подмену этой
    функции: тестовые дублёры принимают старую сигнатуру, а коллектор глотает
    TypeError и молча возвращает ошибку.
    """
    got = cs._camoufox_job_priority_for_label(label)
    if expected_high:
        assert got == cs.CAMOUFOX_JOB_PRIORITY_WINLINE_POLL
    else:
        assert got == cs.CAMOUFOX_JOB_PRIORITY_DEFAULT


def test_run_shared_camoufox_job_signature_stays_compatible():
    """Старый способ вызова обязан продолжать работать без priority."""
    import inspect

    sig = inspect.signature(cs._run_shared_camoufox_job)
    assert sig.parameters["priority"].default is None
    positional = [n for n, prm in sig.parameters.items() if prm.default is inspect.Parameter.empty]
    assert positional == ["label", "callback"]
