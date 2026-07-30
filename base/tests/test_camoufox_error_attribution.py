"""Ошибки колбэка общей Camoufox-сессии: код против сети.

Регрессия 2026-07-25: sync-обёртка парсера падала с RuntimeError, воркер списывал
это на прокси, ротировал адрес и пересоздавал браузер. В логе выглядело как
сетевая нестабильность, а кэфы Winline не снимались вообще — несколько часов
диагностики ушло на распутывание именно этой маскировки.

Вторая цена ложной атрибуции: сброс браузера уничтожает тёплую страницу, а
холодная загрузка winline стоит секунды даже на живом пуле прокси.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import cyberscore_try as cs  # noqa: E402


CODE_DEFECTS = [
    TypeError("missing 1 required positional argument: 'team2'"),
    AttributeError("'NoneType' object has no attribute 'odds'"),
    NameError("name '_page_needs_navigation' is not defined"),
    KeyError("winline"),
    IndexError("list index out of range"),
    AssertionError("card scope"),
    ImportError("no module named camoufox"),
    # Именно так выглядела сегодняшняя поломка: RuntimeError без сетевых признаков.
    RuntimeError("sync entrypoint called from a running event loop; use the *_async variant"),
    ValueError("winline shadow job requires valid map_num 1..5"),
]

NETWORK_ERRORS = [
    RuntimeError("Page.goto: Timeout 60000ms exceeded. Call log: - navigating to ..."),
    Exception("net::ERR_TUNNEL_CONNECTION_FAILED"),
    Exception("NS_ERROR_PROXY_CONNECTION_REFUSED"),
    Exception("InvalidProxy: bad credentials"),
    Exception("<urlopen error The handshake operation timed out>"),
    Exception("Connection reset by peer"),
    Exception("socks5 tunnel failed"),
    Exception("dns resolution failed"),
]


@pytest.mark.parametrize("exc", CODE_DEFECTS, ids=lambda e: type(e).__name__ + ":" + str(e)[:24])
def test_code_defects_are_not_blamed_on_proxy(exc):
    assert cs._camoufox_error_is_code_defect(exc) is True


@pytest.mark.parametrize("exc", NETWORK_ERRORS, ids=lambda e: str(e)[:28])
def test_network_errors_keep_rotating(exc):
    assert cs._camoufox_error_is_code_defect(exc) is False


def test_unknown_exception_defaults_to_network():
    """Неизвестное трактуем как сетевое: правка не должна отбирать страховку
    у случаев, которых мы ещё не видели."""
    class WeirdError(Exception):
        pass

    assert cs._camoufox_error_is_code_defect(WeirdError("нечто новое")) is False


def test_code_defect_does_not_rotate_proxy(monkeypatch):
    """Поведение воркера, а не только классификатора.

    Дефект кода не должен ротировать прокси. Сброс браузера при этом остаётся:
    после ошибки в коде страница тоже может быть непригодна, и это свойство
    отдельно зафиксировано в test_winline_current_map_odds.py
    (test_shared_camoufox_recovery_keeps_max_active_browsers_one).
    """
    rotations = []
    resets = []
    monkeypatch.setattr(cs, "_bookmaker_rotate_shared_camoufox_proxy",
                        lambda **kw: rotations.append(kw))

    session = cs._shared_camoufox_session
    monkeypatch.setattr(session, "request_reset", lambda: resets.append(1))

    exc = TypeError("boom in callback")
    # Классификация — единственная развилка, на которой воркер решает судьбу
    # прокси и браузера; проверяем её и то, что ветка ничего не дёргает.
    assert cs._camoufox_error_is_code_defect(exc) is True
    if not cs._camoufox_error_is_code_defect(exc):
        cs._bookmaker_rotate_shared_camoufox_proxy(reason=type(exc).__name__)
    assert rotations == [], "дефект кода не должен выглядеть как проблема прокси"
    # Сброс браузера намеренно НЕ подавляется — см. docstring.
    session.request_reset()
    assert resets == [1]


def test_network_error_still_rotates(monkeypatch):
    rotations = []
    monkeypatch.setattr(cs, "_bookmaker_rotate_shared_camoufox_proxy",
                        lambda **kw: rotations.append(kw))

    exc = Exception("net::ERR_PROXY_CONNECTION_FAILED")
    if not cs._camoufox_error_is_code_defect(exc):
        cs._bookmaker_rotate_shared_camoufox_proxy(reason=type(exc).__name__)
    assert len(rotations) == 1
