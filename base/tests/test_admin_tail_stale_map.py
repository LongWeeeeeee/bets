from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402


@pytest.fixture(autouse=True)
def _isolated_tail(monkeypatch):
    fixture_path = Path(__file__).parent / "fixtures" / "map_verdicts_navi_yandex_map3_20260927.json"
    entries = json.loads(fixture_path.read_text(encoding="utf-8"))["entries"]
    monkeypatch.setattr(runtime, "_admin_tail_page", 0)
    monkeypatch.setattr(runtime, "_admin_tail_last_press_monotonic", None, raising=False)
    monkeypatch.setattr(runtime, "_load_map_verdict_journal", lambda: entries)
    monkeypatch.setattr(runtime, "monitored_matches", {}, raising=False)
    sent = []
    monkeypatch.setattr(runtime, "send_message", lambda message, **kwargs: sent.append(str(message)))
    return sent


def _press(sent):
    before = len(sent)
    runtime._send_admin_log_tail(line_count=100, raw_odds=False)
    return sent[before:]


def test_admin_tail_idle_press_restarts_at_latest_map(monkeypatch, _isolated_tail):
    monkeypatch.setattr(runtime, "_admin_tail_page", 1)
    monkeypatch.setattr(runtime, "_admin_tail_last_press_monotonic", 100.0, raising=False)
    monkeypatch.setattr(runtime, "_admin_tail_monotonic", lambda: 7300.0, raising=False)

    messages = _press(_isolated_tail)

    assert len(messages) == 4
    assert "[страница 1/2]" in messages[0]
    assert "Team Yandex vs Natus Vincere" in messages[0]
    assert "карта 3" in messages[0]
    assert all("9 : 27" not in message for message in messages)


def test_admin_tail_marks_older_series_maps_finished(monkeypatch, _isolated_tail):
    monkeypatch.setattr(runtime, "_admin_tail_monotonic", lambda: 100.0, raising=False)
    first_page = _press(_isolated_tail)
    second_page = _press(_isolated_tail)

    map3 = next(message for message in first_page if "9018969340" in message)
    map2 = next(message for message in first_page if "9018837779" in message)
    map1 = next(message for message in second_page if "9018736585" in message)
    for message in (map1, map2):
        assert "завершена — в серии уже 3-я карта" in message
        assert "статус live" not in message
    assert "карта 1" in map1
    assert "счёт 9 : 27 (последний разбор)" in map1
    assert "завершена" not in map3
    for message in first_page:
        if "Team Cake vs Team avice" in message or "Team Stray vs TEAM YBN" in message:
            assert "завершена" not in message


def test_admin_tail_consecutive_presses_keep_paging(monkeypatch, _isolated_tail):
    monkeypatch.setattr(runtime, "_admin_tail_page", 1)
    monkeypatch.setattr(runtime, "_admin_tail_last_press_monotonic", 100.0, raising=False)
    monkeypatch.setattr(runtime, "_admin_tail_monotonic", lambda: 7300.0, raising=False)

    first = _press(_isolated_tail)
    second = _press(_isolated_tail)
    third = _press(_isolated_tail)

    assert [len(first), len(second), len(third)] == [4, 1, 4]
    assert "[страница 1/2]" in first[0]
    assert "[страница 2/2]" in second[0]
    assert "[страница 1/2]" in third[0]


def test_admin_tail_live_age_is_visible_after_ten_minutes(monkeypatch, _isolated_tail):
    entries = runtime._load_map_verdict_journal()
    map3_ts = entries["dltv.org/matches/9018969340.1"]["updated_ts"]
    now = [map3_ts + 60]
    monkeypatch.setattr(runtime, "_admin_tail_wall_now", lambda: now[0], raising=False)
    monkeypatch.setattr(runtime, "_admin_tail_monotonic", lambda: 100.0, raising=False)

    fresh = _press(_isolated_tail)[0]
    now[0] = map3_ts + 1500
    _press(_isolated_tail)  # older page
    stale = _press(_isolated_tail)[0]

    assert "статус live" in fresh
    assert "без обновлений" not in fresh
    assert "статус live (без обновлений 25 мин)" in stale
