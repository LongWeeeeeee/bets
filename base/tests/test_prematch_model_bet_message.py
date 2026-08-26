"""Ставка предматчевой модели: переписывается ЗАГОЛОВОК, тело сохраняется.

Требование прямое: когда модель уверена (|индекс| >= 8), таргет разворачивается
на её сторону, но сообщение остаётся тем же — звёздные хиты, блоки
Early/Late/All/Mix, WR, ELO, линии и кэфы никуда не деваются. Собрать ставке
СВОЁ короткое тело значит потерять всё, на что смотрит оператор.
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as C  # noqa: E402

FULL_BODY = "\n".join([
    "СТАВКА НА Old Team x0.5",
    "Radiant Team VS Dire Team",
    "Счёт серии: 1:0",
    "Lanes: top R, mid D, bot R",
    "ELO: Radiant 1620 / Dire 1480",
    "Оценка WR: Early 62%",
    "⭐ Star hits (WR60+):",
    "Early: Counterpick_1vs1 +12.0 (WR60)",
    "Late: (28-60 min):",
    "Counterpick_1vs1: 4.0",
    "All:",
    "Solo: 2.0",
    "Mix:",
    "Protracker_solo: 55.0",
    "Time: 00:42",
    "Кэфы: 1.75 / 2.05",
])
MODEL_LINE = "🤖 ML-модель: Dire 68.0% | ML от кэфа: 1.54\n"


def _build(target: str, body: str, model_line: str = MODEL_LINE) -> list[str]:
    return C._build_prematch_model_bet_message(
        radiant_team_name="Radiant Team",
        dire_team_name="Dire Team",
        target_team_name=target,
        live_league={},
        top="", mid="", bot="",
        protracker_payload={},
        team_elo_block="",
        game_time_seconds=42,
        radiant_lead=0,
        model_line=model_line,
        full_message_text=body,
    ).splitlines()


def test_header_is_rewritten_to_the_model_side():
    got = _build("Dire Team", FULL_BODY)
    assert got[0].startswith("СТАВКА НА ")
    assert "Dire Team" in got[0]
    # Старый множитель x0.5 не должен пережить разворот таргета.
    assert "x0.5" not in got[0]


def test_every_body_line_survives():
    src = FULL_BODY.splitlines()
    got = _build("Dire Team", FULL_BODY)
    lost = [line for line in src[1:] if line not in got]
    assert not lost, f"из тела пропали строки: {lost}"


def test_body_without_header_gets_one_prepended():
    """Ветка без звёзд отдаёт тело БЕЗ первой строки-заголовка."""
    src = FULL_BODY.splitlines()
    got = _build("Radiant Team", "\n".join(src[1:]))
    assert got[0].startswith("СТАВКА НА ")
    assert "Radiant Team" in got[0]
    lost = [line for line in src[1:] if line not in got]
    assert not lost, f"из тела пропали строки: {lost}"


def test_model_line_is_not_duplicated_when_body_already_has_it():
    # В боевом теле строка модели уже есть внутри блока звёздных хитов.
    body = FULL_BODY.replace("⭐ Star hits (WR60+):",
                             MODEL_LINE.rstrip("\n") + "\n⭐ Star hits (WR60+):")
    got = _build("Dire Team", body)
    assert sum(1 for line in got if "ML-модель" in line) == 1


def test_model_line_is_added_when_body_lacks_it():
    got = _build("Dire Team", FULL_BODY)
    assert any("ML от кэфа" in line for line in got)
