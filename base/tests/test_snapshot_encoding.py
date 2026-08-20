"""Запись снимков ELO: экранирование вместо сырого UTF-8.

В названиях команд живут эмодзи — на боевом снимке их тринадцать (🤡, 💢, 🐾 и
прочие). Одного символа вне BMP хватает, чтобы CPython держал ВЕСЬ прочитанный
файл как UCS-4: замерено — 365.6 МБ на диске превращаются в 1462.3 МБ строки,
ровно вчетверо. С `ensure_ascii` (это умолчание `json.dump`) те же данные стоят
731 МБ. Отступы — ещё 102 МБ, 27.9% файла.

Проверяется главное: экранирование НЕ меняет данные. Снимок хранит рейтинги, по
которым идут ставки, и подмена хоть одного символа в имени команды сломала бы
разрешение организации.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ELO import live_team_strength as LTS  # noqa: E402

PAYLOAD = {
    "meta": {"reference_timestamp": 1_700_000_000, "note": "тир-1"},
    "teams_by_org_key": {
        "org:1": {"team_name": "🤡 Клоуны", "current_strength": 1512.5},
        "org:2": {"team_name": "Team Ünïcodé", "current_strength": 1488.0},
        "org:3": {"team_name": "𝓐 Fancy", "current_strength": 1500.0},
    },
    "model_state": {"player_global": {"123": 1600.25}},
}


def test_round_trip_keeps_every_character(tmp_path):
    p = tmp_path / "snap.json"
    LTS._write_json_atomic(p, PAYLOAD)
    back = json.loads(p.read_text(encoding="utf-8"))
    assert back == PAYLOAD
    names = {v["team_name"] for v in back["teams_by_org_key"].values()}
    assert "🤡 Клоуны" in names and "𝓐 Fancy" in names


def test_file_is_pure_ascii(tmp_path):
    """Ни одного байта выше 127: иначе читатель снова получит широкую строку."""
    p = tmp_path / "snap.json"
    LTS._write_json_atomic(p, PAYLOAD)
    raw = p.read_bytes()
    assert max(raw) < 128, "в файле остались не-ASCII байты"
    assert b"\\ud83e" in raw.lower() or b"\\ud83d" in raw.lower(), (
        "эмодзи должен быть записан экранированной парой")


def test_no_indentation_is_written(tmp_path):
    """Отступы стоили 102 МБ и 12.7 млн переводов строк на боевом снимке."""
    p = tmp_path / "snap.json"
    LTS._write_json_atomic(p, PAYLOAD)
    raw = p.read_bytes()
    assert b"\n" not in raw
    assert b", " not in raw and b": " not in raw


def test_decoded_string_stays_narrow(tmp_path):
    """Тот самый эффект, ради которого правка и сделана."""
    p = tmp_path / "snap.json"
    LTS._write_json_atomic(p, PAYLOAD)
    ascii_text = p.read_text(encoding="utf-8")
    wide_text = json.dumps(PAYLOAD, ensure_ascii=False, indent=2)
    assert all(ord(c) < 128 for c in ascii_text)
    assert any(ord(c) > 0xFFFF for c in wide_text)
    # На боевом объёме это разница между 365 МБ и 1462 МБ.
    assert sys.getsizeof(ascii_text) < sys.getsizeof(wide_text)


def test_atomic_write_leaves_no_temporary_file(tmp_path):
    p = tmp_path / "snap.json"
    LTS._write_json_atomic(p, PAYLOAD)
    assert p.exists()
    assert list(tmp_path.iterdir()) == [p], "остался временный файл"
