"""Алиасы из цепочек переименований: ищем старый тег, но не чужую команду.

Главный риск здесь не «не нашли карточку», а «нашли ЧУЖУЮ». Ростерная склейка
объединяет и переходы игроков (Talon -> Aurora), и если такое имя уйдёт в поиск
карточки, парсер может снять кэфы другого матча на той же странице. Поэтому
таблица собирается только из цепочек, где активность тегов не пересекается, а
тесты стерегут обе стороны: старый тег подставляется, переход — нет.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import team_name_aliases as A  # noqa: E402


def _reload_with(monkeypatch, tmp_path, table):
    path = tmp_path / "team_org_aliases.json"
    path.write_text(json.dumps(table, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setenv("TEAM_ORG_ALIASES", str(path))
    monkeypatch.setattr(A, "_ORG_TABLE", None, raising=False)


def test_compact_key_folds_spaces():
    # `Iron Wing` из live-потока и `ironwing` из справочника — одно имя.
    assert A.compact_key("Iron Wing") == A.compact_key("ironwing") == "ironwing"
    assert A.compact_key("Team Spirit Academy") == "teamspiritacademy"


def test_rename_chain_gives_old_tag(monkeypatch, tmp_path):
    _reload_with(monkeypatch, tmp_path, {"ironwing": ["tundra", "1w"]})
    assert A.alias_spellings("Iron Wing") == ["tundra", "1w"]
    assert A.alias_spellings("IRON WING") == ["tundra", "1w"]


def test_manual_dictionary_wins_over_generated(monkeypatch, tmp_path):
    """Ручные написания идут первыми: они подтверждены на странице букмекера."""
    _reload_with(monkeypatch, tmp_path, {"betboomteam": ["Something Else"]})
    got = A.alias_spellings("BetBoom Team")
    assert got[:3] == ["BoomBoys", "BB Team", "BetBoom"]
    assert got[-1] == "Something Else"


def test_no_alias_when_table_missing(monkeypatch, tmp_path):
    """Файла нет — работает только ручной справочник, как до правки."""
    monkeypatch.setenv("TEAM_ORG_ALIASES", str(tmp_path / "нет-такого-файла.json"))
    monkeypatch.setattr(A, "_ORG_TABLE", None, raising=False)
    assert A.alias_spellings("Iron Wing") == []
    assert A.alias_spellings("BetBoom Team") == ["BoomBoys", "BB Team", "BetBoom"]


def test_name_never_aliases_to_itself(monkeypatch, tmp_path):
    _reload_with(monkeypatch, tmp_path, {"tundra": ["Tundra", "TUNDRA", "1w"]})
    assert A.alias_spellings("Tundra") == ["1w"]


def test_shipped_table_excludes_player_transfers():
    """Боевая таблица не должна связывать команды, играющие параллельно.

    Замер 13.08: ростерная склейка даёт `aurora <- talon` и `invictus <- g2xig`,
    но обе команды каждой пары продолжают играть. Такие связи в таблицу не
    попадают — иначе поиск карточки может увести на чужой матч.

    Обратный пример, который проходить ДОЛЖЕН: `chimera -> virtuspro`. Он
    выглядит так же подозрительно, но данные другие — Chimera отыграла 94 карты
    и остановилась 2025-03-01, а этот состав Virtus.pro начал 2025-04-01. Старый
    тег мёртв, подставлять его безопасно. Разницу видно только по времени, а не
    по названиям, поэтому критерий и построен на активности.
    """
    path = BASE_DIR.parent / "data" / "team_org_aliases.json"
    if not path.exists():
        return                                        # таблица не собрана — нечего проверять
    table = json.loads(path.read_text(encoding="utf-8"))
    for left, right in (("aurora", "talon"), ("invictus", "g2xig")):
        assert right not in [A.compact_key(x) for x in table.get(left, [])], (
            f"{left} -> {right}: это переход игроков, а не переименование"
        )
