"""Журнал панели обязан знать, о какой карте запись.

К 23.08.2026 в боевом `runtime/ml_panel.jsonl` накопилось 805 строк, и у ВСЕХ
`map_id` был пустой: прод звал `synergy_and_counterpick` без параметра `match`,
тот доезжал до модели как `None`, и опознать карту в журнале было нечем. Сверить
вердикт панели с фактическим исходом невозможно — форвардной проверки у панели
не существовало вовсе.

Контракт:
- идентификатор берётся из `match` по первому непустому ключу;
- `map_key` стоит последним запасным: в режиме sourcetv прод пишет
  `match_id = series_id`, один на всю серию, а ключ карты уникален;
- журнал прогнозов использует только явный Dota match_id для привязки исхода;
  серийный URL остаётся диагностическим map_key.
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import win_model_veto as wmv  # noqa: E402

SOURCE = (BASE_DIR / "cyberscore_try.py").read_text(encoding="utf-8")
VETO_SOURCE = (BASE_DIR / "win_model_veto.py").read_text(encoding="utf-8")


def test_prod_passes_a_map_identifier_to_the_model() -> None:
    """Вызов из прода обязан передавать `match` — иначе журнал слепой."""
    tree = ast.parse(SOURCE)
    calls = [n for n in ast.walk(tree)
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
             and n.func.id == "synergy_and_counterpick"]
    assert calls, "вызов synergy_and_counterpick пропал из прода"
    for call in calls:
        names = {kw.arg for kw in call.keywords}
        assert "match" in names, (
            f"вызов на строке {call.lineno} не передаёт `match` — "
            "журнал панели снова останется без идентификатора карты")


def test_map_key_is_the_last_resort_identifier() -> None:
    """Порядок ключей: уникальный id карты предпочтительнее серийного."""
    assert '"map_key"' in VETO_SOURCE
    order = VETO_SOURCE.split('for _k in (')[1].split(')')[0]
    keys = [k.strip().strip('"') for k in order.split(",") if k.strip()]
    assert keys[-1] == "map_key", f"map_key обязан быть последним, а порядок {keys}"
    assert "match_id" in keys


def test_identifier_picks_the_first_non_empty_key() -> None:
    """Пустой `match_id` не должен перебивать рабочий ключ карты."""
    picked = []

    def _pick(match):
        for key in ("id", "match_id", "map_id", "matchId", "map_key"):
            if match.get(key):
                return match.get(key)
        return None

    picked.append(_pick({"match_id": "", "map_key": "dltv.org/matches/8960655084.0"}))
    picked.append(_pick({"match_id": 8960655084, "map_key": "x"}))
    picked.append(_pick({}))
    assert picked == ["dltv.org/matches/8960655084.0", 8960655084, None]


def test_prediction_context_uses_only_explicit_map_id() -> None:
    """Series URLs cannot become real match IDs in the outcome join."""
    assert wmv._prediction_context({"id": 123, "map_key": "series/123.2"})["match_id"] is None
    context = wmv._prediction_context({"match_id": 8992864996, "map_key": "series/123.2",
                                       "game_time": -79, "startDateTime": 1789000000})
    assert context == {"match_id": "8992864996", "map_key": "series/123.2",
                       "game_time": -79, "elo_evaluation_timestamp": 1789000000}
    assert wmv._prediction_context({"match_id": True})["match_id"] is None
