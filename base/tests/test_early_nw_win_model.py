"""Early-NW модель: контракт молчаливого отказа и осмысленность оценки.

Главное, что здесь проверяется, — модель НИКОГДА не роняет карточку. Любая
поломка (нет артефакта, неполный драфт, выключена через env) обязана давать
None, а не исключение: строка в панели необязательная.

Второе — что оценка отвечает на СВОЙ вопрос. Цель early-NW модели это сторона
раннего перевеса по нетворту, а не победа карты, поэтому на драфте «ранний пуш
против поздних керри» она обязана расходиться с late-моделью: ранний перевес
берёт пуш, длинную игру выигрывают керри.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from base import early_nw_win_model as enwm  # noqa: E402
from base import late_win_model as lwm  # noqa: E402
from base import win_model_veto as wmv  # noqa: E402

# Тот же драфт, что в тестах late-модели, чтобы две оценки сравнивались на одном
# входе: Lycan/Broodmother/Beastmaster/Death Prophet/Undying против
# Medusa/Spectre/Faceless Void/Phantom Lancer/Alchemist (E-240).
EARLY_PUSH = (77, 61, 38, 43, 85)
LATE_CARRY = (94, 67, 41, 12, 73)
ARTIFACT = enwm.MODEL_DIR / "early_nw_model.joblib"
needs_artifact = pytest.mark.skipif(not ARTIFACT.exists(), reason="боевой артефакт не собран")


def _slots(heroes):
    return {f"pos{i + 1}": {"hero_id": h, "account_id": 0} for i, h in enumerate(heroes)}


def test_vector_order_comes_from_win_model_veto():
    """Порядок героев — собственность win_model_veto; early-NW модель его принимает."""
    vector = wmv._heroes_vector(_slots(EARLY_PUSH), _slots(LATE_CARRY))
    assert vector == EARLY_PUSH + LATE_CARRY


def test_early_nw_model_does_not_import_win_model_veto():
    """`early_nw_win_model` не имеет права импортировать `win_model_veto`.

    Прод зовёт `import win_model_veto` верхним уровнем (cyberscore_try.py:67).
    Импорт `from base.win_model_veto import ...` отсюда завёл бы ВТОРУЮ копию
    модуля — со своим `_LAST_FILL`, кэшами и своей загруженной моделью.
    """
    source = (ROOT / "base" / "early_nw_win_model.py").read_text(encoding="utf-8")
    offenders = [line.strip() for line in source.splitlines()
                 if "win_model_veto" in line and line.lstrip().startswith(("import ", "from "))]
    assert not offenders, f"early_nw_win_model импортирует win_model_veto: {offenders}"


@pytest.mark.parametrize("bad", [None, (), (1, 2, 3), tuple(range(11))])
def test_broken_input_is_silent(bad):
    """Неполный или кривой вектор — None, не исключение."""
    assert enwm.radiant_probability(bad) is None
    assert enwm.verdict(bad) is None
    assert enwm.panel_line(bad) is None


def test_disabled_by_env(monkeypatch):
    monkeypatch.setattr(enwm, "ENABLED", False)
    assert enwm.radiant_probability(EARLY_PUSH + LATE_CARRY) is None
    assert enwm.panel_line(EARLY_PUSH + LATE_CARRY) is None


def test_missing_artifact_is_silent(monkeypatch, tmp_path):
    """Нет файлов модели — молчим и не роняем карточку."""
    monkeypatch.setenv("EARLY_NW_MODEL_DIR", str(tmp_path / "нет-такого"))
    module = importlib.reload(enwm)
    try:
        assert module.panel_line(EARLY_PUSH + LATE_CARRY) is None
        assert module.load_error() is not None
    finally:
        monkeypatch.delenv("EARLY_NW_MODEL_DIR", raising=False)
        importlib.reload(module)


def test_last_early_nw_is_none_for_unknown_index():
    assert wmv.last_early_nw(None) is None
    assert wmv.last_early_nw(123456.789) is None


@pytest.mark.parametrize("hostile", ["строка", 12345, object(), {"a": 1},
                                     ("a",) * 10, (None,) * 10, (-1,) * 10])
def test_hostile_input_never_raises(hostile):
    """Ни один вход не имеет права выбросить исключение наружу.

    Оценка необязательная, а зовут её из `_prematch_index`, который решает
    ставку. Исключение отсюда уронило бы боевую оценку ради строки в карточке.
    """
    assert enwm.verdict(hostile) is None
    assert enwm.panel_line(hostile) is None
    assert enwm.early_nw_index(hostile) is None


def test_prematch_index_isolates_early_nw_failure():
    """Блок early-NW оценки в `_prematch_index` обязан быть обёрнут в try/except.

    Проверяем исходником: вызвать `_prematch_index` в тесте нельзя — он тянет
    предматчевый скорер с ELO-снимком на несколько гигабайт.
    """
    source = (ROOT / "base" / "win_model_veto.py").read_text(encoding="utf-8")
    start = source.index("_enwm = None")
    block = source[start:source.index("_remember_fill()", start)]
    assert "except Exception as _early_nw_exc" in block, "отказ early-NW модели не изолирован"
    assert '_LAST_FILL["early_nw"] = None' in block, "при отказе поле обязано занулиться"
    assert block.index("try:") < block.index("import early_nw_win_model")


def test_early_nw_verdict_is_journalled():
    """Отказ early-NW модели обязан быть виден в журнале оценок, а не молчать."""
    source = (ROOT / "base" / "win_model_veto.py").read_text(encoding="utf-8")
    start = source.index("_journal_eval(radiant_team=")
    call = source[start:start + 1800]
    for field in ("early_nw_side=", "early_nw_confidence=", "early_nw_error="):
        assert field in call, f"в журнал оценок не пишется {field}"


def test_panel_prints_early_nw_above_late():
    """Строка early-NW обязана стоять ВЫШЕ late-строки: раньше по игровому времени.

    Проверяем исходником сборщика панели, а не прогоном: `_format_win_model_line`
    тянет весь боевой контекст.
    """
    source = (ROOT / "base" / "cyberscore_try.py").read_text(encoding="utf-8")
    # Ищем по escape-последовательностям эмодзи: они в исходнике записаны
    # текстом и однозначны, в отличие от подписи, которая у одной строки
    # экранирована, а у другой нет.
    early = source.index("\\U0001F550")             # 🕐 — early-NW
    late = source.index("\\U0001F551")              # 🕑 — late
    assert early < late, "early-NW строка печатается ниже late-строки"


def test_early_nw_line_is_not_caught_by_the_bet_gate_regex():
    """Новая строка не имеет права сойти за `🤖 ML-модель:`.

    По той строке работает гейт отправки ставки (`BET_REQUIRE_WIN_MODEL`).
    Если бы регексп поймал early-NW строку, гейт стал бы читать чужую сторону.
    """
    # `cyberscore_try` здесь НЕ импортируется: он зовёт `import win_model_veto`
    # верхним уровнем, а тест уже держит `base.win_model_veto` — вышло бы две
    # копии модуля со своим состоянием, ровно то, от чего защищает
    # `test_early_nw_model_does_not_import_win_model_veto`. Достаём сами
    # шаблоны из исходника и компилируем их.
    import ast
    import re as _re

    source = (ROOT / "base" / "cyberscore_try.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    patterns: dict[str, str] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            continue
        if target.id not in ("_WIN_MODEL_PANEL_RE", "_LATE_WIN_MODEL_PANEL_RE"):
            continue
        first = node.value.args[0] if node.value.args else None
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            patterns[target.id] = first.value
    assert set(patterns) == {"_WIN_MODEL_PANEL_RE", "_LATE_WIN_MODEL_PANEL_RE"}, patterns

    panel = ("\U0001F550 Early NW ML-модель: Dire 61.0%\n"
             "\U0001F551 Late ML-модель: Radiant 55.0%")
    general = _re.compile(patterns["_WIN_MODEL_PANEL_RE"], _re.M)
    late_re = _re.compile(patterns["_LATE_WIN_MODEL_PANEL_RE"], _re.M)
    assert general.search(panel) is None, "гейт ставки принял early-NW строку за 🤖 ML-модель"
    late = late_re.search(panel)
    assert late is not None and late.group("side") == "Radiant"


@needs_artifact
def test_sides_are_symmetric():
    """Зеркальный драфт даёт зеркальный ответ: перекос стороны должен быть мал."""
    direct = enwm.radiant_probability(EARLY_PUSH + LATE_CARRY)
    mirror = enwm.radiant_probability(LATE_CARRY + EARLY_PUSH)
    assert direct is not None and mirror is not None
    assert abs((1.0 - direct) - mirror) < 0.02


@needs_artifact
def test_index_matches_probability():
    heroes = EARLY_PUSH + LATE_CARRY
    probability = enwm.radiant_probability(heroes)
    assert enwm.early_nw_index(heroes) == pytest.approx((probability - 0.5) * 100.0, abs=1e-3)


@needs_artifact
def test_panel_line_shape():
    line = enwm.panel_line(EARLY_PUSH + LATE_CARRY)
    assert line is not None
    assert line.startswith("Early NW ML-модель: ")
    assert line.endswith("%")
    side, value = line[len("Early NW ML-модель: "):].split()
    assert side in ("Radiant", "Dire")
    assert 50.0 <= float(value.rstrip("%")) <= 100.0


@needs_artifact
def test_early_nw_prefers_the_pushing_side():
    """Ранний перевес по нетворту берёт сторона раннего пуша, а не поздних керри."""
    verdict = enwm.verdict(EARLY_PUSH + LATE_CARRY)
    assert verdict is not None, enwm.load_error()
    assert verdict["side"] == "Radiant"


@needs_artifact
def test_early_nw_disagrees_with_late_on_this_draft():
    """Смысл отдельной строки: на этом драфте early-NW и late расходятся.

    Ранний перевес берёт пуш (Radiant), длинную игру выигрывают керри (Dire).
    Если тест начнёт падать — две оценки сошлись, и отдельная строка потеряла
    смысл; это повод перемерить, а не править тест.
    """
    early = enwm.early_nw_index(EARLY_PUSH + LATE_CARRY)
    late = lwm.late_index(EARLY_PUSH + LATE_CARRY)
    if late is None:
        pytest.skip(f"late-модель недоступна: {lwm.load_error()}")
    assert early > 0 > late
