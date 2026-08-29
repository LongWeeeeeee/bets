"""Late-модель победы: контракт молчаливого отказа и осмысленность оценки.

Главное, что здесь проверяется, — модель НИКОГДА не роняет карточку. Любая
поломка (нет артефакта, неполный драфт, выключена через env) обязана давать
None, а не исключение: строка в панели необязательная.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from base import late_win_model as lwm  # noqa: E402
from base import win_model_veto as wmv  # noqa: E402

# Ранний пуш против поздних керри: Lycan/Broodmother/Beastmaster/Death Prophet/
# Undying по замеру E-240 теряют 8-17 п.п. на длинных картах, а Medusa/Spectre/
# Faceless Void/Phantom Lancer/Alchemist прибавляют 9-18 п.п.
EARLY_PUSH = (77, 61, 38, 43, 85)
LATE_CARRY = (94, 67, 41, 12, 73)
ARTIFACT = lwm.MODEL_DIR / "late_win_model.joblib"
needs_artifact = pytest.mark.skipif(not ARTIFACT.exists(), reason="боевой артефакт не собран")


def _slots(heroes):
    return {f"pos{i + 1}": {"hero_id": h, "account_id": 0} for i, h in enumerate(heroes)}


def test_vector_order_comes_from_win_model_veto():
    """Порядок героев — собственность win_model_veto; late-модель его только принимает."""
    vector = wmv._heroes_vector(_slots(EARLY_PUSH), _slots(LATE_CARRY))
    assert vector == EARLY_PUSH + LATE_CARRY


def test_no_second_copy_of_win_model_veto():
    """Прод зовёт `import win_model_veto`; вторая копия модуля дала бы своё `_LAST_FILL`."""
    loaded = {name for name in sys.modules if name.endswith("win_model_veto")}
    assert len(loaded) == 1, f"загружено несколько копий: {sorted(loaded)}"


@pytest.mark.parametrize("bad", [None, (), (1, 2, 3), tuple(range(11))])
def test_broken_input_is_silent(bad):
    """Неполный или кривой вектор — None, не исключение."""
    assert lwm.radiant_probability(bad) is None
    assert lwm.verdict(bad) is None
    assert lwm.panel_line(bad) is None


def test_disabled_by_env(monkeypatch):
    monkeypatch.setattr(lwm, "ENABLED", False)
    assert lwm.radiant_probability(EARLY_PUSH + LATE_CARRY) is None
    assert lwm.panel_line(EARLY_PUSH + LATE_CARRY) is None


def test_missing_artifact_is_silent(monkeypatch, tmp_path):
    """Нет файлов модели — молчим и не роняем карточку."""
    monkeypatch.setenv("LATE_WIN_MODEL_DIR", str(tmp_path / "нет-такого"))
    module = importlib.reload(lwm)
    try:
        assert module.panel_line(EARLY_PUSH + LATE_CARRY) is None
        assert module.load_error() is not None
    finally:
        monkeypatch.delenv("LATE_WIN_MODEL_DIR", raising=False)
        importlib.reload(module)


def test_last_late_is_none_for_unknown_index():
    assert wmv.last_late(None) is None
    assert wmv.last_late(123456.789) is None


@needs_artifact
def test_late_model_prefers_the_scaling_side():
    """Сторона с поздними керри должна выигрывать НА ДЛИННЫХ картах."""
    verdict = lwm.verdict(EARLY_PUSH + LATE_CARRY)
    assert verdict is not None, lwm.load_error()
    assert verdict["side"] == "Dire"
    assert verdict["confidence"] > 0.6


@needs_artifact
def test_sides_are_symmetric():
    """Зеркальный драфт даёт зеркальный ответ: перекос стороны должен быть мал."""
    direct = lwm.radiant_probability(EARLY_PUSH + LATE_CARRY)
    mirror = lwm.radiant_probability(LATE_CARRY + EARLY_PUSH)
    assert direct is not None and mirror is not None
    assert abs((1.0 - direct) - mirror) < 0.02


@needs_artifact
def test_index_matches_probability():
    heroes = EARLY_PUSH + LATE_CARRY
    probability = lwm.radiant_probability(heroes)
    assert lwm.late_index(heroes) == pytest.approx((probability - 0.5) * 100.0, abs=1e-3)


@needs_artifact
def test_panel_line_shape():
    line = lwm.panel_line(EARLY_PUSH + LATE_CARRY)
    assert line is not None
    assert line.startswith("Late ML-модель: ")
    assert line.endswith("%")
    side, value = line[len("Late ML-модель: "):].split()
    assert side in ("Radiant", "Dire")
    assert 50.0 <= float(value.rstrip("%")) <= 100.0


@needs_artifact
def test_late_disagrees_with_general_on_this_draft():
    """Смысл всей затеи: на этом драфте общая модель и late-модель расходятся.

    Общая любит ранний пуш Radiant, late-модель отдаёт длинную игру Dire.
    Если тест начнёт падать — модели сошлись, и отдельная late-оценка потеряла
    смысл; это повод перемерить, а не править тест.
    """
    general = wmv.win_index_draft(_slots(EARLY_PUSH), _slots(LATE_CARRY))
    late = lwm.late_index(EARLY_PUSH + LATE_CARRY)
    if general is None:
        pytest.skip(f"общая драфт-модель недоступна: {wmv.load_error()}")
    assert general > 0 > late
