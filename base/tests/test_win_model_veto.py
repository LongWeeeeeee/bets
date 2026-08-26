"""Вето драфтовой ML-модели: блок со знаком против модели не идёт.

Проверяются четыре вещи, каждая из которых способна тихо сломать прод:
  * вето срабатывает только на НЕСОГЛАСИИ и только выше порога СВОЕЙ секции;
  * пороги секций разные (early 10, early_end 5, mid 9, all 0) — это результат
    тюнинга «понижать, пока отсекается мусор», а не круглые числа;
  * отсутствие индекса, сломанная модель и выключенный флаг = вето НЕ работает
    (fail-open): молча обнулять отбор из-за модели недопустимо;
  * `format_output_dict` действительно перестаёт считать блок валидным.
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import draft_model_paths as P  # noqa: E402
import win_model_veto as V  # noqa: E402


class _StubEncoder:
    """Ширина боевого каталога на 1.19 млн строк."""
    n_columns = 16758


class _StubModel:
    """Ширина каталогов на 5.09 млн — те самые шесть пар синергии разницы."""
    n_features_in_ = 16764


def _block(index, **extra):
    data = {V.INDEX_KEY: index}
    data.update(extra)
    return data


def test_veto_only_on_disagreement():
    # модель за Radiant (+12), блок тоже за Radiant -> вето нет
    assert V.blocks_veto(1, _block(12.0), "early_output") is False
    # модель за Dire (-12), блок за Radiant -> вето
    assert V.blocks_veto(1, _block(-12.0), "early_output") is True
    assert V.blocks_veto(-1, _block(12.0), "early_output") is True


def test_section_thresholds_differ():
    # early_output: порог 10 — несогласие на 9 не режет, на 11 режет
    assert V.blocks_veto(1, _block(-9.0), "early_output") is False
    assert V.blocks_veto(1, _block(-11.0), "early_output") is True
    # early_end_output: порог 5
    assert V.blocks_veto(1, _block(-4.0), "early_end_output") is False
    assert V.blocks_veto(1, _block(-6.0), "early_end_output") is True
    # mid_output: порог 9
    assert V.blocks_veto(1, _block(-8.0), "mid_output") is False
    assert V.blocks_veto(1, _block(-9.5), "mid_output") is True
    # all_output: режется любое несогласие
    assert V.blocks_veto(1, _block(-0.5), "all_output") is True


def test_fail_open_without_index_or_flag(monkeypatch):
    assert V.blocks_veto(1, {}, "all_output") is False              # индекса нет
    assert V.blocks_veto(1, _block(None), "all_output") is False    # индекс None
    assert V.blocks_veto(1, _block("нет"), "all_output") is False   # мусор в поле
    assert V.blocks_veto(0, _block(-12.0), "all_output") is False   # знака блока нет
    monkeypatch.setattr(V, "VETO_ENABLED", False)
    assert V.blocks_veto(1, _block(-12.0), "all_output") is False   # флаг выключен


def test_win_index_fails_open_on_broken_model(monkeypatch, tmp_path):
    """Нет файлов модели -> None, а не исключение и не блокировка."""
    monkeypatch.setattr(V, "MODEL_DIR", tmp_path / "нет-такой-папки")
    monkeypatch.setitem(V._state, "loaded", False)
    monkeypatch.setattr(V, "_cache", {})
    draft = {f"pos{i}": {"hero_id": i} for i in range(1, 6)}
    assert V.win_index(draft, {f"pos{i}": {"hero_id": i + 10} for i in range(1, 6)}) is None
    assert V.load_error() is not None


def test_unknown_hero_refuses_instead_of_scoring_partial_draft(monkeypatch):
    """Незнакомый герой = отказ, а не индекс по НЕПОЛНОМУ драфту.

    `DraftFeatureEncoder` намеренно зануляет невиданную колонку вместо
    исключения — для обучения это верно, но в бою давало число, посчитанное по
    девяти героям из десяти. Замер на боевом артефакте: подмена одного героя
    двигала индекс с +5.299 на −7.464, то есть переворачивала ЗНАК при порогах
    вето 10/5/9/0/0, и блок ветовался в обратную сторону.

    Обе дороги к незнакомому герою проверяются отдельно, потому что в коде это
    разные ветки: id за верхней границей словаря (новый герой патча) и id
    внутри границы, но ни разу не встреченный (незанятый слот нумерации Dota —
    таких в боевом словаре 28 штук: 24, 115-118, 122, ... 154).
    """
    import numpy as np

    class _Encoder:
        # словарь на id 1..20, где 7 — незанятый слот
        hero_lut = np.array([-1] + [-1 if i == 7 else i for i in range(1, 21)], dtype=np.int32)

        def __init__(self):
            self.calls = 0

        def transform(self, heroes):
            self.calls += 1
            return heroes

    class _Model:
        @staticmethod
        def predict_proba(matrix):
            # sklearn отдаёт ndarray, и код индексирует его как [0, 1]
            return np.array([[0.4, 0.6]])

    encoder = _Encoder()
    monkeypatch.setattr(V, "_cache", {})
    monkeypatch.setattr(V, "_unknown_seen", set())
    monkeypatch.setitem(V._state, "loaded", True)
    monkeypatch.setitem(V._state, "encoder", encoder)
    monkeypatch.setitem(V._state, "model", _Model())

    def draft(ids):
        return {f"pos{i}": {"hero_id": h} for i, h in enumerate(ids, 1)}

    ours = draft([1, 2, 3, 4, 5])

    # контроль: полностью знакомый драфт считается как раньше
    assert V.win_index(ours, draft([11, 12, 13, 14, 15])) == 10.0
    assert encoder.calls == 1

    # id за верхней границей словаря — новый герой патча
    assert V.win_index(ours, draft([11, 12, 13, 14, 999])) is None
    # id внутри границы, но не встречавшийся — незанятый слот нумерации
    assert V.win_index(ours, draft([11, 12, 13, 14, 7])) is None
    # модель на незнакомом драфте не звалась вовсе
    assert encoder.calls == 1

    # отказ не должен быть молчаливым: E-195 — неполный вход прожил незамеченным
    assert V.unknown_heroes_seen() == (7, 999)


def test_encoder_alias_makes_veto_independent_of_launch_layout(monkeypatch):
    """Артефакт распаковывается независимо от того, лежит ли `base/` на пути.

    Энкодер сохранён joblib'ом из скрипта обучения, импортировавшего класс как
    `draft_features`, поэтому в пикле записан модуль верхнего уровня. Прод
    выживал только потому, что `functions.py` делает `import win_model_veto`,
    то есть стартует с `base/` в `sys.path`. Из корня проекта тот же файл падал
    с ModuleNotFoundError, `_load` возвращал False и вето отключалось ЦЕЛИКОМ —
    молча и без записи в лог.
    """
    monkeypatch.delitem(sys.modules, "draft_features", raising=False)
    V._alias_draft_features()
    assert "draft_features" in sys.modules
    assert hasattr(sys.modules["draft_features"], "DraftFeatureEncoder")


def test_mixed_artifacts_disable_veto_instead_of_scoring(monkeypatch, tmp_path):
    """Кодировщик и модель из разных сборок = вето выключено, причина в логе.

    У панели такая сверка есть и намеренно ПАДАЕТ, у драфт-модели её не было
    вовсе — поэтому класс ошибок «файлы из разных каталогов лежат рядом» никак
    не проявлялся. Контракт вето другой: отказ всегда в сторону разрешения,
    поэтому несогласованный артефакт не роняет прод, а отключает вето.

    Ширины взяты боевые: 16 758 у каталога на 1.19 млн строк против 16 764 у
    каталогов на 5.09 млн — ровно те шесть пар синергии, которым не хватило
    поддержки на меньшем корпусе.
    """
    import joblib

    directory = tmp_path / "смешанный-каталог"
    directory.mkdir()
    # классы на уровне модуля: joblib не пиклит объявленные внутри функции
    joblib.dump(_StubEncoder(), directory / P.ENCODER_FILE)
    joblib.dump(_StubModel(), directory / P.MODEL_FILE)

    monkeypatch.setattr(V, "MODEL_DIR", directory)
    monkeypatch.setattr(V, "_cache", {})
    monkeypatch.setitem(V._state, "loaded", False)

    draft = {f"pos{i}": {"hero_id": i} for i in range(1, 6)}
    assert V.win_index(draft, {f"pos{i}": {"hero_id": i + 10} for i in range(1, 6)}) is None
    assert "16758" in V.load_error() and "16764" in V.load_error()


def test_served_model_names_the_catalog(monkeypatch):
    """Какой логит раздаётся — должно быть видно строкой, а не выводиться.

    Веса верхней предматчевой модели обучены на логите конкретного каталога, но
    сам артефакт этого не помнит: рассинхрон E-201 стоил −0.0046 AUC и не был
    виден ниоткуда. Отпечаток не чинит рассинхрон, но делает его заметным.
    """
    assert V.served_model().startswith(V.MODEL_DIR.name)


def test_catalog_path_has_single_source_of_truth(monkeypatch):
    """Путь к каталогу берётся из общей константы и переключается одним местом.

    Раньше он был продублирован в шести файлах без общей константы, поэтому
    смена одного не меняла остальные.
    """
    monkeypatch.setenv("WIN_MODEL_DIR", "/tmp/каталог-из-переменной")
    assert P.model_dir() == Path("/tmp/каталог-из-переменной")
    monkeypatch.delenv("WIN_MODEL_DIR")
    assert P.model_dir().name == P.DEFAULT_CATALOG


def test_format_output_dict_respects_veto(monkeypatch):
    import functions

    monkeypatch.setattr(
        functions, "STAR_THRESHOLDS_BY_WR",
        {60: {"all_output": [("counterpick_1vs1", 4)]}}, raising=False)

    # согласие: блок валиден
    assert functions.format_output_dict(
        {"all_output": {"counterpick_1vs1": 9, V.INDEX_KEY: 6.0}},
        target_wr=60, late_signal_gate_enabled=False) is True
    # несогласие: блок отменён
    assert functions.format_output_dict(
        {"all_output": {"counterpick_1vs1": 9, V.INDEX_KEY: -6.0}},
        target_wr=60, late_signal_gate_enabled=False) is False
    # индекса нет — поведение как до правки
    assert functions.format_output_dict(
        {"all_output": {"counterpick_1vs1": 9}},
        target_wr=60, late_signal_gate_enabled=False) is True


def test_ml_win_index_is_star_only_in_all_output(monkeypatch):
    """Модель как ЗВЁЗДНАЯ метрика: порог только в all_output, 10 на WR60.

    В early и late порогов нет намеренно — там замер показал шум либо минус,
    а в all_output метрика лишь добавляет карты: вето стоит на нуле и уже сняло
    все несогласия, поэтому убирать ей нечего (E-73, E-76).
    """
    import functions

    monkeypatch.setattr(
        functions, "STAR_THRESHOLDS_BY_WR",
        {60: {"all_output": [("ml_win_index", 10)], "early_output": [], "mid_output": []}},
        raising=False)

    # модель одна делает блок валидным
    assert functions.format_output_dict(
        {"all_output": {V.INDEX_KEY: 12.0}}, target_wr=60,
        late_signal_gate_enabled=False) is True
    # ниже порога — нет
    assert functions.format_output_dict(
        {"all_output": {V.INDEX_KEY: 8.0}}, target_wr=60,
        late_signal_gate_enabled=False) is False
    # в блоке без порога метрика звезды не создаёт
    assert functions.format_output_dict(
        {"early_output": {V.INDEX_KEY: 12.0}}, target_wr=60,
        late_signal_gate_enabled=False) is False


def test_star_and_veto_do_not_conflict(monkeypatch):
    """Метрика и вето — один индекс: согласие даёт звезду, несогласие рушит блок."""
    import functions

    monkeypatch.setattr(
        functions, "STAR_THRESHOLDS_BY_WR",
        {60: {"all_output": [("counterpick_1vs1", 4), ("ml_win_index", 10)]}},
        raising=False)

    assert functions.format_output_dict(
        {"all_output": {"counterpick_1vs1": 9, V.INDEX_KEY: 12.0}}, target_wr=60,
        late_signal_gate_enabled=False) is True
    # знак модели против знака блока: конфликт хитов, блок не идёт
    assert functions.format_output_dict(
        {"all_output": {"counterpick_1vs1": 9, V.INDEX_KEY: -12.0}}, target_wr=60,
        late_signal_gate_enabled=False) is False


# ── Предматчевая модель: единый порог 8 и самостоятельная ставка (E-142) ──────
# Пороги E-73 (10/5/9/0) описывают ПАБЛИК-ДРАФТОВУЮ шкалу. У предматчевой модели
# порог один на все секции, и перепутать источники — значит резать не то.


def _pre(index):
    return {V.INDEX_KEY: index, V.SOURCE_KEY: V.SOURCE_PREMATCH}


def _draft(index):
    return {V.INDEX_KEY: index, V.SOURCE_KEY: V.SOURCE_DRAFT}


def test_prematch_threshold_is_the_same_in_every_section():
    for section in ("early_output", "early_end_output", "mid_output", "all_output"):
        assert V._min_index_for(section, V.SOURCE_PREMATCH) == 8.0


def test_draft_source_keeps_legacy_thresholds():
    assert V._min_index_for("early_output", V.SOURCE_DRAFT) == 10.0
    assert V._min_index_for("mid_output", V.SOURCE_DRAFT) == 9.0
    assert V._min_index_for("early_end_output", V.SOURCE_DRAFT) == 5.0
    # Блок без источника трактуется как драфтовый: старое поведение сохраняется.
    assert V._min_index_for("early_output") == 10.0


def test_prematch_veto_fires_above_eight_in_every_section():
    for section in ("early_output", "early_end_output", "mid_output", "all_output"):
        assert V.blocks_veto(-1, _pre(8.5), section) is True     # блок против модели
        assert V.blocks_veto(1, _pre(8.5), section) is False     # блок за модель
        assert V.blocks_veto(-1, _pre(7.9), section) is False    # ниже порога


def test_draft_index_does_not_borrow_prematch_threshold():
    # 8.5 выше предматчевого порога, но ниже драфтовых 10 и 9 — резать нельзя.
    assert V.blocks_veto(-1, _draft(8.5), "early_output") is False
    assert V.blocks_veto(-1, _draft(8.5), "mid_output") is False


def test_model_bet_side_and_price():
    bet = V.model_bet({}, _pre(8.5), None)
    assert bet is not None
    assert bet["side"] == "radiant"
    assert round(bet["confidence"], 4) == 0.585
    assert bet["min_odds"] > 1.0
    # Отрицательный индекс -> сторона Dire.
    assert V.model_bet(_pre(-9.0))["side"] == "dire"


def test_model_bet_silent_below_threshold_and_for_draft_source():
    assert V.model_bet(_pre(7.9)) is None
    # Драфтовый источник не пускается: его винрейт на LAN не мерялся, и порог 8
    # на этой шкале не проверялся.
    assert V.model_bet(_draft(20.0)) is None
    assert V.model_bet({}, None, "не словарь") is None


def test_prematch_bridge_reads_pos_keys_not_integers():
    """Мост обязан понимать боевой формат словаря позиций — `pos1`..`pos5`.

    Куплено на проде 13.08: `_prematch_index` читал ключи 1..5 и "1".."5",
    которых в живых словарях нет, поэтому предматчевая модель молча возвращала
    None, а вето всё это время работало на драфтовой. Ошибка невидима снаружи:
    индекс приходит, просто не от той модели.
    """
    captured = {}

    class _Model:
        def score(self, **kw):
            captured.update(kw)
            raise RuntimeError("дальше не идём — проверяем только извлечение слотов")

    import types

    import base as _base_pkg

    stub = types.ModuleType("prematch_scorer")
    stub.get_model = lambda: _Model()
    # Мост сперва пробует `from base import prematch_scorer`, поэтому подменять
    # надо и запись в sys.modules, и атрибут пакета — иначе подхватится живой
    # модуль и тест проверит не то.
    saved = {k: sys.modules.get(k) for k in ("prematch_scorer", "base.prematch_scorer")}
    saved_attr = getattr(_base_pkg, "prematch_scorer", None)
    sys.modules["prematch_scorer"] = stub
    sys.modules["base.prematch_scorer"] = stub
    _base_pkg.prematch_scorer = stub
    # Драфтовый логит нужен мосту раньше скорера — подменяем и его.
    saved_draft = V.win_index_draft
    V.win_index_draft = lambda a, b: 5.0
    try:
        rad = {f"pos{i}": {"account_id": 100 + i, "hero_id": i} for i in range(1, 6)}
        dire = {f"pos{i}": {"account_id": 200 + i, "hero_id": 5 + i} for i in range(1, 6)}
        V._prematch_index(rad, dire)
    finally:
        V.win_index_draft = saved_draft
        for key, mod in saved.items():
            if mod is None:
                sys.modules.pop(key, None)
            else:
                sys.modules[key] = mod
        if saved_attr is None:
            if hasattr(_base_pkg, "prematch_scorer"):
                delattr(_base_pkg, "prematch_scorer")
        else:
            _base_pkg.prematch_scorer = saved_attr

    assert captured, "скорер не был вызван — слоты из pos-ключей не извлеклись"
    assert captured["radiant_accounts"] == [101, 102, 103, 104, 105]
    assert captured["dire_accounts"] == [201, 202, 203, 204, 205]
    assert captured["radiant_heroes"] == [1, 2, 3, 4, 5]
    assert captured["dire_heroes"] == [6, 7, 8, 9, 10]
    assert captured["strictness"] == "accounts"
