#!/usr/bin/env python3
"""Панель окон в живом пути: сборка блоков из того, что есть в момент ставки.

Точка вызова — `win_model_veto`, где уже посчитаны десять героев, десять
аккаунтов и словарь боевых 35 признаков (`ScoreResult.features`). Отсюда
собираются блоки панели и получаются вердикты для сообщения и журнала.

ЧТО ОТДАЁТСЯ ЖИВЬЁМ И ЧТО НЕТ. Замер цены недоставленного блока
(`missing_block_cost.md`) показал, чего стоит каждый пропуск на худшей цели:

    core    777 колонок  −0.048   боевые 35 и симметричный блок карточки — есть
    priors  114          −0.092   снимок причинных приоров — есть
    public    9          −0.027   паблик-модели — ЕСТЬ (`public_kills_block`)
    dict     12          −0.004   словарь окон килов — есть
    rating    6          −0.003   Glicko/TrueSkill — есть (`team_ratings`)
    pairs     8                   парная синергия F8 — есть (`pair_priors`)
    hybrid    2                   гибридный рейтинг — есть (`hybrid_block`)

Непоставленных блоков не осталось: заполненность 100%. Сумма по группам даёт
928 — ровно столько колонок в артефакте. Прежние 779 и 122 давали 938 и спорили
с абзацем ниже про «17 недостающих из 928»: число 122 взято из поля `n_prior`
артефакта и осталось с тех пор, когда F8 ещё не был отдельной группой
(114 + 8 = 122). Проверено счётом по префиксам имён колонок 19.08.2026.

Недостающее не подставляется нулями молча: блок помечается отсутствующим, его
доля уходит в заполненность и в журнал. Заполненность считается ПО ДОЛЕ
КОЛОНОК — 17 недостающих из 928 дают 98%, а не «две группы из пяти».

ТЯЖЁЛОЕ СЧИТАЕТСЯ ОДИН РАЗ. Таблицы карточки (742 колонки) и снимок приоров
загружаются при первом вызове и живут в процессе: на каждой карте это заняло бы
секунды и сорвало бы live.
"""
from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CARD_PATH = Path(os.getenv("PANEL_CARD", str(PROJECT_ROOT / "base" / "hero_features_v7.json")))


def _manifest_card() -> str:
    """Отпечаток карточки из манифеста панели. Пусто — артефакт собран
    до появления отпечатка, и сверка молчит, а не падает."""
    import json as _json
    from prematch_panel_scorer import DEFAULT_DIR
    try:
        man = _json.loads((Path(DEFAULT_DIR) / "manifest.json")
                          .read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return ""
    return str(man.get("card_fingerprint") or "")
KILLS_DB = Path(os.getenv(
    "PANEL_KILLS_DB",
    str(PROJECT_ROOT / "bets_data" / "analise_pub_matches" /
        "kills_window_dict_raw.sqlite3")))
BASELINES = Path(os.getenv("PANEL_BASELINES",
                           str(PROJECT_ROOT / "base" / "hero_baselines_protest.json")))
ENABLED = os.getenv("ML_PANEL_ENABLED", "1") not in ("0", "false", "False")
# Гибрид отключается отдельно: его снимок весит 366 МБ, а живая точность
# замерена ниже, чем у прочих блоков (см. `verify_hybrid_block.md`).
HYBRID_ENABLED = os.getenv("ML_PANEL_HYBRID", "1") not in ("0", "false", "False")
# Доля драфта считается через SHAP и стоит времени. По умолчанию только для окон:
# именно они показываются, а тотал и время в панели всё равно молчат.
DRAFT_KEYS = tuple(os.getenv("ML_PANEL_DRAFT_KEYS",
                             "w_5_15,w_10_20,w_15_25,w_20_30").split(","))

WINDOWS = ("5_15", "10_20", "15_25", "20_30")
DICT_FIELDS = ("expected_diff", "lead_probability", "games")

_lock = threading.Lock()
_state: dict[str, Any] = {"loaded": False, "bundle": None, "tables": None,
                          "snap": None, "error": None, "loaded_ts": 0}


def _load() -> dict[str, Any]:
    """Единая ленивая загрузка. Ошибка запоминается: панель молчит, live живёт."""
    with _lock:
        if _state["loaded"]:
            return _state
        _state["loaded"] = True
        try:
            import sys

            sys.path.insert(0, str(PROJECT_ROOT / "base"))
            from prematch_panel_scorer import load_bundle
            bundle = load_bundle()
            if not bundle.ready:
                _state["error"] = "артефакт панели не готов"
                return _state
            from hero_side_tables import card_fingerprint, hero_tables
            from pregame_features import PregameFeatures
            from causal_priors import load_snapshot

            pf = PregameFeatures(baselines=BASELINES)
            C, B, _cn, _bn = hero_tables(pf)
            card = card_fingerprint(CARD_PATH)
            # Обещание `hero_side_tables.py:18-21` — «кладётся в артефакт при
            # обучении и сверяется при загрузке» — не выполнялось: значение
            # уходило в `status()` и не сличалось ни с чем (аудит 19.08.2026).
            # Без сверки подмена карточки героев (патч баланса без
            # переобучения) молча сдвигает смысл 742 колонок `sym_` — 80% входа.
            want_card = _manifest_card()
            if want_card and card != want_card:
                raise ValueError(
                    f"карточка героев не та, на которой училась панель: "
                    f"сейчас {card}, в манифесте {want_card} — 742 колонки "
                    f"`sym_` означали бы другое, панель отключена")
            _state.update(bundle=bundle, tables=(C, B),
                          snap=load_snapshot(),
                          card=card,
                          loaded_ts=int(time.time()))
        except Exception as exc:                     # noqa: BLE001
            _state["error"] = f"{type(exc).__name__}: {exc}"
        return _state


class _SqliteKillsWindow(dict):
    """Ленивый словарь окон поверх боевой sqlite.

    Наследование от `dict` обязательно: `calculate_kills_window_advantage`
    начинается с `if not isinstance(heroes_data, dict): return None` и на
    обычной обёртке молча вернула бы None. Таблица называется `stats` и хранит
    колонки, а не kv-блоб.
    """

    def __init__(self, path: Path) -> None:
        super().__init__()
        import sqlite3

        self.conn = sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)
        self.cols = [d[1] for d in self.conn.execute("pragma table_info(stats)")][1:]

    def get(self, key, default=None):
        k = str(key)
        if k in self:
            return dict.__getitem__(self, k)
        row = self.conn.execute("select * from stats where key=?", (k,)).fetchone()
        val = dict(zip(self.cols, row[1:])) if row else default
        dict.__setitem__(self, k, val)
        return val


def _kills_dict():
    """Словарь окон, если он есть. Загружается один раз."""
    if "kwdict" in _state:
        return _state["kwdict"]
    obj = None
    try:
        if KILLS_DB.exists():
            obj = _SqliteKillsWindow(KILLS_DB)
    except Exception as exc:                         # noqa: BLE001
        _state["dict_error"] = f"{type(exc).__name__}: {exc}"
    _state["kwdict"] = obj
    return obj


def _dict_block(heroes10) -> dict[str, float] | None:
    """Двенадцать колонок словаря окон. NaN там, где словарь молчит — ровно то,
    что писал офлайн-сборщик, и ровно то, что видела модель при обучении."""
    hd = _kills_dict()
    if hd is None:
        return None
    try:
        import functions as F

        rad = [f"{int(h)}pos{p}" for p, h in enumerate(heroes10[0][:5], 1)]
        dire = [f"{int(h)}pos{p}" for p, h in enumerate(heroes10[0][5:], 1)]
        res = F.calculate_kills_window_advantage(rad, dire, hd) or {}
    except Exception as exc:                         # noqa: BLE001
        _state["dict_error"] = f"{type(exc).__name__}: {exc}"
        return None
    out: dict[str, float] = {}
    for w in WINDOWS:
        payload = res.get(w) or {}
        for f in DICT_FIELDS:
            v = payload.get(f)
            out[f"kwdict_{w}_{f}"] = float("nan") if v is None else float(v)
    return out


def status() -> dict[str, Any]:
    """Что загрузилось — для диагностики и журнала."""
    st = _load()
    b = st.get("bundle")
    return {"ready": bool(b is not None and getattr(b, "ready", False)),
            "models": len(getattr(b, "models", {}) or {}),
            "columns": len(getattr(b, "columns", ()) or ()),
            "snapshot": st.get("snap") is not None,
            "card": st.get("card"), "error": st.get("error"),
            "last_error": st.get("last_error"),
            "kills_dict": st.get("kwdict") is not None,
            "dict_error": st.get("dict_error")}


def evaluate_map(radiant_heroes: Sequence[int], dire_heroes: Sequence[int],
                 radiant_accounts: Sequence[int], dire_accounts: Sequence[int],
                 prod_features: Mapping[str, float] | None,
                 prod_order: Sequence[str] = (),
                 now_ts: int | None = None,
                 team_ids: Sequence[int] | None = None,
                 tier: str | None = None) -> list:
    """Вердикты панели по карте. Пустой список — панель не готова или выключена."""
    if not ENABLED:
        return []
    st = _load()
    bundle = st.get("bundle")
    if bundle is None or not getattr(bundle, "ready", False):
        return []
    try:
        import numpy as np

        from prematch_panel_scorer import (block_from_matrix,
                                           block_from_prod_features, score)
        from hero_side_tables import sym_block

        heroes10 = np.asarray([list(radiant_heroes) + list(dire_heroes)],
                              dtype=np.int64)
        accounts10 = np.asarray([list(radiant_accounts) + list(dire_accounts)],
                                dtype=np.int64)
        if heroes10.shape[1] != 10 or accounts10.shape[1] != 10:
            return []
        C, B = st["tables"]
        blocks: dict[str, Any] = {
            "card": block_from_matrix("sym_", sym_block(heroes10, C, B))}
        if prod_features is not None and prod_order:
            blocks["prod35"] = block_from_prod_features(
                prod_features, prod_order, expected=bundle.prod35_order)
        snap = st.get("snap")
        if snap is not None:
            from causal_priors import sym_priors
            names: list[str] = []
            vals = sym_priors(heroes10, accounts10, snap, names)
            pri = {nm: float(vals[0, j]) for j, nm in enumerate(names)}
            blocks["priors"] = pri
            # F8 (парная синергия) живёт своим снимком: боевой хранит ключи по
            # герою и аккаунту, а паре нужен ключ на ПАРУ. Одиночные приоры для
            # вычитания берутся здесь же, чтобы уменьшаемое и вычитаемое шли из
            # одного источника.
            import pair_priors
            from causal_priors import PRIOR_NAMES

            jq = [PRIOR_NAMES.index(m) for m in pair_priors.SYN_METRICS]
            hp = snap.hero_priors(heroes10[0])[:, jq]
            fblock = pair_priors.block(heroes10[0], hp)
            if fblock is not None:
                blocks["pairs"] = fblock
        dblock = _dict_block(heroes10)
        if dblock is not None:
            blocks["dict"] = dblock
        # Паблик-логиты: чистая функция десяти hero_id, 4.6 мс, самый дорогой
        # из пропусков (−0.027). Рецепт сверен с обучением до 3e-08.
        import public_kills_block

        pblock = public_kills_block.block(heroes10[0])
        if pblock is not None:
            blocks["public"] = pblock
        # Рейтинги: чтение снимка, без накопления. Время карты нужно для роста
        # RD по простою — оно считается от даты последнего матча игрока, а не от
        # даты снимка, поэтому устаревание снимка тут не врёт.
        import team_ratings

        when = int(time.time()) if now_ts is None else int(now_ts)
        rblock = team_ratings.block(when, accounts10[0])
        if rblock is not None:
            blocks["rating"] = rblock
        # Гибрид: боевой пакет ELO, уже вшитый в бота. Снимок тяжёлый (366 МБ,
        # ~3.6 с), поэтому поднимается лениво и один раз на процесс.
        if HYBRID_ENABLED:
            import hybrid_block

            hblock = hybrid_block.block(when, accounts10[0],
                                        team_ids=team_ids, tier=tier)
            if hblock is not None:
                blocks["hybrid"] = hblock
        return score(bundle, blocks, prod35_names=prod_order,
                     with_draft=bool(DRAFT_KEYS), draft_keys=DRAFT_KEYS)
    except Exception as exc:                         # noqa: BLE001
        # Молча вернуть пустоту нельзя: панель тогда «просто не появляется», и
        # причина теряется. Ошибка запоминается и видна в `status()`.
        _state["last_error"] = f"{type(exc).__name__}: {exc}"
        return []
