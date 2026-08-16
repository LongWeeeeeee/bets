#!/usr/bin/env python3
"""Панель окон в живом пути: сборка блоков из того, что есть в момент ставки.

Точка вызова — `win_model_veto`, где уже посчитаны десять героев, десять
аккаунтов и словарь боевых 35 признаков (`ScoreResult.features`). Отсюда
собираются блоки панели и получаются вердикты для сообщения и журнала.

ЧТО ОТДАЁТСЯ ЖИВЬЁМ И ЧТО НЕТ. Замер цены недоставленного блока
(`missing_block_cost.md`) показал, чего стоит каждый пропуск на худшей цели:

    core    779 колонок  −0.048   боевые 35 и симметричный блок карточки — есть
    priors  122          −0.092   снимок причинных приоров — есть
    public    9          −0.027   выходы паблик-моделей — НЕТ, не сохранены
    dict     12          −0.004   словарь окон килов — пока не подключён
    rating    6          −0.003   Glicko/TrueSkill — не портирован, и не стоит

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
BASELINES = Path(os.getenv("PANEL_BASELINES",
                           str(PROJECT_ROOT / "base" / "hero_baselines_protest.json")))
ENABLED = os.getenv("ML_PANEL_ENABLED", "1") not in ("0", "false", "False")
# Доля драфта считается через SHAP и стоит времени. По умолчанию только для окон:
# именно они показываются, а тотал и время в панели всё равно молчат.
DRAFT_KEYS = tuple(os.getenv("ML_PANEL_DRAFT_KEYS",
                             "w_5_15,w_10_20,w_15_25,w_20_30").split(","))

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
            _state.update(bundle=bundle, tables=(C, B),
                          snap=load_snapshot(),
                          card=card_fingerprint(CARD_PATH),
                          loaded_ts=int(time.time()))
        except Exception as exc:                     # noqa: BLE001
            _state["error"] = f"{type(exc).__name__}: {exc}"
        return _state


def status() -> dict[str, Any]:
    """Что загрузилось — для диагностики и журнала."""
    st = _load()
    b = st.get("bundle")
    return {"ready": bool(b is not None and getattr(b, "ready", False)),
            "models": len(getattr(b, "models", {}) or {}),
            "columns": len(getattr(b, "columns", ()) or ()),
            "snapshot": st.get("snap") is not None,
            "card": st.get("card"), "error": st.get("error"),
            "last_error": st.get("last_error")}


def evaluate_map(radiant_heroes: Sequence[int], dire_heroes: Sequence[int],
                 radiant_accounts: Sequence[int], dire_accounts: Sequence[int],
                 prod_features: Mapping[str, float] | None,
                 prod_order: Sequence[str] = ()) -> list:
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
            blocks["prod35"] = block_from_prod_features(prod_features, prod_order)
        snap = st.get("snap")
        if snap is not None:
            from causal_priors import sym_priors
            names: list[str] = []
            vals = sym_priors(heroes10, accounts10, snap, names)
            pri = {nm: float(vals[0, j]) for j, nm in enumerate(names)}
            # F8 (парная синергия) в снимке отсутствует — блок отдаём как есть,
            # а недостающие имена добьём нейтралью через отдельную группу.
            blocks["priors"] = pri
        return score(bundle, blocks, prod35_names=prod_order,
                     with_draft=bool(DRAFT_KEYS), draft_keys=DRAFT_KEYS)
    except Exception as exc:                         # noqa: BLE001
        # Молча вернуть пустоту нельзя: панель тогда «просто не появляется», и
        # причина теряется. Ошибка запоминается и видна в `status()`.
        _state["last_error"] = f"{type(exc).__name__}: {exc}"
        return []
