#!/usr/bin/env python3
"""Живой провайдер блока `publogit_*` — девять выходов паблик-моделей.

Блок стоит дороже всех недостающих: замер цены пропуска (`missing_block_cost.md`,
процитирован в `prematch_panel_live`) дал −0.027 на худшей цели против −0.003 у
рейтингов. При этом он самый дешёвый в доставке: ВСЕ девять чисел — чистая
функция десяти hero_id, никакого снимка и никакого автомата накопления.

РЕЦЕПТ СВЕРЕН С ОБУЧЕНИЕМ, А НЕ ВЫВЕДЕН ИЗ ИМЁН. На 3000 карт про-корпуса живой
расчёт совпал с офлайн-эталоном (`public_kills_pro_features.npz`) с max|Δ| =
3e-08 по всем девяти колонкам — это точность хранения float32, то есть расхождения
нет вовсе. Проверялись именно те три развилки, на которых легко ошибиться:

    signed=True  → encoder_signed      (окна килов и rad27)
    signed=False → encoder_unsigned    (dur43 и tot51)
    team27       → hstack(team_signed, team_unsigned), 33514 колонок

`team27_dire` — ТА ЖЕ модель на перевёрнутых сторонах, а не `1 - team27_rad`:
запасная гипотеза дала max|Δ| = 0.83, то есть модель несимметрична, и «сэкономить»
один вызов нельзя.

`rad27_signed_broken` воспроизводится КАК ЕСТЬ. Имя честно говорит, что колонка
собиралась с ошибкой, но модель панели училась именно на ней; «починить» её здесь
значило бы подать число, которого модель не видела.

ЗНАЧЕНИЯ — ВЕРОЯТНОСТИ, несмотря на префикс `publogit_`: в обучающей матрице
лежит `predict_proba[:, 1]` (диапазон 0.04..0.97), а не лог-шанс. Логарифмировать
на живом пути нельзя.
"""
from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any, Sequence

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = Path(os.getenv("PANEL_PUBLIC_DIR",
                           str(PROJECT_ROOT / "ml-models" / "public_kills")))

# Порядок обязан совпадать с `names` из public_kills_pro_features.npz — именно он
# лёг в матрицу обучения панели.
COLUMNS: tuple[str, ...] = (
    "publogit_w_5_15", "publogit_w_10_20", "publogit_w_15_25",
    "publogit_w_20_30", "publogit_dur43", "publogit_tot51",
    "publogit_rad27_signed_broken", "publogit_team27_rad",
    "publogit_team27_dire",
)
# Одиночные модели: ключ файла → имя колонки. Энкодер выбирается по флагу
# `signed` внутри самого артефакта, а не по этой таблице.
SINGLE: tuple[tuple[str, str], ...] = (
    ("w_5_15", "publogit_w_5_15"), ("w_10_20", "publogit_w_10_20"),
    ("w_15_25", "publogit_w_15_25"), ("w_20_30", "publogit_w_20_30"),
    ("dur43", "publogit_dur43"), ("tot51", "publogit_tot51"),
    ("rad27", "publogit_rad27_signed_broken"),
)

_lock = threading.Lock()
_state: dict[str, Any] = {"loaded": False, "enc": None, "mdl": None,
                          "error": None}


def _load() -> dict[str, Any]:
    """Ленивая загрузка моделей и энкодеров. Ошибка запоминается: блок просто
    не поставляется, панель считает заполненность честно, live продолжает жить."""
    with _lock:
        if _state["loaded"]:
            return _state
        _state["loaded"] = True
        try:
            import sys

            import joblib

            # Энкодеры пиклятся как `draft_features.DraftFeatureEncoder`, и без
            # `base` в пути распаковка падает на ModuleNotFoundError.
            if str(PROJECT_ROOT / "base") not in sys.path:
                sys.path.insert(0, str(PROJECT_ROOT / "base"))
            enc = {k: joblib.load(MODEL_DIR / f"encoder_{k}.joblib")
                   for k in ("signed", "unsigned", "team_signed",
                             "team_unsigned")}
            keys = [k for k, _ in SINGLE] + ["team27"]
            mdl = {k: joblib.load(MODEL_DIR / f"{k}.joblib") for k in keys}
            _state.update(enc=enc, mdl=mdl)
        except Exception as exc:                     # noqa: BLE001
            _state["error"] = f"{type(exc).__name__}: {exc}"
        return _state


def block(heroes10: Sequence[int]) -> dict[str, float] | None:
    """Девять колонок по десяти hero_id (пятеро Radiant, затем пятеро Dire).

    `None`, если артефакты не загрузились: непоставленный блок честнее, чем
    девять нулей, которых модель не видела.
    """
    st = _load()
    if st.get("enc") is None or st.get("mdl") is None:
        return None
    try:
        import scipy.sparse as sp

        h = np.asarray(list(heroes10), dtype=np.int64).reshape(1, 10)
        enc, mdl = st["enc"], st["mdl"]
        out: dict[str, float] = {}
        for key, col in SINGLE:
            spec = mdl[key]
            e = enc["signed"] if spec["signed"] else enc["unsigned"]
            out[col] = float(spec["model"].predict_proba(e.transform(h))[0, 1])

        team = mdl["team27"]["model"]

        def team_p(x: np.ndarray) -> float:
            X = sp.hstack([enc["team_signed"].transform(x),
                           enc["team_unsigned"].transform(x)], format="csr")
            return float(team.predict_proba(X)[0, 1])

        out["publogit_team27_rad"] = team_p(h)
        # Перевёрнутые стороны, а не 1-p: модель несимметрична (проверено).
        out["publogit_team27_dire"] = team_p(
            np.concatenate([h[:, 5:], h[:, :5]], axis=1))
        return out
    except Exception as exc:                         # noqa: BLE001
        _state["error"] = f"{type(exc).__name__}: {exc}"
        return None


def status() -> dict[str, Any]:
    """Что загрузилось — для диагностики панели."""
    st = _load()
    return {"ready": st.get("mdl") is not None, "error": st.get("error"),
            "dir": str(MODEL_DIR)}
