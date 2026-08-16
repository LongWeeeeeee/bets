#!/usr/bin/env python3
"""Живой скорер предматчевой панели: сборка вектора по именам и семь вердиктов.

Собирает вход из блоков, скорит модели и отдаёт вердикты для телеграм-сообщения
и журнала. Ставок не делает.

ВЕКТОР СОБИРАЕТСЯ ПО ИМЕНАМ, а не по позициям. Состав входа за время работы над
панелью менялся трижды, и при сборке по позициям следующая пересборка сдвинула
бы порядок молча — модель приняла бы чужие числа за свои. Имена колонок лежат в
артефакте (`feature_names.json`), записанные тем же кодом, что отбирал колонки
при обучении.

БЛОК, КОТОРЫЙ НЕ ПОСТАВИЛИ, НЕ ЗАМЕНЯЕТСЯ НУЛЯМИ МОЛЧА. Каждый блок даёт свой
провайдер; отсутствующий помечается, его колонки заполняются нейтральным
значением, а доля недостающего идёт в заполненность и в журнал. Гейт
`ML_PANEL_MIN_FILL` не даёт выставить вердикт по наполовину собранному входу:
предикт по половине признаков — это другой предикт, а не осторожный.

Отсутствие артефактов или снимка — не ошибка, а состояние «панель ещё не
выложена»: `score()` возвращает пустой список, live-путь продолжает работать.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DIR = Path(os.getenv(
    "ML_PANEL_DIR", str(PROJECT_ROOT / "ml-models" / "prematch_panel")))
NEUTRAL = 0.0          # чем заполняются колонки непоставленного блока

# Префикс имени колонки → группа заполненности. Порядок проверки важен:
# `F6_`/`F7_`/`F8_` относятся к снимку, всё остальное считается живьём.
GROUP_OF: tuple[tuple[str, str], ...] = (
    ("F6_", "priors"), ("F7_", "priors"), ("F8_", "priors"),
    ("kwdict_", "dict"), ("publogit_", "public"),
    ("prod35_", "core"), ("sym_", "core"), ("rating_", "rating"),
    ("hybrid_", "core"),
)


def group_of(name: str) -> str:
    for pref, grp in GROUP_OF:
        if name.startswith(pref):
            return grp
    return "core"


@dataclass
class PanelBundle:
    """Загруженный артефакт: спецификации, порядок колонок, модели."""

    specs: tuple[Any, ...]
    columns: tuple[str, ...]
    models: dict[str, Any]
    n_prior: int = 0

    @property
    def ready(self) -> bool:
        return bool(self.specs and self.columns and self.models)


def load_bundle(directory: Path | None = None) -> PanelBundle:
    """Артефакт с диска. Пустой набор, если чего-то нет — панель молчит."""
    from ml_panel import load_specs

    d = Path(directory or DEFAULT_DIR)
    specs = load_specs(d)
    try:
        meta = json.loads((d / "feature_names.json").read_text(encoding="utf-8"))
        columns = tuple(str(c) for c in meta["columns"])
        n_prior = int(meta.get("n_prior", 0))
    except (OSError, ValueError, KeyError):
        return PanelBundle((), (), {}, 0)
    models: dict[str, Any] = {}
    try:
        from catboost import CatBoostClassifier
    except ImportError:
        return PanelBundle((), (), {}, 0)
    for s in specs:
        path = d / f"{s.key}.cbm"
        if not path.exists():
            continue
        m = CatBoostClassifier()
        try:
            m.load_model(str(path))
        except Exception:                      # битый файл не должен ронять live
            continue
        if m.n_features_in_ != len(columns):
            raise ValueError(
                f"{s.key}: модель ждёт {m.n_features_in_} колонок, а в "
                f"feature_names.json их {len(columns)} — артефакт несогласован")
        models[s.key] = m
    return PanelBundle(tuple(specs), columns, models, n_prior)


def assemble(columns: Sequence[str],
             blocks: Mapping[str, Mapping[str, float] | None]
             ) -> tuple[np.ndarray, dict[str, bool]]:
    """Вектор в порядке `columns` и отметка, какие группы реально поставлены.

    `blocks` — имя группы → словарь «имя колонки → значение», либо None, если
    блок не поставлен. Колонки непоставленной группы получают NEUTRAL, а сама
    группа помечается отсутствующей: это и есть заполненность.
    """
    present = {g: (v is not None) for g, v in blocks.items()}
    out = np.full(len(columns), NEUTRAL, dtype=np.float32)
    missing_named: list[str] = []
    for i, nm in enumerate(columns):
        grp = group_of(nm)
        src = blocks.get(grp)
        if src is None:
            present.setdefault(grp, False)
            continue
        if nm in src:
            out[i] = float(src[nm])
        else:
            missing_named.append(nm)
    if missing_named:
        # Поставленный блок обязан отдать ВСЕ свои колонки: пропуск внутри блока
        # означает рассинхрон имён, а не отсутствие данных, и молчать о нём хуже,
        # чем упасть на сборке артефакта.
        raise KeyError(f"блок поставлен, но не дал {len(missing_named)} колонок, "
                       f"например {missing_named[:3]}")
    return out, present


def score(bundle: PanelBundle,
          blocks: Mapping[str, Mapping[str, float] | None]):
    """Вердикты по всем моделям панели. Пустой список, если артефакта нет."""
    from ml_panel import evaluate

    if not bundle.ready:
        return []
    x, present = assemble(bundle.columns, blocks)
    row = x.reshape(1, -1)
    out = []
    for s in bundle.specs:
        m = bundle.models.get(s.key)
        if m is None:
            continue
        try:
            raw = float(m.predict_proba(row)[0, 1])
        except Exception:
            raw = None
        v = evaluate(s, raw, present)
        if v is not None:
            out.append(v)
    return out


def block_from_prod_features(features: Mapping[str, float],
                             order: Sequence[str]) -> dict[str, float]:
    """Боевые 35 колонок: `prod35_i` — это `order[i]` из `ScoreResult.features`.

    Имена в обучающей матрице позиционные (`prod35_0..34`), но порядок задаётся
    `feature_names` боевого артефакта, и он же лежит в `PrematchModel.features`.
    Берём значения по этому порядку, а не по алфавиту — иначе колонки съедут.
    """
    return {f"prod35_{i}": float(features.get(nm, 0.0))
            for i, nm in enumerate(order)}


def block_from_matrix(prefix: str, values: np.ndarray) -> dict[str, float]:
    """Блок с позиционными именами (`sym_0..`, `rating_0..`, `hybrid_0..`)."""
    v = np.asarray(values, dtype=np.float64).ravel()
    return {f"{prefix}{i}": float(x) for i, x in enumerate(v)}
