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
# У словаря окон офлайн ПИСАЛ NaN там, где данных нет, и модель училась именно
# на этом. Подставлять ноль вместо него — подсовывать значение, которого модель
# не видела; NaN у CatBoost имеет свою ветку.
NEUTRAL_BY_GROUP: dict[str, float] = {"dict": float("nan")}

# Префикс имени колонки → группа заполненности. Порядок проверки важен:
# `F6_`/`F7_`/`F8_` относятся к снимку, всё остальное считается живьём.
# `F8` вынесен из `priors` намеренно: снимок хранит ключи по герою и аккаунту,
# а парная синергия требует ключей на ПАРЫ, которых там нет. Общая группа
# заставила бы поставщика приоров отдавать колонки, которых у него не бывает.
GROUP_OF: tuple[tuple[str, str], ...] = (
    ("F6_", "priors"), ("F7_", "priors"), ("F8_", "pairs"),
    ("kwdict_", "dict"), ("publogit_", "public"),
    ("prod35_", "prod35"), ("sym_", "card"), ("rating_", "rating"),
    ("hybrid_", "hybrid"),
)


def group_of(name: str) -> str:
    """Группа поставки. `prod35` и `card` разделены намеренно: первый приходит
    из живого пути, второй считается из статических таблиц, и падение одного не
    должно требовать второго."""
    for pref, grp in GROUP_OF:
        if name.startswith(pref):
            return grp
    return "other"


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
        # Ширину спрашиваем у `feature_names_`: `n_features_in_` после
        # `load_model` остаётся нулём, и проверка по нему падала бы всегда.
        width = len(getattr(m, "feature_names_", None) or ()) or None
        if width is not None and width != len(columns):
            raise ValueError(
                f"{s.key}: модель ждёт {width} колонок, а в "
                f"feature_names.json их {len(columns)} — артефакт несогласован")
        models[s.key] = m
    return PanelBundle(tuple(specs), columns, models, n_prior)


def assemble(columns: Sequence[str],
             blocks: Mapping[str, Mapping[str, float] | None]
             ) -> tuple[np.ndarray, dict[str, bool], dict[str, int]]:
    """Вектор, отметка поставленных групп и размер каждой группы в колонках.

    `blocks` — имя группы → словарь «имя колонки → значение», либо None, если
    блок не поставлен. Колонки непоставленной группы получают NEUTRAL, а сама
    группа помечается отсутствующей: это и есть заполненность.
    """
    present = {g: (v is not None) for g, v in blocks.items()}
    sizes: dict[str, int] = {}
    for nm in columns:
        g = group_of(nm)
        sizes[g] = sizes.get(g, 0) + 1
    out = np.full(len(columns), NEUTRAL, dtype=np.float32)
    missing_named: list[str] = []
    for i, nm in enumerate(columns):
        grp = group_of(nm)
        src = blocks.get(grp)
        if src is None:
            present.setdefault(grp, False)
            out[i] = NEUTRAL_BY_GROUP.get(grp, NEUTRAL)
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
    return out, present, sizes


# Драфтовые блоки: всё, что определяется выбранными героями и их позициями.
# `F6_` — приоры ГЕРОЯ, тоже свойство драфта; `F7_` — приоры ИГРОКА, это уже
# сила состава, а не пик, и в долю драфта не входит. `rating` и `hybrid` —
# сила команд, тоже не драфт.
DRAFT_PREFIX: tuple[str, ...] = ("sym_", "publogit_", "kwdict_", "F6_", "F8_")
# Драфтовые имена внутри боевых 35 колонок — сопоставляются через `prod35_names`
# из артефакта, потому что сами колонки названы позиционно.
DRAFT_PROD35: frozenset[str] = frozenset({
    "draft_logit", "vs_wr", "cp_lane", "syn_pos_mean", "hero_pool", "wr30",
    "farm_dep", "draft_logit_x_elo_gap", "draft_logit_x_games_exp"})


def draft_mask(columns: Sequence[str],
               prod35_names: Sequence[str] = ()) -> np.ndarray:
    """Какие колонки относятся к драфту, а какие к силе состава."""
    order = list(prod35_names)
    out = np.zeros(len(columns), dtype=bool)
    for i, nm in enumerate(columns):
        if nm.startswith(DRAFT_PREFIX):
            out[i] = True
        elif nm.startswith("prod35_") and order:
            try:
                j = int(nm.split("_", 1)[1])
            except ValueError:
                continue
            if 0 <= j < len(order) and order[j] in DRAFT_PROD35:
                out[i] = True
    return out


def draft_share(model: Any, row: np.ndarray, mask: np.ndarray,
                toward_positive: bool = True) -> float | None:
    """Доля драфта в решении по ЭТОЙ карте — ЗНАКОВАЯ, к стороне вердикта.

      +N%  драфт тянет ЗА ту сторону, которую называет модель;
      −N%  драфт тянет ПРОТИВ неё, и модель приняла решение вопреки ему.

    Без знака число отвечало бы на вопрос «насколько драфт вообще участвовал»,
    а спрашивают обычно другое: «драфт был за или против?». Знак приводится к
    стороне вердикта, как это сделано в боевой строке предматчевой модели.

    Знаменатель — сумма МОДУЛЕЙ всех вкладов, поэтому доли групп в сумме дают
    единицу и число не может улететь за 100%. В боевой строке знаменателем
    служит сумма только положительных вкладов, из-за чего доли там не
    складываются; здесь это исправлено.
    """
    try:
        from catboost import Pool

        sv = model.get_feature_importance(Pool(row), type="ShapValues")
    except Exception:
        return None
    v = np.asarray(sv)[0][:-1]                  # последний элемент — базовое значение
    if len(v) != len(mask):
        return None
    total = float(np.abs(v).sum())
    if total <= 0:
        return None
    signed = float(v[mask].sum())
    if not toward_positive:                     # вердикт назвал отрицательную сторону
        signed = -signed
    return signed / total


def score(bundle: PanelBundle,
          blocks: Mapping[str, Mapping[str, float] | None],
          prod35_names: Sequence[str] = (), with_draft: bool = True,
          draft_keys: Sequence[str] | None = None):
    """Вердикты по всем моделям панели. Пустой список, если артефакта нет."""
    from ml_panel import evaluate

    if not bundle.ready:
        return []
    x, present, sizes = assemble(bundle.columns, blocks)
    # Заполненность — доля КОЛОНОК, а не групп: отсутствие шести колонок
    # рейтинга и семисот сорока двух колонок карточки — разные события, а счёт
    # по группам делает их одинаковыми и гасит вердикт на пустом месте.
    total_cols = sum(sizes.values()) or 1
    missing_groups = tuple(sorted(g for g in sizes if not present.get(g, False)))
    lost = sum(sizes[g] for g in missing_groups)
    fill = 1.0 - lost / total_cols
    row = x.reshape(1, -1)
    mask = draft_mask(bundle.columns, prod35_names) if with_draft else None
    out = []
    for s in bundle.specs:
        m = bundle.models.get(s.key)
        if m is None:
            continue
        try:
            raw = float(m.predict_proba(row)[0, 1])
        except Exception:
            raw = None
        share = None
        # SHAP стоит ~90 мс на модель. Считаем только там, где доля драфта
        # реально показывается, иначе платим за восемь ради четырёх.
        want = draft_keys is None or s.key in draft_keys
        if with_draft and want and raw is not None and mask is not None:
            # Сторона вердикта — та же, что выберет `evaluate`: по КАЛИБРОВАННОЙ
            # вероятности, а не по сырой. Иначе у моделей с сильной калибровкой
            # знак доли драфта разошёлся бы со стороной в той же строке.
            share = draft_share(m, row, mask,
                                toward_positive=s.calibrate(raw) >= 0.5)
        v = evaluate(s, raw, present, draft_share=share, fill=fill,
                     missing=missing_groups)
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
