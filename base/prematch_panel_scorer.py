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
from dataclasses import dataclass, field
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
    # Какой боевой признак лежал в каждом слоте `prod35_i` во время обучения.
    # Пустой кортеж — артефакт собран до появления отпечатка, проверка молчит.
    prod35_order: tuple[str, ...] = ()
    # Среднее колонки в обучении — ЧЕСТНАЯ нейтраль для непоставленного блока.
    # Общий ноль годится только там, где признак центрирован; см. `assemble`.
    neutral_by_column: dict[str, float] = field(default_factory=dict)

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
        prod35_order = tuple(str(x) for x in (meta.get("prod35_order") or ()))
        neutral_col = {str(k): float(v) for k, v in
                       (meta.get("neutral_by_column") or {}).items()}
    except (OSError, ValueError, KeyError):
        return PanelBundle((), (), {}, 0)
    # Состав и ПОРЯДОК колонок против отпечатка, записанного при обучении.
    # Проверка ниже ловит только длину, а модели позиционные (`feature_names_`
    # у `.cbm` это `['0'..'927']`): перестановка при той же длине прошла бы
    # молча и все семь моделей скорили бы по чужим позициям. Отсутствие
    # отпечатка — не ошибка: артефакт мог быть собран до его появления.
    try:
        man = json.loads((d / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        man = {}
    want_sha = str(man.get("columns_sha") or "")
    if want_sha:
        from hero_side_tables import columns_sha
        got_sha = columns_sha(columns)
        if got_sha != want_sha:
            raise ValueError(
                f"состав колонок панели разошёлся с обучением: "
                f"feature_names.json даёт {got_sha}, в манифесте {want_sha}. "
                f"Модели позиционные, скорить по такому входу нельзя — "
                f"пересобрать панель или вернуть прежний feature_names.json.")
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
    return PanelBundle(tuple(specs), columns, models, n_prior, prod35_order,
                       neutral_col)


def assemble(columns: Sequence[str],
             blocks: Mapping[str, Mapping[str, float] | None],
             neutral_by_column: Mapping[str, float] | None = None
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
            # Нейтраль — значение, НЕ НЕСУЩЕЕ информации, то есть среднее
            # колонки в обучении. Общий ноль таким был не всегда: у девяти
            # `publogit_*` (вероятности, среднее 0.30-0.88) ноль лежит на
            # 3.4-11.8 sd ниже нормы, а у `F8_pair_syn0_max_sum` среднее +3.09.
            #
            # ЭТО НАВЕДЕНИЕ ПОРЯДКА, А НЕ ПОЧИНКА, и путать нельзя. Замер
            # 19.08.2026 (`panel_missing_blocks_cost.md`): отсутствие четырёх
            # блоков стоит −0.0228 AUC в среднем по восьми моделям (от −0.0137
            # до −0.0298), а замена нуля на среднее возвращает +0.0001 — пяти
            # моделям чуть лучше, трём чуть хуже. Никакая КОНСТАНТА не заменяет
            # блок: модель теряет не смещение, а сам различающий сигнал.
            # Единственная настоящая починка — поставить провайдеров на боевую
            # машину, где сейчас нет ни одного из четырёх.
            if neutral_by_column and nm in neutral_by_column:
                out[i] = neutral_by_column[nm]
            else:
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
# `hero_pool` сюда НЕ входит: размер пула героев — свойство игрока, и в
# предматчевой линии он лежит в кластере карьеры (E-214 §1). Два разных понятия
# «драфт» в одном сообщении читать невозможно.
DRAFT_PROD35: frozenset[str] = frozenset({
    "draft_logit", "vs_wr", "cp_lane", "syn_pos_mean", "wr30",
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


#: Блоки для разложения. Те же три, что в строке предматчевой модели: иначе в
#: одном сообщении жили бы два разных понятия «драфт».
ELO_PROD35: frozenset[str] = frozenset({"elo", "opp_elo", "hybrid_strength"})
ELO_PREFIX: tuple[str, ...] = ("rating_", "hybrid_")


def block_of(name: str, prod35_names: Sequence[str] = ()) -> str:
    """Блок колонки: «драфт», «ELO» или «игроки»."""
    if name.startswith(ELO_PREFIX):
        return "ELO"
    if name.startswith(DRAFT_PREFIX):
        return "драфт"
    if name.startswith("prod35_") and prod35_names:
        try:
            j = int(name.split("_", 1)[1])
        except ValueError:
            return "игроки"
        if 0 <= j < len(prod35_names):
            nm = prod35_names[j]
            if nm in ELO_PROD35:
                return "ELO"
            if nm in DRAFT_PROD35:
                return "драфт"
    return "игроки"


def shap_values(model: Any, row: np.ndarray, n_cols: int):
    """Вклады признаков по этой карте; None — если посчитать не вышло.

    Вынесено отдельно, потому что вызов стоит около 90 мс на модель, а нужен он
    и доле драфта, и разложению по блокам. Считать дважды — платить вдвое ни за
    что: обе величины выводятся из одного вектора.
    """
    try:
        from catboost import Pool

        sv = model.get_feature_importance(Pool(row), type="ShapValues")
    except Exception:                            # noqa: BLE001
        return None
    v = np.asarray(sv)[0][:-1]                   # последний элемент — базовое значение
    return v if len(v) == n_cols else None


def group_shares(model: Any, row: np.ndarray, columns: Sequence[str],
                 prod35_names: Sequence[str] = (),
                 flip: bool = False, values=None) -> dict:
    """Доли блоков в решении по ЭТОЙ карте, знаковые.

    `flip=False` — знак как у самой модели (у оконных это ориентация Radiant:
    минус за Dire, плюс за Radiant). `flip=True` — развернуть к стороне вердикта;
    нужно там, где сторон нет вовсе («да/нет», «≥55/≤50»).

    Знаменатель — сумма модулей АГРЕГАТОВ, а не отдельных колонок: тот же, что у
    предматчевой строки (`cyberscore_try.py`), иначе в одном сообщении жили бы
    две шкалы. По колонкам он давал бы 21-41% в сумме — 858 драфтовых колонок
    гасят друг друга, а их шум остаётся в знаменателе. По блокам доли дают ровно
    100%, и знак показывает, кто тянет против.
    """
    v = values if values is not None else shap_values(model, row, len(columns))
    if v is None:
        return {}
    out: dict = {}
    for name, w in zip(columns, v):
        b = block_of(name, prod35_names)
        out[b] = out.get(b, 0.0) + float(w)
    total = sum(abs(x) for x in out.values())
    if total <= 0:
        return {}
    sign = -1.0 if flip else 1.0
    return {k: sign * val / total for k, val in out.items()}


def draft_share(model: Any, row: np.ndarray, mask: np.ndarray,
                toward_positive: bool = True, values=None) -> float | None:
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
    v = values if values is not None else shap_values(model, row, len(mask))
    if v is None:
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
    x, present, sizes = assemble(bundle.columns, blocks,
                                 bundle.neutral_by_column)
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
        parts: dict = {}
        # SHAP стоит ~90 мс на модель. Считаем только там, где доля драфта
        # реально показывается, иначе платим за восемь ради четырёх.
        want = draft_keys is None or s.key in draft_keys
        if with_draft and want and raw is not None and mask is not None:
            # Сторона вердикта — та же, что выберет `evaluate`: по КАЛИБРОВАННОЙ
            # вероятности, а не по сырой. Иначе у моделей с сильной калибровкой
            # знак доли драфта разошёлся бы со стороной в той же строке.
            _sv = shap_values(m, row, len(bundle.columns))
            share = draft_share(m, row, mask, values=_sv,
                                toward_positive=s.calibrate(raw) >= 0.5)
            # У оконных моделей `positive`/`negative` — это сами стороны, и SHAP
            # уже стоит в ориентации Radiant. Разворачивать к вердикту нужно
            # только цели без сторон («да/нет», «≥55/≤50»).
            _sides = {str(s.positive), str(s.negative)} == {"Radiant", "Dire"}
            parts = group_shares(m, row, bundle.columns, prod35_names, values=_sv,
                                 flip=(not _sides) and s.calibrate(raw) < 0.5)
        v = evaluate(s, raw, present, draft_share=share, parts=parts, fill=fill,
                     missing=missing_groups)
        if v is not None:
            out.append(v)
    return out


_ABSENT_PROD35_REPORTED: set = set()


def _report_absent_prod35(absent: Sequence[str]) -> None:
    """Какие боевые признаки не пришли — по одному разу на набор имён.

    Молчать нельзя: подстановка нейтрали внешне неотличима от настоящего
    значения, а набор зависит от ветки и может смениться незаметно.
    """
    key = ",".join(sorted(absent))
    if key in _ABSENT_PROD35_REPORTED:
        return
    _ABSENT_PROD35_REPORTED.add(key)
    print(f"[panel] ветка не дала {len(absent)} боевых признаков "
          f"({key}) — слоты заполнены нейтралью обучения", flush=True)


def block_from_prod_features(features: Mapping[str, float],
                             order: Sequence[str],
                             expected: Sequence[str] = (),
                             neutral: Mapping[str, float] | None = None
                             ) -> dict[str, float]:
    """Боевые 35 колонок: `prod35_i` — это `order[i]` из `ScoreResult.features`.

    `neutral` — нейтрали колонок из артефакта панели: ими заполняются слоты,
    которых не дала отработавшая ветка лестницы.

    Имена в обучающей матрице позиционные (`prod35_0..34`), но порядок задаётся
    `feature_names` боевого артефакта, и он же лежит в `PrematchModel.features`.
    Берём значения по этому порядку, а не по алфавиту — иначе колонки съедут.

    `expected` — отпечаток из артефакта панели: порядок, на котором её учили.
    Без него смена ДЛИНЫ ещё ловилась (сборка по именам не досчиталась бы
    колонки), а перестановка при той же длине 35 — нет: живой путь берёт порядок
    у ТЕКУЩЕЙ модели, и значения молча легли бы не в свои слоты. Проверено
    19.08.2026: семь версий артефакта v2/v3 несут один и тот же порядок, то есть
    сейчас слоты совпадают — сторож ставится на будущее.
    """
    exp = tuple(str(x) for x in expected)
    got = tuple(str(x) for x in order)
    if exp and got != exp:
        if len(got) != len(exp):
            raise ValueError(f"боевых признаков {len(got)}, а панель училась на "
                             f"{len(exp)} — артефакты разных поколений")
        i = next(j for j in range(len(exp)) if exp[j] != got[j])
        raise ValueError(f"порядок боевых признаков разошёлся с обучением "
                         f"панели: слот {i} сейчас {got[i]!r}, а учили на "
                         f"{exp[i]!r}")
    # ВЕТКА ЛЕСТНИЦЫ ОТДАЁТ ПОДМНОЖЕСТВО. `order` — признаки ПОЛНОЙ модели, а
    # `features` приходит от ветки, которая реально отработала: на `no_org` нет
    # `h2h_resid`, на `no_account` — признаков игроков. 24.08.2026 промах здесь
    # считали рассинхроном и роняли весь расчёт: панель молчала на КАЖДОЙ карте
    # без h2h (боевая ветка была именно `no_org`).
    #
    # Пропущенный слот получает НЕЙТРАЛЬ КОЛОНКИ — среднее обучения из артефакта,
    # то же значение, которым `assemble` заполняет непоставленную группу. Ноль
    # тут не годится как общее правило: он нейтрален для `h2h_resid` (остаток
    # «команды не встречались»), но не для `elo` или `games`.
    #
    # Выбрасывать блок целиком — тоже нет: 35 колонок с `elo` и `draft_logit`
    # стоят дороже одной подставленной, а `assemble` требует от поставленного
    # блока ВСЕ его колонки, так что частичный словарь упал бы там же.
    out: dict[str, float] = {}
    absent: list[str] = []
    for i, nm in enumerate(order):
        col = f"prod35_{i}"
        if nm in features:
            out[col] = float(features[nm])
            continue
        absent.append(nm)
        out[col] = float((neutral or {}).get(col, NEUTRAL))
    if absent and order and len(absent) == len(order):
        # Не совпало НИ ОДНО имя — это уже не ветка, а чужой порядок.
        raise KeyError("боевая модель не дала ни одного из "
                       f"{len(order)} признаков порядка — он не от этой модели")
    if absent:
        _report_absent_prod35(absent)
    return out


def block_from_matrix(prefix: str, values: np.ndarray) -> dict[str, float]:
    """Блок с позиционными именами (`sym_0..`, `rating_0..`, `hybrid_0..`)."""
    v = np.asarray(values, dtype=np.float64).ravel()
    return {f"{prefix}{i}": float(x) for i, x in enumerate(v)}
