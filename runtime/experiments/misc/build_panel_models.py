#!/usr/bin/env python3
"""Сборка боевых артефактов панели: семь моделей, калибровка, пороги.

Обучает то, что пойдёт в прод, и сразу считает всё, что нужно для честного
показа: калибровку в общую шкалу вероятности и порог в этой же шкале.

СОСТАВ ВХОДА определён замерами, а не соображениями. Он один для всех семи
целей: живое ядро плюс 122 приора F6/F7/F8. Что и почему исключено:

  844 карточные колонки каталога (F1-F5, F9, F10) — на тотале дали −0.0020, на
  стороне ровно 0.0000. Гипотеза, что в них сидит потерянное качество, была
  проверена и опровергнута; это третий независимый замер того же нуля.

  одиннадцать батчей идей — каждый требует своего автомата накопления и своего
  пути чтения, то есть своего риска разъехаться с обучением, а суммарная опора
  на них мала.

  `ideas_batch4` и `ban_features` — по замеру ≤0.001 в любую сторону, причём
  второй вдобавок потребовал бы живого сбора pickBans.

Приоры F6/F7/F8 — единственное, что требует снимка накопления, и единственное,
что окупается: тотал 0.7121 → 0.7442, сторона 0.7190 → 0.7331.

КАЛИБРОВКА. CatBoost отдаёт вероятность, но у семи моделей с разными AUC эти
вероятности означают разную частоту попаданий, и сравнивать их между собой
нельзя. Изотоническая регрессия на ВАЛИДАЦИИ приводит все семь к одной шкале:
после неё «уверенность 70%» у окна и у тотала значит одно и то же — семь
попаданий из десяти. Лестница узлов кладётся в артефакт, применяется без
sklearn.

ПОРОГ подбирается на валидации: минимальная уверенность, при которой доля
попаданий держит планку `TARGET_HIT` и в поток попадает не меньше `MIN_SHARE`.
Порог обязан выбираться НЕ на тесте, иначе замеренная доля попаданий будет
завышена подгонкой.

Тест трогается один раз в конце — для отчёта.

Запуск: nohup venv_catboost/bin/python3 runtime/experiments/misc/build_panel_models.py ...
Выход:  ml-models/prematch_panel/{panel.json, <key>.cbm}
        runtime/artifacts/misc/build_panel_models.md
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "runtime/experiments/misc"))
sys.path.insert(0, str(ROOT / "base"))
from pro_features_wide import auc  # noqa: E402
import kills27_max_model as K  # noqa: E402
from ml_panel import ModelSpec, atomic_write_specs  # noqa: E402

ART = ROOT / "runtime/artifacts/misc"
OUT_DIR = Path(os.getenv("PANEL_OUT", str(ROOT / "ml-models" / "prematch_panel")))
# Та же карточка, что читает живой путь (`prematch_panel_live.CARD_PATH`).
CARD_PATH = Path(os.getenv("PANEL_CARD", str(ROOT / "base" / "hero_features_v7.json")))

OUT_MD = ART / "build_panel_models.md"
CB = dict(iterations=3000, learning_rate=0.03, depth=6, l2_leaf_reg=6,
          random_seed=20260814, early_stopping_rounds=150, verbose=False,
          thread_count=8)
EXTRA = ["catalog_features.npz", "catalog_features_w2.npz",
         "kills_dict_features.npz", "public_kills_pro_features.npz"]
# Состав входа определён замерами, а не соображениями (E-202, catalog_split_cost):
#   живое ядро      боевые 35, симметричный блок карточки, словарь окон,
#                   паблик-логиты, Glicko/TrueSkill, гибридный логит;
#   + приоры 122    F6 (герой), F7 (игрок), F8 (пары) — единственное, что
#                   требует снимка накопления, и единственное, что окупается:
#                   на тотале 0.7121 → 0.7442, на стороне 0.7190 → 0.7331.
# НЕ входят 844 карточные колонки каталога (F1-F5, F9, F10): на тотале они дали
# −0.0020, на стороне ровно 0.0000. Третий независимый замер того же нуля —
# ровно как у прежних «конструкций над карточкой». Гипотеза, что потерянные
# −0.0287 сидят в них, проверена и опровергнута.
LIVE_BASE = ("prod35", "sym", "rating", "hybrid")
PRIOR_FAM = ("F6_", "F7_", "F8_")
TARGET_HIT = float(os.getenv("TARGET_HIT", "0.66"))
MIN_SHARE = float(os.getenv("MIN_SHARE", "0.10"))
# Насколько модель обязана обгонять постоянный ответ большинством.
LIFT_OVER_BASE = float(os.getenv("LIFT_OVER_BASE", "0.04"))
MAX_KNOTS = 48
# Группы заполненности — то, что в живом пути реально может отсутствовать:
# боевые колонки и карточка есть всегда, словарь окон и снимок приоров — нет.
GROUPS = ("core", "dict", "priors")

_lines: list[str] = []


def say(s: str = "") -> None:
    print(s, flush=True)
    _lines.append(s)
    OUT_MD.write_text("\n".join(_lines) + "\n", encoding="utf-8")


def isotonic_knots(p: np.ndarray, y: np.ndarray) -> tuple[tuple, tuple]:
    """Лестница калибровки, прореженная до MAX_KNOTS узлов."""
    from sklearn.isotonic import IsotonicRegression

    iso = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip").fit(p, y)
    xs = np.asarray(iso.X_thresholds_, dtype=float)
    ys = np.asarray(iso.y_thresholds_, dtype=float)
    if len(xs) > MAX_KNOTS:
        idx = np.unique(np.linspace(0, len(xs) - 1, MAX_KNOTS).astype(int))
        xs, ys = xs[idx], ys[idx]
    return tuple(round(float(v), 6) for v in xs), tuple(round(float(v), 6) for v in ys)


def pick_threshold(conf: np.ndarray, hit: np.ndarray, y: np.ndarray
                   ) -> tuple[float, float, float, float]:
    """Минимальная уверенность, где модель ОБГОНЯЕТ большинство при объёме.

    Абсолютной планки мало, и это выяснилось на цели «время 43+»: карт длиннее
    43 минут меньшинство, поэтому постоянный ответ «нет» уже даёт около 70%
    попаданий, порог 0.500 брался даром, и панель светила бы зелёным на каждой
    карте, ничего не сообщая.

    Поэтому планка динамическая: `max(TARGET_HIT, доля большинства + LIFT)`.
    Большинство считается на той же валидации. Если модель не обгоняет его ни
    при каком пороге, возвращается 0.90 — вердиктов почти не будет, и это
    честный ответ, а не поломка.
    """
    base = float(max(y.mean(), 1.0 - y.mean()))
    floor = max(TARGET_HIT, base + LIFT_OVER_BASE)
    grid = np.arange(0.50, 0.901, 0.005)
    for t in grid:
        m = conf >= t
        share = float(m.mean())
        if share < MIN_SHARE or m.sum() < 200:
            continue
        rate = float(hit[m].mean())
        if rate >= floor:
            return float(t), rate, share, base
    return 0.90, float("nan"), 0.0, base


def select_columns(names: list[str], spans: dict[str, tuple[int, int]],
                   n_base: int, total: int) -> tuple[list[int], int]:
    """Индексы колонок отправляемого набора и число приоров среди них.

    Вынесено отдельной функцией, потому что этот же отбор нужен второй стороне —
    записи имён колонок в артефакт. Живой скорер собирает вектор ПО ИМЕНАМ, а не
    по позициям: порядок блоков менялся за вечер трижды, и сборка по позициям
    разошлась бы молча.
    """
    keep = {i for i, nm in enumerate(names[:n_base]) if nm.startswith(LIVE_BASE)}
    for f in ("kills_dict_features.npz", "public_kills_pro_features.npz"):
        if f in spans:
            keep.update(range(*spans[f]))
    lo, hi = spans.get("catalog_features.npz", (0, 0))
    n_prior = sum(1 for i in range(lo, hi) if names[i].startswith(PRIOR_FAM))
    keep.update(i for i in range(lo, hi) if names[i].startswith(PRIOR_FAM))
    if n_prior == 0:
        raise SystemExit("приоры F6/F7/F8 не найдены — вход собран не тот")
    return sorted(keep), n_prior


def build_targets(d):
    """Семь целей панели. Общая функция: пересчёт порогов зовёт её же."""
    dur, rk, dk, pst = d["dur_min"], d["rk"], d["dk"], d["pst"]
    tot = pst[:, :, 0].sum(1)
    rad, dire = pst[:, :5, 0].sum(1), pst[:, 5:, 0].sum(1)
    # (ключ, заголовок, «да», «нет», цель, маска)
    TARGETS = []
    for a_, b_ in ((5, 15), (10, 20), (15, 25), (20, 30)):
        diff = (rk[:, b_] - rk[:, a_]) - (dk[:, b_] - dk[:, a_])
        TARGETS.append((f"w_{a_}_{b_}", f"окно {a_}-{b_}", "Radiant", "Dire",
                        (diff > 0).astype(int), (dur >= b_) & (diff != 0)))
    TARGETS.append(("dur43", "время 43+", "да", "нет",
                    (dur >= 43).astype(int), np.ones(len(dur), bool)))
    TARGETS.append(("total_55_50", "тотал ≥55", "≥55", "≤50",
                    (tot >= 55).astype(int), (tot >= 55) | (tot <= 50)))
    # Отдельной модели «дайр >=30» здесь больше нет (удалена 21.08.2026, alex):
    # «30+ у дайра» — то же событие, что «30+ у радианта», записанное с другой
    # стороны, и вторая копия делила корпус пополам вместо того, чтобы учить
    # одну модель на обеих ориентациях.
    # ОСТАЛОСЬ СДЕЛАТЬ: сама эта цель пока считается в ориентации RADIANT, то
    # есть модель отвечает «возьмёт ли 30+ радиант», а не «какая из сторон
    # возьмёт 30+». Честная симметричная постановка — обучение на обеих
    # ориентациях (как `team27` в public_kills, где dire это ТА ЖЕ модель на
    # перевёрнутых сторонах). Пока этого нет, заголовок оставлен прежним, чтобы
    # он не обещал больше, чем артефакт умеет.
    TARGETS.append(("rad_30_25", "радиант ≥30", "≥30", "≤25",
                    (rad >= 30).astype(int), (rad >= 30) | (rad <= 25)))
    return TARGETS


def main() -> None:
    t0 = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    d = K.build_all()
    names = list(d["names"])
    parts = [d["X"]]
    off = d["X"].shape[1]
    n_base = off
    spans: dict[str, tuple[int, int]] = {}
    for fname in EXTRA:
        p = ART / fname
        if not p.exists():
            continue
        z = np.load(p, allow_pickle=True)
        if not np.array_equal(z["mids"], d["mids"]):
            raise SystemExit(f"{fname}: другой набор карт")
        F = z["F"].astype(np.float32)
        # Отбор по диапазону, а не по префиксу имени: у словаря окон имена
        # `kwdict_*`, у паблик-моделей `publogit_*`, и фильтр по имени файла
        # молча выбросил бы оба живых блока — на этом уже споткнулись.
        cn = ([str(x) for x in z["names"]] if "names" in z.files
              else [f"{Path(fname).stem}_{i}" for i in range(F.shape[1])])
        spans[fname] = (off, off + F.shape[1])
        parts.append(F)
        names += cn
        off += F.shape[1]
    X = np.empty((len(d["ts"]), off), dtype=np.float32)
    o = 0
    for blk in parts:
        X[:, o:o + blk.shape[1]] = blk
        o += blk.shape[1]
    del parts, blk

    kept, n_prior = select_columns(names, spans, n_base, off)
    # Матрица СУЖАЕТСЯ, а не прячется за `ignored_features`. С ignored модель
    # сохраняет полную ширину входа и на предсказании требует все 1988 колонок,
    # хотя пользуется 928 — живому пути пришлось бы вечно подавать 1060
    # фантомных нулей, а `feature_names.json` разошёлся бы с моделью.
    X = np.ascontiguousarray(X[:, kept])
    cols = [names[i] for i in kept]
    if len(set(cols)) != len(cols):
        raise SystemExit("имена колонок не уникальны — сборка по именам невозможна")
    # `prod35_order` — отпечаток входа: какой боевой признак лежал в каждом
    # слоте `prod35_i` во время обучения. Смена ДЛИНЫ и без него ловится
    # (сборка по именам не досчитается колонки), а вот перестановка при той же
    # длине 35 — нет: живой путь берёт порядок у текущей модели и молча положил
    # бы значения не в те слоты.
    prod35_order = [str(x) for x in (d.get("names35") or ())]
    (OUT_DIR / "feature_names.json").write_text(json.dumps(
        {"columns": cols, "n_prior": n_prior, "prod35_order": prod35_order},
        ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"вход панели: {len(cols)} колонок (ядро {len(cols)-n_prior}, "
          f"приоры {n_prior}) из {off}", flush=True)
    sub, val, test = d["sub"], d["val"], d["test"]
    dur, rk, dk, pst = d["dur_min"], d["rk"], d["dk"], d["pst"]
    tot = pst[:, :, 0].sum(1)
    rad, dire = pst[:, :5, 0].sum(1), pst[:, 5:, 0].sum(1)

    TARGETS = build_targets(d)

    say("# Боевые артефакты панели: обучение, калибровка, пороги")
    say()
    say(f"Вход {len(cols)} колонок из {off}: живое ядро {len(cols)-n_prior} "
        f"(боевые 35, симметричный блок карточки, словарь окон, паблик-логиты, "
        f"Glicko/TrueSkill, гибридный логит) плюс {n_prior} приоров F6/F7/F8 — "
        f"единственное, что требует снимка и единственное, что окупается. "
        f"844 карточные колонки каталога исключены по замеру (−0.0020 на "
        f"тотале, 0.0000 на стороне). Порог подбирается на валидации, тест "
        f"трогается один раз.")
    say()
    say("| модель | AUC тест | порог | доля потока | сбылось (тест) |")
    say("|---|---|---|---|---|")

    from catboost import CatBoostClassifier

    specs: list[ModelSpec] = []
    for key, title, pos, neg, y, mask in TARGETS:
        s_, v_, t_ = sub & mask, val & mask, test & mask
        m = CatBoostClassifier(eval_metric="AUC", **CB)
        m.fit(X[s_], y[s_], eval_set=(X[v_], y[v_]))
        p_val = m.predict_proba(X[v_])[:, 1]
        p_te = m.predict_proba(X[t_])[:, 1]
        a_te = auc(y[t_], p_te)
        kx, ky = isotonic_knots(p_val, y[v_])
        spec = ModelSpec(key=key, title=title, positive=pos, negative=neg,
                         threshold=0.5, groups=GROUPS, knots_x=kx, knots_y=ky)
        c_val = np.array([spec.calibrate(v) for v in p_val])
        conf_v = np.maximum(c_val, 1.0 - c_val)
        hit_v = np.where(c_val >= 0.5, y[v_], 1 - y[v_])
        thr, rate_v, share_v, base_v = pick_threshold(conf_v, hit_v, y[v_])
        c_te = np.array([spec.calibrate(v) for v in p_te])
        conf_t = np.maximum(c_te, 1.0 - c_te)
        hit_t = np.where(c_te >= 0.5, y[t_], 1 - y[t_])
        sel = conf_t >= thr
        rate_t = float(hit_t[sel].mean()) if sel.sum() else float("nan")
        specs.append(ModelSpec(key=key, title=title, positive=pos, negative=neg,
                               threshold=round(thr, 4), groups=GROUPS,
                               knots_x=kx, knots_y=ky))
        m.save_model(str(OUT_DIR / f"{key}.cbm"))
        say(f"| {title} | **{a_te:.4f}** | "
            f"{thr:.3f} | {sel.mean():.1%} | **{rate_t:.1%}** |")
        print(f"  {key}: AUC {a_te:.4f}, порог {thr:.3f}, вал {rate_v:.1%} → "
              f"тест {rate_t:.1%} ({time.time()-t0:.0f} c)", flush=True)

    atomic_write_specs(specs, OUT_DIR)
    # ДВА ОТПЕЧАТКА ВХОДА. Модели сохраняются БЕЗ имён колонок: у `.cbm`
    # `feature_names_` это `['0'..'927']`, то есть скоринг чисто позиционный, а
    # единственная сверка при загрузке проверяла только ДЛИНУ. Перестановка 928
    # колонок при той же длине прошла бы молча — тот же класс отказа, что E-201,
    # только сразу на всём входе.
    #   * `columns_sha` — от `feature_names.json`: правка состава без
    #     переобучения перестаёт быть незаметной;
    #   * `card_fingerprint` — от карточки героев: её подмена сдвигает смысл 742
    #     колонок `sym_`, и `hero_side_tables.py:18-21` обещает эту сверку.
    # Обещание было не выполнено: значение писалось в манифест и в `status()`,
    # но не сличалось ни с чем (аудит 19.08.2026).
    from hero_side_tables import card_fingerprint as _card_fp, columns_sha
    (OUT_DIR / "manifest.json").write_text(json.dumps(
        {"built": "build_panel_models.py", "columns": len(cols), "columns_source": off,
         "n_prior": n_prior,
         "target_hit": TARGET_HIT, "min_share": MIN_SHARE,
         "test_maps": int(test.sum()), "models": [s.key for s in specs],
         "columns_sha": columns_sha(cols),
         "card_fingerprint": _card_fp(CARD_PATH)},
        indent=2, ensure_ascii=False), encoding="utf-8")
    say()
    say(f"Артефакты: `{OUT_DIR}` — {len(specs)} моделей `.cbm` плюс `panel.json` "
        f"с калибровкой и порогами. Прогон занял {time.time()-t0:.0f} c.")


if __name__ == "__main__":
    main()
