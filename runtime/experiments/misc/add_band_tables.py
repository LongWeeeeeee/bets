#!/usr/bin/env python3
"""Таблицы полос в артефакт: уверенность → фактический винрейт → кэф.

Калиброванная вероятность — оценка, а рекомендованный кэф должен опираться на
ФАКТ: сколько раз модель угадала в этой полосе уверенности на невиданных картах.
Здесь по тесту строится таблица «полоса → доля попаданий → карт» и кладётся в
`panel.json` рядом с калибровкой.

Кэф считается как `1 / доля попаданий`: это точка безубытка. Ниже неё ставка
убыточна даже при идеальном исполнении, выше — начинается запас. Никакой маржи
от себя не добавляю: пусть запас будет виден явно, а не спрятан в число.

Полосы делаются по калиброванной уверенности с шагом 0.05 и склеиваются, пока в
полосе меньше `MIN_BAND` карт: доля попаданий по сотне карт — это ±5 п.п. шума,
и рекомендовать по ней кэф нельзя.

Запуск: venv_catboost/bin/python3 runtime/experiments/misc/add_band_tables.py
Выход:  дописывает `bands` в ml-models/prematch_panel/panel.json
        runtime/artifacts/misc/add_band_tables.md
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
import kills27_max_model as K  # noqa: E402
import build_panel_models as B  # noqa: E402
from ml_panel import ModelSpec  # noqa: E402

ART = ROOT / "runtime/artifacts/misc"
OUT_MD = ART / "add_band_tables.md"
STEP = 0.05
MIN_BAND = 300           # меньше — доля попаданий уже шум, кэф по ней врёт

_lines: list[str] = []


def say(s: str = "") -> None:
    print(s, flush=True)
    _lines.append(s)
    OUT_MD.write_text("\n".join(_lines) + "\n", encoding="utf-8")


def bands_for(conf: np.ndarray, hit: np.ndarray) -> list[dict]:
    """Полосы уверенности со склейкой мелких: (низ, доля попаданий, карт)."""
    edges = list(np.arange(0.50, 1.0 + 1e-9, STEP))
    raw: list[tuple[float, np.ndarray]] = []
    for lo, hi in zip(edges, edges[1:] + [1.01]):
        m = (conf >= lo) & (conf < hi)
        if m.any():
            raw.append((float(lo), hit[m]))
    out: list[dict] = []
    acc_lo, acc = None, []
    for lo, h in raw:
        if acc_lo is None:
            acc_lo = lo
        acc.append(h)
        n = sum(len(a) for a in acc)
        if n >= MIN_BAND:
            v = np.concatenate(acc)
            out.append({"lo": round(acc_lo, 3), "hit": round(float(v.mean()), 4),
                        "n": int(len(v))})
            acc_lo, acc = None, []
    if acc:                                   # хвост склеиваем с предыдущей
        v = np.concatenate(acc)
        if out:
            prev = out[-1]
            tot = prev["n"] + len(v)
            out[-1] = {"lo": prev["lo"],
                       "hit": round((prev["hit"] * prev["n"] + v.sum()) / tot, 4),
                       "n": tot}
        else:
            out.append({"lo": round(acc_lo or 0.5, 3),
                        "hit": round(float(v.mean()), 4), "n": int(len(v))})
    return out


def main() -> None:
    t0 = time.time()
    from catboost import CatBoostClassifier

    meta = json.loads((B.OUT_DIR / "feature_names.json").read_text(encoding="utf-8"))
    cols = [str(c) for c in meta["columns"]]
    payload = json.loads((B.OUT_DIR / "panel.json").read_text(encoding="utf-8"))
    panel = {m["key"]: m for m in payload["models"]}
    d = K.build_all()
    names = list(d["names"])
    parts = [d["X"]]
    off = d["X"].shape[1]
    for fname in B.EXTRA:
        p = ART / fname
        if not p.exists():
            continue
        z = np.load(p, allow_pickle=True)
        F = z["F"].astype(np.float32)
        cn = ([str(x) for x in z["names"]] if "names" in z.files
              else [f"{Path(fname).stem}_{i}" for i in range(F.shape[1])])
        parts.append(F)
        names += cn
        off += F.shape[1]
    full = np.empty((len(d["ts"]), off), dtype=np.float32)
    o = 0
    for blk in parts:
        full[:, o:o + blk.shape[1]] = blk
        o += blk.shape[1]
    del parts, blk
    pos = {nm: i for i, nm in enumerate(names)}
    X = np.ascontiguousarray(full[:, [pos[c] for c in cols]])
    del full
    test = d["test"]

    say("# Таблицы полос: уверенность → фактический винрейт → кэф безубытка")
    say()
    say(f"Считано на тесте ({int(test.sum()):,} про-карт), шаг полосы "
        f"{STEP}, полосы меньше {MIN_BAND} карт склеены с соседней: доля "
        f"попаданий по сотне карт — это ±5 п.п. шума. Кэф — точка безубытка "
        f"`1 / доля попаданий`, без добавленной маржи: запас должен быть виден.")
    say()

    # СТАВОЧНАЯ ПОПУЛЯЦИЯ ДЛЯ ОКОН КИЛОВ. `build_targets` даёт маску
    # `(dur >= конец окна) & (diff != 0)`, то есть НИЧЬИ ВЫБРОШЕНЫ: это верно для
    # обучения, но неверно для рекомендованной цены. На ставке ничья — проигрыш,
    # и кэф, снятый без неё, занижен на 3-5 п.п. винрейта (замер 21.08.2026:
    # 0.628 против 0.585 на окне 5-15). Здесь окна меряются на популяции
    # `(dur >= конец окна) & ряд килов настоящий`, ничья считается промахом.
    #
    # Фильтр распарсенности обязателен именно потому, что ничьи больше не
    # выбрасываются: у 74.4% сырого про-корпуса поминутный ряд пустой, и его нули
    # прошли бы как «ничья» и утопили бы винрейт на ровном месте. Раньше их
    # молча отсекала та же маска `diff != 0`.
    #
    # ТО ЖЕ САМОЕ КАСАЕТСЯ ЦЕЛЕЙ С МЁРТВОЙ ЗОНОЙ. `total_55_50` учится на маске
    # `(tot >= 55) | (tot <= 50)`, `rad_30_25` — на `(rad >= 30) | (rad <= 25)`:
    # исходы 51-54 и 26-29 из обучения ВЫБРОШЕНЫ. Для обучения это законно, для
    # цены — нет: на ставке карта, попавшая в зазор, проигрывает ОБЕИМ сторонам,
    # и кэф, снятый без неё, обещает винрейт, которого не бывает. У `dur43`
    # такого зазора нет — там деление сплошное.
    #
    # Хранится тройка (популяция, победа при ставке «да», победа при ставке
    # «нет»): у окон это знак разницы килов, у зазорных целей — попадание в свою
    # половину. Промах обеих означает мёртвую зону.
    rk_, dk_, dur_, pst_ = d["rk"], d["dk"], d["dur_min"], d["pst"]
    parsed_ = (rk_[:, :41].sum(1) > 0) | (dk_[:, :41].sum(1) > 0)
    BET_POP: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]] = {}
    for a_, b_ in ((5, 15), (10, 20), (15, 25), (20, 30)):
        diff_ = (rk_[:, b_] - rk_[:, a_]) - (dk_[:, b_] - dk_[:, a_])
        BET_POP[f"w_{a_}_{b_}"] = ((dur_ >= b_) & parsed_, diff_ > 0, diff_ < 0)
    tot_ = pst_[:, :, 0].sum(1)
    rad_ = pst_[:, :5, 0].sum(1)
    # Ноль килов за карту не бывает: это неразобранная карта, и без фильтра она
    # прошла бы как честная победа стороны «≤50» / «≤25».
    BET_POP["total_55_50"] = (tot_ > 0, tot_ >= 55, tot_ <= 50)
    BET_POP["rad_30_25"] = (tot_ > 0, rad_ >= 30, rad_ <= 25)

    for key, title, pos_, neg_, y, mask in B.build_targets(d):
        path = B.OUT_DIR / f"{key}.cbm"
        if not path.exists() or key not in panel:
            continue
        m = CatBoostClassifier()
        m.load_model(str(path))
        bet = BET_POP.get(key)
        t_ = test & (bet[0] if bet else mask)
        spec = ModelSpec(key=key, title=title, positive=pos_, negative=neg_,
                         threshold=float(panel[key]["threshold"]),
                         groups=B.GROUPS,
                         knots_x=tuple(panel[key]["knots_x"]),
                         knots_y=tuple(panel[key]["knots_y"]))
        p = np.array([spec.calibrate(v) for v in m.predict_proba(X[t_])[:, 1]])
        conf = np.maximum(p, 1 - p)
        if bet:
            win_pos, win_neg = bet[1][t_], bet[2][t_]
            hit = np.where(p >= 0.5, win_pos, win_neg).astype(float)
            tie_share = float((~win_pos & ~win_neg).mean())          # мёртвая зона
        else:
            yt = y[t_]
            hit = np.where(p >= 0.5, yt, 1 - yt).astype(float)
            tie_share = 0.0
        bands = bands_for(conf, hit)
        panel[key]["bands"] = bands
        say(f"## {title}")
        say()
        if bet:
            say(f"Ставочная популяция: {int(t_.sum()):,} карт, исходов вне "
                f"обеих сторон {tie_share:.1%} и они засчитаны в ПРОИГРЫШ. Кэф "
                f"по этой таблице выше, чем по обучающей популяции, — это и "
                f"есть цена ничьей и мёртвой зоны, а не расхождение.")
            say()
        say("| уверенность | карт | сбылось | кэф безубытка |")
        say("|---|---|---|---|")
        for b in bands:
            odds = 1.0 / max(b["hit"], 1e-6)
            say(f"| от {b['lo']:.2f} | {b['n']:,} | **{b['hit']:.1%}** | "
                f"**{odds:.2f}** |")
        say()
        print(f"  {key}: {len(bands)} полос ({time.time()-t0:.0f} c)", flush=True)

    payload["models"] = [panel[m["key"]] for m in payload["models"]]
    tmp = B.OUT_DIR / "panel.json.tmp"
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                   encoding="utf-8")
    tmp.replace(B.OUT_DIR / "panel.json")
    say(f"Полосы записаны в `panel.json`. Прогон занял {time.time()-t0:.0f} c.")


if __name__ == "__main__":
    main()
