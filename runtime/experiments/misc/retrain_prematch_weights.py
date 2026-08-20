#!/usr/bin/env python3
"""Пересчёт весов предматчевой модели с другим окном обучения.

Зачем отдельный скрипт, а не штатный `build_prematch_artifact_v2.py`: тот в
режиме переобучения падает по форме, потому что кэши признаков собраны на
482 486 картах, а корпус вырос до 1 260 250 (`base = np.load(EXT)["F"][keep]`,
маска нового корпуса по старому кэшу). Чинить это — отдельная работа: надо
пересобрать все кэши на новом корпусе или выровнять сборщик по `mid`.

Здесь матрица признаков собирается тем же путём, что в аудите боевого пути
(`audit_live_path.train_columns`), плюс колонка `h2h_resid` берётся из
`h2h_as_served.npz` — ровно как её берёт сборщик после починки E-177.

ГЛАВНОЕ — ПРОВЕРКА ЭКВИВАЛЕНТНОСТИ. Прежде чем обучать что-то новое, скрипт
воспроизводит СОХРАНЁННЫЕ в артефакте нормировки и коэффициенты для его же
четырёх окон. Если хоть одно число разойдётся сверх допуска — скрипт
останавливается и ничего не пишет: значит матрица собрана не так, как при
обучении боевых весов, и любые новые веса будут посчитаны не в той шкале.

Новый файл весов пишется рядом, боевой не трогается. Подставляется в цепочку
через `PREMATCH_WEIGHTS` у `finalize_artifact.py`.

Запуск: venv_catboost/bin/python3 runtime/experiments/misc/retrain_prematch_weights.py [окна через запятую]
Выход:  runtime/artifacts/misc/prematch_weights_<тег>.npz + retrain_prematch_weights.md
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import numpy as np
from sklearn.linear_model import LogisticRegression

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "runtime/experiments/misc"))
sys.path.insert(0, str(ROOT / "base"))
from ideas_batch2 import COMPACT, RICH, TEST_FROM  # noqa: E402
from pro_features_wide import auc  # noqa: E402
import audit_live_path as A  # noqa: E402

ART = ROOT / "runtime/artifacts/misc"
BASE_ART = ART / "prematch_model_artifact_v3_hybrid.npz"
H2H = ART / "h2h_as_served.npz"
HYBRID = ART / "map_winner_hybrid_quality_forward/hybrid_features.npz"
OUT_MD = ART / "retrain_prematch_weights.md"
DAY = 86400
TOL_MU = 1e-9          # допуск на воспроизведение нормировок
TOL_COEF = 5e-3        # коэффициенты: тот же оптимизатор, но не тот же порядок строк

_lines: list[str] = []


def say(s: str = "") -> None:
    print(s, flush=True)
    _lines.append(s)
    OUT_MD.write_text("\n".join(_lines) + "\n", encoding="utf-8")


def build_matrix():
    zc, zr = np.load(COMPACT), np.load(RICH)
    hz = np.load(HYBRID, allow_pickle=True)
    old = hz["mids"].astype(np.int64)
    cpos = {int(m): i for i, m in enumerate(zc["mids"].tolist())}
    rpos = {int(m): i for i, m in enumerate(zr["mids"].tolist())}
    have = np.array([int(m) in cpos and int(m) in rpos for m in old.tolist()])
    old_ok = old[have]
    ci = np.array([cpos[int(m)] for m in old_ok.tolist()])
    ri = np.array([rpos[int(m)] for m in old_ok.tolist()])
    ts = zc["ts"][ci].astype(np.int64)
    y = zc["wins"][ci].astype(int)
    dep = np.load(BASE_ART, allow_pickle=True)
    names = [str(x) for x in dep["feature_names"]]
    X, _ = A.train_columns(ts, zc["accounts"][ci], zr["pstats"][ri],
                           zr["durations"][ri].astype(float) / 60.0, old_ok,
                           np.ones(len(ts), bool), dep["ctx_mu"], dep["ctx_sd"], names)
    h2h = np.load(H2H)["value"]
    if len(h2h) != len(X):
        raise SystemExit(f"h2h_as_served длиной {len(h2h)}, а карт {len(X)}")
    X[:, names.index("h2h_resid")] = h2h
    return X, y, ts, names, dep


def fit_window(X, y, mask):
    mu, sd = X[mask].mean(0), X[mask].std(0) + 1e-9
    m = LogisticRegression(C=1.0, max_iter=5000).fit((X[mask] - mu) / sd, y[mask])
    return mu, sd, m.coef_[0], float(m.intercept_[0])


def probs(X, rows, mus, sds, coefs, ints):
    """Вероятности ансамбля: усреднение ВЕРОЯТНОСТЕЙ, как в prematch_scorer."""
    acc = np.zeros(int(rows.sum()))
    zmax = 0.0
    for mu, sd, c, b in zip(mus, sds, coefs, ints):
        Z = (X[rows] - mu) / sd
        zmax = max(zmax, float(np.abs(Z).max()))
        z = np.clip(Z @ c + b, -60.0, 60.0)   # за пределами полосы sigmoid = 0/1
        acc += 1.0 / (1.0 + np.exp(-z))
    p = acc / len(coefs)
    if not np.isfinite(p).all():
        raise SystemExit("в вероятностях появились нечисла")
    print(f"    максимум |стандартизованного значения| на тесте {zmax:.1f}",
          flush=True)
    return p


def main() -> None:
    t0 = time.time()
    windows = tuple(int(x) for x in (sys.argv[1] if len(sys.argv) > 1 else "120").split(","))
    X, y, ts, names, dep = build_matrix()
    train, test = ts < TEST_FROM, ts >= TEST_FROM
    tmax = ts[train].max()
    yt = y[test]
    say("# Пересчёт весов предматчевой модели: другое окно обучения")
    say()
    say(f"Карт {len(y):,}, обучение {int(train.sum()):,}, тест {int(test.sum()):,}. "
        f"Матрица собрана путём аудита боевого пути, `h2h_resid` — из "
        f"`h2h_as_served.npz`.")
    say()

    # ---------- гейт: воспроизводим сохранённые веса ----------
    say("## Проверка эквивалентности: воспроизводится ли боевой артефакт")
    say()
    say("| окно | max|Δmu| | max|Δsd| | max|Δcoef| | Δintercept |")
    say("|---|---|---|---|---|")
    # Окна берём ИЗ ФОРМЫ АРТЕФАКТА, а не из зашитого списка. До E-192/E-193
    # боевые веса были ансамблем 90/180/365/730, сейчас там ОДНО окно 120 дней,
    # и зашитая четвёрка роняла сверку на `dep["mu"][1]` с IndexError — то есть
    # гейт эквивалентности, один из двух работающих механизмов защиты, сам не
    # запускался.
    #
    # Сам артефакт длину окна НЕ хранит (тот же класс «нет отпечатка входа», что
    # и в E-201), поэтому соответствие числа окон их длинам — по конвенции:
    # одно окно = 120 дней (см. имя файла весов `prematch_weights_win120.npz`),
    # четыре = прежний ансамбль. Незнакомое число окон останавливает сверку, а
    # не угадывает.
    _n_windows = int(np.asarray(dep["mu"]).shape[0])
    _known = {1: (120,), 4: (90, 180, 365, 730)}
    if _n_windows not in _known:
        raise SystemExit(
            f"в артефакте {_n_windows} окон весов — неизвестная конфигурация. "
            "Сверка эквивалентности не может знать их длины: артефакт их не "
            "хранит. Добавьте соответствие в `_known` осознанно.")
    old_windows = _known[_n_windows]
    say(f"окон весов в артефакте: {_n_windows} -> длины {old_windows}")
    say()
    ok = True
    for i, dd in enumerate(old_windows):
        mk = train & (ts >= tmax - dd * DAY)
        mu, sd, c, b = fit_window(X, y, mk)
        dmu = float(np.abs(mu - dep["mu"][i]).max())
        dsd = float(np.abs(sd - dep["sd"][i]).max())
        dc = float(np.abs(c - dep["coef"][i]).max())
        db = abs(b - float(dep["intercept"][i]))
        say(f"| {dd} | {dmu:.2e} | {dsd:.2e} | {dc:.2e} | {db:.2e} |")
        if dmu > TOL_MU or dsd > TOL_MU or dc > TOL_COEF:
            ok = False
    say()
    if not ok:
        say("**ОСТАНОВЛЕНО.** Матрица собрана не так, как при обучении боевых "
            "весов: новые коэффициенты оказались бы в другой шкале. Ничего не "
            "записано.")
        raise SystemExit(1)
    say("Совпало в пределах допуска — рецепт тот же, можно переобучать.")
    say()

    # ---------- старые и новые веса на тесте ----------
    p_old = probs(X, test, dep["mu"], dep["sd"], dep["coef"], dep["intercept"])
    mus, sds, coefs, ints = [], [], [], []
    for dd in windows:
        mk = train & (ts >= tmax - dd * DAY)
        if mk.sum() < 10000:
            say(f"окно {dd} пропущено: карт {int(mk.sum()):,} < 10 000")
            continue
        mu, sd, c, b = fit_window(X, y, mk)
        mus.append(mu); sds.append(sd); coefs.append(c); ints.append(b)
    if not mus:
        raise SystemExit("ни одно окно не набрало 10 000 карт")
    p_new = probs(X, test, mus, sds, coefs, ints)
    a_old, a_new = auc(yt, p_old), auc(yt, p_new)
    tag = "_".join(str(w) for w in windows)
    say("## Старые веса против новых")
    say()
    say("| веса | моделей | AUC на тесте |")
    say("|---|---|---|")
    say(f"| ансамбль 90/180/365/730 (в бою) | 4 | {a_old:.4f} |")
    say(f"| окно {tag} | {len(mus)} | **{a_new:.4f}** |")
    say()
    say(f"Разница {a_new - a_old:+.4f}. Средняя |разница| вероятностей "
        f"{float(np.abs(p_new - p_old).mean()):.4f}, доля карт, где стороны "
        f"расходятся, {float(((p_new > 0.5) != (p_old > 0.5)).mean()):.2%}.")
    say()

    out = ART / f"prematch_weights_win{tag}.npz"
    z = dict(np.load(BASE_ART, allow_pickle=True))
    z["mu"] = np.array(mus)
    z["sd"] = np.array(sds)
    z["coef"] = np.array(coefs)
    z["intercept"] = np.array(ints)
    # numpy сам дописывает `.npz`, поэтому временное имя обязано на него
    # заканчиваться — иначе rename не найдёт файла
    tmp = out.with_name(out.stem + ".tmp.npz")
    np.savez_compressed(tmp, **z)
    tmp.replace(out)                       # rebuild-then-replace
    say(f"Файл весов: `{out.name}` (моделей {len(mus)}, признаков "
        f"{len(z['feature_names'])}). Боевой артефакт не тронут; подставляется "
        f"в цепочку через `PREMATCH_WEIGHTS={out}`.")
    say()
    say(f"Прогон занял {time.time() - t0:.0f} c.")


if __name__ == "__main__":
    main()
