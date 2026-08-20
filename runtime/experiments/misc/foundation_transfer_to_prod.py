#!/usr/bin/env python3
"""Сколько из +0.0036 драфтовой смеси доходит до боевой предматчевой модели.

ЗАЧЕМ. `draft_foundation_summary.md` (17.08) даёт на паблике 0.6261 у смеси
против 0.6225 у линейной решётки пар — то есть пробивает асимптоту 0.625,
которую E-74 объявил информационным потолком входа «10 героев по позициям».
Первым пунктом в «Ограничениях» той сводки стоит: перенос прибавки в боевую
про-модель НЕ ИЗМЕРЕН. Здесь он измеряется.

КАК. Тем же способом, что E-201 мерил цену перекоса каталогов: собирается
боевая матрица, подменяются ТРИ колонки (`draft_logit` и две её интеракции с
контекстом, потому что они от него зависят), дальше два числа на вариант —
без переобучения весов (что случится при простом переключении) и с
переобучением (честный потолок замены).

Встроенная проверка: интеракции пересобираются из СТАРОГО логита и сравниваются
с теми, что лежат в матрице. Не совпало — скрипт встаёт, потому что тогда любая
подмена считалась бы не в той шкале. Взято из `swap_draft_logit_ab.py`.

ЧТО ПОДМЕНЯЕТСЯ (три варианта на одном тесте):
  old    — колонка как есть в матрице: логит, под который подогнаны боевые веса;
  lin    — линейная решётка текущего боевого каталога, отдельной строкой,
           чтобы отделить вклад смеси от вклада каталога;
  blend  — z(lin) + z(d256-победа) + z(d128-мультизадача), равные веса.

ПОЧЕМУ РАВНЫЕ ВЕСА. В сводке подобранные веса дают 0.6261 против 0.6260 у
равных: настройки в прибавке нет. Равные веса убирают ручку, которую иначе
пришлось бы подбирать, и по правилу дома подбирать её было бы негде — валидация
здесь про-шная, а веса смеси подбирались на паблике.

НОРМИРОВКА КОМПОНЕНТ СЧИТАЕТСЯ ПО ОБУЧАЮЩЕМУ ОКНУ, не по всему набору и тем
более не по тесту: иначе масштаб смеси знал бы про тест.

ОГОВОРКА ПРО ВРЕМЯ, снята до выводов. Сети учились на паблике 03-24..06-20,
про-тест идёт с 03-29 — периоды перекрываются. Но боевой логит учился на ВСЕХ
5.09 млн до 08-09 (`win_model_veto` признаёт, что честного AUC у него нет), то
есть у сетей временного преимущества МЕНЬШЕ, чем у действующего признака.
Дельта сравнима с дельтой E-201; абсолютные числа форвардными не являются ни у
одного варианта.

Запуск: venv_catboost/bin/python3 runtime/experiments/misc/foundation_transfer_to_prod.py
Выход:  runtime/artifacts/misc/foundation_transfer_to_prod.md
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import warnings

import numpy as np

warnings.filterwarnings("ignore", message=".*encountered in matmul.*")

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "runtime/experiments/misc"))
sys.path.insert(0, str(ROOT / "base"))
from ideas_batch2 import COMPACT, RICH, TEST_FROM  # noqa: E402
from pro_features_wide import auc  # noqa: E402
import retrain_prematch_weights as R  # noqa: E402

ART = ROOT / "runtime/artifacts/misc"
EXP = ROOT / "data/public_draft_hero10_experiment"
PUB_CORPUS = ART / "draft_full_corpus.npz"
NET_WIN = ART / "draft_foundation_win.pt"        # d=256, ffn=1024, голова победы
NET_MULTI = ART / "draft_foundation_multi.pt"    # d=128, ffn=512, 14 signed + 6 unsigned
OUT_MD = ART / "foundation_transfer_to_prod.md"
OUT_NPZ = ART / "foundation_transfer_to_prod.npz"
WINDOW = int(os.getenv("WINDOW", "120"))
SEED = 20260820
DAY = 86400

_lines: list[str] = []


def say(s: str = "") -> None:
    print(s, flush=True)
    _lines.append(s)
    OUT_MD.write_text("\n".join(_lines) + "\n", encoding="utf-8")


def pro_heroes(mids: np.ndarray) -> np.ndarray:
    zc = np.load(COMPACT)
    pos = {int(m): i for i, m in enumerate(zc["mids"].tolist())}
    return zc["heroes"][np.array([pos[int(m)] for m in mids.tolist()])].astype(np.int64)


def linear_logit(her: np.ndarray, catalog: str) -> np.ndarray:
    import joblib

    d = EXP / catalog
    enc = joblib.load(d / "win_feature_encoder.joblib")
    mod = joblib.load(d / "radiant_win_model.joblib")
    out = np.empty(len(her), np.float64)
    for lo in range(0, len(her), 200_000):
        hi = min(lo + 200_000, len(her))
        out[lo:hi] = mod.decision_function(enc.transform(her[lo:hi]))
    return out


def net_logits(her_raw: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Выходы обеих сетей на про-драфтах. Словарь героев берётся из паблика."""
    import torch
    from draft_foundation import DraftNet, SIGNED, UNSIGNED

    ids = np.unique(np.load(PUB_CORPUS)["heroes"])
    lut = np.full(int(ids.max()) + 1, -1, np.int64)
    lut[ids] = np.arange(len(ids))
    her = lut[her_raw]
    if her.min() < 0:
        raise SystemExit("в про-корпусе герой, которого нет в паблик-словаре")

    def build(path: Path, d_model: int, ffn: int, n_s: int, n_u: int):
        net = DraftNet(len(ids), d_model=d_model, ffn=ffn, n_signed=n_s, n_unsigned=n_u)
        net.load_state_dict(torch.load(path, map_location="cpu"), strict=True)
        return net.eval()

    nets = {
        "net256": (build(NET_WIN, 256, 1024, 1, 0), 0),
        "netmulti": (build(NET_MULTI, 128, 512, len(SIGNED), len(UNSIGNED)),
                     list(SIGNED).index("win")),
    }
    dev = torch.device("mps") if torch.backends.mps.is_available() else torch.device("cpu")
    out = {}
    for name, (net, col) in nets.items():
        net = net.to(dev)
        buf = np.empty(len(her), np.float64)
        t0 = time.time()
        with torch.no_grad():
            for lo in range(0, len(her), 32768):
                hi = min(lo + 32768, len(her))
                s, _ = net(torch.as_tensor(her[lo:hi], device=dev))
                buf[lo:hi] = s[:, col].float().cpu().numpy()
        print(f"  {name}: {len(her):,} карт за {time.time()-t0:.0f} c, "
              f"sd {buf.std():.4f}", flush=True)
        out[name] = buf
    return out["net256"], out["netmulti"]


def zscore(v: np.ndarray, mask: np.ndarray) -> np.ndarray:
    """Нормировка ПО ОБУЧАЮЩЕМУ ОКНУ: масштаб не имеет права знать про тест."""
    return (v - v[mask].mean()) / v[mask].std()


def boot(y: np.ndarray, pa: np.ndarray, pb: np.ndarray, n: int = 500) -> tuple:
    rng = np.random.default_rng(SEED)
    d = np.empty(n)
    idx = np.arange(len(y))
    for i in range(n):
        s = rng.choice(idx, len(idx), replace=True)
        if y[s].min() == y[s].max():
            d[i] = 0.0
            continue
        d[i] = auc(y[s], pb[s]) - auc(y[s], pa[s])
    return float(np.percentile(d, 2.5)), float(np.percentile(d, 97.5)), float((d > 0).mean())


def main() -> None:
    t0 = time.time()
    X, y, ts, names, dep = R.build_matrix()
    print(f"[{time.time()-t0:.0f}с] матрица {X.shape}", flush=True)

    zc = np.load(COMPACT)
    hz = np.load(ART / "map_winner_hybrid_quality_forward/hybrid_features.npz",
                 allow_pickle=True)
    zr = np.load(RICH)
    cpos = {int(m): i for i, m in enumerate(zc["mids"].tolist())}
    rpos = {int(m): i for i, m in enumerate(zr["mids"].tolist())}
    old = hz["mids"].astype(np.int64)
    mids = old[np.array([int(m) in cpos and int(m) in rpos for m in old.tolist()])]
    if len(mids) != len(X):
        raise SystemExit(f"карт {len(mids)}, а строк {len(X)}")

    i_d = names.index("draft_logit")
    i_e = names.index("draft_logit_x_elo_gap")
    i_g = names.index("draft_logit_x_games_exp")
    ctx = np.column_stack([np.abs(X[:, 1]), np.abs(X[:, 2])])
    ctxz = (ctx - dep["ctx_mu"]) / dep["ctx_sd"]

    e_err = float(np.abs(X[:, i_d] * ctxz[:, 0] - X[:, i_e]).max())
    g_err = float(np.abs(X[:, i_d] * ctxz[:, 1] - X[:, i_g]).max())
    print(f"проверка интеракций: max|Δ| {e_err:.2e} и {g_err:.2e}", flush=True)
    if max(e_err, g_err) > 1e-6:
        raise SystemExit("формула интеракций восстановлена неверно")

    train, test = ts < TEST_FROM, ts >= TEST_FROM
    tmax = ts[train].max()
    mk = train & (ts >= tmax - WINDOW * DAY)
    yt = y[test]

    her = pro_heroes(mids)
    print("считаю линейный логит боевого каталога…", flush=True)
    lin = linear_logit(her, "2026-08-15_all_public_5m_full")
    print("считаю сети…", flush=True)
    n256, nmul = net_logits(her)

    zl, z2, zm = zscore(lin, mk), zscore(n256, mk), zscore(nmul, mk)
    blend = (zl + z2 + zm) / 3.0
    # смесь стоит в шкале СТАРОЙ колонки, иначе вариант без переобучения мерил бы
    # смену амплитуды, а не смену содержания (ровно грабля E-200/E-201)
    scale = X[mk, i_d].std() / blend[mk].std()
    variants = {
        "old": X[:, i_d],
        "lin": lin,
        "blend": blend * scale + X[mk, i_d].mean(),
    }

    def swap(col: np.ndarray) -> np.ndarray:
        Xn = X.copy()
        Xn[:, i_d] = col
        Xn[:, i_e] = col * ctxz[:, 0]
        Xn[:, i_g] = col * ctxz[:, 1]
        return Xn

    def prob(Xm, mu, sd, c, b):
        # lbfgs в линейном поиске заходит в веса, где matmul переполняется, и
        # возвращается обратно; итог конечен. Проверено: базовая строка даёт
        # 0.7188 и коэффициент драфта +0.2794 — оба числа E-201 до знака.
        # Поэтому шум глушим, а взамен ТРЕБУЕМ конечности итога.
        with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
            z = ((Xm - mu) / sd) @ c + b
        if not np.isfinite(z).all():
            raise SystemExit("в логите появились нечисла — молча считать нельзя")
        return 1 / (1 + np.exp(-np.clip(z, -60, 60)))

    say("# Перенос драфтовой смеси в предматчевую модель")
    say()
    say(f"Карт {len(y):,}, обучающее окно {WINDOW} дней ({int(mk.sum()):,}), "
        f"тест {int(test.sum()):,} после `TEST_FROM`. Подменяются три колонки; "
        f"формула интеракций проверена восстановлением (max|Δ| "
        f"{max(e_err, g_err):.1e}).")
    say()
    say("## Компоненты смеси на про-корпусе")
    say()
    say("| компонент | sd | корр. со старым логитом |")
    say("|---|---:|---:|")
    for nm, v in (("линейная решётка (боевой каталог)", lin),
                  ("трансформер d=256, победа", n256),
                  ("трансформер d=128, мультизадача", nmul)):
        say(f"| {nm} | {v.std():.4f} | {np.corrcoef(v, X[:, i_d])[0,1]:.4f} |")
    say(f"| **смесь (равные веса на z)** | {blend.std():.4f} | "
        f"{np.corrcoef(blend, X[:, i_d])[0,1]:.4f} |")
    say()

    res = {}
    mu, sd, c, b = R.fit_window(X, y, mk)          # боевые веса под старую колонку
    for nm, col in variants.items():
        Xn = swap(col)
        p_swap = prob(Xn[test], mu, sd, c, b)
        mu2, sd2, c2, b2 = R.fit_window(Xn, y, mk)
        p_ref = prob(Xn[test], mu2, sd2, c2, b2)
        res[nm] = (auc(yt, p_swap), auc(yt, p_ref), p_swap, p_ref, float(c2[i_d]))
        print(f"{nm}: без переобучения {res[nm][0]:.4f}, с переобучением "
              f"{res[nm][1]:.4f}", flush=True)

    say("## Результат")
    say()
    say("| подаётся в веса | без переобучения | с переобучением | коэф. драфта |")
    say("|---|---:|---:|---:|")
    for nm, title in (("old", "старая колонка (боевой ориентир)"),
                      ("lin", "линейная решётка боевого каталога"),
                      ("blend", "**смесь: линейная + d256 + мультизадача**")):
        a_s, a_r, _, _, cf = res[nm]
        say(f"| {title} | {a_s:.4f} | {a_r:.4f} | {cf:+.4f} |")
    say()

    base = res["old"][0]
    say("## Прибавка к боевому ориентиру")
    say()
    say("| вариант | Δ AUC | 95% ДИ | доля>0 |")
    say("|---|---:|---|---:|")
    for nm in ("lin", "blend"):
        for k, tag in ((2, "без переобучения"), (3, "с переобучением")):
            lo, hi, sh = boot(yt, res["old"][2], res[nm][k])
            say(f"| {nm}, {tag} | {res[nm][k-2]-base:+.4f} | "
                f"[{lo:+.4f}, {hi:+.4f}] | {sh:.2f} |")
    say()
    say(f"Прибавка смеси НА ПАБЛИКЕ (сводка 17.08): +0.0036 "
        f"(0.6225 → 0.6261). Коэффициент переноса по строке «смесь, "
        f"с переобучением»: "
        f"{(res['blend'][1]-base)/0.0036:.2f}.")
    say()
    say("## Винрейт выбора модели по порогам уверенности (тест)")
    say()
    say("| порог |p−0.5| | вариант | карт | винрейт |")
    say("|---|---|---:|---:|")
    for thr in (0.05, 0.10, 0.15, 0.20):
        for nm in ("old", "blend"):
            p = res[nm][3]
            m = np.abs(p - 0.5) >= thr
            if m.sum() == 0:
                continue
            hit = ((p[m] > 0.5) == (yt[m] == 1)).mean()
            say(f"| {thr:.2f} | {nm} | {int(m.sum()):,} | {hit:.4f} |")
    say()
    np.savez_compressed(OUT_NPZ, mids=mids, lin=lin, net256=n256, netmulti=nmul,
                        old=X[:, i_d], blend=variants["blend"], ts=ts, y=y)
    say(f"Готово за {time.time()-t0:.0f} c. Предсказания и колонки — `{OUT_NPZ.name}`.")


if __name__ == "__main__":
    main()
