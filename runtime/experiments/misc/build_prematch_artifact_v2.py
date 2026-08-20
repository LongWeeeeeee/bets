#!/usr/bin/env python3
"""Артефакт v2: 23 признака + 6 взаимодействий (боевой максимум 0.7140).

Отличия от v1:
  * добавлены `lvl_rel_pos` (уровень против нормы позиции, E-138, +0.0011),
    `kda_player` (KDA игрока за последние 20 матчей, E-117) и `farm_dep`
    (фарм-зависимость героя — добиваний в минуту по корпусу, E-117);
  * добавлены 6 колонок взаимодействий: драфт-логит, ELO и форма умножаются на
    |разрыв рейтингов| и на опыт составов. Взаимодействие с ЭПОХОЙ намеренно НЕ
    включено: в обучении она в [0,1], а в бою всегда >1 — модель экстраполировала
    бы по времени. Цена отказа измерена: 0.7140 против 0.7149.

Нормировка контекста (mu/sd) сохраняется в артефакт — скорер обязан считать
взаимодействия ровно так же, иначе веса окажутся не на своих местах.

Запуск: venv_catboost/bin/python3 runtime/experiments/misc/build_prematch_artifact_v2.py
"""
from __future__ import annotations

import json
import math
import os
import sys
from collections import defaultdict, deque
from pathlib import Path

import numpy as np
from sklearn.linear_model import LogisticRegression

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "runtime/experiments/misc"))
from ideas_batch2 import B1, B1_IDX, COMPACT, EI, EXT, LIVE, RICH  # noqa: E402
from ideas_batch2 import CACHE as B2C, NI as B2NI  # noqa: E402
from ideas_batch5 import CACHE as B5C, NI as B5NI  # noqa: E402
from ideas_batch5b import CACHE as B5BC, NI as B5BNI  # noqa: E402
from pro_features_wide import auc  # noqa: E402

# Аудит: та же обрезка по времени, что у v1 — накопители lvl/kda/farm обязаны
# видеть только прошлое, иначе честной оценки боевого пути не выйдет.
CUTOFF = int(os.getenv("PREMATCH_CUTOFF", "0"))
# Режим ночного обновления: пересобрать ТОЛЬКО снимок, веса не трогать. Нужен
# потому, что кэши признаков (`ideas_batch*.npz`, EXT, логит) привязаны к длине
# корпуса: как только корпус подрос, склейка матрицы падает по форме. Веса при
# этом устаревают куда медленнее снимка — их обновляют отдельно и с замером.
SNAPSHOT_ONLY = os.getenv("PREMATCH_SNAPSHOT_ONLY", "0") == "1"
# Отдельное имя у режима «только снимок»: он НЕ содержит весов, и затирать им
# обученный артефакт нельзя — на переобучение уходит полчаса.
_SUF = (f"_cut{CUTOFF}" if CUTOFF else "_snapshot" if SNAPSHOT_ONLY
        else ("" if os.getenv("PREMATCH_WITH_HYBRID", "0") == "1" else "_nohybrid"))
OUT = ROOT / f"runtime/artifacts/misc/prematch_model_artifact_v2{_SUF}.npz"
SPEC = ROOT / f"runtime/artifacts/misc/prematch_model_spec_v2{_SUF}.json"
BASE20 = ["draft_logit", "elo", "games", "hero_games", "pos_games", "opp_elo",
          "hero_pool", "form", "hero_gpm_rel", "imp_recent", "wr30", "h2h_resid",
          "gpm_rel_pos", "vs_wr", "imp50", "imp_rel_pos", "lh_rel_hero",
          "gpm_ewma", "lh30", "imp30"]
EXTRA3 = ["lvl_rel_pos", "kda_player", "farm_dep"]
INTER_KEYS = ["draft_logit", "elo", "form"]          # что умножаем
INTER_CTX = ["elo_gap", "games_exp"]                 # на что умножаем
# E-168 (кандидат C, +0.0044 на тесте 26 016): рейтинг Hybrid и пять признаков
# добавляются РЯДОМ, `elo` и контекст взаимодействий не трогаются.
NEW6 = ["hybrid_strength", "cp_lane", "syn_pos_mean",
        "a_hdmg_rel_pos", "a_hdmg_rel_hero", "a_nw_rel_pos"]
# `hybrid_strength` требует в бою team_id/team_name/tier/timestamp: он считается
# через `preview_team_strength`, а `win_index_ex` зовут из середины разбора
# драфта, где этих полей нет. Пока проводки нет, колонка выключается —
# оставшиеся пять дают измеренные +0.0017 [+0.0003,+0.0031] против +0.0044
# у полного набора, и не требуют ни одной правки вне артефакта и скорера.
WITH_HYBRID = os.getenv("PREMATCH_WITH_HYBRID", "0") == "1"
NEW_COLS = NEW6 if WITH_HYBRID else [c for c in NEW6 if c != "hybrid_strength"]
FEATURES = BASE20 + EXTRA3 + NEW_COLS + [f"{k}_x_{c}" for c in INTER_CTX for k in INTER_KEYS]
K, HL_EWMA = 24.0, 90.0
P_GPM, P_LH, P_IMP, P_LEVEL = 3, 6, 11, 12
P_KILL, P_DEATH, P_ASSIST = 0, 1, 2
TEST_FROM = 1774742400


def main() -> None:
    zc, zr = np.load(COMPACT), np.load(RICH)
    pos_ = {int(m): i for i, m in enumerate(zr["mids"].tolist())}
    keep = np.array([int(m) in pos_ for m in zc["mids"].tolist()])
    idx_r = np.array([pos_[int(m)] for m in zc["mids"][keep].tolist()])
    ts, wins = zc["ts"][keep], zc["wins"][keep].astype(int)
    heroes, accounts = zc["heroes"][keep], zc["accounts"][keep]
    pst = zr["pstats"][idx_r]
    dur = zr["durations"][idx_r].astype(float) / 60.0
    if CUTOFF:
        msk = ts < CUTOFF
        ts, wins, heroes, accounts = ts[msk], wins[msk], heroes[msk], accounts[msk]
        pst, dur = pst[msk], dur[msk]
    n = len(ts)
    print(f"карт: {n:,}" + (f" (обрезка по ts < {CUTOFF})" if CUTOFF else ""), flush=True)

    # ---- новые счётчики
    lvl_res: dict[int, list] = defaultdict(lambda: [0.0, 0.0])
    kda_q: dict[int, deque] = defaultdict(lambda: deque(maxlen=20))
    lh_hero: dict[int, list] = defaultdict(lambda: [0.0, 0.0])
    pos_sum = np.zeros((6, 14)); pos_cnt = np.zeros(6)
    # Отдельный накопитель ПОМИНУТНОЙ нормы позиции: pos_sum копит сырой вектор
    # и нужен другим признакам, а делить сырую норму на длительность нельзя —
    # получится смесь двух шкал вместо величины, которой мерился кандидат C.
    pos_lvl_pm = np.zeros(6)
    lvl_col = np.zeros(n); kda_col = np.zeros(n); farm_col = np.zeros(n)
    for i in range(n):
        sides = []
        for s in range(2):
            a5 = [int(a) for a in accounts[i, s*5:(s+1)*5]]
            h5 = [int(h) for h in heroes[i, s*5:(s+1)*5]]
            lv = [lvl_res[a][0]/lvl_res[a][1] if lvl_res[a][1] else 0.0 for a in a5 if a > 0]
            kd = [float(np.mean(kda_q[a])) if kda_q[a] else 0.0 for a in a5 if a > 0]
            fd = [lh_hero[h][0]/lh_hero[h][1] if lh_hero[h][1] else 0.0 for h in h5]
            sides.append((float(np.mean(lv)) if lv else 0.0,
                          float(np.mean(kd)) if kd else 0.0,
                          float(np.mean(fd)) if fd else 0.0))
        lvl_col[i] = sides[0][0] - sides[1][0]
        kda_col[i] = sides[0][1] - sides[1][1]
        farm_col[i] = sides[0][2] - sides[1][2]
        dm = max(float(dur[i]), 1.0)
        for s in range(2):
            for p in range(5):
                a, h = int(accounts[i, s*5+p]), int(heroes[i, s*5+p])
                st = pst[i, s*5+p]
                if a > 0 and pos_cnt[p+1] > 0:
                    pn = pos_sum[p+1] / pos_cnt[p+1]
                    # поминутный уровень против ПОМИНУТНОЙ же нормы позиции —
                    # один в один с dur_pass, которым мерился кандидат C
                    lvl_pm = float(st[P_LEVEL]) / dm
                    pn_pm = pos_lvl_pm[p + 1] / pos_cnt[p + 1]
                    c = lvl_res[a]; c[0] += lvl_pm - pn_pm; c[1] += 1
                if a > 0:
                    kda_q[a].append((float(st[P_KILL]) + float(st[P_ASSIST]))
                                    / (1.0 + float(st[P_DEATH])))
                c = lh_hero[h]; c[0] += float(st[P_LH]) / dm; c[1] += 1
                pos_sum[p+1] += st; pos_lvl_pm[p+1] += float(st[P_LEVEL]) / dm
                pos_cnt[p+1] += 1
        if (i+1) % 100_000 == 0: print(f"  {i+1:,}/{n:,}", flush=True)

    if CUTOFF or SNAPSHOT_ONLY:
        _v1 = (f"prematch_model_artifact_cut{CUTOFF}.npz" if CUTOFF
               else "prematch_model_artifact.npz")
        z1 = np.load(ROOT / "runtime/artifacts/misc" / _v1)
        acc_old = z1["accounts"]
        lvl_map = {a: (lvl_res[a][0]/lvl_res[a][1] if lvl_res[a][1] else 0.0) for a in lvl_res}
        kda_map = {a: (float(np.mean(kda_q[a])) if kda_q[a] else 0.0) for a in kda_q}
        acc_new = np.column_stack([acc_old,
                                   np.array([lvl_map.get(int(r[0]), 0.0) for r in acc_old]),
                                   np.array([kda_map.get(int(r[0]), 0.0) for r in acc_old])])
        farm_map = np.array([[h, lh_hero[h][0]/lh_hero[h][1]] for h in sorted(lh_hero)
                             if lh_hero[h][1] > 0], dtype=np.float64)
        np.savez_compressed(OUT, accounts=acc_new, acc_hero=z1["acc_hero"],
                            acc_pos=z1["acc_pos"], hero_wr30=z1["hero_wr30"],
                            vs_pairs=z1["vs_pairs"], h2h=z1["h2h"], hero_farm=farm_map,
                            snapshot_ts=z1["snapshot_ts"])
        print(f"снимок с обрезкой: {OUT}; аккаунтов {len(acc_new):,}, героев с фармом "
              f"{len(farm_map):,}", flush=True)
        return

    # ---- матрица признаков
    # Предохранитель рассинхрона. Кэши признаков адресуются ПОЗИЦИЕЙ в массиве,
    # а не `mid`, поэтому любой рост корпуса делает маску `keep` длиннее кэша, и
    # numpy падает с `IndexError: boolean index did not match` где-то в глубине.
    # Так уже дважды теряли время (14.08 и 15.08). Здесь ошибка называет числа и
    # средство сразу.
    _cache_rows = int(np.load(EXT)["F"].shape[0])
    if len(keep) != _cache_rows:
        raise SystemExit(
            f"РАССИНХРОН КОРПУСА И КЭШЕЙ: корпус {len(keep):,} карт, кэш признаков "
            f"{_cache_rows:,}. Переобучение весов на таком входе невозможно.\n"
            "Снимок для прода это НЕ задевает — он идёт веткой PREMATCH_SNAPSHOT_ONLY=1 "
            "и кэшей не читает.\n"
            "Чтобы переобучать веса, нужно пересобрать кэши признаков на текущем "
            "корпусе (ideas_batch*, EXT, драфт-логит) либо выровнять сборщики по `mid`.")

    base = np.load(EXT)["F"][keep]
    lgt = np.load(ROOT/"runtime/artifacts/misc/pro_draft_logit_full.npz")["logit"]
    f1, f2 = np.load(B1)["F"], np.load(B2C)["F"]
    f5, f5b = np.load(B5C)["F"], np.load(B5BC)["F"]
    LIVE8 = [x for x in LIVE if x != "strong_wr"]
    # E-177: h2h обучается на том, что скорер РЕАЛЬНО считает — по организациям и
    # без удвоения. Обучающая колонка `i23_h2h_resid` собиралась как
    # `side[0] − side[1]` при `side[1] = −side[0]`, то есть вдвое больше боевой,
    # и вдобавок по сырым team_id, которых в бою нет (`_prematch_index` передаёт 0).
    _h2h = ROOT / "runtime/artifacts/misc/h2h_as_served.npz"
    h2h_col = np.load(_h2h)["value"] if _h2h.exists() else f2[:, B2NI["i23_h2h_resid"]]
    if not _h2h.exists():
        print("ВНИМАНИЕ: h2h_as_served.npz нет, беру старую колонку", flush=True)
    X20 = np.column_stack([lgt] + [base[:, EI[x]] for x in LIVE8]
        + [f1[:, B1_IDX["i3_hero_gpm_rel"]], f1[:, B1_IDX["i2_imp_recent"]],
           f2[:, B2NI["i14_wr30"]], h2h_col]
        + [f5[:, B5NI[x]] for x in ("a_gpm_rel_pos","b_vs_wr","a_imp50","a_imp_rel_pos","a_lh_rel_hero")]
        + [f5b[:, B5BNI[x]] for x in ("v_gpm_ewma","v_lh30","v_imp30")])
    X23 = np.column_stack([X20, lvl_col, kda_col, farm_col])

    # --- новые колонки, выравнивание строго по mid ---
    mids_k = zc["mids"][keep].astype(np.int64)
    ART = ROOT / "runtime/artifacts/misc"
    # HYBRID_COL: колонка, посчитанная ТЕМ ЖЕ вызовом, что доступен в бою
    # (`preview_team_strength` без team_id и с тиром TIER3 — прод и сам ставит
    # TIER3, когда тир неизвестен). Обучаться на недостижимой в бою величине
    # нельзя: живой расчёт разойдётся с весами.
    hcol = os.getenv("HYBRID_COL")
    if hcol:
        hz = np.load(hcol)
        hmap = {int(m): i for i, m in enumerate(hz["mids"].tolist())}
        # 2.9% компактных карт в корпусе на диске уже нет — свежий обход ELO их
        # не видит. Для них берётся значение старой колонки: определения
        # различаются только тиром чтения и коррелируют 0.997, а выбрасывать
        # строки нельзя — сместится тестовая выборка и сломается сравнение с
        # базой 26 016.
        old = np.load(ART / "map_winner_hybrid_quality_forward/hybrid_features.npz",
                      allow_pickle=True)
        omap = {int(m): i for i, m in enumerate(old["mids"].tolist())}
        ovals = old["F"][:, 1]
        # массивы достаются ОДИН раз: обращение hz["value"] внутри включения
        # распаковывает npz на каждой из 482 486 итераций (проверено — прогон
        # встал на час без единой строки в логе)
        hvals = hz["value"]
        gap = 0
        buf = np.empty(len(mids_k))
        for i2, m in enumerate(mids_k.tolist()):
            j = hmap.get(int(m))
            if j is None:
                gap += 1
                buf[i2] = ovals[omap[int(m)]]
            else:
                buf[i2] = hvals[j]
        hstr = buf
        print(f"hybrid: заполнено старой колонкой {gap:,} из {len(mids_k):,} "
              f"({100*gap/len(mids_k):.2f}%)", flush=True)
        assert gap / len(mids_k) < 0.05, "слишком много пропусков в tier3-колонке"
    else:
        hz = np.load(ART / "map_winner_hybrid_quality_forward/hybrid_features.npz",
                     allow_pickle=True)
        hmap = {int(m): i for i, m in enumerate(hz["mids"].tolist())}  # ОДИН раз, не в включении
        hix = np.array([hmap[int(m)] for m in mids_k.tolist()])
        assert np.all(hz["mids"][hix] == mids_k), "hybrid не выровнен по mid"
        hstr = hz["F"][hix][:, 1]                                   # strength_diff/400
    b8 = np.load(ART / "ideas_batch8.npz")["F"]
    B8_NI = {"cp_pos_mean": 0, "cp_pos_conf": 1, "cp_lane": 2, "cp_pos_top": 3,
             "syn_pos_mean": 4}
    f5 = np.load(B5C)["F"]
    col_src = {"hybrid_strength": hstr,
               "cp_lane": b8[:, B8_NI["cp_lane"]], "syn_pos_mean": b8[:, B8_NI["syn_pos_mean"]],
               "a_hdmg_rel_pos": f5[:, B5NI["a_hdmg_rel_pos"]],
               "a_hdmg_rel_hero": f5[:, B5NI["a_hdmg_rel_hero"]],
               "a_nw_rel_pos": f5[:, B5NI["a_nw_rel_pos"]]}
    NEW = np.column_stack([col_src[c] for c in NEW_COLS])
    assert NEW.shape == (len(X23), len(NEW_COLS)), (NEW.shape, len(NEW_COLS))
    X29 = np.column_stack([X23, NEW])
    train = ts < TEST_FROM
    ctx = np.column_stack([np.abs(X20[:, 1]), np.abs(X20[:, 2])])   # |elo|, |games|
    ctx_mu, ctx_sd = ctx[train].mean(0), ctx[train].std(0) + 1e-9
    ctxz = (ctx - ctx_mu) / ctx_sd
    key_idx = [BASE20.index(k) for k in INTER_KEYS]
    INT = np.column_stack([X23[:, key_idx] * ctxz[:, j:j+1] for j in range(ctx.shape[1])])
    X = np.column_stack([X29, INT])
    assert X.shape[1] == len(FEATURES), (X.shape, len(FEATURES))

    test = ~train
    # Окна обучения. Исторически их четыре (90/180/365/730) и скорер усредняет
    # вероятности — способ не выбирать глубину истории руками. По E-191/E-192
    # ансамбль ничего не даёт: на forward-протоколе из пяти окон его среднее
    # 0.7188 против 0.7195 у одиночного окна 120 дней, а на худшем окне он ХУЖЕ
    # любого короткого (0.6888 против 0.6894-0.6902), то есть страховкой не
    # является. Список задаётся переменной окружения, чтобы менять его заменой
    # значения, а не правкой кода.
    # По умолчанию с 15.08 — ОДНО окно 120 дней (решение alex по E-192).
    # Прежнее значение "90,180,365,730" можно вернуть переменной окружения.
    windows = tuple(int(x) for x in
                    os.getenv("PREMATCH_TRAIN_WINDOWS", "120").split(","))
    print(f"окна обучения: {windows}", flush=True)
    tmax = ts[train].max(); mus, sds, coefs, ints, ps = [], [], [], [], []
    for dd in windows:
        mk = train & (ts >= tmax - dd*86400)
        if mk.sum() < 10000: continue
        mu, sd = X[mk].mean(0), X[mk].std(0)+1e-9
        m = LogisticRegression(C=1.0, max_iter=5000).fit((X[mk]-mu)/sd, wins[mk])
        mus.append(mu); sds.append(sd); coefs.append(m.coef_[0]); ints.append(m.intercept_[0])
        ps.append(m.predict_proba((X[test]-mu)/sd)[:, 1])
    print(f"\nAUC на тесте: {auc(wins[test], np.mean(ps, axis=0)):.4f}", flush=True)

    # ---- снимок: v1 + три новых поля
    z1 = np.load(ROOT/"runtime/artifacts/misc/prematch_model_artifact.npz")
    acc_old = z1["accounts"]
    lvl_map = {a: (lvl_res[a][0]/lvl_res[a][1] if lvl_res[a][1] else 0.0) for a in lvl_res}
    kda_map = {a: (float(np.mean(kda_q[a])) if kda_q[a] else 0.0) for a in kda_q}
    acc_new = np.column_stack([acc_old,
                               np.array([lvl_map.get(int(r[0]), 0.0) for r in acc_old]),
                               np.array([kda_map.get(int(r[0]), 0.0) for r in acc_old])])
    farm_map = np.array([[h, lh_hero[h][0]/lh_hero[h][1]] for h in sorted(lh_hero)
                         if lh_hero[h][1] > 0], dtype=np.float64)
    np.savez_compressed(OUT, accounts=acc_new, acc_hero=z1["acc_hero"], acc_pos=z1["acc_pos"],
                        hero_wr30=z1["hero_wr30"], vs_pairs=z1["vs_pairs"], h2h=z1["h2h"],
                        hero_farm=farm_map, ctx_mu=ctx_mu, ctx_sd=ctx_sd,
                        mu=np.array(mus), sd=np.array(sds), coef=np.array(coefs),
                        intercept=np.array(ints), snapshot_ts=z1["snapshot_ts"],
                        feature_names=np.array(FEATURES))
    SPEC.write_text(json.dumps({"features": FEATURES, "n_features": len(FEATURES),
                                "inter_keys": INTER_KEYS, "inter_ctx": INTER_CTX,
                                "accounts": len(acc_new), "heroes_farm": len(farm_map),
                                "snapshot_ts": int(z1["snapshot_ts"][0])},
                               ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"сохранено: {OUT}\nпризнаков: {len(FEATURES)}, аккаунтов: {len(acc_new):,}")


if __name__ == "__main__":
    main()
