#!/usr/bin/env python3
"""Собирает боевой артефакт: веса + состояние под новые колонки + опознание организаций.

Три источника, и ни один не пересчитывается зря:
  1. `prematch_model_artifact_v2*.npz` — веса, нормировки и снимок аккаунтов
     (его пересобирает `build_prematch_artifact_v2.py`);
  2. `live_maps_full.npz` — состояние, из которого скорер считает пять новых
     колонок E-168 (`add_live_maps.py`);
  3. опознание организаций (`team_merge`, `org_roster`, `h2h_org`) — берётся из
     уже собранного v3. Оно считается только по составам и истории встреч из
     COMPACT и от набора признаков не зависит вовсе, поэтому гонять
     `add_org_identity.py` заново незачем.

Запуск: venv_catboost/bin/python3 runtime/experiments/misc/finalize_artifact.py
"""
from __future__ import annotations

import os
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
ART = ROOT / "runtime/artifacts/misc"
SRC = Path(os.getenv("PREMATCH_SRC", ART / "prematch_model_artifact_v2_nohybrid.npz"))
LIVE = ART / "live_maps_full.npz"
ORG = ART / "prematch_model_artifact_v3.npz"
# ВАЖНО: имя по умолчанию — то, которое проверяет и доставляет
# `scripts/run/rebuild_prematch_snapshot.sh`. До 15.08 здесь стоял
# `_v3_nohybrid`, а раннер отправлял `_v3_hybrid`, которого никто не писал: прод
# получал один и тот же файл от 14.08 (снимок 11.08, 354 тыс. аккаунтов вместо
# 1.55 млн), хотя пересборка отрабатывала каждую ночь без ошибок.
OUT = Path(os.getenv("PREMATCH_OUT", ART / "prematch_model_artifact_v3_hybrid.npz"))
ORG_KEYS = ("team_merge", "org_roster", "h2h_org")
LIVE_KEYS = ("vs_pos", "vs_flat", "syn_pos", "syn_flat")


# Источник весов — ОТДЕЛЬНЫЙ файл, который цепочка никогда не перезаписывает.
# Раньше здесь стоял `_v3_hybrid`, то есть выход шага 5; после починки OUT это
# был бы один и тот же файл, и веса читались бы из собственного результата.
WEIGHTS = Path(os.getenv("PREMATCH_WEIGHTS", ART / "prematch_weights_win120.npz"))
W_KEYS = ("mu", "sd", "coef", "intercept", "ctx_mu", "ctx_sd", "feature_names")
# Веса веток лестницы — тоже отдельным файлом и тоже необязательные:
# `train_branch_weights.py` собирает их независимо от цепочки снимка.
BRANCHES = Path(os.getenv("PREMATCH_BRANCHES", ART / "branch_weights.npz"))
CALIB = Path(os.getenv("PREMATCH_BRANCH_CALIB", ART / "branch_calibration.npz"))
# Рейтинг организаций — вход коротких веток. Ключ `org_rating`, 131 259 строк,
# около 3 МБ: рядом с таблицей на 1.55 млн аккаунтов это ничто.
ORGRAT = Path(os.getenv("PREMATCH_ORG_RATING", ART / "org_rating_snapshot.npz"))


def main() -> None:
    z = dict(np.load(SRC, allow_pickle=True))
    if "coef" not in z:
        # снимок собран в режиме PREMATCH_SNAPSHOT_ONLY: веса берём из
        # предыдущего боевого артефакта, они переобучаются отдельно
        zw = np.load(WEIGHTS, allow_pickle=True)
        for k in W_KEYS:
            z[k] = zw[k]
        print(f"веса взяты из {WEIGHTS.name}: признаков {len(z['feature_names'])}")
    zl = np.load(LIVE)
    zo = np.load(ORG, allow_pickle=True)
    for k in LIVE_KEYS:
        z[k] = zl[k]
    # Отклонения урона и нетворса ПРИСТРАИВАЮТСЯ к уже существующим таблицам, а
    # не кладутся отдельными словарями. Причина операционная: `acc_hero` — это
    # 1.7 млн строк, и второй словарь того же размера стоил в замере +424 МБ
    # RSS при боевых 949 МБ. Лишняя колонка в numpy-массиве стоит 14 МБ.
    hd = {int(r[0]): (float(r[1]), float(r[2])) for r in zl["acc_hdmg_nw"]}
    acc = z["accounts"]
    add = np.array([hd.get(int(r[0]), (0.0, 0.0)) for r in acc], dtype=np.float64)
    # E-177: три недостающие величины снимка приписываются В КОНЕЦ, чтобы не
    # сдвинуть индексы уже существующих колонок, на которые смотрит скорер
    src_v1 = ART / "prematch_model_artifact.npz"
    z1 = np.load(src_v1)
    if "acc_extra" in z1:
        ex = {int(r[0]): (float(r[1]), float(r[2]), float(r[3])) for r in z1["acc_extra"]}
        add2 = np.array([ex.get(int(r[0]), (0.0, 0.0, 0.0)) for r in acc], dtype=np.float64)
        z["accounts"] = np.column_stack([acc, add, add2])
        print(f"acc_extra подмешан: {len(ex):,} аккаунтов")
    else:
        z["accounts"] = np.column_stack([acc, add])
        print("ВНИМАНИЕ: в v1-артефакте нет acc_extra — колонок будет меньше")
    hh = {(int(r[0]), int(r[1])): float(r[2]) for r in zl["acc_hero_hdmg"]}
    ah = z["acc_hero"]
    add_h = np.array([hh.get((int(r[0]), int(r[1])), 0.0) for r in ah], dtype=np.float64)
    z["acc_hero"] = np.column_stack([ah, add_h])
    print(f"accounts: {acc.shape} -> {z['accounts'].shape}; "
          f"acc_hero: {ah.shape} -> {z['acc_hero'].shape}")
    for k in ORG_KEYS:
        if k not in zo:
            raise SystemExit(f"в {ORG.name} нет поля {k} — опознание организаций брать неоткуда")
        z[k] = zo[k]
    names = [str(x) for x in z["feature_names"]]
    if len(names) != len(z["coef"][0]):
        raise SystemExit(f"признаков {len(names)}, а весов {len(z['coef'][0])}")
    # Веса веток лестницы. Файла нет — артефакт собирается как раньше, и скорер
    # ведёт себя по-старому: одна модель на все колонки, отказ при неполном
    # входе. Так работает боевая машина, пока ей не доставлен новый артефакт.
    if BRANCHES.exists():
        zb = np.load(BRANCHES, allow_pickle=True)
        bn = [str(x) for x in zb["feature_names"]]
        if bn != names:
            raise SystemExit(
                f"порядок признаков в {BRANCHES.name} разошёлся с артефактом: "
                f"первое расхождение на слоте "
                f"{next(i for i in range(min(len(bn), len(names))) if bn[i] != names[i])}"
                if len(bn) == len(names) else
                f"в {BRANCHES.name} признаков {len(bn)}, а в артефакте {len(names)}")
        # Полная ветка обязана быть побитовой копией боевых весов. Если веса
        # переобучили, а ветки нет, `full` начнёт отдавать не то же число, что
        # прежняя модель, — и лестница молча поедет по уже существующим картам.
        i_full = next((i for i, x in enumerate(zb["branch_names"])
                       if str(x) == "full"), None)
        if i_full is not None:
            off = int(zb["branch_lens"][:i_full].sum())
            n_full = int(zb["branch_lens"][i_full])
            got = zb["branch_coef"][off:off + n_full]
            if got.shape != z["coef"][0].shape or not np.allclose(got, z["coef"][0]):
                raise SystemExit(
                    "веса полной ветки разошлись с боевыми: пересобрать "
                    f"{BRANCHES.name} через train_branch_weights.py")
        for k in zb.files:
            if k.startswith("branch_"):
                z[k] = zb[k]
        print(f"ветки лестницы: {[str(x) for x in zb['branch_names']]}")
        need_org = any(c in ("org_rating_diff", "org_rating_exp")
                       for c in (str(x) for x in zb["branch_cols"]))
        if need_org:
            if not ORGRAT.exists():
                raise SystemExit(
                    f"ветки просят рейтинг организаций, а {ORGRAT.name} нет — "
                    "сначала build_org_rating.py")
            z["org_rating"] = np.load(ORGRAT)["org_rating"]
            print(f"рейтинг организаций: {len(z['org_rating']):,} строк")
        if CALIB.exists():
            zk = np.load(CALIB, allow_pickle=True)
            for k in zk.files:
                if k.startswith("cal_"):
                    z[k] = zk[k]
            print(f"калибровка веток: {len(zk['cal_branch'])} полос")
        else:
            print(f"{CALIB.name} нет — короткие ветки без своей таблицы винрейта")
    else:
        print(f"{BRANCHES.name} нет — артефакт без веток, скорер по-старому")
    tmp = OUT.with_suffix(".tmp.npz")
    np.savez_compressed(tmp, **z)
    tmp.replace(OUT)
    print(f"признаков: {len(names)}")
    print("новые колонки:", [n for n in names if n in
                             ("hybrid_strength", "cp_lane", "syn_pos_mean",
                              "a_hdmg_rel_pos", "a_hdmg_rel_hero", "a_nw_rel_pos")])
    print(f"аккаунтов: {len(z['accounts']):,}; ячеек (аккаунт, герой) {len(z['acc_hero']):,}; "
          f"ячеек контрпика {len(z['vs_pos']):,}; организаций {len(z['org_roster']):,}")
    print(f"сохранено: {OUT} ({OUT.stat().st_size/1048576:.0f} МБ)")


if __name__ == "__main__":
    main()
