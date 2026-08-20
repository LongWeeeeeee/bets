#!/usr/bin/env python3
"""Аудит боевого пути: считает ли прод все 35 признаков тем же, чем учился, и честен ли AUC.

Замысел. Обучающие колонки считались «на момент карты»: состояние накопителей
включало всё прошлое и ничего из будущего. Прод же в момент карты имеет СНИМОК,
собранный раньше. Поэтому единственная честная проверка боевого пути — собрать
снимок строго по данным до границы теста и прогнать через него все тестовые
карты кодом самого скорера.

Что меряется:
  1. по каждому из 35 признаков — отношение sd и корреляция с обучающей колонкой.
     Отношение sd ловит перепутанный масштаб (ошибка E-166 стоила 0.116 AUC),
     корреляция — перепутанный знак, сторону, не ту ячейку;
  2. AUC боевого пути на тестовом окне. Отдельно — на первых трёх днях: снимок
     пересобирается ночным заданием и в бою живёт около суток, поэтому именно
     это число сравнимо с боевым, а весь тест (снимок стареет на месяцы) даёт
     нижнюю границу;
  3. AUC без 177 тестовых карт, которые нашлись в паблик-корпусе драфт-модели;
  4. как часто срабатывают дефолты — «ячейки нет, беру ноль». Каждый такой ноль
     это уверенность из воздуха, и его частота должна быть видна.

Запуск: venv_catboost/bin/python3 runtime/experiments/misc/audit_live_path.py
"""
from __future__ import annotations

import math
import os
import sys
from collections import Counter
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "runtime/experiments/misc"))
sys.path.insert(0, str(ROOT / "base"))
import undercount_next as U  # noqa: E402
from ideas_batch2 import COMPACT, RICH, TEST_FROM  # noqa: E402
from ideas_batch5 import CACHE as B5C, NI as B5NI  # noqa: E402
from pro_features_wide import auc  # noqa: E402
import prematch_scorer as ps  # noqa: E402

ART = ROOT / "runtime/artifacts/misc"
DEPLOYED = ART / "prematch_model_artifact_v3_hybrid.npz"
AUDIT = ART / f"prematch_audit_artifact_cut{TEST_FROM}.npz"
OUT = ART / "audit_live_path.md"
B8_NI = {"cp_pos_mean": 0, "cp_pos_conf": 1, "cp_lane": 2, "cp_pos_top": 3, "syn_pos_mean": 4}


def assemble() -> None:
    """Артефакт с боевыми весами, но снимком строго из прошлого."""
    dep = dict(np.load(DEPLOYED, allow_pickle=True))
    cut = np.load(ART / f"prematch_model_artifact_v2_cut{TEST_FROM}.npz")
    live = np.load(ART / "live_maps_at_test.npz")
    org = np.load(ART / f"org_h2h_cut{TEST_FROM}.npz")

    z = {k: dep[k] for k in ("mu", "sd", "coef", "intercept", "ctx_mu", "ctx_sd",
                             "feature_names", "team_merge", "org_roster")}
    z["h2h_org"] = org["h2h_org"]
    z["snapshot_ts"] = cut["snapshot_ts"]
    for k in ("acc_pos", "hero_wr30", "vs_pairs", "h2h", "hero_farm"):
        z[k] = cut[k]
    hd = {int(r[0]): (float(r[1]), float(r[2])) for r in live["acc_hdmg_nw"]}
    acc = cut["accounts"]
    add = np.array([hd.get(int(r[0]), (0.0, 0.0)) for r in acc])
    # E-177: три колонки, которых снимку не хватало; без них скорер откатится на
    # старое поведение и проверка покажет починку там, где её нет
    z1 = np.load(ART / f"prematch_model_artifact_cut{TEST_FROM}.npz")
    ex = {int(r[0]): (float(r[1]), float(r[2]), float(r[3])) for r in z1["acc_extra"]}
    add2 = np.array([ex.get(int(r[0]), (0.0, 0.0, 0.0)) for r in acc])
    z["accounts"] = np.column_stack([acc, add, add2])
    hh = {(int(r[0]), int(r[1])): float(r[2]) for r in live["acc_hero_hdmg"]}
    ah = cut["acc_hero"]
    z["acc_hero"] = np.column_stack(
        [ah, np.array([hh.get((int(r[0]), int(r[1])), 0.0) for r in ah])])
    for k in ("vs_pos", "vs_flat", "syn_pos", "syn_flat"):
        z[k] = live[k]
    np.savez_compressed(AUDIT, **z)
    print(f"аудит-артефакт собран: аккаунтов {len(z['accounts']):,}, ячеек {len(z['acc_hero']):,}, "
          f"пар h2h_org {len(z['h2h_org']):,} (в боевом {len(dep['h2h_org']):,})", flush=True)


def train_columns(ts, accounts, pst, dur_min, mids, test, ctx_mu, ctx_sd, names):
    """Обучающие значения 35 колонок для тестовых карт."""
    G = np.load(U.CACHE, allow_pickle=True)["G"]              # боевые 29 колонок
    # G, `ideas_batch8` и `batch5` берутся ПОЗИЦИОННО: ни у одного из трёх нет
    # массива `mids`, и восстановить, какой карте отвечает строка, по ним нельзя.
    # А `lvl_pm` и `hstr` ниже строятся по `mids`, после чего одно кладётся в
    # другое (`X23[:, 20] = lvl_pm`). То есть корректность держится на допущении
    # «строки кэшей идут в том же порядке карт, что и `mids`».
    #
    # Допущение проверено эмпирически (19.08.2026) и подтвердилось: эти самые 35
    # колонок в одиночку дают AUC 0.6852 на 26 016 тестовых карт
    # (`kills27_max_model.md`) при базе 0.5. Цель считается по `mids`-порядку из
    # `pst`; будь порядок кэшей другим, колонки описывали бы ЧУЖИЕ карты и дали
    # бы монетку. Сверить по идентификатору по-прежнему нечем — при пересборке
    # кэша это единственная доступная проверка.
    #
    # Сторож ниже ловит дешёвый случай: если из корпуса выпала хоть одна карта,
    # `mids` короче кэша, и дальше numpy падал бы с невнятной ошибкой формы на
    # присваивании колонки 20.
    if G.shape[0] != len(mids):
        raise SystemExit(
            f"позиционный кэш {U.CACHE.name} на {G.shape[0]} строк, а карт "
            f"{len(mids)} — соответствие строк картам потеряно. Пересобрать кэш "
            f"на текущем наборе карт; молча брать по позиции нельзя.")
    lvl_pm, _ = U.dur_pass(ts, accounts, pst, dur_min)        # поминутный уровень
    hz = np.load(ART / "hybrid_strength_tier3.npz")
    hmap = {int(m): i for i, m in enumerate(hz["mids"].tolist())}
    hv = hz["value"]
    old = np.load(ART / "map_winner_hybrid_quality_forward/hybrid_features.npz", allow_pickle=True)
    omap = {int(m): i for i, m in enumerate(old["mids"].tolist())}
    ovals = old["F"][:, 1]
    hstr = np.array([hv[hmap[int(m)]] if int(m) in hmap else ovals[omap[int(m)]]
                     for m in mids.tolist()])
    b8 = np.load(ART / "ideas_batch8.npz")["F"]
    f5 = np.load(B5C)["F"]
    X23 = np.column_stack([G[:, :23]])
    X23[:, 20] = lvl_pm
    NEW = np.column_stack([hstr, b8[:, B8_NI["cp_lane"]], b8[:, B8_NI["syn_pos_mean"]],
                           f5[:, B5NI["a_hdmg_rel_pos"]], f5[:, B5NI["a_hdmg_rel_hero"]],
                           f5[:, B5NI["a_nw_rel_pos"]]])
    ctx = np.column_stack([np.abs(X23[:, 1]), np.abs(X23[:, 2])])
    ctxz = (ctx - ctx_mu) / ctx_sd
    keys = [names.index(k) for k in ("draft_logit", "elo", "form")]
    INT = np.column_stack([X23[:, keys] * ctxz[:, j:j + 1] for j in range(2)])
    X = np.column_stack([X23, NEW, INT])
    assert X.shape[1] == len(names), (X.shape, len(names))
    return X[test], hstr[test]


def main() -> None:
    if not AUDIT.exists():
        assemble()
    zc, zr = np.load(COMPACT), np.load(RICH)
    # РАБОЧИЙ НАБОР БЕРЁТСЯ ИЗ ЗАМОРОЖЕННОГО ПОРЯДКА, а не из корпуса. Всё, что
    # ниже адресуется ПОЗИЦИЕЙ — `G` внутри `train_columns`, `ideas_batch8`,
    # `batch5` и `pro_draft_logit_full` — лежит в порядке `hybrid_features` на
    # 482 486 карт и своих `mids` не несёт. Пока корпус был ровно такого же
    # размера, обход по корпусу совпадал СЛУЧАЙНО; корпус вырос до 1 260 420, и
    # совпадение кончилось. Именно поэтому последний отчёт этого аудита датирован
    # 14.08: с тех пор он падал на несовпадении форм, а его никто не перезапускал
    # — а это единственный прибор, которым проверяется порог автоматической
    # ставки `_PREMATCH_MIN_INDEX`. Порядок восстанавливаем так же, как в
    # `kills27_max_model.build_all()`.
    hz = np.load(ART / "map_winner_hybrid_quality_forward/hybrid_features.npz",
                 allow_pickle=True)
    frozen = hz["mids"].astype(np.int64)
    cpos = {int(x): i for i, x in enumerate(zc["mids"].tolist())}
    rpos = {int(x): i for i, x in enumerate(zr["mids"].tolist())}
    have = np.array([int(m) in cpos and int(m) in rpos for m in frozen.tolist()])
    if not have.all():
        raise SystemExit(
            f"из замороженного набора {int((~have).sum())} карт нет в корпусе — "
            "позиционные кэши перестали отвечать строкам, пересобрать их")
    mids = frozen
    ci = np.array([cpos[int(m)] for m in mids.tolist()])
    idx_r = np.array([rpos[int(m)] for m in mids.tolist()])
    ts, wins = zc["ts"][ci], zc["wins"][ci].astype(int)
    heroes, accounts = zc["heroes"][ci], zc["accounts"][ci]
    pst = zr["pstats"][idx_r]
    dur_min = zr["durations"][idx_r].astype(float) / 60.0
    test = ts >= TEST_FROM
    lgt = np.load(ART / "pro_draft_logit_full.npz")["logit"]

    m = ps.PrematchModel(AUDIT)
    names = list(m.features)
    print(f"признаков в артефакте: {len(names)}; тестовых карт: {int(test.sum()):,}", flush=True)

    Xtr, hstr_all = train_columns(ts, accounts, pst, dur_min, mids,
                                  test, m.ctx_mu, m.ctx_sd, names)
    idx = np.flatnonzero(test)
    hstr_test = hstr_all
    ys, ps_live, rows_live, ok_idx = [], [], [], []
    refused = Counter()
    zero_cell = Counter()
    for k, i in enumerate(idx):
        h = [int(x) for x in heroes[i]]
        a = [int(x) for x in accounts[i]]
        try:
            r = m.score(radiant_accounts=a[:5], dire_accounts=a[5:],
                        radiant_heroes=h[:5], dire_heroes=h[5:],
                        radiant_team_id=0, dire_team_id=0,
                        draft_logit=float(lgt[i]),
                        hybrid_strength=float(hstr_test[k]),
                        strictness="accounts", now_ts=int(ts[i]),
                        # гейт свежести отключён НАМЕРЕННО: качество по возрасту
                        # снимка меряется ниже отдельной таблицей
                        max_age_days=0.0)
        except ps.MissingData as e:
            refused[str(e).split(":")[0][:45]] += 1
            continue
        ys.append(int(wins[i]))
        ps_live.append(r.probability)
        rows_live.append([r.features[n] for n in names])
        ok_idx.append(k)
        for note in r.notes:
            zero_cell[note.split("—")[0].strip()[:45]] += 1
        if (k + 1) % 5000 == 0:
            print(f"  {k+1:,}/{len(idx):,}", flush=True)

    L = np.array(rows_live)
    T = Xtr[np.array(ok_idx)]
    y = np.array(ys)
    p = np.array(ps_live)
    print(f"\nпосчитано {len(y):,}, отказов {sum(refused.values()):,}", flush=True)

    lines = ["# Аудит боевого пути предматчевой модели", "",
             f"Снимок собран строго по картам до {TEST_FROM}; тестовые карты через него "
             f"прогнаны кодом скорера. Посчитано {len(y):,} из {int(test.sum()):,}, "
             f"отказов {sum(refused.values()):,}.", "",
             "## 1. Совпадает ли боевой расчёт с обучающим по каждому признаку", "",
             "| признак | sd в бою | sd в обучении | отношение | корреляция |", "|---|---|---|---|---|"]
    bad = []
    for j, nm in enumerate(names):
        lv, tv = L[:, j], T[:, j]
        sl, st = float(lv.std()), float(tv.std())
        ratio = sl / st if st > 1e-12 else float("nan")
        corr = float(np.corrcoef(lv, tv)[0, 1]) if sl > 1e-12 and st > 1e-12 else float("nan")
        lines.append(f"| {nm} | {sl:.5f} | {st:.5f} | {ratio:.3f} | {corr:.4f} |")
        if not (0.5 < ratio < 2.0) or (corr == corr and corr < 0.5):
            bad.append((nm, ratio, corr))
    print("подозрительные колонки:", bad or "нет", flush=True)

    # AUC
    a_all = auc(y, p)
    tt = ts[idx][np.array(ok_idx)]
    age = (tt - int(m.snapshot_ts)) / 86400.0
    lines += ["", "## 2. AUC боевого пути по возрасту снимка", "",
              "Гейт свежести в проде ВКЛЮЧЁН: `win_model_veto.py:429` передаёт и "
              "`now_ts`, и `max_age_days`. Но выставлен он на "
              "`_SNAPSHOT_MAX_AGE_DAYS = 30` суток (сверено на serv1 20.08.2026); "
              "прежний текст здесь утверждал три суток и что гейт отключён — неверно "
              "вдвойне. Боевому качеству отвечает первая строка: снимок пересобирается "
              "ночным заданием и живёт около суток. Остальные строки показывают, чем "
              "платит устаревание, и заодно что порог в 30 суток вдесятеро слабее "
              "измеренного обрыва — если пересборка встанет, деградацию он не "
              "остановит.", "",
              "| возраст снимка | карт | AUC |", "|---|---|---|"]
    for lo, hi, tag in ((0, 3, "до 3 суток (боевая свежесть)"), (3, 7, "3-7 суток"),
                        (7, 14, "7-14 суток"), (14, 30, "14-30 суток"),
                        (30, 1e9, "больше 30 суток")):
        mk = (age >= lo) & (age < hi)
        if mk.sum() < 100:
            continue
        lines.append(f"| {tag} | {int(mk.sum()):,} | {auc(y[mk], p[mk]):.4f} |")
    lines.append(f"| всё окно теста | {len(y):,} | {a_all:.4f} |")
    fresh = age < 3.0
    a_fresh = auc(y[fresh], p[fresh]) if fresh.sum() > 100 else float("nan")

    # --- диагностика h2h_resid: он единственный разошёлся с обучением
    j_h = names.index("h2h_resid")
    lz, tz = L[:, j_h], T[:, j_h]
    lines += ["", "### Почему разошёлся h2h_resid", "",
              f"- нулей в бою: {int((lz == 0).sum()):,} из {len(lz):,} "
              f"({100.0*(lz == 0).mean():.1f}%)",
              f"- нулей в обучении: {int((tz == 0).sum()):,} ({100.0*(tz == 0).mean():.1f}%)",
              f"- на картах, где в бою НЕ ноль: корреляция "
              f"{float(np.corrcoef(lz[lz != 0], tz[lz != 0])[0, 1]):.4f}, "
              f"отношение sd {float(lz[lz != 0].std()/max(tz[lz != 0].std(), 1e-9)):.3f}"]
    # --- утечка: 177 тестовых карт лежат в паблик-корпусе драфт-модели
    leak_path = ART / "leaked_mids_in_public.npy"
    if leak_path.exists():
        leaked = set(int(x) for x in np.load(leak_path).tolist())
        mt = mids[idx][np.array(ok_idx)]
        lm = np.array([int(x) in leaked for x in mt.tolist()])
        if lm.sum():
            a_clean = auc(y[~lm], p[~lm])
            lines.append(f"| без {int(lm.sum())} карт, найденных в паблик-корпусе "
                         f"| {int((~lm).sum()):,} | {a_clean:.4f} |")

    # --- честность заявленного винрейта: таблица снята с офлайн-турниров,
    #     а применяется ко всем матчам подряд
    import json as _json
    meta = {int(k): v for k, v in _json.load(
        open(ROOT / "runtime/artifacts/misc/league_meta.json")).items()}
    leagues = zc["leagues"][ci][idx][np.array(ok_idx)]
    is_lan = np.array([1 if meta.get(int(l), {}).get("lan") else 0 for l in leagues])
    conf = np.maximum(p, 1.0 - p)
    picked = np.where(p > 0.5, 1, 0)
    hit = (picked == y).astype(int)
    # Сырые массивы боевого пути наружу: на них строится разделение калибровки
    # по площадке (`venue_calibration_grid.py`). Скоринг стоит несколько минут,
    # и гонять его заново ради каждой перенарезки полос незачем.
    np.savez_compressed(ART / "audit_live_path_raw.npz",
                        conf=conf, hit=hit, is_lan=is_lan, y=y, p=p)
    lines += ["", "## 4. Заявленный винрейт против фактического", "",
              "Таблица `LAN_ODDS_GRID` измерена на настоящих офлайн-турнирах, но "
              "`model_bet` отдаёт `expected_wr` для ЛЮБОГО матча — проверки на офлайн в коде нет.",
              "", "| выборка | карт | заявлено моделью | фактический винрейт |", "|---|---|---|---|"]
    for tag, msk in (("все матчи", np.ones(len(y), bool)),
                     ("офлайн (LAN)", is_lan > 0),
                     ("онлайн", is_lan == 0)):
        for lo in (0.58, 0.65, 0.75):
            m2 = msk & (conf >= lo)
            if m2.sum() < 50:
                continue
            claimed = float(np.mean([ps.lan_expected_wr(c) for c in conf[m2]]))
            actual = float(hit[m2].mean())
            lines.append(f"| {tag}, уверенность ≥ {lo:.0%} | {int(m2.sum()):,} | "
                         f"{claimed:.1%} | {actual:.1%} |")

    lines += ["", "## 5. Отказы и дефолты", ""]
    for k2, v in refused.most_common(6):
        lines.append(f"- отказ «{k2}»: {v:,}")
    for k2, v in zero_cell.most_common(6):
        lines.append(f"- замечание «{k2}»: {v:,}")
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"AUC весь тест {a_all:.4f}; первые 3 дня {a_fresh:.4f} ({int(fresh.sum()):,} карт)")
    print("отчёт:", OUT, flush=True)


if __name__ == "__main__":
    main()
