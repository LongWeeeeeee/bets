#!/usr/bin/env python3
"""Признаки из каталога `dota2_pregame_merged_dedup.xlsx` — восемь семейств.

Каталог (3670 строк, 2649 базовых величин, авторы: база 747 + grok/kimi/gemini)
писался под модель килов. Строк там больше, чем можно построить: 592 требуют
драфт-лога (порядка пиков и банов) — его в корпусе нет; 173 — это выходы
отдельных OOF-моделей, а не извлекатель; 302 — телеметрия реплея (пикоффы,
роши, руны, варды, иллюзии), которой в записи матча нет.

Здесь строится ТО, ЧТО СТРОИТСЯ, и ровно в тех формах, которых в действующем
входе E-184/E-185 НЕТ. Действующий блок `sym` даёт по каждому полю карточки
ровно одну величину — СУММУ по пятёрке. Каталог же почти целиком про другие
формы: дыры (`*_hole`), счётчики носителей, максимум, разброс, позиционную
привязку и перекрёстные произведения «наш инструмент против их контр-инструмента».
Их сумма выразить не может, и дерево не выведет их из сумм.

Семейства (все выдаются парой diff/sum, как требует регистр каталога):
  F1 holes    дыры и счётчики носителей по таксономии контроля (кат. 20);
  F2 avail    когда контроль доступен: минимальный уровень и доля «нужен предмет»;
  F3 shape    форма распределения величины по пятёрке: максимум и разброс;
  F4 pos      значение карточки НА ПОЗИЦИИ (сейчас позиции есть только у baseline);
  F5 cross    «наш X против их Y» — произведения, недоступные разности сумм;
  F6 hprior   исторические приоры ГЕРОЯ на сами цели: килы команды в окнах
              0-10/10-20/20-30/30-40, доля карт с 27+, длительность, перевес
              по нетворту и опыту на 10/20/30-й минуте (кат. 25);
  F7 pprior   то же для ИГРОКА;
  F8 pair     парная синергия по килам: остаток пары над суммой одиночных.

ПРИЧИННОСТЬ — главное место, на котором первая версия сгорела. Приоры F6-F8
считаются СТРОГО ПО ПРОШЛОМУ: для каждой карты берётся состояние счётчиков до
неё, сама карта в свой приор не входит. Снимок «всё до границы теста», который
стоял в первой версии, давал винрейту игрока AUC 0.7296 на обучении против
0.5884 на тесте — обучающая карта попадала в историю собственных игроков, модель
на это опиралась и на тесте теряла 0.041 AUC. Это тот же класс ошибки, что
измерен у драфт-логита в E-177.

Итог карты по килам берётся из `pstats` поимённо: накопленный ряд `rk/dk`
обрывается на 40-й минуте (E-186 §4), суммы по нему занижены.

Запуск: nohup venv_catboost/bin/python3 scripts/pro_chain/catalog_features.py ...
Выход:  runtime/artifacts/misc/catalog_features.npz + .md
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT") or Path(__file__).resolve().parents[2])
sys.path.insert(0, str(ROOT / "base"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from pregame_features import PregameFeatures  # noqa: E402

ART = ROOT / "runtime/artifacts/misc"
COMPACT, RICH = ART / "pro_corpus_compact.npz", ART / "pro_corpus_rich.npz"
HYBRID = ART / "map_winner_hybrid_quality_forward/hybrid_features.npz"
CARD = ROOT / "base/hero_features_v7.json"
OUT_NPZ, OUT_MD = ART / "catalog_features.npz", ART / "catalog_features.md"
TEST_FROM = 1774742400
K_HERO, K_PLAYER, K_PAIR = 200.0, 60.0, 120.0     # шринкедж приоров
EPS = 1e-6

# ---------------------------------------------------------------- поля карточки
# F1: таксономия контроля и инструментов — «дыра» и «сколько носителей»
COUNT_FIELDS = [
    "stun_count", "root_count", "silence_count", "slow_count", "sleep_count",
    "taunt_count", "fear_count", "leash_count", "disarm_count", "break_count",
    "banish_count", "forced_movement_count", "hex_count", "dispel_count",
    "save_count", "undispellable_count", "channeling_ult_count",
    "bkb_pierce_ability_count", "global_ability_count", "ult_count",
]
# F2: доступность инструмента (минимальный уровень / нужен ли предмет)
AVAIL_CC = ["stun", "root", "silence", "slow", "hex", "sleep", "taunt", "fear",
            "leash", "disarm", "break", "banish", "forced_movement"]
# F3: форма распределения по пятёрке
SHAPE_FIELDS = [
    "burst_score", "control_score", "save_score", "defense_score",
    "initiation_score", "teamfight_score", "laning_score", "late_score",
    "tempo_score", "push_score", "farm_score", "macro_score", "resilience_score",
    "mobility_score", "magic_damage_score", "physical_damage_score", "dot_score",
    "anti_bkb_score", "fight_cooldown_score", "hg_defence", "healing_per_min",
    "complexity", "simplicity_score", "hard_carry", "hard_initiation",
    "soft_initiation", "pusher_early", "pusher_late", "max_health", "max_mana",
    "armor", "magic_resistance", "movement_speed", "attack_range", "is_melee",
    "magic_pct", "phys_pct", "pure_pct", "burst_window_s", "ult_cd_lvl3",
    "max_cast_range", "max_ability_reach", "turn_rate", "str_gain", "agi_gain",
]
# F4: то же, но привязанное к позиции
POS_FIELDS = ["burst_score", "control_score", "save_score", "mobility_score",
              "teamfight_score", "laning_score", "late_score", "tempo_score",
              "farm_score", "push_score", "max_health", "attack_range"]
# F5: перекрёстные произведения «наш X против их Y».
# (имя, поле нашей стороны, поле их стороны, форма) — формы: prod, gap
CROSS = [
    ("burst_vs_saves", "burst_score", "save_score", "gap"),
    ("burst_vs_ehp", "burst_score", "max_health", "gap"),
    ("lockdown_vs_dispel", "control_score", "dispel_enemy_strength", "gap"),
    ("hex_vs_dispel", "hex_count", "dispel_enemy_strength", "gap"),
    ("stun_vs_dispel", "stun_count", "dispel_enemy_strength", "gap"),
    ("silence_vs_channel", "silence_count", "channeling_spell_count", "prod"),
    ("interrupt_vs_channel_ult", "stun_count", "channeling_ult_count", "prod"),
    ("magic_vs_magicres", "magic_damage_score", "magic_resistance", "gap"),
    ("magic_vs_res_eff", "magic_damage_score", "magic_res_effective_max", "gap"),
    ("phys_vs_armor", "physical_damage_score", "armor", "gap"),
    ("pure_vs_ehp", "pure_pct", "max_health", "prod"),
    ("bkbpierce_vs_bkb", "bkb_pierce_ability_count", "item_share_black_king_bar", "prod"),
    ("control_vs_bkb", "control_score", "item_share_black_king_bar", "gap"),
    ("antibkb_vs_bkb", "anti_bkb_score", "item_share_black_king_bar", "prod"),
    ("break_vs_passive", "break_count", "resilience_score", "prod"),
    ("mobility_vs_control", "mobility_score", "control_score", "gap"),
    ("gapclose_vs_kiting", "mobility_score", "attack_range", "gap"),
    ("melee_vs_range", "is_melee", "attack_range", "prod"),
    ("range_vs_melee", "attack_range", "is_melee", "prod"),
    ("push_vs_hgdef", "push_score", "hg_defence", "gap"),
    ("siege_vs_defense", "push_score", "defense_score", "gap"),
    ("burst_vs_heal", "burst_score", "healing_per_min", "gap"),
    ("dot_vs_dispel", "dot_score", "dispel_enemy_strength", "gap"),
    ("undispellable_vs_dispel", "undispellable_count", "dispel_enemy_strength", "gap"),
    ("strongdispel_vs_undisp", "dispel_enemy_strength", "undispellable_count", "gap"),
    ("save_vs_pickoff", "save_score", "burst_score", "gap"),
    ("init_vs_save", "initiation_score", "save_score", "gap"),
    ("teamfight_vs_teamfight", "teamfight_score", "teamfight_score", "gap"),
    ("laning_vs_laning", "laning_score", "laning_score", "gap"),
    ("late_vs_late", "late_score", "late_score", "gap"),
    ("tempo_vs_defense", "tempo_score", "defense_score", "gap"),
    ("tempo_vs_late", "tempo_score", "late_score", "gap"),
    ("early_push_vs_hg", "pusher_early", "hg_defence", "gap"),
    ("hardinit_vs_mobility", "hard_initiation", "mobility_score", "gap"),
    ("forced_vs_hgdef", "forced_movement_count", "hg_defence", "prod"),
    ("taunt_vs_range", "taunt_count", "attack_range", "prod"),
    ("mana_vs_manapool", "silence_count", "max_mana", "gap"),
    ("orchid_vs_escape", "item_share_orchid", "role_escape", "prod"),
    ("sheep_vs_carry", "item_share_sheepstick", "role_carry", "prod"),
    ("pipe_vs_magic", "item_share_pipe", "magic_damage_score", "gap"),
    ("crimson_vs_phys", "item_share_crimson_guard", "physical_damage_score", "gap"),
    ("lotus_vs_targeted", "item_share_lotus_orb", "control_score", "gap"),
    ("halberd_vs_rightclick", "item_share_heavens_halberd", "physical_damage_score", "gap"),
    ("shiva_vs_phys", "item_share_shivas_guard", "physical_damage_score", "gap"),
    ("sphere_vs_single", "item_share_sphere", "burst_score", "gap"),
    ("nullifier_vs_saves", "item_share_nullifier", "save_score", "prod"),
    ("silveredge_vs_passive", "item_share_silver_edge", "resilience_score", "prod"),
    ("mkb_vs_evasion", "item_share_monkey_king_bar", "item_share_butterfly", "prod"),
    ("basher_vs_bkb", "item_share_basher", "item_share_black_king_bar", "gap"),
    ("farm_vs_tempo", "farm_score", "tempo_score", "gap"),
]

PRIOR_NAMES = [
    "own_kills", "enemy_kills", "kill_diff", "tot_kills", "p_own27", "p_tot54",
    "dur", "p_dur32", "p_dur36", "p_dur40",
    "k_0_10", "k_10_20", "k_20_30", "k_30_40",
    "ek_0_10", "ek_10_20", "ek_20_30", "ek_30_40",
    "win_0_10", "win_10_20", "win_20_30",
    "nwd_10", "nwd_20", "nwd_30", "xpd_10", "xpd_20", "winrate",
]
# что из приоров игрока подаётся в модель (полный список — 27 колонок на сторону,
# и это уже перебор для 5 слотов; берём величины, прямо связанные с целями)
P_KEEP = ("own_kills", "kill_diff", "p_own27", "dur", "p_dur36", "k_0_10",
          "k_10_20", "k_20_30", "win_10_20", "nwd_10", "winrate", "tot_kills")


def _sha1(p: Path) -> str:
    return hashlib.sha1(p.read_bytes()).hexdigest()[:12]


def card_matrix(pf: PregameFeatures) -> tuple[np.ndarray, dict[str, int], int]:
    """Матрица (hero_id -> вектор числовых полей карточки) и индекс имён."""
    ids = sorted(pf.heroes)
    hmax = max(ids) + 1
    fields = list(pf.hero_numeric_fields) + [f"{f}_count" for f in pf.hero_list_fields]
    C = np.zeros((hmax, len(fields)), dtype=np.float64)
    for h in ids:
        row = pf.heroes[h]
        C[h] = [float(row.get(f) or 0.0) for f in pf.hero_numeric_fields] + \
               [float(len(row.get(f) or [])) for f in pf.hero_list_fields]
    return C, {f: i for i, f in enumerate(fields)}, hmax


def prior_values(zr) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Величины карты с точки зрения РАДИАНТА и ДАЙРА плюс маски валидности."""
    ps, dur = zr["pstats"], zr["durations"].astype(np.float32) / 60.0
    rk, dk = zr["rk"].astype(np.float32), zr["dk"].astype(np.float32)
    nw, xp = zr["nw"].astype(np.float32), zr["xp"].astype(np.float32)
    wins = zr["wins"].astype(np.float32)
    rkills = ps[:, :5, 0].sum(1).astype(np.float32)
    dkills = ps[:, 5:, 0].sum(1).astype(np.float32)
    tot = rkills + dkills
    has_ps = tot > 0
    has_ser = (rk.max(1) + dk.max(1)) > 0
    n = len(dur)
    alw = np.ones(n, bool)

    def w(a, b, arr):
        return arr[:, min(b, 40)] - arr[:, min(a, 40)]

    cols_r, cols_d, masks = [], [], []

    def push(vr, vd, m):
        cols_r.append(np.nan_to_num(np.asarray(vr, dtype=np.float32)))
        cols_d.append(np.nan_to_num(np.asarray(vd, dtype=np.float32)))
        masks.append(m)

    push(rkills, dkills, has_ps)
    push(dkills, rkills, has_ps)
    push(rkills - dkills, dkills - rkills, has_ps)
    push(tot, tot, has_ps)
    push((rkills >= 27).astype(np.float32), (dkills >= 27).astype(np.float32), has_ps)
    push((tot >= 54).astype(np.float32), (tot >= 54).astype(np.float32), has_ps)
    push(dur, dur, alw)
    for t in (32, 36, 40):
        push((dur >= t).astype(np.float32), (dur >= t).astype(np.float32), alw)
    for a, b in ((0, 10), (10, 20), (20, 30), (30, 40)):
        push(w(a, b, rk), w(a, b, dk), has_ser & (dur >= b))
    for a, b in ((0, 10), (10, 20), (20, 30), (30, 40)):
        push(w(a, b, dk), w(a, b, rk), has_ser & (dur >= b))
    for a, b in ((0, 10), (10, 20), (20, 30)):
        d_ = w(a, b, rk) - w(a, b, dk)
        push((d_ > 0).astype(np.float32), (d_ < 0).astype(np.float32),
             has_ser & (dur >= b) & (d_ != 0))
    for mnt in (10, 20, 30):
        push(nw[:, mnt], -nw[:, mnt], has_ser & (dur >= mnt))
    for mnt in (10, 20):
        push(xp[:, mnt], -xp[:, mnt], has_ser & (dur >= mnt))
    push(wins, 1.0 - wins, alw)
    return (np.column_stack(cols_r), np.column_stack(cols_d),
            np.column_stack(masks))


# End-time availability is mandatory; no start-order cumulative approximation.
from end_time_prior import EndTimePrior as CausalPrior


def slot_priors(cp: CausalPrior, Vr, Vd, M, nm: int, k: float,
                take: np.ndarray) -> np.ndarray:
    """Приоры на каждый слот выровненных карт: (карт, 10, величин)."""
    q = Vr.shape[1]
    out = np.empty((len(take) // 10, 10, q), dtype=np.float32)
    v_row = np.empty(nm * 10, dtype=np.float32)
    m_row = np.empty(nm * 10, dtype=np.float32)
    vv, mm = v_row.reshape(nm, 10), m_row.reshape(nm, 10)
    for j in range(q):
        vv[:, :5] = Vr[:, j][:, None]
        vv[:, 5:] = Vd[:, j][:, None]
        mm[:] = M[:, j][:, None]
        # Fixed neutral reference, independent of future labels.
        g = 36.0 if PRIOR_NAMES[j] == "dur" else 0.0
        out[:, :, j] = cp.compute(v_row, m_row, k, g)[take].reshape(-1, 10)
    return out


def side_block(her5: np.ndarray, C: np.ndarray, fidx: dict[str, int],
               HPS: np.ndarray, PPS: np.ndarray, PAIR: np.ndarray,
               names_out: list[str] | None) -> np.ndarray:
    """Все семейства для ОДНОЙ стороны. Имена пишутся при первом вызове."""
    out: list[np.ndarray] = []

    def add(v: np.ndarray, nm: str) -> None:
        out.append(np.asarray(v, dtype=np.float32))
        if names_out is not None:
            names_out.append(nm)

    def col(f: str) -> np.ndarray:
        return C[:, fidx[f]][her5]

    # ---- F1: дыры и носители
    for f in COUNT_FIELDS:
        if f not in fidx:
            continue
        V = col(f)
        s = V.sum(1)
        add((V > 0).sum(1), f"F1_{f}_carriers")
        add((s <= 0).astype(np.float32), f"F1_{f}_hole")
        add(V.max(1), f"F1_{f}_max")
        add(V.max(1) / (s + EPS), f"F1_{f}_conc")
    # ---- F2: доступность контроля
    for cc in AVAIL_CC:
        lv, ni = f"{cc}_enemy_min_level", f"{cc}_enemy_needs_item"
        if lv not in fidx:
            continue
        L = col(lv)
        cnt = col(f"{cc}_count") if f"{cc}_count" in fidx else None
        if cnt is not None:
            L = np.where(cnt > 0, L, 99.0)
        add(L.min(1), f"F2_{cc}_earliest")
        if ni in fidx:
            add(col(ni).sum(1), f"F2_{cc}_needs_item")
    # ---- F3: форма распределения
    for f in SHAPE_FIELDS:
        if f not in fidx:
            continue
        V = col(f)
        add(V.max(1), f"F3_{f}_max")
        add(V.std(1), f"F3_{f}_std")
    # ---- F4: значение на позиции
    for f in POS_FIELDS:
        if f not in fidx:
            continue
        V = col(f)
        for p in range(5):
            add(V[:, p], f"F4_{f}_pos{p+1}")
    # ---- F6: приоры героя
    for j, nm in enumerate(PRIOR_NAMES):
        V = HPS[:, :, j]
        add(V.mean(1), f"F6_h_{nm}_mean")
        if nm in ("own_kills", "k_10_20", "dur", "p_own27", "win_10_20", "nwd_10"):
            add(V.std(1), f"F6_h_{nm}_std")
            add(V.max(1), f"F6_h_{nm}_max")
    # ---- F7: приоры игрока
    for j, nm in enumerate(PRIOR_NAMES):
        if nm not in P_KEEP:
            continue
        V = PPS[:, :, j]
        add(V.mean(1), f"F7_p_{nm}_mean")
        if nm in ("own_kills", "k_10_20", "win_10_20"):
            add(V.max(1), f"F7_p_{nm}_max")
            add(V.min(1), f"F7_p_{nm}_min")
    # ---- F8: парная синергия по килам
    for w_ in range(PAIR.shape[2]):
        add(PAIR[:, :, w_].mean(1), f"F8_pair_syn{w_}_mean")
        add(PAIR[:, :, w_].max(1), f"F8_pair_syn{w_}_max")
    return np.column_stack(out)


def cross_block(A: dict[str, np.ndarray], B: dict[str, np.ndarray],
                names_out: list[str] | None) -> np.ndarray:
    """F5: «наш X против их Y». A — наши агрегаты, B — их."""
    out = []
    for nm, fa, fb, form in CROSS:
        if fa not in A or fb not in B:
            continue
        x, y = A[fa], B[fb]
        out.append((x * y if form == "prod" else x - y).astype(np.float32))
        if names_out is not None:
            names_out.append(f"F5_{nm}")
    return np.column_stack(out)


def main() -> None:
    t0 = time.time()
    pf = PregameFeatures()
    C, fidx, hmax = card_matrix(pf)
    print(f"карточка: {CARD.name} sha1 {_sha1(CARD)}, полей {C.shape[1]}", flush=True)

    zc, zr = np.load(COMPACT), np.load(RICH)
    hz = np.load(HYBRID, allow_pickle=True)
    old = hz["mids"].astype(np.int64)
    cpos = {int(m): i for i, m in enumerate(zc["mids"].tolist())}
    rpos = {int(m): i for i, m in enumerate(zr["mids"].tolist())}
    have = np.array([int(m) in cpos and int(m) in rpos for m in old.tolist()])
    old_ok = old[have]
    lim = int(os.getenv("CATFEAT_LIMIT", "0"))
    if lim:
        old_ok = old_ok[:lim]
    ri = np.array([rpos[int(m)] for m in old_ok.tolist()])
    ci = np.array([cpos[int(m)] for m in old_ok.tolist()])
    heroes = zr["heroes"][ri].astype(np.int64)
    if not np.array_equal(heroes, zc["heroes"][ci].astype(np.int64)):
        raise SystemExit("состав героев в compact и rich разошёлся")
    print(f"выровнено карт: {len(old_ok):,}", flush=True)

    # ---------- причинные приоры по ВСЕМУ корпусу ----------
    ts_all = zr["ts"]
    nm = len(ts_all)
    zr_all = {k: zr[k] for k in ("pstats", "durations", "rk", "dk", "nw", "xp", "wins")}
    Vr, Vd, M = prior_values(zr_all)
    del zr_all
    her_all = zr["heroes"].astype(np.int64)
    acc_all = zr["accounts"].astype(np.int64)
    end_all = ts_all + zr["durations"]
    ts_rep = np.repeat(ts_all, 10)
    take = (ri[:, None] * 10 + np.arange(10)).ravel()
    print(f"строк (карта, слот): {nm * 10:,}; величин {Vr.shape[1]}", flush=True)

    cp_h = CausalPrior(her_all.ravel(), ts_rep, np.repeat(end_all, 10))
    HPS_all = slot_priors(cp_h, Vr, Vd, M, nm, K_HERO, take)
    print(f"приоры героя: {HPS_all.shape} за {time.time()-t0:.0f} c", flush=True)
    uniq = np.unique(acc_all)
    cp_p = CausalPrior(acc_all.ravel(), ts_rep, np.repeat(end_all, 10))
    PPS_all = slot_priors(cp_p, Vr, Vd, M, nm, K_PLAYER, take)
    print(f"приоры игрока: {PPS_all.shape}, игроков {len(uniq):,}, "
          f"за {time.time()-t0:.0f} c", flush=True)
    del cp_h, cp_p

    # ---------- причинная парная синергия по килам ----------
    hids = np.unique(her_all)
    hdense = np.zeros(max(hmax, int(hids.max()) + 1), dtype=np.int64)
    hdense[hids] = np.arange(len(hids))
    H = len(hids)
    pidx = [(i, j) for i in range(5) for j in range(i + 1, 5)]
    d_all = hdense[her_all]
    pk = np.empty((nm, 20), dtype=np.int64)
    for t, (i, j) in enumerate(pidx):
        for off, sl in ((0, 0), (10, 5)):
            a, b = d_all[:, sl + i], d_all[:, sl + j]
            pk[:, off + t] = np.minimum(a, b) * H + np.maximum(a, b)
    ts_rep20 = np.repeat(ts_all, 20)
    cp_pair = CausalPrior(pk.ravel() + 1, ts_rep20, np.repeat(end_all, 20))
    jq = {n: i for i, n in enumerate(PRIOR_NAMES)}
    take20 = (ri[:, None] * 20 + np.arange(20)).ravel()
    PAIR_A = np.empty((len(ri), 10, 2), dtype=np.float32)
    PAIR_B = np.empty((len(ri), 10, 2), dtype=np.float32)
    v_row = np.empty(nm * 20, dtype=np.float32)
    m_row = np.empty(nm * 20, dtype=np.float32)
    vv, mm = v_row.reshape(nm, 20), m_row.reshape(nm, 20)
    for w_, qn in enumerate(("own_kills", "k_10_20")):
        j = jq[qn]
        vv[:, :10] = Vr[:, j][:, None]
        vv[:, 10:] = Vd[:, j][:, None]
        mm[:] = M[:, j][:, None]
        g = 0.0  # fixed reference; a pre-test global mean leaks into early training
        p = cp_pair.compute(v_row, m_row, K_PAIR, g)[take20].reshape(-1, 20)
        # остаток пары над средним одиночных приоров тех же двух героев
        for t, (i_, j_) in enumerate(pidx):
            PAIR_A[:, t, w_] = p[:, t] - 0.5 * (HPS_all[:, i_, j] + HPS_all[:, j_, j])
            PAIR_B[:, t, w_] = p[:, 10 + t] - 0.5 * (HPS_all[:, 5 + i_, j]
                                                     + HPS_all[:, 5 + j_, j])
    del cp_pair, v_row, m_row, pk, d_all, Vr, Vd, M
    print(f"парная синергия готова за {time.time()-t0:.0f} c", flush=True)

    # ---------- сборка по сторонам ----------
    names_a: list[str] = []
    SA = side_block(heroes[:, :5], C, fidx, HPS_all[:, :5], PPS_all[:, :5],
                    PAIR_A, names_a)
    SB = side_block(heroes[:, 5:], C, fidx, HPS_all[:, 5:], PPS_all[:, 5:],
                    PAIR_B, None)
    del HPS_all, PPS_all, PAIR_A, PAIR_B
    print(f"базовых величин на сторону: {SA.shape[1]}", flush=True)

    xfields = sorted(({c[1] for c in CROSS} | {c[2] for c in CROSS}) & set(fidx))

    def sums(her5):
        return {f: C[:, fidx[f]][her5].sum(1).astype(np.float32) for f in xfields}
    AS, BS = sums(heroes[:, :5]), sums(heroes[:, 5:])
    for f in xfields:                      # одна шкала: иначе «бурст минус HP» — шум
        both = np.concatenate([AS[f], BS[f]])
        mu, sd = float(both.mean()), float(both.std()) + EPS
        AS[f] = (AS[f] - mu) / sd
        BS[f] = (BS[f] - mu) / sd
    names_x: list[str] = []
    XA, XB = cross_block(AS, BS, names_x), cross_block(BS, AS, None)
    del AS, BS
    SA, SB = np.hstack([SA, XA]), np.hstack([SB, XB])
    names = names_a + names_x
    del XA, XB
    print(f"итого величин на сторону: {SA.shape[1]}", flush=True)

    F = np.empty((SA.shape[0], SA.shape[1] * 2), dtype=np.float32)
    F[:, :SA.shape[1]] = SA - SB
    F[:, SA.shape[1]:] = SA + SB
    fnames = [f"{n}_diff" for n in names] + [f"{n}_sum" for n in names]
    del SA, SB
    ok = np.isfinite(F).all(0) & (F.std(0) > 1e-9)
    F, fnames = F[:, ok], [n for n, k in zip(fnames, ok) if k]
    print(f"ИТОГО новых колонок: {F.shape[1]} (отсеяно {int((~ok).sum())})", flush=True)

    np.savez(OUT_NPZ, F=F, names=np.array(fnames), mids=old_ok,
             card_sha1=_sha1(CARD))
    fam: dict[str, int] = {}
    for n in fnames:
        fam[n.split("_")[0]] = fam.get(n.split("_")[0], 0) + 1
    key = {"F1": "дыры и носители контроля", "F2": "доступность контроля",
           "F3": "форма распределения по пятёрке", "F4": "карточка на позиции",
           "F5": "наш X против их Y", "F6": "приоры героя на цели",
           "F7": "приоры игрока", "F8": "парная синергия по килам"}
    lines = ["# Признаки каталога: сборка", "",
             f"Карточка `{CARD.name}` sha1 `{_sha1(CARD)}`, числовых полей "
             f"{C.shape[1]}. Карт {len(old_ok):,}. Приоры причинные: для каждой "
             f"карты берётся состояние по {nm:,} картам корпуса СТРОГО ДО неё.", "",
             "| семейство | колонок |", "|---|---|"]
    for k_ in sorted(fam):
        lines.append(f"| {k_} — {key.get(k_, '')} | {fam[k_]} |")
    lines += ["", f"Всего {F.shape[1]} колонок. Файл `{OUT_NPZ.name}`."]
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(fam, ensure_ascii=False))
    print(f"готово за {time.time() - t0:.0f} c -> {OUT_NPZ}", flush=True)


if __name__ == "__main__":
    main()
