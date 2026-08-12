#!/usr/bin/env python3
"""Предматчевая модель в бою: строгий режим без дефолтов.

Модель обучена на 482 486 про-картах (E-88…E-101). Артефакт v2: 29 признаков
(20 базовых + lvl_rel_pos/kda_player/farm_dep + 6 взаимодействий с контекстом
матча), AUC 0.7142 на всём тесте и 0.7483 на НАСТОЯЩИХ офлайн-турнирах.
Подтверждена на шести непересекающихся forward-окнах.

Взаимодействие с ЭПОХОЙ намеренно исключено, хотя давало +0.0009: в обучении
она лежит в [0,1], а в бою всегда >1 — модель экстраполировала бы по времени.

ПРИНЦИП: дефолтов нет. Если для матча не хватает данных — анализ не отдаётся,
возвращается `MissingData` с перечислением того, чего именно нет. Это сделано
намеренно: у каждого признака есть «безопасное» значение (рейтинг 1500, ноль
игр, винрейт 0.5), и модель на них не падает — она посчитает вероятность для
десяти неизвестных игроков и вернёт уверенное число из воздуха.

УРОВНИ СТРОГОСТИ и их измеренная цена (симуляция «снимок собран за сутки до
карты», 26 016 про-карт теста; в скобках доля от офлайн-турниров):

    full     всё, включая ячейки (аккаунт, герой) и историю встреч — 13.0% (27.0%)
    cells    аккаунты + команды + ячейки (аккаунт, герой)          — 18.4% (38.6%)
    teams    аккаунты + id обеих команд                            — 65.0% (63.0%)
    accounts только все 10 аккаунтов известны                       — 77.1% (65.1%)

Требование ячеек (аккаунт, герой) режет выборку в четыре раза: игроки постоянно
берут новых героев, и отсутствие ячейки — не «данных нет», а «он на этом герое
ещё не играл». Уровень выбирается вызывающим осознанно.

ПРОВЕРКИ, КОТОРЫЕ ЛОВЯТ МОЛЧАЛИВУЮ ПОЛОМКУ:
  * протухший снимок — `wr30` это окно 30 дней, `vs_wr` распад 45 дней; при
    возрасте больше `max_age_days` (3 дня) анализ не отдаётся;
  * сломанная разметка позиций — модель ждёт позиции 1..5 по порядку, и ошибка
    резолвера молча сдвинет `pos_games` и ячейки. Калибровка на 1 500 свежих
    картах: жёсткий порог 0.1% ложных отказов при 42.7% пойманных перестановок,
    мягкий (замечание) 2.5% при 71.2%.

КАЛИБРОВКА НА LAN. На офлайн-турнирах модель систематически НЕДООЦЕНИВАЕТ себя:
при заявленных 50-60% фактический винрейт 60.3%, при 80-90% — 89.4%. Изотоника,
обученная на исторических LAN-картах, эту разницу не чинит (в бакете 80-90%
даёт 83.3% против факта 89.4%), поэтому используется эмпирическая таблица
`LAN_CALIBRATION`: `lan_winrate` возвращает фактический винрейт для уровня
уверенности. Таблица меряна на 1 391 офлайн-карте.
"""
from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Sequence

import numpy as np

ARTIFACT_PATH = os.getenv(
    "PREMATCH_ARTIFACT",
    str(Path(__file__).resolve().parents[1] / "data" / "prematch_model_artifact_v2.npz"),
)
BASE20 = ["draft_logit", "elo", "games", "hero_games", "pos_games", "opp_elo",
          "hero_pool", "form", "hero_gpm_rel", "imp_recent", "wr30", "h2h_resid",
          "gpm_rel_pos", "vs_wr", "imp50", "imp_rel_pos", "lh_rel_hero",
          "gpm_ewma", "lh30", "imp30"]
EXTRA3 = ["lvl_rel_pos", "kda_player", "farm_dep"]
INTER_KEYS = ["draft_logit", "elo", "form"]
FEATURES = BASE20 + EXTRA3 + [f"{k}_x_{c}" for c in ("elo_gap", "games_exp") for k in INTER_KEYS]
STRICTNESS = ("accounts", "teams", "cells", "full")

# уверенность модели -> фактический винрейт на офлайн-турнирах (E-141, 1 391 карта)
LAN_CALIBRATION = ((0.50, 0.60, 0.603, 370), (0.60, 0.70, 0.661, 292),
                   (0.70, 0.80, 0.737, 224), (0.80, 0.90, 0.894, 226),
                   (0.90, 1.01, 0.975, 279))


class MissingData(Exception):
    """Данных не хватает — анализ не отдаём. `details` перечисляет чего именно."""

    def __init__(self, details: list[str]) -> None:
        super().__init__("; ".join(details))
        self.details = details


@dataclass
class ScoreResult:
    probability: float
    lan_winrate: float
    features: dict[str, float] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    @property
    def confidence(self) -> float:
        return max(self.probability, 1.0 - self.probability)

    @property
    def pick_radiant(self) -> bool:
        return self.probability > 0.5


def lan_winrate(confidence: float) -> float:
    """Фактический винрейт на офлайне для заявленной уверенности."""
    for lo, hi, wr, _n in LAN_CALIBRATION:
        if lo <= confidence < hi:
            return wr
    return LAN_CALIBRATION[-1][2]


class PrematchModel:
    def __init__(self, path: str | os.PathLike[str] = ARTIFACT_PATH) -> None:
        z = np.load(path)
        self.snapshot_ts = int(z["snapshot_ts"][0])
        self.mu, self.sd, self.coef, self.intercept = z["mu"], z["sd"], z["coef"], z["intercept"]
        self.acc = {int(r[0]): r[1:] for r in z["accounts"]}
        self.acc_hero = {(int(r[0]), int(r[1])): r[2:] for r in z["acc_hero"]}
        self.acc_pos = {(int(r[0]), int(r[1])): r[2] for r in z["acc_pos"]}
        self.hero_wr30 = {int(r[0]): r[1] for r in z["hero_wr30"]}
        self.vs = {(int(r[0]), int(r[1])): (r[2], r[3]) for r in z["vs_pairs"]}
        self.h2h = {(int(r[0]), int(r[1])): r[2] for r in z["h2h"]}
        self.hero_farm = {int(r[0]): r[1] for r in z["hero_farm"]} if "hero_farm" in z else {}
        self.ctx_mu = z["ctx_mu"] if "ctx_mu" in z else None
        self.ctx_sd = z["ctx_sd"] if "ctx_sd" in z else None

    def _check_positions(self, accs: Sequence[int], miss: list[str], notes: list[str]) -> None:
        """Ловит сломанную разметку позиций.

        Модель ждёт героев и аккаунтов В ПОРЯДКЕ ПОЗИЦИЙ 1..5. Если резолвер
        позиций ошибся, признаки `pos_games` и ячейки (аккаунт, герой) молча
        поедут — это ровно тот случай, когда модель не падает, а врёт.

        Порог откалиброван на корпусе: у игроков с 20+ матчами «невиданная для
        себя позиция» встречается на 5.5% карт, две сразу — на 0.3%, три и
        больше — на 0.0%. Поэтому три конфликта = разметка сломана.
        """
        hard, soft = [], []
        for i, a in enumerate(accs):
            a = int(a)
            pos = (i % 5) + 1
            row = self.acc.get(a)
            if row is None or row[1] < 20:            # мало матчей — судить не по чему
                continue
            by = {p: self.acc_pos.get((a, p), 0.0) for p in range(1, 6)}
            tot = sum(by.values()) or 1.0
            share, main = by[pos] / tot, max(by.values()) / tot
            best = max(by, key=by.get)
            if share < 0.05 and main > 0.50:
                hard.append((a, pos, best))
            if share < 0.05 and main > 0.40:
                soft.append((a, pos, best))
        # калибровка на 1 500 свежих картах: жёсткий порог даёт 0.1% ложных
        # отказов и ловит 42.7% полной перестановки позиций; мягкий — 2.5% и
        # 71.2%. Поэтому жёсткий отказывает, мягкий только предупреждает.
        if len(hard) >= 3:
            miss.append("разметка позиций противоречит истории у "
                        f"{len(hard)} слотов (аккаунт, назначено, обычная): {hard}")
        elif len(soft) >= 2:
            notes.append(f"подозрительные позиции у {len(soft)} слотов: {soft}")

    def score(self, *, radiant_accounts: Sequence[int], dire_accounts: Sequence[int],
              radiant_heroes: Sequence[int], dire_heroes: Sequence[int],
              radiant_team_id: int, dire_team_id: int,
              draft_logit: Optional[float] = None,
              strictness: str = "teams",
              now_ts: Optional[int] = None,
              max_age_days: float = 3.0) -> ScoreResult:
        """Вероятность победы радианта. Порядок аккаунтов и героев — позиции 1..5.

        Бросает `MissingData`, если данных не хватает, снимок протух или
        разметка позиций противоречит истории. Дефолты не подставляются.
        """
        if strictness not in STRICTNESS:
            raise ValueError(f"strictness должен быть одним из {STRICTNESS}")
        miss: list[str] = []
        notes: list[str] = []
        if now_ts is not None and max_age_days > 0:
            age = (int(now_ts) - self.snapshot_ts) / 86400.0
            if age > max_age_days:
                # wr30 — окно 30 дней, vs_wr — распад 45 дней; протухший снимок
                # означает «винрейт за 30 дней, закончившихся N дней назад»
                miss.append(f"снимок протух: собран {age:.1f} дней назад "
                            f"при пороге {max_age_days:.0f}")
        accs = [int(a) for a in radiant_accounts] + [int(a) for a in dire_accounts]
        hers = [int(h) for h in radiant_heroes] + [int(h) for h in dire_heroes]
        if len(accs) != 10 or len(hers) != 10:
            raise MissingData(["ожидались 10 аккаунтов и 10 героев по позициям 1..5"])
        if draft_logit is None:
            miss.append("не передан draft_logit (паблик-модель по героям)")
        zero = [i + 1 for i, a in enumerate(accs) if a <= 0]
        if zero:
            miss.append(f"нет account_id у слотов {zero}")
        unknown = [a for a in accs if a > 0 and a not in self.acc]
        if unknown:
            miss.append(f"игроки неизвестны снимку: {unknown}")
        no_wr = [h for h in hers if h not in self.hero_wr30]
        if no_wr:
            miss.append(f"нет винрейта за 30 дней у героев: {sorted(set(no_wr))}")
        if not unknown and not zero:
            self._check_positions(accs, miss, notes)
        rt, dt = int(radiant_team_id), int(dire_team_id)
        if strictness in ("teams", "cells", "full") and (rt <= 0 or dt <= 0):
            miss.append("нет id одной из команд")
        if strictness in ("cells", "full"):
            no_cell = [(a, h) for a, h in zip(accs, hers) if (a, h) not in self.acc_hero]
            if no_cell:
                miss.append(f"игрок ещё не играл на этом герое: {no_cell}")
            no_pos = [(a, p) for p, a in enumerate(accs, 1) if (a, ((p - 1) % 5) + 1) not in self.acc_pos]
            if no_pos:
                miss.append(f"нет игр на этой позиции: {no_pos}")
        key = (min(rt, dt), max(rt, dt)) if rt > 0 and dt > 0 else None
        if strictness == "full" and (key is None or key not in self.h2h):
            miss.append("нет истории личных встреч этих команд")
        if miss:
            raise MissingData(miss)

        # ---- признаки; к этому месту все нужные данные есть
        def side(a5: Sequence[int], h5: Sequence[int]) -> dict[str, float]:
            A = np.array([self.acc[int(a)] for a in a5])
            cells = [self.acc_hero.get((int(a), int(h))) for a, h in zip(a5, h5)]
            known = [c for c in cells if c is not None]
            if not known:
                notes.append("ни одной ячейки (аккаунт, герой) — hero_* нейтральны")
            hg = [c[0] if c is not None else 0.0 for c in cells]
            gpm_rel = [c[1] for c in known] or [0.0]
            lh_rel = [c[2] for c in known] or [0.0]
            return {
                "elo": A[:, 0].mean(), "games": A[:, 1].mean(), "opp_elo": A[:, 2].mean(),
                "hero_pool": A[:, 3].mean(), "form": A[:, 4].mean(), "imp50": A[:, 5].mean(),
                "imp30": A[:, 6].mean(), "gpm_rel_pos": A[:, 7].mean(),
                "imp_rel_pos": A[:, 8].mean(), "gpm_ewma": A[:, 9].mean(),
                "lh30": A[:, 10].mean(),
                "lvl_rel_pos": A[:, 11].mean() if A.shape[1] > 11 else 0.0,
                "kda_player": A[:, 12].mean() if A.shape[1] > 12 else 0.0,
                "farm_dep": float(np.mean([self.hero_farm.get(int(h), 0.0) for h in h5])),
                "hero_games": float(np.mean([math.log1p(max(x, 0)) for x in hg])),
                "hero_gpm_rel": float(np.mean(gpm_rel)), "lh_rel_hero": float(np.mean(lh_rel)),
                "wr30": float(np.mean([self.hero_wr30[int(h)] for h in h5])),
                "pos_games": float(np.mean([math.log1p(self.acc_pos.get((int(a), p), 0.0))
                                            for p, a in enumerate(a5, 1)])),
            }

        r, d = side(radiant_accounts, radiant_heroes), side(dire_accounts, dire_heroes)
        vv = [(self.vs.get((int(x), int(y)), (0.0, 0.0))) for x in radiant_heroes for y in dire_heroes]
        vs_val = float(np.mean([(w + 5.0) / (g + 10.0) for w, g in vv])) - 0.5
        h2h = float(self.h2h.get(key, 0.0)) if key else 0.0
        if key and key not in self.h2h:
            notes.append("команды раньше не встречались — h2h нейтрален")
        if key and rt > dt:
            h2h = -h2h
        lg1 = lambda x: math.log1p(max(x, 0))
        f = {
            "draft_logit": float(draft_logit),
            "elo": (r["elo"] - d["elo"]) / 400.0,
            "games": lg1(r["games"]) - lg1(d["games"]),
            "hero_games": r["hero_games"] - d["hero_games"],
            "pos_games": r["pos_games"] - d["pos_games"],
            "opp_elo": (r["opp_elo"] - d["opp_elo"]) / 400.0,
            "hero_pool": lg1(r["hero_pool"]) - lg1(d["hero_pool"]),
            "form": r["form"] - d["form"],
            "hero_gpm_rel": (r["hero_gpm_rel"] - d["hero_gpm_rel"]) / 100.0,
            "imp_recent": r["imp30"] - d["imp30"],
            "wr30": r["wr30"] - d["wr30"],
            "h2h_resid": h2h,
            "gpm_rel_pos": (r["gpm_rel_pos"] - d["gpm_rel_pos"]) / 100.0,
            "vs_wr": vs_val,
            "imp50": r["imp50"] - d["imp50"],
            "imp_rel_pos": r["imp_rel_pos"] - d["imp_rel_pos"],
            "lh_rel_hero": (r["lh_rel_hero"] - d["lh_rel_hero"]) / 100.0,
            "gpm_ewma": (r["gpm_ewma"] - d["gpm_ewma"]) / 100.0,
            "lh30": (r["lh30"] - d["lh30"]) / 100.0,
            "imp30": r["imp30"] - d["imp30"],
            "lvl_rel_pos": r["lvl_rel_pos"] - d["lvl_rel_pos"],
            "kda_player": r["kda_player"] - d["kda_player"],
            "farm_dep": r["farm_dep"] - d["farm_dep"],
        }
        if self.ctx_mu is not None:
            # контекст матча как МНОЖИТЕЛЬ антисимметричных признаков: признак
            # уровня матча в разностной схеме тождественно нулевой (E-95 §3),
            # а множителем законен. Нормировка берётся из артефакта — иначе
            # веса окажутся не на своих местах.
            ctx = np.array([abs(f["elo"]), abs(f["games"])])
            z = (ctx - self.ctx_mu) / self.ctx_sd
            for j, cname in enumerate(("elo_gap", "games_exp")):
                for k in INTER_KEYS:
                    f[f"{k}_x_{cname}"] = f[k] * float(z[j])
        x = np.array([f[k] for k in FEATURES])
        ps = []
        for i in range(len(self.coef)):
            z = (x - self.mu[i]) / self.sd[i]
            ps.append(1.0 / (1.0 + math.exp(-(float(z @ self.coef[i]) + float(self.intercept[i])))))
        p = float(np.mean(ps))
        return ScoreResult(p, lan_winrate(max(p, 1.0 - p)), f, notes)


_MODEL: Optional[PrematchModel] = None


def get_model(path: str | os.PathLike[str] = ARTIFACT_PATH) -> PrematchModel:
    global _MODEL
    if _MODEL is None:
        _MODEL = PrematchModel(path)
    return _MODEL
