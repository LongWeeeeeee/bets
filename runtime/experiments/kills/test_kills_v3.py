#!/usr/bin/env python3
"""Самопроверка килов v3 на синтетике — до дорогого счёта, а не после.

Проверяется ровно то, что ломается молча и выглядит рабочим:
  1. скользящие средние берут только ПРОШЛОЕ (утечка цели в признак);
  2. NaN не отравляет `np.cumsum` и не обнуляет хвост группы;
  3. разбор корпуса: итог килов карты = сумма поминутного массива, а индексы
     NW-лида отвечают тем минутам, на которых прод проверяет гейт;
  4. переворот вида: разностные колонки меняют знак, уровневые — нет. Ошибка
     здесь дала бы модель, которая выучила «радиант», а не «сторону»;
  5. цели: ничья считается поражением, `ge27`/`tot51` совпадают с прямым счётом.

Запуск: venv_catboost/bin/python3 runtime/experiments/kills/test_kills_v3.py
"""
from __future__ import annotations

import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))), "base"))

from window_model_v2 import TrailingIndex, team_mean  # noqa: E402
from kills_v3_extract import NW_IDX, parse, patch_code  # noqa: E402
from kills_v3_train import (  # noqa: E402
    TARGET_KILLS, TARGET_TOTAL, abs_index, flip_signs, lvl_index, make_targets, split_pro,
)

bad = 0


def check(name: str, ok: bool, extra: str = "") -> None:
    global bad
    bad += not ok
    print(f"{name:34s}: {'OK' if ok else 'СЛОМАНО'} {extra}")


def fake_match(rk: list[int], dk: list[int], nw: list[int]) -> dict:
    players = []
    for side in (True, False):
        for pos in range(1, 6):
            players.append({
                "isRadiant": side, "position": f"POSITION_{pos}",
                "heroId": (1 if side else 6) + pos, "kills": 1, "deaths": 2, "assists": 3,
                "numLastHits": 10, "numDenies": 1, "level": 20, "goldPerMinute": 400,
                "experiencePerMinute": 500, "imp": None if (side and pos == 1) else 7,
                "networth": 12000, "heroDamage": 20000, "heroHealing": 0,
                "towerDamage": 1000, "steamAccount": {"id": 100 + pos * (1 if side else 2),
                                                      "isAnonymous": False},
            })
    return {"startDateTime": 1700000000, "durationSeconds": len(rk) * 60,
            "didRadiantWin": sum(rk) > sum(dk), "radiantKills": rk, "direKills": dk,
            "radiantNetworthLeads": nw, "radiantExperienceLeads": nw,
            "players": players, "leagueId": 42, "league": {"id": 42, "tier": "PREMIUM"},
            "radiantTeam": {"id": 7}, "direTeam": {"id": 8}}


def main() -> int:
    # ---- 1-2. скользящие средние
    idx = TrailingIndex(np.array([1, 1, 1, 1, 2, 2, 2]), np.array([10, 20, 30, 40, 5, 6, 7]))
    vals = np.array([1.0, 2.0, np.nan, 4.0, 100.0, 200.0, 300.0])
    check("средние: только прошлое",
          np.allclose(idx.mean(vals, None), [np.nan, 1.0, 1.5, 1.5, np.nan, 100.0, 150.0],
                      equal_nan=True))
    check("средние: окно 2",
          np.allclose(idx.mean(vals, 2), [np.nan, 1.0, 1.5, 2.0, np.nan, 100.0, 150.0],
                      equal_nan=True))
    m, c = team_mean(np.array([1., 2., 3., 4., np.nan] + [10.] * 5), 1)
    check("свод по сторонам", np.allclose(m[0], [2.5, 10.0]) and c.tolist() == [[4.0, 5.0]])

    # ---- 3. разбор
    check("патч из имени файла",
          (patch_code("7.41e_part003.json"), patch_code("7.41_part1.json"),
           patch_code("historical_part1.json")) == (74105, 74100, 0))
    rk = ([0, 1, 0, 2, 0, 1, 1, 0, 3, 0, 2, 1, 0, 0, 1, 2, 0, 1, 1, 0, 2, 3, 0, 1, 0, 1]
          + [1, 0, 2, 0, 1, 3])
    dk = ([1, 0, 0, 1, 1, 0, 2, 1, 0, 0, 1, 1, 1, 0, 0, 1, 2, 0, 0, 1, 1, 0, 1, 0, 2, 0]
          + [0, 1, 0, 1, 0, 2])
    nw = list(range(0, 3200, 100))
    got = parse(fake_match(rk, dk, nw))
    (h, a, kda, sm, bg, t, dur, win, df, tt, v, ks, nwl, xp, nok, lg, tr, tm) = got
    check("итог карты = сумма поминутного", ks == [sum(rk), sum(dk)], f"{ks}")
    check("NW-лид на минутах гейтов", nwl == [nw[i] for i in NW_IDX], f"{nwl}")
    check("окно 10-20 считается по срезу",
          df[1] == sum(rk[10:20]) - sum(dk[10:20]) and tt[1] == sum(rk[10:20]) + sum(dk[10:20]))
    check("валидность по длине массива", v == [True, True, True, True])
    check("imp: пропуск помечен", sm[0][5] == -32768 and sm[1][5] == 7)
    check("лига, тир и команды", (lg, tr, tm) == (42, 6, [7, 8]))
    short = parse(fake_match(rk[:18], dk[:18], nw[:18]))
    check("короткая карта: окна отсекаются", short[10] == [True, False, False, False],
          f"{short[10]}")
    check("карта короче первого окна отбрасывается",
          parse(fake_match(rk[:12], dk[:12], nw[:12])) is None)

    # ---- 4. переворот вида
    names = ["a_diff", "b_lvl", "c_cov", "d_diff"]
    X = np.array([[1.0, 2.0, 3.0, 4.0]], dtype=np.float32)
    F = flip_signs(X, names)
    check("переворот: знак только у diff/cov",
          F.tolist() == [[-1.0, 2.0, -3.0, -4.0]] and X.tolist() == [[1.0, 2.0, 3.0, 4.0]])
    check("модуль разностей не меняется переворотом",
          np.allclose(np.abs(X[:, [0, 3]]), np.abs(F[:, [0, 3]])))
    check("отбор колонок",
          lvl_index(names).tolist() == [1]
          and abs_index(["pl_wdiff_5_15_30", "pl_kills_30_diff", "zzz"]).tolist() == [0, 1])

    # ---- 5. цели
    class Z(dict):
        pass
    z = Z(kills_side=np.array([[27, 26], [26, 27], [30, 30], [10, 15]], dtype=np.int16),
          diffs=np.array([[1, 0, -1, 2]] * 4, dtype=np.int16),
          valid=np.ones((4, 4), bool))
    tg = make_targets(z, np.arange(4))
    check("цель map: ничья = поражение",
          tg["map"]["y"].tolist() == [1, 0, 0, 0])
    check("цель ge27 по стороне radiant",
          tg["ge27"]["y"].tolist() == [1, 0, 1, 0],
          f"порог {TARGET_KILLS}")
    check("цель tot51 по сумме сторон",
          tg["tot51"]["y"].tolist() == [1, 1, 1, 0], f"порог {TARGET_TOTAL}")
    check("окно: ничья = поражение", tg["w_10_20"]["y"].tolist() == [0, 0, 0, 0])

    # ---- 6. сплит про: свежие патчи в тест, они же не должны попасть в train
    patch = np.array([73900] * 50000 + [74100] * 20000 + [74103] * 9000)
    parts, cut = split_pro(patch, len(patch))
    ok = (cut == 74103 and set(patch[parts["test"]]) == {74103}
          and 74103 not in set(patch[parts["train"]])
          and len(parts["train"]) + len(parts["val"]) + len(parts["test"]) == len(patch))
    check("сплит про: тест = свежий патч", ok, f"cut={cut}, тест {len(parts['test']):,}")

    print("ВСЁ ОК" if not bad else f"ПРОВАЛОВ: {bad}")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
