#!/usr/bin/env python3
"""Фактическая доходность предматчевых ставок по РЕАЛЬНЫМ котировкам Winline.

ЗАЧЕМ. Считалось, что прибыльность по записям системы недоказуема: архив
котировок якобы покрывает три матча. Это неверно — так разбирался ключ, а не так
устроен архив. Записи имеют ДВА формата ключа:

    dltv.org/matches/8900882416|map2|BoomBoys|Nigma Galaxy      (редкий)
    sourcetv:league:19944|id:10163973|id:9256405|map1|RE ARISE|Level UP esports

Во втором `match_id` нет вообще — есть лига, id команд, номер карты и имена.
Поэтому join по `match_id` даёт пустоту, а join по (имена команд, номер карты)
находит котировки для ВСЕХ отправленных предматчевых ставок. Это и есть E-103
«архив несоединим» — данные были на месте, не совпадал ключ.

ЧТО СЧИТАЕТСЯ. Ставка предматчевой модели — на победителя карты, и котировки
`winline_current_map_winner` — тот же рынок, поэтому сравнение законно. Для ставок
на килы этот архив НЕ подходит: там другой рынок, и join по нему был бы враньём.

КОТИРОВКА БЕРЁТСЯ БЛИЖАЙШАЯ ПО ВРЕМЕНИ к моменту ставки, а не последняя. Матч
идёт, цена ходит: последняя запись бывает глубоко внутриигровой (1.04 против 8.95
в начале), и по ней доходность вышла бы фантастической. Записи без валидного
`wall` пропускаются — у части их временем стоит заглушка 1700000000.

ПРАВИЛО СТАВКИ повторяет боевое: ставим, только если рынок дал не меньше
`min_odds` из сообщения. Пропущенные показываются отдельно — по ним видно, режет
правило выигрышные карты или проигрышные.

Запуск: venv_catboost/bin/python3 runtime/experiments/misc/prematch_bet_roi.py
Выход:  runtime/artifacts/misc/prematch_bet_roi.md
"""
from __future__ import annotations

import collections
import json
import os
import re
import subprocess
import time
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
ART = ROOT / "runtime/artifacts/misc"
OUT = ART / "prematch_bet_roi.md"
SERV1 = os.getenv("SERV1_HOST", "serv1")
R_ODDS = "/root/main/runtime/winline_odds_history.jsonl"
R_BETS = "/root/main/runtime/prematch_model_bet_sent.jsonl"

KEY = re.compile(r"id:(\d+)\|id:(\d+)\|map(\d+)\|([^|]*)\|([^|]*)")
MID = re.compile(r"/(\d+)\.")


def fetch(path: str) -> list[dict]:
    """Читаем с боевой машины, ничего там не меняя."""
    res = subprocess.run(["ssh", SERV1, f"cat {path}"], capture_output=True, text=True)
    if res.returncode != 0:
        raise SystemExit(f"не прочитать {path}: {res.stderr.strip()[:200]}")
    out = []
    for line in res.stdout.split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except ValueError:
            continue
    return out


def main() -> None:
    t0 = time.time()
    quotes = collections.defaultdict(list)
    for r in fetch(R_ODDS):
        m = KEY.search(str(r.get("canonical_key") or ""))
        if not m or not (r.get("p1_odds") and r.get("p2_odds")):
            continue
        w = r.get("wall")
        if not (isinstance(w, (int, float)) and w > 1.7e9):
            continue
        quotes[(frozenset((m.group(4).strip().lower(), m.group(5).strip().lower())),
                int(m.group(3)))].append(
            (float(w), m.group(4).strip().lower(), m.group(5).strip().lower(),
             float(r["p1_odds"]), float(r["p2_odds"])))

    bets = {}
    for r in fetch(R_BETS):
        mm = MID.search(str(r.get("match_key") or ""))
        if not mm:
            continue
        bets[(mm.group(1), r.get("map_num"), r["side"])] = r

    z = np.load(ART / "pro_corpus_compact.npz", allow_pickle=True)
    pos = {int(m): i for i, m in enumerate(z["mids"].tolist())}
    wins = z["wins"]

    rows, nomatch = [], 0
    for (mid, mp, sd), r in bets.items():
        rt = str(r.get("radiant_team") or "").strip().lower()
        dt = str(r.get("dire_team") or "").strip().lower()
        cand = quotes.get((frozenset((rt, dt)), int(mp or 1)))
        if not cand:
            nomatch += 1
            continue
        bt = float(r["ts"])
        w, n1, n2, p1, p2 = min(cand, key=lambda x: abs(x[0] - bt))
        want = rt if sd == "radiant" else dt
        odds = p1 if n1 == want else (p2 if n2 == want else None)
        i = pos.get(int(mid))
        if odds is None or i is None:
            nomatch += 1
            continue
        rad = bool(int(wins[i]))
        rows.append({"mid": mid, "map": mp, "side": sd, "need": float(r["min_odds"]),
                     "odds": odds, "won": rad if sd == "radiant" else (not rad),
                     "lag_s": w - bt})

    took = [r for r in rows if r["odds"] >= r["need"]]
    skip = [r for r in rows if r["odds"] < r["need"]]
    bank = sum((r["odds"] - 1.0) if r["won"] else -1.0 for r in took)

    lines = ["# Доходность предматчевых ставок по реальным котировкам", ""]
    lines.append(f"Ставок в журнале: {len(bets)}, котировки нашлись у {len(rows)}, "
                 f"не сопоставлено {nomatch}. Котировка берётся ближайшая по времени "
                 f"к ставке (медианный разрыв "
                 f"{np.median([abs(r['lag_s']) for r in rows]) if rows else 0:.0f} с).")
    lines += ["", "| матч | карта | сторона | нужен кэф | рынок | исход |",
              "|---|---:|---|---:|---:|---|"]
    for r in sorted(rows, key=lambda x: x["mid"]):
        mark = ("ЗАШЛА" if r["won"] else "мимо") if r["odds"] >= r["need"] else "пропуск"
        lines.append(f"| {r['mid']} | {r['map']} | {r['side']} | {r['need']:.2f} | "
                     f"{r['odds']:.2f} | {mark} |")
    lines.append("")
    if took:
        lines.append(f"**Поставлено по правилу: {len(took)}, зашло "
                     f"{sum(1 for r in took if r['won'])} "
                     f"({sum(1 for r in took if r['won']) / len(took):.1%}), "
                     f"банк {bank:+.2f} ед., ROI {bank / len(took):+.1%}**")
    if skip:
        sw = sum(1 for r in skip if r["won"])
        lines.append(f"Пропущено правилом: {len(skip)}, из них выиграли бы {sw} "
                     f"({sw / len(skip):.1%}) — если эта доля НИЖЕ поставленных, "
                     f"правило режет верно.")
    lines.append("")
    lines.append("Выборка мала: любое ROI отсюда — наблюдение, а не ожидаемая "
                 "доходность. Смысл отчёта в том, чтобы величина считалась регулярно "
                 "и накапливалась, а не в разовом числе.")
    lines.append(f"\nПрогон занял {time.time() - t0:.0f} c.")
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines), flush=True)


if __name__ == "__main__":
    main()
