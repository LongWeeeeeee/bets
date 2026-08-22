#!/usr/bin/env python3
"""Снимок контрпиков и синергий Stratz по всем героям и всем брэкетам.

ЗАЧЕМ. У Stratz своя пара чисел на каждую пару героев: сырой винрейт против
героя (`winsAverage`) и собственный РЕЙТИНГ (`synergy`) — насколько исход
отличается от ожидаемого по силе двух героев. Это не то же самое, что наши
словари, и вопрос alex — даёт ли рейтинг Stratz сигнал сверх блока `all`
(словарь post_lane).

ЧТО ВЫЯСНЕНО ПРО API (проверено запросами, не догадка):
  * `heroStats.matchUp(heroId, bracketBasicIds, take)` -> `with` и `vs`,
    по 126 записей на сторону при `take: 200`;
  * аргумент `week` в прошлое НЕ работает: любое явное значение отдаёт пусто,
    поэтому доступен только текущий скользящий срез (в ответе он помечен
    номером недели). Снимок надо накапливать повторными прогонами;
  * ЗАПРОС БЕЗ `bracketBasicIds` — это агрегат по всем брэкетам (у Muerta
    91 300 матчей против 12 920 в DIVINE_IMMORTAL). Значение `ALL` из енума
    отдаёт пусто, как и `UNCALIBRATED` с `FILTERED`.

ДВЕ СТОРОНЫ ПАРЫ — НЕЗАВИСИМЫЕ ВЫБОРКИ, А НЕ ОДНА ТАБЛИЦА. Два запроса подряд
на одну и ту же пару в DIVINE_IMMORTAL дали `138 vs 102`: 32 матча, 23 победы,
рейтинг +26.15 — и `102 vs 138`: 25 матчей, 6 побед, рейтинг −30.25. Сумма побед
29 не сходится ни с 32, ни с 25: Stratz считает таблицу каждого героя по СВОЕЙ
подвыборке матчей. Окно тут ни при чём — запросы шли подряд.

Отсюда два следствия для замера. Первое: перед использованием пару надо сводить
воедино — винрейт как взвешенное среднее двух направлений, рейтинг как
(S[a,b] − S[b,a]) / 2. Второе: асимметрия обратна размеру выборки (медиана 0.52%
на `all`, 7.4% на HERALD_GUARDIAN), поэтому по одиночному брэкету пара слишком
шумная: медиана пары в DIVINE_IMMORTAL — 151 матч, в `all` — 1 187.

Квота Stratz считается НА КЛЮЧ (8/сек, 150/мин, 1500/час), ключ привязан к
своему exit-IP — поэтому каждый поток намертво держит свою пару ключ↔прокси из
`base/keys.py`, а не берёт их из общего пула.

Прогресс пишется в jsonl построчно, повторный запуск пропускает уже собранное.

Запуск: venv_catboost/bin/python3 runtime/experiments/kills/stratz_matchups_fetch.py
Выход:  data/stratz_matchups/raw_<week>.jsonl, snapshot_<week>_<дата>.json.gz,
        latest.npz
"""
from __future__ import annotations

import gzip, json, os, queue, sys, threading, time, warnings
from pathlib import Path

warnings.filterwarnings("ignore")
import numpy as np
import requests

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "base"))
from keys import api_to_proxy  # noqa: E402

OUT_DIR = ROOT / "data/stratz_matchups"
URL = "https://api.stratz.com/graphql"
#: `None` = без аргумента брэкета, то есть агрегат по всем.
BRACKETS = [None, "HERALD_GUARDIAN", "CRUSADER_ARCHON", "LEGEND_ANCIENT", "DIVINE_IMMORTAL"]
BRACKET_KEY = {None: "all", **{b: b for b in BRACKETS if b}}
FIELDS = ("heroId2 week matchCount winCount synergy "
          "winRateHeroId1 winRateHeroId2 winsAverage")
RETRIES, PAUSE = 4, 1.2

say = lambda *a: print(*a, flush=True)
_print_lock = threading.Lock()


def gql(sess: requests.Session, key: str, proxy: str, query: str) -> dict:
    """Один запрос с повторами. Пустой ответ — не ошибка, а факт про данные."""
    last = None
    for attempt in range(RETRIES):
        try:
            r = sess.post(URL, json={"query": query}, timeout=60,
                          headers={"Authorization": f"Bearer {key}",
                                   "Content-Type": "application/json",
                                   "User-Agent": "STRATZ_API"},
                          proxies={"http": proxy, "https": proxy})
            if r.status_code == 429:
                time.sleep(20 * (attempt + 1)); continue
            d = r.json()
            if d.get("errors"):
                last = str(d["errors"])[:160]
                time.sleep(2 * (attempt + 1)); continue
            return d.get("data") or {}
        except Exception as e:                       # сеть/прокси/таймаут
            last = f"{type(e).__name__}: {e}"[:160]
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(last or "unknown")


def fetch_one(sess, key, proxy, hero_id: int, bracket) -> dict:
    arg = f"bracketBasicIds:{bracket}," if bracket else ""
    q = (f'{{heroStats{{matchUp(heroId:{hero_id},{arg}take:200)'
         f'{{heroId matchCountWith matchCountVs '
         f'with{{{FIELDS}}} vs{{{FIELDS}}}}}}}}}')
    d = gql(sess, key, proxy, q)
    rows = ((d.get("heroStats") or {}).get("matchUp")) or []
    m = rows[0] if rows else {}
    return {"hero_id": hero_id, "bracket": BRACKET_KEY[bracket],
            "match_count_with": m.get("matchCountWith") or 0,
            "match_count_vs": m.get("matchCountVs") or 0,
            "with": m.get("with") or [], "vs": m.get("vs") or []}


def main() -> None:
    t0 = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pairs = [(k, p) for p, k in api_to_proxy.items()]
    say(f"пар ключ↔прокси: {len(pairs)}")

    sess0 = requests.Session()
    k0, p0 = pairs[0]
    heroes = (gql(sess0, k0, p0,
                  "{constants{heroes{id shortName displayName}}}")
              ["constants"]["heroes"])
    heroes = sorted(heroes, key=lambda h: int(h["id"]))
    hero_ids = [int(h["id"]) for h in heroes]
    say(f"героев: {len(hero_ids)}, брэкетов: {len(BRACKETS)} "
        f"-> запросов {len(hero_ids)*len(BRACKETS)}")

    probe = fetch_one(sess0, k0, p0, hero_ids[0], None)
    week = int((probe["vs"] or probe["with"] or [{}])[0].get("week") or 0)
    say(f"текущая неделя Stratz: {week}")

    raw_path = OUT_DIR / f"raw_{week}.jsonl"
    done = set()
    if raw_path.exists():
        for line in raw_path.read_text(encoding="utf-8").splitlines():
            try:
                r = json.loads(line); done.add((r["hero_id"], r["bracket"]))
            except Exception:
                continue
        say(f"уже собрано: {len(done)} — пропускаю")

    tasks: queue.Queue = queue.Queue()
    for h in hero_ids:
        for b in BRACKETS:
            if (h, BRACKET_KEY[b]) not in done:
                tasks.put((h, b))
    total = tasks.qsize()
    say(f"к сбору: {total}")
    if not total:
        say("нечего собирать")

    fh = raw_path.open("a", encoding="utf-8")
    lock, state = threading.Lock(), {"ok": 0, "empty": 0, "fail": []}

    def worker(key: str, proxy: str) -> None:
        sess = requests.Session()
        while True:
            try:
                hero_id, bracket = tasks.get_nowait()
            except queue.Empty:
                return
            try:
                rec = fetch_one(sess, key, proxy, hero_id, bracket)
            except Exception as e:
                with lock:
                    state["fail"].append((hero_id, BRACKET_KEY[bracket], str(e)[:120]))
                tasks.task_done(); continue
            with lock:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n"); fh.flush()
                state["ok"] += 1
                if not rec["vs"] and not rec["with"]:
                    state["empty"] += 1
                n = state["ok"] + len(state["fail"])
                if n % 50 == 0:
                    with _print_lock:
                        say(f"  {n}/{total}  пусто {state['empty']}  "
                            f"сбоев {len(state['fail'])}  {time.time()-t0:.0f} c")
            tasks.task_done()
            time.sleep(PAUSE)

    ths = [threading.Thread(target=worker, args=pr, daemon=True) for pr in pairs]
    [t.start() for t in ths]
    [t.join() for t in ths]
    fh.close()
    say(f"собрано {state['ok']}, пусто {state['empty']}, сбоев {len(state['fail'])} "
        f"({time.time()-t0:.0f} c)")
    for h, b, e in state["fail"][:10]:
        say(f"  сбой hero {h} {b}: {e}")

    # ---------- упаковка ----------
    recs = [json.loads(x) for x in raw_path.read_text(encoding="utf-8").splitlines() if x.strip()]
    seen, uniq = set(), []
    for r in reversed(recs):                       # последняя запись побеждает
        k = (r["hero_id"], r["bracket"])
        if k in seen:
            continue
        seen.add(k); uniq.append(r)
    bkeys = [BRACKET_KEY[b] for b in BRACKETS]
    NH = len(hero_ids)
    pos = {h: i for i, h in enumerate(hero_ids)}
    shape = (len(bkeys), 2, NH, NH)
    mc = np.zeros(shape, np.int32); wc = np.zeros(shape, np.int32)
    syn = np.full(shape, np.nan, np.float32); wa = np.full(shape, np.nan, np.float32)
    own_wr = np.full((len(bkeys), NH), np.nan, np.float32)
    tot = np.zeros((len(bkeys), 2, NH), np.int64)
    for r in uniq:
        bi = bkeys.index(r["bracket"]); h1 = pos.get(r["hero_id"])
        if h1 is None:
            continue
        tot[bi, 0, h1] = r["match_count_with"]; tot[bi, 1, h1] = r["match_count_vs"]
        for si, side in enumerate(("with", "vs")):
            for e in r[side]:
                h2 = pos.get(int(e["heroId2"]))
                if h2 is None:
                    continue
                mc[bi, si, h1, h2] = int(e["matchCount"] or 0)
                wc[bi, si, h1, h2] = int(e["winCount"] or 0)
                if e.get("synergy") is not None:
                    syn[bi, si, h1, h2] = float(e["synergy"])
                if e.get("winsAverage") is not None:
                    wa[bi, si, h1, h2] = float(e["winsAverage"])
                if e.get("winRateHeroId1") is not None:
                    own_wr[bi, h1] = float(e["winRateHeroId1"])

    npz = OUT_DIR / "latest.npz"
    np.savez_compressed(
        npz, hero_ids=np.array(hero_ids, np.int16),
        brackets=np.array(bkeys), sides=np.array(["with", "vs"]),
        match_count=mc, win_count=wc, synergy=syn, wins_average=wa,
        own_win_rate=own_wr, totals=tot, week=np.int64(week),
        fetched_at=np.int64(int(time.time())),
        hero_short=np.array([h["shortName"] for h in heroes]),
        hero_name=np.array([h["displayName"] for h in heroes]))
    snap = OUT_DIR / f"snapshot_{week}_{time.strftime('%Y%m%d')}.json.gz"
    with gzip.open(snap, "wt", encoding="utf-8") as f:
        json.dump({"week": week, "fetched_at": int(time.time()),
                   "heroes": heroes, "brackets": bkeys, "records": uniq}, f)
    filled = int((mc > 0).sum())
    say(f"\nупаковано: пар с данными {filled:,} из {mc.size:,} ячеек")
    say(f"  {npz}  ({npz.stat().st_size/1e6:.1f} МБ)")
    say(f"  {snap} ({snap.stat().st_size/1e6:.1f} МБ)")
    say(f"всего {time.time()-t0:.0f} c")


if __name__ == "__main__":
    main()
