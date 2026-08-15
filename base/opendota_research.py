#!/usr/bin/env python3
"""Сбор разобранных матчей из OpenDota — то, чего у Stratz нет или мало.

Зачем при уже готовом сборе playback у Stratz. Замер 15.08 на 30 картах нашего
корпуса: у OpenDota разобрано **29 из 30**, причём **9 из 10** тех карт, по
которым Stratz playback НЕ отдаёт. Плюс к этому OpenDota хранит разбор без
срока, а у Stratz детальный playback живёт около 85 суток — то есть по 7.41
доступно 42 560 карт вместо 23 887.

И главное: OpenDota отдаёт то, ради чего пришлось бы качать миллионы событий
урона поштучно, — УЖЕ СВЁРНУТЫЕ матрицы:

  * `damage` / `damage_taken` — урон герой→герой за карту. Это на два порядка
    плотнее убийств: пара героев обменивается одним убийством за несколько карт,
    но уроном — каждую карту. Именно нехватка плотности и оставила ячейку
    «этот саппорт против этого кора» на надёжности 0.24 (E-199).
  * `killed` / `killed_by` — матрица убийств. Сверено с playback Stratz на общей
    карте: 134 убийства против 135, расходится одна ячейка из 43.
  * `ability_targets` — сколько раз КАЖДАЯ способность применена по КАЖДОМУ
    герою. Прямой ответ на «этот саппорт вешает свой стан именно на этого кора».
  * `kills_log` — убийства со временем и жертвой.
  * `teamfights` — бои с уроном, смертями и золотом по каждому игроку.

Лимиты (замерено 15.08 по заголовкам ответа): 60 запросов в минуту и
**3000 в сутки на IP**. Своя квота у каждого адреса, поэтому сбор идёт через
пул прокси. IP самого serv1 не занимаем: с него в OpenDota ходит прод
(`cyberscore_try.py`).

Запуск — см. `collect_pro_patch()` внизу.
"""
from __future__ import annotations

import gzip
import json
import os
import queue
import threading
import time

try:
    import requests
except Exception:                                   # noqa: BLE001
    requests = None

OD_URL = "https://api.opendota.com/api/matches/{}"
OD_CONST = "https://api.opendota.com/api/constants/heroes"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
DAY_RESERVE = 5          # столько запросов оставляем адресу про запас
MAX_TRIES = 3


# ─────────────────────────── справочник героев ───────────────────────────

def hero_npc_map(card_path=None):
    """npc_dota_hero_x -> hero_id. Сначала из OpenDota, иначе из карточки."""
    try:
        r = requests.get(OD_CONST, headers={"User-Agent": UA}, timeout=60)
        if r.status_code == 200:
            return {v["name"]: int(k) for k, v in r.json().items() if v.get("name")}
    except Exception:                               # noqa: BLE001
        pass
    card_path = card_path or os.path.join(os.path.dirname(__file__),
                                          "hero_features_v7.json")
    with open(card_path, encoding="utf-8") as fh:
        card = json.load(fh)
    return {"npc_dota_hero_" + r["hero_slug"]: int(r["hero_id"])
            for r in card.values() if r.get("hero_slug")}


# ─────────────────────────── сжатие ответа ───────────────────────────

def compact(d, npc2id):
    """23 КБ вместо 380: выбрасываем чат, косметику, поминутные ряды.

    Из покупок оставляем ПЕРВУЮ покупку каждого предмета — двадцать танго
    ничего не добавляют, а «на какой минуте появился скипетр» сохраняется.
    """
    def hm(dd):
        return {str(npc2id[k]): v for k, v in (dd or {}).items() if k in npc2id}

    out = {"id": d.get("match_id"), "dur": d.get("duration"),
           "rw": d.get("radiant_win"), "patch": d.get("patch"),
           "lg": d.get("leagueid"), "st": d.get("start_time"),
           "ver": d.get("version"),
           "sc": [d.get("radiant_score"), d.get("dire_score")],
           "players": [], "fights": []}
    for p in d.get("players") or []:
        first = {}
        for e in p.get("purchase_log") or []:
            k = e.get("key")
            if k and k not in first:
                first[k] = e.get("time")
        out["players"].append({
            "hero": p.get("hero_id"), "rad": p.get("isRadiant"),
            "lane": p.get("lane_role"), "pos": p.get("position_est"),
            "roam": p.get("is_roaming"),
            "k": p.get("kills"), "d": p.get("deaths"), "a": p.get("assists"),
            "nw": p.get("net_worth"), "lvl": p.get("level"),
            "hd": p.get("hero_damage"), "stuns": p.get("stuns"),
            "tfp": p.get("teamfight_participation"),
            "dmg": hm(p.get("damage")), "dmgt": hm(p.get("damage_taken")),
            "kill": hm(p.get("killed")), "killby": hm(p.get("killed_by")),
            "klog": [[e.get("time"), npc2id[e["key"]]]
                     for e in (p.get("kills_log") or []) if e.get("key") in npc2id],
            "atg": {a: hm(t) for a, t in (p.get("ability_targets") or {}).items()},
            "buys": sorted(([v, k] for k, v in first.items()),
                           key=lambda x: (x[0] is None, x[0])),
        })
    for f in d.get("teamfights") or []:
        out["fights"].append([f.get("start"), f.get("end"),
                              [[pl.get("damage"), pl.get("deaths"),
                                pl.get("gold_delta")]
                               for pl in (f.get("players") or [])]])
    return out


def done_ids(out_dir):
    """Обработано = карта РЕАЛЬНО лежит в файле, а не «мы к ней ходили»."""
    done = set()
    if not os.path.isdir(out_dir):
        return done
    for name in os.listdir(out_dir):
        if not name.endswith(".jsonl.gz"):
            continue
        try:
            with gzip.open(os.path.join(out_dir, name), "rt", encoding="utf-8") as fh:
                for line in fh:
                    try:
                        done.add(int(json.loads(line)["id"]))
                    except Exception:               # noqa: BLE001
                        continue
        except Exception:                           # noqa: BLE001
            pass                                     # недописанный хвост
    return done


# ─────────────────────────── сбор ───────────────────────────

def collect(ids, out_dir, endpoints, pace=1.05, show_prints=True):
    """По одному потоку на адрес. Каждый адрес держит СВОЮ суточную квоту.

    Адрес, у которого квота кончилась, молча выходит: долбиться в 429 бесполезно,
    Stratz и OpenDota одинаково списывают отказ как обычный запрос.
    """
    os.makedirs(out_dir, exist_ok=True)
    have = done_ids(out_dir)
    todo = [int(i) for i in ids if int(i) not in have]
    if show_prints:
        print(f"🅾️ OpenDota: всего {len(ids):,}, уже есть {len(have):,}, "
              f"к сбору {len(todo):,} через {len(endpoints)} адресов", flush=True)
    if not todo:
        return 0

    npc2id = hero_npc_map()
    q = queue.Queue()
    for i in todo:
        q.put(i)
    tries = {}
    lock = threading.Lock()
    path = os.path.join(out_dir, f"od_{time.strftime('%Y%m%d_%H%M%S')}.jsonl.gz")
    fh = gzip.open(path, "wt", encoding="utf-8")
    stats = {"ok": 0, "unparsed": 0, "err": 0, "drop": 0}

    def worker(px, tag):
        sess = requests.Session()
        if px:
            sess.proxies = {"http": px, "https": px}
        sess.headers.update({"User-Agent": UA})
        left = None
        while True:
            try:
                mid = q.get_nowait()
            except queue.Empty:
                return
            t0 = time.time()
            try:
                r = sess.get(OD_URL.format(mid), timeout=90)
            except Exception as exc:                # noqa: BLE001
                with lock:
                    stats["err"] += 1
                _requeue(mid, q, tries, stats)
                time.sleep(3)
                continue
            try:
                left = int(r.headers.get("x-rate-limit-remaining-day", left or 9999))
            except Exception:                       # noqa: BLE001
                pass
            if r.status_code == 429:
                _requeue(mid, q, tries, stats)
                if show_prints:
                    print(f"   ⛔ {tag}: суточная квота выбрана, адрес выходит",
                          flush=True)
                return
            if r.status_code != 200:
                with lock:
                    stats["err"] += 1
                _requeue(mid, q, tries, stats)
                time.sleep(2)
                continue
            try:
                row = compact(r.json(), npc2id)
            except Exception:                       # noqa: BLE001
                with lock:
                    stats["err"] += 1
                _requeue(mid, q, tries, stats)
                continue
            with lock:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                stats["ok"] += 1
                if not row.get("ver"):
                    stats["unparsed"] += 1
                if show_prints and stats["ok"] % 250 == 0:
                    fh.flush()
                    print(f"   собрано {stats['ok']:,} "
                          f"({stats['unparsed']:,} без разбора), осталось "
                          f"{q.qsize():,}, ошибок {stats['err']}, "
                          f"брошено {stats['drop']}", flush=True)
            if left is not None and left <= DAY_RESERVE:
                if show_prints:
                    print(f"   💤 {tag}: осталось {left} запросов в сутках, выходит",
                          flush=True)
                return
            time.sleep(max(0.0, pace - (time.time() - t0)))

    threads = [threading.Thread(target=worker, args=(px, tag), daemon=True)
               for px, tag in endpoints]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    fh.close()
    if show_prints:
        print(f"✅ собрано {stats['ok']:,} карт ({stats['unparsed']:,} без разбора), "
              f"брошено {stats['drop']}, осталось в очереди {q.qsize():,}\n"
              f"   файл: {path}", flush=True)
    return stats["ok"]


def _requeue(mid, q, tries, stats):
    tries[mid] = tries.get(mid, 0) + 1
    if tries[mid] < MAX_TRIES:
        q.put(mid)
    else:
        stats["drop"] += 1


def proxy_endpoints(include_direct=False):
    """Адреса для сбора.

    Квота у OpenDota считается ПО IP, поэтому список адресов = список квот.
    Берём либо файл из `OD_PROXIES_FILE` (по строке на адрес, там же может быть
    socks-туннель до другой машины), либо пул из keys.py. Свой IP по умолчанию
    не занимаем: с него в OpenDota ходит прод.
    """
    eps = []
    fpath = os.getenv("OD_PROXIES_FILE")
    if fpath and os.path.exists(fpath):
        for line in open(fpath, encoding="utf-8"):
            px = line.strip()
            if px and not px.startswith("#"):
                eps.append((px, px.split("@")[-1]))
    if not eps:
        try:
            from keys import api_to_proxy
            for px in api_to_proxy:
                eps.append((px, px.split("@")[-1]))
        except Exception as exc:                    # noqa: BLE001
            print(f"⚠️ пул прокси недоступен: {exc}")
    if include_direct:
        eps.append((None, "direct"))
    return eps


def collect_pro_patch(ids_file, out_dir, include_direct=False, pace=1.05,
                      loop_sleep=3600):
    """Проход за проходом, пока очередь не кончится.

    Один проход выбирает суточную квоту всех адресов примерно за 50 минут
    (3000 запросов при 60 в минуту), после чего адреса выходят сами. Дальше
    ждём и пробуем снова: пробный запрос в выбранную квоту стоит один запрос,
    поэтому цикл дешёвый.
    """
    ids = sorted({int(x) for x in open(ids_file).read().split()}, reverse=True)
    eps = proxy_endpoints(include_direct)
    while True:
        have = done_ids(out_dir)
        left = [i for i in ids if i not in have]
        if not left:
            print(f"🏁 всё собрано: {len(have):,} карт", flush=True)
            return len(have)
        print(f"\n=== проход {time.strftime('%Y-%m-%d %H:%M:%S')}: "
              f"осталось {len(left):,} из {len(ids):,} ===", flush=True)
        got = collect(ids, out_dir, eps, pace=pace)
        if got == 0:
            print(f"   квоты выбраны, спим {loop_sleep / 3600:.1f} ч", flush=True)
        time.sleep(loop_sleep)


if __name__ == "__main__":
    import sys
    collect_pro_patch(sys.argv[1], sys.argv[2],
                      include_direct="--direct" in sys.argv)
