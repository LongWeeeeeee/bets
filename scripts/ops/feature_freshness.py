#!/usr/bin/env python3
"""Freshness-watchdog: возраст каждого источника признаков, без молчания.

ЗАЧЕМ. До 02.09.2026 протухание источников было невидимым: ELO-снимок отстал на
22 суток (E-249), ночная цепочка 8 ночей подряд не запускалась и возила на прод
один и тот же артефакт (E-193), словари гейтов не пересобирались месяцами.
Модель и гейты при этом продолжают считать и отправлять — просто на старых
числах, и в логе это выглядит как работа.

ЧТО ДЕЛАЕТ. Собирает возраст источников на этой машине и на serv1 (один ssh-ход)
и печатает таблицу. Пороги двух классов:

* ИЗМЕРЕННЫЕ — по ним печатается «ВНИМАНИЕ» (ночная цепочка шлёт такие строки в
  админ-чат своим grep'ом по логу): предматчевый артефакт 3 суток (E-177: AUC
  0.7231 на свежем против 0.6879 на месячном), ELO-снимок 3 суток (nightly с
  02.09), живая дельта 3 суток (`prematch_live_delta.MAX_AGE_SECONDS`), алиасы
  команд 3 суток (nightly), экстракты корпуса 2 суток, рассинхрон кэшей
  признаков с корпусом (блокирует переобучение весов, E-249/цепочка кэшей).
* НЕИЗМЕРЕННЫЕ — возраст печатается, но НЕ алертит: словари гейтов, watcher-
  пороги и hero_position_stats не пересобирались месяцами, цену их протухания
  никто не мерил. Сначала число, потом порог; invent-порог без замера — это
  шум, который приучает игнорировать алерты.

Запуск:  venv_catboost/bin/python3 scripts/ops/feature_freshness.py [--no-remote]
Код возврата: 1 если есть «ВНИМАНИЕ», иначе 0 (ночная цепочка зовёт с `|| true`
и ловит сами строки).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
ART = ROOT / "runtime/artifacts/misc"
SERV1 = os.getenv("FRESHNESS_SERV1", "root@23.26.193.167")
DAY = 86400.0

# Измеренные пороги (суток). Всё, чего здесь нет, печатается без алерта.
THRESHOLDS = {
    "prematch_artifact": 3.0,      # E-177: обрыв качества на 3 сутках
    "elo_snapshot": 3.0,           # nightly с 02.09.2026 (E-249)
    "elo_snapshot_local": 3.0,     # отсюда снимок едет на прод
    "live_delta": 3.0,             # prematch_live_delta.MAX_AGE_SECONDS
    "team_org_aliases": 3.0,       # nightly
    "corpus_extract_compact": 2.0,  # nightly, шаг 1 цепочки
    "corpus_extract_rich": 2.0,
    "corpus_topup_log": 1.0,       # цепочка добирает корпус, если логу > 20 ч
    "kills27_model": 60.0,         # frozen-артефакт, но вечным он не бывает
}

_REMOTE_SNIPPET = r'''
import json, re, time
from datetime import datetime
from pathlib import Path
DAY = 86400.0
now = time.time()
out = {}

def mtime_days(p):
    try:
        return round((now - Path(p).stat().st_mtime) / DAY, 2)
    except OSError:
        return None

def meta_head(p, keys, nbytes=8192):
    """Поля из начала большого JSON: meta идёт первым ключом, грузить 646 МБ незачем."""
    try:
        with open(p, encoding="utf-8") as fh:
            head = fh.read(nbytes)
    except OSError:
        return {}
    res = {}
    for k in keys:
        m = re.search(r'"%s"\s*:\s*("?[^",}]*"?)' % re.escape(k), head)
        if m:
            res[k] = m.group(1).strip('"')
    return res

# 1. предматчевый артефакт: snapshot_ts внутри npz (ленивое чтение одного ключа)
p = "/root/main/data/prematch_model_artifact_v3.npz"
try:
    import numpy as np
    with np.load(p, allow_pickle=True) as z:
        v = z["snapshot_ts"]
        ts = int(v.item() if getattr(v, "shape", ()) == () else v.max())
    out["prematch_artifact"] = {"age_days": round((now - ts) / DAY, 2),
                                "detail": f"snapshot_ts={time.strftime('%d.%m %H:%M', time.gmtime(ts))} UTC"}
except Exception as exc:
    out["prematch_artifact"] = {"age_days": None, "detail": f"не прочитан: {exc}"}

# 2. ELO-снимок: reference_utc и latest_patch из meta (первые килобайты файла)
m = meta_head("/root/main/ELO/output/live_team_elo_snapshot.json",
              ["reference_utc", "team_kills_history_latest_patch", "loaded_matches"])
ref = m.get("reference_utc")
age = None
if ref:
    try:
        age = round((now - datetime.fromisoformat(ref).timestamp()) / DAY, 2)
    except Exception:
        pass
out["elo_snapshot"] = {"age_days": age,
                       "detail": f"reference={ref} patch={m.get('team_kills_history_latest_patch')} matches={m.get('loaded_matches')}"}

# 3. живая дельта: граница снимка и самая свежая карта
try:
    d = json.load(open("/root/main/runtime/prematch_live_delta.json"))
    ends = [int(v.get("end") or 0) for v in (d.get("maps") or {}).values()]
    newest = max(ends) if ends else 0
    out["live_delta"] = {"age_days": round((now - newest) / DAY, 2) if newest else None,
                         "detail": f"карт={len(ends)} последняя={time.strftime('%d.%m %H:%M', time.gmtime(newest)) if newest else 'нет'}"}
except Exception as exc:
    out["live_delta"] = {"age_days": None, "detail": f"не прочитана: {exc}"}

# 4. живой ELO-прогресс
try:
    d = json.load(open("/root/main/runtime/live_elo_progress.json"))
    am = d.get("applied_maps") or {}
    last = max((int(v.get("applied_at") or 0) for v in am.values()), default=0)
    out["live_elo_progress"] = {"age_days": round((now - last) / DAY, 2) if last else None,
                                "detail": f"applied={len(am)} pending={len(d.get('pending_series') or {})} база={time.strftime('%d.%m', time.gmtime(int(d.get('base_reference_timestamp') or 0)))}"}
except Exception as exc:
    out["live_elo_progress"] = {"age_days": None, "detail": f"не прочитан: {exc}"}

# 5. словари и таблицы гейтов — только возраст файла (пороги не измерены)
for name, path in (
    ("dict_early", "/root/main/bets_data/analise_pub_matches/early_dict_raw.sqlite3"),
    ("dict_late", "/root/main/bets_data/analise_pub_matches/late_dict_raw.sqlite3"),
    ("dict_lane", "/root/main/bets_data/analise_pub_matches/lane_dict_raw.sqlite3"),
    ("dict_post_lane", "/root/main/bets_data/analise_pub_matches/post_lane_dict_raw.sqlite3"),
    ("dict_kills_window", "/root/main/bets_data/analise_pub_matches/kills_window_dict_raw.sqlite3"),
    ("watcher_all_only", "/root/main/base/pub_all_only_watcher_thresholds.json"),
    ("watcher_late_pre27", "/root/main/base/pub_late_pre27_watcher_thresholds.json"),
    ("comeback_piecewise", "/root/main/base/pub_late_star_comeback_table_piecewise.json"),
    ("hero_position_stats", "/root/main/base/hero_position_stats.json"),
    ("hero_baselines", "/root/main/base/hero_baselines.json"),
    ("team_org_aliases", "/root/main/data/team_org_aliases.json"),
):
    out[name] = {"age_days": mtime_days(path), "detail": "mtime"}

# 6. frozen-артефакт kills27
try:
    d = json.load(open("/root/main/ml-models/team_kills27/team_kills27_shadow.json"))
    created = str(d.get("created_at_utc") or "")
    age = None
    if created:
        age = round((now - datetime.fromisoformat(created.replace("Z", "+00:00")).timestamp()) / DAY, 2)
    out["kills27_model"] = {"age_days": age, "detail": f"created={created} features={len(d.get('feature_names') or [])}"}
except Exception as exc:
    out["kills27_model"] = {"age_days": None, "detail": f"не прочитан: {exc}"}

print(json.dumps(out))
'''


def _remote_rows() -> dict:
    """Один ssh-ход: возраст продовых источников."""
    try:
        proc = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", SERV1,
             "/root/main/venv/bin/python3 -"],
            input=_REMOTE_SNIPPET, capture_output=True, text=True, timeout=180,
        )
    except Exception as exc:
        return {"_error": {"age_days": None, "detail": f"ssh не выполнился: {exc}"}}
    if proc.returncode != 0:
        return {"_error": {"age_days": None,
                           "detail": f"serv1 rc={proc.returncode}: {proc.stderr.strip()[:200]}"}}
    try:
        return json.loads(proc.stdout.strip().splitlines()[-1])
    except Exception as exc:
        return {"_error": {"age_days": None, "detail": f"ответ serv1 не разобрать: {exc}"}}


def _local_rows() -> dict:
    now = time.time()
    out: dict[str, dict] = {}

    def mtime_days(p: Path):
        try:
            return round((now - p.stat().st_mtime) / DAY, 2)
        except OSError:
            return None

    # экстракты корпуса (ночная цепочка, шаг 1)
    for name, path in (("corpus_extract_compact", ART / "pro_corpus_compact.npz"),
                       ("corpus_extract_rich", ART / "pro_corpus_rich.npz")):
        out[name] = {"age_days": mtime_days(path), "detail": "mtime"}

    # лог добора корпуса: цепочка добирает, если свежего лога нет 20 ч
    logs = sorted((ROOT / "runtime").glob("pro_topup_*.log"), key=lambda p: p.stat().st_mtime)
    out["corpus_topup_log"] = {"age_days": mtime_days(logs[-1]) if logs else None,
                               "detail": logs[-1].name if logs else "логов нет"}

    # локальный ELO-снимок (отсюда он едет на прод)
    snap = ROOT / "ELO/output/live_team_elo_snapshot.json"
    age, detail = None, "нет файла"
    try:
        head = snap.open(encoding="utf-8").read(8192)
        m = re.search(r'"reference_utc"\s*:\s*"([^"]+)"', head)
        if m:
            age = round((now - datetime.fromisoformat(m.group(1)).timestamp()) / DAY, 2)
            detail = f"reference={m.group(1)}"
    except OSError:
        pass
    out["elo_snapshot_local"] = {"age_days": age, "detail": detail}

    # рассинхрон кэшей признаков с корпусом: блокирует переобучение весов
    try:
        n_compact = int(np.load(ART / "pro_corpus_compact.npz")["mids"].shape[0])
        bad = []
        for name, key in (("pro_features_ext", "F"), ("ideas_batch1", "F"),
                          ("ideas_batch2", "F"), ("ideas_batch5", "F"),
                          ("ideas_batch5b", "F")):
            p = ART / f"{name}.npz"
            if not p.exists():
                bad.append(f"{name}: нет")
                continue
            rows = int(np.load(p)[key].shape[0])
            if rows != n_compact:
                bad.append(f"{name}: {rows:,}")
        for name in ("pro_draft_logit", "pro_draft_logit_full"):
            p = ART / f"{name}.npz"
            if not p.exists():
                bad.append(f"{name}: нет")
                continue
            rows = int(np.load(p)["logit"].shape[0])
            if rows != n_compact:
                bad.append(f"{name}: {rows:,}")
        out["feature_caches_sync"] = {
            "age_days": None,
            "detail": f"корпус {n_compact:,}" + (f"; РАСХОЖДЕНИЕ: {', '.join(bad)}" if bad else "; кэши синхронны"),
            "alert": bool(bad),
        }
    except Exception as exc:
        out["feature_caches_sync"] = {"age_days": None, "detail": f"не проверено: {exc}"}

    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--no-remote", action="store_true", help="не ходить на serv1")
    args = parser.parse_args(argv)

    rows = _local_rows()
    if not args.no_remote:
        rows.update(_remote_rows())

    alerts: list[str] = []
    print(f"свежесть источников на {datetime.now(timezone.utc).strftime('%d.%m %H:%M')} UTC")
    print(f"{'источник':<26} {'возраст, сут':>12}  {'порог':>6}  примечание")
    for name in sorted(rows):
        row = rows[name]
        age = row.get("age_days")
        age_s = "—" if age is None else f"{age:.2f}"
        threshold = THRESHOLDS.get(name)
        thr_s = "—" if threshold is None else f"{threshold:.0f}"
        detail = str(row.get("detail") or "")
        stale = bool(row.get("alert")) or (
            threshold is not None and age is not None and age > threshold
        )
        missing = age is None and name != "feature_caches_sync" and threshold is not None
        mark = "⚠" if (stale or missing) else " "
        print(f"{mark} {name:<24} {age_s:>12}  {thr_s:>6}  {detail}")
        if stale or missing:
            if missing:
                why = "нет данных"
            elif threshold is None:
                why = "рассинхрон или отказ источника"
            else:
                why = f"старше порога {threshold:.0f} сут"
            alerts.append(f"ВНИМАНИЕ: свежесть {name} — {why} ({detail})")

    print()
    if alerts:
        for line in alerts:
            print(line)
        print("источники без порога печатаются справочно: цена их протухания не измерена")
        return 1
    print("все измеренные пороги в норме")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
