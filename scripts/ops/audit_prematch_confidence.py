#!/usr/bin/env python3
"""Audit saved signals against exact-ID source outcomes without rescoring models.

Input directory: prematch_model_bet_sent.jsonl, prematch_model_eval.jsonl,
source_labels.json ({match_id: {radiant_win, start, end, ...}}).
Only the first sent prediction per real match ID is counted. The account-ELO
comparison uses its logged sign; it is not a calibrated ELO-only model.
"""
import argparse
import collections
import hashlib
import json
import math
import re
from pathlib import Path


def load_rows(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def match_id(row):
    found = re.fullmatch(r"(?:https?://)?dltv\.org/matches/(\d+)(?:\.\d+)?", row["match_key"])
    if not found:
        raise ValueError("Unrecognized match key: " + row["match_key"])
    return found.group(1)


def first_per_map(rows):
    first = {}
    for row in sorted(rows, key=lambda r: r["ts"]):
        first.setdefault(match_id(row), row)
    return first


def extract_labels(directory, corpus):
    """Use raw source winners, never ELO application ledger or model scores."""
    wanted = set(first_per_map(load_rows(directory / "prematch_model_bet_sent.jsonl")))
    labels = {}

    def add(mid, winner, start, end, source):
        mid = str(mid)
        if mid not in wanted or not isinstance(winner, bool) or not start or not end:
            return
        if mid in labels and labels[mid]["radiant_win"] != winner:
            raise ValueError("Conflicting source winners: " + mid)
        labels.setdefault(mid, {"radiant_win": winner, "start": int(start), "end": int(end), "source": str(source)})

    for path in sorted(corpus.glob("7.41e_part*.json")):
        for mid, row in json.loads(path.read_text()).items():
            if mid in wanted:
                start, duration = row.get("startDateTime"), row.get("durationSeconds")
                add(mid, row.get("didRadiantWin"), start, start + duration if start and duration else None, path)
    for name in ("stratz_team_matches.json", "prematch_live_delta.json"):
        path = directory / name
        data = json.loads(path.read_text())
        if name == "prematch_live_delta.json":
            pairs = data.get("maps", {}).items()
        else:
            pairs = ((r["match_id"], r) for v in data.values() if isinstance(v, dict) for r in v.get("matches", []))
        for mid, row in pairs:
            add(mid, row.get("radiant_won"), row.get("start"), row.get("end"), path)
    target = directory / "source_labels.json"
    temporary = target.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(labels, indent=2) + "\n")
    temporary.replace(target)


def summary(rows):
    n = len(rows)
    if not n:
        return {"n": 0}
    wins = sum(r["won"] for r in rows)
    p = wins / n
    z = 1.96
    den = 1 + z * z / n
    center = (p + z * z / (2 * n)) / den
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / den
    return {
        "n": n, "wins": wins, "winrate": p,
        "wilson95": [center - half, center + half],
        "mean_raw_confidence": sum(r["confidence"] for r in rows) / n,
        "mean_logged_expected_wr": sum(r["expected_wr"] for r in rows) / n,
        "brier": sum((r["confidence"] - r["won"]) ** 2 for r in rows) / n,
        "logloss": -sum(math.log(max(1e-12, r["confidence"] if r["won"]
                                    else 1 - r["confidence"])) for r in rows) / n,
    }


def recent_cards(directory):
    """Separate rolling display cache; never merge it into the sent cohort."""
    verdicts = json.loads((directory / "map_verdicts.json").read_text())
    cache = json.loads((directory / "stratz_team_matches.json").read_text())
    outcomes = {str(r["match_id"]): r for v in cache.values() if isinstance(v, dict)
                for r in v.get("matches", [])}
    outcomes.update(json.loads((directory / "prematch_live_delta.json").read_text())["maps"])
    first = {}
    for value in sorted(verdicts.values(), key=lambda r: r.get("first_seen_ts", 0)):
        mid = str(value.get("match_id") or "")
        found = re.search(r"🤖 ML-модель: (Radiant|Dire) ([0-9.]+)%", value.get("bet_message") or "")
        outcome = outcomes.get(mid, {})
        if mid in first or not found or not isinstance(outcome.get("radiant_won"), bool):
            continue
        first[mid] = {"match_id": mid, "teams": value.get("teams"),
                      "confidence": float(found[2]) / 100, "side": found[1],
                      "won": (found[1] == "Radiant") == outcome["radiant_won"]}
    rows = list(first.values())
    return {"n": len(rows), "wins": sum(r["won"] for r in rows), "rows": rows}


def audit(directory):
    names = ["prematch_model_bet_sent.jsonl", "prematch_model_eval.jsonl", "source_labels.json"]
    bets, evaluations = (load_rows(directory / name) for name in names[:2])
    labels = json.loads((directory / names[2]).read_text())
    first = first_per_map(bets)
    rows, excluded = [], collections.defaultdict(list)
    for mid, bet in first.items():
        label = labels.get(mid)
        if label is None:
            excluded["missing_label"].append(mid)
            continue
        if not isinstance(label.get("radiant_win"), bool):
            raise ValueError("Outcome must be boolean: " + mid)
        if not label.get("end") or bet["ts"] >= label["end"]:
            excluded["missing_end_or_prediction_after_finish"].append(mid)
            continue
        if bet["side"] not in ("radiant", "dire"):
            raise ValueError("Unknown prediction side: " + mid)
        candidates = [r for r in evaluations
                      if r.get("radiant_team") == bet["radiant_team"]
                      and r.get("dire_team") == bet["dire_team"]
                      and abs(r["index"] - bet["index"]) < 0.002
                      and 0 <= bet["ts"] - r["ts"] <= 300]
        # Never choose a convenient attribution when this join is ambiguous.
        ev = candidates[0] if len(candidates) == 1 else {}
        row = dict(bet, match_id=mid, radiant_win=label["radiant_win"],
                   won=(bet["side"] == "radiant") == label["radiant_win"],
                   source=label.get("source"), source_start=label.get("start"),
                   source_end=label["end"], eval_candidates=len(candidates),
                   branch=ev.get("branch", "unknown"), parts=ev.get("parts", {}),
                   seconds_after_start=bet["ts"] - label["start"] if label.get("start") else None)
        elo = bet.get("model_elo")
        row["account_elo_side"] = ("radiant" if elo > 0 else "dire") if elo else None
        rows.append(row)
    bins = {}
    for lo, hi in [(0.5, 0.6), (0.6, 0.7), (0.7, 0.8), (0.8, 0.9), (0.9, 1.01)]:
        bins[f"{lo:.1f}-{hi:.2f}"] = summary([r for r in rows if lo <= r["confidence"] < hi])
    paired = [r for r in rows if r["account_elo_side"]]
    disagreements = [r for r in paired if r["side"] != r["account_elo_side"]]
    attributed = [r for r in rows if r["parts"]]
    shares = collections.defaultdict(list)
    for r in attributed:
        total = sum(abs(v) for v in r["parts"].values())
        for key in ("elo", "draft", "players", "h2h"):
            shares[key].append(abs(r["parts"].get(key, 0)) / total if total else 0)
    report = {
        "input_sha256": {name: hashlib.sha256((directory / name).read_bytes()).hexdigest() for name in names},
        "bet_rows": len(bets), "unique_maps": len(first), "duplicate_rows": len(bets) - len(first),
        "excluded": dict(excluded), "overall": summary(rows), "confidence_bins": bins,
        "since_september_1": summary([r for r in rows if r["ts"] >= 1788220800]),
        "since_september_8": summary([r for r in rows if r["ts"] >= 1788825600]),
        "account_elo_direction_comparison": {
            "n": len(paired), "model_wins": sum(r["won"] for r in paired),
            "elo_wins": sum((r["account_elo_side"] == "radiant") == r["radiant_win"] for r in paired),
            "agreement_n": len(paired) - len(disagreements),
            "disagreement_n": len(disagreements), "model_wins_on_disagreement": sum(r["won"] for r in disagreements),
        },
        "attribution_n": len(attributed),
        "mean_absolute_group_share": {k: sum(v) / len(v) for k, v in shares.items()},
        "elo_group_largest_n": sum(max(r["parts"], key=lambda k: abs(r["parts"][k])) == "elo" for r in attributed),
        "by_branch": {b: summary([r for r in rows if r["branch"] == b]) for b in sorted({r["branch"] for r in rows})},
        "timing": {"after_start_5min_n": sum((r["seconds_after_start"] or 0) > 300 for r in rows),
                   "max_seconds_after_start": max((r["seconds_after_start"] for r in rows if r["seconds_after_start"] is not None), default=None)},
        "high_confidence_losses": [{k: r[k] for k in ("match_id", "radiant_team", "dire_team", "confidence", "side", "branch", "ts")}
                                   for r in rows if r["confidence"] >= 0.8 and not r["won"]],
        "rows": rows,
    }
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--corpus", type=Path, help="Build labels from raw 7.41e shards and saved source caches")
    args = parser.parse_args()
    if args.corpus:
        extract_labels(args.directory, args.corpus)
    report = audit(args.directory)
    if (args.directory / "map_verdicts.json").exists():
        report["recent_cards"] = recent_cards(args.directory)
    target = args.directory / "audit.json"
    temporary = target.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    temporary.replace(target)
    print(json.dumps({k: v for k, v in report.items() if k != "rows"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
