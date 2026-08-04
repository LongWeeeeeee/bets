#!/usr/bin/env python3
"""Retrain the public draft model on fresh data and gate the promotion.

Hero balance drifts with every patch, and the win model decays with it, so the
retrain cadence matters more than any further tweak to the design.  This is the
one command to run when a patch lands.

The comparison is the delicate part.  Scoring a challenger on its own test split
and the champion on the same rows is unfair whenever the champion's fit window
reaches into those rows -- it grades the champion on data it memorised.  Only
matches strictly newer than *both* fit windows are eligible, and the report says
so explicitly rather than quietly averaging the two regimes.

Nothing is deleted, moved or switched automatically: the verdict is printed and
written to refresh_report.json, and promoting stays a human decision.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import joblib
import numpy as np

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR / "base"))

try:
    from base.evaluate_pro_transfer import load_pro_drafts, score_model, transfer_metrics
    from base.train_public_draft_hero10_experiment import (
        chronological_split, scan_public, train_experiment,
    )
except ImportError:  # Direct ``python base/refresh_public_draft_model.py``.
    from evaluate_pro_transfer import load_pro_drafts, score_model, transfer_metrics  # type: ignore
    from train_public_draft_hero10_experiment import (  # type: ignore
        chronological_split, scan_public, train_experiment,
    )

EXPERIMENT_DIR = ROOT_DIR / "data/public_draft_hero10_experiment"
PUBLIC_DIR = ROOT_DIR / "bets_data/analise_pub_matches/json_parts_split_from_object"
PRO_DIR = ROOT_DIR / "pro_heroes_data/json_parts_split_from_object"
REMOTE = "serv1"
REMOTE_PUBLIC = "/root/main/bets_data/analise_pub_matches/json_parts_split_from_object/"
REMOTE_PRO = "/root/main/pro_heroes_data/json_parts_split_from_object/"
REQUIRED_ARTIFACTS = ("results.json", "radiant_win_model.joblib", "win_feature_encoder.joblib")
MIN_PRO_ROWS = 500


def sync_from_serv1(dry_run: bool = False) -> dict[str, Any]:
    """Pull new corpus parts.  Never --delete: serv1 truncates de-duplicated
    files to stubs, and mirroring that would drop matches held only locally."""
    report: dict[str, Any] = {}
    for name, remote, local in (("public", REMOTE_PUBLIC, PUBLIC_DIR), ("pro", REMOTE_PRO, PRO_DIR)):
        local.mkdir(parents=True, exist_ok=True)
        command = ["rsync", "-az", "--partial", "--stats", f"{REMOTE}:{remote}", f"{local}/"]
        if dry_run:
            command.insert(1, "--dry-run")
        finished = subprocess.run(command, capture_output=True, text=True)
        report[name] = {"returncode": finished.returncode, "detail": _transfer_line(finished)}
    return report


def _transfer_line(finished: subprocess.CompletedProcess) -> str:
    """rsync 3.x says "regular files transferred", openrsync on macOS does not.

    Falling through to an empty string would make "nothing to sync" and "we
    failed to read the output" look identical in the report.
    """
    for prefix in ("Number of regular files transferred", "Number of files transferred"):
        for line in finished.stdout.splitlines():
            if line.strip().startswith(prefix):
                return line.strip()
    if finished.stderr.strip():
        return finished.stderr.strip()[:200]
    tail = [line for line in finished.stdout.splitlines() if line.strip()][-1:]
    return f"неразобранный вывод rsync: {tail[0].strip()[:160]}" if tail else "rsync ничего не вывел"


def fit_end(results: dict[str, Any]) -> int:
    """Last match the shipped model was fitted on (train+validation)."""
    return int(results["split_boundaries"]["validation"]["last"]["start_time"])


def find_champion(explicit: Path | None) -> Path | None:
    if explicit is not None:
        return explicit
    candidates = [
        d for d in sorted(EXPERIMENT_DIR.iterdir() if EXPERIMENT_DIR.exists() else [])
        if d.is_dir() and all((d / name).exists() for name in REQUIRED_ARTIFACTS)
    ]
    return candidates[-1] if candidates else None


def verdict(public_delta: float | None, pro_delta: float | None, pro_rows: int,
            min_pro_rows: int = MIN_PRO_ROWS) -> dict[str, Any]:
    """PROMOTE only when the challenger is not worse where the data can tell."""
    reasons: list[str] = []
    if public_delta is None:
        return {"decision": "HOLD", "reasons": ["нет пригодных публичных карт для сравнения"]}
    ok = public_delta >= 0
    reasons.append(f"публичное окно: ΔAUC {public_delta:+.4f}")
    if pro_rows >= min_pro_rows and pro_delta is not None:
        reasons.append(f"про-окно ({pro_rows} карт): ΔAUC {pro_delta:+.4f}")
        ok = ok and pro_delta >= 0
    else:
        reasons.append(f"про-окно пропущено: {pro_rows} карт < {min_pro_rows}, статистики не хватает")
    return {"decision": "PROMOTE" if ok else "HOLD", "reasons": reasons}


def _load(directory: Path) -> tuple[Any, Any, dict[str, Any]]:
    encoder = joblib.load(directory / "win_feature_encoder.joblib")
    model = joblib.load(directory / "radiant_win_model.joblib")
    results = json.loads((directory / "results.json").read_text())
    return encoder, model, results


def _public_auc(matches, encoder, model) -> float | None:
    from sklearn.metrics import roc_auc_score

    if len(matches) < 100:
        return None
    y = np.asarray([m.radiant_win for m in matches])
    if len(np.unique(y)) < 2:
        return None
    heroes = np.asarray([m.heroes for m in matches], dtype=np.int64)
    return float(roc_auc_score(y, model.predict_proba(encoder.transform(heroes))[:, 1]))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--champion", type=Path, default=None, help="каталог текущей модели (по умолчанию — свежайший)")
    parser.add_argument("--challenger", type=Path, default=None,
                        help="сравнить готовый каталог вместо обучения нового")
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--no-sync", action="store_true", help="не тянуть свежие данные с serv1")
    parser.add_argument("--dry-run-sync", action="store_true")
    parser.add_argument("--min-pro-rows", type=int, default=MIN_PRO_ROWS)
    args = parser.parse_args()

    report: dict[str, Any] = {"started": dt.datetime.now().isoformat(timespec="seconds")}
    if not args.no_sync:
        print("sync с serv1…", flush=True)
        report["sync"] = sync_from_serv1(dry_run=args.dry_run_sync)
        print(json.dumps(report["sync"], indent=2, ensure_ascii=False), flush=True)

    champion_dir = find_champion(args.champion)
    if champion_dir is None:
        print("чемпиона нет — первый прогон, ворота пропускаются", flush=True)
    else:
        print(f"чемпион: {champion_dir.name}", flush=True)

    print(f"скан корпуса {PUBLIC_DIR}…", flush=True)
    matches, audit = scan_public(PUBLIC_DIR.glob("*.json"))
    print(f"карт принято: {len(matches):,}", flush=True)

    if args.challenger is not None:
        output_dir = args.challenger
        challenger_results = json.loads((output_dir / "results.json").read_text())
        print(f"претендент взят готовым: {output_dir.name}", flush=True)
    else:
        output_dir = args.output_dir or EXPERIMENT_DIR / f"{dt.date.today().isoformat()}_all_public_refresh"
        print(f"обучение → {output_dir}…", flush=True)
        challenger_results = train_experiment(matches, output_dir)
        (output_dir / "audit.json").write_text(json.dumps(audit, indent=2), encoding="utf-8")
    report["challenger"] = {"dir": str(output_dir), "counts": challenger_results["counts"],
                            "test": challenger_results["models"]["radiant_win"]}

    if champion_dir is not None:
        champion_encoder, champion_model, champion_results = _load(champion_dir)
        challenger_encoder, challenger_model, _ = _load(output_dir)
        champion_end, challenger_end = fit_end(champion_results), fit_end(challenger_results)

        _, _, test_matches = chronological_split(matches)
        eligible_public = [m for m in test_matches if m.start_time > champion_end]
        champion_public = _public_auc(eligible_public, champion_encoder, champion_model)
        challenger_public = _public_auc(eligible_public, challenger_encoder, challenger_model)

        cutoff = max(champion_end, challenger_end)
        pro_drafts, pro_audit = load_pro_drafts(PRO_DIR.glob("*_part*.json"))
        eligible_pro = [d for d in pro_drafts if d.start_time > cutoff]
        champion_pro = transfer_metrics(eligible_pro, score_model(eligible_pro, champion_encoder, champion_model))
        challenger_pro = transfer_metrics(eligible_pro, score_model(eligible_pro, challenger_encoder, challenger_model))

        public_delta = None if None in (champion_public, challenger_public) else challenger_public - champion_public
        pro_delta = (None if None in (champion_pro.get("auc"), challenger_pro.get("auc"))
                     else challenger_pro["auc"] - champion_pro["auc"])
        report["comparison"] = {
            "champion_dir": champion_dir.name,
            "champion_fit_end": champion_end,
            "challenger_fit_end": challenger_end,
            "eligible_public_rows": len(eligible_public),
            "champion_public_auc": champion_public,
            "challenger_public_auc": challenger_public,
            "eligible_pro_rows": len(eligible_pro),
            "champion_pro": champion_pro,
            "challenger_pro": challenger_pro,
            "pro_audit": pro_audit,
        }
        report["verdict"] = verdict(public_delta, pro_delta, len(eligible_pro), args.min_pro_rows)

        print(f"\nсравнение только на матчах свежее обоих обучающих окон")
        print(f"  публичных карт: {len(eligible_public):,}  чемпион AUC {champion_public}  претендент {challenger_public}")
        print(f"  про-карт:       {len(eligible_pro):,}  чемпион AUC {champion_pro.get('auc')}  претендент {challenger_pro.get('auc')}")
        print(f"\nВЕРДИКТ: {report['verdict']['decision']}")
        for reason in report["verdict"]["reasons"]:
            print(f"  — {reason}")
        print("\nничего не переключено: промоут — ручное решение")

    (output_dir / "refresh_report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nотчёт: {output_dir / 'refresh_report.json'}")


if __name__ == "__main__":
    main()
