"""Score a public-trained draft model on PRO maps it never saw.

Public matches are what the win model is fit on, but pro maps are what the
signal is for, so transfer is the metric that decides whether a retrain is an
improvement.  Nothing here reads anything but the ten hero IDs and the outcome.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np


@dataclass(frozen=True)
class ProDraft:
    map_id: int
    start_time: int
    radiant_win: int
    heroes: tuple[int, ...]


def parse_pro_draft(raw: Any) -> ProDraft | None:
    """Ten hero IDs in R1..R5, D1..D5 order plus the outcome, or None.

    Deliberately looser than the public canonicalizer: pro records carry no
    reliable kill timeline or duration, and the win model needs neither.
    """
    if not isinstance(raw, dict):
        return None
    win = raw.get("didRadiantWin")
    players = raw.get("players")
    start = raw.get("startDateTime")
    map_id = raw.get("id")
    if not isinstance(win, bool) or not isinstance(players, list) or len(players) != 10:
        return None
    if not isinstance(start, int) or not isinstance(map_id, int) or map_id <= 0:
        return None
    sides: dict[bool, dict[int, int]] = {True: {}, False: {}}
    seen: set[int] = set()
    for player in players:
        if not isinstance(player, dict):
            return None
        side, position, hero = player.get("isRadiant"), player.get("position"), player.get("heroId")
        if not isinstance(side, bool) or not isinstance(position, str) or not position.startswith("POSITION_"):
            return None
        if isinstance(hero, bool) or not isinstance(hero, int):
            return None
        try:
            slot = int(position.removeprefix("POSITION_"))
        except ValueError:
            return None
        if slot not in range(1, 6) or not 0 < hero < 65536:
            return None
        if slot in sides[side] or hero in seen:
            return None
        sides[side][slot] = hero
        seen.add(hero)
    if any(set(sides[s]) != set(range(1, 6)) for s in (True, False)):
        return None
    heroes = tuple(sides[s][p] for s in (True, False) for p in range(1, 6))
    return ProDraft(map_id, start, int(win), heroes)


def load_pro_drafts(paths: Iterable[Path]) -> tuple[list[ProDraft], dict[str, int]]:
    drafts: list[ProDraft] = []
    seen: set[int] = set()
    audit = {"files": 0, "records": 0, "accepted": 0, "duplicate_map_id": 0, "rejected": 0, "file_error": 0}
    for path in sorted(Path(p) for p in paths):
        audit["files"] += 1
        try:
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            audit["file_error"] += 1
            continue
        records = payload.values() if isinstance(payload, dict) else payload
        if not isinstance(records, (list, type({}.values()))):
            audit["file_error"] += 1
            continue
        for raw in records:
            audit["records"] += 1
            draft = parse_pro_draft(raw)
            if draft is None:
                audit["rejected"] += 1
            elif draft.map_id in seen:
                audit["duplicate_map_id"] += 1
            else:
                seen.add(draft.map_id)
                drafts.append(draft)
    drafts.sort(key=lambda d: (d.start_time, d.map_id))
    audit["accepted"] = len(drafts)
    return drafts, audit


def transfer_metrics(drafts: list[ProDraft], probability: np.ndarray) -> dict[str, Any]:
    """AUC plus the decile spread, which is what a bet actually trades on."""
    from sklearn.metrics import roc_auc_score

    y = np.asarray([d.radiant_win for d in drafts])
    p = np.asarray(probability, dtype=float)
    out: dict[str, Any] = {"rows": int(len(y)), "base_rate": float(np.mean(y)) if len(y) else None}
    if len(y) == 0 or len(np.unique(y)) < 2:
        out.update(auc=None, auc_ci95=None, accuracy=None, top_decile_rate=None, bottom_decile_rate=None)
        return out
    auc = float(roc_auc_score(y, p))
    smaller = int(min((y == 1).sum(), (y == 0).sum()))
    out["auc"] = auc
    out["auc_ci95"] = float(1.96 * np.sqrt(auc * (1 - auc) / smaller)) if smaller else None
    out["accuracy"] = float(np.mean((p >= 0.5) == (y == 1)))
    order = np.argsort(p, kind="stable")
    decile = max(1, len(p) // 10)
    out["top_decile_rate"] = float(np.mean(y[order[-decile:]]))
    out["bottom_decile_rate"] = float(np.mean(y[order[:decile]]))
    out["decile_spread"] = out["top_decile_rate"] - out["bottom_decile_rate"]
    return out


def score_model(drafts: list[ProDraft], encoder: Any, model: Any) -> np.ndarray:
    if not drafts:
        return np.empty(0, dtype=float)
    heroes = np.asarray([d.heroes for d in drafts], dtype=np.int64)
    return model.predict_proba(encoder.transform(heroes))[:, 1]
