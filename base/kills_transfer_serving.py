"""Informational E281 probabilities; never participates in dispatch gates."""
from __future__ import annotations

from functools import lru_cache
import json
import os
from pathlib import Path

import joblib
import numpy as np
from catboost import CatBoostClassifier

from kills_public_transfer import predict_public
from kills_transfer_data import sha256
from kills_transfer_features import causal_pro_context
from train_kills_transfer import calibrate_predict, collapse, feature_frame

DEFAULT_BUNDLE = Path(__file__).resolve().parents[1] / "data/kills_transfer_serving"


def query_context(history, heroes, accounts, teams, start, mid):
    """Select only relevant completed histories, then reuse training code.

    Removing maps without any requested identity cannot change that identity's
    recent 30 observations. Current-map outcomes are explicitly excluded.
    """
    known_accounts = [x for x in accounts if x > 0]
    known_teams = [x for x in teams if x > 0]
    relevant = (np.isin(history["accounts"], known_accounts).any(1)
                | np.isin(history["teams"], known_teams).any(1))
    keep = relevant & (history["ends"] < start) & (history["mids"] != mid)
    query = {"mids": mid, "ts": start, "ends": start + 1, "durations": 1,
             "heroes": heroes, "accounts": accounts, "teams": teams,
             "sids": 0, "stats": np.full((10, 6), np.nan)}
    rows = {key: np.concatenate((value[keep], np.asarray([query[key]], dtype=value.dtype)))
            for key, value in history.items() if key in query}
    context, names = causal_pro_context(rows)
    return context[-1:], names


class KillsServing:
    def __init__(self, directory):
        root = Path(directory)
        self.manifest = json.loads((root / "manifest.json").read_text())
        for name, digest in self.manifest["files"].items():
            if Path(name).name != name or sha256(root / name) != digest:
                raise ValueError(f"Kills bundle integrity failure: {name}")
        with np.load(root / "history.npz", allow_pickle=False) as archive:
            self.history = {key: archive[key] for key in archive.files}
        self.profiles = joblib.load(root / "profiles.joblib")
        self.public = joblib.load(root / "public_models.joblib")
        self.models, self.calibrators = {}, {}
        for target in ("total", "side"):
            arm = self.manifest["selected"][target]
            model = CatBoostClassifier()
            model.load_model(str(root / f"{target}_{arm}.cbm"))
            self.models[target] = model
            self.calibrators[target] = joblib.load(root / f"{target}_{arm}_calibration.joblib")

    @lru_cache(maxsize=256)
    def predict(self, heroes, accounts, teams, start, mid):
        if (len(heroes) != 10 or len(set(heroes)) != 10 or min(heroes) <= 0
                or len(accounts) != 10 or len(teams) != 2 or start <= 0 or mid <= 0):
            raise ValueError("Incomplete kills forecast identity or draft")
        context, names = query_context(self.history, heroes, accounts, teams, start, mid)
        heroes_array = np.asarray([heroes], dtype=np.int64)
        scores = predict_public(self.public, heroes_array)
        result = {}
        for target in ("total", "side"):
            frame = feature_frame(heroes_array, context, names, scores, self.profiles,
                                  self.manifest["selected"][target])
            model = self.models[target]
            if list(frame.columns) != list(model.feature_names_):
                raise ValueError("Kills serving feature contract differs from training")
            raw = model.predict_proba(frame, thread_count=1)[:, 1]
            result[target] = calibrate_predict(self.calibrators[target], collapse(raw, 1, target))
        values = tuple(float(x) for x in (*result["side"], result["total"][0]))
        if not all(np.isfinite(x) and 0 <= x <= 1 for x in values):
            raise ValueError("Invalid kills probabilities")
        return values


def render(probabilities, history_date):
    rad, dire, total = probabilities
    return (f"Килы ML · E-281 (история до {history_date})\n"
            f"Radiant ≥30 килов: {rad:.1%}\n"
            f"Dire ≥30 килов: {dire:.1%}\n"
            f"Карта ≥55 килов: {total:.1%}")


@lru_cache(maxsize=2)
def _bundle(directory):
    return KillsServing(directory)


def forecast_text(rh, dh, ra, da, teams, start, mid):
    directory = os.environ.get("KILLS_TRANSFER_BUNDLE", str(DEFAULT_BUNDLE))
    serving = _bundle(directory)
    probabilities = serving.predict(tuple(rh) + tuple(dh), tuple(ra) + tuple(da),
                                    tuple(teams), int(start), int(mid))
    return render(probabilities, serving.manifest["history_date"])
