"""E281 side/total >=X kills probabilities.

Star-panel rendering (:func:`forecast_text`/:func:`render`) remains purely
informational. Since the owner rule of 13.09.2026 (E-289 addendum), the raw
:func:`forecast_probabilities` numbers are ALSO read by ``win_model_veto``'s
adapter and stashed for ``ml_dispatch``'s ``kills_total`` gate
(``ML_DISPATCH_KILLS_TOTAL_GATE``, see ``base/ml_dispatch.py``); the star
threshold here (``KILLS_TRANSFER_STAR_MIN_PROB``) is independent of that gate.
"""
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


STAR_MIN_PROB_ENV = "KILLS_TRANSFER_STAR_MIN_PROB"
# Порог « ★» у информационных строк: на про-тесте E-281 (687 карт, 23 дня,
# runtime/artifacts/kills/relative_transfer/calibration_pro_test_2026-09-13.txt)
# при P >= 0.70 событие сбывается в 80.6% (карта >= 55) и 76.4% (сторона >= 30);
# при 0.60 — 75.0% / 69.3%, но ★ горела бы на 62% карт по тоталу. Маркер
# информационный: в диспетчер ставок эти строки не входят.
DEFAULT_STAR_MIN_PROB = 0.70


def star_min_prob():
    """Читается при каждом вызове: systemd drop-in / monkeypatch применяются без перезагрузки модуля."""
    raw = os.environ.get(STAR_MIN_PROB_ENV, "")
    try:
        value = float(raw) if raw.strip() else DEFAULT_STAR_MIN_PROB
    except (TypeError, ValueError):
        value = DEFAULT_STAR_MIN_PROB
    return value if 0.0 < value <= 1.0 else DEFAULT_STAR_MIN_PROB


def render(probabilities, history_date, star_min=None):
    rad, dire, total = probabilities
    threshold = star_min_prob() if star_min is None else float(star_min)

    def line(label, value):
        return f"{label}: {value:.1%}" + (" ★" if value >= threshold else "")

    return "\n".join((f"Килы ML · E-281 (история до {history_date})",
                      line("Radiant ≥30 килов", rad),
                      line("Dire ≥30 килов", dire),
                      line("Карта ≥55 килов", total)))


@lru_cache(maxsize=2)
def _bundle(directory):
    return KillsServing(directory)


def forecast_probabilities(rh, dh, ra, da, teams, start, mid):
    """Numeric core of :func:`forecast_text`: (P(Radiant>=30), P(Dire>=30), P(map>=55)).

    Split out (E-281 kills_total gate, owner rule 13.09.2026) so
    ``win_model_veto``'s adapter can stash the raw numbers for
    ``ml_dispatch`` without re-deriving them from rendered text.
    """
    directory = os.environ.get("KILLS_TRANSFER_BUNDLE", str(DEFAULT_BUNDLE))
    serving = _bundle(directory)
    return serving.predict(tuple(rh) + tuple(dh), tuple(ra) + tuple(da),
                           tuple(teams), int(start), int(mid))


def manifest_history_date(directory=None):
    """Cheap manifest-only history-date lookup (no CatBoost/joblib load).

    Used by the ``win_model_veto`` adapter to build :func:`render`'s text
    from :func:`forecast_probabilities` without instantiating the full
    :class:`KillsServing` bundle a second time just for one date string.
    """
    root = Path(directory) if directory else Path(
        os.environ.get("KILLS_TRANSFER_BUNDLE", str(DEFAULT_BUNDLE)))
    return json.loads((root / "manifest.json").read_text())["history_date"]


def forecast_text(rh, dh, ra, da, teams, start, mid):
    probabilities = forecast_probabilities(rh, dh, ra, da, teams, start, mid)
    return render(probabilities, manifest_history_date())
