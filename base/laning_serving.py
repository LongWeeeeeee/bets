"""Display-only team gold-at-10 prediction and standalone All model line."""
from __future__ import annotations

from collections import OrderedDict
import hashlib
import logging
import os
from pathlib import Path
import threading

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = Path(os.getenv("LANING_MODEL_DIR", str(
    ROOT / "data/laning_models/20260909_team_nw10_v1/selected")))
HISTORY_DIR = Path(os.getenv("LANING_HISTORY_DIR", str(
    ROOT / "data/laning_history/20260909_stratz_v1")))
ENABLED = os.getenv("LANING_MODEL_ENABLED", "1") == "1"
LOG = logging.getLogger(__name__)


class LaningService:
    """Lazy immutable model/history; cache includes the actual query timestamp."""

    def __init__(self, model_dir=MODEL_DIR, history_dir=HISTORY_DIR):
        self.model_dir, self.history_dir = Path(model_dir), Path(history_dir)
        self.model = self.history_store = None
        self.error = None
        self.loaded = False
        self.lock = threading.RLock()
        self.cache = OrderedDict()

    def _load(self):
        if self.loaded:
            return self.model is not None
        try:
            try:
                from team_laning_model import TeamLaningModel
                from laning_history_store import LaningHistoryStore
            except ImportError:
                from base.team_laning_model import TeamLaningModel
                from base.laning_history_store import LaningHistoryStore
            model = TeamLaningModel.load(self.model_dir)
            history = LaningHistoryStore(self.history_dir)
            self.model, self.history_store = model, history
            digest = hashlib.sha256((self.model_dir / "team.cbm").read_bytes()).hexdigest()
            LOG.warning("ML Laning loaded: model=%s sha256=%s history_max_end=%s",
                        self.model_dir, digest, history.manifest["max_end_ts"])
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"
            self.model = self.history_store = None
            LOG.warning("ML Laning unavailable: %s", self.error)
        self.loaded = True
        return self.model is not None

    def predict(self, heroes, accounts, timestamp):
        """Return [Dire, exact tie, Radiant], or None on isolated failure."""
        if not ENABLED:
            return None
        with self.lock:
            if not self._load():
                return None
            try:
                # The store validates shape, finite/integer IDs, and cutoff.
                key = (tuple(heroes), tuple(accounts), float(timestamp))
                if key in self.cache:
                    self.cache.move_to_end(key)
                    return self.cache[key].copy()
                history = None
                if getattr(self.model, "with_history", True):
                    history = self.history_store.history(heroes, accounts, timestamp,
                                                         **self.model.history_config)[None, :, :]
                probability = np.asarray(self.model.predict_proba(
                    np.asarray([heroes]), history), dtype=float)
                if (probability.shape != (1, 3) or not np.isfinite(probability).all()
                        or np.any(probability < 0) or np.any(probability > 1)
                        or not np.isclose(probability.sum(), 1, atol=1e-8)):
                    raise ValueError("Invalid team-NW10 probability")
                self.cache[key] = probability[0].copy()
                if len(self.cache) > 256:
                    self.cache.popitem(last=False)
                return probability[0].copy()
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                if error != self.error:
                    LOG.warning("ML Laning prediction unavailable: %s", error)
                self.error = error
                return None


_SERVICE = LaningService()


def panel_lines(radiant_dict, dire_dict, timestamp, *, draft_model):
    """Build fresh per-match strings; each estimate can fail independently.

    All uses the existing WIN_MODEL_DIR reader and its hero-keyed cache. Its
    standalone draft probability is never recovered from an ensemble index.
    """
    result = {"ml_laning_line": "", "all_model_line": ""}
    try:
        index = draft_model.win_index_draft(radiant_dict, dire_dict)
        if index is not None and np.isfinite(index) and -50 <= index <= 50:
            side = "Radiant" if index >= 0 else "Dire"
            result["all_model_line"] = (
                f"🌐 All ML-модель: {side} {50 + abs(index):.1f}%")
    except Exception:
        pass
    try:
        heroes = draft_model._heroes_vector(radiant_dict, dire_dict)
        if heroes is None:
            return result
        accounts = [
            (side.get(f"pos{position}") or {}).get("account_id", 0) or 0
            for side in (radiant_dict, dire_dict) for position in range(1, 6)
        ]
        probability = _SERVICE.predict(heroes, accounts, timestamp)
        if probability is not None:
            winner = int(np.argmax(probability))
            side = ("Dire", "Равенство", "Radiant")[winner]
            result["ml_laning_line"] = (
                f"ML Laning: {side} {probability[winner] * 100:.1f}% (золото, 10 мин)")
    except Exception:
        pass
    return result
