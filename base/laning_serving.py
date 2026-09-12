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


def _dispatch_min_conf():
    """``ML_DISPATCH_MIN_CONF`` read at format time (default 0.60, see ml_dispatch.py)."""
    try:
        return float(os.getenv("ML_DISPATCH_MIN_CONF", "0.60"))
    except (TypeError, ValueError):
        return 0.60


def panel_lines(radiant_dict, dire_dict, timestamp, *, draft_model):
    """Build fresh per-match strings; each estimate can fail independently.

    All uses the existing WIN_MODEL_DIR reader (draft_model.win_index_draft,
    a draft-phase ensemble index, E-260 "All >=20") and its hero-keyed cache.
    Its standalone draft probability is never recovered from an ensemble
    index, and is NOT the 35-feature prematch model (that one lives in
    cyberscore_try/win_model_veto and is keyed by SOURCE_PREMATCH).

    Each line gets a " ★" suffix when the confidence shown in that
    same line is >= ML_DISPATCH_MIN_CONF (default 0.60), read at format
    time so systemd env overrides apply without a restart-free reload.
    """
    result = {"ml_laning_line": "", "all_model_line": ""}
    min_conf = _dispatch_min_conf()
    try:
        index = draft_model.win_index_draft(radiant_dict, dire_dict)
        if index is not None and np.isfinite(index) and -50 <= index <= 50:
            side = "Radiant" if index >= 0 else "Dire"
            confidence = (50 + abs(index)) / 100.0
            star = " ★" if confidence >= min_conf else ""
            result["all_model_line"] = (
                f"🌐 All ML-модель: {side} {50 + abs(index):.1f}%{star}")
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
            confidence = float(probability[winner])
            star = " ★" if confidence >= min_conf else ""
            result["ml_laning_line"] = (
                f"ML Laning: {side} {probability[winner] * 100:.1f}% (золото, 10 мин){star}")
    except Exception:
        pass
    return result


def verdicts(radiant_dict, dire_dict, timestamp, *, draft_model):
    """Export the two model verdicts also shown by :func:`panel_lines`.

    Returns ``{"all": {"side", "confidence"}|None, "lane": {"side",
    "confidence", "p_tie"}|None}`` for ``ml_dispatch.Ctx.all``/``Ctx.lane``
    (stage 2 wraps these dicts into ``ml_dispatch.ModelVerdict``). Sides are
    strictly "Radiant"/"Dire" (never "tie"), matching the ml_dispatch
    contract, even though "lane" reports a tie-aware ML Laning model:

    - "all": same value as ``all_model_line`` above (draft_model.win_index_draft,
      NOT the prematch index) — side by sign of the index, confidence =
      (50 + |index|) / 100.
    - "lane": side is whichever of Radiant/Dire has the higher of the two
      non-tie probabilities (ignoring whether the tie class is actually the
      argmax, unlike the printed ``ml_laning_line``, which can read
      "Равенство"); confidence is that side's own probability; "p_tie" is
      the tie-class probability, exposed separately so a caller can decide
      how to treat near-toss-up predictions.
    """
    result = {"all": None, "lane": None}
    try:
        index = draft_model.win_index_draft(radiant_dict, dire_dict)
        if index is not None and np.isfinite(index) and -50 <= index <= 50:
            side = "Radiant" if index >= 0 else "Dire"
            result["all"] = {"side": side, "confidence": (50 + abs(index)) / 100.0}
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
            dire_p, tie_p, radiant_p = (float(probability[0]), float(probability[1]), float(probability[2]))
            if radiant_p >= dire_p:
                result["lane"] = {"side": "Radiant", "confidence": radiant_p, "p_tie": tie_p}
            else:
                result["lane"] = {"side": "Dire", "confidence": dire_p, "p_tie": tie_p}
    except Exception:
        pass
    return result


def fallback_verdicts(radiant_dict, dire_dict, *, draft_model):
    """Early NW / Early Win / Late verdicts from heroes alone.

    Used when the 35-feature prematch model refuses (owner decision
    12.09.2026 20:10 MSK: "даже при отказе предматчевой модели я хочу видеть
    all late early nw early win"). Reuses the SAME heroes vector as ``all``
    above (``draft_model._heroes_vector``) — the standalone All line and this
    fallback are never out of sync. Each of the three sibling modules keeps
    its own by-heroes cache internally, so calling this every tick for the
    same map costs no extra joblib work. Fail-soft per model: one broken
    bundle (missing artifact, *_MODEL_ENABLED=0, encoder mismatch) never
    blocks the other two, mirroring their own ``verdict()`` contract.
    """
    result = {"early_nw": None, "early_win": None, "late": None}
    try:
        heroes = draft_model._heroes_vector(radiant_dict, dire_dict)
    except Exception:
        heroes = None
    if heroes is None:
        return result
    try:
        try:
            import early_nw_win_model as _enwm
        except ImportError:
            from base import early_nw_win_model as _enwm
        result["early_nw"] = _enwm.verdict(heroes)
    except Exception:
        result["early_nw"] = None
    try:
        try:
            import early_win_model as _ewm
        except ImportError:
            from base import early_win_model as _ewm
        result["early_win"] = _ewm.verdict(heroes)
    except Exception:
        result["early_win"] = None
    try:
        try:
            import late_win_model as _lwm
        except ImportError:
            from base import late_win_model as _lwm
        result["late"] = _lwm.verdict(heroes)
    except Exception:
        result["late"] = None
    return result


def refusal_warning_line(refusal, radiant_dict, dire_dict):
    """``⚠️ ...`` line for a refused prematch call, or "" if nothing to say.

    ``refusal`` is the dict ``win_model_veto.win_prediction_ex`` returns as
    the third element when its index is None: ``{"reason": str, "details":
    [str, ...], "position_mismatch": [(account_id, assigned_pos, usual_pos),
    ...] | None}`` (see ``win_model_veto._LAST_REFUSAL``). Hero names come
    from the existing hero registry getter (``dota2protracker.get_hero_name``);
    there is no account_id -> player-nickname resolver anywhere in this
    codebase (``base/id_to_names.py`` only maps team name -> team_id), so the
    raw account_id is printed on its own — the documented "else the account
    id" fallback.
    """
    if not isinstance(refusal, dict):
        return ""
    reason = str(refusal.get("reason") or "").strip()
    if not reason:
        return ""
    mismatch = refusal.get("position_mismatch")
    if mismatch:
        acc_to_hero = {}
        for side in (radiant_dict, dire_dict):
            if not isinstance(side, dict):
                continue
            for i in range(1, 6):
                entry = side.get(f"pos{i}")
                if not isinstance(entry, dict):
                    continue
                try:
                    acc = int(entry.get("account_id") or 0)
                    hero = int(entry.get("hero_id") or 0)
                except (TypeError, ValueError):
                    continue
                if acc > 0:
                    acc_to_hero[acc] = hero
        try:
            try:
                from base import dota2protracker as _d2pt
            except ImportError:
                import dota2protracker as _d2pt
        except Exception:
            _d2pt = None
        parts = []
        for item in mismatch:
            try:
                acc, assigned, usual = int(item[0]), int(item[1]), int(item[2])
            except (TypeError, ValueError, IndexError):
                continue
            hero_name = ""
            hero_id = acc_to_hero.get(acc)
            if hero_id and _d2pt is not None:
                try:
                    hero_name = _d2pt.get_hero_name(hero_id) or ""
                except Exception:
                    hero_name = ""
            label = (f"{acc} {hero_name} p{assigned} (обычно p{usual})" if hero_name
                     else f"{acc} p{assigned} (обычно p{usual})")
            parts.append(label)
        if parts:
            return ("⚠️ Позиции не соответствуют истории (предматчевая модель "
                    "отказала): " + "; ".join(parts))
    return f"⚠️ Предматчевая модель отказала: {reason}"
