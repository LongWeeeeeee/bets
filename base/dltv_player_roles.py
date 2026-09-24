"""DLTV player roles: soft vote for SourceTV position resolution.

Source: ``GET https://dltv.org/api/v1/search?q=<account_id>`` (plain HTTP JSON,
no JS challenge). The response is fuzzy — only a player whose int(steam_id)
equals the requested account_id is accepted, and only a role in 1..5.

The probe calls :func:`lookup_roles` once per side in the permutation branch of
``_resolve_positions`` and adds ``w`` per matching slot to the permutation
score. This module never raises from :func:`lookup_roles`.
"""

import json
import math
import os
import time
import urllib.request

SEARCH_URL = "https://dltv.org/api/v1/search?q={account_id}"
USER_AGENT = "Mozilla/5.0"
REQUEST_TIMEOUT_S = 4.0
MIN_REQUEST_TIMEOUT_S = 0.5
DEFAULT_ROLE_WEIGHT = 0.2

HIT_TTL_S = 24 * 3600
MISS_TTL_S = 12 * 3600
ERROR_TTL_S = 10 * 60

_DEFAULT_DEADLINE_S = 6.0

# account_id -> {"role": int | None, "ts": float, "err": bool}
_MEM_CACHE = {}


def _repo_root():
    return os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


def _cache_path():
    override = os.environ.get("DLTV_ROLE_CACHE_PATH")
    if override:
        return override
    return os.path.join(_repo_root(), "runtime", "dltv_player_roles_cache.json")


def _now():
    return time.time()


def parse_search_role(payload, account_id):
    """Extract the DLTV role (1..5) for account_id from a search payload.

    Returns None when the player is absent, the role is missing, or the role
    is outside 1..5. Never raises.
    """
    try:
        wanted = int(account_id)
    except (TypeError, ValueError):
        return None
    try:
        players = (payload or {}).get("players") or []
    except AttributeError:
        return None
    for p in players:
        if not isinstance(p, dict):
            continue
        try:
            if int(p.get("steam_id")) != wanted:
                continue
            role = int(p.get("role"))
        except (TypeError, ValueError):
            continue
        if 1 <= role <= 5:
            return role
    return None


def _default_fetch(account_id, timeout=REQUEST_TIMEOUT_S):
    req = urllib.request.Request(
        SEARCH_URL.format(account_id=account_id),
        headers={"User-Agent": USER_AGENT},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


def role_weight():
    """DLTV soft-vote weight: finite value >= 0, else DEFAULT_ROLE_WEIGHT."""
    try:
        w = float(os.environ.get("DLTV_ROLE_WEIGHT", str(DEFAULT_ROLE_WEIGHT)))
    except (TypeError, ValueError):
        return DEFAULT_ROLE_WEIGHT
    if not math.isfinite(w) or w < 0:
        return DEFAULT_ROLE_WEIGHT
    return w


def _read_file_cache(path):
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    return data


def _write_file_cache(path, entries):
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        tmp = path + ".tmp-%d" % os.getpid()
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(entries, fh)
        os.replace(tmp, path)
    except Exception:
        pass


def _lookup_deadline_s(deadline_s):
    if deadline_s is not None:
        try:
            return max(0.0, float(deadline_s))
        except (TypeError, ValueError):
            pass
    try:
        return max(0.0, float(os.environ.get("DLTV_ROLE_LOOKUP_DEADLINE_S", _DEFAULT_DEADLINE_S)))
    except (TypeError, ValueError):
        return _DEFAULT_DEADLINE_S


def lookup_roles(account_ids, deadline_s=None, fetch=None, now=None):
    """Return {account_id: role} for the ids DLTV knows. Never raises.

    ``fetch(account_id) -> dict`` is injectable for tests (default: urllib
    GET). ``now`` is an injectable clock (default: time.time), used for both
    cache TTLs and the total wall deadline. Env knobs: DLTV_ROLE_LOOKUP=0
    disables all network, DLTV_ROLE_LOOKUP_DEADLINE_S caps total wall time,
    DLTV_ROLE_CACHE_PATH overrides the JSON cache file.
    """
    try:
        if str(os.environ.get("DLTV_ROLE_LOOKUP", "")).strip() == "0":
            return {}
        clock = now or _now
        try:
            start = clock()
        except Exception:
            start = _now()
        deadline = _lookup_deadline_s(deadline_s)
        do_fetch = fetch or _default_fetch

        ids = []
        for aid in account_ids or []:
            try:
                ids.append(int(aid))
            except (TypeError, ValueError):
                continue

        path = _cache_path()
        file_entries = _read_file_cache(path)
        # File cache backs memory: adopt file entries memory does not have.
        for key, entry in file_entries.items():
            if key not in _MEM_CACHE and isinstance(entry, dict):
                _MEM_CACHE[key] = entry

        def cached_entry(aid):
            entry = _MEM_CACHE.get(str(aid))
            if not isinstance(entry, dict):
                return None
            try:
                age = clock() - float(entry.get("ts", 0))
            except Exception:
                return None
            role = entry.get("role")
            ttl = HIT_TTL_S if isinstance(role, int) else (
                ERROR_TTL_S if entry.get("err") else MISS_TTL_S)
            if age < ttl:
                return entry
            return None

        roles = {}
        pending = []
        for aid in ids:
            entry = cached_entry(aid)
            if entry is not None:
                if isinstance(entry.get("role"), int):
                    roles[aid] = entry["role"]
            else:
                pending.append(aid)

        fetch_kw = True  # reset once a fetch rejects the timeout kwarg
        for idx, aid in enumerate(pending):
            try:
                elapsed = clock() - start
            except Exception:
                elapsed = 0.0
            if elapsed >= deadline:
                # Deadline ran out: cache the ids we never reached as
                # errors, so a retry within the error TTL makes zero
                # fetches instead of stalling on the outage again.
                for rest in pending[idx:]:
                    entry = {"role": None, "ts": start, "err": True}
                    _MEM_CACHE[str(rest)] = entry
                    file_entries[str(rest)] = entry
                break
            req_timeout = min(REQUEST_TIMEOUT_S,
                              max(MIN_REQUEST_TIMEOUT_S, deadline - elapsed))
            try:
                if fetch_kw:
                    try:
                        payload = do_fetch(aid, timeout=req_timeout)
                    except TypeError:
                        fetch_kw = False
                        payload = do_fetch(aid)
                else:
                    payload = do_fetch(aid)
            except Exception:
                entry = {"role": None, "ts": start, "err": True}
                _MEM_CACHE[str(aid)] = entry
                file_entries[str(aid)] = entry
                continue
            try:
                ts = clock()
            except Exception:
                ts = start
            role = parse_search_role(payload, aid)
            entry = {"role": role, "ts": ts, "err": False}
            _MEM_CACHE[str(aid)] = entry
            file_entries[str(aid)] = entry
            if isinstance(role, int):
                roles[aid] = role

        if file_entries:
            _write_file_cache(path, file_entries)
        return roles
    except Exception:
        return {}
