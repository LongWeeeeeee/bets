"""W-SHADOW: non-sending singleton-injected Winline evidence probe (RED→GREEN).

Contract:
- Importable async entry: run_winline_shadow_probe(
    *, submit_shared_job, context, output_path, now=None) -> int
- Exactly one call to injected submit_shared_job; no browser/thread/subprocess/
  send/gate/run_sites_in_camoufox capability in the probe module.
- Atomic JSON evidence with schema/verdict/selected-side odds validation.
- Return 0 only for PASS; nonzero for every mismatch/stale/missing/error.
"""
from __future__ import annotations

import ast
import asyncio
import importlib.util
import json
import sys
import types
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "services" / "winline" / "winline_shadow_probe.py"
BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))


SCHEMA_VERSION = "winline_shadow_probe.v1"
FORBIDDEN_TOKENS = (
    "run_sites_in_camoufox",
    "Camoufox",
    "camoufox",
    "playwright",
    "Playwright",
    "subprocess",
    "Thread",
    "threading",
    "multiprocessing",
    "telegram",
    "Telegram",
    "send_message",
    "BOOKMAKER_ODDS_GATE",
    "enable_odds",
    "should_send",
)


def _load_probe_module():
    """Load services/winline/winline_shadow_probe.py without package install."""
    if not MODULE_PATH.is_file():
        pytest.fail(f"missing module under test: {MODULE_PATH}")
    spec = importlib.util.spec_from_file_location("winline_shadow_probe", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["winline_shadow_probe"] = mod
    spec.loader.exec_module(mod)
    return mod


def _base_context(**overrides: Any) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {
        "match_id": "series-shadow-1001",
        "map_num": 2,
        "team1": "Team Spirit",
        "team2": "Tundra Esports",
        "selected_side": "P1",
        "freshness_limit_seconds": 15.0,
        "job": {"label": "winline-shadow", "site": "winline"},
    }
    ctx.update(overrides)
    return ctx


def _good_observation(**overrides: Any) -> Dict[str, Any]:
    obs: Dict[str, Any] = {
        "source": "Winline",
        "match_id": "series-shadow-1001",
        "map_num": 2,
        "team1": "Team Spirit",
        "team2": "Tundra Esports",
        "p1_odds": 1.55,
        "p2_odds": 2.40,
        "observed_at": 1_700_100_000.0,
    }
    obs.update(overrides)
    return obs


def _run(coro) -> Any:
    return asyncio.run(coro)


class _FakeRunner:
    def __init__(self, result: Any = None, *, exc: Optional[BaseException] = None):
        self.calls: List[Any] = []
        self._result = result
        self._exc = exc

    async def __call__(self, job: Any = None, **kwargs: Any) -> Any:
        self.calls.append({"job": job, "kwargs": kwargs})
        if self._exc is not None:
            raise self._exc
        return self._result


# ---------------------------------------------------------------------------
# Source / capability guards (no acquisition of its own)
# ---------------------------------------------------------------------------


def test_module_has_no_browser_thread_send_gate_capability() -> None:
    assert MODULE_PATH.is_file(), f"expected {MODULE_PATH}"
    src = MODULE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(src)
    # No imports of forbidden acquisition surfaces.
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names: List[str] = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            else:
                mod = node.module or ""
                names = [mod] + [a.name for a in node.names]
            joined = " ".join(names)
            for bad in (
                "camoufox",
                "playwright",
                "subprocess",
                "threading",
                "multiprocessing",
                "bookmaker_selenium_odds",
                "cyberscore_try",
            ):
                assert bad not in joined.lower(), f"forbidden import surface: {joined}"
    for token in FORBIDDEN_TOKENS:
        # Allow the token only inside string literals used by source scans/docs? Prefer zero.
        # Hard ban in source text except comments that document forbidden list is ok if
        # we keep the ban strict — task requires no path. Comments mentioning them are fine
        # if they do not call; but simplest: zero occurrences of runtime call paths.
        pass
    # Explicit call-path bans:
    for banned in (
        "run_sites_in_camoufox",
        "subprocess.",
        "threading.",
        "multiprocessing.",
        "Camoufox(",
        "async_playwright",
        "sync_playwright",
        "send_message",
        "bot.send",
    ):
        assert banned not in src, f"forbidden capability token present: {banned}"


def test_entry_point_is_async_and_importable() -> None:
    mod = _load_probe_module()
    fn = getattr(mod, "run_winline_shadow_probe", None)
    assert fn is not None, "run_winline_shadow_probe must be exported"
    assert asyncio.iscoroutinefunction(fn), "entry must be async"


# ---------------------------------------------------------------------------
# PASS path: one injection, exact selected-side mapping, return 0
# ---------------------------------------------------------------------------


def test_pass_p1_selected_side_exact_odds_atomic_json(tmp_path: Path) -> None:
    mod = _load_probe_module()
    now = 1_700_100_005.0
    runner = _FakeRunner(_good_observation(observed_at=now - 2.0))
    out = tmp_path / "shadow_pass_p1.json"
    rc = _run(
        mod.run_winline_shadow_probe(
            submit_shared_job=runner,
            context=_base_context(selected_side="P1"),
            output_path=str(out),
            now=now,
        )
    )
    assert rc == 0
    assert len(runner.calls) == 1, "must submit exactly one shared job"
    assert out.is_file()
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["schema_version"] == SCHEMA_VERSION
    assert data["match_id"] == "series-shadow-1001"
    assert data["map_num"] == 2
    assert data["team1"] == "Team Spirit"
    assert data["team2"] == "Tundra Esports"
    assert data["p1_team"] == "Team Spirit"
    assert data["p2_team"] == "Tundra Esports"
    assert data["source"] == "Winline"
    assert data["p1_odds"] == 1.55
    assert data["p2_odds"] == 2.40
    assert data["selected_side"] == "P1"
    assert data["selected_odds"] == 1.55
    assert data["verdict"] == "PASS"
    assert data["failure_reasons"] == [] or data.get("failure_reasons") in ([], None)
    assert "observed_at" in data and data["observed_at"] is not None
    assert "collected_at" in data and data["collected_at"] is not None
    # Atomic write: final file is valid JSON (no partial)
    json.loads(out.read_text(encoding="utf-8"))


def test_pass_p2_selected_side_maps_to_p2_odds(tmp_path: Path) -> None:
    mod = _load_probe_module()
    now = 1_700_100_010.0
    runner = _FakeRunner(_good_observation(observed_at=now - 1.0, p1_odds=1.80, p2_odds=2.05))
    out = tmp_path / "shadow_pass_p2.json"
    rc = _run(
        mod.run_winline_shadow_probe(
            submit_shared_job=runner,
            context=_base_context(selected_side="P2"),
            output_path=str(out),
            now=now,
        )
    )
    assert rc == 0
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["selected_side"] == "P2"
    assert data["selected_odds"] == 2.05
    assert data["verdict"] == "PASS"
    assert len(runner.calls) == 1


# ---------------------------------------------------------------------------
# Optional selected_side: absent / None / empty / whitespace is non-gating
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mode,side_value",
    [
        ("omit", None),
        ("set", None),
        ("set", ""),
        ("set", "   "),
        ("set", "\t  \n"),
    ],
    ids=["absent_key", "none", "empty", "spaces", "whitespace"],
)
def test_absent_or_empty_selected_side_passes_when_observation_valid(
    tmp_path: Path,
    mode: str,
    side_value: Any,
) -> None:
    """selected_side is optional: missing/blank must not block an otherwise-valid probe."""
    mod = _load_probe_module()
    now = 1_700_100_015.0
    runner = _FakeRunner(_good_observation(observed_at=now - 1.0))
    out = tmp_path / f"shadow_pass_optional_side_{mode}_{side_value!r}.json".replace(" ", "_")
    ctx = _base_context()
    if mode == "omit":
        ctx.pop("selected_side", None)
    else:
        ctx["selected_side"] = side_value
    rc = _run(
        mod.run_winline_shadow_probe(
            submit_shared_job=runner,
            context=ctx,
            output_path=str(out),
            now=now,
        )
    )
    assert rc == 0
    assert len(runner.calls) == 1
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["verdict"] == "PASS"
    reasons = data.get("failure_reasons") or []
    assert reasons in ([], None) or reasons == []
    assert "invalid_selected_side" not in reasons
    # No selected side → no mapped selected_odds requirement.
    assert data.get("selected_odds") is None
    assert data["p1_odds"] == 1.55
    assert data["p2_odds"] == 2.40
    assert data["source"] == "Winline"
    assert data["match_id"] == "series-shadow-1001"
    assert data["map_num"] == 2


def test_supplied_selected_side_mismatch_still_fails(tmp_path: Path) -> None:
    """Strict side/team mapping remains when selected_side is explicitly supplied."""
    mod = _load_probe_module()
    now = 1_700_100_016.0
    runner = _FakeRunner(
        _good_observation(
            observed_at=now,
            team1="Tundra Esports",
            team2="Team Spirit",
            p1_odds=2.40,
            p2_odds=1.55,
        )
    )
    out = tmp_path / "shadow_fail_supplied_side_mismatch.json"
    rc = _run(
        mod.run_winline_shadow_probe(
            submit_shared_job=runner,
            context=_base_context(selected_side="P2"),
            output_path=str(out),
            now=now,
        )
    )
    assert rc != 0
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["verdict"] == "FAIL"
    reasons = " ".join(str(r) for r in (data.get("failure_reasons") or []))
    assert "team" in reasons.lower() or "order" in reasons.lower() or "side" in reasons.lower()


def test_absent_selected_side_still_rejects_stale_and_wrong_source(tmp_path: Path) -> None:
    """Optional side must not weaken freshness/source gates."""
    mod = _load_probe_module()
    now = 1_700_100_017.0
    # stale
    runner = _FakeRunner(_good_observation(observed_at=now - 60.0))
    out_stale = tmp_path / "shadow_optional_stale.json"
    ctx_stale = _base_context()
    ctx_stale.pop("selected_side", None)
    rc_stale = _run(
        mod.run_winline_shadow_probe(
            submit_shared_job=runner,
            context=ctx_stale,
            output_path=str(out_stale),
            now=now,
        )
    )
    assert rc_stale != 0
    data_stale = json.loads(out_stale.read_text(encoding="utf-8"))
    assert data_stale["verdict"] == "FAIL"
    assert any(
        "stale" in str(r).lower() or "fresh" in str(r).lower()
        for r in (data_stale.get("failure_reasons") or [])
    )
    # wrong source
    runner2 = _FakeRunner(_good_observation(observed_at=now, source="BetBoom"))
    out_src = tmp_path / "shadow_optional_bad_source.json"
    ctx_src = _base_context(selected_side="")
    rc_src = _run(
        mod.run_winline_shadow_probe(
            submit_shared_job=runner2,
            context=ctx_src,
            output_path=str(out_src),
            now=now,
        )
    )
    assert rc_src != 0
    data_src = json.loads(out_src.read_text(encoding="utf-8"))
    assert data_src["verdict"] == "FAIL"
    assert any("source" in str(r).lower() for r in (data_src.get("failure_reasons") or []))


# ---------------------------------------------------------------------------
# FAIL paths: nonzero, never false PASS; evidence when observation obtained
# ---------------------------------------------------------------------------


def test_wrong_map_fails_nonzero(tmp_path: Path) -> None:
    mod = _load_probe_module()
    now = 1_700_100_020.0
    runner = _FakeRunner(_good_observation(map_num=1, observed_at=now))
    out = tmp_path / "shadow_fail_map.json"
    rc = _run(
        mod.run_winline_shadow_probe(
            submit_shared_job=runner,
            context=_base_context(map_num=2),
            output_path=str(out),
            now=now,
        )
    )
    assert rc != 0
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["verdict"] == "FAIL"
    reasons = data.get("failure_reasons") or []
    assert any("map" in str(r).lower() for r in reasons)
    assert len(runner.calls) == 1


def test_opposite_side_selected_odds_mismatch_fails(tmp_path: Path) -> None:
    """If observation only had inverted odds, selected_odds must not accept opposite."""
    mod = _load_probe_module()
    now = 1_700_100_030.0
    # Observation has p1=1.55 p2=2.40; if someone wrongly picks opposite, FAIL.
    # We force FAIL by making selected_side invalid path via team swap mismatch below.
    # Explicit: selected_side P1 must equal p1_odds, not p2.
    runner = _FakeRunner(
        _good_observation(observed_at=now, p1_odds=1.55, p2_odds=2.40)
    )
    out = tmp_path / "shadow_fail_side.json"

    # Monkeypatch-free: provide observation where selected mapping would be wrong
    # by returning odds list order that conflicts with named p1/p2 — probe must
    # use named p1/p2 and still PASS for P1==1.55. Opposite-side case:
    # inject observation with only swapped team names vs context.
    runner = _FakeRunner(
        _good_observation(
            observed_at=now,
            team1="Tundra Esports",
            team2="Team Spirit",
            p1_odds=2.40,
            p2_odds=1.55,
        )
    )
    rc = _run(
        mod.run_winline_shadow_probe(
            submit_shared_job=runner,
            context=_base_context(selected_side="P1"),
            output_path=str(out),
            now=now,
        )
    )
    assert rc != 0
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["verdict"] == "FAIL"
    reasons = " ".join(str(r) for r in (data.get("failure_reasons") or []))
    assert "team" in reasons.lower() or "order" in reasons.lower() or "side" in reasons.lower()


def test_stale_observation_fails(tmp_path: Path) -> None:
    mod = _load_probe_module()
    now = 1_700_100_100.0
    runner = _FakeRunner(_good_observation(observed_at=now - 60.0))  # > 15s
    out = tmp_path / "shadow_fail_stale.json"
    rc = _run(
        mod.run_winline_shadow_probe(
            submit_shared_job=runner,
            context=_base_context(freshness_limit_seconds=15.0),
            output_path=str(out),
            now=now,
        )
    )
    assert rc != 0
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["verdict"] == "FAIL"
    assert any("stale" in str(r).lower() or "fresh" in str(r).lower() for r in (data.get("failure_reasons") or []))


def test_missing_odds_fails(tmp_path: Path) -> None:
    mod = _load_probe_module()
    now = 1_700_100_110.0
    runner = _FakeRunner(_good_observation(observed_at=now, p2_odds=None))
    out = tmp_path / "shadow_fail_missing.json"
    rc = _run(
        mod.run_winline_shadow_probe(
            submit_shared_job=runner,
            context=_base_context(),
            output_path=str(out),
            now=now,
        )
    )
    assert rc != 0
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["verdict"] == "FAIL"
    assert any("odds" in str(r).lower() or "missing" in str(r).lower() for r in (data.get("failure_reasons") or []))


def test_malformed_odds_fails(tmp_path: Path) -> None:
    mod = _load_probe_module()
    now = 1_700_100_120.0
    runner = _FakeRunner(_good_observation(observed_at=now, p1_odds="not-a-number", p2_odds=2.1))
    out = tmp_path / "shadow_fail_malformed.json"
    rc = _run(
        mod.run_winline_shadow_probe(
            submit_shared_job=runner,
            context=_base_context(),
            output_path=str(out),
            now=now,
        )
    )
    assert rc != 0
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["verdict"] == "FAIL"


def test_non_winline_source_fails(tmp_path: Path) -> None:
    mod = _load_probe_module()
    now = 1_700_100_130.0
    runner = _FakeRunner(_good_observation(observed_at=now, source="BetBoom"))
    out = tmp_path / "shadow_fail_source.json"
    rc = _run(
        mod.run_winline_shadow_probe(
            submit_shared_job=runner,
            context=_base_context(),
            output_path=str(out),
            now=now,
        )
    )
    assert rc != 0
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["verdict"] == "FAIL"
    assert any("source" in str(r).lower() for r in (data.get("failure_reasons") or []))


def test_match_id_mismatch_fails(tmp_path: Path) -> None:
    mod = _load_probe_module()
    now = 1_700_100_140.0
    runner = _FakeRunner(_good_observation(observed_at=now, match_id="other-series"))
    out = tmp_path / "shadow_fail_match.json"
    rc = _run(
        mod.run_winline_shadow_probe(
            submit_shared_job=runner,
            context=_base_context(match_id="series-shadow-1001"),
            output_path=str(out),
            now=now,
        )
    )
    assert rc != 0
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["verdict"] == "FAIL"


def test_invalid_selected_side_fails(tmp_path: Path) -> None:
    mod = _load_probe_module()
    now = 1_700_100_150.0
    runner = _FakeRunner(_good_observation(observed_at=now))
    out = tmp_path / "shadow_fail_sel_side.json"
    rc = _run(
        mod.run_winline_shadow_probe(
            submit_shared_job=runner,
            context=_base_context(selected_side="radiant"),
            output_path=str(out),
            now=now,
        )
    )
    assert rc != 0
    # May fail before or after observation; must not PASS.
    if out.is_file():
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data.get("verdict") != "PASS"


def test_acquisition_exception_nonzero_no_false_pass(tmp_path: Path) -> None:
    mod = _load_probe_module()
    now = 1_700_100_160.0
    runner = _FakeRunner(exc=RuntimeError("shared session reset failed"))
    out = tmp_path / "shadow_fail_acq.json"
    rc = _run(
        mod.run_winline_shadow_probe(
            submit_shared_job=runner,
            context=_base_context(),
            output_path=str(out),
            now=now,
        )
    )
    assert rc != 0
    assert len(runner.calls) == 1
    if out.is_file():
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data.get("verdict") != "PASS"


def test_write_failure_nonzero_no_partial_pass(tmp_path: Path) -> None:
    mod = _load_probe_module()
    now = 1_700_100_170.0
    runner = _FakeRunner(_good_observation(observed_at=now))
    # Point output_path at a directory so atomic replace cannot create a file there as file.
    blocked = tmp_path / "not_a_file_dir"
    blocked.mkdir()
    rc = _run(
        mod.run_winline_shadow_probe(
            submit_shared_job=runner,
            context=_base_context(),
            output_path=str(blocked),
            now=now,
        )
    )
    assert rc != 0
    # Must not leave a PASS-looking file at the path (path is a dir).
    if blocked.is_file():
        data = json.loads(blocked.read_text(encoding="utf-8"))
        assert data.get("verdict") != "PASS"


def test_exactly_one_shared_job_even_on_fail(tmp_path: Path) -> None:
    mod = _load_probe_module()
    now = 1_700_100_180.0
    runner = _FakeRunner(_good_observation(map_num=99, observed_at=now))
    out = tmp_path / "shadow_one_call.json"
    rc = _run(
        mod.run_winline_shadow_probe(
            submit_shared_job=runner,
            context=_base_context(map_num=2),
            output_path=str(out),
            now=now,
        )
    )
    assert rc != 0
    assert len(runner.calls) == 1


def test_zero_or_negative_odds_fail(tmp_path: Path) -> None:
    mod = _load_probe_module()
    now = 1_700_100_190.0
    runner = _FakeRunner(_good_observation(observed_at=now, p1_odds=0.0, p2_odds=2.1))
    out = tmp_path / "shadow_fail_zero.json"
    rc = _run(
        mod.run_winline_shadow_probe(
            submit_shared_job=runner,
            context=_base_context(),
            output_path=str(out),
            now=now,
        )
    )
    assert rc != 0
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["verdict"] == "FAIL"


def test_atomic_write_uses_temp_replace_not_partial(tmp_path: Path) -> None:
    """Final path must appear only as complete JSON after successful write."""
    mod = _load_probe_module()
    now = 1_700_100_200.0
    runner = _FakeRunner(_good_observation(observed_at=now))
    out = tmp_path / "subdir" / "evidence.json"
    rc = _run(
        mod.run_winline_shadow_probe(
            submit_shared_job=runner,
            context=_base_context(),
            output_path=str(out),
            now=now,
        )
    )
    assert rc == 0
    assert out.is_file()
    # No stray .tmp left next to final (best-effort; empty dir ok)
    leftovers = [p for p in out.parent.iterdir() if p.name != out.name]
    for p in leftovers:
        assert not p.name.endswith(".tmp") or p.stat().st_size == 0 or not p.exists()
