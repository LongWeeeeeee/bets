"""CLI entrypoint: python -m services.anti_stall_supervisor

Wires W1–W4 into a single locked hygiene tick. W5 unit files stay inert.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Optional

# Support both `python -m services.anti_stall_supervisor` (package) and
# direct path execution during staging assembly.
_PKG = Path(__file__).resolve().parent
if str(_PKG.parent) not in sys.path:
    # Ensure /root/main is on path so `services.*` imports resolve when needed.
    root = _PKG.parent.parent if _PKG.parent.name == "services" else _PKG.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

try:
    from . import adapters as adapters_mod
    from . import runner as runner_mod
except ImportError:  # pragma: no cover - direct script path
    if str(_PKG) not in sys.path:
        sys.path.insert(0, str(_PKG))
    import adapters as adapters_mod  # type: ignore
    import runner as runner_mod  # type: ignore

PACKAGE_DIR = _PKG
DEFAULT_CONFIG = PACKAGE_DIR / "config.json"
DEFAULT_VAR = Path("/root/main/runtime/anti_stall_supervisor_var")


def _default_config() -> dict[str, Any]:
    var = DEFAULT_VAR
    return {
        "hermes_root": "/root/.hermes",
        "policy_path": str(PACKAGE_DIR / "policy.json"),
        "var_dir": str(var),
        "lock_path": str(var / "hygiene.lock"),
        "state_path": str(var / "state.json"),
        "report_path": str(var / "report.json"),
        "audit_path": str(var / "audit.jsonl"),
        "linter_path": "/root/main/runtime/kanban_plan_lint.py",
        "linter_timeout_s": 60,
        "tick_deadline_s": 240,
        "quiet": True,
        "dry_run": False,
        "include_linter_cmd": False,
    }


def load_merged_config(path: Optional[str]) -> dict[str, Any]:
    cfg = _default_config()
    if path:
        loaded = runner_mod.load_config(path)
        cfg.update(loaded)
    # Ensure var paths exist only under configured var_dir (created on demand by runner lock/atomic write).
    return cfg


def build_arg_parser():
    ap = runner_mod.build_arg_parser()
    # Extend description
    ap.description = "Hermes anti-stall supervisor (integrated W1–W4; W5 inert)"
    ap.set_defaults(config=str(DEFAULT_CONFIG) if DEFAULT_CONFIG.is_file() else "")
    ap.add_argument(
        "--print-report-summary",
        action="store_true",
        help="After tick, print one JSON line with planned_actions/rc (explicit; breaks quiet)",
    )
    ap.add_argument(
        "--emit-running-tuples",
        action="store_true",
        help="Print running card tuples as JSON and exit (W1 adapter; no tick)",
    )
    return ap


def main(argv: Optional[list[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    ap = build_arg_parser()
    args = ap.parse_args(argv)

    if args.self_test:
        # Delegate structural self-test of runner lock/state, then adapter import smoke.
        rc = runner_mod.self_test()
        if rc != 0:
            return rc
        try:
            ads = adapters_mod.build_adapters({})
            assert callable(ads["snapshot"]) and callable(ads["decide"]) and callable(ads["execute"])
        except Exception as exc:
            print(f"ADAPTER_SELF_TEST_FAIL:{exc}", file=sys.stderr)
            return 1
        # Keep runner's SELF_TEST_OK line behavior; runner.self_test already printed.
        return 0

    cfg = load_merged_config(args.config or None)
    if args.var_dir:
        vd = Path(args.var_dir)
        cfg["var_dir"] = str(vd)
        cfg.setdefault("state_path", str(vd / "state.json"))
        cfg.setdefault("report_path", str(vd / "report.json"))
        cfg.setdefault("audit_path", str(vd / "audit.jsonl"))
        # When var-dir overridden, also re-base default paths if they still point at global var.
        cfg["state_path"] = str(vd / "state.json")
        cfg["report_path"] = str(vd / "report.json")
        cfg["audit_path"] = str(vd / "audit.jsonl")
        if not args.lock_path:
            cfg["lock_path"] = str(vd / "hygiene.lock")
    if args.lock_path:
        cfg["lock_path"] = args.lock_path
    if args.dry_run:
        cfg["dry_run"] = True

    if args.emit_running_tuples:
        tuples = adapters_mod.running_card_tuples(cfg.get("hermes_root") or "/root/.hermes")
        # Explicit flag: printing is intentional (not routine tick).
        sys.stdout.write(json.dumps({"running": tuples}, sort_keys=True) + "\n")
        return 0

    adapters = adapters_mod.build_adapters(cfg)
    # Strip non-callable helpers before handing to runner.
    run_adapters = {
        "snapshot": adapters["snapshot"],
        "decide": adapters["decide"],
        "execute": adapters["execute"],
    }

    rc = runner_mod.run_tick(cfg, adapters=run_adapters)

    # Post-tick: if not dry-run, merge decision observations into state for next tick.
    # Runner already wrote state; we re-open and merge decision-native fields when present
    # in the in-memory plan cache (non-dry-run only).
    if not cfg.get("dry_run"):
        try:
            plan = adapters.get("_cache", {}).get("plan")
            state_path = Path(cfg.get("state_path"))
            if plan and state_path.is_file():
                state, _err = runner_mod.load_state(state_path)
                now_ns = adapters.get("_cache", {}).get("now_ns") or 0
                merged = adapters_mod.merge_plan_into_runner_state(
                    state, plan, now_ns=int(now_ns or 0), dry_run=False
                )
                runner_mod.atomic_write_json(state_path, merged)
        except Exception:
            # Fail-soft on observation merge; next tick still works with empty prior tasks.
            pass

    if args.print_report_summary or args.print_success:
        report_path = Path(cfg.get("report_path"))
        summary: dict[str, Any] = {"rc": rc}
        if report_path.is_file():
            try:
                rep = json.loads(report_path.read_text(encoding="utf-8"))
                decision = rep.get("decision") or {}
                planned = int(decision.get("actions_planned") or 0)
                applied = int(decision.get("actions_applied") or 0)
                summary.update(
                    {
                        "planned_actions": planned,
                        "actions_applied": applied,
                        "decision_key": rep.get("decision_key"),
                        "fail_closed": rep.get("fail_closed"),
                        "dry_run": rep.get("dry_run"),
                        "errors": rep.get("errors") or [],
                    }
                )
            except Exception as exc:
                summary["report_error"] = str(exc)
        if args.print_success and rc == 0:
            summary["status"] = "SUCCESS"
        # Explicit opt-in only.
        sys.stdout.write(json.dumps(summary, sort_keys=True) + "\n")

    return int(rc)


if __name__ == "__main__":
    sys.exit(main())
