"""Run or resume one public-match sweep; scheduling is owned by the caller."""
import argparse
import fcntl
import json
import os
import time
from pathlib import Path

INTERVAL_SECONDS = 5 * 86400
OVERLAP_SECONDS = 86400
ROOT = Path(__file__).resolve().parents[1]


def write_state(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def run_sweep(data_dir, runtime, collect, *, initial_since=None, now=None):
    """Keep the cutoff and terminal-player cursor unchanged across retries."""
    runtime.mkdir(parents=True, exist_ok=True)
    now = int(time.time() if now is None else now)
    state_path = runtime / "pub_recrawl.json"
    with (runtime / "pub_recrawl.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("Public sweep already running; skipped", flush=True)
            return "busy"
        state = json.loads(state_path.read_text()) if state_path.exists() else None
        if state and state["status"] == "complete":
            if now < state["started_at"] + INTERVAL_SECONDS:
                print("Public sweep not due yet; skipped", flush=True)
                return "not_due"
            cutoff = state["started_at"] - OVERLAP_SECONDS
            state = None
        elif state is None:
            if initial_since is None or not 0 < initial_since < now:
                raise ValueError("First sweep requires an explicit valid --since epoch")
            cutoff = int(initial_since)
        elif state["status"] not in {"preparing", "running", "failed"}:
            raise ValueError("Unknown public sweep status")

        if state is None:
            state = {"status": "preparing", "started_at": now, "cutoff": cutoff,
                     "cursor_backup": str(data_dir / ("processed_ids_to_graph.txt.bak_recrawl_" + str(time.time_ns())))}
            write_state(state_path, state)
        if state["status"] == "preparing":
            graph = data_dir / "processed_ids_to_graph.txt"
            backup = Path(state["cursor_backup"])
            # Recover a crash between rename and writing status=running.
            if graph.exists() and not backup.exists():
                graph.rename(backup)
            state["status"] = "running"
            write_state(state_path, state)

        state.update(status="running", pid=os.getpid(), attempt_started_at=now)
        write_state(state_path, state)
        (runtime / "get_pubs.pid").write_text(str(os.getpid()) + "\n")
        print(f"Public sweep pid={os.getpid()} started_at={state['started_at']} cutoff={state['cutoff']}", flush=True)
        try:
            collect(state["cutoff"])
        except BaseException as error:
            state.update(status="failed", failed_at=int(time.time()), error_type=type(error).__name__)
            write_state(state_path, state)
            raise
        state.update(status="complete", completed_at=int(time.time()))
        state.pop("error_type", None)
        write_state(state_path, state)
        print("Public sweep complete", flush=True)
        return "complete"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", type=int, help="Initial cutoff only; retries retain the original cutoff")
    args = parser.parse_args()
    os.chdir(ROOT)
    if int(os.environ.get("PUBS_MAX_PLAYERS", "0") or "0") != 0:
        raise ValueError("Recurring sweeps must not use PUBS_MAX_PLAYERS")
    os.environ["PUBS_IDS_MODE"] = "file"
    import maps_research as mr

    if mr.load_pub_player_ids() is None:
        raise FileNotFoundError(mr.PUBS_PLAYER_IDS_FILE)
    if not mr.STRATZ_PROXY_MAP:
        raise ValueError("STRATZ proxy pool is empty")
    # The legacy reader uses a different path from its writer. Do not let an
    # unrelated checkpoint replace this sweep's terminal-player cursor.
    legacy_checkpoint = ROOT / "analise_pub_matches" / "maps_state.json"
    if legacy_checkpoint.exists():
        raise RuntimeError(f"Legacy checkpoint requires inspection: {legacy_checkpoint}")

    def collect(cutoff):
        # get_pubs passes this explicit value to get_maps_new/iter_pub_pages.
        mr.start_date_time = cutoff
        mr.get_pubs()

    run_sweep(mr.ANALYSE_PUB_DIR, ROOT / "runtime", collect, initial_since=args.since)


if __name__ == "__main__":
    main()
