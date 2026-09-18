"""Collect DLTV snapshots or export point-in-time player metadata features."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import uuid
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from base.player_metadata import PlayerHistory, TIME_POLICIES, parse_dltv_match


def fsync_directory(path):
    directory_fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def ensure_directory(path):
    path = Path(path)
    if path.is_dir():
        return
    ensure_directory(path.parent)
    path.mkdir(exist_ok=True)
    fsync_directory(path.parent)


def write_new(path, payload):
    """Publish a complete file without replacing an existing observation."""
    path = Path(path)
    ensure_directory(path.parent)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    with temporary.open("x") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    # Hard link publishes atomically and refuses clobbering. Retain the staging
    # link as recovery evidence; repository policy forbids unattended deletion.
    os.link(str(temporary), str(path))
    fsync_directory(path.parent)


def position_accounts(players):
    """Require explicit map positions at the JSONL boundary, then sort."""
    if (not isinstance(players, list) or len(players) != 5
            or any(not isinstance(p, dict) or type(p.get("position")) is not int for p in players)
            or {p["position"] for p in players} != {1, 2, 3, 4, 5}):
        raise ValueError("need explicit unique map positions 1..5")
    return [p["account_id"] for p in sorted(players, key=lambda p: p["position"])]


def load_snapshot(path):
    """Verify retained source bytes and normalized fields before using them."""
    snapshot = json.loads(path.read_text())
    url = urlparse(snapshot["source_url"])
    if (url.scheme != "https" or url.netloc != "dltv.org"
            or not url.path.startswith("/matches/" + str(snapshot["series_id"]) + "/")):
        raise ValueError("invalid DLTV snapshot source URL")
    # Keep source newlines byte-exact: read_text() folds CRLF before hashing.
    html = path.with_name("source.html").read_bytes().decode("utf-8")
    if hashlib.sha256(html.encode()).hexdigest() != snapshot.get("source_sha256"):
        raise ValueError("snapshot source hash mismatch")
    expected = parse_dltv_match(html, source_url=snapshot["source_url"], observed_at=snapshot["observed_at"])
    if expected != snapshot:
        raise ValueError("snapshot fields differ from retained source")
    return snapshot


def collect(url, output):
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.netloc != "dltv.org" or not parsed.path.startswith("/matches/"):
        raise ValueError("expected https://dltv.org/matches/... URL")
    import requests
    response = requests.get(url, timeout=30, allow_redirects=False)
    response.raise_for_status()
    if response.status_code != 200:
        raise ValueError("unexpected response/redirect")
    observed = time.time()
    snapshot = parse_dltv_match(response.text, source_url=url, observed_at=observed)
    PlayerHistory([snapshot])  # Validate everything before publishing.
    run = Path(output) / (str(time.time_ns()) + "-" + str(snapshot["series_id"]))
    ensure_directory(run.parent)
    run.mkdir(exist_ok=False)
    fsync_directory(run.parent)
    write_new(run / "source.html", response.text)
    write_new(run / "snapshot.json", json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n")
    return {"snapshot": str(run / "snapshot.json"), "players": len(snapshot["players"]),
            "earnings_known": sum(p["earnings_usd"] is not None for p in snapshot["players"]),
            "ranks_known": sum(p["rank"] is not None for p in snapshot["players"]),
            "regions_known": sum(p["rank_region"] is not None for p in snapshot["players"])}


def export(snapshot_dir, matches, output, *, time_policy="strict"):
    paths = sorted(Path(snapshot_dir).glob("*/snapshot.json"))
    if not paths:
        raise ValueError("no player snapshots found")
    history = PlayerHistory([load_snapshot(p) for p in paths])
    rows = []
    ids = set()
    with Path(matches).open() as stream:
        for line in stream:
            match = json.loads(line)
            mid = str(match["match_id"])
            if mid in ids:
                raise ValueError("duplicate match ID")
            ids.add(mid)
            result = history.features(position_accounts(match["radiant_players"]),
                                      position_accounts(match["dire_players"]), asof=match["start_ts"],
                                      time_policy=time_policy)
            result["match_id"] = mid
            rows.append(json.dumps(result, ensure_ascii=False))
    if not rows:
        raise ValueError("no matches")
    write_new(output, "\n".join(rows) + "\n")
    return {"rows": len(rows), "snapshots": len(paths), "output": str(output), "time_policy": time_policy}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    collect_parser = sub.add_parser("collect")
    collect_parser.add_argument("--url", required=True)
    collect_parser.add_argument("--output", required=True)
    export_parser = sub.add_parser("export")
    export_parser.add_argument("--snapshots", required=True)
    export_parser.add_argument("--matches", required=True)
    export_parser.add_argument("--output", required=True)
    export_parser.add_argument("--time-policy", choices=TIME_POLICIES, default="strict",
                               help="calendar_period assumes constant monthly earnings / weekly ranks (UTC); retrospective only")
    args = parser.parse_args()
    result = (collect(args.url, args.output) if args.command == "collect"
              else export(args.snapshots, args.matches, args.output, time_policy=args.time_policy))
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
