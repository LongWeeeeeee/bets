"""Archive complete public Valve division responses; never infer Steam identities."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from base.player_metadata import REGIONS
from base.tools.player_metadata import write_new


def source_url(region):
    if region not in REGIONS:
        raise ValueError("unknown leaderboard division")
    return ("https://www.dota2.com/webapi/ILeaderboard/GetDivisionLeaderboard/"
            "v0001?division=" + region + "&leaderboard=0")


def normalize(raw, region, observed_at):
    """Retain tied ranks and unknown account IDs, without truncating the table."""
    url = source_url(region)
    data = json.loads(raw)
    posted = data.get("time_posted")
    rows = data.get("leaderboard")
    if type(posted) is not int or not 0 < posted <= observed_at + 300:
        raise ValueError("invalid source publication time")
    if not isinstance(rows, list) or not 1 <= len(rows) <= 20000:
        raise ValueError("empty or oversized leaderboard")
    previous = 0
    players = []
    for row in rows:
        rank = row.get("rank")
        if type(rank) is not int or rank <= 0 or rank < previous:
            raise ValueError("invalid or unsorted rank")
        if not isinstance(row.get("name"), str):
            raise ValueError("missing player name")
        previous = rank
        players.append({**row, "account_id": None})
    return {"schema": "valve-regional-rank-snapshot-v1", "region": region,
            "observed_at": observed_at, "rank_updated_at": posted,
            "source_url": url, "source_sha256": hashlib.sha256(raw).hexdigest(),
            "identity_status": "unresolved_public_leaderboard_has_no_account_id",
            "players": players}


def verify_snapshot(path):
    path = Path(path)
    snapshot = json.loads(path.read_bytes())
    expected = normalize(path.with_name("source.json").read_bytes(),
                         snapshot["region"], snapshot["observed_at"])
    if expected != snapshot:
        raise ValueError("snapshot differs from retained source")
    return snapshot


def collect(output, *, get=None, pause=time.sleep):
    if get is None:
        import requests
        get = requests.get
    run = Path(output) / str(time.time_ns())
    manifest = {"schema": "valve-rank-collection-v1", "started_at": time.time(),
                "regions": {}, "complete": False}
    for region in REGIONS:
        result = {"url": source_url(region)}
        manifest["regions"][region] = result
        try:
            response = get(result["url"], timeout=30, allow_redirects=False)
            result["http_status"] = response.status_code
            raw = response.content
            # Retain even malformed/error responses as evidence of a failed run.
            write_new(run / region / "source.json", raw.decode("utf-8"))
            if response.status_code != 200:
                raise ValueError("HTTP " + str(response.status_code))
            snapshot = normalize(raw, region, time.time())
            path = run / region / "snapshot.json"
            write_new(path, json.dumps(snapshot, ensure_ascii=False) + "\n")
            verify_snapshot(path)
            result.update(status="ok", players=len(snapshot["players"]),
                          source_sha256=snapshot["source_sha256"],
                          rank_updated_at=snapshot["rank_updated_at"])
        except (ValueError, OSError, TypeError, AttributeError) as exc:
            result.update(status="error", error=str(exc))
        except Exception as exc:
            # requests transport exceptions are optional until collection runs.
            result.update(status="error", error=type(exc).__name__ + ": " + str(exc))
        if result.get("http_status") in (403, 429):
            break
        if region != REGIONS[-1]:
            pause(1)
    manifest["finished_at"] = time.time()
    manifest["complete"] = (len(manifest["regions"]) == len(REGIONS)
                            and all(r["status"] == "ok" for r in manifest["regions"].values()))
    write_new(run / "collection.json", json.dumps(manifest, indent=2) + "\n")
    return {"manifest": str(run / "collection.json"), **manifest}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    result = collect(args.output)
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["complete"] else 1


if __name__ == "__main__":
    sys.exit(main())
