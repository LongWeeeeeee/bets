import json
from types import SimpleNamespace

import pytest

from base.tools.collect_rank_snapshots import collect, normalize, verify_snapshot


def payload(rows=None):
    return json.dumps({"time_posted": 100, "leaderboard": rows or [
        {"rank": 1, "name": "same", "country": "ua"},
        {"rank": 1, "name": "same", "country": "ua"},
        {"rank": 1006, "name": "yowaai", "country": "ua"}]}).encode()


def test_preserves_deep_ranks_ties_and_ambiguous_identity():
    snapshot = normalize(payload(), "europe", 200)
    assert [p["rank"] for p in snapshot["players"]] == [1, 1, 1006]
    assert all(p["account_id"] is None for p in snapshot["players"])
    assert snapshot["rank_updated_at"] == 100
    with pytest.raises(ValueError, match="publication time"):
        normalize(payload().replace(b'100,', b'1000,'), "europe", 200)


def test_collect_all_and_detect_source_tampering(tmp_path):
    response = SimpleNamespace(status_code=200, content=payload())
    result = collect(tmp_path, get=lambda *a, **k: response, pause=lambda _: None)
    assert result["complete"]
    assert len(result["regions"]) == 4
    paths = list(tmp_path.glob("*/europe/snapshot.json"))
    assert len(paths) == 1
    assert len(verify_snapshot(paths[0])["players"]) == 3
    paths[0].with_name("source.json").write_bytes(payload().replace(b'yowaai', b'other'))
    with pytest.raises(ValueError, match="differs"):
        verify_snapshot(paths[0])


@pytest.mark.parametrize("status", [403, 429])
def test_access_failure_stops_and_leaves_receipt(tmp_path, status):
    calls = []
    def get(*args, **kwargs):
        calls.append(args)
        return SimpleNamespace(status_code=status, content=b'access denied')
    result = collect(tmp_path, get=get, pause=lambda _: None)
    assert not result["complete"] and len(calls) == 1
    assert len(list(tmp_path.glob("*/collection.json"))) == 1
    assert list(tmp_path.glob("*/europe/source.json"))[0].read_bytes() == b'access denied'


def test_malformed_response_does_not_publish_snapshot(tmp_path):
    response = SimpleNamespace(status_code=200, content=b'{"time_posted":100,"leaderboard":[]}')
    result = collect(tmp_path, get=lambda *a, **k: response, pause=lambda _: None)
    assert not result["complete"]
    assert not list(tmp_path.glob("*/*/snapshot.json"))
