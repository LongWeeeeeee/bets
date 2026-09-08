import json

import numpy as np

from base.build_laning_corpus import _consolidate, build_corpus, canonicalize_match


def _match(mid=10, ts=100, *, account=True, lane="DIRE_STOMP", nw=1234):
    players = []
    for radiant in (True, False):
        for pos in range(1, 6):
            player = {"isRadiant": radiant, "position": f"POSITION_{pos}",
                      "heroId": pos + (0 if radiant else 20)}
            if account:
                player["steamAccount"] = {"id": pos + (100 if not radiant else 0)}
            players.append(player)
    return {"id": mid, "startDateTime": ts, "durationSeconds": 660, "players": players,
            "radiantNetworthLeads": [0] * 10 + [nw], "bottomLaneOutcome": lane,
            "midLaneOutcome": "TIE", "topLaneOutcome": "RADIANT_STOMP"}


def test_mapping_missing_account_and_invalid_lane():
    row, reason = canonicalize_match(_match(account=False, lane="bogus"))
    assert reason is None
    assert row["accounts"] == (0,) * 10
    assert row["lane_labels"] == (-1, 2, 4)


def test_duplicate_positive_account_id_is_rejected_but_zero_is_allowed():
    match = _match()
    match["players"][1]["steamAccount"] = {"id": match["players"][0]["steamAccount"]["id"]}
    row, reason = canonicalize_match(match)
    assert row is None
    assert reason == "duplicate_account_id"


def test_anonymous_and_sentinel_accounts_and_missing_nw_are_kept():
    match = _match()
    match["durationSeconds"] = 600
    match["players"][0]["steamAccount"] = {"id": 4294967295}
    match["players"][1]["steamAccount"] = "anonymous"
    match.pop("radiantNetworthLeads")
    row, reason = canonicalize_match(match)
    assert reason is None
    assert row["accounts"][:2] == (0, 0)
    assert np.isnan(row["team_nw10"])


def test_cache_hit_preserves_source_counts(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "part.json").write_text(json.dumps([_match(), {"id": "bad"}]), encoding="utf-8")
    first = build_corpus(source, tmp_path / "out", workers=1)
    second = build_corpus(source, tmp_path / "out", workers=1)
    assert first["sources"][0]["counts"] == second["sources"][0]["counts"]
    assert second["sources"][0]["cache"] == "hit"


def test_imported_validation_change_invalidates_cached_rows(tmp_path, monkeypatch):
    import base.build_laning_corpus as builder
    source = tmp_path / "source"
    source.mkdir()
    (source / "part.json").write_text(json.dumps([_match()]), encoding="utf-8")
    first = builder.build_corpus(source, tmp_path / "out", workers=1)
    assert first["rows"] == 1

    def changed_heroes_validation(players):
        return None, "changed_validation"

    monkeypatch.setattr(builder, "_canonical_heroes", changed_heroes_validation)
    second = builder.build_corpus(source, tmp_path / "out", workers=1)
    assert second["sources"][0]["cache"] != "hit"
    assert second["rows"] == 0


def test_identical_missing_nw_rows_are_not_conflicts(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    match = _match()
    match.pop("radiantNetworthLeads")
    payload = json.dumps([match])
    (source / "a.json").write_text(payload, encoding="utf-8")
    (source / "b.json").write_text(payload, encoding="utf-8")
    manifest = build_corpus(source, tmp_path / "out", workers=1)
    assert manifest["global_counts"] == {"duplicate_conflict": 0, "duplicate_identical": 1}


def test_consolidate_empty_parts():
    arrays, counts = _consolidate([])
    assert arrays["mid"].shape == (0,)
    assert arrays["heroes"].shape == (0, 10)
    assert counts == {"duplicate_conflict": 0, "duplicate_identical": 0}
    arrays, counts = _consolidate([_consolidate([])[0]])
    assert arrays["mid"].shape == (0,)
    assert counts == {"duplicate_conflict": 0, "duplicate_identical": 0}


def test_fixture_build_sorts_and_rejects_conflicting_duplicate(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "a.json").write_text(json.dumps([_match(2, 200), _match(1, 300)]), encoding="utf-8")
    (source / "b.json").write_text(json.dumps([_match(2, 200), _match(1, 301)]), encoding="utf-8")
    (source / "scan_manifest.json").write_text("{}", encoding="utf-8")
    output = tmp_path / "out"
    manifest = build_corpus(source, output, workers=1)
    with np.load(output / "rows.npz", allow_pickle=False) as rows:
        assert rows["mid"].tolist() == [2]
        assert rows["ts"].tolist() == [200]
        assert rows["team_nw10"].tolist() == [1234]
        assert rows["lane_labels"].tolist() == [[0, 2, 4]]
    assert manifest["global_counts"] == {"duplicate_conflict": 1, "duplicate_identical": 1}
    assert manifest["skipped_metadata"] == ["scan_manifest.json"]
    assert manifest["sources"][0]["source_sha256"]
