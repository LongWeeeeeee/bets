import json
import math
import os

import prematch_prediction_journal as journal
from prematch_prediction_journal import record_outcome, record_prediction


def _prediction(map_id="map-1", **extra):
    row = {
        "stable_match_id": map_id,
        "map_key": f"dltv:{map_id}:poll-99",
        "game_time": 100,
        "index": 7,
        "raw_probability": 0.61,
        "branch": "full",
        "features": {"a": 1, "b": 2},
        "artifact_sha256": "abc",
        "snapshot_ts": 10,
        "calibration": "none",
        "reason": "audit",
    }
    row.update(extra)
    return row


def _rows(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_prediction_distinguishes_maps_and_dedups_poll_suffix_and_restart(tmp_path):
    path = tmp_path / "predictions.jsonl"
    assert record_prediction(_prediction("map-1"), path)
    assert not record_prediction(_prediction("map-1", map_key="dltv:map-1:poll-100"), path)
    assert record_prediction(_prediction("map-2"), path)
    # A fresh process has no in-memory state, but the file still deduplicates.
    journal._PREDICTION_STATES.clear()
    assert not record_prediction(_prediction("map-1"), path)
    assert len(_rows(path)) == 2


def test_changed_prediction_same_map_is_kept(tmp_path):
    path = tmp_path / "predictions.jsonl"
    assert record_prediction(_prediction(), path)
    assert record_prediction(_prediction(raw_probability=0.62), path)
    assert len(_rows(path)) == 2


def test_prediction_nonfinite_values_are_json_null(tmp_path):
    path = tmp_path / "predictions.jsonl"
    assert record_prediction(_prediction(features={"nan": math.nan, "inf": math.inf}), path)
    row = _rows(path)[0]
    assert row["features"] == {"nan": None, "inf": None}
    assert row["schema_version"] == 2
    assert isinstance(row["prediction_id"], str)


def test_prediction_io_failure_can_retry(tmp_path, monkeypatch):
    path = tmp_path / "predictions.jsonl"
    original = type(path).open
    calls = {"n": 0}

    def fail_once(self, *args, **kwargs):
        if self == path and "a+" not in args and calls["n"] == 0:
            calls["n"] += 1
            raise OSError("temporary")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(type(path), "open", fail_once)
    assert not record_prediction(_prediction(), path)
    assert record_prediction(_prediction(), path)
    assert len(_rows(path)) == 1


def test_prediction_cache_reads_only_external_append_and_resets_after_replace(tmp_path, monkeypatch):
    path = tmp_path / "predictions.jsonl"
    assert record_prediction(_prediction("first"), path)
    starts = []
    original = journal._read_appended_rows

    def observe_read(target, cursor, consume):
        starts.append(cursor)
        return original(target, cursor, consume)

    monkeypatch.setattr(journal, "_read_appended_rows", observe_read)
    assert record_prediction(_prediction("second"), path)
    assert starts == []

    external_start = path.stat().st_size
    with path.open("a") as fh:
        fh.write(json.dumps({"prediction_id": "external"}) + "\n")
    assert record_prediction(_prediction("third"), path)
    assert starts == [external_start]

    replacement = tmp_path / "replacement.jsonl"
    replacement.write_text(json.dumps({"prediction_id": "replacement"}) + "\n")
    os.replace(replacement, path)
    starts.clear()
    assert record_prediction(_prediction("first"), path)
    assert starts == [0]


def test_prediction_preserves_torn_tail_with_separator_before_retrying_append(tmp_path):
    path = tmp_path / "predictions.jsonl"
    failed = b'{"prediction_id":"failed"'
    path.write_bytes(failed)
    assert record_prediction(_prediction(), path)
    raw = path.read_bytes()
    assert raw.startswith(failed + b"\n")
    assert json.loads(raw.splitlines()[1])["stable_match_id"] == "map-1"


def test_prediction_ingests_valid_unterminated_tail_before_dedup(tmp_path):
    path = tmp_path / "predictions.jsonl"
    row = _prediction()
    row["prediction_id"] = journal._prediction_id(row)
    path.write_text(json.dumps(row))
    assert not record_prediction(_prediction(), path)
    assert _rows(path) == [row]


def test_unknown_map_identity_normalizes_only_dltv_kills_suffix_and_keeps_teams(tmp_path):
    path = tmp_path / "predictions.jsonl"
    unknown = _prediction(stable_match_id=None, match_id=None,
                          map_key="https://www.dltv.org/matches/123.45",
                          team1="Radiant", team2="Dire")
    assert record_prediction(unknown, path)
    assert not record_prediction({**unknown,
                                  "map_key": "https://www.dltv.org/matches/123.54"}, path)
    assert record_prediction({**unknown, "map_key": "opaque.45"}, path)
    assert record_prediction({**unknown, "map_key": "opaque.54"}, path)

    no_key = {**unknown, "map_key": ""}
    assert record_prediction(no_key, path)
    assert record_prediction({**no_key, "team1": "Other radiant"}, path)


def test_outcome_dedup_and_conflict_never_overwrites(tmp_path):
    path = tmp_path / "outcomes.jsonl"
    assert record_outcome(42, True, 100, 200, "stratz", path)
    assert not record_outcome(42, True, 100, 200, "stratz", path)
    assert not record_outcome(42, False, 100, 200, "other", path)
    rows = _rows(path)
    assert len(rows) == 1
    assert rows[0]["match_id"] == 42
    assert rows[0]["radiant_win"] is True
    assert rows[0]["source"] == "stratz"


def test_outcome_requires_complete_typed_result(tmp_path):
    path = tmp_path / "outcomes.jsonl"
    assert not record_outcome(42, 1, 100, 200, "stratz", path)
    assert not record_outcome(True, True, 100, 200, "stratz", path)
    assert not record_outcome(42, True, 0, 200, "stratz", path)
    assert not record_outcome(42, True, 200, 100, "stratz", path)
    assert not path.exists()


def test_outcome_allows_equal_start_and_end(tmp_path):
    path = tmp_path / "outcomes.jsonl"
    assert record_outcome(42, True, 100, 100, "stratz", path)


def test_outcome_cache_ingests_external_append_without_rescanning(tmp_path, monkeypatch):
    path = tmp_path / "outcomes.jsonl"
    assert record_outcome(42, True, 100, 200, "stratz", path)
    starts = []
    original = journal._read_appended_rows

    def observe_read(target, cursor, consume):
        starts.append(cursor)
        return original(target, cursor, consume)

    monkeypatch.setattr(journal, "_read_appended_rows", observe_read)
    assert record_outcome(43, False, 100, 200, "stratz", path)
    assert starts == []

    external_start = path.stat().st_size
    with path.open("a") as fh:
        fh.write(json.dumps({"match_id": 44, "radiant_win": True}) + "\n")
    assert not record_outcome(44, True, 100, 200, "stratz", path)
    assert starts == [external_start]


def test_stratz_player_result_route_journals_completed_raw_result(monkeypatch):
    import stratz_map_result

    seen = []
    monkeypatch.setattr(stratz_map_result, "_journal_result",
                        lambda row, source: seen.append((row, source)))
    result = {"id": 77, "didRadiantWin": True, "startDateTime": 100,
              "endDateTime": 200, "players": [{"steamAccountId": 1}]}
    assert stratz_map_result.match_players(77, query_raw=lambda _: result) is result
    assert seen == [(result, "stratz_player_result")]
