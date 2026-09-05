import json

from ELO.data_loader import load_matches, _parse_match


def raw_map(mid, start):
    return {"id": mid, "startDateTime": start, "durationSeconds": 600,
            "didRadiantWin": True, "radiantTeam": {"id": 10, "name": "A"},
            "direTeam": {"id": 20, "name": "B"},
            "players": [{"isRadiant": i < 5, "steamAccount": {"id": i + 1},
                         "position": f"POSITION_{i % 5 + 1}"} for i in range(10)]}


def test_streamed_object_records_counters_sorting_and_progress(tmp_path):
    late, early = raw_map(2, 200), raw_map(1, 100)
    invalid = dict(raw_map(3, 300), players=[])
    (tmp_path / "7.41e_part1.json").write_text(json.dumps({"2": late, "bad": invalid, "1": early, "null": None}))
    (tmp_path / "z_array.json").write_text(json.dumps([early]))
    progress = []
    matches, summary = load_matches(tmp_path, progress=lambda p, n: progress.append((p.name, n)))
    assert [m.match_id for m in matches] == [1, 2]
    assert [m.result_timestamp for m in matches] == [700, 800]
    assert [m.source_patch for m in matches] == ["7.41e", "7.41e"]
    assert summary == {"files": 2, "raw_matches": 4, "seen_matches": 4,
                       "loaded_matches": 2, "skipped_invalid": 1, "skipped_non_dict": 1}
    assert progress == [("7.41e_part1.json", 2), ("z_array.json", 2)]


def test_invalid_or_shared_account_ids_are_not_rating_participants():
    shared = raw_map(1, 100)
    shared["players"][5]["steamAccount"]["id"] = 1
    assert _parse_match(shared) is None
    zero = raw_map(2, 100)
    zero["players"][0]["steamAccount"]["id"] = 0
    assert _parse_match(zero) is None
    boolean = raw_map(3, 100)
    boolean["players"][0]["steamAccount"]["id"] = True
    assert _parse_match(boolean) is None


def test_experiment_deduplicates_overlapping_archive_parts(tmp_path, monkeypatch):
    from ELO import run_experiment
    from ELO.config import EvaluationConfig

    source, output = tmp_path / "source", tmp_path / "output"
    source.mkdir()
    first, second = raw_map(1, 1786000000), raw_map(2, 1786004000)
    (source / "7.41e_part1.json").write_text(json.dumps({"1": first}))
    (source / "7.41e_part2.json").write_text(json.dumps({"1": first, "2": second}))
    monkeypatch.setattr(run_experiment.sys, "argv", ["run_experiment", "--data-dir", str(source),
                                                    "--output-dir", str(output)])
    monkeypatch.setattr(run_experiment, "EvaluationConfig",
                        lambda: EvaluationConfig(evaluation_fraction=1, min_train_matches=0))
    run_experiment.main()
    report = json.loads((output / "report.json").read_text())
    assert report["dataset_summary"]["loaded_matches"] == 2
    assert report["dataset_summary"]["duplicate_records"] == 1
    assert all(model["matches"] == 2 for model in report["models"].values())
