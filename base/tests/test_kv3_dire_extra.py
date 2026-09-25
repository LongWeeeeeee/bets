"""Optional Dire target against the real six-model bundle and the promoted Dire model.

The Dire payload comes from the tracked bundle (ml-models/prematch_panel_kv3), so
these tests run in a clean checkout and on serv1, where runtime/ does not exist.
"""
from __future__ import annotations

import json
import shutil
import sys
import subprocess
import hashlib
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from catboost import CatBoostClassifier

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import kv3_panel_serving as serving  # noqa: E402
from ml_panel import ModelVerdict, journal_row, load_specs  # noqa: E402


ROOT = Path(__file__).resolve().parents[2]
PROD = ROOT / "ml-models/prematch_panel_kv3"
PROMOTE = ROOT / "runtime/experiments/kills/dire30/promote.py"
FIXTURE = Path(__file__).parent / "fixtures/kv3_panel_b_maps.npz"


def _dire_source(tmp_path):
    """One-target Dire bundle built from the tracked promoted files."""
    source = tmp_path / "dire_source"
    if source.exists():
        return source
    source.mkdir(parents=True)
    panel = json.loads((PROD / "panel.json").read_text())
    entry = [model for model in panel["models"] if model["key"] == "dire_30_25"]
    assert len(entry) == 1, "promoted bundle must carry exactly one dire_30_25 entry"
    for suffix in (".cbm", ".calib.json"):
        shutil.copy2(PROD / ("dire_30_25" + suffix), source)
    shutil.copy2(PROD / "feature_names.json", source)
    manifest = json.loads((PROD / "manifest.json").read_text())
    manifest["targets"] = ["dire_30_25"]
    (source / "panel.json").write_text(json.dumps({"models": entry}))
    (source / "manifest.json").write_text(json.dumps(manifest))
    return source


def _copy_bundle(tmp_path, *, dire):
    target = tmp_path / "bundle"
    shutil.copytree(PROD, target)
    # Each scenario starts from the six-target bundle, even after production
    # has promoted the optional Dire target.
    for suffix in (".cbm", ".calib.json"):
        (target / ("dire_30_25" + suffix)).unlink(missing_ok=True)
    panel = json.loads((target / "panel.json").read_text())
    panel["models"] = [model for model in panel["models"]
                       if model["key"] != "dire_30_25"]
    manifest = json.loads((target / "manifest.json").read_text())
    manifest["targets"] = [key for key in manifest["targets"]
                           if key != "dire_30_25"]
    if dire:
        source = _dire_source(tmp_path)
        for suffix in (".cbm", ".calib.json"):
            shutil.copy2(source / ("dire_30_25" + suffix), target)
        position = next(i for i, model in enumerate(panel["models"])
                        if model["key"] == "total_55_50")
        panel["models"].insert(position,
                               json.loads((source / "panel.json").read_text())["models"][0])
        manifest["targets"].insert(position, "dire_30_25")
    (target / "panel.json").write_text(json.dumps(panel))
    (target / "manifest.json").write_text(json.dumps(manifest))
    return target


def _candidate(directory):
    features = json.loads((directory / "feature_names.json").read_text())
    specs = {spec.key: spec for spec in load_specs(directory)}
    models = {}
    for key in specs:
        model = CatBoostClassifier()
        model.load_model(str(directory / (key + ".cbm")))
        models[key] = model
    return SimpleNamespace(panel_columns=features["panel_columns"],
                           kv3_columns=features["kv3_columns"], specs=specs,
                           models=models,
                           manifest_sha256=hashlib.sha256((directory / "manifest.json").read_bytes()).hexdigest(),
                           cutoff=1782864000,
                           state=SimpleNamespace(serving_last_overvisible_seconds=0))


def _serve(candidate, x928, kv3):
    a = [ModelVerdict(key=key, title=candidate.specs[key].title,
                      side=candidate.specs[key].negative, probability=.2,
                      threshold=candidate.specs[key].threshold, fill=1, ok=False)
         for key in serving.TARGETS]
    return serving.replace_verdicts(None, x928, a,
                                    {"radiant_team_id": 1, "dire_team_id": 2},
                                    feature_vector=kv3, candidate=candidate)


def test_dire_model_on_same_row_and_six_fixture_predictions(monkeypatch, tmp_path):
    monkeypatch.setenv("ML_PANEL_KV3", "1")
    directory = _copy_bundle(tmp_path, dire=True)
    candidate = _candidate(directory)
    with np.load(FIXTURE, allow_pickle=False) as z:
        for i, key in enumerate(z["keys"]):
            verdicts = _serve(candidate, z["x928"][i], z["kv3"][i])
            by_key = {v.key: v for v in verdicts}
            assert by_key[str(key)].probability == pytest.approx(float(z["expected_b"][i]), abs=1e-9)
            assert list(by_key).index("dire_30_25") == list(by_key).index("rad_30_25") + 1
            row = np.concatenate((z["x928"][i], z["kv3"][i])).reshape(1, -1)
            raw = float(candidate.models["dire_30_25"].predict_proba(row, thread_count=1)[0, 1])
            assert by_key["dire_30_25"].probability == pytest.approx(
                candidate.specs["dire_30_25"].calibrate(raw), abs=1e-9)
            assert by_key["dire_30_25"].metadata["model"] == "B_kv3"


def test_absent_or_disabled_dire_preserves_verdicts_and_journal(monkeypatch, tmp_path):
    monkeypatch.setenv("ML_PANEL_KV3", "1")
    baseline = _candidate(_copy_bundle(tmp_path / "base", dire=False))
    extra = _candidate(_copy_bundle(tmp_path / "extra", dire=True))
    with np.load(FIXTURE, allow_pickle=False) as z:
        x928, kv3 = z["x928"][0], z["kv3"][0]
        before = _serve(baseline, x928, kv3)
        assert "dire_30_25" not in [v.key for v in before]
        monkeypatch.setenv("ML_PANEL_KV3_DIRE", "0")
        disabled = _serve(extra, x928, kv3)
        assert all(v.metadata["bundle_sha"] == extra.manifest_sha256 for v in disabled)
        comparable = [replace(v, metadata={**v.metadata,
                                            "bundle_sha": baseline.manifest_sha256})
                      for v in disabled]
        assert comparable == before
        assert journal_row(1, comparable) == journal_row(1, before)


def test_bad_optional_prediction_falls_back_all_six(monkeypatch, tmp_path):
    monkeypatch.setenv("ML_PANEL_KV3", "1")
    candidate = _candidate(_copy_bundle(tmp_path, dire=True))

    def fail(*args, **kwargs):
        raise ValueError("dire model failed")

    candidate.models["dire_30_25"].predict_proba = fail
    with np.load(FIXTURE, allow_pickle=False) as z:
        result = _serve(candidate, z["x928"][0], z["kv3"][0])
    assert len(result) == 6
    assert all(v.metadata["model"] == "A_fallback" for v in result)
    assert all("dire model failed" in v.metadata["reason"] for v in result)


@pytest.mark.skipif(not PROMOTE.exists(),
                    reason="promote.py is an offline tool under git-ignored runtime/")
def test_promotion_dry_run_and_real_only_change_dire_and_manifests(tmp_path):
    target = _copy_bundle(tmp_path, dire=False)
    before = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
              for p in target.iterdir() if p.is_file()}
    command = [sys.executable, str(PROMOTE), str(_dire_source(tmp_path)), str(target)]
    dry = subprocess.run(command + ["--dry-run"], capture_output=True, text=True, check=True)
    assert json.loads(dry.stdout)["dry_run"] is True
    assert {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in target.iterdir() if p.is_file()} == before
    real = subprocess.run(command, capture_output=True, text=True, check=True)
    result = json.loads(real.stdout)
    assert hashlib.sha256((target / "manifest.json").read_bytes()).hexdigest() == result["manifest_sha256"]
    assert set(p.name for p in target.iterdir()) == set(before) | {
        "dire_30_25.cbm", "dire_30_25.calib.json"}
    for name, sha in before.items():
        if name not in ("panel.json", "manifest.json"):
            assert hashlib.sha256((target / name).read_bytes()).hexdigest() == sha
    assert _keys_for_test(target) == list(serving.TARGETS[:5]) + ["dire_30_25", "total_55_50"]


@pytest.mark.parametrize("mode", ["absent", "incomplete", "present", "disabled"])
def test_real_loader_treats_dire_as_optional(monkeypatch, tmp_path, mode):
    from base import kills_v3_serving

    directory = _copy_bundle(tmp_path, dire=mode in ("present", "disabled"))
    if mode == "incomplete":
        panel = json.loads((directory / "panel.json").read_text())
        source = _dire_source(tmp_path)
        panel["models"].append(json.loads((source / "panel.json").read_text())["models"][0])
        (directory / "panel.json").write_text(json.dumps(panel))
        shutil.copy2(source / "dire_30_25.cbm", directory)
    features = json.loads((directory / "feature_names.json").read_text())
    manifest = json.loads((directory / "manifest.json").read_text())
    parameters = manifest["od3_parameters"]
    state = SimpleNamespace(serving_meta={
        "cutoff": 1782864000, "history_start": parameters["history_start"],
        "visibility_delay": parameters["visibility_delay"],
        "builder_params": parameters,
        "tp_names": features["kv3_source_names"] + [f"unused_{i}" for i in range(30)]})
    monkeypatch.setattr(kills_v3_serving, "load_state", lambda path: state)
    # load_state is stubbed; the loader still stats the state file, which is
    # git-ignored data and absent in a clean checkout.
    state_file = tmp_path / "state.npz"
    state_file.write_bytes(b"")
    monkeypatch.setenv("KV3_STATE_PATH", str(state_file))
    monkeypatch.setattr(serving, "_candidate", None)
    monkeypatch.setenv("KV3_PANEL_DIR", str(directory))
    if mode == "disabled":
        monkeypatch.setenv("ML_PANEL_KV3_DIRE", "0")
    specs = {spec.key: spec for spec in load_specs(directory)}
    bundle = SimpleNamespace(columns=features["panel_columns"],
                             specs=tuple(specs[key] for key in serving.TARGETS))
    candidate = serving._load_uncached(bundle)
    assert ("dire_30_25" in candidate.models) is (mode == "present")


def _keys_for_test(directory):
    return [item["key"] for item in json.loads((directory / "panel.json").read_text())["models"]]
