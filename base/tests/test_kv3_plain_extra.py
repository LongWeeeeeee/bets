"""Plain optional kills targets against a temporary tracked KV3 bundle."""
import json
import shutil
from dataclasses import replace
from types import SimpleNamespace

import numpy as np
import pytest

from test_kv3_dire_extra import PROD, FIXTURE, _candidate, _serve
from ml_panel import journal_row, load_specs


PLAIN = (("rad_ge30", "радиант ≥30", "≥30", "≤29"),
         ("dire_ge30", "дайр ≥30", "≥30", "≤29"),
         ("total_ge55", "тотал ≥55", "≥55", "≤54"))


def _bundle(path, plain):
    shutil.copytree(PROD, path)
    panel = json.loads((path / "panel.json").read_text())
    manifest = json.loads((path / "manifest.json").read_text())
    keys = {item[0] for item in PLAIN}
    panel["models"] = [entry for entry in panel["models"] if entry["key"] not in keys]
    manifest["targets"] = [key for key in manifest["targets"] if key not in keys]
    for key in keys:
        for suffix in (".cbm", ".calib.json"):
            (path / (key + suffix)).unlink(missing_ok=True)
    if plain:
        source = next(entry for entry in panel["models"] if entry["key"] == "dire_30_25")
        for key, title, positive, negative in PLAIN:
            entry = dict(source, key=key, title=title, positive=positive, negative=negative)
            panel["models"].append(entry)
            manifest["targets"].append(key)
            for suffix in (".cbm", ".calib.json"):
                shutil.copy2(path / ("dire_30_25" + suffix), path / (key + suffix))
    (path / "panel.json").write_text(json.dumps(panel))
    (path / "manifest.json").write_text(json.dumps(manifest))
    return path


def test_plain_predictions_and_six_fixture_predictions(monkeypatch, tmp_path):
    monkeypatch.setenv("ML_PANEL_KV3", "1")
    candidate = _candidate(_bundle(tmp_path / "plain", True))
    with np.load(FIXTURE, allow_pickle=False) as fixture:
        for i, key in enumerate(fixture["keys"]):
            verdicts = _serve(candidate, fixture["x928"][i], fixture["kv3"][i])
            by_key = {verdict.key: verdict for verdict in verdicts}
            assert by_key[str(key)].probability == pytest.approx(float(fixture["expected_b"][i]), abs=1e-9)
            assert [v.key for v in verdicts][-3:] == [item[0] for item in PLAIN]
            row = np.concatenate((fixture["x928"][i], fixture["kv3"][i])).reshape(1, -1)
            for target, *_ in PLAIN:
                raw = candidate.models[target].predict_proba(row, thread_count=1)[0, 1]
                assert by_key[target].probability == pytest.approx(candidate.specs[target].calibrate(raw), abs=1e-9)
                assert by_key[target].metadata["model"] == "B_kv3"
            assert set(item["key"] for item in journal_row(1, verdicts)["models"]) >= set(by_key)


def test_plain_absent_or_disabled_preserves_verdict_and_journal(monkeypatch, tmp_path):
    monkeypatch.setenv("ML_PANEL_KV3", "1")
    baseline = _candidate(_bundle(tmp_path / "base", False))
    extra = _candidate(_bundle(tmp_path / "extra", True))
    with np.load(FIXTURE, allow_pickle=False) as fixture:
        x928, kv3 = fixture["x928"][0], fixture["kv3"][0]
        before = _serve(baseline, x928, kv3)
        assert not any(v.key in {item[0] for item in PLAIN} for v in before)
        monkeypatch.setenv("ML_PANEL_KV3_PLAIN", "0")
        disabled = _serve(extra, x928, kv3)
        comparable = [replace(v, metadata={**v.metadata, "bundle_sha": baseline.manifest_sha256})
                      for v in disabled]
        assert comparable == before
        assert journal_row(1, comparable) == journal_row(1, before)


@pytest.mark.parametrize("mode", ["absent", "incomplete", "present", "disabled"])
def test_real_loader_requires_entry_and_both_files(monkeypatch, tmp_path, mode):
    from base import kills_v3_serving
    import kv3_panel_serving as serving

    directory = _bundle(tmp_path / "bundle", mode in ("present", "disabled"))
    if mode == "incomplete":
        panel = json.loads((directory / "panel.json").read_text())
        source = next(entry for entry in panel["models"] if entry["key"] == "dire_30_25")
        panel["models"].append(dict(source, key="rad_ge30", title="радиант ≥30",
                                    positive="≥30", negative="≤29"))
        (directory / "panel.json").write_text(json.dumps(panel))
        shutil.copy2(directory / "dire_30_25.cbm", directory / "rad_ge30.cbm")
    features = json.loads((directory / "feature_names.json").read_text())
    parameters = json.loads((directory / "manifest.json").read_text())["od3_parameters"]
    state = SimpleNamespace(serving_meta={
        "cutoff": 1782864000, "history_start": parameters["history_start"],
        "visibility_delay": parameters["visibility_delay"], "builder_params": parameters,
        "tp_names": features["kv3_source_names"] + [f"unused_{i}" for i in range(30)]})
    monkeypatch.setattr(kills_v3_serving, "load_state", lambda path: state)
    state_file = tmp_path / "state.npz"
    state_file.write_bytes(b"")
    monkeypatch.setenv("KV3_STATE_PATH", str(state_file))
    monkeypatch.setenv("KV3_PANEL_DIR", str(directory))
    if mode == "disabled":
        monkeypatch.setenv("ML_PANEL_KV3_PLAIN", "0")
    monkeypatch.setattr(serving, "_candidate", None)
    specs = {spec.key: spec for spec in load_specs(directory)}
    bundle = SimpleNamespace(columns=features["panel_columns"],
                             specs=tuple(specs[key] for key in serving.TARGETS))
    candidate = serving._load_uncached(bundle)
    present = mode == "present"
    assert all((key in candidate.models) is present for key, *_ in PLAIN)


def test_plain_prediction_error_falls_back_all_six(monkeypatch, tmp_path):
    monkeypatch.setenv("ML_PANEL_KV3", "1")
    candidate = _candidate(_bundle(tmp_path / "plain", True))

    def fail(*args, **kwargs):
        raise ValueError("plain model failed")

    candidate.models["dire_ge30"].predict_proba = fail
    with np.load(FIXTURE, allow_pickle=False) as fixture:
        verdicts = _serve(candidate, fixture["x928"][0], fixture["kv3"][0])
    assert len(verdicts) == 6
    assert all(v.metadata["model"] == "A_fallback" for v in verdicts)
    assert all("plain model failed" in v.metadata["reason"] for v in verdicts)
