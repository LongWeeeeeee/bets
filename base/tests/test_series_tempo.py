"""Series tempo ledger and the actual winner-off panel journal boundary."""

import dataclasses
import json
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import ml_panel
import prematch_panel_live as panel
import series_tempo as tempo
from base import win_model_veto as veto


@pytest.fixture(autouse=True)
def isolated_ledger(tmp_path, monkeypatch):
    monkeypatch.setenv("SERIES_TEMPO_LEDGER", str(tmp_path / "ledger.json"))
    monkeypatch.setenv("SERIES_TEMPO_SHADOW", "1")
    monkeypatch.setattr(tempo, "_loaded_path", None)
    yield
    monkeypatch.setattr(tempo, "_loaded_path", None)


def _entry(match_id, series_id, game, rad, dire, game_time=1000, series_type=None,
           team_ids=(1, 2)):
    return {str(match_id): {"match_id": match_id, "series_id": series_id,
                            "series_game_number": game, "radiant_score": rad,
                            "dire_score": dire, "game_time": game_time,
                            "series_type": series_type,
                            "radiant_team_id": team_ids[0], "dire_team_id": team_ids[1]}}


def _lineups():
    return ({f"pos{i}": {"hero_id": i, "account_id": i} for i in range(1, 6)},
            {f"pos{i}": {"hero_id": i + 5, "account_id": i + 5} for i in range(1, 6)})


B_BUNDLE = Path(__file__).resolve().parents[2] / "ml-models" / "prematch_panel_kv3"


def _b_total_spec():
    return {spec.key: spec for spec in ml_panel.load_specs(B_BUNDLE)}["total_55_50"]


def _off_journal(monkeypatch, tmp_path, match_id, probability=0.56):
    """Run the real evaluate_map from winner-off; stub only its heavy score inputs."""
    journal = tmp_path / "ml_panel.jsonl"
    monkeypatch.setattr(ml_panel, "DEFAULT_JOURNAL", journal)
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "0")
    monkeypatch.setenv("ML_PANEL_KV3", "0")
    monkeypatch.setattr(panel, "ENABLED", True)
    import kv3_panel_serving
    monkeypatch.setattr(kv3_panel_serving, "_specs",
                        {spec.key: spec for spec in ml_panel.load_specs(B_BUNDLE)})
    monkeypatch.setattr(panel, "HYBRID_ENABLED", False)
    monkeypatch.setattr(panel, "DRAFT_KEYS", ())
    monkeypatch.setattr(panel, "_load", lambda: {"bundle": SimpleNamespace(ready=True),
                                                "tables": (None, None), "snap": None})
    monkeypatch.setattr(panel, "_dict_block", lambda *_: None)
    monkeypatch.setattr(panel, "live_rating_state", lambda: None)
    monkeypatch.setitem(sys.modules, "public_kills_block", SimpleNamespace(block=lambda *_: None))
    monkeypatch.setitem(sys.modules, "team_ratings", SimpleNamespace(block=lambda *a, **k: None))
    monkeypatch.setitem(sys.modules, "hero_side_tables", SimpleNamespace(sym_block=lambda *a: []))
    verdict = ml_panel.ModelVerdict("total_55_50", "Total", "Radiant", probability,
                                    0.55, 1.0, True,
                                    metadata={"model": "B_kv3", "bundle_sha": "fixture"})
    monkeypatch.setitem(sys.modules, "prematch_panel_scorer", SimpleNamespace(
        block_from_matrix=lambda *a: {}, block_from_prod_features=lambda *a, **k: {},
        score=lambda *a, **k: [verdict]))
    monkeypatch.setitem(sys.modules, "duration43_serving", SimpleNamespace(
        replace_verdict=lambda verdicts, *a, **k: verdicts,
        status=lambda: {"ready": False}))
    monkeypatch.setitem(sys.modules, "kills_transfer_serving", SimpleNamespace(
        forecast_probabilities=lambda *a: (0.6, 0.4, 0.5),
        manifest_history_date=lambda: None, render=lambda *a: "kills panel"))
    veto.win_prediction_ex(*_lineups(), "Team A", "Team B",
                           match={"id": match_id, "match_id": match_id})
    assert journal.exists(), veto._LAST_PANEL.get("error")
    return json.loads(journal.read_text(encoding="utf-8").splitlines()[-1])["models"][0]


def test_shadow_formula_and_unshifted_journal_probability(tmp_path, monkeypatch):
    monkeypatch.setenv("SERIES_TEMPO_APPLY", "0")   # rollback switch = journal-only shadow
    tempo.observe(_entry(101, 55, 1, 31, 40), now=100)
    tempo.observe(_entry(102, 55, 2, 0, 0), now=101)
    row = _off_journal(monkeypatch, tmp_path, 102)
    shadow = row["metadata"]["series_tempo"]
    base_logit = math.log(0.56 / 0.44)
    sigmoid = lambda value: 1 / (1 + math.exp(-value))
    assert (shadow["series_id"], shadow["game_number"], shadow["n_prev"]) == (55, 2, 1)
    assert shadow["link"] == "sid"
    assert shadow["prev_total_mean"] == 71
    assert shadow["model"] == "B_kv3"
    assert abs(shadow["p_level"] - sigmoid(base_logit + tempo.A_LEVEL)) <= 1e-12
    assert abs(shadow["p_tempo"] - sigmoid(base_logit + tempo.A + tempo.BETA * (71 - tempo.MU))) <= 1e-12
    assert row["p"] == 0.56
    assert (shadow["applied"], shadow["skip"]) == (False, "switch_off")
    assert row["metadata"]["bundle_sha"] == "fixture"


def test_off_switch_and_unknown_match_do_not_attach(tmp_path, monkeypatch):
    tempo.observe(_entry(101, 55, 1, 31, 40), now=100)
    monkeypatch.setenv("SERIES_TEMPO_SHADOW", "0")
    before = (tmp_path / "ledger.json").read_bytes()
    tempo.observe(_entry(102, 55, 2, 0, 0), now=101)
    assert (tmp_path / "ledger.json").read_bytes() == before
    assert "series_tempo" not in _off_journal(monkeypatch, tmp_path, 101)["metadata"]
    assert tempo.shadow(0.5, 101) is None
    monkeypatch.setenv("SERIES_TEMPO_SHADOW", "1")
    assert "series_tempo" not in _off_journal(monkeypatch, tmp_path, 999)["metadata"]


def test_throttle_new_map_prune_and_time_order(tmp_path):
    path = tmp_path / "ledger.json"
    tempo.observe(_entry(101, 55, 1, 20, 20), now=100)
    assert json.loads(path.read_text())["55"]["101"]["kills"] == 40
    tempo.observe(_entry(101, 55, 1, 21, 20, 1010), now=101)
    assert json.loads(path.read_text())["55"]["101"]["kills"] == 40
    tempo.observe(_entry(102, 55, 2, 2, 3), now=102)
    persisted = json.loads(path.read_text())["55"]
    assert persisted["101"]["kills"] == 41
    assert (persisted["101"]["first_seen_ts"], persisted["101"]["last_seen_ts"]) == (100, 101)
    assert tempo.lookup(102) == (55, 2, [41])
    tempo.observe(_entry(103, 66, 1, 0, 0), now=102 + 48 * 3600 + 1)
    assert "55" not in json.loads(path.read_text())


def test_replace_failure_leaves_old_ledger(tmp_path, monkeypatch):
    path = tmp_path / "ledger.json"
    tempo.observe(_entry(101, 55, 1, 20, 20), now=100)
    old = path.read_bytes()
    def fail_replace(*_):
        raise OSError("simulated crash before replace")
    monkeypatch.setattr(tempo.os, "replace", fail_replace)
    tempo.observe(_entry(102, 55, 2, 0, 0), now=101)
    assert path.read_bytes() == old


def test_restart_loads_ledger_and_stale_bridge_does_not_keep_it_alive(tmp_path, monkeypatch):
    first = _entry(101, 55, 1, 31, 40)
    first["101"]["timestamp"] = 100
    tempo.observe(first, now=100)
    monkeypatch.setattr(tempo, "_loaded_path", None)
    assert tempo.lookup(101) == (55, 1, [])
    tempo.observe(first, now=100 + 48 * 3600 + 1)
    assert "55" not in json.loads((tmp_path / "ledger.json").read_text())


def test_invalid_game_number_still_uses_ledger():
    tempo.observe(_entry(101, 55, 0, 20, 20), now=100)
    assert tempo.shadow(0.5, 101)["continuation_source"] is None
    tempo.observe(_entry(102, 55, 0, 0, 0), now=101)
    shadow = tempo.shadow(0.5, 102)
    assert (shadow["n_prev"], shadow["prev_total_mean"], shadow["continuation_source"]) == (1, 40, "ledger")


def test_pair_linkage_with_real_map_two_shape(tmp_path, monkeypatch):
    capture = Path(__file__).parent / "fixtures" / "series_tempo" / "snap_20260924T220949.json"
    second = json.loads(capture.read_text(encoding="utf-8"))
    second_row = second["9014406398"]
    assert (second_row["series_id"], second_row["series_game_number"], second_row["series_type"]) == (None, 2, 1)
    first = _entry(9000000001, None, 1, 31, 40, series_type=1,
                   team_ids=(second_row["dire_team_id"], second_row["radiant_team_id"]))
    first["9000000001"]["timestamp"] = second_row["timestamp"] - 3600
    tempo.observe(first, now=first["9000000001"]["timestamp"])
    tempo.observe(second, now=second_row["timestamp"])
    row = _off_journal(monkeypatch, tmp_path, second_row["match_id"])
    shadow = row["metadata"]["series_tempo"]
    assert (shadow["link"], shadow["series_id"], shadow["game_number"]) == ("pair", None, 2)
    assert (shadow["n_prev"], shadow["prev_total_mean"], shadow["p_raw"]) == (1, 71, 0.56)
    assert row["p"] == round(shadow["p_tempo"], 6)


def test_pair_does_not_link_stale_previous_map(monkeypatch):
    monkeypatch.setenv("SERIES_TEMPO_LINK_WINDOW_S", "14400")
    tempo.observe(_entry(201, None, 1, 20, 30, series_type=1), now=100)
    tempo.observe(_entry(202, None, 2, 0, 0, series_type=1), now=100 + 14401)
    shadow = tempo.shadow(0.5, 202)
    assert shadow is not None
    assert (shadow["link"], shadow["n_prev"], shadow["prev_total_mean"]) == ("pair", 0, None)
    assert (shadow["continuation_source"], shadow["p_tempo"]) == ("sourcetv_game_number", None)


def test_late_last_poll_within_60_seconds_still_links():
    tempo.observe(_entry(201, None, 2, 20, 30), now=100)
    tempo.observe(_entry(202, None, 2, 0, 0), now=200)
    tempo.observe(_entry(201, None, 2, 21, 36), now=230)
    shadow = tempo.shadow(0.5, 202)
    assert (shadow["n_prev"], shadow["prev_total_mean"], shadow["continuation_source"]) == (1, 57, "ledger")


def test_pair_does_not_link_different_known_series_type():
    tempo.observe(_entry(301, None, 1, 20, 30, series_type=1), now=100)
    tempo.observe(_entry(302, None, 2, 0, 0, series_type=2), now=101)
    shadow = tempo.shadow(0.5, 302)
    assert shadow is not None
    assert (shadow["link"], shadow["n_prev"], shadow["prev_total_mean"]) == ("pair", 0, None)


def test_pair_keeps_distinct_matches_even_with_same_game_number():
    tempo.observe(_entry(301, None, 1, 20, 30, series_type=1), now=100)
    tempo.observe(_entry(302, None, 1, 31, 40, series_type=1), now=200)
    tempo.observe(_entry(303, None, 1, 1, 1, series_type=1), now=150)
    tempo.observe(_entry(302, None, 1, 1, 1, game_time=1001, series_type=1), now=150)
    tempo.observe(_entry(304, None, 2, 0, 0, series_type=1), now=201)
    shadow = tempo.shadow(0.5, 304)
    assert shadow is not None
    assert (shadow["n_prev"], shadow["prev_total_mean"]) == (3, 41)
    assert tempo.shadow(0.5, 301)["n_prev"] == 0
    assert tempo.shadow(0.5, 302)["prev_total_mean"] == 26


def test_reappearing_match_keeps_one_slot_and_first_seen(tmp_path):
    tempo.observe(_entry(301, None, 2, 20, 30, series_type=1), now=100)
    tempo.observe({}, now=120)
    tempo.observe(_entry(301, None, 2, 21, 36, series_type=1), now=150)
    maps = json.loads((tmp_path / "ledger.json").read_text())["pair:1:2"]
    assert list(maps) == ["301"]
    assert (maps["301"]["first_seen_ts"], maps["301"]["last_seen_ts"], maps["301"]["kills"]) == (100, 150, 57)


def test_old_game_number_ledger_is_ignored(tmp_path, monkeypatch):
    path = tmp_path / "ledger.json"
    path.write_text(json.dumps({"55": {"1": {"match_id": 101, "kills": 99, "seen_ts": 100}}}))
    monkeypatch.setattr(tempo, "_loaded_path", None)
    assert tempo.shadow(0.5, 101) is None
    tempo.observe(_entry(102, 55, 2, 0, 0), now=200)
    assert tempo.shadow(0.5, 102)["n_prev"] == 0


def test_malformed_ledger_row_is_dropped_and_shadow_recovers(tmp_path, monkeypatch):
    # A parseable row with a non-numeric timestamp or kills used to make every later
    # observe() raise inside the prune loop, so the shadow stopped for good, even
    # across restarts (hard-verifier C3, 2026-09-25).
    path = tmp_path / "ledger.json"
    good = {"match_id": 101, "kills": 57, "game_time": 2500, "series_type": 1,
            "sourcetv_game_number": 1, "first_seen_ts": 100, "last_seen_ts": 1000}
    bad_ts = dict(good, match_id=102, last_seen_ts="x")
    bad_kills = dict(good, match_id=104, kills=None)
    path.write_text(json.dumps({"pair:1:2": {"102": bad_ts, "101": good, "104": bad_kills}}))
    monkeypatch.setattr(tempo, "_loaded_path", None)
    tempo.observe(_entry(103, None, 2, 0, 0, series_type=1), now=1500)
    assert sorted(json.loads(path.read_text())["pair:1:2"]) == ["101", "103"]
    shadow = tempo.shadow(0.5, 103)
    assert (shadow["n_prev"], shadow["prev_total_mean"], shadow["continuation_source"]) == (1, 57, "ledger")


def test_positive_series_id_takes_priority_if_feed_adds_it_later():
    tempo.observe(_entry(501, None, 2, 0, 0, series_type=1), now=100)
    tempo.observe(_entry(502, 77, 1, 25, 30, series_type=1), now=101)
    tempo.observe(_entry(501, 77, 2, 1, 1, series_type=1), now=102)
    shadow = tempo.shadow(0.5, 501)
    assert shadow is not None
    assert (shadow["link"], shadow["series_id"], shadow["prev_total_mean"]) == ("sid", 77, 55)


def test_zero_team_id_is_unknown_without_series_id():
    tempo.observe(_entry(401, None, 2, 0, 0, team_ids=(0, 2)), now=100)
    assert tempo.shadow(0.5, 401) is None


def test_real_captured_series_boundary(tmp_path, monkeypatch):
    fixture_dir = Path(__file__).parent / "fixtures" / "series_tempo"
    names = ("215735", "220949", "221901", "224335")
    msk = timezone(timedelta(hours=3))
    for suffix in names:
        path = fixture_dir / f"snap_20260924T{suffix}.json"
        captured_at = datetime.strptime(path.stem.removeprefix("snap_"), "%Y%m%dT%H%M%S")
        snapshot = json.loads(path.read_text(encoding="utf-8"))
        assert snapshot[next(iter(snapshot))]["series_game_number"] == 2
        if suffix == "221901":
            assert (snapshot["9014406398"]["radiant_score"], snapshot["9014406398"]["dire_score"]) == (21, 36)
        if suffix == "224335":
            assert snapshot["9014519155"]["game_time"] == -79
        tempo.observe(snapshot,
                      now=captured_at.replace(tzinfo=msk).timestamp())
    row = _off_journal(monkeypatch, tmp_path, 9014519155)
    shadow = row["metadata"]["series_tempo"]
    total = 57
    assert (shadow["link"], shadow["n_prev"], shadow["prev_total_mean"]) == ("pair", 1, total)
    assert shadow["sourcetv_game_number"] == 2
    assert shadow["continuation_source"] == "ledger"
    logit = math.log(0.56 / 0.44)
    assert abs(shadow["p_level"] - 1 / (1 + math.exp(-logit - tempo.A_LEVEL))) <= 1e-12
    assert abs(shadow["p_tempo"] - 1 / (1 + math.exp(-logit - tempo.A - tempo.BETA * (total - tempo.MU)))) <= 1e-12
    # Stage B (owner 25.09): the panel serves the tempo-corrected probability through B's own spec.
    assert shadow["applied"] is True and shadow["variant"] == "tempo" and shadow["p_raw"] == 0.56
    assert abs(row["p"] - round(shadow["p_tempo"], 6)) <= 1e-6
    spec = _b_total_spec()
    conf = shadow["p_tempo"]
    assert row["side"] == spec.positive
    assert (row["band_hit"], row["band_n"]) == spec.band_hit(conf)
    assert row["ok"] is (conf >= spec.threshold)
    served = next(v for v in veto._LAST_PANEL["verdicts"] if v.key == "total_55_50")
    assert served.probability == conf and served.side == spec.positive
    assert row["odds"] == round(spec.fair_odds(conf), 4)


def _verdict(p, model="B_kv3", fill=1.0, draft_share=None):
    spec = _b_total_spec()
    return ml_panel.ModelVerdict("total_55_50", spec.title,
                                 spec.positive if p >= 0.5 else spec.negative, p,
                                 spec.threshold, fill, False, draft_share=draft_share,
                                 metadata={"model": model})


def _correction(p, n_prev=1, mean=70.0, game=2, model="B_kv3"):
    tempo.observe(_entry(501, None, game, int(mean) // 2, int(mean) - int(mean) // 2, series_type=1), now=100)
    tempo.observe(_entry(502, None, game, 0, 0, series_type=1), now=200)
    shadow = tempo.shadow(p, 502 if n_prev else 501, model)
    return shadow


def test_serve_tempo_recomputes_side_band_odds_from_spec():
    spec = _b_total_spec()
    correction = _correction(0.48)
    served, info = tempo.serve(_verdict(0.48), spec, correction)
    assert info["applied"] and info["variant"] == "tempo" and info["p_raw"] == 0.48
    assert served.probability == correction["p_tempo"] > 0.5
    assert served.side == spec.positive
    conf = served.probability
    assert (served.band_hit, served.band_n) == spec.band_hit(conf)
    assert served.odds == spec.fair_odds(conf)
    assert served.ok is False                    # B threshold 0.99


def test_serve_level_only_from_sourcetv_game_number():
    spec = _b_total_spec()
    tempo.observe(_entry(601, None, 2, 10, 10, series_type=1), now=100)
    correction = tempo.shadow(0.7, 601, "B_kv3")
    assert (correction["n_prev"], correction["continuation_source"]) == (0, "sourcetv_game_number")
    served, info = tempo.serve(_verdict(0.7), spec, correction)
    assert info["variant"] == "level" and served.probability == correction["p_level"] > 0.7


def test_serve_leaves_first_map_fallback_and_draft_gated_verdicts():
    spec = _b_total_spec()
    tempo.observe(_entry(701, None, 1, 10, 10, series_type=1), now=100)
    first = tempo.shadow(0.7, 701, "B_kv3")
    verdict = _verdict(0.7)
    assert tempo.serve(verdict, spec, first) == (verdict, {**first, "applied": False, "skip": "first_map"})
    correction = _correction(0.7, model="A_fallback")
    fallback = _verdict(0.7, model="A_fallback")
    assert tempo.serve(fallback, spec, correction)[0] is fallback
    gated = _verdict(0.7, draft_share=-0.1)
    assert tempo.serve(gated, spec, _correction(0.7))[1]["skip"] == "draft_gate"
    assert tempo.serve(verdict, None, _correction(0.7))[1]["skip"] == "spec_unavailable"


def test_serve_keeps_min_fill_gate(monkeypatch):
    spec = dataclasses.replace(_b_total_spec(), threshold=0.55)
    correction = _correction(0.7)
    assert tempo.serve(_verdict(0.7, fill=1.0), spec, correction)[0].ok is True
    assert tempo.serve(_verdict(0.7, fill=0.5), spec, correction)[0].ok is False
