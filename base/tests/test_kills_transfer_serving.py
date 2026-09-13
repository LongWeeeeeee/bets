"""Causality, orientation and text regression coverage for E281 serving."""
import numpy as np
import ast
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kills_transfer_features import causal_pro_context
from kills_transfer_serving import query_context, render


def test_filtered_query_matches_training_and_excludes_current_outcome():
    n = 42
    rows = {"mids": np.arange(1, n+1), "ts": np.arange(n)*4000+100,
            "ends": np.arange(n)*4000+1900, "durations": np.full(n, 1800),
            "heroes": np.tile(np.arange(1, 11), (n, 1)),
            "accounts": np.tile(np.arange(101, 111), (n, 1)),
            "teams": np.tile([1001, 1002], (n, 1)), "sids": np.zeros(n, int),
            "stats": np.random.default_rng(81).uniform(1, 50, (n, 10, 6))}
    # A map for unrelated identities and a boundary map ending at query start.
    rows["accounts"][5] += 10000
    rows["teams"][5] += 10000
    rows["ends"][-2] = rows["ts"][-1]
    expected, names = causal_pro_context(rows)
    got, got_names = query_context(rows, rows["heroes"][-1], rows["accounts"][-1],
                                    rows["teams"][-1], rows["ts"][-1], rows["mids"][-1])
    np.testing.assert_allclose(got, expected[-1:], equal_nan=True)
    assert names == got_names
    rows["stats"][-1] = 100000
    again, _ = query_context(rows, rows["heroes"][-1], rows["accounts"][-1],
                             rows["teams"][-1], rows["ts"][-1], rows["mids"][-1])
    np.testing.assert_array_equal(again, got)


def test_unknown_identities_do_not_share_history():
    rows = {"mids": np.array([1]), "ts": np.array([10]), "ends": np.array([20]),
            "durations": np.array([10]), "heroes": np.arange(1, 11)[None],
            "accounts": np.zeros((1, 10), int), "teams": np.zeros((1, 2), int),
            "sids": np.array([0]), "stats": np.ones((1, 10, 6))}
    got, _ = query_context(rows, tuple(range(1, 11)), (0,)*10, (0, 0), 100, 2)
    assert np.isnan(got[:, :, :24]).all()
    assert (got[:, :, 24:] == 0).all()


def test_render_preserves_three_independent_probabilities():
    text = render((.72, .63, .81), "11.09.2026")
    assert "Radiant ≥30 килов: 72.0%" in text
    assert "Dire ≥30 килов: 63.0%" in text
    assert "Карта ≥55 килов: 81.0%" in text
    assert "история до 11.09.2026" in text


def test_render_stars_only_events_at_or_above_default_threshold(monkeypatch):
    monkeypatch.delenv("KILLS_TRANSFER_STAR_MIN_PROB", raising=False)
    lines = render((.72, .63, .81), "11.09.2026").split("\n")
    assert lines[1] == "Radiant ≥30 килов: 72.0% ★"
    assert lines[2] == "Dire ≥30 килов: 63.0%"
    assert lines[3] == "Карта ≥55 килов: 81.0% ★"
    # ровно на пороге — ★ есть (>=), чуть ниже — нет
    edge = render((.70, .699, .70), "11.09.2026").split("\n")
    assert edge[1].endswith(" ★") and edge[3].endswith(" ★")
    assert not edge[2].endswith("★")


def test_render_star_threshold_from_env_and_argument(monkeypatch):
    monkeypatch.setenv("KILLS_TRANSFER_STAR_MIN_PROB", "0.60")
    lines = render((.72, .63, .81), "11.09.2026").split("\n")
    assert lines[2] == "Dire ≥30 килов: 63.0% ★"
    monkeypatch.setenv("KILLS_TRANSFER_STAR_MIN_PROB", "garbage")
    assert render((.72, .63, .81), "11.09.2026").split("\n")[2] == "Dire ≥30 килов: 63.0%"
    monkeypatch.setenv("KILLS_TRANSFER_STAR_MIN_PROB", "0")
    assert render((.72, .63, .81), "11.09.2026").split("\n")[2] == "Dire ≥30 килов: 63.0%"
    monkeypatch.delenv("KILLS_TRANSFER_STAR_MIN_PROB", raising=False)
    explicit = render((.72, .63, .81), "11.09.2026", star_min=0.85).split("\n")
    assert not any(line.endswith("★") for line in explicit)


def test_betting_adapter_appends_text_without_modifying_verdicts(monkeypatch, capsys):
    import kills_transfer_serving
    tree = ast.parse((Path(__file__).resolve().parents[1] / "win_model_veto.py").read_text())
    adapter = next(node for node in ast.walk(tree) if isinstance(node, ast.Try)
                   and any(isinstance(child, ast.ImportFrom)
                           and child.module == "kills_transfer_serving" for child in node.body))
    code = compile(ast.fix_missing_locations(ast.Module(body=[adapter], type_ignores=[])), "adapter", "exec")
    panel = {"text": "EXISTING WINDOWS", "verdicts": ["unchanged"]}
    calls, errors = [], []

    def forecast(*args):
        calls.append(args)
        return (.72, .63, .81)

    monkeypatch.setattr(kills_transfer_serving, "forecast_probabilities", forecast)
    # Hermetic: history-date lookup would otherwise read the real bundle's
    # manifest.json (present locally but not guaranteed in every test env).
    monkeypatch.setattr(kills_transfer_serving, "manifest_history_date", lambda: "11.09.2026")
    scope = dict(_LAST_PANEL=panel, rh=(1,2,3,4,5), dh=(6,7,8,9,10), ra=(1,)*5, da=(2,)*5,
                 _rt_id=11, _dt_id=22, match={"id": 999}, elo_evaluation_timestamp=lambda _: 123,
                 _report_panel_silence=errors.append)
    exec(code, scope)
    assert calls[0][-3:] == ((11,22),123,999)
    assert panel["text"].startswith("EXISTING WINDOWS\nКилы ML")
    assert "Dire ≥30 килов: 63.0%" in panel["text"]
    assert panel["verdicts"] == ["unchanged"] and not errors
    assert panel["kills30"] == {"radiant": .72, "dire": .63, "total": .81}
    before = panel["text"]

    def fail(*_args):
        raise ValueError("bad bundle")

    monkeypatch.setattr(kills_transfer_serving, "forecast_probabilities", fail)
    exec(code, scope)
    assert panel["text"] == before and panel["verdicts"] == ["unchanged"]
    assert panel["kills30"] is None
    assert "E281 kills" in panel["kills_error"]
    assert "[kills_transfer] E281 kills" in capsys.readouterr().out
    exec(code, scope)
    assert capsys.readouterr().out == ""
