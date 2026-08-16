"""Тесты панели предматчевых ML-моделей.

Проверяется то, что легко сломать незаметно: калибровка на границах, выбор
лучшего среди семейства по КАЛИБРОВАННОЙ уверенности, гейт заполненности и
неизменность формата журнала.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ml_panel import (  # noqa: E402
    JOURNAL_SCHEMA, ModelSpec, append_journal, atomic_write_specs, best_of,
    evaluate, journal_row, load_specs, render,
)

KNOTS_X = (-2.0, -1.0, 0.0, 1.0, 2.0)
KNOTS_Y = (0.20, 0.35, 0.50, 0.68, 0.85)


def spec(key="w_5_15", threshold=0.60, groups=("draft", "form", "dict")):
    return ModelSpec(key=key, title=f"окно {key}", positive="Radiant",
                     negative="Dire", threshold=threshold, groups=groups,
                     knots_x=KNOTS_X, knots_y=KNOTS_Y)


def full(groups):
    return {g: True for g in groups}


class TestCalibration:
    def test_knots_reproduced_exactly(self):
        s = spec()
        for x, y in zip(KNOTS_X, KNOTS_Y):
            assert s.calibrate(x) == pytest.approx(y)

    def test_linear_between_knots(self):
        assert spec().calibrate(0.5) == pytest.approx((0.50 + 0.68) / 2)

    def test_clamped_outside_range(self):
        s = spec()
        assert s.calibrate(-99.0) == pytest.approx(KNOTS_Y[0])
        assert s.calibrate(+99.0) == pytest.approx(KNOTS_Y[-1])

    def test_without_knots_is_identity(self):
        s = ModelSpec("k", "t", "A", "B", 0.6)
        assert s.calibrate(0.42) == pytest.approx(0.42)

    def test_monotone(self):
        s = spec()
        xs = [-3.0, -1.5, -0.2, 0.0, 0.3, 1.4, 3.0]
        vals = [s.calibrate(x) for x in xs]
        assert vals == sorted(vals)


class TestVerdict:
    def test_side_and_confidence_flip_below_half(self):
        v = evaluate(spec(), -1.0, full(("draft", "form", "dict")))
        assert v.side == "Dire"
        assert v.confidence == pytest.approx(1.0 - 0.35)

    def test_missing_group_lowers_fill(self):
        v = evaluate(spec(), 2.0, {"draft": True, "form": True, "dict": False})
        assert v.fill == pytest.approx(2 / 3)
        assert v.missing == ("dict",)

    def test_low_fill_blocks_verdict_even_when_confident(self):
        """Уверенность 0.85 выше порога, но заполненность 2/3 ниже гейта."""
        v = evaluate(spec(threshold=0.60), 2.0,
                     {"draft": True, "form": False, "dict": False})
        assert v.confidence > v.threshold
        assert v.ok is False

    def test_ok_when_confident_and_filled(self):
        v = evaluate(spec(threshold=0.60), 2.0, full(("draft", "form", "dict")))
        assert v.ok is True

    def test_below_threshold_not_ok(self):
        v = evaluate(spec(threshold=0.80), 1.0, full(("draft", "form", "dict")))
        assert v.confidence == pytest.approx(0.68)
        assert v.ok is False

    def test_missing_score_returns_none(self):
        assert evaluate(spec(), None, full(("draft",))) is None


class TestBestOf:
    def test_picks_highest_calibrated_confidence(self):
        g = ("draft", "form", "dict")
        a = evaluate(spec("a", 0.55), 1.0, full(g))       # p 0.68
        b = evaluate(spec("b", 0.55), 2.0, full(g))       # p 0.85
        assert best_of([a, b]).key == "b"

    def test_ignores_models_below_threshold(self):
        g = ("draft", "form", "dict")
        a = evaluate(spec("a", 0.55), 1.0, full(g))       # проходит
        b = evaluate(spec("b", 0.95), 2.0, full(g))       # не проходит
        assert best_of([a, b]).key == "a"

    def test_none_when_nobody_passes(self):
        g = ("draft",)
        a = evaluate(spec("a", 0.99, groups=g), 1.0, full(g))
        assert best_of([a]) is None

    def test_raw_score_does_not_decide(self):
        """Сырой скор выше у 'a', но калибровка ставит 'b' впереди."""
        flat = ModelSpec("a", "a", "R", "D", 0.55, ("draft",),
                         (0.0, 10.0), (0.50, 0.58))
        steep = ModelSpec("b", "b", "R", "D", 0.55, ("draft",),
                          (0.0, 1.0), (0.50, 0.90))
        a = evaluate(flat, 9.0, {"draft": True})
        b = evaluate(steep, 0.9, {"draft": True})
        assert a.raw > b.raw
        assert best_of([a, b]).key == "b"


class TestRender:
    def test_marks_and_percentages(self):
        g = ("draft", "form", "dict")
        ok = evaluate(spec("a", 0.55), 2.0, full(g))
        bad = evaluate(spec("b", 0.95), 0.0, full(g))
        text = render([ok, bad])
        assert text.splitlines()[0] == "🤖 ML:"
        assert "🟢" in text and "🔴" in text
        assert "85%" in text and "зап. 100%" in text

    def test_highlight_adds_check(self):
        g = ("draft",)
        v = evaluate(spec("a", 0.55, groups=g), 2.0, full(g))
        assert render([v], highlight=["a"]).endswith("✅")

    def test_empty_gives_empty_string(self):
        assert render([]) == ""


class TestJournal:
    def test_row_shape_is_frozen(self):
        g = ("draft",)
        v = evaluate(spec("a", 0.55, groups=g), 2.0, full(g))
        row = journal_row(123, [v], extra={"league": "X"})
        assert row["schema"] == JOURNAL_SCHEMA
        assert row["map_id"] == "123"
        assert row["league"] == "X"
        assert set(row["models"][0]) == {"key", "side", "p", "confidence",
                                         "threshold", "fill", "ok", "missing",
                                         "raw", "draft_share", "band_hit",
                                         "band_n", "odds"}

    def test_append_writes_one_line_each(self, tmp_path):
        p = tmp_path / "j.jsonl"
        append_journal({"a": 1}, p)
        append_journal({"a": 2}, p)
        lines = p.read_text(encoding="utf-8").splitlines()
        assert [json.loads(x)["a"] for x in lines] == [1, 2]


BANDS = ({"lo": 0.50, "hit": 0.535, "n": 6415},
         {"lo": 0.70, "hit": 0.718, "n": 1705},
         {"lo": 0.85, "hit": 0.940, "n": 521})


def banded(threshold=0.60):
    return ModelSpec("w", "окно", "Radiant", "Dire", threshold, ("draft",),
                     KNOTS_X, KNOTS_Y, BANDS)


class TestBandsAndOdds:
    def test_band_picked_by_upper_bound(self):
        s = banded()
        assert s.band_hit(0.72) == (0.718, 1705)
        assert s.band_hit(0.99) == (0.940, 521)

    def test_below_first_band_is_none(self):
        assert ModelSpec("w", "t", "A", "B", 0.6, bands=BANDS).band_hit(0.10) is None

    def test_odds_are_break_even_of_observed_hit(self):
        assert banded().fair_odds(0.86) == pytest.approx(1 / 0.940)

    def test_no_bands_no_odds(self):
        assert ModelSpec("w", "t", "A", "B", 0.6).fair_odds(0.9) is None

    def test_verdict_carries_band_and_odds(self):
        v = evaluate(banded(), 2.0, {"draft": True})
        assert v.band_hit == pytest.approx(0.940)
        assert v.band_n == 521
        assert v.odds == pytest.approx(1 / 0.940)

    def test_render_shows_draft_and_odds(self):
        v = evaluate(banded(0.55), 2.0, {"draft": True}, draft_share=0.42)
        text = render([v])
        assert "драфт +42%" in text
        assert "кэф от 1.06" in text

    def test_render_shows_draft_against(self):
        """Отрицательная доля означает, что решение принято ВОПРЕКИ драфту."""
        v = evaluate(banded(0.55), 2.0, {"draft": True}, draft_share=-0.19)
        assert "драфт -19%" in render([v])

    def test_render_omits_absent_extras(self):
        v = evaluate(ModelSpec("w", "окно", "A", "B", 0.55, ("draft",),
                               KNOTS_X, KNOTS_Y), 2.0, {"draft": True})
        text = render([v])
        assert "драфт" not in text and "кэф" not in text


class TestArtifact:
    def test_bands_survive_roundtrip(self, tmp_path):
        atomic_write_specs([banded()], tmp_path)
        back = load_specs(tmp_path)[0]
        assert back.band_hit(0.9) == (0.940, 521)

    def test_roundtrip(self, tmp_path):
        s = spec()
        atomic_write_specs([s], tmp_path)
        back = load_specs(tmp_path)
        assert len(back) == 1
        assert back[0].key == s.key
        assert back[0].knots_y == s.knots_y

    def test_missing_artifact_is_silent(self, tmp_path):
        assert load_specs(tmp_path / "nope") == ()

    def test_broken_entry_skipped_not_fatal(self, tmp_path):
        (tmp_path / "panel.json").write_text(json.dumps(
            {"models": [{"key": "ok", "title": "t", "positive": "A",
                         "negative": "B", "threshold": 0.6},
                        {"key": "broken"}]}), encoding="utf-8")
        got = load_specs(tmp_path)
        assert [s.key for s in got] == ["ok"]
