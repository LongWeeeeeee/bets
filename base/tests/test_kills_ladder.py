"""E-352 kills ladder: math against the REAL ladder.json, recomputed independently."""
import json
import math
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import kills_ladder  # noqa: E402

LADDER = Path(__file__).resolve().parents[2] / "ml-models" / "prematch_panel_kv3" / "ladder.json"


@pytest.fixture(autouse=True)
def _real_dir(monkeypatch):
    monkeypatch.delenv("KV3_PANEL_DIR", raising=False)
    monkeypatch.delenv("ML_PANEL_KILLS_LADDER", raising=False)


def _expected(p_own, p_opp):
    """Independent recomputation straight from the JSON file."""
    raw = json.loads(LADDER.read_text())
    assert raw["schema"] == "kills-ladder-v1"
    clip = raw["clip_logit"]
    shift, gamma = raw["correction"]["shift"], raw["correction"]["gamma_gap"]
    gap_clip = raw["correction"].get("gap_clip")

    def lg(p):
        return max(-clip, min(clip, math.log(p / (1 - p))))

    x, y = lg(p_own), lg(p_opp)
    gap = x - y
    if gap_clip is not None:
        gap = max(-gap_clip, min(gap_clip, gap))
    probs, floor = {}, 1.0
    for line in sorted(int(k) for k in raw["lines"]):
        a, b = raw["lines"][str(line)]
        floor = min(floor, 1 / (1 + math.exp(-(a + b * x + shift + gamma * gap))))
        probs[line] = floor
    hits = [line for line, p in probs.items() if p >= 0.5]
    median = max(hits) if hits else max(min(probs) - 1, 0)
    return probs, median, raw["display_lines"]


@pytest.mark.parametrize("p_own,p_opp", [(0.354, 0.379), (0.379, 0.354), (0.9, 0.05),
                                           (0.05, 0.9), (0.5, 0.5), (0.72, 0.30), (0.30, 0.72)])
def test_probabilities_match_independent_recompute(p_own, p_opp):
    probs, median = kills_ladder.probabilities(p_own, p_opp)
    exp_probs, exp_median, _ = _expected(p_own, p_opp)
    assert median == exp_median
    assert probs.keys() == exp_probs.keys()
    for line in exp_probs:
        assert probs[line] == pytest.approx(exp_probs[line], abs=1e-12)


def test_pinned_example_from_the_owner_card():
    # Radiant 35.4 % / Dire 37.9 % are the owner's numbers of 05.10 (E-352).
    probs, median = kills_ladder.probabilities(0.354, 0.379)
    assert median == 26
    assert [round(probs[line], 4) for line in (16, 21, 26)] == [0.7829, 0.6541, 0.5272]
    assert kills_ladder.render_line("Radiant", 0.354, 0.379) == \
        "Radiant ≈26 килов · ИТБ15,5 78% · ИТБ20,5 65% · ИТБ25,5 53%"
    assert kills_ladder.render_line("Dire", 0.379, 0.354) == \
        "Dire ≈29 килов · ИТБ15,5 83% · ИТБ20,5 71% · ИТБ25,5 60%"


def test_gap_correction_moves_the_underdog_down():
    base = kills_ladder.probabilities(0.354, 0.354)[0]
    weaker = kills_ladder.probabilities(0.354, 0.60)[0]
    assert all(weaker[line] < base[line] for line in base)


def test_monotone_non_increasing_in_line():
    for p_own in (0.01, 0.2, 0.5, 0.8, 0.99):
        probs, _ = kills_ladder.probabilities(p_own, 0.4)
        values = [probs[line] for line in sorted(probs)]
        assert all(a >= b for a, b in zip(values, values[1:]))


def test_running_min_enforced_on_a_non_monotone_table():
    table = {"lines": ((10, (0.0, 1.0)), (11, (3.0, 1.0)), (12, (-1.0, 1.0))),
             "shift": 0.0, "gamma": 0.0, "clip": 6.0, "display": (10,)}
    probs, median = kills_ladder.probabilities(0.5, 0.5, table)
    assert probs[10] == pytest.approx(0.5) and probs[11] == probs[10]
    assert probs[12] == pytest.approx(1 / (1 + math.exp(1.0)))
    assert median == 11


def test_median_when_no_line_reaches_half_is_smallest_line_minus_one():
    probs, median = kills_ladder.probabilities(0.001, 0.99)
    assert max(probs.values()) < 0.5
    assert median == min(probs) - 1 == 5
    table = {"lines": ((1, (-9.0, 0.0)),), "shift": 0.0, "gamma": 0.0, "clip": 6.0,
             "display": (1,)}
    assert kills_ladder.probabilities(0.5, 0.5, table)[1] == 0  # floored at 0


def test_logit_is_clipped():
    clipped = kills_ladder.probabilities(1e-9, 1 - 1e-9)
    edge = kills_ladder.probabilities(1 / (1 + math.exp(6)), 1 - 1 / (1 + math.exp(6)))
    assert clipped[0] == pytest.approx(edge[0], abs=1e-9)
    assert kills_ladder.probabilities(0.0, 1.0) is not None


@pytest.mark.parametrize("n,word", [(1, "кил"), (21, "кил"), (31, "кил"), (101, "кил"),
                                     (2, "кила"), (4, "кила"), (22, "кила"), (24, "кила"),
                                     (0, "килов"), (5, "килов"), (20, "килов"), (26, "килов"),
                                     (11, "килов"), (12, "килов"), (14, "килов"),
                                     (111, "килов"), (112, "килов")])
def test_russian_plural(n, word):
    assert kills_ladder.plural_kills(n) == word


def test_env_zero_disables_and_default_is_on(monkeypatch):
    assert kills_ladder.enabled() is True
    monkeypatch.setenv("ML_PANEL_KILLS_LADDER", "0")
    assert kills_ladder.enabled() is False


def test_missing_or_corrupt_table_is_fail_open(monkeypatch, tmp_path):
    monkeypatch.setenv("KV3_PANEL_DIR", str(tmp_path))
    assert kills_ladder.load() is None
    assert kills_ladder.render_line("Radiant", 0.4, 0.4) is None
    (tmp_path / "ladder.json").write_text("{not json")
    assert kills_ladder.load() is None
    assert kills_ladder.render_line("Radiant", 0.4, 0.4) is None
    bad = json.loads(LADDER.read_text())
    bad["schema"] = "other"
    (tmp_path / "ladder.json").write_text(json.dumps(bad))
    assert kills_ladder.load() is None
    bad["schema"] = "kills-ladder-v1"
    bad["display_lines"] = [16, 99]
    (tmp_path / "ladder.json").write_text(json.dumps(bad))
    assert kills_ladder.load() is None


def test_table_is_parsed_once_per_file_version(monkeypatch):
    first = kills_ladder.load()
    assert first is not None
    assert kills_ladder.load() is first
    assert first["display"] == (16, 21, 26) and first["clip"] == 6.0


def test_gap_is_clipped_to_gap_clip_from_the_real_table():
    raw = json.loads(LADDER.read_text())
    gap_clip = raw["correction"]["gap_clip"]
    assert gap_clip == 1.26
    own, opp = 0.72, 0.30
    assert math.log(own / (1 - own)) - math.log(opp / (1 - opp)) > gap_clip  # 1.79 is clipped
    probs, _ = kills_ladder.probabilities(own, opp)
    # Same own probability with an opponent exactly at the clip edge gives the same ladder.
    edge_opp = 1 / (1 + math.exp(-(math.log(own / (1 - own)) - gap_clip)))
    edge, _ = kills_ladder.probabilities(own, edge_opp)
    for line in probs:
        assert probs[line] == pytest.approx(edge[line], abs=1e-12)
    # Without the clip the same inputs would give a visibly higher P(>=26): proves the clip bites.
    unclipped = dict(kills_ladder.load(), gap_clip=None)
    assert kills_ladder.probabilities(own, opp, unclipped)[0][26] > probs[26] + 0.01


def _write_table(tmp_path, monkeypatch, **correction):
    raw = json.loads(LADDER.read_text())
    raw["correction"].update(correction)
    (tmp_path / "ladder.json").write_text(json.dumps(raw))
    monkeypatch.setenv("KV3_PANEL_DIR", str(tmp_path))


def test_display_at_the_upper_table_edge_has_a_plus(monkeypatch, tmp_path):
    _write_table(tmp_path, monkeypatch, shift=40.0)
    probs, median = kills_ladder.probabilities(0.5, 0.5)
    assert median == max(probs) == 45
    assert kills_ladder.render_line("Radiant", 0.5, 0.5) == \
        "Radiant ≈45+ килов · ИТБ15,5 100% · ИТБ20,5 100% · ИТБ25,5 100%"


def test_display_below_the_lower_table_edge_has_a_leq(monkeypatch, tmp_path):
    _write_table(tmp_path, monkeypatch, shift=-40.0)
    probs, median = kills_ladder.probabilities(0.5, 0.5)
    assert median == min(probs) - 1 == 5
    assert kills_ladder.render_line("Dire", 0.5, 0.5) == \
        "Dire ≤5 килов · ИТБ15,5 0% · ИТБ20,5 0% · ИТБ25,5 0%"


def test_interior_median_keeps_the_plain_approx():
    assert kills_ladder.render_line("Radiant", 0.354, 0.379).startswith("Radiant ≈26 килов ·")
