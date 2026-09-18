import json
from pathlib import Path

import numpy as np
import pytest

from base.tools.compare_prematch_refits import (
    check_alignment, historical_metadata_coverage, metrics, paired_interval,
)
from base.player_metadata import parse_dltv_match


def test_probabilities_and_cluster_pairing():
    y = np.array([0, 1, 1])
    p = np.array([.2, .8, .7])
    result = metrics(y, p)
    assert result['correct'] == 3
    assert result['brier'] == pytest.approx(.0566666667)
    paired = paired_interval(y, p, p, np.array([1, 1, 2]))
    assert paired['candidate_minus_baseline'] == pytest.approx(0, abs=1e-15)
    assert paired['series_bootstrap_95pct'] == pytest.approx([0, 0], abs=1e-15)
    assert paired['series'] == 2


def test_snapshot_cannot_be_backfilled():
    fixture = json.loads((Path(__file__).parent/'fixtures/player_metadata_dltv.json').read_text())
    snapshot = parse_dltv_match('var series_item = ' + json.dumps(fixture['series_item']) + ';',
                               source_url='https://dltv.org/matches/427986/example',
                               observed_at=1789682093.737)
    assert historical_metadata_coverage([snapshot], np.array([1789168030]))['covered_maps'] == 0
    with pytest.raises(ValueError, match='alignment'):
        historical_metadata_coverage([snapshot], np.array([1789682094]))


def test_reference_must_match_map_and_label_order():
    matrix = {k: np.array(v) for k, v in dict(mids=[1, 2], y=[0, 1],
                                              ts=[10, 20], sids=[3, 4]).items()}
    split = dict(mids=matrix['mids'], train=np.array([True, False]), test=np.array([False, True]))
    reference = {k: v[split['test']] for k, v in matrix.items()}
    check_alignment(matrix, split, reference)
    reference['y'] = np.array([0])
    with pytest.raises(ValueError, match='identity'):
        check_alignment(matrix, split, reference)
