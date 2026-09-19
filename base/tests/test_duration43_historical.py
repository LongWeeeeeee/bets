"""Causality and identity regressions for historical duration stacking."""
import numpy as np
from base.tools.duration43_historical import past_split, join_nw, invariant_features, winner_masks, utc


def test_auxiliary_excludes_boundary_and_unfinished_labels():
    cut = utc('2025-01-01')
    ts = np.array([cut-100, cut-60, cut-30, cut, cut+60])
    duration = np.array([60,60,60,60,60])
    train, predict = past_split(ts, duration, 2025)
    assert train.tolist() == [True,False,False,False,False]
    assert predict.tolist() == [False,False,False,True,True]


def test_nw_requires_exact_identity_and_does_not_impute_missing():
    rows = {'mids':np.array([1,2,3]), 'ts':np.array([100,200,300]),
            'durations':np.array([2000,2000,2000]), 'wins':np.array([1,0,1]),
            'heroes':np.tile(np.arange(1,11),(3,1))}
    source = {'mid':np.array([1,2]), 'ts':np.array([100,201]),
              'duration':np.array([2000,2000]), 'wins':np.array([1,0]),
              'heroes':rows['heroes'][:2], 'early_nw':np.array([2,1])}
    labels, report = join_nw(rows,source)
    assert labels.tolist() == [2,-1,-1]
    assert report['conflicting'] == 1 and report['usable_nw'] == 1


def test_duration_features_do_not_depend_on_side_orientation():
    p=np.array([[.2,.8,.6,.3],[.5,.6,.3,.2]])
    np.testing.assert_allclose(invariant_features(p),invariant_features(1-p),atol=1e-7)


def test_conditional_winner_band_boundaries():
    duration=np.array([1199,1200,2040,2041,2579,2580])
    masks=winner_masks(duration,np.array([-1,2,0,1,2,-1]))
    assert masks[0].tolist() == [False,False,True,True,False,False]
    assert masks[1].tolist() == [False,True,True,False,False,False]
    assert masks[4].tolist() == [False,False,False,True,True,False]
    assert masks[5].tolist() == [False,False,False,False,False,True]
