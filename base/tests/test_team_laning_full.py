import numpy as np
import pytest
from types import SimpleNamespace
from scripts.ops import train_team_laning_full as trainer

from scripts.ops.train_team_laning_full import (
    CUTS, PAIR_CTR_MINIMAL, Runner, auc_binary, deterministic_sample,
    exclude_pub_overlap, load_quantized_pool, logloss,
    model_meta, quantized_pool, raw_file_pool, save_model, save_quantized_pool,
    split_rows, staleness_config, write_tsv,
)


def test_split_purges_boundary_crossings():
    c1,c2=CUTS
    ts=np.array([c1-100,c1-100,c1,c2-100,c2-100,c2,c1-100],dtype=np.int64)
    duration=np.array([50,100,50,50,100,50,0])
    target=np.array([1,1,-1,1,1,-1,np.nan],dtype=float)
    (train,val,test),report=split_rows(ts,duration,target)
    np.testing.assert_array_equal(train,[0])
    np.testing.assert_array_equal(val,[2,3])
    np.testing.assert_array_equal(test,[5])
    assert report["purged_cut1"]==1 and report["purged_cut2"]==1
    assert report["nonfinite"]==1


def test_staleness_mapping_and_pub_pro_exclusion():
    assert staleness_config(0)==(3600,30*86400)
    for gap in (7,14,20,30):
        assert staleness_config(gap)==(gap*86400,(30-gap)*86400)
    with pytest.raises(ValueError):
        staleness_config(31)
    np.testing.assert_array_equal(exclude_pub_overlap(np.array([1,2,3,4]),
                                                       np.array([2,4,6])),[True,False,True,False])


def test_direction_auc_uses_mid_ranks_for_tied_scores():
    assert auc_binary(np.array([.1,.2,.2,.3]),np.array([0,1,0,1]))==.875


def test_evaluation_refuses_unsealed_targets(tmp_path):
    corpus=tmp_path/"corpus"
    corpus.mkdir()
    (corpus/"manifest.json").write_text("{}")
    runner=Runner(SimpleNamespace(output_dir=tmp_path,corpus=corpus))
    with pytest.raises(ValueError,match="selection.json"):
        runner.evaluate()


def test_val_sample_is_order_independent_and_pair_ctr_is_pair_only(tmp_path):
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "manifest.json").write_text("{}")
    runner = Runner(SimpleNamespace(output_dir=tmp_path, corpus=corpus,
                                    val_rows=4, pair_ctr="minimal"))
    runner.corpus = {"mid": np.array([19, 17, 23, 5, 13, 11])}
    runner.splits = (np.array([], dtype=np.int64), np.arange(6), np.array([], dtype=np.int64))
    expected = deterministic_sample(np.arange(6), runner.corpus["mid"], 4)
    np.testing.assert_array_equal(runner.fit_val_rows(), expected)
    assert len(expected) == 4
    assert runner.ctr_params(False) == {}
    assert runner.ctr_params(True) == {"per_feature_ctr": PAIR_CTR_MINIMAL}
    assert len(PAIR_CTR_MINIMAL) == 13


@pytest.mark.parametrize("pairs",[False,True])
def test_file_pool_categorical_hashes_replay_serving(tmp_path,pairs):
    from catboost import CatBoostClassifier
    from base.team_laning_model import TeamLaningModel
    heroes=np.vstack([np.roll(np.arange(1,11),i%10) for i in range(90)]).astype(np.int32)
    history=np.broadcast_to((np.arange(90,dtype=np.float32)%7)[:,None,None],
                            (90,10,12)).copy()
    corpus={"heroes":heroes,"team_nw10":np.tile([-1.,0.,1.],30)}
    path=tmp_path/"train.tsv"
    write_tsv(path,corpus,np.arange(90),lambda rows:history[rows],pairs,chunk=30)
    pool=quantized_pool(path,pairs,threads=2)
    borders=tmp_path/"borders.tsv"
    pool.save_quantization_borders(str(borders))
    validation=raw_file_pool(path,2)
    save_quantized_pool(pool,tmp_path/"train.qbin")
    pool=load_quantized_pool(tmp_path/"train.qbin")
    model=CatBoostClassifier(iterations=2,depth=2,verbose=False,
                             allow_writing_files=False,metadata=model_meta(pairs),
                             **({"per_feature_ctr": PAIR_CTR_MINIMAL} if pairs else {}))
    model.fit(pool,eval_set=validation,use_best_model=False)
    save_model(model,tmp_path/"team.cbm")
    expected=model.predict_proba(validation)
    labels=np.asarray(validation.get_label(),dtype=int)
    assert abs(logloss(labels,expected)-model.get_evals_result()["validation"]["MultiClass"][-1])<1e-12
    actual=TeamLaningModel.load(tmp_path).predict_proba(heroes,history)
    assert np.max(np.abs(expected-actual))<1e-9


def _file_case(tmp_path):
    heroes=np.vstack([np.roll(np.arange(1,11),i%10) for i in range(90)]).astype(np.int32)
    history=np.broadcast_to((np.arange(90,dtype=np.float32)%7)[:,None,None],
                            (90,10,12)).copy()
    corpus={"heroes":heroes,"team_nw10":np.tile([-1.,0.,1.],30)}
    return heroes,history,corpus


def test_separate_file_validation_has_column_description(tmp_path):
    from catboost import CatBoostClassifier
    heroes,history,corpus=_file_case(tmp_path)
    train=tmp_path/"train.tsv"
    validation=tmp_path/"validation.tsv"
    write_tsv(train,corpus,np.arange(60),lambda rows:history[rows],True)
    write_tsv(validation,corpus,np.arange(60,90),lambda rows:history[rows],True)
    quantized=quantized_pool(train,True,threads=2)
    raw=raw_file_pool(validation,2)
    model=CatBoostClassifier(iterations=2,depth=2,verbose=False,
        allow_writing_files=False,loss_function="MultiClass",per_feature_ctr=PAIR_CTR_MINIMAL)
    model.fit(quantized,eval_set=raw,use_best_model=False)
    assert model.predict_proba(raw).shape==(30,3)


def test_quantized_pair_refit_replay_uses_raw_reference(tmp_path):
    from catboost import CatBoostClassifier
    from base.team_laning_model import TeamLaningModel
    heroes,history,corpus=_file_case(tmp_path)
    train=tmp_path/"train.tsv"
    write_tsv(train,corpus,np.arange(90),lambda rows:history[rows],True)
    pool=quantized_pool(train,True,threads=2)
    save_quantized_pool(pool,tmp_path/"train.qbin")
    pool=load_quantized_pool(tmp_path/"train.qbin")
    model=CatBoostClassifier(iterations=2,depth=2,verbose=False,
        allow_writing_files=False,loss_function="MultiClass",metadata=model_meta(True),
        per_feature_ctr=PAIR_CTR_MINIMAL)
    model.fit(pool)
    save_model(model,tmp_path/"team.cbm")
    with pytest.raises(Exception,match="IDynamicBlockIteratorPtr"):
        model.predict_proba(pool.slice(np.arange(30)),thread_count=1)
    reference=trainer.refit_replay_reference(model,heroes[:30],history[:30],True)
    actual=TeamLaningModel.load(tmp_path).predict_proba(heroes[:30],history[:30])
    assert np.max(np.abs(reference-actual))<1e-9


@pytest.mark.parametrize("name,cached,field",[
    ("C1",{"learning_rate":.08,"pair_ctr":None,"val_rows":30},"learning_rate"),
    ("C2",{"learning_rate":.15,"pair_ctr":"full","val_rows":30},"pair_ctr"),
    ("C1",{"learning_rate":.15,"pair_ctr":None,"val_rows":20},"val_rows"),
])
def test_cached_fit_rejects_parameter_mismatch(tmp_path,name,cached,field):
    import json
    corpus=tmp_path/"corpus"
    corpus.mkdir()
    (corpus/"manifest.json").write_text("{}")
    runner=Runner(SimpleNamespace(output_dir=tmp_path,corpus=corpus,
        learning_rate=.15,pair_ctr="minimal",val_rows=30))
    runner.splits=(np.arange(60),np.arange(30),np.arange(30))
    directory=tmp_path/name
    directory.mkdir()
    (directory/"team.cbm").touch()
    (directory/"validation.npz").touch()
    (directory/"validation.json").write_text(json.dumps(cached))
    with pytest.raises(ValueError,match=f"cached.*{field}"):
        runner.fit(name)


def test_cached_refit_rejects_learning_rate_mismatch(tmp_path):
    import json
    corpus=tmp_path/"corpus"
    corpus.mkdir()
    (corpus/"manifest.json").write_text("{}")
    runner=Runner(SimpleNamespace(output_dir=tmp_path,corpus=corpus,
        learning_rate=.15))
    (tmp_path/"selection.json").write_text(json.dumps({"winner":"C2",
        "validation":{"C2":{"best_iteration":10}}}))
    directory=tmp_path/"refit_check"
    directory.mkdir()
    (directory/"team.cbm").touch()
    (directory/"report.json").write_text(json.dumps({"learning_rate":.08,
        "iterations":12}))
    with pytest.raises(ValueError,match="cached refit learning_rate"):
        runner.refit()


def test_pro_history_excludes_lookup_failures(monkeypatch):
    class Store:
        def __init__(self, path):
            pass

        def history(self, heroes, accounts, ts, **config):
            if ts==2:
                raise ValueError("missing history")
            return np.full((10,12),ts,dtype=np.float32)

    monkeypatch.setattr(trainer,"LaningHistoryStore",Store)
    pro={"mids":np.array([1,2,3]),"heroes":np.ones((3,10),dtype=int),
         "accounts":np.ones((3,10),dtype=int),"ts":np.array([1,2,3])}
    history,valid=trainer.pro_history(pro,{})
    np.testing.assert_array_equal(valid,[True,False,True])
    np.testing.assert_array_equal(history[:,0,0],[1,3])


def test_known_prefixed_plan_resumes_without_rewriting(tmp_path):
    import json
    corpus=tmp_path/"corpus"
    corpus.mkdir()
    (corpus/"manifest.json").write_text("{}")
    args=SimpleNamespace(output_dir=tmp_path/"out",corpus=corpus,
        threads=2,max_iterations=20,smoke_rows=10,smoke_iterations=2,
        val_rows=5,pair_ctr="minimal",candidates=["C0","C1","C2"])
    runner=Runner(args)
    runner.plan()
    path=args.output_dir/"plan.json"
    saved=json.loads(path.read_text())
    saved["source_sha256"]["trainer"]=trainer.PRE_FIX_TRAINER_SHA
    path.write_text(json.dumps(saved))
    Runner(args).plan()
    assert json.loads(path.read_text())==saved
    args.val_rows=6
    with pytest.raises(ValueError,match="run plan or source changed"):
        Runner(args).plan()
