#!/usr/bin/env python3
"""Follow-up ELO lab for narrow hypotheses found in the first sweep."""
from __future__ import annotations
import sys as _sys, pathlib as _pathlib
_sys.path.insert(0, str(_pathlib.Path(__file__).resolve().parent))  # соседи по каталогу эксперимента
import argparse, copy, json, sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0,str(ROOT))

from ELO.config import HybridEloConfig
from ELO.data_loader import load_matches
from ELO.series_data import build_series_bundles
from ELO.tiering import classify_leagues, attach_league_tiers, attach_league_tiers_time_aware
from elo_model_lab_20260719 import (
    ExactLineupResidualModel, aligned, by_tier, collect_rows, make_hybrid,
    metric, paired_block_ci, slice_rows, transform_temperature,
)


def summarize(rows_by_name):
    aligned(*rows_by_name.values())
    base=rows_by_name['current_prod']
    n=len(base); val=(.70,.85); test=(.85,1.0)
    out={}
    for name,rows in rows_by_name.items():
        v=slice_rows(rows,*val); t=slice_rows(rows,*test); bt=slice_rows(base,*test)
        out[name]={
            'validation15':metric(v), 'test15':metric(t), 'test15_by_tier':by_tier(t),
            'test_vs_current':paired_block_ci(bt,t),
        }
    return out


def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--data-dir',type=Path,required=True); ap.add_argument('--output-dir',type=Path,required=True); a=ap.parse_args(); a.output_dir.mkdir(parents=True,exist_ok=True)
    matches,load_summary=load_matches(a.data_dir)
    static=copy.deepcopy(matches); temporal=copy.deepcopy(matches)
    info,league_summary=classify_leagues(static); attach_league_tiers(static,info); attach_league_tiers_time_aware(temporal)
    static_bundles,series_summary=build_series_bundles(static); temporal_bundles,_=build_series_bundles(temporal)
    cfg=HybridEloConfig()
    trials={}
    trials['current_prod']=collect_rows(make_hybrid(cfg,3),static_bundles,'current_prod')
    trials['time_aware_tiers']=collect_rows(make_hybrid(cfg,3),temporal_bundles,'time_aware_tiers')
    for full in (4,8,16,24):
      for weight in (.05,.10,.20):
        name=f'roster_t4_w{int(weight*100):02d}_full{full}'
        trials[name]=collect_rows(make_hybrid(replace(cfg,max_roster_weight=weight,roster_full_weight_matches=full),4),static_bundles,name)
    for full in (4,8,16):
      for weight in (.05,.10,.20):
        name=f'exact_lineup_w{int(weight*100):02d}_full{full}'
        trials[name]=collect_rows(ExactLineupResidualModel(cfg,max_weight=weight,full_weight_matches=full),static_bundles,name)
    for global_w in (.0,.25,.5,.68,.75,1.0):
      name=f'player_g{int(global_w*100):03d}_l{int((1-global_w)*100):03d}'
      trials[name]=collect_rows(make_hybrid(replace(cfg,player_global_weight=global_w,player_tier_weight=1-global_w,player_role_weight=0.0,max_roster_weight=0.0,player_org_uncertainty_boost_max=0.0,lineup_uncertainty_boost_max=0.0,patch_local_reset_mode='none',bo3_sweep_bonus_weight=0.0),3),static_bundles,name)
    # Calibrate promising structural variants at conservative fixed temperatures.
    for source in ['current_prod','time_aware_tiers','roster_t4_w10_full8','exact_lineup_w10_full8','player_g000_l100','player_g025_l075','player_g050_l050']:
      for temp in (1.1,1.2,1.3,1.4): trials[f'{source}_temp{temp:.1f}']=transform_temperature(trials[source],temp)
    reports=summarize(trials)
    val_rank=sorted(reports,key=lambda n:reports[n]['validation15']['log_loss']); test_rank=sorted(reports,key=lambda n:reports[n]['test15']['log_loss'])
    report={'generated_at_utc':datetime.now(timezone.utc).isoformat(),'dataset':load_summary,'league_summary':league_summary,'series_summary':series_summary,'trials':reports,'validation_ranking':val_rank,'test_ranking':test_rank,'selected_on_validation':val_rank[0]}
    (a.output_dir/'report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    lines=['# Follow-up ELO experiments','','| # | variant | val LL | test LL | test acc | ΔLL | CI95 |','|---:|---|---:|---:|---:|---:|---|']
    for i,n in enumerate(val_rank,1):
      x=reports[n]; c=x['test_vs_current']; ci=c['block_bootstrap_ci95']; lines.append(f"| {i} | {n} | {x['validation15']['log_loss']:.6f} | {x['test15']['log_loss']:.6f} | {x['test15']['accuracy']:.4f} | {c['delta_log_loss_trial_minus_baseline']:+.6f} | [{ci[0]:+.6f},{ci[1]:+.6f}] |")
    (a.output_dir/'report.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    print('selected',val_rank[0]); print('saved',a.output_dir/'report.json')

if __name__=='__main__': main()
