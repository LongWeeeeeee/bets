#!/usr/bin/env python3
"""Narrow candidate sweep after broad ELO lab; no production writes."""
from __future__ import annotations
import argparse, json, math, sys, unicodedata
from collections import defaultdict
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import numpy as np
from scipy.optimize import minimize_scalar

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0,str(ROOT))

import ELO.models as elo_models
import runtime.elo_model_lab_20260719 as lab
from ELO.config import HybridEloConfig
from ELO.data_loader import load_matches
from ELO.domain import LeagueTier
from ELO.roster import RosterLineageTracker
from ELO.series_data import build_series_bundles
from ELO.team_identity import resolve_org_key as original_resolve_org_key
from ELO.tiering import attach_league_tiers, classify_leagues


def safe_resolve_org_key(team_id: int|None, team_name: str) -> str:
    key=original_resolve_org_key(team_id,team_name)
    if key!='name:unknown': return key
    normalized=unicodedata.normalize('NFKC',str(team_name or '')).casefold()
    slug=''.join(ch for ch in normalized if ch.isalnum())
    if slug: return f'name-u:{slug}'
    if isinstance(team_id,int): return f'unknown-id:{team_id}'
    return 'name:unknown'


def fit_temperature(rows:list[dict[str,Any]], start:int, end:int)->float:
    section=rows[start:end]
    def objective(temp:float)->float:
        transformed=lab.transform_temperature(section,float(temp))
        return float(lab.metric(transformed)['log_loss'])
    result=minimize_scalar(objective,bounds=(0.7,3.0),method='bounded',options={'xatol':1e-5})
    return float(result.x)


def residual_adjust(rows, *, form_k=0.0, form_half_life=30.0, pair_k=0.0,
                    pair_prior=8.0, pair_half_life=180.0, pair_min_n=0,
                    pair_cap=40.0, pair_identity='org', pair_tiers=None):
    form={}; pair={}; out=[]; diag=[]
    allowed=set(pair_tiers or [t.value for t in LeagueTier])
    def decay(value,elapsed,half):
        return value*math.pow(.5,elapsed/(half*86400.0)) if value and elapsed>0 and half>0 else value
    def fget(key,ts):
        value,last=form.get(key,(0.0,ts)); return decay(value,ts-last,form_half_life)
    for row in rows:
        ts=int(row['timestamp']); a=str(row['team_a_org']); b=str(row['team_b_org']); tier=row['league_tier']
        fa=fget(a,ts) if form_k else 0.0; fb=fget(b,ts) if form_k else 0.0
        form_adj=fa-fb
        if pair_identity=='roster':
            meta=row.get('metadata') or {}; ka=str(meta.get('radiant_roster_key') or a); kb=str(meta.get('dire_roster_key') or b)
        else: ka,kb=a,b
        if ka<=kb: key=(ka,kb); sign=1.0
        else: key=(kb,ka); sign=-1.0
        rating,n,last=pair.get(key,(0.0,0,ts)); rating=decay(rating,ts-last,pair_half_life)
        shrink=n/(n+pair_prior) if pair_prior>0 else 1.0
        active=(pair_k>0 and n>=pair_min_n and tier in allowed)
        pair_adj=sign*max(-pair_cap,min(pair_cap,rating*shrink)) if active else 0.0
        pmap=lab.logistic(lab.logit(row['p_map'])+lab.elo_to_logit(form_adj+pair_adj))
        p=lab.clip(lab.probability_to_win_series(pmap,row['best_of'])); y=float(row['actual']); err=y-p
        nr=dict(row); nr['p_map']=pmap; nr['p']=p; out.append(nr)
        diag.append({'idx':row['idx'],'pair':key,'pair_n_pre':n,'pair_adjust_elo':pair_adj,'form_adjust_elo':form_adj,'tier':tier})
        if form_k:
            form[a]=(fa+form_k*err,ts); form[b]=(fb-form_k*err,ts)
        if pair_k:
            pair[key]=(rating+sign*pair_k*err,n+1,ts)
    return out,diag


def summarize(rows_by_name):
    lab.aligned(*rows_by_name.values()); base=rows_by_name['current_prod']; n=len(base); val_start=int(n*.70); test_start=int(n*.85)
    out={}
    for name,rows in rows_by_name.items():
        v=rows[val_start:test_start]; t=rows[test_start:]; bt=base[test_start:]
        quarters=[]
        for q in range(4):
            lo=test_start+(n-test_start)*q//4; hi=test_start+(n-test_start)*(q+1)//4
            quarters.append({'start_utc':lab.iso(rows[lo]['timestamp']),'end_utc':lab.iso(rows[hi-1]['timestamp']),**lab.metric(rows[lo:hi])})
        out[name]={'validation15':lab.metric(v),'test15':lab.metric(t),'test15_by_tier':lab.by_tier(t),
                   'test_vs_current':lab.paired_block_ci(bt,t),'test_quarters':quarters}
    return out,val_start,test_start


def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--data-dir',type=Path,required=True); ap.add_argument('--output-dir',type=Path,required=True); a=ap.parse_args(); a.output_dir.mkdir(parents=True,exist_ok=True)
    matches,summary=load_matches(a.data_dir); info,league_summary=classify_leagues(matches); attach_league_tiers(matches,info); bundles,series_summary=build_series_bundles(matches)
    cfg=HybridEloConfig(); rows={}
    rows['current_prod']=lab.collect_rows(lab.make_hybrid(cfg,3),bundles,'current_prod')
    # Switch only the experiment's identity resolver; production source remains untouched.
    elo_models.resolve_org_key=safe_resolve_org_key; lab.resolve_org_key=safe_resolve_org_key
    configs={
      'safe_current':cfg,
      'safe_no_org_uncert':replace(cfg,player_org_uncertainty_boost_max=0.0),
      'safe_no_lineup_uncert':replace(cfg,lineup_uncertainty_boost_max=0.0),
      'safe_no_patch_reset':replace(cfg,patch_local_reset_mode='none'),
      'safe_no_bo3_bonus':replace(cfg,bo3_sweep_bonus_weight=0.0),
      'safe_core_g68_l32':replace(cfg,player_org_uncertainty_boost_max=0.0,lineup_uncertainty_boost_max=0.0,patch_local_reset_mode='none',bo3_sweep_bonus_weight=0.0),
      'safe_core_g50_l50':replace(cfg,player_global_weight=.50,player_tier_weight=.50,player_org_uncertainty_boost_max=0.0,lineup_uncertainty_boost_max=0.0,patch_local_reset_mode='none',bo3_sweep_bonus_weight=0.0),
      'safe_core_g25_l75':replace(cfg,player_global_weight=.25,player_tier_weight=.75,player_org_uncertainty_boost_max=0.0,lineup_uncertainty_boost_max=0.0,patch_local_reset_mode='none',bo3_sweep_bonus_weight=0.0),
      'safe_core_g00_l100':replace(cfg,player_global_weight=0.0,player_tier_weight=1.0,player_role_weight=.12,player_org_uncertainty_boost_max=0.0,lineup_uncertainty_boost_max=0.0,patch_local_reset_mode='none',bo3_sweep_bonus_weight=0.0),
      'safe_core_g25_l75_roster4_w10':replace(cfg,player_global_weight=.25,player_tier_weight=.75,player_org_uncertainty_boost_max=0.0,lineup_uncertainty_boost_max=0.0,patch_local_reset_mode='none',bo3_sweep_bonus_weight=0.0,max_roster_weight=.10,roster_full_weight_matches=8),
      'safe_core_g50_l50_roster4_w10':replace(cfg,player_global_weight=.50,player_tier_weight=.50,player_org_uncertainty_boost_max=0.0,lineup_uncertainty_boost_max=0.0,patch_local_reset_mode='none',bo3_sweep_bonus_weight=0.0,max_roster_weight=.10,roster_full_weight_matches=8),
    }
    for name,c in configs.items():
        m=lab.make_hybrid(c,4 if 'roster4' in name else 3); rows[name]=lab.collect_rows(m,bundles,name)
    n=len(rows['current_prod']); vs=int(n*.70); te=int(n*.85)
    # Tune confidence only on validation; no final-test selection.
    temp_sources=['current_prod','safe_current','safe_no_org_uncert','safe_core_g68_l32','safe_core_g50_l50','safe_core_g25_l75','safe_core_g00_l100','safe_core_g25_l75_roster4_w10','safe_core_g50_l50_roster4_w10']
    fitted={}
    for source in temp_sources:
        temp=fit_temperature(rows[source],vs,te); fitted[source]=temp; rows[source+'_calibrated']=lab.transform_temperature(rows[source],temp)
        print('temperature',source,temp,flush=True)
    base=rows['safe_core_g25_l75']
    specs={
      'safe_form_k8_h14':dict(form_k=8.0,form_half_life=14.0),
      'safe_form_k8_h30':dict(form_k=8.0,form_half_life=30.0),
      'safe_form_k16_h14':dict(form_k=16.0,form_half_life=14.0),
      'safe_h2h_org_k16_n3_cap20':dict(pair_k=16.0,pair_prior=8.0,pair_min_n=3,pair_cap=20.0,pair_identity='org'),
      'safe_h2h_org_k16_n5_cap20':dict(pair_k=16.0,pair_prior=8.0,pair_min_n=5,pair_cap=20.0,pair_identity='org'),
      'safe_h2h_org_k32_n5_cap20':dict(pair_k=32.0,pair_prior=12.0,pair_min_n=5,pair_cap=20.0,pair_identity='org'),
      'safe_h2h_org_k16_n3_cap20_t12':dict(pair_k=16.0,pair_prior=8.0,pair_min_n=3,pair_cap=20.0,pair_identity='org',pair_tiers=['TIER1','TIER2']),
      'safe_h2h_roster_k16_n3_cap20':dict(pair_k=16.0,pair_prior=6.0,pair_min_n=3,pair_cap=20.0,pair_identity='roster'),
      'safe_h2h_roster_k32_n3_cap20':dict(pair_k=32.0,pair_prior=8.0,pair_min_n=3,pair_cap=20.0,pair_identity='roster'),
    }
    dynamic_diag={}
    for name,kw in specs.items(): rows[name],dynamic_diag[name]=residual_adjust(base,**kw)
    reports,vs,te=summarize(rows)
    val_rank=sorted(reports,key=lambda x:reports[x]['validation15']['log_loss']); test_rank=sorted(reports,key=lambda x:reports[x]['test15']['log_loss'])
    report={'generated_at_utc':datetime.now(timezone.utc).isoformat(),'dataset':summary,'league_summary':league_summary,'series_summary':series_summary,
            'split':{'validation':[vs,te],'test':[te,n],'validation_utc':[lab.iso(rows['current_prod'][vs]['timestamp']),lab.iso(rows['current_prod'][te-1]['timestamp'])],'test_utc':[lab.iso(rows['current_prod'][te]['timestamp']),lab.iso(rows['current_prod'][-1]['timestamp'])]},
            'fitted_temperatures':fitted,'trials':reports,'validation_ranking':val_rank,'test_ranking':test_rank}
    (a.output_dir/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    lines=['# Candidate ELO lab','','| # | Variant | Val LL | Test LL | Acc | ΔLL vs prod | CI95 |','|---:|---|---:|---:|---:|---:|---|']
    for i,name in enumerate(val_rank,1):
        x=reports[name]; c=x['test_vs_current']; ci=c['block_bootstrap_ci95']; lines.append(f"| {i} | {name} | {x['validation15']['log_loss']:.6f} | {x['test15']['log_loss']:.6f} | {x['test15']['accuracy']:.4f} | {c['delta_log_loss_trial_minus_baseline']:+.6f} | [{ci[0]:+.6f},{ci[1]:+.6f}] |")
    (a.output_dir/'report.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    print('saved',a.output_dir/'report.json',flush=True)

if __name__=='__main__': main()
