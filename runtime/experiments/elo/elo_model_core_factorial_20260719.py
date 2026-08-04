#!/usr/bin/env python3
"""Factorial ablation around the 25/75 player-core candidate; research only."""
from __future__ import annotations
import sys as _sys, pathlib as _pathlib
_sys.path.insert(0, str(_pathlib.Path(__file__).resolve().parent))  # соседи по каталогу эксперимента
import argparse, json, sys
from dataclasses import replace
from datetime import datetime, timezone
from itertools import product
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0,str(ROOT))

import ELO.models as elo_models
import elo_model_lab_20260719 as lab
import elo_model_lab_candidate_20260719 as candidate
from ELO.config import HybridEloConfig
from ELO.data_loader import load_matches
from ELO.series_data import build_series_bundles
from ELO.tiering import attach_league_tiers, classify_leagues


def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--data-dir',type=Path,required=True); ap.add_argument('--output',type=Path,required=True); a=ap.parse_args()
    matches,summary=load_matches(a.data_dir); info,_=classify_leagues(matches); attach_league_tiers(matches,info); bundles,_=build_series_bundles(matches)
    cfg=HybridEloConfig(); prod=lab.collect_rows(lab.make_hybrid(cfg,3),bundles,'prod')
    elo_models.resolve_org_key=candidate.safe_resolve_org_key; lab.resolve_org_key=candidate.safe_resolve_org_key
    rows={'prod':prod}; configs={}
    for org_uncert,lineup_uncert,patch_reset,bo3_bonus in product((False,True),repeat=4):
        name=f"g25_o{int(org_uncert)}_l{int(lineup_uncert)}_p{int(patch_reset)}_b{int(bo3_bonus)}"
        c=replace(
            cfg,
            player_global_weight=.25,
            player_tier_weight=.75,
            player_org_uncertainty_boost_max=cfg.player_org_uncertainty_boost_max if org_uncert else 0.0,
            lineup_uncertainty_boost_max=cfg.lineup_uncertainty_boost_max if lineup_uncert else 0.0,
            patch_local_reset_mode=cfg.patch_local_reset_mode if patch_reset else 'none',
            bo3_sweep_bonus_weight=cfg.bo3_sweep_bonus_weight if bo3_bonus else 0.0,
        )
        configs[name]={'org_uncert':org_uncert,'lineup_uncert':lineup_uncert,'patch_reset':patch_reset,'bo3_bonus':bo3_bonus}
        rows[name]=lab.collect_rows(lab.make_hybrid(c,3),bundles,name)
    lab.aligned(*rows.values()); n=len(prod); vs=int(n*.70); te=int(n*.85); reports={}
    for name,r in rows.items():
        reports[name]={
            'validation':lab.metric(r[vs:te]),
            'test':lab.metric(r[te:]),
            'test_by_tier':lab.by_tier(r[te:]),
            'vs_prod':lab.paired_block_ci(prod[te:],r[te:]),
        }
    ranking=sorted(configs,key=lambda n:reports[n]['validation']['log_loss'])
    out={'generated_at':datetime.now(timezone.utc).isoformat(),'dataset':summary,'split':{'validation':[vs,te],'test':[te,n]},'configs':configs,'reports':reports,'validation_ranking':ranking,'selected_on_validation':ranking[0]}
    a.output.parent.mkdir(parents=True,exist_ok=True); a.output.write_text(json.dumps(out,indent=2),encoding='utf-8')
    print('selected',ranking[0]); print('saved',a.output)

if __name__=='__main__': main()
