#!/usr/bin/env python3
from __future__ import annotations
# --- bootstrap раскладки: соседние эксперименты живут в runtime/experiments/<тема>/
import sys as _sys, pathlib as _pathlib
_repo_root = next((p for p in _pathlib.Path(__file__).resolve().parents if (p / '.git').exists()), None)
if _repo_root is not None:
    for _exp_dir in sorted((_repo_root / 'runtime' / 'experiments').glob('*')):
        if _exp_dir.is_dir() and str(_exp_dir) not in _sys.path:
            _sys.path.insert(0, str(_exp_dir))
import sys as _sys, pathlib as _pathlib
_sys.path.insert(0, str(_pathlib.Path(__file__).resolve().parent))  # соседи по каталогу эксперимента
import argparse, json, sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0,str(ROOT))

import ELO.models as elo_models
import elo_model_lab_20260719 as lab
import elo_model_lab_candidate_20260719 as candidate
from ELO.config import HybridEloConfig, SimpleTeamEloConfig
from ELO.data_loader import load_matches
from ELO.models import SimpleTeamEloModel
from ELO.series_data import build_series_bundles
from ELO.tiering import classify_leagues, attach_league_tiers


def main():
 ap=argparse.ArgumentParser(); ap.add_argument('--data-dir',type=Path,required=True); ap.add_argument('--output',type=Path,required=True); a=ap.parse_args()
 matches,summary=load_matches(a.data_dir); info,_=classify_leagues(matches); attach_league_tiers(matches,info); bundles,_=build_series_bundles(matches)
 prod=lab.collect_rows(lab.make_hybrid(HybridEloConfig(),3),bundles,'prod')
 elo_models.resolve_org_key=candidate.safe_resolve_org_key; lab.resolve_org_key=candidate.safe_resolve_org_key
 core_cfg=replace(HybridEloConfig(),player_global_weight=.25,player_tier_weight=.75,player_org_uncertainty_boost_max=0.0,lineup_uncertainty_boost_max=0.0,patch_local_reset_mode='none',bo3_sweep_bonus_weight=0.0)
 core=lab.collect_rows(lab.make_hybrid(core_cfg,3),bundles,'core')
 team=lab.collect_rows(SimpleTeamEloModel(SimpleTeamEloConfig(base_k=20.0,team_decay_half_life_days=0.0)),bundles,'team')
 roster=lab.collect_rows(lab.make_hybrid(replace(core_cfg,max_roster_weight=.10,roster_full_weight_matches=8),4),bundles,'roster4')
 form,_=candidate.residual_adjust(core,form_k=8.0,form_half_life=14.0)
 h2h_roster,_=candidate.residual_adjust(core,pair_k=16.0,pair_prior=6.0,pair_min_n=3,pair_cap=20.0,pair_identity='roster')
 h2h_t12,_=candidate.residual_adjust(core,pair_k=16.0,pair_prior=8.0,pair_min_n=3,pair_cap=20.0,pair_identity='org',pair_tiers=['TIER1','TIER2'])
 rows={'prod':prod,'core':core,'team':team,'roster4':roster,'form':form,'h2h_roster':h2h_roster,'h2h_t12':h2h_t12}
 for team_weight in (.05,.10,.15,.20,.30):
  rows[f'core_team{int(team_weight*100):02d}']=lab.blend_map_logits(core,team,team_weight)
 lab.aligned(*rows.values()); n=len(core); test=int(n*.85)
 report={'generated_at':datetime.now(timezone.utc).isoformat(),'dataset':summary,'n':n,'test_start':test,'variants':{}}
 for name,r in rows.items():
  test_rows=r[test:]; x={'test':lab.metric(test_rows),'test_by_tier':lab.by_tier(test_rows),'vs_prod':lab.paired_block_ci(prod[test:],test_rows),'vs_core':lab.paired_block_ci(core[test:],test_rows)}
  blocks=[]
  # Eight contiguous blocks covering the newest 50% (about 625 series each).
  start=n//2
  for i in range(8):
   lo=start+(n-start)*i//8; hi=start+(n-start)*(i+1)//8
   m=lab.metric(r[lo:hi]); base=lab.metric(prod[lo:hi]); core_m=lab.metric(core[lo:hi]); blocks.append({'start_utc':lab.iso(r[lo]['timestamp']),'end_utc':lab.iso(r[hi-1]['timestamp']),'n':hi-lo,'log_loss':m['log_loss'],'delta_vs_prod':m['log_loss']-base['log_loss'],'delta_vs_core':m['log_loss']-core_m['log_loss']})
  x['recent_half_blocks']=blocks; report['variants'][name]=x
 a.output.parent.mkdir(parents=True,exist_ok=True); a.output.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8'); print('saved',a.output)

if __name__=='__main__': main()
