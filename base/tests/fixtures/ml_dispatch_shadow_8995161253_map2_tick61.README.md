# ML Dispatch Shadow Fixture: dltv.org/matches/8995161253.61

**Capture date:** 2026-09-12 (MSK)

**Source:** 
- Host: serv1 (root@23.26.193.167)
- File: /root/main/runtime/ml_dispatch_decisions.jsonl
- Record match_key: `dltv.org/matches/8995161253.61`
- Commit: 7d60666
- Mode: DISPATCH_MODE=shadow

**Capture command:**
```bash
ssh -o ConnectTimeout=15 root@23.26.193.167 'cd /root/main && python3 -' < /private/tmp/claude-501/-Users-alex-Documents-ingame/e5d7376a-5572-42cf-9d23-4c97fdf26548/scratchpad/shadow_permap.py
```

**Record content:**
- Map 2 of match vs Stariy_Bog Club vs Daxak Club
- Game time: 1597.0 seconds
- ELO diff: 17.89 (Radiant 1835.16, Dire 1817.27)
- Timestamp: 1789220824.264

**Verdicts captured:**
- early_nw: Dire 0.7324
- early_win: Dire 0.6086
- late: Dire 0.5838
- all: Dire 0.6033
- lane: Dire 0.6296

**Decisions captured:**
- One decision: win market, Dire side, win_single_model_confirm rule, expected_wr 0.60856, min_odds 1.64
