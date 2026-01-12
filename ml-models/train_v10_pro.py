/Users/alex/Documents/ingame/venv_catboost/bin/python /Users/alex/Documents/ingame/base/cyberscore_try.py 
/Users/alex/Documents/ingame/venv_catboost/lib/python3.9/site-packages/urllib3/__init__.py:35: NotOpenSSLWarning: urllib3 v2 only supports OpenSSL 1.1.1+, currently the 'ssl' module is compiled with 'LibreSSL 2.8.3'. See: https://github.com/urllib3/urllib3/issues/3020
  warnings.warn(
🌐 Используется прокси: http://vNWtJOpPHE:9lQLWIL8Q4@45.132.252.237:44744
INFO:__main__:Инициализация прокси: http://vNWtJOpPHE:9lQLWIL8Q4@45.132.252.237:44744 (индекс 0)
🌐 Прокси отключены, используется прямое подключение
✅ Загружено 14 матчей из map_id_check.txt
🌐 Получение списка активных матчей...
INFO:__main__:
============================================================
🔄 НАЧАЛО ЦИКЛА ПРОВЕРКИ МАТЧЕЙ
============================================================
✅ Найдено активных матчей: 1

🔍 DEBUG: Начало обработки матча #0
   Статус: 16:27
   URL: dltv.org/matches/423763/team-lynx-vs-zero-tenacity-european-pro-league-season-33.0
   Score: 0 : 0
   🌐 Запрос страницы матча...
   ✅ Страница получена
   🌐 Запрос JSON данных...
   ✅ JSON данные получены
   ✅ fast_picks найдены - драфт начался
   📋 live_league_data keys: ['players', 'radiant_team', 'dire_team', 'lobby_id', 'match_id', 'spectators', 'league_id', 'league_node_id', 'stream_delay_s', 'radiant_series_wins', 'dire_series_wins', 'series_type', 'scoreboard']
   🏆 League ID: 18866
   Lead: -4610, Game time: 1047
   🔍 Парсинг драфта и позиций...
      🔍 parse_draft_and_positions(): начало
      Ищем команды: 'zerotenacity' (radiant), 'lynx' (dire)
      Найдено 2 команд в HTML lineups
      Команда в HTML: 'lynx' (5 игроков)
      ✅ Это Dire команда
      Команда в HTML: 'zerotenacity' (5 игроков)
      ✅ Это Radiant команда
      Radiant игроков в HTML: 5
      Dire игроков в HTML: 5
      🔍 Парсим fast_picks из JSON...
      first_team: 5 героев
         - belony: hero_id=101
            ✅ Добавлен в dire pos4
         - 7jesu: hero_id=6
            ✅ Добавлен в dire pos1
         - vladikthehtiviy: hero_id=29
            ✅ Добавлен в dire pos3
         - mellojul: hero_id=13
            ✅ Добавлен в dire pos2
         - drt: hero_id=37
            ✅ Добавлен в dire pos5
      second_team: 5 героев
         - mooz: hero_id=64
            ✅ Добавлен в radiant pos5
         - jeezy: hero_id=45
            ✅ Добавлен в radiant pos4
         - nefrit: hero_id=60
            ✅ Добавлен в radiant pos3
         - kami: hero_id=8
            ✅ Добавлен в radiant pos1
         - worick: hero_id=47
            ✅ Добавлен в radiant pos2
      Итого героев: radiant=5, dire=5, total=10
      📊 ФИНАЛЬНЫЙ СОСТАВ: radiant=5/5, dire=5/5, total=10/10
      ✅ parse_draft_and_positions(): завершено успешно (все 10 героев)
   ✅ Драфт успешно распарсен
Traceback (most recent call last):
  File "/Users/alex/Documents/ingame/base/cyberscore_try.py", line 1341, in <module>
    status = general(use_proxy=False)
  File "/Users/alex/Documents/ingame/base/cyberscore_try.py", line 1281, in general
    answer = check_head(heads, bodies, i, maps_data)
  File "/Users/alex/Documents/ingame/base/cyberscore_try.py", line 1225, in check_head
    early_block = _format_metrics("10-28 Minute:", early_output, early_metric_list)
  File "/Users/alex/Documents/ingame/base/cyberscore_try.py", line 1200, in _format_metrics
    for key, label in metrics:
ValueError: too many values to unpack (expected 2)

Process finished with exit code 1
#!/usr/bin/env python3
"""
Train draft predictor on pro matches.
Since we have only ~7k matches, we'll use simpler statistics.
"""

import json
from collections import defaultdict
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd


def build_pro_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """Build statistics from pro matches."""
    hero_stats: Dict[int, List[int]] = defaultdict(list)
    hero_pos_stats: Dict[Tuple[int, int], List[int]] = defaultdict(list)
    lane_matchup_stats: Dict[Tuple[int, int, int], List[int]] = defaultdict(list)
    
    for _, row in df.iterrows():
        rw = 1 if row["radiant_win"] else 0
        
        for pos in range(1, 6):
            rh = int(row[f"radiant_hero_{pos}"])
            dh = int(row[f"dire_hero_{pos}"])
            
            hero_stats[rh].append(rw)
            hero_stats[dh].append(1 - rw)
            
            hero_pos_stats[(rh, pos)].append(rw)
            hero_pos_stats[(dh, pos)].append(1 - rw)
            
            lane_matchup_stats[(rh, dh, pos)].append(rw)
    
    # Compute final stats with lower thresholds for pro matches
    hero_wr = {}
    for h, wins in hero_stats.items():
        if len(wins) >= 30:  # Lower threshold for pro
            hero_wr[str(h)] = [float(np.mean(wins)), len(wins)]
    
    hero_pos_wr = {}
    for (h, p), wins in hero_pos_stats.items():
        if len(wins) >= 15:  # Lower threshold
            hero_pos_wr[f"{h}_{p}"] = [float(np.mean(wins)), len(wins)]
    
    lane_matchup = {}
    for (h1, h2, p), wins in lane_matchup_stats.items():
        if len(wins) >= 5:  # Very low threshold
            lane_matchup[f"{h1}_{h2}_{p}"] = [float(np.mean(wins)), len(wins)]
    
    return {
        "hero_wr": hero_wr,
        "hero_pos_wr": hero_pos_wr,
        "lane_matchup": lane_matchup,
        "meta": {
            "total_matches": len(df),
            "hero_count": len(hero_wr),
            "pos_wr_count": len(hero_pos_wr),
            "matchup_count": len(lane_matchup),
        }
    }


def evaluate_on_test(
    test_df: pd.DataFrame,
    stats: Dict[str, Any],
) -> None:
    """Evaluate on test set."""
    hero_wr = {int(k): v for k, v in stats["hero_wr"].items()}
    hero_pos_wr = {}
    for k, v in stats["hero_pos_wr"].items():
        h, p = k.rsplit("_", 1)
        hero_pos_wr[(int(h), int(p))] = v
    
    lane_matchup = {}
    for k, v in stats["lane_matchup"].items():
        parts = k.split("_")
        h1, h2, pos = int(parts[0]), int(parts[1]), int(parts[2])
        lane_matchup[(h1, h2, pos)] = v
    
    scores = []
    actuals = []
    
    for _, row in test_df.iterrows():
        r_pos = {pos: int(row[f"radiant_hero_{pos}"]) for pos in range(1, 6)}
        d_pos = {pos: int(row[f"dire_hero_{pos}"]) for pos in range(1, 6)}
        
        # Hero WR
        r_wr = sum(hero_wr.get(r_pos[p], (0.5, 0))[0] for p in range(1, 6))
        d_wr = sum(hero_wr.get(d_pos[p], (0.5, 0))[0] for p in range(1, 6))
        hero_wr_diff = r_wr - d_wr
        
        # Pos WR
        pos_wr_diff = 0.0
        for pos in range(1, 6):
            rh, dh = r_pos[pos], d_pos[pos]
            r_pwr = hero_pos_wr.get((rh, pos), (0.5, 0))[0]
            d_pwr = hero_pos_wr.get((dh, pos), (0.5, 0))[0]
            pos_wr_diff += r_pwr - d_pwr
        
        # Lane matchup
        matchup_score = 0.0
        for pos in range(1, 6):
            rh, dh = r_pos[pos], d_pos[pos]
            if (rh, dh, pos) in lane_matchup:
                wr, cnt = lane_matchup[(rh, dh, pos)]
                weight = min(1.0, cnt / 20)
                matchup_score += (wr - 0.5) * weight
        
        # Combined score
        score = hero_wr_diff * 1.0 + pos_wr_diff * 2.0 + matchup_score * 1.0
        scores.append(score)
        actuals.append(1 if row["radiant_win"] else 0)
    
    scores = np.array(scores)
    actuals = np.array(actuals)
    preds = (scores >= 0).astype(int)
    
    print(f"\nScore distribution:")
    print(f"  Mean: {scores.mean():.4f}")
    print(f"  Std: {scores.std():.4f}")
    
    print(f"\nOverall accuracy: {(preds == actuals).mean():.2%}")
    
    print("\nThreshold analysis:")
    for th in [0.10, 0.20, 0.30, 0.40, 0.50]:
        mask = np.abs(scores) >= th
        if mask.sum() < 20:
            continue
        
        wr = (preds[mask] == actuals[mask]).mean()
        cov = mask.mean()
        print(f"  |score|>={th:.2f}: WR={wr:.2%}, Cov={cov:.2%} ({mask.sum()} matches)")


def main() -> None:
    print("Loading pro matches...")
    df = pd.read_csv("data/pro_matches_enriched.csv")
    df = df.sort_values("start_time").reset_index(drop=True)
    print(f"Total: {len(df)} matches")
    
    # Time-based split
    test_size = int(len(df) * 0.2)
    train_df = df.iloc[:-test_size]
    test_df = df.iloc[-test_size:]
    print(f"Train: {len(train_df)}, Test: {len(test_df)}")
    
    print("\nBuilding statistics from pro matches...")
    stats = build_pro_stats(train_df)
    print(f"Stats: hero_wr={len(stats['hero_wr'])}, pos_wr={len(stats['hero_pos_wr'])}, "
          f"matchup={len(stats['lane_matchup'])}")
    
    print("\n=== EVALUATION ON PRO TEST SET ===")
    evaluate_on_test(test_df, stats)
    
    # Save stats
    output_path = "ml-models/draft_v10_pro_stats.json"
    with open(output_path, "w") as f:
        json.dump(stats, f)
    print(f"\nSaved to {output_path}")
    
    # Also test pub-trained model on same test set for comparison
    print("\n=== COMPARISON: PUB-TRAINED MODEL ON PRO TEST ===")
    with open("ml-models/draft_v10_stats.json") as f:
        pub_stats = json.load(f)
    evaluate_on_test(test_df, pub_stats)


if __name__ == "__main__":
    main()
