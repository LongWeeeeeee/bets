import ast
import json
from pathlib import Path

from id_to_names import tier_one_teams, tier_two_teams
from maps_research import _load_league_tier_map

base_dir = Path('/Users/alex/Documents/ingame/pro_heroes_data/json_parts_split_from_object')

ids = list(tier_one_teams.values()) + list(tier_two_teams.values())
ids_new = []
for i in ids:
    if isinstance(i, set):
        for foo in i:
            ids_new.append(foo)
    else:
        ids_new.append(i)
ids = ids_new

unknown_teams = {}
league_tier_map = _load_league_tier_map()

data_copy = {}
json_files = sorted(
    p for p in base_dir.glob("*.json")
    if p.name not in {"clean_data.json", "merged.json"}
)

for path in json_files:
    try:
        with path.open('r', encoding='utf-8') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                f.seek(0)
                data = ast.literal_eval(f.read())
    except Exception as e:
        print(f"Ошибка чтения {path}: {e}")
        continue

    if not isinstance(data, dict):
        print(f"Файл {path} пропущен: корень не dict")
        continue

    for match_id, match_data in data.items():
        if match_id in data_copy:
            continue
        try:
            league = match_data.get('league') or {}
            if (not league or not league.get('tier')) and league_tier_map:
                league_id = match_data.get('leagueId') or league.get('id')
                if league_id is not None:
                    league_info = league_tier_map.get(int(league_id))
                    if league_info:
                        match_data['league'] = {
                            'id': league_info.get('id', int(league_id)),
                            'name': league_info.get('name'),
                            'tier': league_info.get('tier'),
                        }
                        league = match_data['league']

            if not league or league.get('id') is None or not league.get('tier'):
                continue
            if league.get('tier') == 'AMATEUR':
                continue

            radiant_id = (match_data.get('radiantTeam') or {}).get('id')
            dire_id = (match_data.get('direTeam') or {}).get('id')
            if any(i not in ids for i in [radiant_id, dire_id]):
                if radiant_id not in ids:
                    unknown_teams[(match_data.get('radiantTeam') or {}).get('name')] = radiant_id
                if dire_id not in ids:
                    unknown_teams[(match_data.get('direTeam') or {}).get('name')] = dire_id
                continue
        except Exception:
            continue

        data_copy[match_id] = match_data

with open(base_dir / 'clean_data.json', 'w', encoding='utf-8') as f:
    json.dump(data_copy, f, ensure_ascii=False)
