import json
from id_to_name import translate
from cyberscore_try import check_bad_map, proceed_map
with open('./1722505765_top600_heroes_data/1722505765_top600_output.txt', 'r') as f:
    maps_data = json.load(f)
    output_data = []
    for counter, match in enumerate(maps_data):
        print(f'{counter} | {len(maps_data)}')
        if not (all(None not in [player['position'], player['hero']['id']] for player in maps_data[match]['players']) \
                and maps_data[match]['startDateTime'] >= 1732147200\
                and (maps_data[match]['durationSeconds'] / 60) >= 26):
            continue
        radiant_heroes_and_pos, dire_heroes_and_pos = {}, {}
        for player in maps_data[match]['players']:
            hero_id = player['hero']['id']
            position = player['position']
            position_key = f'pos{position[-1]}'
            if player.get('isRadiant'):
                radiant_heroes_and_pos.setdefault(position_key, {}).setdefault('hero_id', hero_id)
                radiant_heroes_and_pos.setdefault(position_key, {}).setdefault('hero_name', translate[hero_id])
            else:
                dire_heroes_and_pos.setdefault(position_key, {}).setdefault('hero_id', hero_id)
                dire_heroes_and_pos.setdefault(position_key, {}).setdefault('hero_name', translate[hero_id])
        result = check_bad_map(match=maps_data[match], radiant_heroes_and_pos=radiant_heroes_and_pos, dire_heroes_and_pos=dire_heroes_and_pos)
        if result is None:
            continue
        radiant_heroes_and_pos, dire_heroes_and_pos = result
        output_dict = proceed_map(dire_heroes_and_pos=dire_heroes_and_pos,
                                  radiant_heroes_and_pos=radiant_heroes_and_pos)
        pass
        output_data.append({
            'top_message': output_dict['top_message'], 'bot_message': output_dict['bot_message'],
            'mid_message': output_dict['mid_message'], 'bottomLaneOutcome': maps_data[match]['bottomLaneOutcome'],
            'topLaneOutcome': maps_data[match]['topLaneOutcome'], 'midLaneOutcome': maps_data[match]['midLaneOutcome'], 'didRadiantWin': maps_data[match]['didRadiantWin']})

    with open('analysed_maps_output_lanes.txt', 'w') as f:
        json.dump(output_data, f)