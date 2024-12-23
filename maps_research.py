import json
import asyncio
from id_to_name import top5000EU
import json
import shutil
from urllib.parse import quote
from analyze_maps import new_proceed_map
from id_to_name import pro_teams
import requests
import os
from keys import api_token_3, api_token_4, api_token_5, api_token_2, api_token_1, api_token_6, api_token_7, \
    api_token_8, api_token_9, api_token_10, api_token_11, api_token_12, api_token_13, api_token_14, \
    api_token_15, api_token_16, api_token_17, api_token_18

proxies = {
        'https': 'http://90gwi7LEfz:aKI0jgSViq@77.221.150.248:42037',
    }

def load_json_file(filepath, default):
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return default

def load_and_process_json_files(mkdir, **kwargs):
    result = {}
    for key, flag in kwargs.items():
        if flag:
            result[key] = load_json_file(f'./{mkdir}/{key}', {})
        else:
            result[key] = {}
    return result

def get_maps_new(game_mods, maps_to_save, ids,
                 show_prints=None, skip=0, count=0, only_in_ids=False):
    tokens = [api_token_3, api_token_4, api_token_5, api_token_2, api_token_1, api_token_6, api_token_7,
              api_token_8, api_token_9, api_token_10, api_token_11, api_token_12, api_token_13, api_token_14,
              api_token_15, api_token_16, api_token_17, api_token_18]
    api_token = api_token_16
    ids_to_graph, total_map_ids, output_data = [], [], []
    for check_id in set(ids):
        if check_id == 9467224:
            pass
        count += 1
        ids_to_graph.append(check_id)

        if show_prints:
            print(f'{count}/{len(ids)}')

        if len(ids_to_graph) == 5 or count == len(ids):
            api_token, tokens = proceed_get_maps(ids=ids, skip=skip, game_mods=game_mods, only_in_ids=only_in_ids,
                                                 output_data=output_data, ids_to_graph=ids_to_graph, tokens=tokens,
                                                 api_token=api_token)
            ids_to_graph = []  # Очистка после обработки

    if len(output_data) > 0:
        try:
            with open(f'{maps_to_save}.txt', 'r') as f:
                data = json.load(f)
            out = list(set(output_data + data))
            with open(f'{maps_to_save}.txt', 'w') as f:
                json.dump(out, f)
        except FileNotFoundError:
            with open(f'{maps_to_save}.txt', 'w') as f:
                json.dump(list(set(output_data)), f)


def proceed_get_maps(skip, ids, only_in_ids, output_data, tokens, api_token, ids_to_graph=None, game_mods=None,
                     check=True):
    while check:
        if game_mods == [2, 22]:
            query = '''
            {
              players(steamAccountIds: %s) {
                steamAccountId
                matches(request: {startDateTime: 1727827200,
                 take: 100, skip: %s, gameModeIds: %s, isStats:true}) {
                  id
              }}
            }''' % (ids_to_graph, skip, game_mods)
        else:
            query = '''
            {
              teams(teamIds: %s) {
                matches(request: {startDateTime: 1729296000, take: 100, skip: %s, isStats:true}) {
                  id
                  radiantTeam {
                    name
                    id
                  }
                  direTeam {
                    name
                    id
                  }
                }
              }
            }''' % (ids_to_graph, skip)

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Origin": "https://api.stratz.com",
            "Referer": "https://api.stratz.com/graphiql",
            "User-Agent": "STRATZ_API",
            "Authorization": f"Bearer {api_token}"
        }
        try:
            response = requests.post('https://api.stratz.com/graphql', json={"query": query}, headers=headers, proxies=proxies)
            data = json.loads(response.text)
            if game_mods == [2, 22]:
                if any(player['matches'] for player in data['data']['players']):
                    skip += 100
                    for player in data['data']['players']:
                        for match in player['matches']:
                            output_data.append(match['id'])

                else:
                    check = False
            else:
                check = False
                for team in data['data']['teams']:
                    for match in team['matches']:
                        if only_in_ids:
                            if all(team_id in ids for team_id in [match['radiantTeam']['id'], match['direTeam']['id']]):
                                output_data.append(match['id'])
                        else:
                            output_data.append(match['id'])

        except Exception as e:
            print(f"Unexpected error: {e}")
            if tokens:
                api_token = tokens.pop(0)
                print('меняю токен')

            else:
                tokens = [api_token_3, api_token_4, api_token_5, api_token_2, api_token_1, api_token_6, api_token_7,
                          api_token_8, api_token_9, api_token_10, api_token_11, api_token_12, api_token_13,
                          api_token_14,
                          api_token_15, api_token_16, api_token_17]
                api_token = tokens.pop(0)
                print('обновляю токены')
    return api_token, tokens


def eat_temp_files(mkdir, file_data, file_name):
    folder_path = f"./{mkdir}/temp_files"
    if os.path.exists(folder_path):
        for file in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file)
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    for map_id in data:
                        if map_id not in file_data:
                            file_data[map_id] = data[map_id]
            except:
                pass
        with open(f'./{mkdir}/{file_name}_new.txt', 'w') as f:
            json.dump(file_data, f)
        os.remove(f'./{mkdir}/{file_name}.txt')
        os.rename(f'./{mkdir}/{file_name}_new.txt', f'./{mkdir}/{file_name}.txt')
        shutil.rmtree(f'./{mkdir}/temp_files')
        return file_data


def research_map_proceed(maps_to_explore, file_data, file_name, mkdir, counter=0, another_counter=0,
                         show_prints=None):
    tokens = [api_token_3, api_token_4, api_token_5, api_token_2, api_token_1, api_token_6, api_token_7,
              api_token_8, api_token_9, api_token_10, api_token_11, api_token_12, api_token_13, api_token_14,
              api_token_15, api_token_16, api_token_17]
    api_token = tokens.pop()
    new_data, error_maps = {}, set()
    # Попытка загрузить временные данные
    answer = eat_temp_files(mkdir, file_data, file_name)
    if answer is not None:
        file_data = answer

    new_maps = [map_id for map_id in maps_to_explore if str(map_id) not in file_data]
    # Основной цикл по картам
    for map_id in new_maps:
        # Проверка, если данные по карте уже есть
        another_counter += 1
        if show_prints:
            print(f'{another_counter}/{len(new_maps)}')
        # Сохраняем данные каждые 300 итераций
        if another_counter % 300 == 0:
            save_temp_file(new_data, mkdir, another_counter)
            new_data = {}

        query = '''
        {
          match(id:%s){
            startDateTime
            league{
              id
              tier
              region
              basePrizePool
              prizePool
              tournamentUrl
              displayName
            }
            direTeam{
              id
              name
            }
            radiantTeam{
              id
              name
            }
            id
            direKills
            radiantKills
            bottomLaneOutcome
            topLaneOutcome
            midLaneOutcome
            radiantNetworthLeads
            didRadiantWin
            durationSeconds
            players{
              steamAccount{
                id
                isAnonymous
              }
              imp
              position
              isRadiant
              hero{
                id
              }
            }
          }
        }''' % map_id

        encoded_query = quote(query, safe='')
        referer = f"https://api.stratz.com/graphiql?query={encoded_query}"

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Origin": "https://api.stratz.com",
            "Referer": f'{referer}',
            "User-Agent": "STRATZ_API",
            "Authorization": f"Bearer {api_token}"
        }
        try_counter = 0
        check = True
        while check == True:
            if try_counter >= 3:
                break
            try:
                response = requests.post('https://api.stratz.com/graphql', json={"query": query},
                                         headers=headers, timeout=10, proxies=proxies)
                if response.status_code == 200:
                    check = False
                    data = response.json()['data']['match']

                    if data['direKills'] is not None and \
                            all(None not in [player['position'], player['hero']['id'], player['steamAccount']] for
                                player in data['players']):
                        new_data[map_id] = data
                else:
                    try_counter += 1
                    print(response.status_code)
                    if tokens:
                        api_token = tokens.pop(0)
                        print('меняю токен')

                    else:
                        tokens = [api_token_3, api_token_4, api_token_5, api_token_2, api_token_1,
                                  api_token_6, api_token_7, api_token_8, api_token_9, api_token_10,
                                  api_token_11, api_token_12, api_token_13, api_token_14, api_token_15,
                                  api_token_16, api_token_17]
                        api_token = tokens.pop(0)
                        print('обновляю токены')
            except Exception as e:
                try_counter += 1
                pass

    save_temp_file(new_data, mkdir, another_counter)
    # eat_temp_files(mkdir, file_data, file_name)


def save_temp_file(new_data, mkdir, another_counter):
    print('Сохраняю результат')
    # Создание папки для временных файлов
    temp_folder = f"./{mkdir}/temp_files"
    if not os.path.isdir(temp_folder):
        os.makedirs(temp_folder)

    path = f'{temp_folder}/{another_counter}.txt'

    # Генерация уникального имени файла
    while os.path.isfile(path):
        another_counter += 1
        path = f'{temp_folder}/{another_counter}.txt'

    # Сохранение данных во временный файл
    with open(path, 'w') as f:
        json.dump(new_data, f)


def save_json_file(filepath, data):
    with open(filepath, 'w') as f:
        json.dump(data, f)


def research_maps(maps_to_explore, file_name, mkdir, show_prints=None):
    path = f'./{mkdir}/{maps_to_explore}.txt'
    with open(path, 'r+') as f:
        maps_to_explore = json.load(f)
    try:
        with open(f'./{mkdir}/{file_name}.txt', 'r+') as f:
            file_data = json.load(f)
    except FileNotFoundError:
        with open(f'./{mkdir}/{file_name}.txt', 'w') as f:
            pass
        file_data = {}
    # asyncio.run(research_map_proceed_async(maps_to_explore, file_data, file_name, mkdir, show_prints))
    research_map_proceed(
        maps_to_explore=maps_to_explore, file_data=file_data,
        file_name=file_name, mkdir=mkdir, show_prints=True)





def normalize_team_name(team_name):
    translate = {
        'g2 x ig': 'g2.invictus gaming',
        'lava esports ': 'lava uphone',
        'infinity': 'infinity esports',
        'fusion esports': 'fusion',
        'team hryvnia': 'passion.ua',
        'bocajuniors': 'team waska',
        'cuyes e-sports': 'cuyes esports',
        'boom esports': 'team waska',
        'entity': 'cloud9',
        'tea': 'avulus',
        'team tea': 'avulus',
        'wbg.xg': 'xtreme gaming',
        'talon': 'talon esports',
        'invictus gaming': "yakult's brothers",
        'Waska': 'team waska'
    }
    return translate.get(team_name.lower(), team_name.lower())


def explore_database(mkdir, file_name, pro=False, lane=None,
                     over40=None, total_time_kills_teams=None, time_kills=None,
                     counterpick1vs2=None, counterpick1vs3=None, synergy=None, counterpick1vs1=None):
    database = load_json_file(f'./{mkdir}/{file_name}.txt', {})
    answer = eat_temp_files(mkdir, database, file_name)
    if answer is not None:
        database = answer
    players_imp_data = load_json_file(f'./egb/players_imp_data.txt', {'used_maps': []})

    # Загрузка всех необходимых файлов
    data_files = load_and_process_json_files(
        mkdir, total_time_kills_dict=time_kills,
        over40_dict=over40, lane_dict=lane,
        total_time_kills_dict_teams=total_time_kills_teams,
        counterpick1vs3=counterpick1vs3, counterpick1vs2=counterpick1vs2,
        counterpick1vs1=counterpick1vs1, synergy=synergy)

    used_maps = load_json_file(f'./{mkdir}/used_maps', [])

    result = analyze_database(
        database=database, players_imp_data=players_imp_data,
        total_time_kills_dict=data_files['total_time_kills_dict'], over40_dict=data_files['over40_dict'],
        lane_dict=data_files['lane_dict'], pro=pro, used_maps=used_maps,
        total_time_kills_dict_teams=data_files['total_time_kills_dict_teams'],
        counterpick1vs2=data_files['counterpick1vs2'], counterpick1vs3=data_files['counterpick1vs3'],
        synergy=data_files['synergy'], counterpick1vs1=data_files['counterpick1vs1'])

    if result is not None:
        lane_dict, players_imp_data, total_time_kills_dict, synergy, counterpick, \
            over40_dict, total_time_kills_dict_teams, counterpick1vs2, counterpick1vs3, used_maps = result

        print('Сохранение обновленных данных')

        save_json_file(f'./egb/players_imp_data.txt', players_imp_data)
        save_json_file(f'./{mkdir}/used_maps.txt', used_maps)
        if total_time_kills_teams:
            save_json_file(f'./{mkdir}/total_time_kills_dict.txt', total_time_kills_dict)
            save_json_file(f'./{mkdir}/total_time_kills_dict_teams.txt', total_time_kills_dict_teams)
        if counterpick1vs2:
            save_json_file(f'./{mkdir}/synergy.txt', synergy)
            save_json_file(f'./{mkdir}/counterpick1vs1.txt', counterpick)
            save_json_file(f'./{mkdir}/lane_dict.txt', lane_dict)
            save_json_file(f'./{mkdir}/over40_dict.txt', over40_dict)
            save_json_file(f'./{mkdir}/counterpick1vs2.txt', counterpick1vs2)
            save_json_file(f'./{mkdir}/counterpick1vs3.txt', counterpick1vs3)





def analyze_database(database, players_imp_data, over40_dict, used_maps=None,
                     total_time_kills_dict=None, pro=False,
                     synergy_and_counterpick_dict=None, lane_dict=None, check=False,
                     total_time_kills_dict_teams=None, counterpick1vs2=None, counterpick1vs3=None,
                     counterpick1vs1=None, synergy=None):
    counter = []
    new_maps = [str(map_id) for map_id in database if str(map_id) not in used_maps]

    # Инициализируем итоговые словари для накопления данных

    for count, map_id in enumerate(new_maps, start=1):
        check = True
        match = database[map_id]
        print(f'{count}/{len(new_maps)}')
        if pro:
            if all(name in match and match[name] is not None for name in ['direTeam', 'radiantTeam']):
                counter.append(map_id)
                radiant_team_name = normalize_team_name(match['radiantTeam']['name'])
                dire_team_name = normalize_team_name(match['direTeam']['name'])

                result = new_proceed_map(
                    match=match, map_id=map_id, players_imp_data=players_imp_data,
                    total_time_kills_dict=total_time_kills_dict,
                    total_time_kills_dict_teams=total_time_kills_dict_teams,
                    radiant_team_name=radiant_team_name, dire_team_name=dire_team_name,
                )
                lane_dict, players_imp_data, total_time_kills_dict, synergy, counterpick1vs1, \
                    over40_dict, total_time_kills_dict_teams, counterpick1vs2, counterpick1vs3 = result
        else:
            if all(None not in [player['position'], player['hero']['id']] for player in match['players']) \
                    and match['startDateTime'] >= 1727827200 \
                    and (match['durationSeconds'] / 60) >= 26:
                counter.append(map_id)
                result = new_proceed_map(
                    match=match, map_id=map_id, players_imp_data=players_imp_data,
                    lane_dict=lane_dict, synergy=synergy, counterpick1vs1=counterpick1vs1,
                    over40_dict=over40_dict, counterpick1vs2=counterpick1vs2, counterpick1vs3=counterpick1vs3)
                lane_dict, players_imp_data, total_time_kills_dict, synergy, counterpick1vs1, \
                    over40_dict, total_time_kills_dict_teams, counterpick1vs2, counterpick1vs3 = result

    if check:
        used_maps = counter
        return lane_dict, players_imp_data, total_time_kills_dict, synergy, counterpick1vs1, \
            over40_dict, total_time_kills_dict_teams, counterpick1vs2, counterpick1vs3, used_maps


def update_pro(show_prints=None, game_mods=None, only_in_ids=None):
    team_ids = set([pro_teams[team]['id'] for team in pro_teams])
    get_maps_new(maps_to_save='./pro_heroes_data/pro_maps', show_prints=show_prints,
                 ids=team_ids, game_mods=game_mods, only_in_ids=True)
    research_maps(maps_to_explore='pro_maps', file_name='pro_output', mkdir='pro_heroes_data', show_prints=show_prints)
    explore_database(mkdir='pro_heroes_data', file_name='pro_output', pro=True,
                     time_kills=True, total_time_kills_teams=True)


# def update_all_teams(show_prints=None, only_in_ids=None):
#     team_ids = set()
#     if only_in_ids is not None:
#         for name in only_in_ids:
#             team_ids.add(all_teams[name.lower()]['id'])
#     else:
#         for team in all_teams:
#             team_ids.add(all_teams[team]['id'])
#     get_maps_new(maps_to_save='./all_teams/maps', game_mods=[2],
#                  show_prints=show_prints, ids=team_ids, only_in_ids=only_in_ids)
#     research_maps(maps_to_explore='maps', file_name='output', mkdir='all_teams', show_prints=show_prints)
#     explore_database(mkdir='all_teams', file_name='output', pro=True,
#                      time_kills=True, total_time_kills_teams=True)


def update_my_protracker(show_prints=None):
    get_maps_new(maps_to_save='./1722505765_top600_heroes_data/1722505765_top600_maps', game_mods=[2, 22],
                 show_prints=show_prints, ids=top5000EU.keys())
    research_maps(mkdir='1722505765_top600_heroes_data', maps_to_explore='1722505765_top600_maps',
                  file_name='1722505765_top600_output', show_prints=show_prints)
    explore_database(mkdir='1722505765_top600_heroes_data', file_name='1722505765_top600_output',
                     lane=True, over40=True, counterpick1vs2=True,
                     counterpick1vs3=True, counterpick1vs1=True, synergy=True)
    # explore_database(mkdir='1722505765_top600_heroes_data', file_name='1722505765_top600_output',
    #                  lane=True)


# def update_heroes_data(database_list=None, mkdir=None):
#     get_maps_new(maps_to_save='./heroes_data/heroes_data_maps', game_mods=[2, 22], start_date_time=1716508800,
#     players_dict=top_600_asia_europe_nonanon)
#     research_maps(maps_to_explore='heroes_data_maps', output='heroes_data_output', mkdir='heroes_data')
#     explore_database(mkdir='heroes_data', output='heroes_data_output', start_date_time=1716508800)

if __name__ == "__main__":
    # with open('teams_stat_dict.txt', 'r+') as f:
    #     data = json.load(f)
    # teams_ids = set()
    # for team in data:
    #     id = data[team]['id']
    #     if id > 0:
    #         teams_ids.add(id)
    # set(teams_ids)
    # pass
    with open('./all_teams/1722505765_top600_output.txt', 'r+') as f:
        data = json.load(f)
    with open('./pro_heroes_data/pro_output.txt', 'r') as f:
        to_be_merged = json.load(f)
    for map_id in to_be_merged:
        if map_id not in data:
            data[map_id] = to_be_merged[map_id]
    with open('./all_teams/1722505765_top600_output.txt', 'w') as f:
        json.dump(data, f)
