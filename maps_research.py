import json
import asyncio
import aiohttp
from id_to_name import top5000EU, all_teams
import json
import shutil
from urllib.parse import quote
from analyze_maps import new_proceed_map
import ijson
from id_to_name import pro_teams
import requests
from aiohttp_socks import ProxyConnector
import os
from keys import orig_tokens, start_date_time

CONCURRENCY_LIMIT = 20 # EXPERIMENT with this value (e.g., 10, 25, 50)
semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
tasks = []

def load_json_file(filepath, default):
    return default

def load_and_process_json_files(mkdir, **kwargs):
    result = {}
    for key, flag in kwargs.items():
        if flag:
            result[key] = load_json_file(f'./{mkdir}/{key}', {})
        else:
            result[key] = None
    return result

async def get_maps_new(game_mods, maps_to_save, ids,
                 show_prints=None, skip=0, count=0, only_in_ids=False):
    tokens = orig_tokens
    api_token = tokens.pop(0)
    ids_to_graph, total_map_ids, output_data, all_teams, player_ids = [], [], [], {}, set()
    # connector = ProxyConnector.from_url(socks5_proxy)
    async with aiohttp.ClientSession() as session:
        for check_id in set(ids):
            count += 1
            ids_to_graph.append(check_id)

            if show_prints:
                print(f'{count}/{len(ids)}')

            if len(ids_to_graph) == 5 or count == len(ids):
                api_token, tokens, player_ids = await proceed_get_maps(skip=skip, game_mods=game_mods, only_in_ids=only_in_ids,
                                                     output_data=output_data, ids_to_graph=ids_to_graph, tokens=tokens,
                                                     api_token=api_token, all_teams=all_teams, session=session, player_ids=player_ids,
                                                     player_ids_check=True)
                ids_to_graph = []  # Очистка после обработки
    with open('count_synergy_16th_5000/player_ids.txt', 'w') as f:
        json.dump(list(player_ids), f)
    count = 0
    async with aiohttp.ClientSession() as session:
        for check_id in set(player_ids):
            count += 1
            ids_to_graph.append(check_id)

            if show_prints:
                print(f'{count}/{len(player_ids)}')

            if len(ids_to_graph) == 5 or count == len(player_ids):
                api_token, tokens = await proceed_get_maps(skip=skip, game_mods=game_mods, only_in_ids=only_in_ids,
                                                     output_data=output_data, ids_to_graph=ids_to_graph, tokens=tokens,
                                                     api_token=api_token, all_teams=all_teams, session=session)
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
        with open('all_teams.txt', 'w') as f:
            json.dump(all_teams, f)


def collect_all_maps(folder_path, maps=None, output=None):
    if maps: ids_to_exclude_from_json = set()
    if output: ids_to_exclude_from_json = {}
    if os.path.exists(folder_path):
        json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
        print(f"Найдено {len(json_files)} JSON файлов для обработки.")

        if not json_files:
            print("В указанной папке нет JSON файлов.")

        for i, file_name in enumerate(json_files):
            file_path = os.path.join(folder_path, file_name)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:  # Добавлено encoding='utf-8'
                    # data теперь называется loaded_json_content для ясности
                    loaded_json_content = json.load(f)

                    if isinstance(loaded_json_content, list):
                        for item_in_list in loaded_json_content:
                            if isinstance(item_in_list, dict) and 'id' in item_in_list:
                                # ID из JSON (например, 8216280864) преобразуем в строку
                                if maps:
                                    ids_to_exclude_from_json.add(str(item_in_list['id']))
                                if output:
                                    ids_to_exclude_from_json[str(item_in_list['id'])] = item_in_list
                            else:
                                # Можно добавить логирование, если структура элемента списка неожиданная
                                # print(f"Предупреждение: в файле {file_name} элемент списка не словарь или не имеет 'id': {item_in_list_id}")
                                pass  # Пропускаем некорректные элементы молча или логируем
                    # Если вдруг какой-то файл содержит один объект матча, а не список
                    elif isinstance(loaded_json_content, dict):
                        for map_id in loaded_json_content:
                            if maps:
                                raise EnvironmentError
                                # ids_to_exclude_from_json.add(str(loaded_json_content['id']))
                            if output:
                                ids_to_exclude_from_json[str(map_id)] = loaded_json_content[map_id]
                    else:
                        print(
                            f"Предупреждение: файл {file_name} не содержит ожидаемый формат (список словарей или словарь с 'id'). Тип: {type(loaded_json_content)}. Файл пропущен.")

                if (i + 1) % 100 == 0 or (i + 1) == len(json_files):  # Печатаем прогресс
                    print(
                        f"Обработано {i + 1}/{len(json_files)} файлов. Собрано {len(ids_to_exclude_from_json)} уникальных ID для исключения из JSON.")

            except json.JSONDecodeError:
                print(f"Ошибка декодирования JSON в файле: {file_path}. Файл пропущен.")
            except Exception as e:
                print(f"Непредвиденная ошибка при обработке файла {file_path}: {e}. Файл пропущен.")
    else:
        print(f"Папка {folder_path} не найдена.")

    print(f"Всего собрано {len(ids_to_exclude_from_json)} уникальных ID для исключения из JSON файлов.")
    return ids_to_exclude_from_json



async def proceed_get_maps(skip, session, only_in_ids, output_data, tokens, api_token,
                           player_ids=None, all_teams=None, ids_to_graph=None, game_mods=None,
                     check=True, player_ids_check=False):
    while check:
        if game_mods == [2, 22]:
            query = f'''
            {{
              players(steamAccountIds: {ids_to_graph}) {{
                steamAccount{{
                    smurfFlag
                    isAnonymous
                    id
                    name 
                }}
                
                matches(request: {{startDateTime: {start_date_time} ,
                 take: 100, skip: {skip}, gameModeIds: {game_mods}, isStats:true, rankIds: [72,73,74,75,76,77,78,79,80]}}) {{
                  id
                  players{{
                  intentionalFeeding
                  steamAccount{{
                    smurfFlag
                    id
                    isAnonymous
                  }}
                  }}
              }}
            }}}}'''
        else:
            query = f'''
            {{
              teams(teamIds: {ids_to_graph}) {{
                matches(request: {{startDateTime: {start_date_time}, take: 100, skip: {skip}, isStats:true}}) {{
                  id
                  radiantTeam {{
                    name
                    id
                  }}
                  direTeam {{
                    name
                    id
                  }}
                }}
              }}
            }}'''

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
            async with (session.post('https://api.stratz.com/graphql',
                                   json={"query": query},
                                   headers=headers,
                                   ssl=False) as response):
                data = await response.json()


                #                        if data['direKills'] is not None:
                #            if all(None not in [player['position'], player['hero']['id'], player['steamAccount']] for
                 #                   player in data['players']):
                  #              if all(player['intentionalFeeding'] is False for player in data['players']):
                 #                   if all(player['steamAccount']['smurfFlag'] in [0, 2] for player in data['players']):
                if game_mods == [2, 22]:

                    if any(player['matches'] for player in data['data']['players']):
                        skip += 100
                        for player in data['data']['players']:
                            if player['steamAccount']['smurfFlag'] not in [0, 2] and player['steamAccount']['isAnonymous']:
                                continue
                            for match in player['matches']:
                                output_data.append(match['id'])
                                if not player_ids_check: continue
                                for extra_player in match['players']:
                                    if extra_player['intentionalFeeding'] or extra_player['steamAccount']['smurfFlag'] not in [0, 2] or extra_player['steamAccount']['isAnonymous']:
                                        continue
                                    player_ids.add(extra_player['steamAccount']['id'])




                    else:
                        check = False
                else:
                    check = False
                    for team in data['data']['teams']:
                        for match in team['matches']:
                            if match['radiantTeam']['name'] not in all_teams:
                                all_teams[match['radiantTeam']['name']] = match['radiantTeam']['id']
                            elif match['direTeam']['name'] not in all_teams:
                                all_teams[match['direTeam']['name']] = match['direTeam']['id']
                            if only_in_ids:
                                output_data.append(match['id'])
                            else:
                                output_data.append(match['id'])

        except Exception as e:
            print(f"Unexpected error: {e}")
            if tokens:
                api_token = tokens.pop(0)
                print('меняю токен')

            else:
                tokens = orig_tokens
                api_token = tokens.pop(0)
                print('обновляю токены')
    if player_ids_check:
        return api_token, tokens, player_ids
    else:
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


async def research_map_proceed(maps_to_explore, mkdir, another_counter=0,
                         show_prints=True, pro=False):
    tokens = orig_tokens
    api_token = tokens.pop()
    new_data, error_maps = {}, set()
    # Попытка загрузить временные данные
    try:
        with open(f'./trash_maps.txt', 'r') as f1:
            trash_maps = set(json.load(f1))
    except:
        trash_maps = set()
    if pro: final_new_maps = maps_to_explore
    else:
        current_maps_to_filter = {map_id for map_id in maps_to_explore if map_id not in trash_maps}
        folder_path = "count_synergy_16th_5000/json_parts_split_from_object"

        ids_to_exclude_from_json = collect_all_maps(folder_path=folder_path, output=True)

        # --- Шаг 3: Финальная фильтрация ---
        print("\nШаг 3: Финальная фильтрация...")
        # Основной цикл по картам
        final_new_maps = {map_id for map_id in current_maps_to_filter if str(map_id) not in ids_to_exclude_from_json}
    async with aiohttp.ClientSession() as session:
        for map_id in final_new_maps:
            data_len = len(new_data)
            # Проверка, если данные по карте уже есть
            another_counter += 1
            if show_prints:
                print(f'{another_counter}/{len(final_new_maps)}')
            # Сохраняем данные каждые 300 итераций
            if len(new_data) == 500:
                save_temp_file(new_data, mkdir, another_counter)
                new_data = {}
            if len(trash_maps) % 499 == 0:
                with open(f'./trash_maps.txt', 'w') as f1:
                    json.dump(list(trash_maps), f1)


            query = f'''
            {{
              match(id:{map_id}){{
                startDateTime
                radiantNetworthLeads
                league{{
                  id
                  tier
                  region
                  basePrizePool
                  prizePool
                  tournamentUrl
                  displayName
                }}
                direTeam{{
                  id
                  name
                }}
                radiantTeam{{
                  id
                  name
                }}
                id
                direKills
                radiantKills
                bottomLaneOutcome
                topLaneOutcome
                midLaneOutcome
                radiantNetworthLeads
                didRadiantWin
                durationSeconds
                players{{
                  intentionalFeeding
                  steamAccount{{
                    smurfFlag
                    id
                    isAnonymous
                  }}
                  imp
                  position
                  isRadiant
                  hero{{
                    id
                  }}
                }}
              }}
            }}'''

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
                    async with session.post('https://api.stratz.com/graphql',
                                            json={"query": query},
                                            headers=headers,
                                            ssl=False, timeout=15) as response:

                        data = await response.json()
                        data = data['data']['match']
                        check = False
                        if data['direKills'] is not None:
                            if all(None not in [player['position'], player['hero']['id'], player['steamAccount']] for
                                    player in data['players']):
                                if all(player['intentionalFeeding'] is False for player in data['players']):
                                    if all(player['steamAccount']['smurfFlag'] in [0, 2] for player in data['players']):
                                        new_data[map_id] = data
                                    else:
                                        print('smurf')
                                        trash_maps.add(map_id)
                                else:
                                    print('feed')
                                    trash_maps.add(map_id)

                            else:
                                print('None')
                                trash_maps.add(map_id)
                        else:
                            print('kills None')
                            trash_maps.add(map_id)

                except Exception as e:
                    print(f'error, error code: {response.status}, error: {e}')
                    try_counter += 1
                    if tokens:
                        api_token = tokens.pop(0)
                        print(f'меняю токен {try_counter}/{len(tokens)}')

                    else:
                        tokens = orig_tokens
                        api_token = tokens.pop(0)
                        print('обновляю токены')
            if len(new_data) == data_len:
                print(map_id)


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


def research_maps(maps_to_explore, file_name, mkdir, show_prints=None, pro=False):

    if pro: pass
    else:
        path = f'./{mkdir}/{maps_to_explore}.txt'
        with open(path, 'r+') as f:
            maps_to_explore = json.load(f)
    asyncio.run(research_map_proceed(
        maps_to_explore=maps_to_explore,
        mkdir=mkdir, show_prints=True, pro=pro))







def explore_database(mkdir, file_name, pro=False, lane=None,
                     over35=None, total_time_kills_teams=None, time_kills=None,
                     counterpick1vs2=None, synergy=None, counterpick1vs1=None, counterpick1vs3=None,
                     synergy4=None):
    # database = open(f'./{mkdir}/{file_name}.txt')
    # database = json.load(database)
    # answer = eat_temp_files(mkdir, database, file_name)
    folder_path = "count_synergy_10th_2000/json_parts_split_from_object"

    database = collect_all_maps(folder_path=folder_path, output=True)
    # with open('./dltv/dltv_output.txt', 'r') as f:
    #     dltv_maps = json.load(f)
    #     c = 0
    #     for match_id in dltv_maps:
    #         try:
    #             del database[match_id]
    #             c+=1
    #         except: pass
    #     print(f'удалено {c} maps')
    # Загрузка всех необходимых файлов
    data_files = load_and_process_json_files(
        mkdir, total_time_kills_dict=time_kills,
        over35_dict=over35, lane_dict=lane,
        total_time_kills_dict_teams=total_time_kills_teams,
        counterpick1vs2=counterpick1vs2,
        counterpick1vs1=counterpick1vs1, synergy=synergy,

        counterpick1vs3=counterpick1vs3,
        synergy4=synergy4)

    used_maps = load_json_file(f'./{mkdir}/used_maps.txt', [])

    result = analyze_database(
        database=database,
        total_time_kills_dict=data_files['total_time_kills_dict'], over35_dict=data_files['over35_dict'],
        lane_dict=data_files['lane_dict'], pro=pro, used_maps=used_maps,
        total_time_kills_dict_teams=data_files['total_time_kills_dict_teams'],
        counterpick1vs2=data_files['counterpick1vs2'],
        synergy=data_files['synergy'], counterpick1vs1=data_files['counterpick1vs1'],
        counterpick1vs3=data_files['counterpick1vs3'],
        synergy4=data_files['synergy4'])

    if result is not None:
        lane_dict, total_time_kills_dict, synergy, counterpick1vs1, \
            over35_dict, total_time_kills_dict_teams, counterpick1vs2, \
            counterpick1vs3, synergy4, used_maps = result

        print('Сохранение обновленных данных')

        save_json_file(f'./{mkdir}/used_maps.txt', used_maps)
        if total_time_kills_teams:
            save_json_file(f'./{mkdir}/total_time_kills_dict.txt', total_time_kills_dict)
            save_json_file(f'./{mkdir}/total_time_kills_dict_teams.txt', total_time_kills_dict_teams)
        if counterpick1vs2:
            save_json_file(f'./{mkdir}/synergy.txt', synergy)
            save_json_file(f'./{mkdir}/counterpick1vs1.txt', counterpick1vs1)
            save_json_file(f'./{mkdir}/over35_dict.txt', over35_dict)
            save_json_file(f'./{mkdir}/counterpick1vs2.txt', counterpick1vs2)
            save_json_file(f'./{mkdir}/counterpick1vs3.txt', counterpick1vs3)
            save_json_file(f'./{mkdir}/synergy4.txt', synergy4)
        if lane_dict:
            save_json_file(f'./{mkdir}/lane_dict.txt', lane_dict)


def check_match(match):
    if match['startDateTime'] >= int(start_date_time) and match['direKills'] is not None and all(player['intentionalFeeding'] is False and player['steamAccount']['smurfFlag'] in [0, 2]
                and None not in [player['position'], player['hero']['id'], player['steamAccount']]
                for player in match['players']) and len(match['radiantNetworthLeads']) >= 20:
        return True



def analyze_database(database, over35_dict, used_maps=None,
                     total_time_kills_dict=None, pro=False,
                     lane_dict=None, check=False,
                     total_time_kills_dict_teams=None, counterpick1vs2=None,
                     counterpick1vs1=None, synergy=None,
                     counterpick1vs3=None,
                     synergy4=None
                     ):
    counter = []
    # new_maps = [str(map_id) for map_id in database if str(map_id) not in used_maps]

    # Инициализируем итоговые словари для накопления данных
    for count, map_id in enumerate(database):
        check = True
        match = database[map_id]
        count+=1
        print(f'{count}/{len(database)}')
        if pro:
            if all(name in match and match[name] is not None for name in ['direTeam', 'radiantTeam']):
                counter.append(map_id)
                radiant_team_name = match['radiantTeam']['name'].lower()
                dire_team_name = match['direTeam']['name'].lower()

                result = new_proceed_map(
                    match=match,
                    total_time_kills_dict=total_time_kills_dict,
                    total_time_kills_dict_teams=total_time_kills_dict_teams,
                    radiant_team_name=radiant_team_name, dire_team_name=dire_team_name,
                    counterpick1vs3=counterpick1vs3,
                    synergy4=synergy4
                )
                lane_dict, total_time_kills_dict, synergy, counterpick1vs1, \
                    over35_dict, total_time_kills_dict_teams, counterpick1vs2 = result
        else:
            if check_match(match=match):
                counter.append(map_id)
                result = new_proceed_map(
                    match=match,
                    lane_dict=lane_dict, synergy=synergy, counterpick1vs1=counterpick1vs1,
                    over35_dict=over35_dict, counterpick1vs2=counterpick1vs2, counterpick1vs3=counterpick1vs3,
                    synergy4=synergy4
                )
                lane_dict, total_time_kills_dict, synergy, counterpick1vs1, \
                    over35_dict, total_time_kills_dict_teams, counterpick1vs2, \
                    counterpick1vs3, synergy4 = result

    if check:
        used_maps = counter
        return lane_dict, total_time_kills_dict, synergy, counterpick1vs1, \
                    over35_dict, total_time_kills_dict_teams, counterpick1vs2, \
                    counterpick1vs3, synergy4, used_maps


# def update_pro(show_prints=None, game_mods=None, only_in_ids=None):
#     team_ids = set([all_teams[team] for team in all_teams])
#     asyncio.run(get_maps_new(maps_to_save='./pro_heroes_data/pro_maps', show_prints=show_prints,
#                  ids=team_ids, game_mods=game_mods, only_in_ids=True))
#     research_maps(maps_to_explore='pro_maps', file_name='pro_output', mkdir='pro_heroes_data', show_prints=show_prints)
#     explore_database(mkdir='pro_heroes_data', file_name='pro_output', pro=True,
#                      time_kills=True, total_time_kills_teams=True)


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


def update_my_protracker(show_prints=True):
    # from id_to_name import get_players
    # players_dict = set()
    # regions = [[5000, 'EUROPE']]
    # for top, region in regions:
    #     players_dict = get_players(top=top, region=region, players_dict=players_dict, skip=0)
    # with open('./count_synergy_16th_5000/player_ids.txt', 'r') as f:
    #     players_dict = json.load(f)
    # players_dict = {25907144, 34505203, 72393079, 73401082, 74371974, 76904792, 81475303, 84385735, 86698277, 86738694, 87231335, 89654154, 90137663, 91369376, 91663166, 92487440, 92601861, 92949094, 93526520, 93618577, 93817671, 93845249, 94054712, 94281932, 94738847, 97658618, 99611577, 99983413, 100594231, 100598959, 100616105, 101325470, 101356886, 101446572, 101695162, 103126502, 103802963, 104185879, 104334048, 104758571, 105920571, 106305042, 106863163, 107023378, 108928414, 109757023, 111114687, 112426916, 112531417, 112696656, 113112046, 113152484, 113435203, 113826617, 113926403, 114047775, 114619230, 116837520, 117005046, 117514269, 118078153, 118305301, 118559150, 120613892, 120840516, 121870008, 121885658, 122817493, 123213918, 123717676, 123787715, 124286163, 124936122, 125189164, 126842529, 127565532, 127961011, 128398556, 128958133, 129958758, 130416036, 131303632, 133558180, 134357466, 134659079, 135607261, 135673679, 136003052, 136342177, 136353896, 136535226, 136552946, 136829091, 137092505, 137129583, 137645683, 138237476, 138543123, 139031324, 139301882, 139937922, 140141752, 140178149, 140251702, 140411011, 141153841, 142139318, 142991995, 143278997, 143693439, 144048558, 144174966, 145065875, 145550466, 146711951, 147767183, 148180390, 148520614, 148612029, 149486894, 150960169, 150961567, 150982730, 151669649, 152168157, 152285172, 152455523, 152545459, 152859296, 153836240, 154259887, 154715080, 155447692, 155564702, 156029808, 158072796, 158425391, 158847773, 162126721, 162274034, 162421742, 162641196, 163458082, 164506534, 164749789, 165110440, 165110447, 165390194, 167488455, 167976729, 168028715, 169025618, 169416589, 169516760, 170809075, 170834508, 170896543, 171097887, 171262902, 172099728, 173118561, 173378701, 173476224, 173978074, 175061340, 175350492, 177203952, 177953305, 178692606, 179266369, 179313961, 179684293, 181145703, 181987328, 183378746, 183719386, 184298001, 184620877, 185059559, 185202677, 185590374, 186837494, 187123736, 187949640, 188317959, 188551857, 188711567, 188962397, 190100004, 190789329, 190826739, 191066225, 191116892, 191362875, 191597529, 192981126, 193418507, 193815691, 193884241, 194353328, 194555122, 194720582, 194979527, 195415954, 195522403, 196481318, 196490133, 196493853, 196867605, 196931374, 197737818, 197849259, 197913112, 199162825, 199222238, 199666759, 199833749, 199953095, 201826550, 202217968, 202691456, 206379758, 206642367, 207945924, 209103998, 211287099, 216763662, 217472313, 218231587, 218675899, 219755398, 219950601, 221532774, 223342537, 223382342, 225163338, 228517469, 229311481, 230487729, 235505993, 235549078, 238509899, 241107899, 245373129, 249043309, 249101305, 250353613, 252551173, 252716815, 252737052, 255787643, 255788703, 255882036, 256444993, 257694165, 257921351, 262476000, 263721816, 276102642, 276449590, 279460864, 282317658, 282359850, 285282252, 285319482, 285770518, 286985748, 287966898, 288775038, 292921272, 293731272, 293904640, 294369286, 295697470, 296702734, 301750126, 302214028, 303615577, 306482761, 307173570, 307291170, 308163407, 311360822, 311534425, 312039559, 312436974, 312542697, 315272623, 317880638, 318286721, 319292743, 320017600, 320252024, 321554723, 321580662, 322442240, 323266527, 326327879, 333700720, 335539055, 336937491, 337575662, 339768501, 340421206, 341923347, 343084576, 345509021, 346412363, 349495318, 349569834, 353084091, 353424606, 354160772, 355168766, 356343152, 359536546, 360648679, 363724571, 363758022, 365381070, 369967285, 371876003, 372105535, 373066595, 373667079, 373706861, 375507918, 383361785, 383788462, 383867949, 386383012, 386449078, 388407304, 388905349, 389022189, 391072386, 392006194, 392565237, 394071544, 394879200, 399804216, 399862798, 399920568, 401077718, 401902808, 402583877, 402611144, 405351356, 405499625, 410590632, 411804252, 412413765, 417235485, 418942836, 422748003, 425584844, 445291085, 450493225, 457637739, 458287006, 466792903, 469412275, 475522685, 480412663, 489696354, 835864135, 836056780, 836325393, 838174509, 847565596, 847783992, 850487736, 851178649, 851295431, 857788254, 858106446, 858781190, 860145568, 860264647, 860414909, 872008996, 875096848, 879017980, 881155636, 881332966, 885187787, 886742476, 889201734, 890035185, 891566317, 893135754, 898455820, 909004503, 917164766, 917319178, 919735867, 935495351, 968550668, 972243573, 997587648, 999961232, 1001714951, 1004229172, 1004763817, 1010173059, 1031547092, 1035994292, 1041276674, 1044002267, 1054936816, 1060164724, 1061076986, 1062148399, 1079923091, 1092267175, 1095157609, 1095600726, 1101855381, 1110152099, 1114778241, 1125296112, 1125860552, 1133388674, 1142230743, 1171243748, 1172719712, 1202267677, 1220993352, 1281944743, 1286226756, 1302599438, 1305619263, 1519044617, 1619169848, 1657135701, 1673586483, 1674334778, 1674613696, 1675023758, 1675517497, 1682179459, 1806010772}
    # asyncio.run(get_maps_new(maps_to_save='./count_synergy_16th_5000/1722505765_top600_maps', game_mods=[2, 22],
    #              show_prints=show_prints, ids=players_dict))
    # research_maps(mkdir='count_synergy_16th_5000', maps_to_explore='1722505765_top600_maps',
    #               file_name='winrate_check_output', show_prints=show_prints)
    explore_database(mkdir='count_synergy_10th_2000', file_name='1722505765_top600_output',
                     over35=True, counterpick1vs2=True, synergy=True, counterpick1vs1=True, lane=True,
                     counterpick1vs3=True, synergy4=True)


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
    # with open('./all_teams/1722505765_top600_output.json', 'r+') as f:
    #     data = json.load(f)
    # with open('./pro_heroes_data/pro_output.txt', 'r') as f:
    #     to_be_merged = json.load(f)
    # for map_id in to_be_merged:
    #     if map_id not in data:
    #         data[map_id] = to_be_merged[map_id]
    # with open('./all_teams/1722505765_top600_output.json', 'w') as f:
    #     json.dump(data, f)
    update_my_protracker(show_prints=True)