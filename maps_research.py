"""
Модернизированная версия maps_research.py
Изменения:
- Асинхронная работа с aiohttp для максимальной производительности
- Прокси и API ключи берутся из словаря api_to_proxy в keys.py
- Умное управление rate limits (7/сек, 138/мин, 1488/час, 14988/день)
- Автоматическое переключение между парами прокси-API при достижении лимитов
- Система сохранения состояния для get_maps_new:
  * Автоматическое сохранение каждые 100 обработанных ID
  * Восстановление после сбоев/перезапусков
  * Двухфазная обработка (исходные ID -> player IDs)
  * Атомарная запись для предотвращения повреждения данных
"""

import json
import shutil
from urllib.parse import quote
from analyze_maps import new_proceed_map, send_message
import ijson
import os
from keys import api_to_proxy, start_date_time
import asyncio
import aiohttp
from aiohttp_socks import ProxyConnector
from collections import deque
from datetime import datetime, timedelta
import time

# Rate limits для API
RATE_LIMITS = {
    'second': 7,
    'minute': 138,
    'hour': 1488,
    'day': 14988
}

CONCURRENCY_LIMIT = 20  # Одновременных запросов


class RateLimitTracker:
    """Отслеживает использование API для одной пары прокси-API"""
    
    def __init__(self, proxy_url, api_token):
        self.proxy_url = proxy_url
        self.api_token = api_token
        self.requests_log = {
            'second': deque(),
            'minute': deque(),
            'hour': deque(),
            'day': deque()
        }
        self.lock = asyncio.Lock()
        self.is_rate_limited = False  # Флаг, что API вернул ошибку лимита
        self.rate_limit_time = 0  # Время когда был достигнут лимит
    
    async def can_make_request(self):
        """Проверяет, можно ли сделать запрос с учетом всех лимитов"""
        async with self.lock:
            now = time.time()
            
            # Проверяем, прошло ли 3 минуты с момента rate limit от API
            if self.is_rate_limited:
                if now - self.rate_limit_time < 180:  # 3 минуты = 180 секунд
                    return False
                else:
                    # Прошло 3 минуты, сбрасываем флаг
                    self.is_rate_limited = False
                    print(f"✅ Восстановление пары: прошло 3 минуты с момента rate limit")
            
            # Очищаем старые записи и проверяем лимиты
            time_windows = {
                'second': 1,
                'minute': 60,
                'hour': 3600,
                'day': 86400
            }
            
            for period, window in time_windows.items():
                # Удаляем старые записи
                while self.requests_log[period] and now - self.requests_log[period][0] > window:
                    self.requests_log[period].popleft()
                
                # Проверяем лимит
                if len(self.requests_log[period]) >= RATE_LIMITS[period]:
                    return False
            
            return True
    
    async def mark_rate_limited(self):
        """Помечает tracker как достигший лимита по ответу API"""
        async with self.lock:
            self.is_rate_limited = True
            self.rate_limit_time = time.time()
            proxy_short = self.proxy_url.split('@')[-1] if '@' in self.proxy_url else self.proxy_url[:30]
            print(f"⛔ API вернул rate limit для прокси {proxy_short}, блокируем на 3 минуты")
    
    async def record_request(self):
        """Записывает факт выполнения запроса"""
        async with self.lock:
            now = time.time()
            for period in self.requests_log:
                self.requests_log[period].append(now)


class ProxyAPIPool:
    """Управляет пулом прокси-API пар с автоматическим переключением"""
    
    def __init__(self, api_to_proxy_dict):
        self.trackers = [
            RateLimitTracker(proxy_url, api_token)
            for proxy_url, api_token in api_to_proxy_dict.items()
        ]
        self.current_index = 0
        self.semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    
    async def get_available_tracker(self):
        """Получает доступный tracker или ждет, пока он освободится"""
        max_attempts = len(self.trackers) * 10
        attempt = 0
        previous_index = self.current_index
        
        while attempt < max_attempts:
            # Проверяем все trackers начиная с текущего
            for i in range(len(self.trackers)):
                idx = (self.current_index + i) % len(self.trackers)
                tracker = self.trackers[idx]
                
                if await tracker.can_make_request():
                    # Выводим информацию при смене пары
                    if idx != previous_index:
                        proxy_short = tracker.proxy_url.split('@')[-1] if '@' in tracker.proxy_url else tracker.proxy_url[:30]
                        api_short = tracker.api_token[:20] + '...' if len(tracker.api_token) > 20 else tracker.api_token
                        print(f"🔄 Переключение на пару #{idx + 1}: Прокси={proxy_short}, API={api_short}")
                    
                    self.current_index = idx
                    return tracker
            
            # Если ни один не доступен, ждем немного
            print(f"⏳ Все API достигли лимитов, ожидание 0.5 сек...")
            await asyncio.sleep(0.5)
            attempt += 1
        
        # Если после всех попыток все еще нет доступных, берем первый
        print("⚠️ Превышено время ожидания, использую первый доступный tracker")
        return self.trackers[0]
    
    async def make_request(self, url, **kwargs):
        """Выполняет запрос с автоматическим выбором tracker и rate limiting"""
        max_retries = len(self.trackers) + 1  # Попробуем все пары + 1 попытка после сна
        retry_count = 0
        
        while retry_count < max_retries:
            async with self.semaphore:
                tracker = await self.get_available_tracker()
                
                # Создаем connector для этого прокси
                connector = ProxyConnector.from_url(tracker.proxy_url)
                
                # Обновляем headers с нужным токеном
                if 'headers' in kwargs:
                    kwargs['headers']['Authorization'] = f"Bearer {tracker.api_token}"
                
                try:
                    # Создаем отдельную сессию с connector для этого прокси
                    async with aiohttp.ClientSession(connector=connector) as proxy_session:
                        async with proxy_session.post(url, ssl=False, **kwargs) as response:
                            data = await response.json()
                            
                            # Проверяем на rate limit от API
                            if isinstance(data, dict) and data.get('message') == 'API rate limit exceeded':
                                proxy_short = tracker.proxy_url.split('@')[-1] if '@' in tracker.proxy_url else tracker.proxy_url[:30]
                                print(f"⛔ API rate limit exceeded для прокси {proxy_short}")
                                
                                # Помечаем tracker как заблокированный
                                await tracker.mark_rate_limited()
                                
                                # Переключаемся на следующий tracker
                                old_index = self.current_index
                                self.current_index = (self.current_index + 1) % len(self.trackers)
                                retry_count += 1
                                
                                # Проверяем, все ли пары заблокированы
                                all_limited = all(t.is_rate_limited for t in self.trackers)
                                if all_limited:
                                    print(f"😴 Все пары заблокированы rate limit! Сон на 3 минуты...")
                                    await asyncio.sleep(180)  # 3 минуты
                                    # Сбрасываем флаги после сна
                                    for t in self.trackers:
                                        t.is_rate_limited = False
                                    print(f"⏰ Пробуждение! Продолжаем работу...")
                                    retry_count = 0  # Сбрасываем счетчик после сна
                                
                                continue  # Пробуем следующую пару
                            
                            # Если все ок, записываем запрос и возвращаем данные
                            await tracker.record_request()
                            return data
                            
                except Exception as e:
                    proxy_short = tracker.proxy_url.split('@')[-1] if '@' in tracker.proxy_url else tracker.proxy_url[:30]
                    print(f"❌ Ошибка запроса через прокси {proxy_short}: {e}")
                    
                    # При ошибке переключаемся на следующий tracker
                    old_index = self.current_index
                    self.current_index = (self.current_index + 1) % len(self.trackers)
                    next_tracker = self.trackers[self.current_index]
                    next_proxy_short = next_tracker.proxy_url.split('@')[-1] if '@' in next_tracker.proxy_url else next_tracker.proxy_url[:30]
                    next_api_short = next_tracker.api_token[:20] + '...' if len(next_tracker.api_token) > 20 else next_tracker.api_token
                    print(f"🔄 Переключение на пару #{self.current_index + 1} (из-за ошибки): Прокси={next_proxy_short}, API={next_api_short}")
                    retry_count += 1
                    
                    if retry_count < max_retries:
                        continue
                    else:
                        raise
        
        raise Exception("Исчерпаны все попытки запроса")


# Создаем глобальный пул прокси-API
proxy_pool = ProxyAPIPool(api_to_proxy)

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


def save_get_maps_state(maps_to_save, processed_ids, output_data, player_ids, all_teams, phase):
    """
    Сохраняет текущее состояние get_maps_new
    
    Args:
        maps_to_save: базовый путь для сохранения
        processed_ids: список обработанных ID
        output_data: собранные карты
        player_ids: собранные ID игроков
        all_teams: словарь команд
        phase: фаза обработки (1 - первый проход, 2 - второй проход)
    """
    state_file = f'{maps_to_save}_state.json'
    state = {
        'processed_ids': list(processed_ids),
        'output_data': output_data,
        'player_ids': list(player_ids),
        'all_teams': all_teams,
        'phase': phase,
        'timestamp': time.time()
    }
    
    temp_file = f'{state_file}.tmp'
    with open(temp_file, 'w') as f:
        json.dump(state, f, indent=2)
    
    # Атомарная замена файла
    if os.path.exists(state_file):
        os.replace(temp_file, state_file)
    else:
        os.rename(temp_file, state_file)
    
    print(f"💾 Состояние сохранено: {len(processed_ids)} ID обработано, {len(output_data)} карт собрано")


def load_get_maps_state(maps_to_save):
    """
    Загружает сохраненное состояние get_maps_new
    
    Returns:
        tuple: (processed_ids, output_data, player_ids, all_teams, phase) или None
    """
    state_file = f'{maps_to_save}_state.json'
    
    if not os.path.exists(state_file):
        return None
    
    try:
        with open(state_file, 'r') as f:
            state = json.load(f)
        
        processed_ids = set(state['processed_ids'])
        output_data = state['output_data']
        player_ids = set(state['player_ids'])
        all_teams = state['all_teams']
        phase = state['phase']
        timestamp = state.get('timestamp', 0)
        
        print(f"📂 Восстановлено состояние: {len(processed_ids)} ID обработано, {len(output_data)} карт собрано")
        print(f"   Фаза: {phase}, Сохранено: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))}")
        
        return processed_ids, output_data, player_ids, all_teams, phase
    except Exception as e:
        print(f"⚠️ Ошибка загрузки состояния: {e}")
        return None


def clear_get_maps_state(maps_to_save):
    """Удаляет файл состояния после успешного завершения"""
    state_file = f'{maps_to_save}_state.json'
    if os.path.exists(state_file):
        os.remove(state_file)
        print(f"🗑️  Файл состояния удален: {state_file}")

async def get_maps_new(game_mods, maps_to_save, ids,
                 show_prints=None, skip=0, count=0, only_in_ids=False):
    # Попытка восстановить состояние
    saved_state = load_get_maps_state(maps_to_save)
    
    if saved_state:
        processed_ids, output_data, player_ids, all_teams, phase = saved_state
        count = len(processed_ids)
    else:
        processed_ids = set()
        output_data = []
        player_ids = set()
        all_teams = {}
        phase = 1
    
    ids_to_graph = []
    ids_set = set(ids)
    
    # Фаза 1: Обработка исходных ID
    if phase == 1:
        print(f"🔄 Фаза 1: Обработка {len(ids_set)} исходных ID...")
        remaining_ids = ids_set - processed_ids
        
        for check_id in remaining_ids:
            count += 1
            ids_to_graph.append(check_id)
            processed_ids.add(check_id)

            if show_prints:
                print(f'{count}/{len(ids_set)}')

            if len(ids_to_graph) == 5 or count == len(ids_set):
                player_ids = await proceed_get_maps(skip=skip, game_mods=game_mods, only_in_ids=only_in_ids,
                                                     output_data=output_data, ids_to_graph=ids_to_graph,
                                                     all_teams=all_teams, player_ids=player_ids,
                                                     player_ids_check=True, pro=False)
                ids_to_graph = []
            
            # Сохранение каждые 100 ID
            if len(processed_ids) % 100 == 0:
                save_get_maps_state(maps_to_save, processed_ids, output_data, player_ids, all_teams, phase=1)
        
        # Сохранение после завершения фазы 1
        save_get_maps_state(maps_to_save, processed_ids, output_data, player_ids, all_teams, phase=1)
        
        with open('count_synergy_10th_2000/player_ids.txt', 'w') as f:
            json.dump(list(player_ids), f)
        
        # Переход к фазе 2
        phase = 2
        processed_ids = set()  # Сбрасываем для новой фазы
        count = 0
    
    # Фаза 2: Обработка player_ids
    if phase == 2:
        print(f"🔄 Фаза 2: Обработка {len(player_ids)} player ID...")
        remaining_player_ids = player_ids - processed_ids
        
        for check_id in remaining_player_ids:
            count += 1
            ids_to_graph.append(check_id)
            processed_ids.add(check_id)

            if show_prints:
                print(f'{count}/{len(player_ids)}')

            if len(ids_to_graph) == 5 or count == len(player_ids):
                await proceed_get_maps(skip=skip, game_mods=game_mods, only_in_ids=only_in_ids,
                                                     output_data=output_data, ids_to_graph=ids_to_graph,
                                                     all_teams=all_teams, pro=False)
                ids_to_graph = []
            
            # Сохранение каждые 100 ID
            if len(processed_ids) % 100 == 0:
                save_get_maps_state(maps_to_save, processed_ids, output_data, player_ids, all_teams, phase=2)
        
        # Сохранение после завершения фазы 2
        save_get_maps_state(maps_to_save, processed_ids, output_data, player_ids, all_teams, phase=2)
    
    # Финальное сохранение результатов
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
    
    # Удаление файла состояния после успешного завершения
    clear_get_maps_state(maps_to_save)
    print(f"✅ Обработка завершена! Собрано {len(output_data)} карт")


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



async def proceed_get_maps(skip, only_in_ids, output_data,
                           player_ids=None, all_teams=None, ids_to_graph=None, game_mods=None,
                     check=True, player_ids_check=False, pro=False):
    while check:
        if pro:
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
        else:
            query = f'''
            {{
              players(steamAccountIds: {ids_to_graph}) {{
                steamAccount{{
                    smurfFlag
                    isAnonymous
                    id
                    name 
                }}

                matches(request:
                 {{startDateTime: {start_date_time},
                 take: 100,
                  skip: {skip},
                   gameModeIds: {game_mods},
                    regionIds: [1, 2, 3, 5],
                     bracketIds: [8],
                      isStats: true}}) {{
                  id
                  regionId
                  averageRank
                  lobbyType
                  players{{
                    intentionalFeeding
                    steamAccount{{
                      smurfFlag
                      id
                      isAnonymous
                    }}
                  }}
                }}
              }}
            }}
            '''

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Origin": "https://api.stratz.com",
            "Referer": "https://api.stratz.com/graphiql",
            "User-Agent": "STRATZ_API",
            "Authorization": "Bearer placeholder"  # Будет заменен в make_request
        }
        try:
            data = await proxy_pool.make_request(
                url='https://api.stratz.com/graphql',
                json={"query": query},
                headers=headers
            )

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
            check = False
    
    if player_ids_check:
        return player_ids
    else:
        return None


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
        folder_path = "count_synergy_10th_2000/json_parts_split_from_object"

        ids_to_exclude_from_json = collect_all_maps(folder_path=folder_path, output=True)

        # --- Шаг 3: Финальная фильтрация ---
        print("\nШаг 3: Финальная фильтрация...")
        # Основной цикл по картам
        final_new_maps = {map_id for map_id in current_maps_to_filter if str(map_id) not in ids_to_exclude_from_json}
    
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
            "Authorization": "Bearer placeholder"  # Будет заменен в make_request
        }
        try_counter = 0
        check = True
        while check == True:
            if try_counter >= 3:
                break
            try:
                response_data = await proxy_pool.make_request(
                    url='https://api.stratz.com/graphql',
                    json={"query": query},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15)
                )

                data = response_data['data']['match']
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
                print(f'error, error: {e}')
                try_counter += 1
                print(f'попытка {try_counter}/3')
        
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

    if pro: 
        pass
    else:
        path = f'./{mkdir}/{maps_to_explore}.txt'
        if not os.path.exists(path):
            print(f"Файл {path} не найден. Пропускаю research_maps.")
            return
        with open(path, 'r+') as f:
            maps_to_explore = json.load(f)
    asyncio.run(research_map_proceed(
        maps_to_explore=maps_to_explore,
        mkdir=mkdir, show_prints=True, pro=pro))







def explore_database(mkdir, file_name, pro=False, lane=None,
                     over40=None, total_time_kills_teams=None, time_kills=None,
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
        over40_dict=over40, lane_dict=lane,
        total_time_kills_dict_teams=total_time_kills_teams,
        counterpick1vs2=counterpick1vs2,
        counterpick1vs1=counterpick1vs1, synergy=synergy,

        counterpick1vs3=counterpick1vs3,
        synergy4=synergy4)

    used_maps = load_json_file(f'./{mkdir}/used_maps.txt', [])

    result = analyze_database(
        database=database,
        total_time_kills_dict=data_files['total_time_kills_dict'], over40_dict=data_files['over40_dict'],
        lane_dict=data_files['lane_dict'], pro=pro, used_maps=used_maps,
        total_time_kills_dict_teams=data_files['total_time_kills_dict_teams'],
        counterpick1vs2=data_files['counterpick1vs2'],
        synergy=data_files['synergy'], counterpick1vs1=data_files['counterpick1vs1'],
        counterpick1vs3=data_files['counterpick1vs3'],
        synergy4=data_files['synergy4'])

    if result is not None:
        lane_dict, total_time_kills_dict, synergy, counterpick1vs1, \
            over40_dict, total_time_kills_dict_teams, counterpick1vs2, \
            counterpick1vs3, synergy4, used_maps = result

        print('Сохранение обновленных данных')

        save_json_file(f'./{mkdir}/used_maps.txt', used_maps)
        if total_time_kills_teams:
            save_json_file(f'./{mkdir}/total_time_kills_dict.txt', total_time_kills_dict)
            save_json_file(f'./{mkdir}/total_time_kills_dict_teams.txt', total_time_kills_dict_teams)
        if counterpick1vs2:
            save_json_file(f'./{mkdir}/synergy.txt', synergy)
            save_json_file(f'./{mkdir}/counterpick1vs1.txt', counterpick1vs1)
            save_json_file(f'./{mkdir}/over40_dict.txt', over40_dict)
            save_json_file(f'./{mkdir}/counterpick1vs2.txt', counterpick1vs2)
            save_json_file(f'./{mkdir}/counterpick1vs3.txt', counterpick1vs3)
            save_json_file(f'./{mkdir}/synergy4.txt', synergy4)
        if lane_dict:
            save_json_file(f'./{mkdir}/lane_dict.txt', lane_dict)


def check_match(match):
    #match['startDateTime'] >= int(start_date_time) and
    if match['direKills'] is not None and all(player['intentionalFeeding'] is False and player['steamAccount']['smurfFlag'] in [0, 2]
                and None not in [player['position'], player['hero']['id'], player['steamAccount']]
                for player in match['players']) and len(match['radiantNetworthLeads']) >= 20:
        return True



def analyze_database(database, over40_dict, used_maps=None,
                     total_time_kills_dict=None, pro=False,
                     lane_dict=None, check=False,
                     total_time_kills_dict_teams=None, counterpick1vs2=None,
                     counterpick1vs1=None, synergy=None,
                     counterpick1vs3=None,
                     synergy4=None
                     ):
    counter = []
    # new_maps = [str(map_id) for map_id in database if str(map_id) not in used_maps]
    win_loose = {}
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
                    over40_dict, total_time_kills_dict_teams, counterpick1vs2 = result
        else:
            if check_match(match=match):
                counter.append(map_id)
                win_loose = new_proceed_map(
                    match=match,
                    lane_dict=lane_dict, synergy=synergy, counterpick1vs1=counterpick1vs1,
                    over40_dict=over40_dict, counterpick1vs2=counterpick1vs2, counterpick1vs3=counterpick1vs3,
                    synergy4=synergy4, win_loose=win_loose
                )
                # lane_dict, total_time_kills_dict, synergy, counterpick1vs1, \
                #     over40_dict, total_time_kills_dict_teams, counterpick1vs2, \
                #     counterpick1vs3, synergy4 = result
    send_message(win_loose)
    if check:
        used_maps = counter
        return lane_dict, total_time_kills_dict, synergy, counterpick1vs1, \
                    over40_dict, total_time_kills_dict_teams, counterpick1vs2, \
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
    # regions = [[5000, 'EUROPE'],[2500, 'SEA_ASIA']]
    # for top, region in regions:
    #     players_dict = get_players(top=top, region=region, players_dict=players_dict, skip=0)
    players_dict = {2491317, 3165951, 5200380, 12020620, 13707413, 13959693, 16184552, 17541892, 20603135, 20647693, 21646886, 22127749, 22228773, 25765934, 25907144, 26316691, 26717183, 27985297, 31554592, 31966955, 32995405, 34505203, 35125459, 37700130, 37980025, 38672293, 39155815, 39270453, 39890357, 40013910, 40428390, 40547474, 40907284, 41843638, 42194754, 43488703, 44634230, 45008830, 45086239, 45831366, 46527182, 46723069, 47529331, 47646696, 47865081, 48578808, 49094404, 49181168, 52197664, 53199954, 54543434, 56005097, 56305272, 56351509, 56850123, 56939869, 57181321, 58327179, 58829853, 59243037, 59853536, 60175284, 60758299, 61129702, 61365910, 61525086, 61709759, 62669678, 63300894, 63703276, 64163607, 64527692, 66620961, 66624488, 67131497, 67615717, 67690521, 67893845, 67948122, 69379556, 70473277, 71869498, 72393079, 73401082, 73442395, 74371974, 75154578, 75378194, 75926289, 76140346, 77498503, 77657475, 78272635, 78937420, 80209737, 80560677, 81475303, 82169593, 82848151, 83320648, 83347338, 83755246, 83785648, 84040359, 84385735, 84435726, 84534848, 84555447, 84658533, 84852390, 84853828, 85000862, 85312703, 85405799, 86056757, 86196027, 86214606, 86596735, 86692726, 86697587, 86698277, 86715129, 86723143, 86727555, 86738694, 86743032, 86749028, 86790014, 86800800, 86811043, 86815623, 86818655, 86841653, 86930446, 86935863, 86953414, 86978647, 86999714, 87012746, 87095102, 87100244, 87231335, 87508680, 87657476, 88060825, 88600100, 88809428, 89050869, 89128606, 89140271, 89354043, 89945143, 90255910, 90324740, 90373262, 90523273, 90549446, 90897593, 91004946, 91041819, 91064780, 91888718, 92010712, 92125240, 92253768, 92465827, 92487440, 92601861, 92673082, 92706637, 92847434, 92949094, 93157994, 93300465, 93526520, 93613926, 93618577, 93845249, 93850611, 93857526, 93954809, 94054712, 94099603, 94381641, 95134626, 95583197, 95727214, 96073976, 96133582, 96468516, 97067110, 97072681, 97565075, 97601572, 97658618, 97975407, 98030136, 98064388, 98083057, 98134803, 98316657, 98887913, 99036634, 99611577, 99622043, 99724180, 99753336, 100243458, 100264552, 100502984, 100547629, 100610603, 100708506, 100750516, 100927601, 101062921, 101213815, 101356886, 101446572, 101555768, 101717646, 101779337, 101836533, 101875787, 101919209, 101966248, 102071988, 102076232, 102113695, 102564708, 102750905, 102806766, 103039499, 103181197, 103532829, 103548361, 103983996, 104185879, 104194483, 104242598, 104334048, 104945782, 105178768, 105189649, 105296976, 105344475, 105670426, 105938583, 106028932, 106059944, 106278629, 106305042, 106601423, 106682821, 106784945, 106798176, 106809101, 106810939, 107023378, 107026088, 107095093, 107339735, 107429672, 107459600, 108044419, 108141300, 108203659, 108329926, 108361882, 108452107, 108813260, 108816362, 108928414, 109151792, 109221881, 109310784, 109526481, 109757023, 109810461, 110155287, 110547982, 110605738, 110622167, 110846314, 111028607, 111065730, 111114687, 111295526, 111588646, 111750003, 112015645, 112162809, 112252338, 112296859, 112426916, 112570267, 112603011, 112702897, 112998794, 113086124, 113106253, 113112046, 113184394, 113399157, 113440464, 113506847, 113545167, 113562644, 113627294, 113628755, 113656339, 113685595, 113823711, 113826617, 113846851, 113926403, 113995822, 114035408, 114047775, 114062796, 114411775, 114585639, 114596500, 114619230, 115032022, 115081825, 115116817, 115122729, 115124260, 115289941, 115461162, 115619315, 115771119, 115832981, 116170570, 116387233, 116517704, 116636452, 116655004, 116771693, 116790182, 116798601, 116898106, 116985284, 117005046, 117110711, 117200195, 117511039, 117514269, 117969907, 118078153, 118134220, 118370366, 118452902, 118480463, 118616714, 118790872, 118948478, 119315361, 119582553, 119894854, 119942696, 119952353, 120140212, 120300150, 120484392, 120492597, 120613892, 120823356, 120825388, 120834725, 120840516, 121021550, 121245522, 121312218, 121427985, 121443783, 121463940, 121592403, 121604175, 121716645, 121730609, 121835291, 121870008, 121885658, 121935872, 121952627, 122009948, 122111630, 122226456, 122296320, 122867857, 123098687, 123213918, 123233667, 123479534, 123480548, 123497536, 123622933, 123717676, 123723783, 123787715, 123884539, 124286163, 124499908, 124583384, 124805486, 124933983, 124955874, 125103744, 125340437, 125576983, 125720851, 125941160, 126334304, 126491938, 126507159, 126715113, 126894593, 127128776, 127402903, 127565532, 127583085, 127631696, 127709939, 127979994, 128047066, 128281272, 128325052, 128363431, 128433092, 128469479, 128620862, 128882487, 128958133, 129382258, 129431940, 129466072, 129634323, 129672402, 129682328, 129737989, 130024006, 130424429, 130556400, 130881171, 131072392, 131074643, 131242096, 131246802, 131303632, 131338538, 131627283, 131706718, 131750073, 131899109, 132028861, 132075585, 132113929, 132405276, 132762072, 132945279, 133066657, 133167741, 133467677, 133558180, 133566339, 133604012, 133638954, 133742226, 133763935, 133958007, 133972492, 134039073, 134042150, 134993353, 135269892, 135403353, 135490756, 135695148, 135842744, 135893728, 135910042, 136234544, 136280816, 136336564, 136342177, 136421659, 136438697, 136584519, 136640634, 136642777, 136656581, 136708164, 136811583, 136829091, 136948938, 137112063, 137218474, 137235488, 137340624, 137371827, 137437979, 137578769, 137834028, 137858228, 138105639, 138155777, 138163279, 138201042, 138237476, 138276994, 138428588, 138502942, 138535219, 138717510, 138733307, 138759332, 139056232, 139058996, 140035883, 140135497, 140251702, 140308444, 140747062, 140894725, 140986936, 141044566, 141441619, 141522029, 141598505, 141690233, 141792676, 141793311, 141820629, 141821056, 141986372, 142036603, 142126517, 142192158, 142261292, 143067868, 143647827, 144035532, 144199048, 144314200, 144330586, 144567157, 144618590, 144645376, 144831806, 145035735, 145169950, 145372258, 145521249, 145548621, 145550466, 145822880, 146151456, 146260864, 146314431, 146383297, 146553623, 146696628, 146711951, 146740161, 147191038, 147301034, 147326075, 147371897, 147421987, 147455609, 147680845, 147765887, 148050388, 148180390, 148306100, 148345718, 148392709, 148458573, 148520614, 148531879, 148612029, 148720022, 148748880, 148844441, 149060164, 149121138, 149346253, 149469043, 149868395, 149877520, 149976499, 150042707, 150166015, 150302606, 150354245, 150408190, 150436963, 150583903, 150599254, 150722482, 150748053, 150794204, 150955045, 150961567, 151012205, 151040811, 151482973, 151669649, 152168157, 152224059, 152285172, 152326301, 152358864, 152455523, 152653448, 152724304, 152763828, 152775717, 152867636, 153125655, 153241037, 153298625, 153836240, 153958490, 154042978, 154095378, 154215694, 154247846, 154259887, 154547698, 154715080, 155152821, 155347318, 155384312, 155550258, 155934273, 156057252, 156090907, 156091598, 156094537, 156485213, 156847191, 156867495, 157231117, 157420163, 157979092, 158004558, 158023533, 158028416, 158028751, 158072796, 158212442, 158313372, 158370131, 159804661, 159893598, 160567764, 160637401, 160840894, 160915747, 160922188, 161025060, 161237856, 161303613, 161315016, 161395326, 161473562, 161523234, 161874561, 162003654, 162056712, 162254677, 162281629, 162390140, 162407868, 162682017, 162682114, 162755197, 162798290, 162934986, 162973137, 163168569, 163265271, 163318777, 163458082, 163852918, 164047347, 164318843, 164506534, 164608923, 165044320, 165056865, 165079425, 165110440, 165110447, 165165106, 165169752, 165213126, 165390194, 165432797, 165533857, 165666832, 165896239, 166130266, 166156236, 166303352, 166307359, 166354697, 166431679, 166436385, 166580596, 167594359, 167676347, 167732534, 167789888, 167963776, 167976729, 168248956, 168324025, 168444329, 168747933, 168997198, 169025618, 169093366, 169178078, 169319226, 169359999, 169375107, 169819177, 170002205, 170159478, 170343586, 170441479, 170537507, 170631251, 170649608, 170691507, 170800331, 170809075, 170834508, 170909614, 171017631, 171097887, 171343020, 171348003, 171382172, 171484881, 171516467, 171663650, 171981096, 171987940, 172209415, 172269889, 172509009, 172641071, 172784619, 172884720, 172962400, 173288904, 173554608, 173609670, 173623671, 173635137, 173727526, 173775411, 173978074, 174205711, 174240411, 174394307, 174653834, 174674219, 174738592, 174785170, 174926595, 175061340, 175088787, 175215418, 175350492, 175364221, 175709801, 175850524, 175891708, 176001885, 176019821, 176337606, 176383824, 176516783, 176574300, 176876117, 176923146, 176999327, 177203952, 177763156, 177810199, 177811921, 177953305, 178007891, 178049714, 178211469, 178362814, 178386292, 178398852, 178439734, 178518803, 178582083, 178593079, 178613196, 178633570, 178647305, 178692606, 178712891, 178775992, 178950805, 179136229, 179151998, 179266369, 179273059, 179332933, 179528074, 179684293, 180019416, 180119881, 180206394, 180578103, 180672452, 180771645, 180943830, 181145254, 181267029, 181512810, 181661363, 181774949, 181800065, 181981546, 181987328, 182120149, 182158278, 182181879, 182597785, 183098834, 183099079, 183126002, 183243764, 183633429, 183719386, 183803114, 183950220, 184298001, 184442022, 184455528, 184594928, 184620877, 184888058, 184945792, 185001094, 185059559, 185202677, 185250947, 185297764, 185390462, 185590374, 185922448, 185996267, 186208519, 186287226, 186485592, 186580415, 186793900, 186837494, 186867023, 187023498, 187123736, 187162352, 187209969, 187237155, 187484163, 187590892, 187594662, 187807370, 187949640, 188096911, 188158297, 188288391, 188351504, 188553676, 188615888, 188690722, 188710502, 188908561, 188918464, 188954158, 188962397, 189038762, 189046705, 189099381, 189182601, 189629677, 189757870, 189875608, 190100004, 190161507, 190335195, 190336611, 190507128, 190652639, 190748369, 190826739, 190859741, 190996591, 191066225, 191116892, 191362875, 191432914, 191434304, 191451281, 191471157, 191476203, 191688798, 191734430, 191823389, 191930708, 191976933, 191983286, 191988361, 192455650, 192596064, 192905118, 192981126, 193099178, 193299534, 193490297, 193725688, 194119745, 194133783, 194316774, 194555122, 194651551, 194672667, 194720582, 194964226, 194979527, 195252395, 195254520, 195316945, 195370818, 195522403, 195539164, 195640618, 195887659, 195965262, 195966181, 196041577, 196102929, 196134157, 196150144, 196349774, 196403525, 196481318, 196485328, 196490133, 196670605, 196808931, 197060254, 197108428, 197175284, 197888454, 197934936, 198060138, 198097008, 198161112, 198225216, 198492947, 198993981, 198998203, 199162825, 199293770, 199475844, 199666759, 199797003, 199866366, 200362560, 200381676, 200450157, 200619949, 200890524, 201267628, 201282013, 201814651, 201838948, 201943746, 202256116, 202269510, 202451730, 202612285, 202691456, 202739063, 203080877, 203300544, 203367056, 203600694, 203647531, 203820910, 203919918, 203948336, 204091228, 204252726, 204264623, 204433019, 204655516, 204889891, 205264217, 205386205, 206266593, 206477977, 206593195, 206642367, 206764710, 207100109, 207275256, 207283505, 207299604, 207373044, 207527352, 207997426, 208407289, 208680365, 208993316, 209063076, 209103998, 209224693, 209238427, 209437333, 210294047, 210439141, 210478107, 210522998, 210673394, 210717878, 210854005, 210877577, 211050073, 211320339, 212189815, 212250622, 212316317, 212439729, 212713081, 212719394, 212958343, 214853734, 215132465, 215151322, 215911245, 216046097, 216609078, 216733371, 216763662, 217472313, 217475266, 217511153, 217549711, 217599053, 217642019, 217903575, 218231587, 218476207, 218675899, 218717442, 218804865, 219089581, 219225327, 219443278, 219479162, 219498267, 220386762, 220722971, 220896899, 221178083, 221378793, 221521139, 221532774, 221553445, 221893707, 221945821, 221990709, 222017567, 222478478, 222822017, 223286675, 223292351, 223382342, 225394157, 225418122, 226017482, 226145427, 226786631, 227302161, 228334306, 228392819, 228440453, 228810690, 228909633, 229246501, 229311481, 229361989, 229580899, 230233646, 230447517, 230487729, 231034202, 231383285, 231551445, 231656717, 232202813, 232281409, 232474483, 232510319, 232935459, 233213323, 234408142, 234895626, 235505993, 235627765, 235880461, 236214375, 236215455, 236397067, 236538994, 236556669, 236770322, 236858633, 237126534, 238214429, 238476827, 238509899, 239681562, 239904854, 240368067, 240633114, 241278470, 241446086, 241500617, 241881425, 241931261, 242371606, 242498151, 242885483, 243566367, 243671312, 243682175, 243956913, 244138351, 244656332, 244695857, 244988455, 245191708, 245373129, 245759489, 245820572, 245832512, 246236955, 246774973, 246904429, 247409832, 247706713, 248111944, 248180032, 248280724, 249043309, 249323455, 249639667, 250358373, 250375405, 250502369, 250543269, 250858864, 251079631, 251592500, 251735016, 252299657, 252369016, 252659836, 252737052, 252775376, 253215083, 253261807, 253696271, 253833725, 254321085, 254555136, 254775288, 255666139, 255805339, 255882036, 256242905, 256363333, 256444993, 256653700, 256876689, 256953147, 257086687, 257542592, 257579104, 257617140, 257921351, 258160485, 258266759, 258446395, 258607808, 258657655, 258737965, 258891667, 260861848, 261844400, 262051984, 262476000, 263052040, 263103040, 263337812, 263635796, 264385748, 265390096, 265865492, 266262432, 268538568, 268869104, 270028424, 271754424, 272073508, 272852308, 273482624, 274595804, 274709016, 275825964, 275978120, 276039696, 276096844, 276449590, 277625412, 278859394, 278869764, 279002614, 279136308, 279191078, 279460864, 279559282, 279609182, 279618516, 279901390, 279932958, 280060896, 280585026, 280609660, 280948870, 281011866, 281262206, 282855844, 283080086, 283324818, 284780604, 284969604, 285100046, 285282252, 285327298, 285333612, 285333678, 285652188, 286299456, 286710912, 286828712, 286885652, 287417056, 287851776, 288133988, 288775038, 289499094, 289612202, 290809126, 290993576, 291045600, 292001818, 292030276, 292740031, 292921272, 292987051, 293022110, 293082908, 293164215, 293216275, 293363491, 293532454, 293615161, 294588813, 294789305, 295697470, 296226696, 296497612, 296601357, 296702734, 297148885, 297366329, 297380131, 297396547, 297447116, 297468592, 297591746, 297897435, 298060380, 298239411, 298369590, 298874811, 299103822, 299821048, 300504856, 300713811, 300716391, 301312124, 301514738, 301740026, 301936781, 302214028, 302380382, 302568414, 303258117, 303469415, 303568093, 303701909, 303879204, 303886403, 304312989, 304837639, 306856502, 307173570, 307396124, 307856613, 307971881, 308125438, 308163407, 308329834, 308609737, 308856459, 309113107, 309161920, 309277176, 312051473, 312194525, 312436974, 312527359, 312542697, 312580335, 312825747, 313119492, 313198707, 313297594, 313433186, 313825023, 313833845, 315472419, 315616422, 315670214, 316064160, 316491582, 316555102, 316616502, 316762095, 317465188, 317506304, 317657059, 317725979, 317880638, 318038024, 318112183, 318238575, 318710781, 318911129, 319084760, 319292743, 319871163, 319905167, 320017600, 320080435, 320252024, 320718017, 321554723, 321580662, 322450346, 322566964, 323266527, 323496545, 324045303, 324244459, 324601317, 325511281, 325547263, 325697153, 325799640, 325973787, 326046442, 326327879, 327143805, 327449067, 327731197, 327742421, 328010952, 328913574, 329007457, 329076390, 329386008, 329611191, 329766071, 330535786, 330840377, 331348180, 333007691, 333700720, 333905852, 334566461, 334617307, 335011172, 335091471, 335279284, 335610712, 337374555, 337505597, 337903205, 337958343, 338044201, 338181971, 338259526, 339195074, 339632663, 339686412, 339753482, 339790768, 339833319, 340123379, 340421206, 340825469, 341418243, 341515002, 341953415, 342003544, 342326251, 343084576, 343846855, 344349056, 344389023, 344393935, 344425669, 344443117, 344574251, 344828931, 345372059, 345378043, 345509021, 346174811, 346332962, 346412363, 346506228, 347445342, 347513727, 347998471, 348357637, 348668812, 348672390, 348974674, 349057170, 349342480, 349472155, 349495318, 349650808, 350377601, 350885810, 351476862, 351781933, 352906755, 353413837, 353656289, 353881304, 354009983, 354079122, 354083756, 354160772, 354533584, 355837764, 356324058, 356400427, 356538484, 356573404, 356787210, 357287712, 357370088, 357531333, 357819589, 358409506, 358507879, 359722008, 359749304, 359879068, 360079984, 360118425, 360132778, 360404518, 360423961, 360642005, 360659667, 361748943, 361906956, 362263224, 363005951, 363008486, 363026994, 363292733, 363475622, 363758022, 364498804, 364746700, 364802633, 364948043, 364974151, 365253927, 365521186, 365523159, 365868289, 366357573, 366398637, 366444186, 366953338, 367687213, 367974389, 369156326, 369683701, 370323506, 370520057, 370600515, 370769752, 370779420, 370854638, 371001816, 371467479, 371495800, 371943423, 372490639, 372801387, 372891840, 373210081, 373489202, 374150060, 374536721, 374623623, 374818694, 374823065, 375507918, 375793581, 376088654, 376313457, 376427456, 376571133, 376730488, 377432413, 377594124, 377616175, 377724939, 378440755, 378521778, 378551799, 379454309, 379887850, 380007350, 380440331, 380722508, 381193936, 381753634, 382407968, 382775198, 382848051, 383346827, 383361785, 383788462, 383867949, 383977514, 383997867, 384878585, 385404935, 385586303, 385825293, 386503299, 387152114, 387624608, 387656006, 387876442, 388392877, 388589639, 388707622, 389121242, 390073012, 390336766, 390408873, 391331182, 391497388, 392006194, 392549232, 392702734, 393052467, 393456696, 393842021, 393847908, 393869026, 393974742, 394078824, 394234915, 394275208, 394354939, 394524089, 394977691, 395690381, 395770377, 396277209, 396545059, 396554917, 396848434, 397080786, 397315771, 397625202, 398506698, 398767662, 398950206, 399464883, 399476935, 399804216, 400239835, 400657456, 401827248, 401902808, 402280845, 402287064, 402611144, 404114988, 404154129, 404410626, 404512293, 404645038, 404964897, 405351356, 405445628, 405499625, 406694028, 407321629, 407562825, 407765713, 407891938, 408364902, 408733163, 410056852, 410072351, 410707457, 411063029, 411097119, 411510025, 412152474, 412174806, 412413765, 412463210, 412543641, 413498479, 413599014, 413892599, 414557813, 414973718, 415473943, 416371040, 416418612, 416524935, 416784721, 417030995, 417512732, 417749874, 418813910, 418942836, 419391848, 420104228, 420176078, 422314512, 423220980, 423467745, 424273225, 424422885, 425049577, 425540885, 425584844, 427168167, 428007970, 429497183, 429844080, 430006448, 430155127, 431240692, 431457461, 433032599, 433261269, 433352740, 433363560, 433852622, 434100407, 434755159, 436274304, 436486876, 436656290, 437152940, 438717879, 438836119, 439605756, 439757836, 442187860, 442223499, 442242287, 443702135, 445066597, 445291085, 446376753, 446705756, 447424247, 447875920, 449065301, 450274524, 451069097, 452213172, 452400903, 452446948, 453504156, 454690995, 454883625, 454977989, 455630651, 455829227, 456264722, 457496870, 457621650, 457637739, 457914913, 458287006, 459306936, 459343331, 459925046, 460194092, 460767869, 463024240, 464363281, 465131798, 465282352, 466564763, 468131460, 468788065, 469622125, 471493286, 471809694, 475522685, 475559870, 478807359, 479625499, 479879630, 480210721, 480858308, 482364904, 484759837, 486015288, 487210990, 487622338, 487735933, 488261567, 488628155, 490010495, 491284230, 493481492, 835222562, 835864135, 836056780, 837037101, 837545117, 837720607, 838343295, 838547248, 839365482, 839466110, 839685636, 839735324, 840005916, 840803120, 841287202, 841353374, 841501097, 841818065, 842099314, 842335283, 842615887, 842698659, 842728075, 843519982, 844857704, 845219612, 845223870, 847783992, 848126223, 848141962, 848916773, 849411353, 850079896, 850309212, 850487736, 850533871, 851178649, 851295431, 852452671, 853268682, 855300499, 855450670, 855916143, 856095498, 856620094, 856687769, 856752307, 856820732, 857353902, 857593427, 857601376, 857601863, 857788254, 858106446, 858200619, 858781190, 859004824, 859615206, 860145568, 860264647, 860414909, 861440502, 862645772, 864364265, 864749189, 865440330, 867752791, 868883031, 868921899, 868980362, 869040407, 869160902, 869539367, 869784418, 870365931, 870479576, 870590705, 872008996, 872625732, 872925891, 873580368, 874379105, 874490073, 874567654, 875096848, 875598140, 876325372, 876528288, 877061388, 877213187, 878212255, 878962586, 879017980, 879044945, 880067347, 880907562, 881099776, 881332966, 881501853, 882922124, 884673041, 885225050, 885933575, 886266033, 886773124, 887245504, 887728510, 889201734, 889388989, 889691780, 890678758, 891148882, 891521610, 892069860, 892486081, 892939726, 893135754, 894613260, 895486980, 895794923, 896035252, 896084686, 896677483, 897273319, 897428366, 897543081, 897834096, 898455820, 899041750, 899936658, 900643374, 901021922, 901102841, 901206719, 902094045, 902101738, 902465110, 903119204, 903884299, 904490226, 904839538, 905662040, 906475169, 906660704, 906941247, 907925531, 908118861, 908134955, 908587991, 909083791, 909128189, 909149398, 910116352, 910189566, 910218432, 910969212, 910997837, 911977148, 912097305, 912373527, 912703606, 913372435, 913614376, 914221413, 915063962, 916319073, 917164766, 917319178, 918477782, 919090597, 919735867, 922481045, 923708928, 925856362, 926353311, 926587776, 927326161, 931886741, 931895643, 935495351, 937477270, 937516772, 941879312, 948191382, 955109178, 964617027, 968069031, 968585092, 968859991, 970012546, 970235809, 973202367, 973592336, 974488164, 979399437, 979792615, 980665966, 984985665, 993824087, 995397364, 995403262, 995667006, 997587648, 998155679, 998466316, 998580897, 999952445, 999961232, 1002076115, 1002287646, 1003333618, 1003401851, 1003586753, 1004022359, 1004229172, 1004249475, 1004580359, 1004763817, 1005588988, 1005788083, 1006857249, 1006861725, 1007517501, 1007814480, 1008300808, 1008373688, 1010173059, 1010759955, 1010931390, 1011268049, 1011355641, 1011397511, 1011445406, 1011819293, 1012991711, 1014897295, 1015154919, 1015510214, 1016507529, 1017541130, 1017663348, 1019586822, 1020402350, 1020412893, 1021019497, 1021279660, 1022219070, 1022236968, 1023573600, 1023968276, 1024719290, 1025246566, 1025767498, 1027910355, 1028012361, 1028846643, 1029291357, 1030672675, 1030756480, 1031122378, 1031547092, 1031790480, 1033072176, 1034089639, 1035016879, 1035128290, 1035138143, 1035449086, 1036341224, 1036520759, 1038277917, 1039169512, 1040637259, 1040716533, 1041276674, 1041598418, 1041725393, 1041811781, 1041970784, 1044101915, 1045249367, 1045532701, 1048424504, 1050218588, 1051079468, 1051126528, 1051757896, 1052461436, 1054066720, 1054936816, 1055196204, 1057776020, 1058937770, 1059183335, 1060164724, 1061344988, 1061899994, 1062148399, 1062882100, 1065446145, 1065598106, 1066756375, 1067552966, 1070109387, 1071126616, 1075369709, 1075839481, 1076995043, 1077478163, 1077637422, 1079923091, 1080343131, 1081467433, 1081873410, 1081989585, 1083673105, 1084642475, 1087917967, 1088362710, 1089051790, 1089590614, 1089797805, 1090128070, 1090198538, 1091212757, 1092267175, 1093995984, 1095600726, 1096103223, 1097195023, 1097379861, 1098527872, 1099344409, 1099418270, 1099509027, 1099874454, 1099887568, 1100766500, 1103046515, 1103432782, 1105621123, 1108208030, 1109417220, 1110152099, 1110429613, 1111234112, 1112243296, 1114260951, 1114792376, 1116642425, 1118125983, 1119841680, 1121785400, 1122935307, 1123283776, 1124106707, 1125257504, 1125860552, 1126606107, 1126703680, 1127314797, 1127645678, 1128187071, 1131389030, 1131793506, 1132030436, 1132684405, 1134626011, 1135889149, 1138225686, 1138276331, 1139937786, 1140134683, 1142230743, 1143772958, 1144214248, 1145610752, 1146802207, 1149749863, 1150028510, 1150329251, 1152948930, 1153800463, 1155935970, 1156655996, 1157346453, 1157374398, 1158414931, 1165956049, 1170660729, 1171243748, 1171982662, 1172719712, 1177585143, 1178006142, 1178406648, 1178785720, 1179510756, 1183701822, 1188765581, 1200050970, 1202267677, 1202863252, 1205046630, 1206624461, 1207166714, 1207615646, 1208758738, 1211296073, 1212707449, 1213475464, 1214323220, 1216150974, 1218097624, 1219089554, 1220591501, 1220993352, 1222043939, 1222254953, 1223253318, 1224741901, 1224974216, 1229557033, 1232481380, 1234769726, 1235153161, 1236289015, 1238937936, 1239324341, 1240202683, 1244582084, 1246575030, 1248900675, 1250634082, 1251464022, 1251524067, 1252089177, 1254217986, 1254571967, 1255586956, 1256390893, 1256585435, 1256730030, 1257059131, 1257870542, 1258156041, 1258223447, 1260302354, 1260737963, 1262398070, 1263983608, 1264439745, 1265231881, 1267726025, 1269653267, 1273944397, 1275271667, 1276506919, 1278904699, 1279379465, 1281944743, 1282209240, 1283185539, 1285051453, 1286226756, 1289398969, 1289522719, 1289745225, 1297619842, 1297679104, 1298133452, 1300298642, 1302599438, 1304966559, 1305502610, 1305619263, 1310988726, 1314371186, 1358391329, 1385013277, 1386764234, 1387245390, 1393794674, 1416100148, 1423013958, 1425337986, 1425732785, 1428762120, 1429032883, 1436599829, 1446994323, 1466390884, 1480439612, 1483762406, 1485070510, 1507596122, 1512112426, 1527713649, 1546211137, 1558597865, 1562453254, 1569968706, 1585720779, 1602938518, 1609059032, 1609360034, 1616275304, 1616760314, 1619169848, 1622789802, 1638010962, 1654587652, 1657135701, 1658469071, 1660896645, 1661242107, 1668987173, 1670120825, 1673556864, 1673586483, 1673716301, 1673876903, 1673922491, 1673931706, 1673997704, 1674001912, 1674066842, 1674082353, 1674121893, 1674270943, 1674334778, 1674349924, 1674376855, 1674390053, 1674486174, 1674515741, 1674563781, 1674600214, 1674613696, 1674652786, 1674674920, 1674753460, 1674754832, 1674857583, 1674882777, 1675023758, 1675176267, 1675238807, 1675320614, 1675505247, 1675517497, 1676079603, 1677568629, 1682179459, 1686483577, 1688718899, 1689570680, 1690224795, 1691906421, 1695364867, 1695501033, 1696077062, 1713187866, 1727498689, 1733653579, 1744955828, 1780121856, 1780151902, 1788910838, 1791711423, 1795351321, 1801248862, 1806010772, 1813002283, 1822920241, 1823933608, 1842491604, 1850734159, 1856659972}
    asyncio.run(get_maps_new(maps_to_save='./count_synergy_10th_2000/1722505765_top600_maps', game_mods=[2, 22],
                 show_prints=show_prints, ids=players_dict))
    # research_maps(mkdir='count_synergy_10th_2000', maps_to_explore='1722505765_top600_maps',
    #               file_name='winrate_check_output', show_prints=show_prints)
    # explore_database(mkdir='count_synergy_10th_2000', file_name='1722505765_top600_output',
    #                  over40=True, counterpick1vs2=True, synergy=True, counterpick1vs1=True, lane=True,
    #                  counterpick1vs3=True, synergy4=True)
    #

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