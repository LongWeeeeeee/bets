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
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from functools import partial

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
    """Загружает JSON файл с кешированием"""
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Ошибка загрузки {filepath}: {e}")
    return default

def load_and_process_json_files(mkdir, **kwargs):
    """Загружает JSON файлы с параллельной обработкой"""
    result = {}
    
    # Собираем задачи для параллельной загрузки
    files_to_load = [(key, f'./{mkdir}/{key}') for key, flag in kwargs.items() if flag]
    
    if files_to_load:
        # Параллельная загрузка файлов
        with ThreadPoolExecutor(max_workers=min(4, len(files_to_load))) as executor:
            futures = {executor.submit(load_json_file, filepath, {}): key 
                      for key, filepath in files_to_load}
            
            for future in futures:
                key = futures[future]
                try:
                    result[key] = future.result()
                except Exception as e:
                    print(f"⚠️ Ошибка при загрузке {key}: {e}")
                    result[key] = {}
    
    # Добавляем None для незапрошенных ключей
    for key, flag in kwargs.items():
        if not flag:
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

async def retry_request_with_proxy_rotation(request_func, *args, max_retries=None, sleep_time=300, **kwargs):
    """
    Retry функция с перебором прокси и сном при исчерпании всех прокси
    
    Args:
        request_func: асинхронная функция для выполнения запроса
        max_retries: максимальное количество попыток (None = бесконечно)
        sleep_time: время сна в секундах при исчерпании всех прокси
    """
    attempt = 0
    
    while max_retries is None or attempt < max_retries:
        try:
            result = await request_func(*args, **kwargs)
            return result
        except Exception as e:
            attempt += 1
            print(f"⚠️ Ошибка при запросе (попытка {attempt}): {e}")
            
            # Пробуем переключиться на следующий прокси
            if hasattr(proxy_pool, 'trackers') and len(proxy_pool.trackers) > 0:
                # Проверяем есть ли доступные прокси
                available_trackers = [t for t in proxy_pool.trackers if not t.is_rate_limited]
                
                if not available_trackers:
                    print(f"❌ Все прокси исчерпаны, спим {sleep_time} секунд...")
                    await asyncio.sleep(sleep_time)
                    
                    # Сбрасываем rate limits для всех прокси
                    for tracker in proxy_pool.trackers:
                        tracker.is_rate_limited = False
                        tracker.rate_limit_time = None
                    print("🔄 Rate limits сброшены, продолжаем...")
                else:
                    print(f"🔄 Переключаемся на следующий прокси (доступно {len(available_trackers)})...")
                    await asyncio.sleep(2)  # Небольшая задержка перед повторной попыткой
            else:
                # Если нет информации о прокси, просто ждем
                await asyncio.sleep(5)
    
    raise Exception(f"Не удалось выполнить запрос после {max_retries} попыток")


async def get_maps_new(game_mods, maps_to_save, ids, mkdir='count_synergy_10th_2000',
                 show_prints=None, skip=0, count=0, only_in_ids=False):
    """
    Объединенная функция для сбора и обработки матчей.
    Включает логику фильтрации trash maps и сохранения во временные файлы.
    """
    # Загрузка trash_maps
    try:
        with open('./trash_maps.txt', 'r') as f:
            trash_maps = set(json.load(f))
        print(f"📋 Загружено {len(trash_maps)} trash maps")
    except:
        trash_maps = set()
        print("📋 Файл trash_maps.txt не найден, начинаем с пустого набора")
    
    # Файл для хранения уже обработанных ID из ids_to_graph
    processed_graph_ids_file = f"./{mkdir}/processed_ids_to_graph.txt"
    
    # Загружаем уже обработанные ID из ids_to_graph
    processed_graph_ids = set()
    if os.path.exists(processed_graph_ids_file):
        try:
            with open(processed_graph_ids_file, 'r', encoding='utf-8') as f:
                processed_graph_ids = set(json.load(f))
            print(f"📋 Загружено {len(processed_graph_ids)} уже обработанных IDs из ids_to_graph")
        except Exception as e:
            print(f"⚠️ Ошибка при загрузке processed_graph_ids: {e}")
            processed_graph_ids = set()
    else:
        print(f"📋 Файл {processed_graph_ids_file} не найден, начинаем с нуля")

    # Попытка восстановить состояние
    saved_state = load_get_maps_state(maps_to_save)

    if saved_state:
        processed_ids, output_data, player_ids, all_teams, phase = saved_state
        count = len(processed_ids)
        print(f"♻️  Восстановлено состояние: {len(processed_ids)} ID обработано, фаза {phase}")
    else:
        processed_ids = set()
        output_data = {}  # Теперь словарь для хранения полных данных матчей
        player_ids = set()
        all_teams = {}
        phase = 1

    ids_to_graph = []
    ids_set = set(ids)  # Преобразуем все ID в строки для консистентности
    maps_counter = 0

    # Создаём папку temp_files если её нет
    temp_folder = f"./{mkdir}/temp_files"
    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)
        print(f"📁 Создана папка: {temp_folder}")

    # Фаза 1: Обработка исходных ID
    if phase == 1:
        # Фильтруем ID которые уже были в ids_to_graph
        ids_set = ids_set - processed_graph_ids
        print(f"🔄 Фаза 1: Обработка {len(ids_set)} новых ID (отфильтровано {len(processed_graph_ids)} уже обработанных)...")
        remaining_ids = ids_set - processed_ids

        for check_id in remaining_ids:
            count += 1
            ids_to_graph.append(check_id)
            processed_ids.add(check_id)
            processed_graph_ids.add(str(check_id))  # Добавляем в обработанные

            if show_prints:
                print(f'{count}/{len(ids_set)}')

            if len(ids_to_graph) == 5 or count == len(ids_set):
                # Получаем полные данные матчей с retry логикой
                matches, new_player_ids = await retry_request_with_proxy_rotation(
                    proceed_get_maps_with_data,
                    skip=skip, game_mods=game_mods, only_in_ids=only_in_ids,
                    ids_to_graph=ids_to_graph, all_teams=all_teams, player_ids_check=True
                )

                # Обрабатываем каждый матч
                for match in matches:
                    map_id = str(match['id'])

                    # Проверяем качество матча
                    is_valid, reason = check_match_quality(match)

                    if is_valid:
                        output_data[map_id] = match
                        maps_counter += 1
                    else:
                        trash_maps.add(map_id)
                        if show_prints:
                            print(f"🗑️  Матч {map_id} отклонён: {reason}")

                player_ids.update(new_player_ids)
                ids_to_graph = []

            # Сохранение каждые 100 ID
            if len(processed_ids) % 100 == 0:
                # Сохраняем временные данные
                if len(output_data) > 0:
                    save_temp_file(output_data, mkdir, count)
                    print(f"💾 Сохранено {len(output_data)} матчей во временный файл {count}.txt")
                    output_data = {}  # Очищаем после сохранения

                # Сохраняем trash_maps
                with open('./trash_maps.txt', 'w') as f:
                    json.dump(list(trash_maps), f)

                # Сохраняем обработанные ID из ids_to_graph
                with open(processed_graph_ids_file, 'w', encoding='utf-8') as f:
                    json.dump(list(processed_graph_ids), f)

                # Сохраняем состояние
                save_get_maps_state(maps_to_save, processed_ids, {}, player_ids, all_teams, phase=1)

        # Финальное сохранение после фазы 1
        if len(output_data) > 0:
            save_temp_file(output_data, mkdir, count)
            print(f"💾 Финально сохранено {len(output_data)} матчей фазы 1")
            output_data = {}

        # Сохраняем обработанные ID из ids_to_graph
        with open(processed_graph_ids_file, 'w', encoding='utf-8') as f:
            json.dump(list(processed_graph_ids), f)

        save_get_maps_state(maps_to_save, processed_ids, {}, player_ids, all_teams, phase=1)

    # Сохраняем player_ids для следующей фазы
    with open(f'{mkdir}/player_ids.txt', 'w') as f:
        json.dump(list(player_ids), f)

    # Переход к фазе 2
    phase = 2
    processed_ids = set()
    count = 0

    # Фаза 2: Обработка player_ids
    if phase == 2:
        # Фильтруем player_ids которые уже были в ids_to_graph
        player_ids_set = set(map(str, player_ids))
        player_ids_filtered = player_ids_set - processed_graph_ids
        print(f"🔄 Фаза 2: Обработка {len(player_ids_filtered)} новых player ID (отфильтровано {len(player_ids_set & processed_graph_ids)} уже обработанных)...")
        remaining_player_ids = player_ids_filtered - processed_ids

        for check_id in remaining_player_ids:
            count += 1
            ids_to_graph.append(check_id)
            processed_ids.add(check_id)
            processed_graph_ids.add(str(check_id))  # Добавляем в обработанные

            if show_prints:
                print(f'{count}/{len(player_ids_filtered)}')

            if len(ids_to_graph) == 5 or count == len(player_ids_filtered):
                # Получаем полные данные матчей с retry логикой
                matches, _ = await retry_request_with_proxy_rotation(
                    proceed_get_maps_with_data,
                    skip=skip, game_mods=game_mods, only_in_ids=only_in_ids,
                    ids_to_graph=ids_to_graph, all_teams=all_teams, player_ids_check=False
                )

                # Обрабатываем каждый матч
                for match in matches:
                    map_id = str(match['id'])

                    # Пропускаем уже обработанные или trash
                    if map_id in trash_maps:
                        continue

                    # Проверяем качество матча
                    is_valid, reason = check_match_quality(match)

                    if is_valid:
                        output_data[map_id] = match
                        maps_counter += 1
                    else:
                        trash_maps.add(map_id)
                        if show_prints:
                            print(f"🗑️  Матч {map_id} отклонён: {reason}")

                ids_to_graph = []

            # Сохранение каждые 100 ID
            if len(processed_ids) % 100 == 0:
                # Сохраняем временные данные
                if len(output_data) > 0:
                    save_temp_file(output_data, mkdir, count)
                    print(f"💾 Сохранено {len(output_data)} матчей во временный файл {count}.txt")
                    output_data = {}

                # Сохраняем trash_maps
                with open('./trash_maps.txt', 'w') as f:
                    json.dump(list(trash_maps), f)

                # Сохраняем обработанные ID из ids_to_graph
                with open(processed_graph_ids_file, 'w', encoding='utf-8') as f:
                    json.dump(list(processed_graph_ids), f)

                # Сохраняем состояние
                save_get_maps_state(maps_to_save, processed_ids, {}, player_ids, all_teams, phase=1)

        # Финальное сохранение после фазы 1
        if len(output_data) > 0:
            save_temp_file(output_data, mkdir, count)
            print(f"💾 Финально сохранено {len(output_data)} матчей фазы 1")
            output_data = {}

        # Сохраняем обработанные ID из ids_to_graph
        with open(processed_graph_ids_file, 'w', encoding='utf-8') as f:
            json.dump(list(processed_graph_ids), f)

        save_get_maps_state(maps_to_save, processed_ids, {}, player_ids, all_teams, phase=1)

    # Сохранение метаданных
    with open('all_teams.txt', 'w') as f:
        json.dump(all_teams, f)

    with open('./trash_maps.txt', 'w') as f:
        json.dump(list(trash_maps), f)

    # Финальное сохранение обработанных ID из ids_to_graph
    with open(processed_graph_ids_file, 'w', encoding='utf-8') as f:
        json.dump(list(processed_graph_ids), f)

    # Удаление файла состояния после успешного завершения
    clear_get_maps_state(maps_to_save)

    print(f"\n✅ Обработка завершена!")
    print(f"🎮 Собрано валидных матчей: {maps_counter}")
    print(f"🗑️  Отклонено trash maps: {len(trash_maps)}")
    print(f"📋 Всего обработано уникальных IDs: {len(processed_graph_ids)}")
    print(f"📄 IDs сохранены в: {processed_graph_ids_file}")

    # Объединение temp_files в файлы по 500 МБ
    print(f"\n📦 Объединяем временные файлы...")
    merged_files = merge_temp_files_by_size(
        mkdir=mkdir,
        max_size_mb=500,
        cleanup=True  # Удаляем temp_files после объединения
    )

    if merged_files:
        print(f"✅ Временные файлы объединены: {len(merged_files)} файлов")


def _process_single_json_file(file_path, maps, output):
    """Обрабатывает один JSON файл и возвращает результат"""
    result_set = set() if maps else None
    result_dict = {} if output else None
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            loaded_json_content = json.load(f)

            if isinstance(loaded_json_content, list):
                for item_in_list in loaded_json_content:
                    if isinstance(item_in_list, dict) and 'id' in item_in_list:
                        if maps:
                            result_set.add(str(item_in_list['id']))
                        if output:
                            result_dict[str(item_in_list['id'])] = item_in_list
            elif isinstance(loaded_json_content, dict):
                for map_id in loaded_json_content:
                    if maps:
                        raise EnvironmentError
                    if output:
                        result_dict[str(map_id)] = loaded_json_content[map_id]
    except json.JSONDecodeError:
        print(f"Ошибка декодирования JSON в файле: {file_path}. Файл пропущен.")
    except Exception as e:
        print(f"Непредвиденная ошибка при обработке файла {file_path}: {e}. Файл пропущен.")
    
    return result_set, result_dict


def collect_all_maps(folder_path, maps=None, output=None):
    if maps: ids_to_exclude_from_json = set()
    if output: ids_to_exclude_from_json = {}
    
    if os.path.exists(folder_path):
        json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
        print(f"Найдено {len(json_files)} JSON файлов для обработки.")

        if not json_files:
            print("В указанной папке нет JSON файлов.")
            return ids_to_exclude_from_json

        file_paths = [os.path.join(folder_path, f) for f in json_files]
        
        # Используем ThreadPoolExecutor для параллельной обработки файлов
        with ThreadPoolExecutor(max_workers=min(8, len(json_files))) as executor:
            process_func = partial(_process_single_json_file, maps=maps, output=output)
            results = list(executor.map(process_func, file_paths))
        
        # Объединяем результаты
        for i, (result_set, result_dict) in enumerate(results):
            if maps and result_set:
                ids_to_exclude_from_json.update(result_set)
            if output and result_dict:
                ids_to_exclude_from_json.update(result_dict)
            
            if (i + 1) % 100 == 0 or (i + 1) == len(json_files):
                print(f"Обработано {i + 1}/{len(json_files)} файлов. Собрано {len(ids_to_exclude_from_json)} уникальных ID для исключения из JSON.")
    else:
        print(f"Папка {folder_path} не найдена.")

    print(f"Всего собрано {len(ids_to_exclude_from_json)} уникальных ID для исключения из JSON файлов.")
    return ids_to_exclude_from_json



async def proceed_get_maps_with_data(skip=0, only_in_ids=False, ids_to_graph=None,
                                    game_mods=None, all_teams=None, player_ids_check=False):
    """
    Получает полные данные матчей вместо только ID.
    Возвращает: (matches, player_ids) - список матчей и множество ID игроков
    """
    matches = []
    player_ids = set()
    check = True

    query = f'''
    {{
      players(steamAccountIds: {ids_to_graph}) {{
        steamAccount{{
            smurfFlag
            isAnonymous
            id
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
          direKills
          radiantNetworthLeads
          players {{
            position
            isRadiant
            deaths
            intentionalFeeding
            hero {{
              id
            }}
            steamAccount {{
              id
              smurfFlag
              isAnonymous
            }}
          }}
        }}
      }}
      teams(teamIds: {ids_to_graph}) {{
        matches(request: {{startDateTime: {start_date_time}, take: 100, skip: {skip}, isStats:true}}) {{
          id
          direKills
          radiantNetworthLeads
          radiantTeam {{
            name
            id
          }}
          direTeam {{
            name
            id
          }}
          players {{
            position
            isRadiant
            deaths
            intentionalFeeding
            hero {{
              id
            }}
            steamAccount {{
              id
              smurfFlag
                    isAnonymous
                  }}
                  }}
              }}
      }}
    }}'''

    encoded_query = quote(query)
    referer = f"https://api.stratz.com/graphiql?query={encoded_query}"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Origin": "https://api.stratz.com",
        "Referer": f'{referer}',
        "User-Agent": "STRATZ_API"
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
                    if player['steamAccount']['smurfFlag'] not in [0, 2] or player['steamAccount']['isAnonymous']:
                        continue
                    for match in player['matches']:
                        matches.append(match)
                        if player_ids_check:
                            for extra_player in match['players']:
                                if (not extra_player.get('intentionalFeeding') and
                                    extra_player.get('steamAccount', {}).get('smurfFlag') in [0, 2] and
                                    not extra_player.get('steamAccount', {}).get('isAnonymous')):
                                    player_ids.add(extra_player['steamAccount']['id'])
            else:
                check = False
        else:
            check = False
            for team in data['data']['teams']:
                for match in team['matches']:
                    if match['radiantTeam']['name'] not in all_teams:
                        all_teams[match['radiantTeam']['name']] = match['radiantTeam']['id']
                    if match['direTeam']['name'] not in all_teams:
                        all_teams[match['direTeam']['name']] = match['direTeam']['id']
                    if only_in_ids:
                        matches.append(match)

    except Exception as e:
        print(f"Unexpected error in proceed_get_maps_with_data: {e}")
        check = False

    return matches, player_ids


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


def merge_temp_files_by_size(mkdir, max_size_mb=500, output_dir=None, cleanup=False):
    """
    Объединяет temp_files в файлы с ограничением по размеру, фильтруя дубликаты

    Args:
        mkdir: папка с temp_files (например 'count_synergy_10th_2000')
        max_size_mb: максимальный размер выходного файла в МБ (по умолчанию 500)
        output_dir: директория для сохранения (по умолчанию '{mkdir}/json_parts_split_from_object')
        cleanup: удалить temp_files после объединения (по умолчанию False)

    Returns:
        list: список путей к созданным файлам
    """
    temp_folder = f"./{mkdir}/temp_files"

    if not os.path.exists(temp_folder):
        print(f"❌ Папка {temp_folder} не существует")
        return []

    # Создаём выходную директорию
    if output_dir is None:
        output_dir = f"./{mkdir}/json_parts_split_from_object"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"📁 Создана папка: {output_dir}")

    # Файл с ID уже обработанных матчей
    processed_ids_file = f"{output_dir}/processed_ids.txt"

    # Загружаем уже обработанные ID из файла
    processed_ids = set()
    if os.path.exists(processed_ids_file):
        try:
            with open(processed_ids_file, 'r', encoding='utf-8') as f:
                processed_ids = set(json.load(f))
            print(f"📋 Загружено {len(processed_ids)} уже обработанных ID из {processed_ids_file}")
        except Exception as e:
            print(f"⚠️ Ошибка при загрузке processed_ids: {e}")
            processed_ids = set()
    else:
        print(f"📋 Файл {processed_ids_file} не найден, начинаем с нуля")

    # Находим максимальный номер файла combined{N}.json
    existing_files = [f for f in os.listdir(output_dir) if f.startswith('combined') and f.endswith('.json')]
    max_number = 0
    for f in existing_files:
        try:
            # Извлекаем число из имени файла combined{N}.json
            num = int(f.replace('combined', '').replace('.json', ''))
            max_number = max(max_number, num)
        except:
            continue

    part_number = max_number + 1
    print(f"🔢 Начинаем нумерацию с combined{part_number}.json")

    # Получаем все temp файлы
    temp_files = sorted([f for f in os.listdir(temp_folder) if f.endswith('.txt')])

    if not temp_files:
        print(f"⚠️ В папке {temp_folder} нет .txt файлов")
        return []

    print(f"📊 Найдено {len(temp_files)} временных файлов")
    print(f"🎯 Объединяем в файлы по {max_size_mb} МБ с фильтрацией дубликатов...")

    max_size_bytes = max_size_mb * 1024 * 1024
    current_data = {}
    current_size = 0
    output_files = []
    total_matches = 0
    duplicates_count = 0
    new_ids_in_session = set()  # ID добавленные в текущей сессии

    for i, filename in enumerate(temp_files, 1):
        file_path = os.path.join(temp_folder, filename)

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if not isinstance(data, dict):
                continue

            # Фильтруем дубликаты - добавляем только новые match_id
            new_entries = {}
            for match_id, match_data in data.items():
                # Проверяем и против старых processed_ids, и против новых в текущей сессии
                if match_id not in processed_ids and match_id not in new_ids_in_session:
                    new_entries[match_id] = match_data
                    new_ids_in_session.add(match_id)
                else:
                    duplicates_count += 1

            if not new_entries:
                continue  # Все записи из этого файла - дубликаты

            # Определяем размер новых данных в JSON
            new_data_json = json.dumps(new_entries, ensure_ascii=False)
            new_data_size = len(new_data_json.encode('utf-8'))

            # Если добавление этих данных превысит лимит и у нас уже есть данные
            if current_size + new_data_size > max_size_bytes and current_data:
                # Сохраняем текущий файл
                output_filename = f"{output_dir}/combined{part_number}.json"
                with open(output_filename, 'w', encoding='utf-8') as f:
                    json.dump(current_data, f, ensure_ascii=False)

                file_size_mb = current_size / (1024 * 1024)
                print(f"  ✅ {output_filename}: {len(current_data)} матчей ({file_size_mb:.1f} МБ)")

                output_files.append(output_filename)
                total_matches += len(current_data)

                # Обновляем файл processed_ids
                processed_ids.update(new_ids_in_session)
                with open(processed_ids_file, 'w', encoding='utf-8') as f:
                    json.dump(list(processed_ids), f, ensure_ascii=False)

                # Начинаем новый файл
                current_data = {}
                current_size = 0
                part_number += 1

            # Добавляем только новые данные
            current_data.update(new_entries)
            current_size += new_data_size

            # Прогресс
            if i % 50 == 0 or i == len(temp_files):
                print(f"  📈 Обработано {i}/{len(temp_files)} файлов | Новых: {len(new_ids_in_session)} | Дубликатов: {duplicates_count}")

        except Exception as e:
            print(f"  ⚠️ Ошибка при чтении {file_path}: {e}")
            continue

    # Сохраняем остаток
    if current_data:
        output_filename = f"{output_dir}/combined{part_number}.json"
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(current_data, f, ensure_ascii=False)

        file_size_mb = current_size / (1024 * 1024)
        print(f"  ✅ {output_filename}: {len(current_data)} матчей ({file_size_mb:.1f} МБ)")

        output_files.append(output_filename)
        total_matches += len(current_data)

    # Финальное обновление файла processed_ids
    processed_ids.update(new_ids_in_session)
    with open(processed_ids_file, 'w', encoding='utf-8') as f:
        json.dump(list(processed_ids), f, ensure_ascii=False)

    print(f"\n🎉 Объединение завершено!")
    print(f"📦 Создано файлов: {len(output_files)}")
    print(f"🎮 Новых матчей добавлено: {len(new_ids_in_session)}")
    print(f"🔄 Дубликатов отфильтровано: {duplicates_count}")
    print(f"💾 Всего уникальных ID в базе: {len(processed_ids)}")
    print(f"📄 ID сохранены в: {processed_ids_file}")

    # Очистка temp_files если запрошено
    if cleanup:
        try:
            shutil.rmtree(temp_folder)
            print(f"🗑️  Папка {temp_folder} удалена")
        except Exception as e:
            print(f"⚠️ Не удалось удалить {temp_folder}: {e}")

    return output_files


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

            if len(new_data) == data_len:
                print(map_id)

    save_temp_file(new_data, mkdir, another_counter)
    # eat_temp_files(mkdir, file_data, file_name)


def check_match_quality(match):
    """Проверяет качество данных карты"""
    if match.get('direKills') is None:
        return False, 'kills None'
    if not all(None not in [player.get('position'), player.get('hero', {}).get('id'), player.get('steamAccount')] 
               for player in match.get('players', [])):
        return False, 'None'
    if not all(player.get('intentionalFeeding') is False for player in match.get('players', [])):
        return False, 'feed'
    if not all(player.get('steamAccount', {}).get('smurfFlag') in [0, 2] for player in match.get('players', [])):
        return False, 'smurf'
    if len(match.get('radiantNetworthLeads', [])) < 20:
        return False, 'too short'
    
    # Проверка на аномальную разницу в deaths между игроками команды
    # Проверяем только если максимальное deaths >= 10
    players = match.get('players', [])
    radiant_deaths = [p.get('deaths', 0) for p in players if p.get('isRadiant')]
    dire_deaths = [p.get('deaths', 0) for p in players if not p.get('isRadiant')]
    
    for team_deaths, team_name in [(radiant_deaths, 'radiant'), (dire_deaths, 'dire')]:
        if len(team_deaths) >= 2:
            # Сортируем по возрастанию и берем 2 минимальных
            sorted_deaths = sorted(team_deaths)
            min_first = sorted_deaths[3]
            min_second = sorted_deaths[4]
            
            # Находим максимум для проверки порога
            max_deaths = max(team_deaths)
            
            # Проверяем только если максимум >= 10
            if max_deaths >= 10:
                # Если у игрока со вторым минимальным deaths в 2+ раза больше чем у первого минимального
                if min_first > 0 and min_second >= min_first * 2:
                    return False, f'death anomaly {team_name} ({min_first}/{min_second})'
    
    return True, 'ok'


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
    # Оптимизация: быстрые проверки сначала (fail-fast)
    if match.get('direKills') is None:
        return False
    
    radiant_leads = match.get('radiantNetworthLeads', [])
    if len(radiant_leads) < 20:
        return False
    
    # Проверка игроков - выходим при первом несовпадении
    players = match.get('players', [])
    for player in players:
        if (player.get('intentionalFeeding') is not False or 
            player.get('steamAccount', {}).get('smurfFlag') not in [0, 2] or
            player.get('position') is None or 
            player.get('hero', {}).get('id') is None or 
            player.get('steamAccount') is None):
            return False
    
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
    win_loose = {}
    
    # Оптимизация: используем items() вместо enumerate и прямого доступа к database[map_id]
    total_count = len(database)
    
    # Предварительный расчет для pro режима
    if pro:
        required_fields = ['direTeam', 'radiantTeam']
    
    for count, (map_id, match) in enumerate(database.items(), 1):
        check = True
        
        # Вывод прогресса с пропуском некоторых итераций для ускорения
        if count % 100 == 0 or count == total_count:
            print(f'{count}/{total_count}')
        
        if pro:
            # Оптимизация: проверка наличия полей за один проход
            if all(match.get(name) is not None for name in required_fields):
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
    # ids = set()
    # database = collect_all_maps(folder_path='count_synergy_10th_2000/json_parts_split_from_object', output=True)
    # for map_id in database:
    #     for player in database[map_id]['players']:
    #         ids.add(player['steamAccount']['id'])
    # asyncio.run(get_maps_new(maps_to_save='./count_synergy_10th_2000/1722505765_top600_maps', game_mods=[2, 22],
    #              show_prints=show_prints, ids=ids))

    explore_database(mkdir='count_synergy_10th_2000', file_name='1722505765_top600_output',
                     over40=True, counterpick1vs2=True, synergy=True, counterpick1vs1=True, lane=True,
                     counterpick1vs3=True, synergy4=True)
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