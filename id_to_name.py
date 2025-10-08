"""
Модуль для получения игроков из leaderboard
Использует асинхронность и пул прокси-API из keys.py
"""

import asyncio
import aiohttp
from aiohttp_socks import ProxyConnector
from keys import api_to_proxy
from collections import deque
import time


# Копируем классы из maps_research.py для независимой работы
RATE_LIMITS = {
    'second': 7,
    'minute': 138,
    'hour': 1488,
    'day': 14988
}

CONCURRENCY_LIMIT = 20


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
            
            time_windows = {
                'second': 1,
                'minute': 60,
                'hour': 3600,
                'day': 86400
            }
            
            for period, window in time_windows.items():
                while self.requests_log[period] and now - self.requests_log[period][0] > window:
                    self.requests_log[period].popleft()
                
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
            
            print(f"⏳ Все API достигли лимитов, ожидание 0.5 сек...")
            await asyncio.sleep(0.5)
            attempt += 1
        
        print("⚠️ Превышено время ожидания, использую первый доступный tracker")
        return self.trackers[0]
    
    async def make_request(self, url, **kwargs):
        """Выполняет запрос с автоматическим выбором tracker и rate limiting"""
        max_retries = len(self.trackers) + 1  # Попробуем все пары + 1 попытка после сна
        retry_count = 0
        
        while retry_count < max_retries:
            async with self.semaphore:
                tracker = await self.get_available_tracker()
                connector = ProxyConnector.from_url(tracker.proxy_url)
                
                if 'headers' in kwargs:
                    kwargs['headers']['Authorization'] = f"Bearer {tracker.api_token}"
                
                try:
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


# Создаем глобальный пул
_proxy_pool = ProxyAPIPool(api_to_proxy)


async def get_players_async(top, region, players_dict, skip=0):
    """
    Асинхронная версия get_players
    
    Args:
        top: количество игроков для получения
        region: регион leaderboard
        players_dict: set для хранения ID игроков
        skip: начальная позиция
    
    Returns:
        players_dict с добавленными игроками
    """
    while skip < top:
        query = '''{
           leaderboard{
             season(request:{leaderBoardDivision:%s}){
               players(take:100, skip:%s){
                 steamAccount{
                   smurfFlag
                   isAnonymous
                   id
                   name
               }
               }}
           }
         }''' % (region, skip)
        
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
            data = await _proxy_pool.make_request(
                url='https://api.stratz.com/graphql',
                json={"query": query},
                headers=headers
            )
            
            if 'data' in data and data['data']:
                for player in data['data']['leaderboard']['season']['players']:
                    if (player['steamAccount']['isAnonymous'] is False and 
                        player['steamAccount']['smurfFlag'] == 0):
                        players_dict.add(player['steamAccount']['id'])
                skip += 100
                print(f"Получено игроков: {len(players_dict)}, skip: {skip}/{top}")
            else:
                print(f"Нет данных в ответе, пропускаем skip={skip}")
                skip += 100
                
        except Exception as e:
            print(f"Ошибка при получении игроков (skip={skip}): {e}")
            skip += 100
            
    return players_dict


def get_players(top, region, players_dict, skip=0):
    """
    Синхронная обертка для get_players_async
    Сохранена для обратной совместимости
    """
    return asyncio.run(get_players_async(top, region, players_dict, skip))