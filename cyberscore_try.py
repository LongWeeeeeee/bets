import json
import time
from bs4 import BeautifulSoup
import requests
from functions import send_message, get_map_players, proceed_map, check_old_maps, one_match, sleep_until_morning, is_moscow_night
import urllib3
from maps_research import update_my_protracker

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


from urllib.parse import urlparse  # Добавьте импорт
name_to_pos = {
        'Core': 'pos1',
        'Support': 'pos4',
        'Full Support': 'pos5',
        'Mid': 'pos2',
        'Offlane': 'pos3'
    }
headers = {
        "Host": "dltv.org",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Referer": 'https://dltv.org/results',
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/130.0.0.0 Safari/537.36", }

def add_url(url):
    with open('map_id_check.txt', 'r+') as f:
        data = json.load(f)
        data.append(url)
        f.truncate()
        f.seek(0)
        json.dump(data, f)


def get_heads(response=None, MAX_RETRIES=5, RETRY_DELAY=5, ip_address="46.229.214.49", path = "/matches"):
    try:
        url = f"https://{ip_address}{path}"
        proxies = {
            'https': 'http://SitSyNrlyk:yz3ozbojdu@77.221.150.201:49566',
        }
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    proxies=proxies,
                    verify=False,  # ВНИМАНИЕ: отключена проверка SSL
                    timeout=10
                )
                if response.status_code == 200:
                    break
            except requests.exceptions.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
        if not response or response.status_code != 200:
            return
        soup = BeautifulSoup(response.text, 'lxml')
        live_matches = soup.find('div', class_='live__matches')
        heads = live_matches.find_all('div', class_='live__matches-item__head')
        bodies = live_matches.find_all('div', class_='live__matches-item__body')
        return heads, bodies
    except Exception as e:
        print(f'ошибка при выяснении heads {e}')


def check_head(heads, bodies, i, maps_data):
        # Константы вынесены в начало
        IP_ADDRESS = "46.229.214.49"
        PROXIES = {'https': 'http://SitSyNrlyk:yz3ozbojdu@77.221.150.201:49566'}
        MAX_RETRIES = 5
        RETRY_DELAY = 5

        # Проверка статуса матча
        status_element = heads[i].find('div', class_='event__info-info__time')
        status = status_element.text.lower() if status_element else 'unknown'

        if status != 'draft...' and status == 'finished':
            return 'finished'
        elif status == 'draft...':
            status = 'draft'

        # Извлечение данных
        try:
            score_divs = bodies[i].find_all('div', class_='match__item-team__score')
            uniq_score = sum(int(div.text.strip()) for div in score_divs[:2])

            link_tag = bodies[i].find('a')
            href = link_tag['href']
            parsed_url = urlparse(href)
            path = parsed_url.path
            check_uniq_url = f'dltv.org{path}.{uniq_score}'
            if check_uniq_url in maps_data:
                return status


        except (AttributeError, KeyError, ValueError) as e:
            print(f"Error parsing data: {e}")
            return status

        # HTTP запросы
        url = f"https://{IP_ADDRESS}{path}"
        response = None

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    proxies=PROXIES,
                    verify=False,  # ВНИМАНИЕ: отключена проверка SSL
                    timeout=10
                )
                if response.status_code == 200:
                    break
            except requests.exceptions.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)

        if not response or response.status_code != 200:
            print(f"Failed to retrieve content. Status code: {response.status_code if response else 'No response'}")
            return status

        # Парсинг ответа
        soup = BeautifulSoup(response.text, 'lxml')

        # Извлечение счета
        try:
            score_container = soup.find('div', class_='score__scores live')
            spans = score_container.find_all('span')
            score = [span.text.strip() for span in spans[:2]]
        except AttributeError:
            score = ['?', '?']
        score_summary = 0

        # Извлечение данных команд
        team_data = {'teams': {}}
        teams = soup.find_all('span', class_='team__title-name')
        if len(teams) < 2:
            return status
        name = teams[score_summary].find('span', class_='name').text
        side = teams[score_summary].find('span', class_='side').text.lower()
        team_data['teams'][side] = name
        score_summary += 1
        name2 = teams[score_summary].find('span', class_='name').text
        side2 = teams[score_summary].find('span', class_='side').text.lower()
        team_data['teams'][side2] = name2

        # Обработка карт
        for map_container in soup.find_all('div', class_='picks__new-picks'):
            result = get_map_players(
                    team_data,
                    map_container,
                    soup,
                    name_to_pos
                )
            if result is None:
                print(f"Error processing map: {url}")
                return status
            radiant_team_name, dire_team_name, radiant_heroes, dire_heroes = result

            # Формирование результата
            output_dict = proceed_map(
                url=f'dltv.org{path}',
                radiant_team_name=radiant_team_name,
                dire_team_name=dire_team_name,
                radiant_heroes_and_pos=radiant_heroes,
                dire_heroes_and_pos=dire_heroes,
                data1vs1=data1vs1,
                data1vs2=data1vs2,
                lane_data=lane_data,
                over40_data=over40_data,
                synergy_data=synergy_data
            )

            # Формирование сообщения
            send_message(
                f'ПОМНИ: ЛЮБОЙ ПИК МОЖЕТ ПРОИГРАТЬ\n'
                f"{radiant_team_name} VS {dire_team_name}\n"
                f"Счет: {score}\n"
                f"Kills: Median: {output_dict.get('kills_mediana', 'N/A')} "
                f"| Avg: {output_dict.get('kills_average', 'N/A')}\n"
                # f"over40_solo: {output_dict.get('over40_solo', None)}\n"
                f"over40_duo_counterpick: {output_dict.get('over40_duo_counterpick', 'N/A')}\n"
                f"over40_trio: {output_dict.get('over40_trio', 'N/A')}\n"
                f"over40_1vs2: {output_dict.get('over40_1vs2',None)}\n"
                f"over40_duo: {output_dict.get('over40_duo', None)}\n"
                # f"Lanes:\n{output_dict.get('top_message', '')}"
                # f"{output_dict.get('mid_message', '')}"
                # f"{output_dict.get('bot_message', '')}"
                f"Synergy_and_counterpick:\n"
                # f"support_dif: {output_dict.get('support_dif', None)}\n"
                # f"pos1_matchup: {output_dict.get('pos1_matchup', 'N/A')}\n"
                # f"Synergy_duo: {output_dict.get('synergy_duo', 'N/A')}\n"
                f"Synergy_trio: {output_dict.get('radiant_synergy_trio', 'N/A')}\n"
                # f"Counterpick_duo: {output_dict.get('duo_diff', 'N/A')}\n"
                f"1vs2_counterpick: {output_dict.get('radiant_counterpick_1vs2', 'N/A')}\n"
                f'ПОМНИ: ЛЮБОЙ ПИК МОЖЕТ ПРОИГРАТЬ'
            )

            add_url(check_uniq_url)
        return status






def general(status=None):
    with open('./map_id_check.txt', 'r+') as f:
        maps_data = json.load(f)
    answer = get_heads()
    if not answer:
        print('не удалось выяснить heads')
        return
    heads, bodies = answer
    for i in range(len(heads)):
        status = check_head(heads, bodies, i, maps_data)
    return status


if __name__ == "__main__":
    # update_my_protracker(show_prints=True)
    with open('1722505765_top600_heroes_data/synergy.txt', 'r') as f:
        synergy_data = json.load(f)
    with open('1722505765_top600_heroes_data/counterpick1vs1.txt', 'r') as f2:
        data1vs1 = json.load(f2)
    with open('1722505765_top600_heroes_data/counterpick1vs2.txt', 'r') as f3:
        data1vs2 = json.load(f3)
    with open('1722505765_top600_heroes_data/over40_dict.txt', 'r') as f:
        over40_data = json.load(f)
    with open('1722505765_top600_heroes_data/lane_dict.txt', 'r') as f:
        lane_data = json.load(f)
    # over40_data, lane_data, data1vs2, data1vs1, synergy_data = {}, {}, {}, {}, {}
    # lane_data = {}
    #
    # one_match(radiant_heroes_and_pos={'pos1': {'hero_name': "gyrocopter"}, 'pos2': {'hero_name': "puck"},
    #                                   'pos3': {'hero_name': 'dawnbreaker'}, 'pos4': {'hero_name': 'shadow shaman'},
    #                                   'pos5': {'hero_name': 'pudge'}},
    #           dire_heroes_and_pos={'pos1': {'hero_name': "phantom assassin"}, 'pos2': {'hero_name': "lina"},
    #                                'pos3': {'hero_name': 'timbersaw'}, 'pos4': {'hero_name': 'lion'},
    #                                'pos5': {'hero_name': "elder titan"}},
    #           lane_data=lane_data, data1vs2=data1vs2, data1vs1=data1vs1, over40_data=over40_data,
    #           synergy_data=synergy_data,
    #           radiant_team_name='radiant Team', dire_team_name='Team dire')
    while True:
        if is_moscow_night():
            sleep_until_morning()
        status = general()
        if status == 'draft':
            print('Сплю 20 секунд')
            time.sleep(20)
        elif status == 'finished':
            print('Сплю 5 минут')
            time.sleep(300)
        else:
            print('Сплю 10 минут')
            time.sleep(600)
    # check_old_maps(data1vs1=data1vs1, data1vs2=data1vs2,
    #                 lane_data=lane_data, over40_data=over40_data, synergy_data=synergy_data)
