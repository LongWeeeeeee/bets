import json
import time
from bs4 import BeautifulSoup
import requests
from maps_research import update_my_protracker
from functions import (send_message, get_map_players, proceed_map, format_output_dict,
                       check_old_maps, one_match, is_moscow_night)

trash_list=['team', 'flipster', 'esports', ' ']

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
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    verify=False,  # Р’РќРРњРђРќРР•: РѕС‚РєР»СЋС‡РµРЅР° РїСЂРѕРІРµСЂРєР° SSL
                    timeout=10
                )
                if response.status_code == 200:
                    break
            except requests.exceptions.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
        if not response or response.status_code != 200:
            print(response.status_code)
            return
        soup = BeautifulSoup(response.text, 'lxml')
        live_matches = soup.find('div', class_='live__matches')
        heads = live_matches.find_all('div', class_='live__matches-item__head')
        bodies = live_matches.find_all('div', class_='live__matches-item__body')
        heads_copy, bodies_copy = heads.copy(), bodies.copy()
        for i in range(len(heads)):
            title = heads[i].find('div', class_='event__name').find('div').text
            if not any(i in title.lower() for i in ['dreamleague', 'blast', 'dacha', 'betboom',
                                                    'fissure', 'pgl', 'esports', 'international',
                                                    'european', 'epl', 'esl', 'cct']):
                heads_copy.remove(heads[i])
                bodies_copy.remove(bodies[i])
        return heads_copy, bodies_copy
    except Exception as e:
        print(f'РѕС€РёР±РєР° РїСЂРё РІС‹СЏСЃРЅРµРЅРёРё heads {e}')


def check_head(heads, bodies, i, maps_data, return_status=None, leftover=None):
        # РљРѕРЅСЃС‚Р°РЅС‚С‹ РІС‹РЅРµСЃРµРЅС‹ РІ РЅР°С‡Р°Р»Рѕ
        IP_ADDRESS = "46.229.214.49"
        MAX_RETRIES = 5
        RETRY_DELAY = 5

        # РџСЂРѕРІРµСЂРєР° СЃС‚Р°С‚СѓСЃР° РјР°С‚С‡Р°
        status_element = heads[i].find('div', class_='event__info-info__time')
        status = status_element.text.lower() if status_element else 'unknown'

        if return_status != 'draft...':
            return_status = status
        if status == 'finished':
            return



        # РР·РІР»РµС‡РµРЅРёРµ РґР°РЅРЅС‹С…
        try:
            score_divs = bodies[i].find_all('div', class_='match__item-team__score')
            uniq_score = sum(int(div.text.strip()) for div in score_divs[:2])
            score = f"{score_divs[:2][0].text.strip()} : {score_divs[:2][1].text.strip()}"
            link_tag = bodies[i].find('a')
            href = link_tag['href']
            parsed_url = urlparse(href)
            path = parsed_url.path
            check_uniq_url = f'dltv.org{path}.{uniq_score}'
            if check_uniq_url in maps_data:
                return


        except (AttributeError, KeyError, ValueError) as e:
            print(f"Error parsing data: {e}")
            return return_status

        # HTTP Р·Р°РїСЂРѕСЃС‹
        url = f"https://{IP_ADDRESS}{path}"
        response = None

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    verify=False,  # Р’РќРРњРђРќРР•: РѕС‚РєР»СЋС‡РµРЅР° РїСЂРѕРІРµСЂРєР° SSL
                    timeout=10
                )
                if response.status_code == 200:
                    break
            except requests.exceptions.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)

        if not response or response.status_code != 200:
            print(f"Failed to retrieve content. Status code: {response.status_code if response else 'No response'}")
            return return_status

        soup = BeautifulSoup(response.text, 'lxml')

        from urllib.parse import urljoin
        import re
        m = re.search(r"\$\.get\(['\"](?P<path>/live/[^'\"]+\.json)['\"]", response.text)
        if not m:
            return return_status
        json_path = m.group('path')
        base = "https://dltv.org"  # Р·Р°РјРµРЅРёС€СЊ РЅР° СЂРµР°Р»СЊРЅС‹Р№ СЃР°Р№С‚, РѕС‚РєСѓРґР° СЃС‚СЂР°РЅРёС†Р°
        json_url = urljoin(base, json_path)

        # РєР°С‡Р°РµРј JSON
        radiant_heroes_and_pos,dire_heroes_and_pos = {}, {}
        resp = requests.get(json_url)

        try:
            data = resp.json()
        except:
            return return_status
        if 'fast_picks' not in data:
            return return_status
        if data['db']['first_team']['is_radiant']:
            radiant_team_name = data['db']['first_team']['title'].lower()
            dire_team_name = data['db']['second_team']['title'].lower()
        else:
            dire_team_name = data['db']['first_team']['title'].lower()
            radiant_team_name = data['db']['second_team']['title'].lower()

        from rapidfuzz import fuzz

        def is_same_team(name1, name2, threshold=70):
            return fuzz.ratio(name1, name2) >= threshold
        ROLE_TO_POS = {
            "Core": "pos1",
            "Mid": "pos2",
            "Offlane": "pos3",
            "Support": "pos4",
            "Full Support": "pos5",
        }
        teams = soup.find_all('div', class_='lineups__team')
        radiant_names_pos, dire_names_pos = {}, {}
        for team in teams:
            team_name = team.find('span', class_='title').text.strip().lower()
            for i in trash_list:
                team_name = team_name.replace(i, '')
                dire_team_name = dire_team_name.replace(i, '')
                radiant_team_name = radiant_team_name.replace(i, '')
            if is_same_team(team_name,radiant_team_name):
                names = team.find_all('div', class_='player__name-name')
                poses = team.find_all('div', class_='player__role-item')
                for name, pos in zip(names, poses):
                    pos = ROLE_TO_POS[pos.text.strip()]
                    if pos in radiant_names_pos.values():
                        add_url(check_uniq_url)
                        return
                    radiant_names_pos.setdefault(name.text.strip().lower(), pos)
            elif is_same_team(team_name,dire_team_name):
                names = team.find_all('div', class_='player__name-name')
                poses = team.find_all('div', class_='player__role-item')
                for name, pos in zip(names, poses):
                    pos = ROLE_TO_POS[pos.text.strip()]
                    if pos in dire_names_pos.values():
                        add_url(check_uniq_url)
                        return
                    dire_names_pos.setdefault(name.text.strip().lower(), pos)
        dire_pos_list, radiant_pos_list = ['pos1', 'pos2', 'pos3', 'pos4', 'pos5'], ['pos1', 'pos2', 'pos3', 'pos4', 'pos5']
        if not all(len(i) == 5 for i in [radiant_names_pos, dire_names_pos]):
            return return_status
        for player in data['fast_picks']['first_team']:
            name = player['player']['title'].lower()
            if name in radiant_names_pos:
                radiant_heroes_and_pos.setdefault(radiant_names_pos[name], {}).setdefault('hero_id', player['hero_id'])
                radiant_pos_list.remove(radiant_names_pos[name])
            elif name in dire_names_pos:
                dire_heroes_and_pos.setdefault(dire_names_pos[name], {}).setdefault('hero_id', player['hero_id'])
                dire_pos_list.remove(dire_names_pos[name])
        for player in data['fast_picks']['second_team']:
            name = player['player']['title'].lower()
            if name in dire_names_pos:
                dire_heroes_and_pos.setdefault(dire_names_pos[name], {}).setdefault('hero_id', player['hero_id'])
                dire_pos_list.remove(dire_names_pos[name])
            elif name in radiant_names_pos:
                radiant_heroes_and_pos.setdefault(radiant_names_pos[name], {}).setdefault('hero_id', player['hero_id'])
                radiant_pos_list.remove(radiant_names_pos[name])
            else:
                leftover = player['hero_id']
        if (len(radiant_heroes_and_pos) + len(dire_heroes_and_pos)) < 9:
            return return_status
        if (len(radiant_heroes_and_pos) + len(dire_heroes_and_pos)) != 10:
            if len(dire_pos_list) == 1:
                dire_heroes_and_pos.setdefault(dire_pos_list[0], {}).setdefault('hero_id', leftover)
            elif len(radiant_pos_list) == 1:
                radiant_heroes_and_pos.setdefault(radiant_pos_list[0], {}).setdefault('hero_id', leftover)



        if all(radiant_heroes_and_pos[i]['hero_id'] == 0 for i in radiant_heroes_and_pos):
            return return_status
        output_dict = proceed_map(
            url=f'dltv.org{path}',
            radiant_team_name=radiant_team_name,
            dire_team_name=dire_team_name,
            radiant_heroes_and_pos=radiant_heroes_and_pos,
            dire_heroes_and_pos=dire_heroes_and_pos,
            data_1vs1=data_1vs1,
            data_1vs2=data_1vs2,
            lane_data=lane_data,
            over40_data=over40_data,
            synergy_data=synergy_data,
            data_1vs3=data_1vs3,
            synergy4=synergy4
        )

        output_dict = format_output_dict(output_dict)
        send_message(
            f'ПОМНИ: КОМАНДА ВАЖНЕЕ ПИКА\n'
            f"               Счет: [{score}]\n"
            f"{radiant_team_name} VS {dire_team_name}\n"
            # f"Kills: Median: {output_dict.get('kills_mediana', 'N/A')} "
            # f"| Avg: {output_dict.get('kills_average', 'N/A')}\n"
            f"over40_solo: {output_dict.get('over40_solo', None)}\n"
            f"over40_duo_counterpick: {output_dict['over40_duo_counterpick']}\n"
            f"over40_trio: {output_dict['over40_trio']}\n"
            f"over40_1vs2: {output_dict['over40_1vs2']}\n"
            f"over40_duo: {output_dict['over40_duo']}\n"
            f"over40_pos1_matchup: {output_dict['over40_pos1_matchup']}\n\n"
            f"Lanes:\n{output_dict.get('top_message', '')}"
            f"{output_dict.get('mid_message', '')}"
            f"{output_dict.get('bot_message', '')}"
            f"Synergy_and_counterpick:\n"
            # f"support_dif: {output_dict['support_dif']}\n"
            f"Synergy_duo: {output_dict['synergy_duo']}\n"
            f"Synergy_trio: {output_dict['radiant_synergy_trio']}\n"
            f"Counterpick_duo: {output_dict['duo_diff']}\n"
            f"1vs2_counterpick: {output_dict['radiant_counterpick_1vs2']}\n"
            f'ПОМНИ: КОМАНДА ВАЖНЕЕ ПИКА')
        add_url(check_uniq_url)






def general(return_status=None):
    with open('./map_id_check.txt', 'r+') as f:
        maps_data = json.load(f)
    answer = get_heads()
    if not answer:
        print('не удалось выяснить heads')
        return
    heads, bodies = answer
    for i in range(len(heads)):
        status = check_head(heads, bodies, i, maps_data)
        if status is not None:
            return_status = status
    return return_status


if __name__ == "__main__":
    # update_my_protracker(show_prints=True)
        with open('count_synergy_10th_2000/synergy.txt', 'r') as f:
            synergy_data = json.load(f)
        with open('count_synergy_10th_2000/counterpick1vs1.txt', 'r') as f2:
            data_1vs1 = json.load(f2)
        with open('count_synergy_10th_2000/counterpick1vs2.txt', 'r') as f3:
            data_1vs2 = json.load(f3)
        with open('count_synergy_10th_2000/over40_dict.txt', 'r') as f:
            over40_data = json.load(f)
        with open('count_synergy_10th_2000/lane_dict.txt', 'r') as f:
            lane_data = json.load(f)
        synergy4, data_1vs3 = {}, {}
        check_old_maps(data_1vs1=data_1vs1, data_1vs2=data_1vs2,
                      lane_data=lane_data, over40_data=over40_data, synergy_data=synergy_data,
                      data_1vs3=data_1vs3, synergy4=synergy4)
        # one_match(radiant_heroes_and_pos={'pos1': {'hero_name': "faceless void"}, 'pos2': {'hero_name': "nature's prophet"},
        #                                   'pos3': {'hero_name': 'mars'}, 'pos4': {'hero_name': "snapfire"},
        #                                   'pos5': {'hero_name': "bane"}},
        #           dire_heroes_and_pos={'pos1': {'hero_name': "queen of pain"}, 'pos2': {'hero_name': "earthshaker"},
        #                                'pos3': {'hero_name': 'doom'}, 'pos4': {'hero_name': 'windranger'},
        #                                'pos5': {'hero_name': "tusk"}},
        #           lane_data=lane_data, data_1vs2=data_1vs2, data_1vs1=data_1vs1, over40_data=over40_data,
        #           synergy_data=synergy_data, data_1vs3=data_1vs3, synergy4=synergy4,
        #           radiant_team_name='Tearlaments Team', dire_team_name='dire')
        # one_match(radiant_heroes_and_pos={'pos1': {'hero_name': "sven"}, 'pos2': {'hero_name': "queen of pain"},
        #                                   'pos3': {'hero_name': 'slardar'}, 'pos4': {'hero_name': "ringmaster"},
        #                                   'pos5': {'hero_name': "disruptor"}},
        #           dire_heroes_and_pos={'pos1': {'hero_name': "terrorblade"}, 'pos2': {'hero_name': "sand king"},
        #                                'pos3': {'hero_name': 'ogre magi'}, 'pos4': {'hero_name': 'hoodwink'},
        #                                'pos5': {'hero_name': "jakiro"}},
        #           lane_data=lane_data, data_1vs2=data_1vs2, data_1vs1=data_1vs1, over40_data=over40_data,
        #           synergy_data=synergy_data, data_1vs3=data_1vs3, synergy4=synergy4,
        #           radiant_team_name='Kalamycha Team', dire_team_name='dire')

        # while True:
        #     # if is_moscow_night():
        #     #     sleep_until_morning()
        #     status = general()
        #     if status is None:
        #         print('Сплю 5 минут')
        #         time.sleep(300)
        #     else:
        #         print('Сплю 20 секунд')
        #         time.sleep(20)
