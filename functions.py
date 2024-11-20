import datetime
import html
import json
import os
import re
import shutil
import time
from urllib.parse import quote
from analyze_maps import new_proceed_map
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

import requests
from bs4 import BeautifulSoup

import asyncio

import id_to_name
import keys
from id_to_name import pro_teams
from keys import api_token_3, api_token_4, api_token_5, api_token_2, api_token_1, api_token_6, api_token_7, \
    api_token_8, api_token_9, api_token_10, api_token_11, api_token_12, api_token_13, api_token_14, \
    api_token_15, api_token_16, api_token_17, api_token_18



def get_urls(url, target_datetime=0):
    headers = {
        'Host': 'dltv.org',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/115.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/jxl,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Sec-GPC': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'TE': 'trailers'
    }
    response = requests.get(url=url, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'lxml')
        live_matches_block = soup.find('div', class_='live__matches')
        live_matches = live_matches_block.find_all('div', class_='live__matches-item__body')

        live_matches_urls = set()
        for match in live_matches:
            url = match.find('a')['href']
            live_matches_urls.add(url)
        if not len(live_matches_urls):
            upcoming_matches = soup.find('div', class_="upcoming__matches-item")
            if upcoming_matches:
                target_datetime_str = upcoming_matches['data-matches-odd']
                target_datetime = datetime.datetime.strptime(target_datetime_str,
                                                             '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=2,
                                                                                                       minutes=54)
        if not len(live_matches_urls):
            live_matches_urls = None
        return live_matches_urls, target_datetime
    else:
        print(f'{response.status_code}')


def get_team_names(soup):
    tags_block = soup.find('div', class_='plus__stats-details desktop-none')
    tags = tags_block.find_all('span', class_='title')
    scores = soup.find('div', class_='score__scores live').find_all('span')
    score = [i.text.strip() for i in scores]
    radiant_team_name, dire_team_name = None, None
    for tag in tags:
        team_info = tag.text.strip().split('')
        if team_info[1].replace(' ', '').lower() == 'radiant':
            radiant_team_name = team_info[0].lower().replace(' ', '')
        else:
            dire_team_name = team_info[0].lower().replace(' ', '')
    return radiant_team_name, dire_team_name, score


def get_player_names_and_heroes(soup):
    radiant_players, dire_players = {}, {}
    radiant_block = soup.find('div', class_='picks__new-picks__picks radiant')
    dire_block = soup.find('div', class_='picks__new-picks__picks dire')
    if radiant_block is not None and dire_block is not None:
        radiant_heroes_block = radiant_block.find_all('div', class_='pick player')
        dire_heroes_block = dire_block.find_all('div', class_='pick player')
        for hero in radiant_heroes_block[0:5]:
            hero_name = hero.get('data-tippy-content').replace('Outworld Devourer', 'Outworld Destroyer')
            player_name = hero.find('span', class_='pick__player-title').text.lower()
            player_name = re.sub(r'[^\w\s\u4e00-\u9fff]+', '', player_name)
            radiant_players[player_name] = {'hero': hero_name}
        for hero in dire_heroes_block:
            hero_name = hero.get('data-tippy-content').replace('Outworld Devourer', 'Outworld Destroyer')
            player_name = hero.find('span', class_='pick__player-title').text.lower()
            player_name = re.sub(r'[^\w\s\u4e00-\u9fff]+', '', player_name)
            dire_players[player_name] = {'hero': hero_name}
        if len(radiant_players) == 5 and len(dire_players) == 5:
            return radiant_players, dire_players


def get_team_positions(url):
    response = requests.get(url)
    if response.status_code == 200:
        response_html = html.unescape(response.text)
        soup = BeautifulSoup(response_html, 'lxml')
        picks_item = soup.find_all('div', class_='picks-item with-match-players-tooltip')
        # picks_item = soup.find('div', class_='match-statistics--teams-players')

        heroes = []
        for hero_block in picks_item:
            for hero in list(id_to_name.translate.values()):
                if f'({hero})' in hero_block.text:
                    heroes.append(hero)
        radiant_heroes_and_pos = {}
        dire_heroes_and_pos = {}
        for i in range(5):
            for translate_hero_id in id_to_name.translate:
                if id_to_name.translate[translate_hero_id] == heroes[i]:
                    hero_id = translate_hero_id
                    radiant_heroes_and_pos[f'pos{i + 1}'] = {'hero_id': hero_id, 'hero_name': heroes[i]}
        c = 0
        for i in range(5, 10):
            for translate_hero_id in id_to_name.translate:
                if id_to_name.translate[translate_hero_id] == heroes[i]:
                    hero_id = translate_hero_id
                    dire_heroes_and_pos[f'pos{c + 1}'] = {'hero_id': hero_id, 'hero_name': heroes[i]}
                    c += 1

        return radiant_heroes_and_pos, dire_heroes_and_pos
    else:
        print('нету live матчей')



def levenshtein_distance(s1, s2):
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def similarity_percentage(s1, s2):
    distance = levenshtein_distance(s1, s2)
    max_length = max(len(s1), len(s2))
    return (1 - distance / max_length) * 100


def are_similar(s1, s2, threshold=70):
    return similarity_percentage(s1, s2) >= threshold


def get_map_id(match):
    if match['team_dire'] is not None and match['team_radiant'] is not None \
            and 'Kobold' not in match['tournament']['name']:
        radiant_team_name = match['team_radiant']['name'].lower()
        dire_team_name = match['team_dire']['name'].lower()
        score = match['best_of_score']
        dic = {
            'fissure': 1,
            'riyadh': 1,
            'international': 1,
            'pgl': 1,
            'bb': 1,
            'epl': 2,
        }
        match_name = match['tournament']['name'].lower()
        tier = match['tournament']['tier']

        # Проверка наличия имени в словаре и обновление значения tier
        for name in dic:
            if name in match_name:
                tier = dic[name]
        if tier in [1, 2, 3, 4, 5]:
            for karta in match['related_matches']:
                if karta['status'] == 'online':
                    map_id = karta['id']
                    url = f'https://cyberscore.live/en/matches/{map_id}/'
                    result = if_unique(url)
                    if result is not None:
                        return url, radiant_team_name, dire_team_name, score, tier


def if_unique(url, score):
    check_uniq_url = str(url) + '.' + str(int(score[0]) + int(score[1]))
    with open('map_id_check.txt', 'r+') as f:
        data = json.load(f)
        if check_uniq_url not in data:
            # data.append(url)
            # f.truncate()
            # f.seek(0)
            # json.dump(data, f)
            return True


def add_url(url, score):
    check_uniq_url = str(url) + '.' + str(int(score[0]) + int(score[1]))
    with open('map_id_check.txt', 'r+') as f:
        data = json.load(f)
        data.append(check_uniq_url)
        f.truncate()
        f.seek(0)
        json.dump(data, f)


def find_in_radiant(radiant_players, nick_name, translate, position, radiant_pick, radiant_lst):
    for radiant_player_name in radiant_players:
        if are_similar(radiant_player_name, nick_name, threshold=70):
            radiant_pick[translate[position]] = radiant_players[radiant_player_name]['hero']
            if position in radiant_lst:
                radiant_lst.remove(position)
                return radiant_lst, radiant_pick


def find_in_dire(dire_players, nick_name, translate, position, dire_pick, dire_lst):
    for dire_player_name in dire_players:
        if are_similar(dire_player_name, nick_name, threshold=70):
            dire_pick[translate[position]] = dire_players[dire_player_name]['hero']
            if position in dire_lst:
                dire_lst.remove(position)
                return dire_lst, dire_pick


def if_picks_are_done(soup):
    dire_block = soup.find('div', class_='picks__new-picks__picks dire')
    radiant_block = soup.find('div', class_='picks__new-picks__picks radiant')
    if radiant_block is not None and dire_block is not None:
        items_radiant = radiant_block.find('div', class_='items').find_all('div', class_='pick')
        items_dire = dire_block.find('div', class_='items').find_all('div', class_='pick')
        if len(items_dire) == 5 and len(items_radiant) == 5:
            return True


def clean_up(inp, length=0):
    if len(inp) >= length:
        copy = inp.copy()
        for i in inp:
            if 0.52 >= i >= 0.48:
                copy.remove(i)
        if len(copy) <= length:
            return inp
        else:
            return copy
    else:
        return inp


def send_message(message):
    bot_token = f'{keys.Token}'
    chat_id = f'{keys.Chat_id}'
    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    payload = {
        'chat_id': chat_id,
        'text': message
    }
    requests.post(url, json=payload)


def str_to_json(input_data):
    text = input_data.replace(':', '":').replace('#', '').replace('{', '{"')
    data = re.sub(r",(?=[a-zA-Z])", ',"', text)
    data = re.sub(r'\.(\d{2})(\d{2})', r'\1.\2', data, flags=re.MULTILINE)
    data = re.sub(r'\.(\d{2})(\d1)', r'\1.\2', data, flags=re.MULTILINE)
    data = re.sub(r':0(?=[0-9])', ':', data)
    data = re.sub(r'[\x00-\x1F]+', '', data)

    def multiply_by_10(match):
        number = int(match.group(1))
        return ':' + str(number * 10) + ','

    data = re.sub(r':\.(\d1),', multiply_by_10, data).replace(':.', ':')
    data = re.sub(r':(0)([0-9])', r':\2', data, flags=re.MULTILINE)
    return data


def fetch_hero_data(hero_name):
    hero_url = hero_name.replace(' ', '%20')
    url = f'https://dota2protracker.com/hero/{hero_url}'
    response = requests.get(url)
    if response.status_code != 200:
        print(f'Error fetching data for {hero_name}: {url}')
        return None
    soup = BeautifulSoup(response.text, 'lxml')
    stats = soup.find_all('script')
    matchups = re.search(r'matchups:(\[.*?])', stats[5].text, re.DOTALL)
    synergies = re.search(r'synergies:(\[.*?])', stats[5].text, re.DOTALL)
    matchups = json.loads(str_to_json(matchups.group(1)).strip())
    synergies = json.loads(str_to_json(synergies.group(1)).strip())
    return matchups, synergies


def process_synergy_data(position, synergies, team_positions):
    wr_list = []
    for synergy in synergies:
        tracker_position = synergy['position'].replace('pos ', 'pos')
        data_pos = synergy['other_pos'].replace('pos ', 'pos')
        data_hero = synergy['other_hero']
        data_wr = synergy['win_rate']
        if synergy['num_matches'] >= 15 and data_pos in team_positions and team_positions[data_pos][
            'hero_name'] == data_hero:
            if tracker_position == position:
                wr_list.append(data_wr)
    return wr_list


def process_matchup_data(position, matchups, opposing_team_positions):
    wr_list = []
    for matchup in matchups:
        tracker_position = matchup['position'].replace('pos ', 'pos')
        data_pos = matchup['other_pos'].replace('pos ', 'pos')
        data_hero = matchup['other_hero']
        data_wr = matchup['win_rate']
        if matchup['num_matches'] >= 15 and data_pos in opposing_team_positions and \
                opposing_team_positions[data_pos]['hero_name'] == data_hero:
            if tracker_position == position:
                wr_list.append(data_wr)
    return wr_list


def dota2protracker_old(radiant_heroes_and_positions, dire_heroes_and_positions, synergy=None,
                        counterpick=None):
    start_time = time.time()
    radiant_pos1_with_team, radiant_pos2_with_team, radiant_pos3_with_team, dire_pos1_with_team, \
        dire_pos2_with_team, dire_pos3_with_team = [], [], [], [], [], []
    radiant_wr_with, dire_wr_with, radiant_pos3_vs_team, dire_pos3_vs_team, radiant_wr_against, \
        dire_wr_against, radiant_pos1_vs_team, dire_pos1_vs_team, radiant_pos2_vs_team, dire_pos2_vs_team, \
        radiant_pos4_with_pos5, dire_pos4_with_pos5 = [], [], [], [], [], [], [], [], [], [], None, None
    for position in radiant_heroes_and_positions:
        if position != 'pos5':
            hero_url = radiant_heroes_and_positions[position]['hero_name'].replace(' ', '%20')
            url = f'https://dota2protracker.com/hero/{hero_url}'
            response = requests.get(url)
            if response.status_code != 200:
                print(f'Ошибка dota2protracker\n{url}')
            soup = BeautifulSoup(response.text, 'lxml')
            stats = soup.find_all('script')
            matchups = re.search(r'matchups:(\[.*?])', stats[5].text, re.DOTALL)
            synergies = re.search(r'synergies:(\[.*?])', stats[5].text, re.DOTALL)
            matchups, synergies = json.loads(str_to_json(matchups.group(1)).strip()), json.loads(
                str_to_json(synergies.group(1)).strip())
            for synergy in synergies:
                tracker_position = synergy['position'].replace('pos ', 'pos')
                data_pos = synergy['other_pos'].replace('pos ', 'pos')
                data_hero = synergy['other_hero']
                data_wr = synergy['win_rate']
                if synergy['num_matches'] >= 15:
                    # Extract the values of 'data-hero', 'data-wr', and 'data-pos' attributes
                    if position == 'pos1':
                        if 'pos2' in data_pos and data_hero == radiant_heroes_and_positions['pos2'][
                            'hero_name'] and tracker_position == position:
                            radiant_pos1_with_team.append(data_wr)
                        elif 'pos3' in data_pos and data_hero == radiant_heroes_and_positions['pos3'][
                            'hero_name'] and tracker_position == position:
                            radiant_pos1_with_team.append(data_wr)
                        elif 'pos4' in data_pos and data_hero == radiant_heroes_and_positions['pos4'][
                            'hero_name'] and tracker_position == position:
                            radiant_pos1_with_team.append(data_wr)
                        elif 'pos5' in data_pos and data_hero == radiant_heroes_and_positions['pos5'][
                            'hero_name'] and tracker_position == position:
                            radiant_pos1_with_team.append(data_wr)

                    if position == 'pos2':
                        if 'pos3' in data_pos and data_hero == radiant_heroes_and_positions['pos3'][
                            'hero_name'] and tracker_position == position:
                            radiant_pos2_with_team.append(data_wr)
                        elif 'pos4' in data_pos and data_hero == radiant_heroes_and_positions['pos4'][
                            'hero_name'] and tracker_position == position:
                            radiant_pos2_with_team.append(data_wr)
                        elif 'pos5' in data_pos and data_hero == radiant_heroes_and_positions['pos5'][
                            'hero_name'] and tracker_position == position:
                            radiant_pos2_with_team.append(data_wr)

                    if position == 'pos3':
                        if 'pos4' in data_pos and data_hero == radiant_heroes_and_positions['pos4'][
                            'hero_name'] and tracker_position == position:
                            radiant_pos3_with_team.append(data_wr)
                        elif 'pos5' in data_pos and data_hero == radiant_heroes_and_positions['pos5'][
                            'hero_name'] and tracker_position == position:
                            radiant_pos3_with_team.append(data_wr)
                    if position == 'pos4':
                        if radiant_pos4_with_pos5 is not None:
                            break
                        if 'pos5' in data_pos and data_hero == radiant_heroes_and_positions['pos5'][
                            'hero_name'] and tracker_position == position:
                            radiant_pos4_with_pos5 = data_wr
    if radiant_pos4_with_pos5 is not None:
        radiant_wr_with += [radiant_pos4_with_pos5]
    radiant_wr_with += radiant_pos3_with_team + radiant_pos2_with_team + radiant_pos1_with_team
    for position in dire_heroes_and_positions:

        if position != 'pos5':
            hero_url = dire_heroes_and_positions[position]['hero_name'].replace(' ', '%20')
            url = f'https://dota2protracker.com/hero/{hero_url}'
            response = requests.get(url)
            if response.status_code != 200:
                print(f'Ошибка dota2protracker\n{url}')
            soup = BeautifulSoup(response.text, 'lxml')
            stats = soup.find_all('script')
            matchups = re.search(r'matchups:(\[.*?])', stats[5].text, re.DOTALL)
            synergies = re.search(r'synergies:(\[.*?])', stats[5].text, re.DOTALL)
            matchups, synergies = json.loads(str_to_json(matchups.group(1)).strip()), json.loads(
                str_to_json(synergies.group(1)).strip())
            for synergy in synergies:
                tracker_position = synergy['position'].replace('pos ', 'pos')
                data_pos = synergy['other_pos'].replace('pos ', 'pos')
                data_hero = synergy['other_hero']
                data_wr = synergy['win_rate']
                if synergy['num_matches'] >= 15:
                    if position == 'pos1':
                        if 'pos2' in data_pos and data_hero == dire_heroes_and_positions['pos2'][
                            'hero_name'] and tracker_position == position:
                            dire_pos1_with_team.append(data_wr)
                        elif 'pos3' in data_pos and data_hero == dire_heroes_and_positions['pos3'][
                            'hero_name'] and tracker_position == position:
                            dire_pos1_with_team.append(data_wr)
                        elif 'pos4' in data_pos and data_hero == dire_heroes_and_positions['pos4'][
                            'hero_name'] and tracker_position == position:
                            dire_pos1_with_team.append(data_wr)
                        elif 'pos5' in data_pos and data_hero == dire_heroes_and_positions['pos5'][
                            'hero_name'] and tracker_position == position:
                            dire_pos1_with_team.append(data_wr)

                    if position == 'pos2':
                        if 'pos3' in data_pos and data_hero == dire_heroes_and_positions['pos3'][
                            'hero_name'] and tracker_position == position:
                            dire_pos2_with_team.append(data_wr)
                        elif 'pos4' in data_pos and data_hero == dire_heroes_and_positions['pos4'][
                            'hero_name'] and tracker_position == position:
                            dire_pos2_with_team.append(data_wr)
                        elif 'pos5' in data_pos and data_hero == dire_heroes_and_positions['pos5'][
                            'hero_name'] and tracker_position == position:
                            dire_pos2_with_team.append(data_wr)

                    if position == 'pos3':
                        if 'pos4' in data_pos and data_hero == dire_heroes_and_positions['pos4'][
                            'hero_name'] and tracker_position == position:
                            dire_pos3_with_team.append(data_wr)
                        elif 'pos5' in data_pos and data_hero == dire_heroes_and_positions['pos5'][
                            'hero_name'] and tracker_position == position:
                            dire_pos3_with_team.append(data_wr)
                    if position == 'pos4':
                        if dire_pos4_with_pos5 is not None:
                            break
                        if 'pos5' in data_pos and data_hero == dire_heroes_and_positions['pos5'][
                            'hero_name'] and tracker_position == position:
                            dire_pos4_with_pos5 = data_wr
    if dire_pos4_with_pos5 is not None:
        dire_wr_with += [dire_pos4_with_pos5]
    dire_wr_with += dire_pos3_with_team + dire_pos2_with_team + dire_pos1_with_team
    for position in radiant_heroes_and_positions:

        hero_url = radiant_heroes_and_positions[position]['hero_name'].replace(' ', '%20')
        url = f'https://dota2protracker.com/hero/{hero_url}'
        response = requests.get(url)
        if response.status_code != 200:
            print(f'Ошибка dota2protracker\n{url}')
        soup = BeautifulSoup(response.text, 'lxml')
        stats = soup.find_all('script')
        matchups = re.search(r'matchups:(\[.*?])', stats[5].text, re.DOTALL)
        synergies = re.search(r'synergies:(\[.*?])', stats[5].text, re.DOTALL)
        matchups, synergies = json.loads(str_to_json(matchups.group(1)).strip()), json.loads(
            str_to_json(synergies.group(1)).strip())
        for matchup in matchups:
            tracker_position = matchup['position'].replace('pos ', 'pos')
            data_pos = matchup['other_pos'].replace('pos ', 'pos')
            data_hero = matchup['other_hero']
            data_wr = matchup['win_rate']
            if matchup['num_matches'] >= 15 and data_pos in radiant_heroes_and_positions:
                if position == 'pos1' and tracker_position == 'pos1' and data_hero == \
                        dire_heroes_and_positions[data_pos]['hero_name']:
                    radiant_pos1_vs_team.append(data_wr)
                elif position == 'pos2' and tracker_position == 'pos2' and data_hero == \
                        dire_heroes_and_positions[data_pos]['hero_name']:
                    radiant_pos2_vs_team.append(data_wr)
                elif position == 'pos3' and tracker_position == 'pos3' and data_hero == \
                        dire_heroes_and_positions[data_pos]['hero_name']:
                    radiant_pos3_vs_team.append(data_wr)
                elif position == 'pos4' and tracker_position == 'pos4' and data_hero == \
                        dire_heroes_and_positions[data_pos]['hero_name']:
                    radiant_wr_against.append(data_wr)
                elif position == 'pos5' and tracker_position == 'pos5' and data_hero == \
                        dire_heroes_and_positions[data_pos]['hero_name']:
                    radiant_wr_against.append(data_wr)

                if 'pos1' in data_pos and data_hero == dire_heroes_and_positions['pos1'][
                    'hero_name'] and tracker_position == position:
                    dire_pos1_vs_team.append(100 - data_wr)
                elif 'pos2' in data_pos and data_hero == dire_heroes_and_positions['pos2'][
                    'hero_name'] and tracker_position == position:
                    dire_pos2_vs_team.append(100 - data_wr)
                elif 'pos3' in data_pos and data_hero == dire_heroes_and_positions['pos3'][
                    'hero_name'] and tracker_position == position:
                    dire_pos3_vs_team.append(100 - data_wr)
    radiant_wr_against += radiant_pos3_vs_team + radiant_pos2_vs_team + radiant_pos1_vs_team
    dire_wr_against += dire_pos3_vs_team + dire_pos2_vs_team + dire_pos1_vs_team
    if len(radiant_wr_with) > 0 and len(dire_wr_with) > 0:
        synergy = round((sum(radiant_wr_with) / len(radiant_wr_with)) - (sum(dire_wr_with) / len(dire_wr_with)), 2)
    if len(radiant_wr_against) > 0:
        counterpick = round((sum(radiant_wr_against) / len(radiant_wr_against)) - (
                sum(dire_wr_against) / len(dire_wr_against)), 2)
    end_time = time.time()
    execution_time = end_time - start_time
    print(f'dota2protracker_old time: {execution_time}s')
    return f'\ndota2protracker_old:\nSynergy: {synergy}\nCounterpick: {counterpick}\n'


def some_func():
    with open('teams_stat_dict.txt', 'r') as f:
        data = json.load(f)
        data_copy = data.copy()
        for team in data_copy:
            odd = data[team]['kills'] / data[team]['time']
            data.setdefault(team, {}).setdefault('odd', odd)
        sorted_data = dict(sorted(data.items(), key=lambda item: item[1]["odd"]))
    with open('teams_stat_dict.txt', 'w') as f:
        json.dump(sorted_data, f, indent=4)


def get_pro_players_ids(counter=0):
    bottle, pro_ids = set(), set()
    for name in pro_teams:
        counter += 1
        print(f'{counter}/{len(pro_teams)}')
        bottle.add(pro_teams[name]['id'])
        if len(bottle) == 5 or counter == len(pro_teams):
            query = '''
                    {teams(teamIds: %s){
                        members{
                            lastMatchDateTime
                        steamAccount{
                          id
                          name

                        }
                        team {
                          id
                          name
                        }
                      }
                    }}''' % list(bottle)
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Origin": "https://api.stratz.com",
                "Referer": "https://api.stratz.com/graphiql",
                "User-Agent": "STRATZ_API",
                "Authorization": f"Bearer {api_token_5}"
            }
            response = requests.post('https://api.stratz.com/graphql', json={"query": query}, headers=headers)
            teams = json.loads(response.text)['data']['teams']
            for team in teams:
                last_date = 0
                for member in team['members']:
                    if last_date < member['lastMatchDateTime']:
                        last_date = member['lastMatchDateTime']
                for member in team['members']:
                    if member['lastMatchDateTime'] == last_date:
                        pro_ids.add(member['steamAccount']['id'])
            bottle = set()
    return pro_ids


def get_maps_new(game_mods, maps_to_save, ids,
                 show_prints=None, skip=0, count=0, only_in_ids=False):
    tokens = [api_token_3, api_token_4, api_token_5, api_token_2, api_token_1, api_token_6, api_token_7,
              api_token_8, api_token_9, api_token_10, api_token_11, api_token_12, api_token_13, api_token_14,
              api_token_15, api_token_16, api_token_17, api_token_18]
    api_token = api_token_16
    ids_to_graph, total_map_ids, output_data = [], [], []
    for check_id in set(ids):
        if check_id == 9545759:
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
                json.dump(output_data, f)


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
            response = requests.post('https://api.stratz.com/graphql', json={"query": query}, headers=headers)
            data = json.loads(response.text)
            if game_mods == [2, 22]:
                if any(player['matchesFirstPeriod'] for player in data['data']['players']):
                    for player in data['data']['players']:
                        for match in player['matchesFirstPeriod']:
                            output_data.append(match['id'])
                        skip += 100
                else:
                    check = False
            else:
                for team in data['data']['teams']:
                    for match in team['matches']:
                        if only_in_ids:
                            if match['radiantTeam']['id'] in ids and match['direTeam']['id'] in ids:
                                output_data.append(match['id'])
                        else:
                            output_data.append(match['id'])
                        check = False
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
        # if another_counter % 300 == 0:
        #     save_temp_file(new_data, mkdir, another_counter)
        #     new_data = {}

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
                                         headers=headers, timeout=10)
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

    # save_temp_file(new_data, mkdir, another_counter)
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


def save_final_file(file_data, mkdir, file_name):
    # Сохранение финальных данных в файл
    final_path = f'./{mkdir}/{file_name}.txt'
    with open(final_path, 'w') as f:
        json.dump(file_data, f)


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
                    and (match['durationSeconds'] / 60) >= 21:
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


def merge_dicts(dict1, dict2):
    """
    Функция для объединения двух словарей. Если ключи пересекаются, значения объединяются.
    Если ключ уникален, он просто добавляется.
    """
    for key, value in dict2.items():
        if key in dict1:
            if isinstance(value, dict) and isinstance(dict1[key], dict):
                dict1[key] = merge_dicts(dict1[key], value)
            elif isinstance(value, list) and isinstance(dict1[key], list):
                dict1[key].extend(value)
            else:
                dict1[key] += value
        else:
            dict1[key] = value
    return dict1


def load_json_file(filepath, default):
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return default


def save_json_file(filepath, data):
    with open(filepath, 'w') as f:
        json.dump(data, f)


def load_and_process_json_files(mkdir, **kwargs):
    result = {}
    for key, flag in kwargs.items():
        if flag:
            result[key] = load_json_file(f'./{mkdir}/{key}', {})
        else:
            result[key] = {}
    return result


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




def calculate_average(values):
    return sum(values) / len(values) if len(values) else None


def synergy_team(heroes_and_pos, output, mkdir, data):
    unique_combinations = set()
    for pos in heroes_and_pos:
        hero_id = str(heroes_and_pos[pos]['hero_id'])
        for second_pos in heroes_and_pos:
            second_hero_id = str(heroes_and_pos[second_pos]['hero_id'])
            if hero_id == second_hero_id:
                continue
            key = f"{hero_id + pos},{second_hero_id + second_pos}"
            foo = data.get(key, {})
            if len(foo) >= 20:
                value = foo.count(1) / (foo.count(1) + foo.count(0))
                output[f'{mkdir}_duo'].append(value)
            for third_pos in heroes_and_pos:
                third_hero_id = str(heroes_and_pos[third_pos]['hero_id'])
                if third_hero_id in [second_hero_id, hero_id]:
                    continue
                third_hero_id = str(heroes_and_pos[third_pos]['hero_id'])
                combo = tuple(sorted([hero_id, second_hero_id, third_hero_id]))
                if combo not in unique_combinations:
                    unique_combinations.add(combo)
                    key = f"{hero_id + pos},{second_hero_id + second_pos},{third_hero_id + third_pos}"
                    foo = data.get(key, {})
                    if len(foo) > 9:
                        value = foo.count(1) / (foo.count(1) + foo.count(0))
                        output[f'{mkdir}_trio'].append(value)
                # for fourth_pos in heroes_and_pos:
                #     fourth_hero_id = str(heroes_and_pos[fourth_pos]['hero_id'])
                #     if fourth_hero_id in [third_hero_id, second_hero_id, hero_id]:
                #         continue
                #     combo = tuple(sorted([hero_id, second_hero_id, third_hero_id, fourth_hero_id]))
                #     if combo not in unique_combinations:
                #         unique_combinations.add(combo)
                #         key = f"{hero_id + pos},{second_hero_id + second_pos}," \
                #               f"{third_hero_id + third_pos},{fourth_hero_id + fourth_pos}"
                #         foo = data.get(key, {})
                #         if len(foo) >= 4:
                #             value = foo.count(1) / (foo.count(1) + foo.count(0))
                #             output[f'{mkdir}_quad'].append(value)
    return output


def counterpick_team(heroes_and_pos, heroes_and_pos_opposite, output, mkdir, data1vs1, data1vs2):
    unique_combinations = set()
    for pos in heroes_and_pos:
        hero_id = str(heroes_and_pos[pos]['hero_id'])
        for enemy_pos in heroes_and_pos_opposite:
            enemy_hero_id = str(heroes_and_pos_opposite[enemy_pos]['hero_id'])
            key = f"{hero_id}{pos},{enemy_hero_id}{enemy_pos}"
            foo = data1vs1.get(key, {})
            if len(foo) >= 20:
                value = foo.count(1) / (foo.count(1) + foo.count(0))
                output[f'{mkdir}_duo'].append(value)
            for second_enemy_pos in heroes_and_pos_opposite:
                second_enemy_id = str(heroes_and_pos_opposite[second_enemy_pos]['hero_id'])
                if enemy_hero_id == second_enemy_id:
                    continue
                combo = (hero_id,) + tuple(sorted([enemy_hero_id, second_enemy_id]))
                if combo not in unique_combinations:
                    unique_combinations.add(combo)
                    key = f"{hero_id}{pos},{enemy_hero_id}{enemy_pos}," \
                          f"{second_enemy_id}{second_enemy_pos}"
                    foo = data1vs2.get(key, {})
                    if len(foo) >= 15:
                        value = foo.count(1) / (foo.count(1) + foo.count(0))
                        output[f'{mkdir}_1vs2'].append(value)
                # for third_enemy_pos in heroes_and_pos_opposite:
                #     third_enemy_id = str(heroes_and_pos_opposite[third_enemy_pos]['hero_id'])
                #     if third_enemy_id in [enemy_hero_id, second_enemy_id]:
                #         continue
                #     combo = (hero_id,) + tuple(sorted([enemy_hero_id, second_enemy_id, third_enemy_id]))
                #     if combo not in unique_combinations:
                #         unique_combinations.add(combo)
                #         key = f"{hero_id}{pos},{enemy_hero_id}{enemy_pos}," \
                #               f"{second_enemy_id}{second_enemy_pos},{third_enemy_id}{third_enemy_pos}"
                #         value = data1vs3.get(key, {})
                #         if value:
                #             print(len(value))
    return output


def synergy_and_counterpick_new(radiant_heroes_and_pos, dire_heroes_and_pos):
    start_time = time.time()
    output = {'radiant_synergy_duo': [], 'dire_synergy_duo': [], 'radiant_synergy_trio': [], 'dire_synergy_trio': [],
              'radiant_counterpick_duo': [], 'dire_counterpick_duo': [], 'radiant_counterpick_1vs2': [],
              'dire_counterpick_1vs2': []}
    with open('1722505765_top600_heroes_data/synergy.txt', 'r') as f:
        data = json.load(f)
        synergy_team(radiant_heroes_and_pos, output, 'radiant_synergy', data)
        synergy_team(dire_heroes_and_pos, output, 'dire_synergy', data)
    # with open('1722505765_top600_heroes_data/counterpick1vs3.txt', 'r') as f:
    #     data1vs3 = json.load(f)
    with open('1722505765_top600_heroes_data/counterpick1vs1.txt', 'r') as f:
        data1vs1 = json.load(f)
        with open('1722505765_top600_heroes_data/counterpick1vs2.txt', 'r') as f:
            data1vs2 = json.load(f)
            counterpick_team(heroes_and_pos=radiant_heroes_and_pos, heroes_and_pos_opposite=dire_heroes_and_pos,
                             output=output, mkdir='radiant_counterpick', data1vs2=data1vs2, data1vs1=data1vs1)
            counterpick_team(heroes_and_pos=dire_heroes_and_pos, heroes_and_pos_opposite=radiant_heroes_and_pos,
                             output=output, mkdir='dire_counterpick', data1vs2=data1vs2, data1vs1=data1vs1)

    output['radiant_counterpick_duo'] = clean_up(output['radiant_counterpick_duo'], 4)
    output['radiant_counterpick_1vs2'] = clean_up(output['radiant_counterpick_1vs2'], 4)
    output['dire_counterpick_1vs2'] = clean_up(output['dire_counterpick_1vs2'], 4)
    output['radiant_synergy_duo'] = clean_up(output['radiant_synergy_duo'], 4)
    output['dire_synergy_duo'] = clean_up(output['dire_synergy_duo'], 4)
    try:
        radiant_counterpick_duo = sum(output['radiant_counterpick_duo']) / len(output['radiant_counterpick_duo']) * 100
        duo_diff = radiant_counterpick_duo - (100 - radiant_counterpick_duo)
    except ZeroDivisionError:
        duo_diff = None
    if len(output['radiant_counterpick_1vs2']) > 3 and len(output['dire_counterpick_1vs2']) > 3:
        radiant_counterpick_1vs2 = sum(output['radiant_counterpick_1vs2']) / len(
            output['radiant_counterpick_1vs2'])
        dire_counterpick_1vs2 = sum(output['dire_counterpick_1vs2']) / len(
            output['dire_counterpick_1vs2'])
        radiant_counterpick_1vs2_win_prob = (radiant_counterpick_1vs2 - dire_counterpick_1vs2) * 100
    else:
        radiant_counterpick_1vs2_win_prob = None
    try:
        radiant_synergy_duo = sum(output['radiant_synergy_duo']) / len(output['radiant_synergy_duo'])
        dire_synergy_duo = sum(output['dire_synergy_duo']) / len(output['dire_synergy_duo'])
        synergy_duo = (radiant_synergy_duo - dire_synergy_duo) * 100
    except ZeroDivisionError:
        synergy_duo = None
    if len(output['dire_synergy_trio']) > 3 and len(output['radiant_synergy_trio']) > 3:
        radiant_synergy_trio = sum(output['radiant_synergy_trio']) / len(
            output['radiant_synergy_trio'])
        dire_synergy_trio = sum(output['dire_synergy_trio']) / len(output['dire_synergy_trio'])
        radiant_synergy_trio_win_prob = (radiant_synergy_trio - dire_synergy_trio) * 100
    else:
        radiant_synergy_trio_win_prob = None
    end_time = time.time()
    execution_time = end_time - start_time
    print(f'synergy_and_counterpick_new time: {execution_time}s')
    return f'\nsynergy_and_counterpick_new:\nSynergy_duo: {synergy_duo}\nSynergy_trio: {radiant_synergy_trio_win_prob}\nCounterpick_duo: {duo_diff}\nCounterpick_1vs2: {radiant_counterpick_1vs2_win_prob}'


def calculate_over40(radiant_heroes_and_pos, dire_heroes_and_pos):
    with open('1722505765_top600_heroes_data/over40_dict.txt', 'r') as f:
        data = json.load(f)['value']
    radiant_counterpick_duo, r_avg1vs2 = over40_counter(radiant_heroes_and_pos, dire_heroes_and_pos, data)
    dire_counterpick_duo, d_avg1vs2 = over40_counter(dire_heroes_and_pos, radiant_heroes_and_pos, data)
    radiant_over40 = avg_over40(radiant_heroes_and_pos, data)
    dire_over40 = avg_over40(dire_heroes_and_pos, data)
    if radiant_over40 is not None and dire_over40 is not None:
        over40 = (radiant_over40 - dire_over40) * 100
    else:
        over40 = None
    if None not in [r_avg1vs2, d_avg1vs2]:
        avg1vs2 = r_avg1vs2 - d_avg1vs2
    else:
        avg1vs2 = None
    duo_counterpick = radiant_counterpick_duo - dire_counterpick_duo
    return f'Radiant после 40 минуты сильнее на: \nSynergy: {over40}\nCounterpick_duo: {duo_counterpick}\nCounterpick_1vs2: {avg1vs2}\n\n'


def over40_counter(my_team, enemy_team, data):
    uniq_combo, duo_winrate, winrate1vs2 = set(), [], []
    for pos in my_team:
        hero_id = str(my_team[pos]['hero_id'])
        hero_data = data.get(hero_id, {}).get(pos, {}).get('over40_counterpick_duo', {})
        for enemy_pos in enemy_team:
            enemy_hero_id = str(enemy_team[enemy_pos]['hero_id'])
            duo_data = hero_data.get(enemy_hero_id, {}).get(enemy_pos, {})
            foo = duo_data.get('value', {})
            if len(foo) > 5:
                duo_winrate.append(foo.count(1) / (foo.count(0) + foo.count(1)))
            for second_enemy_pos in enemy_team:
                if second_enemy_pos == enemy_pos:
                    continue
                second_enemy_id = str(enemy_team[second_enemy_pos]['hero_id'])
                combo = tuple(sorted([hero_id, enemy_hero_id, second_enemy_id]))
                if combo not in uniq_combo:
                    uniq_combo.add(combo)
                    trio_data = duo_data.get('1vs2', {}).get(second_enemy_id, {}).get(second_enemy_pos, {}).get('value',
                                                                                                                [])
                    if len(trio_data) >= 6:
                        winrate1vs2.append(trio_data.count(1) / (trio_data.count(0) + trio_data.count(1)))
    duo = sum(duo_winrate) / len(duo_winrate) * 100
    try:
        avg1vs2 = sum(winrate1vs2) / len(winrate1vs2) * 100
    except:
        avg1vs2 = None
    return duo, avg1vs2


def avg_over40(heroes_and_positions, data):
    start_time = time.time()
    over40_duo, over40_trio, time_duo, kills_duo, kills_trio, time_trio, radiant_lane_report_unique_combinations, \
        dire_lane_report_unique_combinations, solo = [], [], [], [], [], [], [], [], []
    over40_unique_combinations = set()
    positions = ['1', '2', '3', '4', '5']
    for pos in positions:
        hero_id = str(heroes_and_positions.get(('pos' + pos), {}).get('hero_id', {}))
        hero_data = data.get(hero_id, {}).get('pos' + pos, {})
        hero_data = hero_data.get('over40_duo', {})
        pass
        for pos2, item in heroes_and_positions.items():
            second_hero_id = str(item['hero_id'])
            if second_hero_id == hero_id:
                continue
            duo_data = hero_data.get(second_hero_id, {}).get(pos2, {})
            combo = tuple(sorted([hero_id, second_hero_id]))
            if combo not in over40_unique_combinations:
                over40_unique_combinations.add(combo)
                if len(duo_data.get('value', {})) >= 15:
                    value = duo_data['value'].count(1) / (duo_data['value'].count(1) + duo_data['value'].count(0))
                    over40_duo.append(value)
            # Третий герой
            for pos3, item3 in heroes_and_positions.items():
                third_hero_id = str(item3['hero_id'])
                if third_hero_id in [second_hero_id, hero_id]:
                    continue
                combo = tuple(sorted([hero_id, second_hero_id, third_hero_id]))
                if combo not in over40_unique_combinations:
                    over40_unique_combinations.add(combo)
                    trio_data = duo_data.get('over40_trio', {}).get(third_hero_id, {}).get(pos3, {}).get('value', {})
                    if len(trio_data) >= 10:
                        value = trio_data.count(1) / (trio_data.count(1) + trio_data.count(0))
                        over40_trio.append(value)
    # avg_over40_duo = calculate_average(clean_up(over40_duo, 4))
    # avg_over40_trio = calculate_average(clean_up(over40_trio, 4))
    avg_over40_duo = calculate_average(over40_duo)
    avg_over40_trio = calculate_average(over40_trio)

    avg_over40 = (avg_over40_duo + avg_over40_trio) / 2 if avg_over40_trio is not None else avg_over40_duo
    end_time = time.time()
    execution_time = end_time - start_time
    print(f'over40 time: {execution_time}s')
    return avg_over40


def calculate_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, top=None, bot=None, bot_est=None, top_est=None):
    with open('1722505765_top600_heroes_data/lane_dict.txt', 'r') as f:
        heroes_data = json.load(f)
    output = {}
    lane_2vs2(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, output)
    if 'top' not in output:
        top2vs1 = lane_2vs1(radiant=radiant_heroes_and_pos, dire=dire_heroes_and_pos,
                            heroes_data=heroes_data, lane='top')
        if top2vs1:
            if len(top2vs1) == 2 and len(top2vs1['top_radiant']['win']) == 2 and len(top2vs1['top_dire']['win']) == 2:
                top2vs1['top_dire']['draw'] = multiply_list(top2vs1['top_dire']['draw'])
                top2vs1['top_dire']['win'] = multiply_list(top2vs1['top_dire']['win'])
                top2vs1['top_dire']['loose'] = multiply_list(top2vs1['top_dire']['loose'])
                top2vs1['top_dire']['win_stomp'] = multiply_list(top2vs1['top_dire']['win_stomp'])
                top2vs1['top_dire']['loose_stomp'] = multiply_list(top2vs1['top_dire']['loose_stomp'])

                top2vs1['top_radiant']['draw'] = multiply_list(top2vs1['top_radiant']['draw'])
                top2vs1['top_radiant']['win'] = multiply_list(top2vs1['top_radiant']['win'])
                top2vs1['top_radiant']['loose'] = multiply_list(top2vs1['top_radiant']['loose'])
                top2vs1['top_radiant']['win_stomp'] = multiply_list(top2vs1['top_radiant']['win_stomp'])
                top2vs1['top_radiant']['loose_stomp'] = multiply_list(top2vs1['top_radiant']['loose_stomp'])

                radiant_draw = top2vs1['top_radiant']['draw'] * top2vs1['top_dire']['draw']
                radiant_win = top2vs1['top_radiant']['win'] * top2vs1['top_dire']['loose']
                radiant_loose = top2vs1['top_radiant']['loose'] * top2vs1['top_dire']['win']
                radiant_win_stomp = top2vs1['top_radiant']['win_stomp'] * top2vs1['top_dire']['loose_stomp']
                radiant_loose_stomp = top2vs1['top_radiant']['loose_stomp'] * top2vs1['top_dire']['win_stomp']

                if any(foo != 0.0 for foo in [radiant_draw, radiant_win, radiant_loose, radiant_win_stomp, radiant_loose_stomp]):
                    total = radiant_loose + radiant_draw + radiant_win + radiant_win_stomp + radiant_loose_stomp
                    output.setdefault('top', {}).setdefault('win', []).append(round(radiant_win / total * 100))
                    output.setdefault('top', {}).setdefault('loose', []).append(round(radiant_loose / total * 100))
                    output.setdefault('top', {}).setdefault('draw', []).append(round(radiant_draw / total * 100))
                    output.setdefault('top', {}).setdefault('win_stomp', []).append(round(radiant_win_stomp / total * 100))
                    output.setdefault('top', {}).setdefault('loose_stomp', []).append(round(radiant_loose_stomp / total * 100))
                    top_first_key, top_first_key_max_value, top_second_max_key, top_second_max_value = find_biggest_param(output['top'])
            # else:
            #     for key in top2vs1.keys():
            #         if 'radiant' in key:
            #             radiant_win = top2vs1[key]['win']
            #             radiant_loose = top2vs1[key]['loose']
            #             radiant_draw = top2vs1[key]['draw']
            #         else:
            #             radiant_win = top2vs1[key]['loose']
            #             radiant_loose = top2vs1[key]['win']
            #             radiant_draw = top2vs1[key]['draw']
    else:
        top_first_key, top_first_key_max_value, top_second_max_key, top_second_max_value = find_biggest_param(output['top'])
    if 'bot' not in output:
        bot2vs1 = lane_2vs1(radiant=radiant_heroes_and_pos, dire=dire_heroes_and_pos,
                            heroes_data=heroes_data, lane='bot')
        if bot2vs1:
            if len(bot2vs1) == 2 and len(bot2vs1['bot_radiant']['win']) == 2 and len(bot2vs1['bot_dire']['win']) == 2:
                bot2vs1['bot_dire']['draw'] = multiply_list(bot2vs1['bot_dire']['draw'])
                bot2vs1['bot_dire']['win'] = multiply_list(bot2vs1['bot_dire']['win'])
                bot2vs1['bot_dire']['loose'] = multiply_list(bot2vs1['bot_dire']['loose'])
                bot2vs1['bot_dire']['win_stomp'] = multiply_list(bot2vs1['bot_dire']['win_stomp'])
                bot2vs1['bot_dire']['loose_stomp'] = multiply_list(bot2vs1['bot_dire']['loose_stomp'])

                bot2vs1['bot_radiant']['draw'] = multiply_list(bot2vs1['bot_radiant']['draw'])
                bot2vs1['bot_radiant']['win'] = multiply_list(bot2vs1['bot_radiant']['win'])
                bot2vs1['bot_radiant']['loose'] = multiply_list(bot2vs1['bot_radiant']['loose'])
                bot2vs1['bot_radiant']['win_stomp'] = multiply_list(bot2vs1['bot_radiant']['win_stomp'])
                bot2vs1['bot_radiant']['loose_stomp'] = multiply_list(bot2vs1['bot_radiant']['loose_stomp'])

                radiant_draw = bot2vs1['bot_radiant']['draw'] * bot2vs1['bot_dire']['draw']
                radiant_win = bot2vs1['bot_radiant']['win'] * bot2vs1['bot_dire']['loose']
                radiant_loose = bot2vs1['bot_radiant']['loose'] * bot2vs1['bot_dire']['win']
                radiant_win_stomp = bot2vs1['bot_radiant']['win_stomp'] * bot2vs1['bot_dire']['loose_stomp']
                radiant_loose_stomp = bot2vs1['bot_radiant']['loose_stomp'] * bot2vs1['bot_dire']['win_stomp']

                if any(foo != 0.0 for foo in
                       [radiant_draw, radiant_win, radiant_loose, radiant_win_stomp, radiant_loose_stomp]):
                    total = radiant_loose + radiant_draw + radiant_win + radiant_win_stomp + radiant_loose_stomp
                    output.setdefault('bot', {}).setdefault('win', []).append(round(radiant_win / total * 100))
                    output.setdefault('bot', {}).setdefault('loose', []).append(round(radiant_loose / total * 100))
                    output.setdefault('bot', {}).setdefault('draw', []).append(round(radiant_draw / total * 100))
                    output.setdefault('bot', {}).setdefault('win_stomp', []).append(round(radiant_win_stomp / total * 100))
                    output.setdefault('bot', {}).setdefault('loose_stomp', []).append(
                        round(radiant_loose_stomp / total * 100))

                    bot_first_key, bot_first_key_max_value, bot_second_max_key, bot_second_max_value = find_biggest_param(output['bot'])
            # else:
            #     for key in top2vs1.keys():
            #         if 'radiant' in key:
            #             radiant_win = top2vs1[key]['win']
            #             radiant_loose = top2vs1[key]['loose']
            #             radiant_draw = top2vs1[key]['draw']
            #         else:
            #             radiant_win = top2vs1[key]['loose']
            #             radiant_loose = top2vs1[key]['win']
            #             radiant_draw = top2vs1[key]['draw']
    else:
        bot_first_key, bot_first_key_max_value, bot_second_max_key, bot_second_max_value = find_biggest_param(output['bot'])
    mid_output = lane_2vs1(radiant=radiant_heroes_and_pos, dire=dire_heroes_and_pos,
                            heroes_data=heroes_data, lane='mid')
    # if 'bot' not in output:
    #     lane_report_def(my_team=radiant_heroes_and_pos, enemy_team=dire_heroes_and_pos,
    #                     heroes_data=heroes_data, lane='bot', side='radiant', output=output)
    #     lane_report_def(my_team=dire_heroes_and_pos, enemy_team=radiant_heroes_and_pos,
    #                     heroes_data=heroes_data, lane='bot', side='dire', output=output)
    #     if len(output['bot']) == 2:
    #         win = output['bot']['radiant']['win'][0] * output['bot']['dire']['loose'][0]
    #         loose = output['bot']['radiant']['loose'][0] * output['bot']['dire']['win'][0]
    #         draw = output['bot']['radiant']['draw'][0] * output['bot']['dire']['draw'][0]
    #         total = win + loose + draw
    #         win = win / total
    #         loose = loose / total
    #         draw = draw / total
    #         bot_est = find_biggest_param({'win': [win], 'loose': [loose], 'draw': [draw]})
    # if 'top' not in output:
    #     lane_report_def(my_team=radiant_heroes_and_pos, enemy_team=dire_heroes_and_pos,
    #                     heroes_data=heroes_data, lane='top', side='radiant', output=output)
    #     lane_report_def(my_team=dire_heroes_and_pos, enemy_team=radiant_heroes_and_pos,
    #                     heroes_data=heroes_data, lane='top', side='dire', output=output)
    #     if len(output['top']) == 2:
    #         win = output['top']['radiant']['win'][0] * output['top']['dire']['loose'][0]
    #         loose = output['top']['radiant']['loose'][0] * output['top']['dire']['win'][0]
    #         draw = output['top']['radiant']['draw'][0] * output['top']['dire']['draw'][0]
    #         total = win + loose + draw
    #         win = win / total
    #         loose = loose / total
    #         draw = draw / total
    #         top_est = find_biggest_param({'win': [win], 'loose': [loose], 'draw': [draw]})
    #         pass
    # mid_lane_report(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, 'radiant', output)
    try:
        mid_first_key, mid_first_key_max_value, mid_second_max_key, mid_second_max_value = find_biggest_param(mid_output['mid_radiant'])
    except KeyError:
        mid = None
    message = f'Radiant до 10-15 минут:\n'
    try:
        message += f'Top: {top_first_key} {top_first_key_max_value}%, {top_second_max_key} {top_second_max_value}%\n'
    except:
        message += 'Top: None\n'
    try:
        message += f'Mid: {mid_first_key} {mid_first_key_max_value}%, {mid_second_max_key} {mid_second_max_value}%\n'
    except:
        message += 'Mid: None\n'
    try:
        message += f'Bot: {bot_first_key} {bot_first_key_max_value}%, {bot_second_max_key} {bot_second_max_value}%\n'
    except:
        message += 'Bot: None\n'
    return message


def new_lane_report_def(radiant, dire, heroes_data):
    name = f"{radiant['pos1']['hero_id']},{radiant['pos5']['hero_id']}_vs_" \
           f"{dire['pos3']['hero_id']},{dire['pos4']['hero_id']}"
    data = heroes_data.get(name, {})
    if len(data) > 0:
        pass
    data = heroes_data.get(f"{radiant['pos3']['hero_id']},{radiant['pos4']['hero_id']}_vs_"
                           f"{dire['pos1']['hero_id']},{dire['pos5']['hero_id']}", {})
    if len(data) > 0:
        pass
    data = heroes_data.get(f"{radiant['pos2']['hero_id']}_vs_{dire['pos2']['hero_id']}", {})
    if len(data) > 0:
        pass
    pass


def find_biggest_param(data, equal_key=None):
    sorted_keys = sorted(data, key=lambda k: data[k][0], reverse=True)
    second_max_key = sorted_keys[1]
    second_max_value = round(data[second_max_key][0])
    first_key = sorted_keys[0]
    first_key_max_value = round(data[first_key][0])
    return(first_key, first_key_max_value, second_max_key, second_max_value)

def lane_2vs2(radiant, dire, heroes_data, output):
    data_2vs2 = heroes_data['2v2_lanes']

    bot_lane = f'{radiant["pos1"]["hero_id"]},{radiant["pos5"]["hero_id"]}_vs_' \
               f'{dire["pos3"]["hero_id"]},{dire["pos4"]["hero_id"]}',
    top_lane = f'{radiant["pos3"]["hero_id"]},{radiant["pos4"]["hero_id"]}_vs_' \
               f'{dire["pos1"]["hero_id"]},{dire["pos5"]["hero_id"]}'
    for lane, key in [[top_lane, 'top'], [bot_lane, 'bot']]:
        value = data_2vs2.get(lane, {}).get('value', [])
        if len(value) > 2:

            loose_stomp = value.count(-2) / (len(value))
            loose = value.count(-1) / (len(value))
            draw = value.count(0) / (len(value))
            win = value.count(1) / (len(value))
            win_stomp = value.count(2) / (len(value))

            output.setdefault(key, {}).setdefault('loose', []).append(loose)
            output.setdefault(key, {}).setdefault('draw', []).append(draw)
            output.setdefault(key, {}).setdefault('win', []).append(win)
            output.setdefault(key, {}).setdefault('win_stomp', []).append(win_stomp)
            output.setdefault(key, {}).setdefault('loose_stomp', []).append(loose_stomp)

def lane_2vs1(radiant, dire, heroes_data, lane):
    heroes_data = heroes_data['2v1_lanes']
    output = {}
    if lane == 'bot':
        for key in [
            f'{radiant["pos1"]["hero_id"]},{radiant["pos5"]["hero_id"]}_vs_{dire["pos3"]["hero_id"]}',
            f'{radiant["pos1"]["hero_id"]},{radiant["pos5"]["hero_id"]}_vs_'
            f'{dire["pos4"]["hero_id"]}']:
            value = heroes_data.get(key, {}).get('value', [])
            if len(value) > 4:
                loose_stomp = value.count(-2) / (len(value))
                loose = value.count(-1) / (len(value))
                draw = value.count(0) / (len(value))
                win = value.count(1) / (len(value))
                win_stomp = value.count(2) / (len(value))
                output.setdefault('bot_radiant', {}).setdefault('loose', []).append(loose)
                output.setdefault('bot_radiant', {}).setdefault('draw', []).append(draw)
                output.setdefault('bot_radiant', {}).setdefault('win', []).append(win)
                output.setdefault('bot_radiant', {}).setdefault('win_stomp', []).append(win_stomp)
                output.setdefault('bot_radiant', {}).setdefault('loose_stomp', []).append(loose_stomp)
        for key in [
            f'{dire["pos3"]["hero_id"]},{dire["pos4"]["hero_id"]}_vs_{radiant["pos1"]["hero_id"]}',
            f'{dire["pos3"]["hero_id"]},{dire["pos4"]["hero_id"]}_vs_'
            f'{radiant["pos5"]["hero_id"]}']:
            value = heroes_data.get(key, {}).get('value', [])
            if len(value) > 4:
                loose_stomp = value.count(-2) / (len(value))
                loose = value.count(-1) / (len(value))
                draw = value.count(0) / (len(value))
                win = value.count(1) / (len(value))
                win_stomp = value.count(2) / (len(value))
                output.setdefault('bot_dire', {}).setdefault('loose', []).append(loose)
                output.setdefault('bot_dire', {}).setdefault('draw', []).append(draw)
                output.setdefault('bot_dire', {}).setdefault('win', []).append(win)
                output.setdefault('bot_dire', {}).setdefault('win_stomp', []).append(win_stomp)
                output.setdefault('bot_dire', {}).setdefault('loose_stomp', []).append(loose_stomp)
    elif lane == 'top':
        for key in [
            f'{radiant["pos3"]["hero_id"]},{radiant["pos4"]["hero_id"]}_vs_{dire["pos1"]["hero_id"]}',
            f'{radiant["pos3"]["hero_id"]},{radiant["pos4"]["hero_id"]}_vs_'
            f'{dire["pos5"]["hero_id"]}']:
            value = heroes_data.get(key, {}).get('value', [])
            if len(value) > 4:
                loose_stomp = value.count(-2) / (len(value))
                loose = value.count(-1) / (len(value))
                draw = value.count(0) / (len(value))
                win = value.count(1) / (len(value))
                win_stomp = value.count(2) / (len(value))
                output.setdefault('top_radiant', {}).setdefault('loose', []).append(loose)
                output.setdefault('top_radiant', {}).setdefault('draw', []).append(draw)
                output.setdefault('top_radiant', {}).setdefault('win', []).append(win)
                output.setdefault('top_radiant', {}).setdefault('win_stomp', []).append(win_stomp)
                output.setdefault('top_radiant', {}).setdefault('loose_stomp', []).append(loose_stomp)
        for key in [
            f'{dire["pos1"]["hero_id"]},{dire["pos5"]["hero_id"]}_vs_{radiant["pos3"]["hero_id"]}',
            f'{dire["pos1"]["hero_id"]},{dire["pos5"]["hero_id"]}_vs_'
            f'{radiant["pos4"]["hero_id"]}']:
            value = heroes_data.get(key, {}).get('value', [])
            if len(value) > 4:
                loose_stomp = value.count(-2) / (len(value))
                loose = value.count(-1) / (len(value))
                draw = value.count(0) / (len(value))
                win = value.count(1) / (len(value))
                win_stomp = value.count(2) / (len(value))
                output.setdefault('top_dire', {}).setdefault('loose', []).append(loose)
                output.setdefault('top_dire', {}).setdefault('draw', []).append(draw)
                output.setdefault('top_dire', {}).setdefault('win', []).append(win)
                output.setdefault('top_dire', {}).setdefault('win_stomp', []).append(win_stomp)
                output.setdefault('top_dire', {}).setdefault('loose_stomp', []).append(loose_stomp)
    elif lane == 'mid':
        for key, side in [[f'{radiant["pos2"]["hero_id"]}_vs_{dire["pos2"]["hero_id"]}', 'radiant'],
            [f'{dire["pos2"]["hero_id"]}_vs_{radiant["pos2"]["hero_id"]}', 'dire']]:
            value = heroes_data.get(key, {}).get('value', [])
            if len(value) > 4:
                if side == 'radiant':
                    loose_stomp = value.count(-2) / (len(value))*100
                    loose = value.count(-1) / (len(value))*100
                    draw = value.count(0) / (len(value))*100
                    win = value.count(1) / (len(value))*100
                    win_stomp = value.count(2) / (len(value))*100
                    output.setdefault('mid_radiant', {}).setdefault('loose', []).append(loose)
                    output.setdefault('mid_radiant', {}).setdefault('draw', []).append(draw)
                    output.setdefault('mid_radiant', {}).setdefault('win', []).append(win)
                    output.setdefault('mid_radiant', {}).setdefault('win_stomp', []).append(win_stomp)
                    output.setdefault('mid_radiant', {}).setdefault('loose_stomp', []).append(loose_stomp)
                else:
                    loose_stomp = value.count(-2) / (len(value))*100
                    loose = value.count(-1) / (len(value))*100
                    draw = value.count(0) / (len(value))*100
                    win = value.count(1) / (len(value))*100
                    win_stomp = value.count(2) / (len(value))*100
                    output.setdefault('mid_dire', {}).setdefault('loose', []).append(loose)
                    output.setdefault('mid_dire', {}).setdefault('draw', []).append(draw)
                    output.setdefault('mid_dire', {}).setdefault('win', []).append(win)
                    output.setdefault('mid_dire', {}).setdefault('win_stomp', []).append(win_stomp)
                    output.setdefault('mid_dire', {}).setdefault('loose_stomp', []).append(loose_stomp)
    return output

    # if 'bot_dire' in output:
    #     output['bot_dire']['draw'] = sum(output['bot_dire']['draw'])/len(output['bot_dire']['draw'])
    #     output['bot_dire']['win'] = sum(output['bot_dire']['win']) / len(output['bot_dire']['win'])
    #     output['bot_dire']['loose'] = sum(output['bot_dire']['loose']) / len(output['bot_dire']['loose'])
    # if 'bot_radiant' in output:
    #     output['bot_radiant']['draw'] = sum(output['bot_radiant']['draw'])/len(output['bot_radiant']['draw'])
    #     output['bot_radiant']['win'] = sum(output['bot_radiant']['win']) / len(output['bot_radiant']['win'])
    #     output['bot_radiant']['loose'] = sum(output['bot_radiant']['loose']) / len(output['bot_radiant']['loose'])
    # if 'top_dire' in output:
    #     output['top_dire']['draw'] = sum(output['top_dire']['draw'])/len(output['top_dire']['draw'])
    #     output['top_dire']['win'] = sum(output['top_dire']['win']) / len(output['top_dire']['win'])
    #     output['top_dire']['loose'] = sum(output['top_dire']['loose']) / len(output['top_dire']['loose'])
    # if 'top_radiant' in output:
    #     output['top_radiant']['draw'] = sum(output['top_radiant']['draw']) / len(output['top_radiant']['draw'])
    #     output['top_radiant']['win'] = sum(output['top_radiant']['win']) / len(output['top_radiant']['win'])
    #     output['top_radiant']['loose'] = sum(output['top_radiant']['loose']) / len(output['top_radiant']['loose'])
    # try:
    #     top_win = output['top_radiant']['win']* output['top_dire']['loose']
    #     top_loose = output['top_dire']['win']* output['top_radiant']['loose']
    #     top_draw = output['top_dire']['draw']* output['top_radiant']['draw']
    #     total = top_draw+top_loose+top_win
    #     top_win=top_win/total
    #     top_loose=top_loose/total
    #     top_draw=top_draw/total
    #
    #     bot_win = output['bot_radiant']['win'] * output['bot_dire']['loose']
    #     bot_loose = output['bot_dire']['win'] * output['bot_radiant']['loose']
    #     bot_draw = output['bot_dire']['draw'] * output['bot_radiant']['draw']
    #     total = bot_loose + bot_win + bot_draw
    #     bot_win = bot_win / total
    #     bot_loose = bot_loose / total
    #     bot_draw = bot_draw / total
    #     return output
    # except: pass


def multiply_list(list, result=1):
    result = 1
    for num in list:
        result *= num
    return result


def mid_lane_report(my_team, enemy_team, heroes_data, side, output):
    heroes_data = heroes_data['value']
    # print('lane_report')
    start_time = time.time()
    avg_kills, avg_time, team_line_report, over40, over40, over40, over50, over55 = [], [], [], [], [], [], [], []
    copy_team_pos_and_heroes = {data['hero_id']: pos for pos, data in my_team.items()}

    data = heroes_data[str(my_team['pos2']['hero_id'])]['pos2']
    team_mate_data = data.get('against_hero', {}).get(str(enemy_team['pos2']['hero_id']), {}).get('pos2', {})
    if len(team_mate_data) > 5:
        total = team_mate_data.count(1) + team_mate_data.count(0) + team_mate_data.count(2)
        output.setdefault('mid', {}).setdefault(side, {}).setdefault('loose', []).append(
            team_mate_data.count(0) / total)
        output.setdefault('mid', {}).setdefault(side, {}).setdefault('draw', []).append(
            team_mate_data.count(2) / total)
        output.setdefault('mid', {}).setdefault(side, {}).setdefault('win', []).append(
            team_mate_data.count(1) / total)
    else:
        dire_data = heroes_data[str(enemy_team['pos2']['hero_id'])]['pos2']['solo']['value']
        radiant_wins = data['solo']['value'].count(1) / len(data['solo']['value'])
        radiant_loose = data['solo']['value'].count(0) / len(data['solo']['value'])
        radiant_draw = data['solo']['value'].count(2) / len(data['solo']['value'])
        dire_wins = dire_data.count(1) / len(dire_data)
        dire_loose = dire_data.count(0) / len(dire_data)
        dire_draw = dire_data.count(2) / len(dire_data)
        win = radiant_wins * dire_loose
        loose = radiant_loose * dire_wins
        draw = radiant_draw * dire_draw
        total = win + loose + draw
        output.setdefault('mid', {}).setdefault(side, {}).setdefault('loose', []).append(
            loose / total)
        output.setdefault('mid', {}).setdefault(side, {}).setdefault('draw', []).append(
            draw / total)
        output.setdefault('mid', {}).setdefault(side, {}).setdefault('win', []).append(
            win / total)
        pass


def lane_report_def(my_team, enemy_team, heroes_data, side, output, mid=None, lane=None):
    heroes_data = heroes_data['value']
    # print('lane_report')
    start_time = time.time()
    avg_kills, avg_time, team_line_report, over40, over40, over40, over50, over55 = [], [], [], [], [], [], [], []
    copy_team_pos_and_heroes = {data['hero_id']: pos for pos, data in my_team.items()}
    for hero_id in copy_team_pos_and_heroes:
        pos = copy_team_pos_and_heroes[hero_id]
        data = heroes_data[str(hero_id)]
        if side == 'radiant':
            if lane == 'bot':
                if pos == 'pos1':
                    team_mate_hero_id = str(my_team['pos5']['hero_id'])
                    team_mate_data = data.get(pos, {}).get('with_hero', {}).get(team_mate_hero_id, {}).get('pos5', {})
                    if len(team_mate_data) > 6:
                        total = team_mate_data.count(1) + team_mate_data.count(0) + team_mate_data.count(2)
                        output.setdefault(lane, {}).setdefault(side, {}).setdefault('loose', []).append(
                            team_mate_data.count(0) / total)
                        output.setdefault(lane, {}).setdefault(side, {}).setdefault('draw', []).append(
                            team_mate_data.count(2) / total)
                        output.setdefault(lane, {}).setdefault(side, {}).setdefault('win', []).append(
                            team_mate_data.count(1) / total)
            if lane == 'top':
                if pos == 'pos3':
                    team_mate_hero_id = str(my_team['pos4']['hero_id'])
                    team_mate_data = data.get(pos, {}).get('with_hero', {}).get(team_mate_hero_id, {}).get('pos4', {})
                    if len(team_mate_data) > 6:
                        total = team_mate_data.count(1) + team_mate_data.count(0) + team_mate_data.count(2)
                        output.setdefault(lane, {}).setdefault(side, {}).setdefault('loose', []).append(
                            team_mate_data.count(0) / total)
                        output.setdefault(lane, {}).setdefault(side, {}).setdefault('draw', []).append(
                            team_mate_data.count(2) / total)
                        output.setdefault(lane, {}).setdefault(side, {}).setdefault('win', []).append(
                            team_mate_data.count(1) / total)
        elif side == 'dire':
            if lane == 'bot':
                if pos == 'pos3':
                    team_mate_hero_id = str(my_team['pos4']['hero_id'])
                    team_mate_data = \
                        data.get(pos, {}).get('with_hero', {}).get(team_mate_hero_id, {}).get('pos4', {})
                    if len(team_mate_data) >= 6:
                        total = team_mate_data.count(1) + team_mate_data.count(0) + team_mate_data.count(2)
                        output.setdefault(lane, {}).setdefault(side, {}).setdefault('loose', []).append(
                            team_mate_data.count(0) / total)
                        output.setdefault(lane, {}).setdefault(side, {}).setdefault('draw', []).append(
                            team_mate_data.count(2) / total)
                        output.setdefault(lane, {}).setdefault(side, {}).setdefault('win', []).append(
                            team_mate_data.count(1) / total)
            elif lane == 'top':
                if pos == 'pos1':
                    team_mate_hero_id = str(my_team['pos5']['hero_id'])
                    team_mate_data = data.get(pos, {}).get('with_hero', {}).get(team_mate_hero_id, {}).get('pos5', {})
                    if len(team_mate_data) >= 6:
                        total = team_mate_data.count(1) + team_mate_data.count(0) + team_mate_data.count(2)
                        output.setdefault(lane, {}).setdefault(side, {}).setdefault('loose', []).append(
                            team_mate_data.count(0) / total)
                        output.setdefault(lane, {}).setdefault(side, {}).setdefault('draw', []).append(
                            team_mate_data.count(2) / total)
                        output.setdefault(lane, {}).setdefault(side, {}).setdefault('win', []).append(
                            team_mate_data.count(1) / total)
        else:
            if pos == 'pos1':
                team_mate_hero_id = str(my_team['pos5']['hero_id'])
                team_mate_data = data.get(pos, {}).get('with_hero', {}).get(team_mate_hero_id, {}).get('pos5', {})
                if len(team_mate_data) >= 6:
                    total = team_mate_data.count(1) + team_mate_data.count(0) + team_mate_data.count(2)
                    output.setdefault(lane, {}).setdefault(side, {}).setdefault('loose', []).append(
                        team_mate_data.count(0) / total)
                    output.setdefault(lane, {}).setdefault(side, {}).setdefault('draw', []).append(
                        team_mate_data.count(2) / total)
                    output.setdefault(lane, {}).setdefault(side, {}).setdefault('win', []).append(
                        team_mate_data.count(1) / total)
            elif pos == 'pos3':
                team_mate_hero_id = str(my_team['pos4']['hero_id'])
                team_mate_data = data.get(pos, {}).get('with_hero', {}).get(team_mate_hero_id, {}).get('pos4', {})
                if len(team_mate_data) >= 6:
                    total = team_mate_data.count(1) + team_mate_data.count(0) + team_mate_data.count(2)
                    output.setdefault(lane, {}).setdefault(side, {}).setdefault('loose', []).append(
                        team_mate_data.count(0) / total)
                    output.setdefault(lane, {}).setdefault(side, {}).setdefault('draw', []).append(
                        team_mate_data.count(2) / total)
                    output.setdefault(lane, {}).setdefault(side, {}).setdefault('win', []).append(
                        team_mate_data.count(1) / total)


def tm_kills(radiant_heroes_and_positions, dire_heroes_and_positions):
    output_data = {'dire_kills_duo': [], 'dire_kills_trio': [], 'dire_time_duo': [], 'dire_time_trio': [],
                   'radiant_kills_duo': [], 'radiant_kills_trio': [], 'radiant_time_duo': [], 'radiant_time_trio': []}
    # print('tm_kills')
    positions = ['1', '2', '3', '4', '5']
    radiant_time_unique_combinations, radiant_kills_unique_combinations, dire_kills_unique_combinations, \
        dire_time_unique_combinations = set(), set(), set(), set()
    with open('./pro_heroes_data/total_time_kills_dict.txt', 'r') as f:
        data = json.load(f)['value']
    for pos in positions:
        # radiant_synergy
        hero_id = str(radiant_heroes_and_positions['pos' + pos]['hero_id'])
        time_data = data.get(hero_id, {}).get('pos' + pos, {}).get('total_time_duo', {})
        kills_data = data.get(hero_id, {}).get('pos' + pos, {}).get('total_kills_duo', {})
        for hero_data in [time_data, kills_data]:
            for pos2, item in radiant_heroes_and_positions.items():
                second_hero_id = str(item['hero_id'])
                if second_hero_id == hero_id:
                    continue
                duo_data = hero_data.get(second_hero_id, {}).get(pos2, {})
                if len(duo_data.get('value', {})) >= 2:
                    combo = tuple(sorted([hero_id, second_hero_id]))
                    if hero_data == time_data:
                        if combo not in radiant_time_unique_combinations:
                            radiant_time_unique_combinations.add(combo)
                            value = (sum(duo_data['value']) / len(duo_data['value'])) / 60
                            output_data['radiant_time_duo'].append(value)
                    elif hero_data == kills_data:
                        if combo not in radiant_kills_unique_combinations:
                            radiant_kills_unique_combinations.add(combo)
                            value = sum(duo_data['value']) / len(duo_data['value'])
                            output_data['radiant_kills_duo'].append(value)
                    # Третий герой
                    for pos3, item3 in radiant_heroes_and_positions.items():
                        third_hero_id = str(item3['hero_id'])
                        if third_hero_id not in [second_hero_id, hero_id]:
                            # Создаём отсортированный кортеж идентификаторов героев для уникальности
                            combo = tuple(sorted([hero_id, second_hero_id, third_hero_id]))
                            if hero_data == time_data:
                                if combo not in radiant_time_unique_combinations:
                                    radiant_time_unique_combinations.add(combo)
                                    trio_data = duo_data.get('total_time_trio', {}).get(third_hero_id, {}).get(pos3,
                                                                                                               {}).get(
                                        'value', {})
                                    if len(trio_data):
                                        value = (sum(trio_data) / len(trio_data)) / 60
                                        output_data['radiant_time_trio'].append(value)
                            elif hero_data == kills_data:
                                if combo not in radiant_kills_unique_combinations:
                                    radiant_kills_unique_combinations.add(combo)
                                    trio_data = duo_data.get('total_kills_trio', {}).get(third_hero_id, {}).get(pos3,
                                                                                                                {}).get(
                                        'value', {})
                                    if len(trio_data):
                                        value = sum(trio_data) / len(trio_data)
                                        output_data['radiant_kills_trio'].append(value)
        # dire_synergy
        hero_id = str(dire_heroes_and_positions['pos' + pos]['hero_id'])
        time_data = data.get(hero_id, {}).get('pos' + pos, {}).get('total_time_duo', {})
        kills_data = data.get(hero_id, {}).get('pos' + pos, {}).get('total_kills_duo', {})
        for hero_data in [time_data, kills_data]:
            for pos2, item in dire_heroes_and_positions.items():
                second_hero_id = str(item['hero_id'])
                if second_hero_id == hero_id:
                    continue
                duo_data = hero_data.get(second_hero_id, {}).get(pos2, {})
                if len(duo_data.get('value', {})) >= 2:
                    combo = tuple(sorted([hero_id, second_hero_id]))
                    if hero_data == time_data:
                        if combo not in dire_time_unique_combinations:
                            dire_time_unique_combinations.add(combo)
                            value = (sum(duo_data['value']) / len(duo_data['value'])) / 60
                            output_data['dire_time_duo'].append(value)
                    elif hero_data == kills_data:
                        if combo not in dire_kills_unique_combinations:
                            dire_kills_unique_combinations.add(combo)
                            value = sum(duo_data['value']) / len(duo_data['value'])
                            output_data['dire_kills_duo'].append(value)
                    # third_hero
                    for pos3, item3 in dire_heroes_and_positions.items():
                        third_hero_id = str(item3['hero_id'])
                        if third_hero_id not in [second_hero_id, hero_id]:
                            combo = tuple(sorted([hero_id, second_hero_id, third_hero_id]))
                            if hero_data == time_data:
                                if combo not in dire_time_unique_combinations:
                                    dire_time_unique_combinations.add(combo)
                                    trio_data = duo_data.get('total_time_trio', {}).get(third_hero_id, {}).get(pos3,
                                                                                                               {}).get(
                                        'value', {})
                                    if len(trio_data):
                                        value = (sum(trio_data) / len(trio_data)) / 60
                                        output_data['dire_time_trio'].append(value)
                            elif hero_data == kills_data:
                                if combo not in dire_kills_unique_combinations:
                                    dire_kills_unique_combinations.add(combo)
                                    trio_data = duo_data.get('total_kills_trio', {}).get(third_hero_id, {}).get(pos3,
                                                                                                                {}).get(
                                        'value', {})
                                    if len(trio_data):
                                        value = sum(trio_data) / len(trio_data)
                                        output_data['dire_kills_trio'].append(value)

    avg_time_trio = calculate_average(output_data['radiant_time_trio'] + output_data['dire_time_trio'])
    avg_kills_trio = calculate_average(output_data['radiant_kills_trio'] + output_data['dire_kills_trio'])
    avg_time_duo = calculate_average(output_data['radiant_time_duo'] + output_data['dire_time_duo'])
    avg_kills_duo = calculate_average(output_data['radiant_kills_duo'] + output_data['dire_kills_duo'])

    avg_kills = (avg_kills_trio + avg_kills_duo) / 2 if avg_kills_trio and avg_kills_duo else avg_kills_duo
    avg_time = (avg_time_duo + avg_time_trio) / 2 if avg_time_trio and avg_time_duo else avg_time_duo

    return round(avg_kills, 2), round(avg_time, 2)


def tm_kills_teams(radiant_heroes_and_pos, dire_heroes_and_pos, radiant_team_name, dire_team_name, min_len):
    # print('tm_kills')
    radiant_team_time_solo, radiant_team_kills_solo, dire_team_time_solo, dire_team_kills_solo = [], [], [], []
    output_data = {'dire_kills_duo': [], 'dire_kills_trio': [], 'dire_time_duo': [], 'dire_time_trio': [],
                   'radiant_kills_duo': [], 'radiant_kills_trio': [], 'radiant_time_duo': [], 'radiant_time_trio': []}
    positions = ['1', '2', '3', '4', '5']
    radiant_time_unique_combinations, radiant_kills_unique_combinations, dire_kills_unique_combinations, \
        dire_time_unique_combinations = set(), set(), set(), set()
    radiant_team_name = radiant_team_name.replace('team waska', 'waska')
    dire_team_name = dire_team_name.replace('team waska', 'waska')
    with open('./pro_heroes_data/total_time_kills_dict_teams.txt', 'r') as f:
        file_data = json.load(f)['teams']
    if radiant_team_name in file_data and dire_team_name in file_data:
        work_data = file_data[radiant_team_name]
        for pos in positions:
            # radiant_synergy
            hero_id = str(radiant_heroes_and_pos['pos' + pos]['hero_id'])
            data = work_data.get(hero_id, {}).get('pos' + pos, {})
            solo_time = data.get('solo_time', {}).get('value')
            if solo_time:
                radiant_team_time_solo.append(sum(solo_time) / len(solo_time))
            solo_kills = data.get('solo_kills', {}).get('value')
            if solo_kills:
                radiant_team_kills_solo.append(sum(solo_kills) / len(solo_kills))
            time_data = data.get('time_duo', {})
            kills_data = data.get('kills_duo', {})
            for hero_data in [time_data, kills_data]:
                for pos2, item in radiant_heroes_and_pos.items():
                    second_hero_id = str(item['hero_id'])
                    if second_hero_id == hero_id:
                        continue
                    duo_data = hero_data.get(second_hero_id, {}).get(pos2, {})
                    if len(duo_data.get('value', {})) >= min_len:
                        combo = tuple(sorted([hero_id, second_hero_id]))
                        if hero_data == time_data:
                            if combo not in radiant_time_unique_combinations:
                                radiant_time_unique_combinations.add(combo)
                                value = (sum(duo_data['value']) / len(duo_data['value'])) / 60
                                output_data['radiant_time_duo'].append(value)
                        elif hero_data == kills_data:
                            if combo not in radiant_kills_unique_combinations:
                                radiant_kills_unique_combinations.add(combo)
                                value = sum(duo_data['value']) / len(duo_data['value'])
                                output_data['radiant_kills_duo'].append(value)
                        # Третий герой
                        # for pos3, item3 in radiant_heroes_and_pos.items():
                        #     third_hero_id = str(item3['hero_id'])
                        #     if third_hero_id not in [second_hero_id, hero_id]:
                        #         # Создаём отсортированный кортеж идентификаторов героев для уникальности
                        #         combo = tuple(sorted([hero_id, second_hero_id, third_hero_id]))
                        #         if hero_data == time_data:
                        #             if combo not in radiant_time_unique_combinations:
                        #                 radiant_time_unique_combinations.add(combo)
                        #                 trio_data = duo_data.get('time_trio', {}).get(third_hero_id, {}).\
                        #                     get(pos3, {}).get('value', {})
                        #                 if len(trio_data):
                        #                     value = (sum(trio_data) / len(trio_data)) / 60
                        #                     output_data['radiant_time_trio'].append(value)
                        #         elif hero_data == kills_data:
                        #             if combo not in radiant_kills_unique_combinations:
                        #                 radiant_kills_unique_combinations.add(combo)
                        #                 trio_data = duo_data.get('kills_trio', {}).get(third_hero_id, {}).get(
                        #                     pos3, {}).get('value', {})
                        #                 if len(trio_data):
                        #                     value = sum(trio_data) / len(trio_data)
                        #                     output_data['radiant_kills_trio'].append(value)

        # dire_synergy
        dire_team_name.replace('g2.invictus gaming', 'g2 x ig')
        work_data = file_data[dire_team_name]
        for pos in positions:
            hero_id = str(dire_heroes_and_pos['pos' + pos]['hero_id'])
            data = work_data.get(hero_id, {}).get('pos' + pos, {})
            solo_time = data.get('solo_time', {}).get('value')
            if solo_time:
                dire_team_time_solo.append(sum(solo_time) / len(solo_time))
            solo_kills = data.get('solo_kills', {}).get('value')
            if solo_kills:
                dire_team_kills_solo.append(sum(solo_kills) / len(solo_kills))
            time_data = work_data.get(hero_id, {}).get('pos' + pos, {}).get('time_duo', {})
            kills_data = work_data.get(hero_id, {}).get('pos' + pos, {}).get('kills_duo', {})
            for hero_data in [time_data, kills_data]:
                for pos2, item in dire_heroes_and_pos.items():
                    second_hero_id = str(item['hero_id'])
                    if second_hero_id == hero_id:
                        continue
                    duo_data = hero_data.get(second_hero_id, {}).get(pos2, {})
                    if len(duo_data.get('value', {})) >= min_len:
                        combo = tuple(sorted([hero_id, second_hero_id]))
                        if hero_data == time_data:
                            if combo not in dire_time_unique_combinations:
                                dire_time_unique_combinations.add(combo)
                                value = (sum(duo_data['value']) / len(duo_data['value'])) / 60
                                output_data['dire_time_duo'].append(value)
                        elif hero_data == kills_data:
                            if combo not in dire_kills_unique_combinations:
                                dire_kills_unique_combinations.add(combo)
                                value = sum(duo_data['value']) / len(duo_data['value'])
                                output_data['dire_kills_duo'].append(value)
                        # third_hero
                        # for pos3, item3 in dire_heroes_and_pos.items():
                        #     third_hero_id = str(item3['hero_id'])
                        #     if third_hero_id not in [second_hero_id, hero_id]:
                        #         combo = tuple(sorted([hero_id, second_hero_id, third_hero_id]))
                        #         if hero_data == time_data:
                        #             if combo not in dire_time_unique_combinations:
                        #                 dire_time_unique_combinations.add(combo)
                        #                 trio_data = duo_data.get('time_trio', {}).get(third_hero_id, {}).get(
                        #                     pos3, {}).get('value', {})
                        #                 if len(trio_data):
                        #                     value = (sum(trio_data) / len(trio_data)) / 60
                        #                     output_data['dire_time_trio'].append(value)
                        #         elif hero_data == kills_data:
                        #             if combo not in dire_kills_unique_combinations:
                        #                 dire_kills_unique_combinations.add(combo)
                        #                 trio_data = duo_data.get('kills_trio', {}).get(third_hero_id, {}).get(
                        #                     pos3, {}).get('value', {})
                        #                 if len(trio_data):
                        #                     value = sum(trio_data) / len(trio_data)
                        #                     output_data['kills_trio'].append(value)
        solo_time = calculate_average(radiant_team_time_solo + dire_team_time_solo) / 60
        solo_kills = calculate_average(radiant_team_kills_solo + dire_team_kills_solo)
        # avg_time_trio = calculate_average(output_data['radiant_time_trio'] + output_data['dire_time_trio'])
        # avg_kills_trio = calculate_average(output_data['radiant_kills_trio'] + output_data['dire_kills_trio'])
        avg_time_duo = calculate_average(output_data['radiant_time_duo'] + output_data['dire_time_duo'])
        avg_kills_duo = calculate_average(output_data['radiant_kills_duo'] + output_data['dire_kills_duo'])

        avg_kills = avg_kills_duo
        avg_time = avg_time_duo

        return avg_kills, avg_time, solo_kills, solo_time
    else:
        if radiant_team_name not in file_data:
            send_message(f'{radiant_team_name} not in team list')
        if dire_team_name not in file_data:
            send_message(f'{dire_team_name} not in team list')
        return None, None, None, None
