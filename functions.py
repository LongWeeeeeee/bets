import datetime
import html
import json
import re
import time
import requests
from bs4 import BeautifulSoup
from id_to_name import translate, name_to_id
import pytz
import id_to_name
import keys
from id_to_name import pro_teams, translate
from keys import api_token_5
from maps_research import update_my_protracker



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
                    result = if_unique(url, score)
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


def add_url(url):
    with open('map_id_check.txt', 'r+') as f:
        data = json.load(f)
        data.append(url)
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


def format_output_dict(output_dict):
    over35_trio = output_dict.get('over35_trio', None)
    over35_1vs2 = output_dict.get('over35_1vs2', None)
    over35_duo = output_dict.get('over35_duo', None)
    over35_duo_counterpick = output_dict.get('over35_duo_counterpick', None)
    radiant_counterpick_1vs2 = output_dict.get('radiant_counterpick_1vs2', None)
    duo_diff = output_dict.get('duo_diff', None)
    radiant_synergy_trio = output_dict.get('radiant_synergy_trio', None)
    synergy_duo = output_dict.get('synergy_duo', None)
    pos1_matchup = output_dict.get('pos1_matchup', None)
    support_dif = output_dict.get('support_dif', None)
    over35_solo = output_dict.get('support_dif', None)
    if over35_solo and over35_solo < 3 and over35_solo > -3:
        over35_trio = None
    if over35_trio and over35_trio < 7 and over35_trio > -7:
        over35_trio = None
    if over35_1vs2 and over35_1vs2 > -8 and over35_1vs2 < 8:
        over35_1vs2 = None
    if over35_duo and over35_duo < 5 and over35_duo > -5:
        over35_duo = None
    if over35_duo_counterpick and over35_duo_counterpick < 6 and over35_duo_counterpick > -6:
        over35_duo_counterpick = None
    if radiant_counterpick_1vs2 and radiant_counterpick_1vs2 < 6 and radiant_counterpick_1vs2 > -6:
        radiant_counterpick_1vs2 = None
    if duo_diff and duo_diff > -8 and duo_diff < 8:
        duo_diff = None
    if radiant_synergy_trio and radiant_synergy_trio < 5 and radiant_synergy_trio > -5:
        radiant_synergy_trio = None
    if synergy_duo and synergy_duo > -13 and synergy_duo < 13:
        synergy_duo = None
    if pos1_matchup and pos1_matchup > -21 and pos1_matchup < 21:
        pos1_matchup = None
    if support_dif and support_dif > -14 and support_dif < 14:
        support_dif = None
    if any(i is not None for i in [support_dif, pos1_matchup, synergy_duo, radiant_synergy_trio, duo_diff,
            radiant_counterpick_1vs2, over35_duo_counterpick, over35_duo, over35_1vs2, over35_trio]):
        if None not in [radiant_synergy_trio, radiant_counterpick_1vs2]:
            if (radiant_synergy_trio > 0 and radiant_counterpick_1vs2 > 0) or (radiant_counterpick_1vs2 < 0 and radiant_synergy_trio < 0):
                return True
        if None not in [over35_1vs2, over35_trio]:
            if (over35_1vs2 > 0 and over35_trio > 0) or (over35_1vs2 < 0 and over35_trio < 0):
                return True
        else:
            return True


def get_map_players(data, match, soup, name_to_pos):
    radiant_pick = match.find('div', class_='picks__new-picks__picks radiant').find('div',
                                                                                    class_='items').find_all(
        'div', class_='pick player')
    dire_pick = match.find('div', class_='picks__new-picks__picks dire').find('div',
                                                                              class_='items').find_all(
        'div', class_='pick player')
    if not radiant_pick:
        return
    for player in radiant_pick:
        data_hero_id = player['data-hero-id']
        data_tippy_content = player['data-tippy-content']
        player_title = player.find('span', class_='pick__player-title').text.lower()
        data.setdefault('radiant', []).append(
            {'hero_id': data_hero_id, 'hero_name': data_tippy_content, 'player_name': player_title})
    if len(data['radiant']) != 5:
        return
    for player in dire_pick:
        data_hero_id = player['data-hero-id']
        data_tippy_content = player['data-tippy-content']
        player_title = player.find('span', class_='pick__player-title').text.lower()
        data.setdefault('dire', []).append(
            {'hero_id': data_hero_id, 'hero_name': data_tippy_content, 'player_name': player_title})
    if len(data['dire']) != 5:
        return
    teams = soup.find_all('div', class_='lineups__team-players')
    for team in teams:
        players = team.find_all('div', class_='player')
        for player in players:
            role_data = player.find('div', class_='player__role')
            if not role_data:
                return
            role = role_data.find('span').text
            role = name_to_pos[role]
            name = player.find('div', class_='player__name').find('div',
                                                                  class_='player__name-name').text.lower()
            for side in [data['radiant'], data['dire']]:
                for i in range(len(side)):
                    if side[i]['player_name'] == name:
                        side[i]['role'] = role
    roles = ['pos1', 'pos2', 'pos3', 'pos4', 'pos5']
    for player in data['radiant']:
        if 'role' in player:
            if player['role'] not in roles:
                return
            roles.remove(player['role'])
    if len(roles) == 1:
        for player in data['radiant']:
            if 'role' not in player:
                player['role'] = roles[0]
    roles = ['pos1', 'pos2', 'pos3', 'pos4', 'pos5']
    for player in data['dire']:
        if 'role' in player:
            if player['role'] not in roles:
                return
            roles.remove(player['role'])
    if len(roles) == 1:
        for player in data['dire']:
            if 'role' not in player:
                player['role'] = roles[0]

    radiant_heroes_and_pos = {
        player['role']: {'hero_name': player['hero_name'], 'hero_id': player['hero_id']} for player in
        data['radiant']}
    dire_heroes_and_pos = {
        player['role']: {'hero_name': player['hero_name'], 'hero_id': player['hero_id']} for
        player in data['dire']}

    if len(radiant_heroes_and_pos) != 5 or len(dire_heroes_and_pos) != 5:
        return
    radiant_team_name = data['teams']['radiant'].lower()
    dire_team_name = data['teams']['dire'].lower()
    radiant_heroes_and_pos = radiant_heroes_and_pos
    dire_heroes_and_pos = dire_heroes_and_pos
    return radiant_team_name, dire_team_name, radiant_heroes_and_pos, dire_heroes_and_pos


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
            key = f"{hero_id + pos}_with_{second_hero_id + second_pos}"
            foo = data.get(key, [])
            if len(foo) >= 30:
                combo = tuple(sorted([hero_id, second_hero_id]))
                if combo not in unique_combinations:
                    unique_combinations.add(combo)
                    value = foo.count(1) / (foo.count(1) + foo.count(0))
                    output[f'{mkdir}_duo'].append(value)
                    if all(p in ['pos4', 'pos5'] for p in (pos, second_pos)):
                        output[f'{mkdir}_support_duo'] = value
            for third_pos in heroes_and_pos:
                third_hero_id = str(heroes_and_pos[third_pos]['hero_id'])
                if third_hero_id in [second_hero_id, hero_id]:
                    continue
                third_hero_id = str(heroes_and_pos[third_pos]['hero_id'])
                key = f"{hero_id + pos},{second_hero_id + second_pos},{third_hero_id + third_pos}"
                foo = data.get(key, [])
                if len(foo) >= 15:
                    combo = tuple(sorted([hero_id, second_hero_id, third_hero_id]))
                    if combo not in unique_combinations:
                        unique_combinations.add(combo)
                        value = foo.count(1) / (foo.count(1) + foo.count(0))
                        output[f'{mkdir}_trio'].append(value)


def counterpick_team(heroes_and_pos, heroes_and_pos_opposite, output, mkdir, data1vs1, data1vs2, data1vs3, pos1_matchup=None):
    unique_combinations = set()
    for pos in heroes_and_pos:
        # if pos in ['pos4', 'pos5']: continue
        hero_id = str(heroes_and_pos[pos]['hero_id'])
        for enemy_pos in heroes_and_pos_opposite:
            enemy_hero_id = str(heroes_and_pos_opposite[enemy_pos]['hero_id'])
            key = f"{hero_id}{pos}_vs_{enemy_hero_id}{enemy_pos}"
            foo = data1vs1.get(key, {})
            if len(foo) >= 30:
                value = foo.count(1) / (foo.count(1) + foo.count(0))
                if enemy_pos == 'pos1' and pos == 'pos1' and mkdir == 'radiant_counterpick':
                    output['pos1_matchup']=value
                output[f'{mkdir}_1vs1'].append(value)
            for second_enemy_pos in heroes_and_pos_opposite:
                second_enemy_id = str(heroes_and_pos_opposite[second_enemy_pos]['hero_id'])
                if enemy_hero_id == second_enemy_id:
                    continue
                key = f"{hero_id}{pos}_vs_{enemy_hero_id}{enemy_pos}," \
                      f"{second_enemy_id}{second_enemy_pos}"
                foo = data1vs2.get(key, {})
                if len(foo) >= 15:
                    combo = (hero_id,) + tuple(sorted([enemy_hero_id, second_enemy_id]))
                    if combo not in unique_combinations:
                        unique_combinations.add(combo)
                        value = foo.count(1) / (foo.count(1) + foo.count(0))
                        if pos in ['pos4', 'pos5']: continue
                        output[f'{mkdir}_1vs2'].append(value)
                # for third_enemy_pos in heroes_and_pos_opposite:
                #     third_enemy_id = str(heroes_and_pos_opposite[third_enemy_pos]['hero_id'])
                #     if third_enemy_id in [enemy_hero_id, second_enemy_id]:
                #         continue
                #     key = f"{hero_id}{pos},{enemy_hero_id}{enemy_pos}," \
                #           f"{second_enemy_id}{second_enemy_pos},{third_enemy_id}{third_enemy_pos}"
                #     foo = data1vs3.get(key, {})
                #     if len(foo) >= 6:
                #         combo = (hero_id,) + tuple(sorted([enemy_hero_id, second_enemy_id, third_enemy_id]))
                #         if combo not in unique_combinations:
                #             unique_combinations.add(combo)
                #             value = foo.count(1) / (foo.count(1) + foo.count(0))
                #             output[f'{mkdir}_1vs3'].append(value)

def synergy_and_counterpick_new(radiant_heroes_and_pos, dire_heroes_and_pos, synergy_data, data1vs2, data1vs1, data1vs3):
    synergy_duo, duo_diff, pos1_matchup, pos1_matchup_out, support_dif = None, None, None, None, None
    output = {'radiant_synergy_duo': [], 'dire_synergy_duo': [], 'radiant_synergy_trio': [], 'dire_synergy_trio': [],
              'radiant_counterpick_1vs1': [], 'dire_counterpick_1vs1': [], 'radiant_counterpick_1vs2': [],
              'dire_counterpick_1vs2': [], 'pos1_matchup': None, 'radiant_synergy_support_duo': None,
              'dire_synergy_support_duo': None}

    synergy_team(radiant_heroes_and_pos, output, 'radiant_synergy', synergy_data)
    synergy_team(dire_heroes_and_pos, output, 'dire_synergy', synergy_data)

    counterpick_team(heroes_and_pos=radiant_heroes_and_pos, heroes_and_pos_opposite=dire_heroes_and_pos,
                     output=output, mkdir='radiant_counterpick', data1vs2=data1vs2,
                     data1vs1=data1vs1, data1vs3=data1vs3)
    counterpick_team(heroes_and_pos=dire_heroes_and_pos, heroes_and_pos_opposite=radiant_heroes_and_pos,
                     output=output, mkdir='dire_counterpick', data1vs2=data1vs2,
                     data1vs1=data1vs1, data1vs3=data1vs3)
    def get_diff(radiant, dire):
        if None not in [radiant, dire]:
            if any(len(foo) > 3 for foo in [radiant, dire]):
                if len(radiant) > 2 and len(dire) > 2:
                    return round((sum(radiant)/len(radiant) - sum(dire)/len(dire))*100)

    radiant_counterpick_1vs2 = get_diff(output['radiant_counterpick_1vs2'], output['dire_counterpick_1vs2'])
    radiant_synergy_trio = get_diff(output['radiant_synergy_trio'], output['dire_synergy_trio'])
    duo_diff = get_diff(output['radiant_counterpick_1vs1'], output['dire_counterpick_1vs1'])
    synergy_duo = get_diff(output['radiant_synergy_duo'], output['dire_synergy_duo'])
    if output['pos1_matchup']:
        pos1_matchup = round(output['pos1_matchup'] * 100 - 50)
    if all(i is not None for i in [output['radiant_synergy_support_duo'], output['dire_synergy_support_duo']]):
        support_dif = round((output['radiant_synergy_support_duo'] - output['dire_synergy_support_duo'])*100)
    return synergy_duo, radiant_synergy_trio, duo_diff, radiant_counterpick_1vs2, pos1_matchup, support_dif


def get_multiplied_results(radiant, dire, radiant_new=1, dire_new =1):
    if all(foo is not None and len(foo)>0 for foo in (radiant, dire)):
        for i in radiant:
            radiant_new *= i
        for i in dire:
            dire_new *= i
        total = (radiant_new + dire_new)
        if total == 0:
            return
        return round(radiant_new / total * 100 - 50)
def get_ordinar_results(radiant, dire):
    if all(foo is not None and len(foo) > 2 for foo in (radiant, dire)):
        return round((sum(radiant)/len(radiant) - sum(dire)/len(dire))*100)

def calculate_over35(radiant_heroes_and_pos, dire_heroes_and_pos, data, over35_duo=None, over35_duo_counterpick=None, over35_solo=None):
    radiant_counterpick_duo, r_winrate_1vs2 =\
        over35_counter(radiant_heroes_and_pos, dire_heroes_and_pos, data)
    dire_counterpick_duo, d_winrate_1vs2 =\
        over35_counter(dire_heroes_and_pos, radiant_heroes_and_pos, data)
    radiant_over35_duo, r_solo, r_trio = avg_over35(radiant_heroes_and_pos, data)
    dire_over35_duo, d_solo, d_trio = avg_over35(dire_heroes_and_pos, data)

    over35_solo = get_ordinar_results(radiant=r_solo, dire=d_solo)
    over35_trio = get_ordinar_results(radiant=r_trio, dire=d_trio)
    over35_1vs2 = get_ordinar_results(radiant=r_winrate_1vs2, dire=d_winrate_1vs2)
    over35_duo = get_ordinar_results(radiant=radiant_over35_duo, dire=dire_over35_duo)
    over35_duo_counterpick = get_ordinar_results(radiant=radiant_counterpick_duo, dire=dire_counterpick_duo)
    return over35_duo, over35_duo_counterpick, over35_1vs2, over35_solo, over35_trio


def over35_counter(heroes_and_pos, heroes_and_pos_opposite, data):
    unique_combinations = set()
    winrate_1vs1, winrate1vs2_cores, winrate1vs2_sups, winrate1vs2 = [], [], [], []
    for pos in heroes_and_pos:
        # if pos in ['pos4', 'pos5']: continue
        hero_id = str(heroes_and_pos[pos]['hero_id'])
        for enemy_pos in heroes_and_pos_opposite:
            enemy_hero_id = str(heroes_and_pos_opposite[enemy_pos]['hero_id'])
            key = f"{hero_id}{pos}_vs_{enemy_hero_id}{enemy_pos}"
            foo = data.get(key, {})
            if len(foo) >= 20:
                value = foo.count(1) / (foo.count(1) + foo.count(0))
                winrate_1vs1.append(value)
            for second_enemy_pos in heroes_and_pos_opposite:
                second_enemy_id = str(heroes_and_pos_opposite[second_enemy_pos]['hero_id'])
                if enemy_hero_id == second_enemy_id:
                    continue

                key = f"{hero_id}{pos}_vs_{enemy_hero_id}{enemy_pos},{second_enemy_id}{second_enemy_pos}"
                foo = data.get(key, {})

                if len(foo) >= 15:
                    combo = (hero_id,) + tuple(sorted([enemy_hero_id, second_enemy_id]))
                    if combo not in unique_combinations:
                        unique_combinations.add(combo)
                        value = foo.count(1) / (foo.count(1) + foo.count(0))
                        winrate1vs2.append(value)

    return winrate_1vs1, winrate1vs2


def check_bad_map(match, maps_data=None, break_flag=False):
    dire_heroes_and_pos = {}
    radiant_heroes_and_pos = {}
    if type(match) is str:
        match = maps_data[match]
        players = match['players']
        for player in players:
            hero_id = player['hero']['id']
            position = player['position']
            position_key = f'pos{position[-1]}'
            if player.get('isRadiant'):
                radiant_heroes_and_pos.setdefault(position_key, {}).setdefault('hero_id', hero_id)
            else:
                dire_heroes_and_pos.setdefault(position_key, {}).setdefault('hero_id', hero_id)
    else:
        if len(dire_heroes_and_pos) == 0:
            dire_heroes_and_pos = match['dire_heroes_and_pos']
        new_dict = {}
        for key in dire_heroes_and_pos:
            hero_name = dire_heroes_and_pos[key]['hero_name'].lower()
            for hero in name_to_id:
                if hero.lower() == hero_name:
                    dire_heroes_and_pos[key]['hero_id'] = name_to_id[hero]
            if 'hero_id' not in dire_heroes_and_pos[key]:
                break_flag = True
                break
            new_key = key.replace(' ', '')
            new_dict[new_key] = dire_heroes_and_pos[key]
        dire_heroes_and_pos = new_dict
        if len(radiant_heroes_and_pos) == 0:
            radiant_heroes_and_pos = match['radiant_heroes_and_pos']
        new_dict = {}
        for key in radiant_heroes_and_pos:
            hero_name = radiant_heroes_and_pos[key]['hero_name'].lower()
            for hero in name_to_id:
                if hero.lower() == hero_name:
                    radiant_heroes_and_pos[key]['hero_id'] = name_to_id[hero]
            if 'hero_id' not in radiant_heroes_and_pos[key]:
                break_flag = True
                break
            new_key = key.replace(' ', '')
            new_dict[new_key] = radiant_heroes_and_pos[key]
        radiant_heroes_and_pos = new_dict
    r_keys = sorted(list(radiant_heroes_and_pos.keys()))
    d_keys = sorted(list(dire_heroes_and_pos.keys()))
    if not all(i == ['pos1', 'pos2', 'pos3', 'pos4', 'pos5'] for i in
               [r_keys, d_keys]) or break_flag:
        return
    else:
        return radiant_heroes_and_pos, dire_heroes_and_pos


def proceed_map(radiant_heroes_and_pos, dire_heroes_and_pos, over35_data, synergy_data, lane_data,
                data1vs2, data1vs1, data1vs3, synergy4, radiant_team_name=None, dire_team_name=None,
                url=None):
    output_dict = {}
    # if radiant_team_name is not None:
    #     answer = \
    #         tm_kills_teams(radiant_heroes_and_pos=radiant_heroes_and_pos,
    #                            dire_heroes_and_pos=dire_heroes_and_pos,
    #                            radiant_team_name=radiant_team_name,
    #                            dire_team_name=dire_team_name, min_len=2)
    #     if answer is not None:
    #         output_dict['kills_mediana'], output_dict['time_mediana'], output_dict['kills_average'],\
    #             output_dict['time_average'] = answer
    #     else:
    #         output_dict['kills_mediana'], output_dict['time_mediana'], output_dict['kills_average'],\
    #             output_dict['time_average'] = None, None, None, None
    output_dict['kills_mediana'], output_dict['time_mediana'], output_dict['kills_average'], \
                    output_dict['time_average'] = None, None, None, None

    output_dict['over35_duo'], output_dict['over35_duo_counterpick'], output_dict['over35_1vs2'],\
        output_dict['over35_solo'], output_dict['over35_trio'] =\
    calculate_over35(radiant_heroes_and_pos, dire_heroes_and_pos, over35_data)

    output_dict['top_message'], output_dict['bot_message'], output_dict['mid_message'] =\
        calculate_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, lane_data)

    output_dict['synergy_duo'], output_dict['radiant_synergy_trio'], output_dict['duo_diff'],\
        output_dict['radiant_counterpick_1vs2'], output_dict['pos1_matchup'], output_dict['support_dif'] =\
        synergy_and_counterpick_new(radiant_heroes_and_pos=radiant_heroes_and_pos,
                                    dire_heroes_and_pos=dire_heroes_and_pos,
                                    synergy_data=synergy_data, data1vs2=data1vs2,
                                    data1vs1=data1vs1, data1vs3=data1vs3)
    return output_dict

def one_match(radiant_heroes_and_pos, dire_heroes_and_pos, lane_data, data1vs1,
              data1vs2, over35_data, synergy_data, data1vs3, synergy4, radiant_team_name=None, dire_team_name=None):
    for key in dire_heroes_and_pos:
        hero_name = dire_heroes_and_pos[key]['hero_name'].lower()
        for hero in name_to_id:
            if hero.lower() == hero_name:
                dire_heroes_and_pos[key]['hero_id'] = name_to_id[hero]
    for key in radiant_heroes_and_pos:
        hero_name = radiant_heroes_and_pos[key]['hero_name'].lower()
        for hero in name_to_id:
            if hero.lower() == hero_name:
                radiant_heroes_and_pos[key]['hero_id'] = name_to_id[hero]

    output_dict = proceed_map(url=None,
                radiant_heroes_and_pos=radiant_heroes_and_pos,
                dire_heroes_and_pos=dire_heroes_and_pos,
                data1vs1=data1vs1, data1vs2=data1vs2,
                lane_data=lane_data, over35_data=over35_data, synergy_data=synergy_data,
                radiant_team_name=radiant_team_name, dire_team_name=dire_team_name,
                synergy4=synergy4, data1vs3=data1vs3)
    if format_output_dict(output_dict):
        # Формирование сообщения
        send_message(
            f'ПОМНИ: ЛЮБОЙ ПИК МОЖЕТ ПРОИГРАТЬ\n'
            f"{radiant_team_name} VS {dire_team_name}\n"
            # f"Kills: Median: {output_dict.get('kills_mediana', 'N/A')} "
            # f"| Avg: {output_dict.get('kills_average', 'N/A')}\n"
            f"over35_solo: {output_dict.get('over35_solo', None)}\n"
            f"over35_duo_counterpick: {output_dict['over35_duo_counterpick']}\n"
            f"over35_trio: {output_dict['over35_trio']}\n"
            f"over35_1vs2: {output_dict['over35_1vs2']}\n"
            f"over35_duo: {output_dict['over35_duo']}\n"
            f"Lanes:\n{output_dict.get('top_message', '')}"
            f"{output_dict.get('mid_message', '')}"
            f"{output_dict.get('bot_message', '')}"
            f"Synergy_and_counterpick:\n"
            f"support_dif: {output_dict['support_dif']}\n"
            f"pos1_matchup: {output_dict['pos1_matchup']}\n"
            f"Synergy_duo: {output_dict['synergy_duo']}\n"
            f"Synergy_trio: {output_dict['radiant_synergy_trio']}\n"
            f"Counterpick_duo: {output_dict['duo_diff']}\n"
            f"1vs2_counterpick: {output_dict['radiant_counterpick_1vs2']}\n"
            f'ПОМНИ: ЛЮБОЙ ПИК МОЖЕТ ПРОИГРАТЬ')
    else:
        send_message(
            'Плохая ставка'
            f"{radiant_team_name} VS {dire_team_name}\n")



def check_old_maps(data1vs1, data1vs2, lane_data, over35_data, synergy_data, data1vs3, synergy4):
    # update_my_protracker(mkdir='dltv', maps_to_explore='past_matches_maps', file_name='dltv.output')
    with open ('dltv/dltv_output.txt', 'r') as f:
        maps_data = json.load(f)
    output_data = []
    for counter, match_id in enumerate(maps_data):
        # if match_id not in ['8282387426', '8229824447', '8294320784', '8240445160', '8237260669', '8293352062', '8264802865', '8287784252', '8240343319', '8251240991', '8275958633', '8272023319', '8271451356', '8264766445', '8283543442', '8239319375', '8251371199', '8264710589', '8232230272', '8253666074', '8253754681', '8244413837', '8238154240', '8292516360', '8246265232', '8283402383', '8275848044', '8252576577', '8277010600', '8275958633', '8269054498', '8251494401', '8239331701', '8239110442', '8239973335', '8238154240', '8234111232', '8245996562', '8239830730', '8299323927', '8293807031', '8292115530', '8269883556', '8264082163', '8239807884', '8241353789', '8275050403', '8251371199', '8245743444', '8276953038', '8239410626', '8238273105', '8234597527', '8244413837']:
        print(f'{counter} | {len(maps_data)}')
        result = check_bad_map(match=match_id, maps_data=maps_data)
        if result is None:
            continue
        radiant_heroes_and_pos, dire_heroes_and_pos = result
        output_dict = proceed_map(dire_heroes_and_pos=dire_heroes_and_pos,
                    radiant_heroes_and_pos=radiant_heroes_and_pos,
                    data1vs1=data1vs1, data1vs2=data1vs2,
                    lane_data=lane_data, over35_data=over35_data, synergy_data=synergy_data,
                    data1vs3=data1vs3, synergy4=synergy4)
        didradiantwin = maps_data[match_id]['didRadiantWin']

        output_data.append({
            'match_id' : match_id, 'radiantNetworthLeads': maps_data[match_id]['radiantNetworthLeads'],
            'top_message': output_dict['top_message'], 'bot_message': output_dict['bot_message'], 'mid_message': output_dict['mid_message'],
            'synergy_duo': output_dict['synergy_duo'], 'radiant_synergy_trio': output_dict['radiant_synergy_trio'],
            'support_dif': output_dict['support_dif'], 'duo_diff': output_dict['duo_diff'],
            'radiant_counterpick_1vs2': output_dict['radiant_counterpick_1vs2'],
            "over35_duo": output_dict['over35_duo'], 'over35_duo_counterpick': output_dict['over35_duo_counterpick'],
            'over35_1vs2': output_dict['over35_1vs2'], 'over35_solo': output_dict['over35_solo'],
            'over35_trio': output_dict['over35_trio'],
            'pos1_matchup': output_dict['pos1_matchup'], 'didRadiantWin': didradiantwin,
            'duration': maps_data[match_id]['durationSeconds'], 'bottomLaneOutcome' : maps_data[match_id]['bottomLaneOutcome'],
            'topLaneOutcome': maps_data[match_id]['topLaneOutcome'], 'midLaneOutcome': maps_data[match_id]['midLaneOutcome']
        })
        pass

    with open('dltv/cyberscore_ouput_classic.txt', 'w') as f:
        json.dump(output_data, f)


def avg_over35(heroes_and_positions, data):
    over35_duo, over35_trio, time_duo, kills_duo, kills_trio, time_trio, radiant_lane_report_unique_combinations, \
        dire_lane_report_unique_combinations, over35_solo = [], [], [], [], [], [], [], [], []
    unique_combinations = set()
    for pos in heroes_and_positions:
        hero_id = str(heroes_and_positions[pos]['hero_id'])
        key = f"{hero_id + pos}"
        foo = data.get(key, [])
        if len(foo) >= 30:
            value = foo.count(1) / (foo.count(1) + foo.count(0))
            over35_solo.append(value)
        for second_pos in heroes_and_positions:
            second_hero_id = str(heroes_and_positions[second_pos]['hero_id'])
            if hero_id == second_hero_id:
                continue
            key = f"{hero_id + pos}_with_{second_hero_id + second_pos}"
            foo = data.get(key, [])
            if len(foo) >= 20:
                value = foo.count(1) / (foo.count(1) + foo.count(0))
                over35_duo.append(value)
            for third_pos in heroes_and_positions:
                third_hero_id = str(heroes_and_positions[third_pos]['hero_id'])
                if third_hero_id in [second_hero_id, hero_id]:
                    continue
                third_hero_id = str(heroes_and_positions[third_pos]['hero_id'])
                key = f"{hero_id + pos},{second_hero_id + second_pos},{third_hero_id + third_pos}"
                foo = data.get(key, [])
                if len(foo) >= 10:
                    combo = tuple(sorted([hero_id, second_hero_id, third_hero_id]))
                    if combo not in unique_combinations:
                        unique_combinations.add(combo)
                        value = foo.count(1) / (foo.count(1) + foo.count(0))
                        over35_trio.append(value)

    return over35_duo, over35_solo, over35_trio


def find_biggest_param(data, mid=False):
    data = {'draw': data['draw'], 'loose': data['loose'], 'win': data['win']}
    sorted_keys = sorted(data, key=lambda k: data[k], reverse=True)
    second_max_key = sorted_keys[1]
    second_max_value = int(data[second_max_key])
    key = sorted_keys[0]
    first_key_max_value = int(data[key])
    if first_key_max_value == second_max_value:
        if all(i in ['win', 'loose'] for i in (key, second_max_key)):
            key = 'draw'
        elif 'draw' in [key, second_max_key]:
            key = 'draw'
    if mid:
        if first_key_max_value < 50:
            first_key_max_value = None
    else:
        if first_key_max_value < 50:
            first_key_max_value = None
    return key, first_key_max_value


def lane_2vs2(radiant, dire, heroes_data, output):
    data_2vs2 = heroes_data['2v2_lanes']

    bot_lane = f'{radiant["pos1"]["hero_id"]}pos1,{radiant["pos5"]["hero_id"]}pos5_vs_' \
               f'{dire["pos3"]["hero_id"]}pos3,{dire["pos4"]["hero_id"]}pos4'
    top_lane = f'{radiant["pos3"]["hero_id"]}pos3,{radiant["pos4"]["hero_id"]}pos4_vs_' \
               f'{dire["pos1"]["hero_id"]}pos1,{dire["pos5"]["hero_id"]}pos5'
    for lane, key in [[top_lane, 'top'], [bot_lane, 'bot']]:
        value = data_2vs2.get(lane, {}).get('value', [])
        if len(value) >= 3:
            loose = value.count(-1) / (len(value))
            draw = value.count(0) / (len(value))
            win = value.count(1) / (len(value))
            total = loose+win+draw
            loose = loose/total*100
            draw = draw / total * 100
            win = win / total * 100

            output.setdefault(key, {}).setdefault('loose', loose)
            output.setdefault(key, {}).setdefault('draw', draw)
            output.setdefault(key, {}).setdefault('win', win)


def multiply_list(lst, result=1):
    if lst:
        for num in lst:
            result *= num
        return result




def get_values(lane_side, key, heroes_data, output):
    value = heroes_data.get(key, {}).get('value', [])
    if len(value) >= 6:
        loose = value.count(-1) / (len(value))
        draw = value.count(0) / (len(value))
        win = value.count(1) / (len(value))
        output.setdefault(lane_side, {}).setdefault('loose', []).append(loose)
        output.setdefault(lane_side, {}).setdefault('draw', []).append(draw)
        output.setdefault(lane_side, {}).setdefault('win', []).append(win)
    else:
        foo = key.split('_vs_')
        to_be_appended = [i for i in foo if len(i.split(',')) == 1]
        output.setdefault(lane_side, {}).setdefault('not_used_hero_pos', []).append(to_be_appended[0])


def lane_2vs1(radiant, dire, heroes_data, lane):
    heroes_data = heroes_data['2v1_lanes']
    output = {}
    if lane == 'bot':
        for key in [
                f'{radiant["pos1"]["hero_id"]}pos1,{radiant["pos5"]["hero_id"]}pos5_vs_{dire["pos3"]["hero_id"]}pos3',
                f'{radiant["pos1"]["hero_id"]}pos1,{radiant["pos5"]["hero_id"]}pos5_vs_'
                f'{dire["pos4"]["hero_id"]}pos4']:
            get_values('bot_radiant', key, heroes_data, output)
        for key in [
                f'{radiant["pos1"]["hero_id"]}pos1_vs_{dire["pos3"]["hero_id"]}pos3,{dire["pos4"]["hero_id"]}pos4',
                f'{radiant["pos5"]["hero_id"]}pos5_vs_{dire["pos3"]["hero_id"]}pos3,{dire["pos4"]["hero_id"]}pos4']:
            get_values('bot_dire', key, heroes_data, output)
    elif lane == 'top':
        for key in [
                f'{radiant["pos3"]["hero_id"]}pos3,{radiant["pos4"]["hero_id"]}pos4_vs_{dire["pos1"]["hero_id"]}pos1',
                f'{radiant["pos3"]["hero_id"]}pos3,{radiant["pos4"]["hero_id"]}pos4_vs_'
                f'{dire["pos5"]["hero_id"]}pos5']:
            get_values('top_radiant', key, heroes_data, output)
        for key in [
                f'{radiant["pos3"]["hero_id"]}pos3_vs_{dire["pos1"]["hero_id"]}pos1,{dire["pos5"]["hero_id"]}pos5',
                f'{radiant["pos4"]["hero_id"]}pos4_vs_{dire["pos1"]["hero_id"]}pos1,{dire["pos5"]["hero_id"]}pos5']:
            get_values('top_dire', key, heroes_data, output)
    elif lane == 'mid':
        key = f'{radiant["pos2"]["hero_id"]}pos2_vs_{dire["pos2"]["hero_id"]}pos2'
        value = heroes_data.get(key, {}).get('value', [])
        if len(value) >= 8:
            loose = value.count(-1) / (len(value))*100
            draw = value.count(0) / (len(value))*100
            win = value.count(1) / (len(value))*100
            output.setdefault('mid_radiant', {}).setdefault('loose',loose)
            output.setdefault('mid_radiant', {}).setdefault('draw', draw)
            output.setdefault('mid_radiant', {}).setdefault('win', win)
    return output


def both_found(lane, data, output):
    data[f'{lane}_dire']['draw'] = multiply_list(data[f'{lane}_dire']['draw'])
    data[f'{lane}_dire']['win'] = multiply_list(data[f'{lane}_dire']['win'])
    data[f'{lane}_dire']['loose'] = multiply_list(data[f'{lane}_dire']['loose'])

    data[f'{lane}_radiant']['draw'] = multiply_list(data[f'{lane}_radiant']['draw'])
    data[f'{lane}_radiant']['win'] = multiply_list(data[f'{lane}_radiant']['win'])
    data[f'{lane}_radiant']['loose'] = multiply_list(data[f'{lane}_radiant']['loose'])

    radiant_draw = (data[f'{lane}_radiant']['draw'] + data[f'{lane}_dire']['draw'])/2
    radiant_win = (data[f'{lane}_radiant']['win'] + data[f'{lane}_dire']['win'])/2
    radiant_loose = (data[f'{lane}_radiant']['loose'] + data[f'{lane}_dire']['loose'])/2

    total = radiant_loose + radiant_draw + radiant_win
    if total not in [0, 0.0]:
        output.setdefault(f'{lane}', {}).setdefault('win', round(radiant_win / total * 100))
        output.setdefault(f'{lane}', {}).setdefault('loose', round(radiant_loose / total * 100))
        output.setdefault(f'{lane}', {}).setdefault('draw', round(radiant_draw / total * 100))

        bot_key, bot_key_value = find_biggest_param(output[f'{lane}'])
        return bot_key, bot_key_value


def synergy_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, lane):
    heroes_data = heroes_data['1_with_1_lanes']
    if lane == 'bot':
        radiant_key = f"{radiant_heroes_and_pos['pos1']['hero_id']}pos1_with_{radiant_heroes_and_pos['pos5']['hero_id']}pos5"
        dire_key = f"{dire_heroes_and_pos['pos3']['hero_id']}pos3_with_{dire_heroes_and_pos['pos4']['hero_id']}pos4"
        radiant_value = heroes_data.get(radiant_key, {}).get('value', [])
        dire_value = heroes_data.get(dire_key, {}).get('value', [])
        if len(radiant_value) > 9 and len(dire_value) > 9:
            radiant_win = radiant_value.count(1) / len(radiant_value)
            radiant_loose = radiant_value.count(-1) / len(radiant_value)
            radiant_tie = radiant_value.count(0) / len(radiant_value)
            dire_win = dire_value.count(1) / len(dire_value)
            dire_loose = dire_value.count(-1) / len(dire_value)
            dire_tie = dire_value.count(0) / len(dire_value)
            win = radiant_win * dire_loose
            draw = radiant_tie * dire_tie
            loose = radiant_loose * dire_win
            total = sum([win, draw, loose])
            win = win / total * 100
            draw = draw / total * 100
            loose = loose / total * 100
            key, first_key_max_value = find_biggest_param({'win': win, 'draw': draw, 'loose': loose})
            return key, first_key_max_value

    elif lane == 'top':
        radiant_key = f"{radiant_heroes_and_pos['pos3']['hero_id']}pos3_with_{radiant_heroes_and_pos['pos4']['hero_id']}pos4"
        dire_key = f"{dire_heroes_and_pos['pos1']['hero_id']}pos1_with_{dire_heroes_and_pos['pos5']['hero_id']}pos5"
        radiant_value = heroes_data.get(radiant_key, {}).get('value', [])
        dire_value = heroes_data.get(dire_key, {}).get('value', [])
        if len(radiant_value) > 9 and len(dire_value) > 9:
            radiant_win = radiant_value.count(1)/len(radiant_value)
            radiant_loose = radiant_value.count(-1)/len(radiant_value)
            radiant_tie = radiant_value.count(0)/len(radiant_value)
            dire_win = dire_value.count(1)/len(dire_value)
            dire_loose = dire_value.count(-1)/len(dire_value)
            dire_tie = dire_value.count(0)/len(dire_value)
            win = radiant_win * dire_loose
            draw = radiant_tie * dire_tie
            loose = radiant_loose * dire_win
            total = sum([win, draw, loose])
            win = win/total*100
            draw = draw/total*100
            loose = loose/total*100
            key, first_key_max_value = find_biggest_param({'win': win, 'draw': draw, 'loose': loose})
            return key, first_key_max_value





def calculate_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data):

    output, bot_key, bot_key_value, top_key, top_key_value, mid_key, mid_key_value = {}, None, None, None, None, None, None
    lane_2vs2(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, output)
    if 'top' not in output:
        top2vs1 = lane_2vs1(radiant=radiant_heroes_and_pos, dire=dire_heroes_and_pos,
                            heroes_data=heroes_data, lane='top')
        if all(len(line) == 2 for line in
               [top2vs1.get('top_radiant', {}).get('win', {}), top2vs1.get('top_dire', {}).get('win', {})]):
            result = both_found(lane='top', data=top2vs1, output=output)
            if result is not None:
                top_key, top_key_value = result
            else: top_key_value = None
        else:
            answer = synergy_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, 'top')
            if answer is None:
                top_key_value = None
            else:
                top_key, top_key_value = answer
    else:
        top_key, top_key_value = find_biggest_param(output['top'])
    if 'bot' not in output:
        bot2vs1 = lane_2vs1(radiant=radiant_heroes_and_pos, dire=dire_heroes_and_pos,
                            heroes_data=heroes_data, lane='bot')
        if all(len(line) == 2 for line in [bot2vs1.get('bot_radiant', {}).get('win', {}), bot2vs1.get('bot_dire', {}).get('win', {})]):
            result = both_found(lane='bot', data=bot2vs1, output=output)
            if result is not None:
                bot_key, bot_key_value = result
            else: bot_key_value = None

        else:
            answer = synergy_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, 'bot')
            if answer is None:
                bot_key_value = None
            else:
                bot_key, bot_key_value = answer
    else:
        bot_key, bot_key_value = find_biggest_param(output['bot'])



    mid_output = lane_2vs1(radiant=radiant_heroes_and_pos, dire=dire_heroes_and_pos,
                           heroes_data=heroes_data, lane='mid')
    if mid_output:
        mid_key, mid_key_value = find_biggest_param(
            mid_output['mid_radiant'], mid=True)



    if top_key_value is None:
        top_message = 'Top: None\n'
    else:
        top_message = f'Top: {top_key} {top_key_value}%\n'
    if bot_key_value is None:
        bot_message = 'Bot: None\n\n'
    else:
        bot_message = f'Bot: {bot_key} {bot_key_value}%\n\n'
    if mid_key_value is None:
        mid_message = 'Mid: None\n'
    else:
        mid_message = f'Mid: {mid_key} {mid_key_value}%\n'
    return top_message, bot_message, mid_message


def is_moscow_night():
    moscow_tz = pytz.timezone("Europe/Moscow")
    now = datetime.datetime.now(moscow_tz)
    return 2 <= now.hour < 6


def sleep_until_morning():
    moscow_tz = pytz.timezone("Europe/Moscow")

    while True:
        now = datetime.datetime.now(moscow_tz)
        # Формируем время 07:00 текущего дня
        morning = now.replace(hour=6, minute=0, second=0, microsecond=0)

        # Если текущее время уже 07:00 или позже, выходим из цикла
        if now >= morning:
            print("Наступило утро!")
            break

        # Вычисляем оставшиеся секунды до 07:00
        remaining_seconds = (morning - now).total_seconds()
        # Будем спать не больше 60 секунд за раз, чтобы часто проверять время
        sleep_interval = min(remaining_seconds, 60)

        print(
            f"Сейчас {now.strftime('%H:%M:%S')} по Москве. До 06:00 осталось {int(remaining_seconds)} секунд. Засыпаем на {int(sleep_interval)} секунд.")
        time.sleep(sleep_interval)



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
            for pos2, item2 in radiant_heroes_and_positions.items():
                second_hero_id = str(item2['hero_id'])
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
                                    trio_data = duo_data.get('total_time_trio', {}).\
                                        get(third_hero_id, {}).get(pos3, {}).get('value', {})
                                    if len(trio_data):
                                        value = (sum(trio_data) / len(trio_data)) / 60
                                        output_data['radiant_time_trio'].append(value)
                            elif hero_data == kills_data:
                                if combo not in radiant_kills_unique_combinations:
                                    radiant_kills_unique_combinations.add(combo)
                                    trio_data = duo_data.get('total_kills_trio', {}).\
                                        get(third_hero_id, {}).get(pos3, 'value', {})
                                    if len(trio_data):
                                        value = sum(trio_data) / len(trio_data)
                                        output_data['radiant_kills_trio'].append(value)
        # dire_synergy
        hero_id = str(dire_heroes_and_positions['pos' + pos]['hero_id'])
        time_data = data.get(hero_id, {}).get('pos' + pos, {}).get('total_time_duo', {})
        kills_data = data.get(hero_id, {}).get('pos' + pos, {}).get('total_kills_duo', {})
        for hero_data in [time_data, kills_data]:
            for pos2, item2 in dire_heroes_and_positions.items():
                second_hero_id = str(item2['hero_id'])
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


def find_lowest(lst):
    if len(lst) > 0:
        c = lst[0]
        for foo in lst:
            if foo < c:
                c = foo
        return c


def sum_if_none(n1, n2):
    if all(i is None for i in [n1, n2]):
        return None
    elif any(i is None for i in [n1, n2]):
        c = 0
        for i in [n1, n2]:
            if i is not None:
                c += i
        return c
    else:
        return (n1 + n2) / 2


def tm_kills_teams(radiant_heroes_and_pos, dire_heroes_and_pos, radiant_team_name, dire_team_name, min_len):
    # print('tm_kills')
    output_data, positions = {}, ['1', '2', '3', '4', '5']
    trslt = {
        'aurora': 'aurora gaming',
        'team waska': 'waska',
        'fusion': 'fusion esports',
        '1win team': '1win',
        'talon esports': 'talon',
        'passion ua': 'team hryvnia',
    }
    radiant_team_name = trslt[radiant_team_name] if radiant_team_name in trslt else radiant_team_name.lower()
    dire_team_name = trslt[dire_team_name] if dire_team_name in trslt else dire_team_name.lower()
    with open('./pro_heroes_data/total_time_kills_dict_teams.txt', 'r') as f:
        file_data = json.load(f)['teams']
    if not all(team in file_data for team in [radiant_team_name, dire_team_name]):
        if radiant_team_name not in file_data:
            print(f'{radiant_team_name} not in team list')
        if dire_team_name not in file_data:
            print(f'{dire_team_name} not in team list')
        return
    for side_name, heroes_and_pos, team_name in [['radiant', radiant_heroes_and_pos, radiant_team_name], ['dire', dire_heroes_and_pos, dire_team_name]]:
        time_unique_combinations, kills_unique_combinations = set(), set()
        work_data = file_data[team_name]
        for pos in positions:
            hero_id = str(heroes_and_pos['pos' + pos]['hero_id'])
            data = work_data.get(hero_id, {}).get('pos' + pos, {})
            if not data:
                continue
            solo_time = data.get('solo_time', {}).get('value', {})
            if solo_time:
                output_data.setdefault(side_name, {}).setdefault('solo_time', [])
                output_data[side_name]['solo_time'] += solo_time
            solo_kills = data.get('solo_kills', {}).get('value', {})
            if solo_kills:
                output_data.setdefault(side_name, {}).setdefault('solo_kills', [])
                # output_data[side_name]['solo_kills'] += [sum(solo_kills)/len(solo_kills)]
                output_data[side_name]['solo_kills'] += solo_kills
            time_data = data.get('time_duo', {})
            kills_data = data.get('kills_duo', {})
            for hero_data in [time_data, kills_data]:
                for pos2, item in heroes_and_pos.items():
                    second_hero_id = str(item['hero_id'])
                    if second_hero_id == hero_id:
                        continue
                    duo_data = hero_data.get(second_hero_id, {}).get(pos2, {})
                    if len(duo_data.get('value', {})) > 0:
                        combo = tuple(sorted([hero_id, second_hero_id]))
                        if hero_data == time_data:
                            if combo not in time_unique_combinations:
                                time_unique_combinations.add(combo)
                                value = duo_data['value']
                                output_data.setdefault(side_name, {}).setdefault('time_duo', [])
                                output_data[side_name]['time_duo'] += value
                        elif hero_data == kills_data:
                            if combo not in kills_unique_combinations:
                                kills_unique_combinations.add(combo)
                                value = duo_data['value']
                                output_data.setdefault(side_name, {}).setdefault('kills_duo', [])
                                # output_data[side_name]['kills_duo'] += [sum(value)/len(value)]
                                output_data[side_name]['kills_duo'] += value
    r_solo_t = output_data.get('radiant', {}).get('solo_time', [])
    d_solo_t = output_data.get('dire', {}).get('solo_time', [])
    r_solo_k = output_data.get('radiant', {}).get('solo_kills', [])
    d_solo_k = output_data.get('dire', {}).get('solo_kills', [])
    r_duo_t = output_data.get('radiant', {}).get('time_duo', [])
    d_duo_t = output_data.get('dire', {}).get('time_duo', [])
    r_duo_k = output_data.get('radiant', {}).get('kills_duo', [])
    d_duo_k = output_data.get('dire', {}).get('kills_duo', [])
    def find_mediana(lst):
        lst = sorted(lst)
        lenght = len(lst)
        if len(lst) == 0:
            return
        elif lenght == 1:

            return lst[0]
        if lenght % 2 != 0:
            return lst[(lenght//2)+1]
        elif lenght %2 == 0:
            return (lst[lenght//2] + lst[lenght//2-1])/2

    kills_mediana = find_mediana(r_solo_k+ d_solo_k + r_duo_k + d_duo_k)
    time_mediana = find_mediana(r_solo_t + d_solo_t + r_duo_t + d_duo_t)
    kills_average = sum(r_solo_k+ d_solo_k + r_duo_k + d_duo_k)/len(r_solo_k+ d_solo_k + r_duo_k + d_duo_k)
    time_average = sum(r_solo_t + d_solo_t + r_duo_t + d_duo_t)/len(r_solo_t + d_solo_t + r_duo_t + d_duo_t)

    if time_mediana is not None:
        time_mediana = time_mediana/60

    return kills_mediana, time_mediana, kills_average, time_average

