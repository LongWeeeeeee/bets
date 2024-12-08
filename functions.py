import datetime
import html
import json
import re
import time
import requests
from bs4 import BeautifulSoup

import id_to_name
import keys
from id_to_name import pro_teams
from keys import api_token_5


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
                key = f"{hero_id + pos},{second_hero_id + second_pos},{third_hero_id + third_pos}"
                foo = data.get(key, {})
                if len(foo) > 9:
                    combo = tuple(sorted([hero_id, second_hero_id, third_hero_id]))
                    if combo not in unique_combinations:
                        unique_combinations.add(combo)
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


def counterpick_team(heroes_and_pos, heroes_and_pos_opposite, output, mkdir, data1vs1, data1vs2, data1vs3):
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

                key = f"{hero_id}{pos},{enemy_hero_id}{enemy_pos}," \
                      f"{second_enemy_id}{second_enemy_pos}"
                foo = data1vs2.get(key, {})
                if len(foo) >= 15:
                    combo = (hero_id,) + tuple(sorted([enemy_hero_id, second_enemy_id]))
                    if combo not in unique_combinations:
                        unique_combinations.add(combo)
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
    with open('1722505765_top600_heroes_data/counterpick1vs3.txt', 'r') as f1:
        data1vs3 = json.load(f1)
    with open('1722505765_top600_heroes_data/counterpick1vs1.txt', 'r') as f2:
        data1vs1 = json.load(f2)
        with open('1722505765_top600_heroes_data/counterpick1vs2.txt', 'r') as f3:
            data1vs2 = json.load(f3)
            counterpick_team(heroes_and_pos=radiant_heroes_and_pos, heroes_and_pos_opposite=dire_heroes_and_pos,
                             output=output, mkdir='radiant_counterpick', data1vs2=data1vs2,
                             data1vs1=data1vs1, data1vs3=data1vs3)
            counterpick_team(heroes_and_pos=dire_heroes_and_pos, heroes_and_pos_opposite=radiant_heroes_and_pos,
                             output=output, mkdir='dire_counterpick', data1vs2=data1vs2,
                             data1vs1=data1vs1, data1vs3=data1vs3)

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
    if output['radiant_counterpick_1vs2'] and output['dire_counterpick_1vs2']:
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
    if output['dire_synergy_trio'] and output['radiant_synergy_trio']:
        radiant_synergy_trio = sum(output['radiant_synergy_trio']) / len(
            output['radiant_synergy_trio'])
        dire_synergy_trio = sum(output['dire_synergy_trio']) / len(output['dire_synergy_trio'])
        radiant_synergy_trio_win_prob = (radiant_synergy_trio - dire_synergy_trio) * 100
    else:
        radiant_synergy_trio_win_prob = None
    end_time = time.time()
    execution_time = end_time - start_time
    print(f'synergy_and_counterpick_new time: {execution_time}s')
    return f'\nsynergy_and_counterpick_new:\nSynergy_duo: {synergy_duo}\n' \
           f'Synergy_trio: {radiant_synergy_trio_win_prob}\n' \
           f'Counterpick_duo: {duo_diff}\nCounterpick_1vs2: {radiant_counterpick_1vs2_win_prob}'


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
    return f'Radiant после 40 минуты сильнее на: \nSynergy: {over40}\n' \
           f'Counterpick_duo: {duo_counterpick}\nCounterpick_1vs2: {avg1vs2}\n\n'


def over40_counter(my_team, enemy_team, data):
    uniq_combo, duo_winrate, winrate1vs2 = set(), [], []
    for pos in my_team:
        hero_id = str(my_team[pos]['hero_id'])
        hero_data = data.get(hero_id, {}).get(pos, {}).get('over40_counterpick_duo', {})
        for enemy_pos in enemy_team:
            enemy_hero_id = str(enemy_team[enemy_pos]['hero_id'])
            duo_data = hero_data.get(enemy_hero_id, {}).get(enemy_pos, {})
            foo = duo_data.get('value', {})
            if len(foo) >= 10:
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
    duo_winrate, winrate1vs2 = clean_up(duo_winrate), clean_up(winrate1vs2)
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
            if len(duo_data.get('value', {})) >= 15:
                if combo not in over40_unique_combinations:
                    over40_unique_combinations.add(combo)
                    value = duo_data['value'].count(1) / (duo_data['value'].count(1) + duo_data['value'].count(0))
                    over40_duo.append(value)
            # Третий герой
            for pos3, item3 in heroes_and_positions.items():
                third_hero_id = str(item3['hero_id'])
                if third_hero_id in [second_hero_id, hero_id]:
                    continue
                combo = tuple(sorted([hero_id, second_hero_id, third_hero_id]))
                trio_data = duo_data.get('over40_trio', {}).get(third_hero_id, {}).get(pos3, {}).get('value', {})
                if len(trio_data) >= 10:
                    if combo not in over40_unique_combinations:
                        over40_unique_combinations.add(combo)
                        value = trio_data.count(1) / (trio_data.count(1) + trio_data.count(0))
                        over40_trio.append(value)
    avg_over40_duo = calculate_average(clean_up(over40_duo, 4))
    avg_over40_trio = calculate_average(clean_up(over40_trio, 4))

    end_time = time.time()
    execution_time = end_time - start_time
    print(f'over40 time: {execution_time}s')
    return (avg_over40_duo + avg_over40_trio) / 2 if avg_over40_trio is not None else avg_over40_duo


def find_biggest_param(data):
    sorted_keys = sorted(data, key=lambda k: data[k][0], reverse=True)
    second_max_key = sorted_keys[1]
    second_max_value = round(data[second_max_key][0])
    first_key = sorted_keys[0]
    first_key_max_value = round(data[first_key][0])
    return first_key, first_key_max_value, second_max_key, second_max_value


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


def multiply_list(lst, result=1):
    for num in lst:
        result *= num
    return result


def mid_lane_report(my_team, enemy_team, heroes_data, side, output):
    heroes_data = heroes_data['value']
    # print('lane_report')

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


def calculate_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, message=f'Radiant до 10-15 минут:\n', top_message='None\n', bot_message='None\n'):
    with open('1722505765_top600_heroes_data/lane_dict.txt', 'r') as f:
        heroes_data = json.load(f)
    output = {}
    lane_2vs2(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, output)
    if 'top' not in output:
        top2vs1 = lane_2vs1(radiant=radiant_heroes_and_pos, dire=dire_heroes_and_pos,
                            heroes_data=heroes_data, lane='top')
        if all(len(line) == 2 for line in
               [top2vs1.get('top_radiant', {}).get('win', {}), top2vs1.get('top_dire', {}).get('win', {})]):
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

            if any(foo != 0.0 for foo in
                   [radiant_draw, radiant_win, radiant_loose, radiant_win_stomp, radiant_loose_stomp]):
                total = radiant_loose + radiant_draw + radiant_win + radiant_win_stomp + radiant_loose_stomp
                output.setdefault('top', {}).setdefault('win', []).append(round(radiant_win / total * 100))
                output.setdefault('top', {}).setdefault('loose', []).append(round(radiant_loose / total * 100))
                output.setdefault('top', {}).setdefault('draw', []).append(round(radiant_draw / total * 100))
                output.setdefault('top', {}).setdefault('win_stomp', []).append(round(radiant_win_stomp / total * 100))
                output.setdefault('top', {}).setdefault('loose_stomp', []).append(round(radiant_loose_stomp / total * 100))
                top_first_key, top_first_key_max_value, top_second_max_key, top_second_max_value = find_biggest_param(output['top'])
                top_message = f'Top: {top_first_key} {top_first_key_max_value}%, {top_second_max_key} {top_second_max_value}%\n'
        else:
            pass
    else:
        top_first_key, top_first_key_max_value, top_second_max_key, top_second_max_value = find_biggest_param(output['top'])
        message += f'Top: {top_first_key} {top_first_key_max_value}%, {top_second_max_key} {top_second_max_value}%\n'
    if 'bot' not in output:
        bot2vs1 = lane_2vs1(radiant=radiant_heroes_and_pos, dire=dire_heroes_and_pos,
                            heroes_data=heroes_data, lane='bot')
        if all(len(line) == 2 for line in [bot2vs1.get('bot_radiant', {}).get('win', {}), bot2vs1.get('bot_dire', {}).get('win', {})]):
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

                    bot_first_key, bot_first_key_max_value,\
                        bot_second_max_key, bot_second_max_value = find_biggest_param(output['bot'])
                    bot_message = f'Bot: {bot_first_key} {bot_first_key_max_value}%, {bot_second_max_key} {bot_second_max_value}%\n'

        else:
            pass
    else:
        bot_first_key, bot_first_key_max_value, bot_second_max_key, bot_second_max_value = find_biggest_param(output['bot'])
        message += f'Bot: {bot_first_key} {bot_first_key_max_value}%, {bot_second_max_key} {bot_second_max_value}%\n'
    if 'bot' not in output:
        loose_stomp, loose, draw, win, win_stomp = [], [], [], [], []
        for radiant_key in [f"{radiant_heroes_and_pos['pos1']['hero_id']}pos1",
                            f"{radiant_heroes_and_pos['pos5']['hero_id']}pos5"]:
            for dire_key in [f"{dire_heroes_and_pos['pos3']['hero_id']}pos3",
                             f"{radiant_heroes_and_pos['pos4']['hero_id']}pos4"]:
                value = heroes_data.get(f'{radiant_key}_vs_{dire_key}', {})
                if len(value) > 9:
                    loose_stomp.append(value.count(-2) / (len(value)))
                    loose.append(value.count(-1) / (len(value)))
                    draw.append(value.count(0) / (len(value)))
                    win.append(value.count(1) / (len(value)))
                    win_stomp.append(value.count(2) / (len(value)))
        loose, loose_stomp, win, win_stomp, draw = multiply_list(loose), multiply_list(loose_stomp), multiply_list(
            win), multiply_list(win_stomp), multiply_list(draw)
        total = loose + loose_stomp + win + win_stomp + draw

        output.setdefault('bot', {}).setdefault('win', []).append(win / total * 100)
        output.setdefault('bot', {}).setdefault('win_stomp', []).append(win_stomp / total * 100)
        output.setdefault('bot', {}).setdefault('loose', []).append(loose / total * 100)
        output.setdefault('bot', {}).setdefault('loose_stomp', []).append(loose_stomp / total * 100)
        output.setdefault('bot', {}).setdefault('draw', []).append(draw / total * 100)
        bot_first_key, bot_first_key_max_value, bot_second_max_key, bot_second_max_value = find_biggest_param(
            output['bot'])
        bot_message = f'Bot: {bot_first_key} {bot_first_key_max_value}%, {bot_second_max_key} {bot_second_max_value}%\n'

    if 'top' not in output:
        loose_stomp, loose, draw, win, win_stomp = [], [], [], [], []
        for radiant_key in [f"{radiant_heroes_and_pos['pos3']['hero_id']}pos3", f"{radiant_heroes_and_pos['pos4']['hero_id']}pos4"]:
            for dire_key in [f"{dire_heroes_and_pos['pos1']['hero_id']}pos1", f"{radiant_heroes_and_pos['pos5']['hero_id']}pos5"]:
                value = heroes_data.get(f'{radiant_key}_vs_{dire_key}', {})
                if len(value) > 9:
                    loose_stomp.append(value.count(-2) / (len(value)))
                    loose.append(value.count(-1) / (len(value)))
                    draw.append(value.count(0) / (len(value)))
                    win.append(value.count(1) / (len(value)))
                    win_stomp.append(value.count(2) / (len(value)))
        loose, loose_stomp, win, win_stomp, draw = multiply_list(loose), multiply_list(loose_stomp), multiply_list(win), multiply_list(win_stomp), multiply_list(draw)
        total = loose + loose_stomp + win + win_stomp + draw

        output.setdefault('top', {}).setdefault('win', []).append(win/total*100)
        output.setdefault('top', {}).setdefault('win_stomp', []).append(win_stomp/total*100)
        output.setdefault('top', {}).setdefault('loose', []).append(loose/total*100)
        output.setdefault('top', {}).setdefault('loose_stomp', []).append(loose_stomp/total*100)
        output.setdefault('top', {}).setdefault('draw', []).append(draw/total*100)
        top_first_key, top_first_key_max_value, top_second_max_key, top_second_max_value = find_biggest_param(
            output['top'])
        top_message = f'Top: {top_first_key} {top_first_key_max_value}%, {top_second_max_key} {top_second_max_value}%\n'
    mid_output = lane_2vs1(radiant=radiant_heroes_and_pos, dire=dire_heroes_and_pos,
                           heroes_data=heroes_data, lane='mid')
    if mid_output:
        mid_first_key, mid_first_key_max_value, mid_second_max_key, mid_second_max_value = find_biggest_param(
            mid_output['mid_radiant'])
        mid_message = f'Mid: {mid_first_key} {mid_first_key_max_value}%, {mid_second_max_key} {mid_second_max_value}%\n'
    else:
        mid_message = f'Mid: None%\n'
    message += top_message + mid_message + bot_message
    return message


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
            print(f'{radiant_team_name} not in team list')
        if dire_team_name not in file_data:
            print(f'{dire_team_name} not in team list')
        return None, None, None, None
