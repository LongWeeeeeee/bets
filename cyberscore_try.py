import json
import time
from bs4 import BeautifulSoup
import requests
import traceback
from id_to_name import translate, name_to_id
from functions import get_team_positions, tm_kills_teams, tm_kills, send_message, add_url, get_map_id, \
    calculate_over40, calculate_lanes, synergy_and_counterpick_new
from maps_research import update_pro, update_my_protracker
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def general(match_list=None, radiant_team_name='radiant', dire_team_name='dire', tier=None, draft=False):
    if match_list is None:
        headers = {
            "Host": "dltv.org",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
            "Referer": 'https://dltv.org/results',
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/130.0.0.0 Safari/537.36",}
        name_to_pos = {
            'Core': 'pos1',
            'Support': 'pos4',
            'Full Support': 'pos5',
            'Mid': 'pos2',
            'Offlane': 'pos3'
        }
        ip_address = "46.229.214.49"
        path = "/matches"  # Desired path

        # URL with the IP address and the specific path
        url = f"https://{ip_address}{path}"
        proxies = {
            'https': 'http://90gwi7LEfz:aKI0jgSViq@77.221.150.248:42037',
        }
        for i in range(5):
            try:
                response = requests.get(url, headers=headers, verify=False, proxies=proxies)
                if response.status_code == 200:
                    break
            except:
                pass
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'lxml')
            live_matches = soup.find('div', class_='live__matches')
            heads = live_matches.find_all('div', class_='live__matches-item__head')
            bodies = live_matches.find_all('div', class_='live__matches-item__body')

            for i in range(len(heads)):
                data = {}
                ip_address = "46.229.214.49"
                status = heads[i].find('div', class_='event__info-info__time')
                if status.text == 'Finished':
                    continue
                elif status.text == 'Draft...':
                    draft = True
                    # continue
                divs = bodies[i].find_all('div', class_='match__item-team__score')
                if any(div.text == '-' for div in divs):
                    continue
                uniq_score = int(divs[0].text) + int(divs[1].text)
                with open('map_id_check.txt', 'r+') as f:
                    maps_data = json.load(f)
                path = bodies[i].find('a')['href'].split('https://dltv.org')[1]
                check_uniq_url = 'dltv.org' + path + f'.{str(uniq_score)}'
                if check_uniq_url in maps_data:
                    continue
                # Correct Host header without protocol and path


                # Desired path

                # URL with the IP address and the specific path
                url = f"https://{ip_address}{path}"
                proxies = {
                    'https': 'http://90gwi7LEfz:aKI0jgSViq@77.221.150.248:42037',
                }
                for i in range(5):
                    try:
                        response = requests.get(url, headers=headers, verify=False, proxies=proxies)
                        if response.status_code == 200:
                            break
                    except:
                        time.sleep(5)
                    # Disable SSL verification
                if response.status_code != 200:
                    print(f"Failed to retrieve content. Status code: {response.status_code}")
                    continue
                soup = BeautifulSoup(response.text, 'lxml')
                # if True:
                #     soup = BeautifulSoup(response_text, 'lxml')
                spans = soup.find('div', class_='score__scores live').find_all('span')
                score = [spans[0].text.strip(), spans[1].text.strip()]

                teams = soup.find_all('span', class_='team__title-name')

                for team in teams:
                    name = team.find('span', class_='name').text
                    side = team.find('span', class_='side').text.lower()
                    data.setdefault('teams', {}).setdefault(side, name)
                maps = soup.find_all('div', class_='picks__new-picks')

                # Process the 'heroes' as needed
                for match in maps:
                    result = get_map_players(data, match, soup, name_to_pos)
                    if result is None:
                        url = url.replace('46.229.214.49', 'dltv.org')
                        print(f'{url}, ошибка выяснении пиков')
                        continue
                    elif result == True:
                        add_url(check_uniq_url, score)
                    else:
                        radiant_team_name, dire_team_name, radiant_heroes_and_pos, dire_heroes_and_pos = result
                        proceed_map(url='dltv.org'+path, score=score, radiant_team_name=radiant_team_name,
                                    dire_team_name=dire_team_name, radiant_heroes_and_pos=radiant_heroes_and_pos,
                                    dire_heroes_and_pos=dire_heroes_and_pos, output_message='', kills=None, time=None)

        return draft
    else:
        for url in match_list:
            radiant_team_name, dire_team_name = radiant_team_name.lower(), dire_team_name.lower()
            proceed_map(url, radiant_team_name, dire_team_name, [0, 0], tier)


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


def proceed_map(url, radiant_team_name, dire_team_name, tier=1, radiant_heroes_and_pos=None,
                dire_heroes_and_pos=None, output_message='', kills=None, time=None, score=None):
    radiant_team_name, dire_team_name = radiant_team_name.lower(), dire_team_name.lower()

    # result = get_team_positions(url)
    # if result is not None:
    if True:
    #     radiant_heroes_and_pos, dire_heroes_and_pos = result
        for pos in radiant_heroes_and_pos:
            hero_name = radiant_heroes_and_pos[pos]['hero_name'].lower()
            for hero in name_to_id:
                if hero.lower() == hero_name:
                    radiant_heroes_and_pos[pos]['hero_id'] = name_to_id[hero]
        for pos in dire_heroes_and_pos:
            hero_name = dire_heroes_and_pos[pos]['hero_name'].lower()
            for hero in name_to_id:
                if hero.lower() == hero_name:
                    dire_heroes_and_pos[pos]['hero_id'] = name_to_id[hero]
        output_message += f'{radiant_team_name} VS {dire_team_name}\nСчет: {score}\n{url}\n'
        print(f'{radiant_team_name} VS {dire_team_name}\nСчет: {score}\n')
        # if url != '':
        if True:
            avg_kills, avg_time, solo_kills, solo_time = tm_kills_teams(radiant_heroes_and_pos=radiant_heroes_and_pos,
                                   dire_heroes_and_pos=dire_heroes_and_pos,
                                   radiant_team_name=radiant_team_name,
                                   dire_team_name=dire_team_name, min_len=2)
            def sum_if_none(n1, n2):
                if all(i is None for i in [n1, n2]):
                    return None
                elif any(i is None for i in [n1, n2]):
                    c = 0
                    for i in [n1, n2]:
                        if i is not None:
                            c += i
                else:
                    return (n1+n2)/2

            kills = sum_if_none(solo_kills, avg_kills)
            time = sum_if_none(solo_time,avg_time)
            output_message += f'avg_kills: {kills}\nAvg time: {time}\n\n'
        output_message += calculate_over40(radiant_heroes_and_pos, dire_heroes_and_pos)
        output_message += calculate_lanes(radiant_heroes_and_pos, dire_heroes_and_pos)
        output_message += synergy_and_counterpick_new(
            radiant_heroes_and_pos=radiant_heroes_and_pos,
            dire_heroes_and_pos=dire_heroes_and_pos)
        # output_message += dota2protracker_gpt(
        #     radiant_heroes_and_positions=radiant_heroes_and_pos,
        #     dire_heroes_and_positions=dire_heroes_and_pos)
        # # result = None
        # try:
            # if score in [[0, 0], [1, 1]]:
            #     update_pro(show_prints=True)
            #     update_all_teams(show_prints=True)

        # except TypeError:
        #     try:
        #         result = tm_kills_teams(radiant_heroes_and_pos=radiant_heroes_and_pos,
        #                                 dire_heroes_and_pos=dire_heroes_and_pos,
        #                                 radiant_team_name=radiant_team_name,
        #                                 dire_team_name=dire_team_name, min_len=1)
        #
        #     except TypeError:
        #         if tier == 3:
        #             avg_kills, avg_time = tm_kills(radiant_heroes_and_pos, dire_heroes_and_pos)
        #             output_message += (
        #                 f'\nСреднее кол-во убийств: {avg_kills}\nСреднее время: {avg_time}'
        #                 f'\nОпасная ставка на время!')
        #         else:
        #             print('не удалось выяснить кол-во килов у команд')
        # if result is not None:
        #     avg_kills_teams, avg_time_teams = result
        #     if tier == 1:
        #         avg_kills, avg_time = tm_kills(radiant_heroes_and_pos, dire_heroes_and_pos)
        #         output_message += (
        #             f'\nСреднее кол-во убийств: {(avg_kills + avg_kills_teams) / 2}\n'
        #             f'Командное: {avg_kills_teams}\nОбщее: {avg_kills}')
        #     else:
        #         output_message += (
        #             f'\nСреднее кол-во убийств: {avg_kills_teams}\n'
        #             f'Среднее время: {avg_time_teams}\n')
        # print(output_message)
        send_message(output_message)
        add_url(url, score)


if __name__ == "__main__":
    # update_pro(show_prints=True)
    # update_all_teams(show_prints=True)
    update_my_protracker(show_prints=True)
    # update_all_teams(show_prints=True, only_in_ids=['BetBoom Team', 'Team Liquid'])
    # general(['https://cyberscore.live/en/matches/112515/'], radiant_team_name='Aurora Gaming', dire_team_name='Beastcoast', tier=1)
    # #
    # proceed_map(url='', radiant_team_name='Talon Esports', dire_team_name="Navi junior", score=[0, 0], tier=1,
    #             radiant_heroes_and_pos={'pos1': {'hero_name': "gyrocopter"}, 'pos2': {'hero_name': "earth spirit"},
    #                                      'pos3': {'hero_name': 'magnus'}, 'pos4': {'hero_name': 'muerta'},
    #                                      'pos5': {'hero_name': 'phoenix'}},
    #             dire_heroes_and_pos= {'pos1': {'hero_name': "sven"}, 'pos2': {'hero_name': "sniper"},
    #                                     'pos3': {'hero_name': 'dawnbreaker'}, 'pos4': {'hero_name': 'lion'},
    #                                     'pos5': {'hero_name': "ogre magi"}},
    #             output_message='')

    #
    while True:
        try:
            # update_pro(show_prints=True)
            draft = general()
        except:
            error_traceback = traceback.format_exc()
            print(error_traceback)
            time.sleep(180)
        if draft == True:
            print('Сплю минуту')
            time.sleep(60)
        else:
            print('Сплю 10 минут')
            time.sleep(600)
