import time, requests, json
from bs4 import BeautifulSoup
import urllib3
from datetime import datetime, timedelta
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
def get_past_matches():
# Today's date in the format YYYY-MM-DD
    start_date = datetime.now().strftime('%Y-%m-%d')

    # Target date to stop the loop
    target_date = '2024-11-20'

    # Convert start_date and target_date to datetime objects
    current_date = datetime.strptime(start_date, '%Y-%m-%d')
    target_date_obj = datetime.strptime(target_date, '%Y-%m-%d')

    # Loop to print each date from start_date to target_date
    database = []
    while current_date == target_date_obj:
        print(current_date.strftime('%Y-%m-%d'))
        current_date -= timedelta(days=1)
        ip_address = "46.229.214.49"
        path = f"/results?date={current_date}"  # Desired path

        # URL with the IP address and the specific path
        url = f"https://{ip_address}{path}"
        proxies = {
            'https': 'http://90gwi7LEfz:aKI0jgSViq@77.221.150.248:42037',
        }
        headers = {
                    "Host": "dltv.org",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                    "Accept-Encoding": "gzip, deflate, br, zstd",
                    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
                    "Referer": 'https://dltv.org/results',
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                                  "Chrome/130.0.0.0 Safari/537.36",}
        for i in range(3):
            response = requests.get(url, headers=headers, verify=False)
            if response.status_code == 200:
                break
            else:
                time.sleep(60)
        if response.status_code != 200:
            print(response.status_code)
            break
        soup = BeautifulSoup(response.text, 'lxml')
        matches_blocks = soup.find_all('div', class_='result__matches-item__body')

        for match in matches_blocks:
            path = match.find('a')['href'].replace('https://dltv.org', '')
            match_url = f"https://{ip_address}{path}"
            for i in range(3):
                response = requests.get(match_url, headers=headers, verify=False)
                if response.status_code == 200:
                    break
                else:
                    time.sleep(60)
            if response.status_code != 200:
                print(response.status_code)
                break
            soup = BeautifulSoup(response.text, 'lxml')
            maps = soup.find_all('div', class_='map__finished-v2')
            for game_map in maps:
                match_id = game_map.find('small')
                if not match_id:
                    continue
                match_id = match_id.text.replace('Match ID: ', '')
                radiant_heroes_and_pos, dire_heroes_and_pos, winner_side, break_flag  = {}, {}, None, False
                picks = game_map.find('div', class_='map__finished-v2__pick')
                teams = game_map.find_all('div', class_='team')
                for team in teams:
                    winner = team.find('div', class_='winner')
                    side = team.find('span', class_='side').text
                    if winner:
                        winner_side = side.lower()
                heroes = picks.find_all('div', class_="heroes__player")
                if len(heroes) != 10:
                    break
                for i in range(0,5):
                    hero_name = heroes[i].find('div', class_='pick')['data-tippy-content']
                    pos = heroes[i].find('div', class_="pick__role")
                    if not pos:
                        break_flag = True
                        break
                    pos = 'pos' + pos.get('class')[1].replace('role__bg-', '')

                    radiant_heroes_and_pos[pos] = {'hero_name': hero_name}
                if break_flag: break
                for i in range(5, 10):
                    hero_name = heroes[i].find('div', class_='pick')['data-tippy-content']
                    pos = heroes[i].find('div', class_="pick__role")
                    if not pos:
                        break_flag = True
                        break
                    pos = 'pos' + pos.get('class')[1].replace('role__bg-', '')
                    dire_heroes_and_pos[pos] = {'hero_name': hero_name}
                if break_flag: break
                if winner_side and radiant_heroes_and_pos and dire_heroes_and_pos:
                    database.append({'winner': winner_side, 'radiant_heroes_and_pos': radiant_heroes_and_pos, 'dire_heroes_and_pos': dire_heroes_and_pos, 'match_id': match_id})
        with open('past_matches_maps.txt', 'w') as f:
            json.dump(database, f)


def check_lines(data):
    for percentage in range(33, 100):
        bot_loose, bot_win, top_loose, top_win, mid_loose, mid_win = 0, 0, 0, 0, 0, 0
    # percentage = 99
        # investigation = 'support_dif'
        # second_inv = 'bot_key'
        # third_inv = 'mid_key'

        for match in data:
            if 'None' not in match['top_message']:
                top = match['top_message'].replace('Top: ', '').replace('%\n', '').split(' ')
                top_value = int(top[1])
                top_key = top[0]
            if 'None' not in match['bot_message']:
                bot = match['bot_message'].replace('Bot: ', '').replace('%\n\n', '').split(' ')
                bot_value = int(bot[1])
                bot_key = bot[0]
            if 'None' not in match['mid_message']:
                mid = match['mid_message'].replace('Mid: ', '').replace('%\n', '').split(' ')
                mid_value = int(mid[1])
                mid_key = mid[0]
            bot_result, top_result, mid_result = match['bottomLaneOutcome'], match['topLaneOutcome'], match['midLaneOutcome']
            if match is None:
                continue
            if 'None' not in match['bot_message'] and bot_value == percentage:
                if bot_result == 'TIE' and bot_key == 'draw':
                    bot_win += 1
                elif bot_result in ['RADIANT_VICTORY', 'RADIANT_STOMP'] and bot_key == 'win':
                    bot_win += 1
                elif bot_result in ['DIRE_VICTORY', 'DIRE_STOMP'] and bot_key == 'loose':
                    bot_win += 1
                else:
                    bot_loose +=1
            if 'None' not in match['top_message'] and top_value == percentage:
                if top_result == 'TIE' and top_key == 'draw':
                    top_win += 1
                elif top_result in ['RADIANT_VICTORY', 'RADIANT_STOMP'] and top_key == 'win':
                    top_win += 1
                elif top_result in ['DIRE_VICTORY', 'DIRE_STOMP'] and top_key == 'loose':
                    top_win += 1
                else:
                    top_loose += 1
            if 'None' not in match['mid_message'] and mid_value == percentage:
                if mid_result == 'TIE' and mid_key == 'draw':
                    mid_win += 1
                elif mid_result in ['RADIANT_VICTORY', 'RADIANT_STOMP'] and mid_key == 'win':
                    mid_win += 1
                elif mid_result in ['DIRE_VICTORY', 'DIRE_STOMP'] and mid_key == 'loose':
                    mid_win += 1
                else:
                    mid_loose += 1
        if (bot_loose + bot_win != 0) and (top_win + top_loose != 0) and mid_win + mid_loose != 0:
            print(
                f'Percentage: {percentage}\n'
                f'Bot winrate: {(bot_win / (bot_loose + bot_win)) * 100} всего: {bot_loose+bot_win}\n'
                f'Top winrate: {(top_win / (top_win + top_loose)) * 100} всего: {top_win + top_loose}\n'
                f'Mid winrate: {(mid_win / (mid_win + mid_loose)) * 100} всего: {mid_win + mid_loose}')


def check_winrate(data):
    for investigation in [
        'support_dif',
        'synergy_duo','radiant_synergy_trio',
        'duo_diff', 'radiant_counterpick_1vs2',
        'pos1_matchup',
        "over40_duo", 'over40_duo_counterpick',
        'over40_1vs2', 'over40_solo',
        'over40_trio']:
        for index in range(1, 101, 1):
            win = 0
            loose = 0
            for match in data:
                if 'over40' in investigation:
                    if match['duration'] < 40:
                        continue
                if match[investigation] is not None and match[investigation] == index and match['didRadiantWin'] == True:
                    win += 1
                elif match[investigation] is not None and match[investigation] == -index and match['didRadiantWin'] == False:
                    win += 1
                elif match[investigation] is not None and match[investigation] == -index and match['didRadiantWin'] == True:
                    loose += 1
                elif match[investigation] is not None and match[investigation] == index and match['didRadiantWin'] == False:
                    loose += 1
            if win + loose > 0:
                if win == 0:
                    print(f'Index: {index} {investigation} winrate: 0, Всего: {win + loose}')
                else:
                    print(
                        f'Index: {index} {investigation} winrate: {win / (win + loose) * 100} Всего: {win + loose}')
            # if match[investigation] == 'win' and match[investigation+'_value'] > index and match['winner'] == 'radiant'\
            #         and match[second_inv] is not None and match[second_inv] == 'win' and match[second_inv+'_value'] > index \
            #         and match[third_inv] == 'win' and match[third_inv + '_value'] > index:
            #
            #     win += 1
            # elif match[investigation] == 'win' and match[investigation+'_value'] > index and match['winner'] == 'dire'\
            #         and match[second_inv] is not None and match[second_inv] == 'win' and match[second_inv + '_value'] > index \
            #         and match[third_inv] == 'win' and match[third_inv + '_value'] > index:
            #
            #     loose += 1
            # elif match[investigation] == 'loose' and match[investigation+'_value'] > index and match['winner'] == 'dire'\
            #         and match[second_inv] is not None and match[second_inv] == 'loose' and match[second_inv + '_value'] > index \
            #         and match[third_inv] == 'loose' and match[third_inv + '_value'] > index:
            #
            #     win += 1
            # elif match[investigation] == 'loose' and match[investigation+'_value'] > index and match['winner'] == 'radiant'\
            #         and match[second_inv] is not None and match[second_inv] == 'loose' and match[second_inv + '_value'] > index \
            #         and match[third_inv] == 'loose' and match[third_inv + '_value'] > index:
            #
            #     loose += 1

def check_two_winrates(data):
    investigation = 'radiant_counterpick_1vs2'
    second_inv = 'pos1_matchup'
    second_index = 14
    third_inv = 'pos1_matchup'
    third_index = 14
    for index in range(1, 101, 1):
        win = 0
        loose = 0
        for match in data:
            if 'over40' in investigation:
                if match['duration'] < 40:
                    continue
            if match[investigation] is not None and match[investigation] == index and match['didRadiantWin'] == True:
                if match[second_inv] is not None and match[second_inv] == -second_index:
                        win += 1
            elif match[investigation] is not None and match[investigation] == -index and match[
                'didRadiantWin'] == False:
                if match[second_inv] is not None and match[second_inv] == second_index:
                        win += 1
            elif match[investigation] is not None and match[investigation] == -index and match['didRadiantWin'] == True:
                if match[second_inv] is not None and match[second_inv] == second_index:
                        loose += 1
            elif match[investigation] is not None and match[investigation] == index and match['didRadiantWin'] == False:
                if match[second_inv] is not None and match[second_inv] == -second_index:
                        loose += 1
        if win + loose > 0:
            if win == 0:
                print(f'Index: {index} {investigation} winrate: 0, Всего: {win + loose}')
            else:
                print(
                    f'Index: {index} {investigation} winrate: {win / (win + loose) * 100} Всего: {win + loose}')


def analyse_winrates():
    with open('dltv_analysed_maps_output.txt', 'r') as f:
        data = json.load(f)
        # check_two_winrates(data)
        # check_winrate(data)
        check_lines(data)






# get_past_matches()
analyse_winrates()