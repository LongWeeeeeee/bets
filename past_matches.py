import time, requests, json
from multiprocessing.connection import answer_challenge
import os

from bs4 import BeautifulSoup
import urllib3
from datetime import datetime, timedelta
from maps_research import research_maps, eat_temp_files
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# os.environ['HTTPS'] = 'localhost,127.0.0.1'
# os.environ['HTTPS'] = "http://FdZ59jIXvS:pS11ypMDJG@193.168.224.92:46203"
# os.environ['NO_PROXY'] = "http://FdZ59jIXvS:pS11ypMDJG@193.168.224.92:46203"
def get_past_matches():

# Today's date in the format YYYY-MM-DD
    def get_matches():

        start_date = datetime.now().strftime('%Y-%m-%d')


        # Target date to stop the loop
        target_date = '2025-05-31'

        # Convert start_date and target_date to datetime objects
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        target_date_obj = datetime.strptime(target_date, '%Y-%m-%d')
        formatted_date = current_date.strftime('%Y-%m-%d')
        # Loop to print each date from start_date to target_date
        database = []
        while current_date > target_date_obj:
            print(current_date.strftime('%Y-%m-%d'))
            current_date -= timedelta(days=1)
            ip_address = "46.229.214.49"
            path = f"/results?date={current_date}"  # Desired path

            # URL with the IP address and the specific path
            url = f"https://{ip_address}{path}"
            headers = {
                "Host": "dltv.org",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
                "Referer": 'https://dltv.org/results',
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/130.0.0.0 Safari/537.36", }
            for i in range(3):
                response = requests.get(url, headers=headers, verify=False)
                if response.status_code == 200:

                    break
                elif response.status_code == 429:
                    time.sleep(15)
                else:
                    print(response.status_code)
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
                title = soup.find('section', class_='event__title').find('a').text
                if not any(i in title.lower() for i in ['dreamleague', 'blast', 'betboom',
                                                        'fissure', 'pgl', 'esports world', 'international',
                                                        'esl', 'clavision']):
                    continue
                maps = soup.find_all('div', class_='map__finished-v2')
                for game_map in maps:

                    match_id = game_map.find('small')
                    if not match_id:
                        continue

                    match_id = match_id.text.replace('Match ID: ', '')
                    if match_id not in database:
                        database.append(match_id)
        with open('dltv/past_matches_maps.txt', 'w') as f:
            json.dump(database, f)


    get_matches()
    mkdir='dltv'
    maps_to_explore='past_matches_maps'
    file_name='dltv_output'
    maps = []
    with open(f'./{mkdir}/{maps_to_explore}.txt') as f:
        maps = json.load(f)
    research_maps(mkdir='dltv', maps_to_explore=maps,
                  file_name='dltv_output', pro=True)
    path = f'./{mkdir}/{maps_to_explore}.txt'
    with open(path, 'r+') as f:
        maps_to_explore = json.load(f)
    try:
        with open(f'./{mkdir}/{file_name}.txt', 'r+') as f:
            file_data = json.load(f)
    except FileNotFoundError:
        with open(f'./{mkdir}/{file_name}.txt', 'w') as f:
            json.dump([], f)
        file_data = {}
    eat_temp_files(mkdir='dltv', file_data=file_data, file_name=file_name)



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
    wrong_maps = []
    for investigation in ['over40_1vs2', 'over40_duo_synergy', 'over40_duo_counterpick', 'over40_solo', 'over40_trio', 'pos1_matchup', 'duo_diff', 'radiant_counterpick_1vs2', 'radiant_synergy_trio', 'support_dif', 'synergy_duo']:
        for index in range(1, 101, 1):
            win = 0
            loose = 0
            for match in data:
                if match[investigation] is None:
                    continue
                if 'over40' in investigation:
                    if match.get('radiantNetworthLeads') is not None and len(match['radiantNetworthLeads']) >= 41:
                            pass
                    else:
                        continue
                else:
                    if len(match.get('radiantNetworthLeads', [])) <= 40:
                            pass
                    else:
                        continue
                if match[investigation] == index and match['didRadiantWin'] == True:
                    win += 1
                elif match[investigation] == -index and match['didRadiantWin'] == False:
                    win += 1
                elif match[investigation] == -index and match['didRadiantWin'] == True:
                    loose += 1
                    if index >= 17 and investigation == 'pos1_matchup':
                        print(f'{index}, {match["match_id"]}')
                elif match[investigation] == index and match['didRadiantWin'] == False:
                    loose += 1
                    if index >= 17 and investigation == 'pos1_matchup':
                        print(f'{index}, {match["match_id"]}')
            if win + loose > 5:
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
    return wrong_maps
def check_two_winrates(data):
    investigation = 'radiant_counterpick_1vs2'
    second_inv = 'duo_diff'
    second_index = 10
    for index in range(1, 101, 1):
        win = 0
        loose = 0
        for match in data:
            if 'over40' in investigation:
                if match['duration'] / 60 < 35:
                    continue
            else:
                if match.get('radiantNetworthLeads') is not None and len(
                        match.get('radiantNetworthLeads', [])) >= 24:
                    if not -2999 < match['radiantNetworthLeads'][10] < 2999:
                        continue
            if match[investigation] is not None and match[investigation] <= -index and match['didRadiantWin'] == True:
                if match[second_inv] is not None and match[second_inv] <= -second_index:
                        loose += 1
            elif match[investigation] is not None and match[investigation] <= -index and match[
                'didRadiantWin'] == False:
                if match[second_inv] is not None and match[second_inv] <= -second_index:
                        win += 1
            elif match[investigation] is not None and match[investigation] >= index and match['didRadiantWin'] == True:
                if match[second_inv] is not None and match[second_inv] >= second_index:
                        win += 1
            elif match[investigation] is not None and match[investigation] >= index and match['didRadiantWin'] == False:
                if match[second_inv] is not None and match[second_inv] >= second_index:
                        loose += 1
        if win + loose > 5:
            if win == 0:
                print(f'Index: {index} {investigation} winrate: 0, Всего: {win + loose}')
            else:
                print(
                    f'Index: {index} {investigation} winrate: {win / (win + loose) * 100} Всего: {win + loose}')


def analyse_winrates():
    with open('dltv/cyberscore_ouput_classic.txt', 'r') as f:
        data = json.load(f)
        # check_two_winrates(data)
        wrong_maps = check_winrate(data)
        # check_lines(data)





# get_past_matches()
analyse_winrates()


