import importlib
from functions import if_unique, add_url, calculate_lanes, calculate_over40,\
    send_message, synergy_and_counterpick_new
import json
import traceback
import requests
import time
from fuckkk import dota2protracker_gpt
import id_to_name
from egb import get_players, get_picks_and_pos, get_exac_match, get_strats_graph_match,\
    check_players_skill
url = "https://egb.com/bets"
params = {
    "active": "true",
    "st": "1714418601",
    "ut": "1714418584"
}

headers = {
    "Host": "egb.com",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/115.0",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.5",
    "Content-Type": "application/json",
    "X-CSRF-Token": "UUu1IL8sY0iZFFc2_FYI4ICk-WT34IRRGjz19DND8CbKEJZ9zTvbeAdcw72OYEecZiDlBaZimaYP-VuJwtmkAQ",
    "DNT": "1",
    "Sec-GPC": "1",
}


def main(match=False):
    # for name in id_to_name.egb:
    #
    # egb_player =

    foo = {}
    score = [0, 0]
    with open('./egb/blacklist.txt', 'r+') as f:
        blacklist = json.load(f)
    importlib.reload(id_to_name)
    response = requests.get(url, params=params, headers=headers)
    data = json.loads(response.text)
    for bet in data['bets']:
        if ('map ' not in bet['gamer_1']['nick'].lower() or 'map '
                not in bet['gamer_2']['nick'].lower())\
                and bet['streams_enabled'] and bet['game'] == 'Dota 2':
            if bet['id'] not in foo:
                foo.setdefault(bet['id'], {}).setdefault('counter', 0)
            answer = get_players(bet, blacklist)
            if type(answer) == list:
                blacklist = answer
                continue
            elif answer is None:
                continue
            elif answer == True:
                continue
            players_ids, dire_and_radiant, title = answer
            response = get_strats_graph_match()
            if response is None:
                continue
            exact_match = get_exac_match(response, players_ids)
            if exact_match is None:
                print('Карта не найдена, вероятно матч только начался')
                match = True
                foo[bet['id']]['counter'] = foo[bet['id']]['counter'] + 1
                if foo[bet['id']]['counter'] > 10:
                    print('stratz залагал')
                    return False
                else:
                    continue
            elif exact_match == False:
                print('stratz залагал')
                return False

            match_id = exact_match['matchId']
            if not if_unique(match_id, score):
                print('Матч уже рассчитан')
                continue

            answer = get_picks_and_pos(match_id)
            if answer is None:
                print('Не удалось выяснить пики')
                match = True
                continue
            elif answer == False:
                add_url(url=match_id, score=[0, 0])
                return
            radiant_heroes_and_pos, dire_heroes_and_pos, match_id, output_message = answer
            output_message += f"{title}\nhttps://stratz.com/matches/{exact_match['matchId']}/live\n\n"
            output_message += check_players_skill(radiant_heroes_and_pos, dire_heroes_and_pos)
            output_message += calculate_over40(radiant_heroes_and_pos, dire_heroes_and_pos)
            output_message += calculate_lanes(radiant_heroes_and_pos, dire_heroes_and_pos)
            output_message += synergy_and_counterpick_new(
                radiant_heroes_and_pos=radiant_heroes_and_pos,
                dire_heroes_and_pos=dire_heroes_and_pos)

            send_message(output_message)
            add_url(url=match_id, score=[0,0])
    with open('./egb/blacklist.txt', 'r+') as f:
        json.dump(blacklist, f)
    if match:
        print('сплю 60 секунд')
        time.sleep(60)
    else:
        print('сплю 5 минут')
        time.sleep(300)


if __name__ == "__main__":
    while True:
        try:
            response = main()
            if response == False:
                print('сплю 5 минут')
                time.sleep(300)
        except:
            error_traceback = traceback.format_exc()
            print(error_traceback)
            print('сплю 3 минуты')
            time.sleep(180)
