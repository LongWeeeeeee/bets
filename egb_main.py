import importlib
from functions import if_unique, add_url, calculate_lanes, calculate_over40,\
    send_message, synergy_and_counterpick_new
import json
import traceback
import requests
import time
import id_to_name
from keys import api_token_3, api_token_4, api_token_5, api_token_2, api_token_1, api_token_6, api_token_7, \
                      api_token_8, api_token_9, api_token_10, api_token_11, api_token_12, api_token_13, \
                      api_token_14, api_token_15, api_token_16, api_token_17
from egb import get_players, get_picks_and_pos, get_strats_graph_match

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


def main(match=False, lags=False):
    tokens = [api_token_3, api_token_4, api_token_5, api_token_2, api_token_1, api_token_6, api_token_7,
              api_token_8, api_token_9, api_token_10, api_token_11, api_token_12, api_token_13,
              api_token_14,
              api_token_15, api_token_16, api_token_17]
    api_token = tokens.pop(0)
    # for name in id_to_name.egb:
    #
    # egb_player =

    foo = {}
    score = [0, 0]
    with open('./egb/blacklist.txt', 'r+') as f:
        blacklist = json.load(f)
    importlib.reload(id_to_name)
    proxies = {
        'https': 'http://90gwi7LEfz:aKI0jgSViq@77.221.150.248:42037',
    }
    try:
        response = requests.get(url, params=params, headers=headers, proxies=proxies)
        data = json.loads(response.text)
    except Exception as e:
        print(e)
        return True, lags

    uniq = set()
    for bet in data['bets']:
        if ('map ' not in bet['gamer_1']['nick'].lower() or 'map '
                not in bet['gamer_2']['nick'].lower())\
                and bet['streams_enabled'] and bet['game'] == 'Dota 2':
            if bet['game_label'] not in uniq:
                uniq.add(bet['game_label'])
            else:
                continue
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
            answer = get_strats_graph_match(players_ids=players_ids, tokens=tokens,
                                            api_token=api_token, proxies=proxies)
            if answer is None:
                print('Карта не найдена, вероятно матч только начался')
                match = True
                foo[bet['id']]['counter'] = foo[bet['id']]['counter'] + 1
                if foo[bet['id']]['counter'] > 10:
                    print('stratz залагал')
                    lags = True
                    continue
                else:
                    continue
            elif answer == False:
                print('stratz залагал')
                lags = True
                continue
            else:
                exac_match_id, api_token = answer
                if exac_match_id not in foo:
                    foo[exac_match_id] = 1
            if not if_unique(exac_match_id, score):
                print('Матч уже рассчитан')
                continue
            if foo[exac_match_id] > 10:
                print('stratz залагал')
                lags = True
                continue
            answer = get_picks_and_pos(match_id=exac_match_id, tokens=tokens, api_token=api_token, proxies=proxies)
            if answer is None:
                print('Не удалось выяснить пики')
                match = True
                foo[exac_match_id] = foo[exac_match_id] + 1
                continue
            elif answer == False:
                add_url(url=exac_match_id, score=[0, 0])
                return
            elif answer == True:
                print('stratz залагал')
                continue
            radiant_heroes_and_pos, dire_heroes_and_pos, match_id, output_message = answer
            output_message += f"{title}\nhttps://stratz.com/matches/{exac_match_id}/live\n\n"
            # output_message += check_players_skill(radiant_heroes_and_pos, dire_heroes_and_pos)
            output_message += calculate_over40(radiant_heroes_and_pos, dire_heroes_and_pos)
            output_message += calculate_lanes(radiant_heroes_and_pos, dire_heroes_and_pos)
            output_message += synergy_and_counterpick_new(
                radiant_heroes_and_pos=radiant_heroes_and_pos,
                dire_heroes_and_pos=dire_heroes_and_pos)

            send_message(output_message)
            add_url(url=match_id, score=[0,0])
    with open('./egb/blacklist.txt', 'r+') as f:
        json.dump(blacklist, f)
    return match, lags


if __name__ == "__main__":
    while True:
        try:
            response = main()
            match, lags = response
            if match == False:
                print('сплю 5 минут')
                time.sleep(300)
            else:
                match, lags = response
                if match == True:
                    print('сплю 60 секунд')
                    time.sleep(60)
                if lags == True:
                    print('сплю 5 минут')
                    time.sleep(300)

        except:
            error_traceback = traceback.format_exc()
            print(error_traceback)
            print('сплю 3 минуты')
            time.sleep(180)
