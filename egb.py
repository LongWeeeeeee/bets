import json
import time
import requests
import re
import id_to_name
from functions import if_unique, send_message
from keys import api_token_17 as api_token

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
    "Sec-GPC": "1",}

players_to_add = set()
def get_players(bet, blacklist):
    dire_and_radiant = {}
    players = [re.sub(r'\bteam\s+|\s+team\b', '', player) for player in bet['game_label'].lower().split(' vs ')]
    player_names = [player.replace("'s", '') for player in players if 'enemy' not in player]
    dire_and_radiant['radiant'] = players[0]
    dire_and_radiant['dire'] = players[1]


    players_ids = []
    if len(player_names) == 2:
        if player_names[0] not in id_to_name.blacklist_players and player_names[1] not in id_to_name.blacklist_players:
            if player_names[0] in id_to_name.egb:
                    players_ids.append(id_to_name.egb[player_names[0]])
            elif player_names[1] in id_to_name.egb:
                players_ids.append(id_to_name.egb[player_names[1]])
            else:
                if not all(player in blacklist for player in player_names):
                    blacklist.append(player_names[0])
                    send_message(f'{player_names[0]} не найден')
                    blacklist.append(player_names[1])
                    send_message(f'{player_names[1]} не найден')
                    return blacklist
                return
        else:
            print('blacklisted')
            return True
    elif len(player_names) == 1:
        if player_names[0] not in id_to_name.blacklist_players:
            if player_names[0] in id_to_name.egb:
                players_ids.append(id_to_name.egb[player_names[0]])
            else:
                if player_names[0] not in blacklist:
                    blacklist.append(player_names[0])
                    send_message(f'{player_names[0]} не найден')
                    return blacklist
                return
        else:
            print('blacklisted')
            return
    print(bet['game_label'])
    return players_ids, dire_and_radiant, bet['game_label']


def spread_heroes_left(heroes_left, radiant_hard, radiant_safe, radiant_mid, dire_hard, dire_safe, dire_mid):
    for player in heroes_left:
        if player['isRadiant']:
            if len(radiant_hard) == 1:
                radiant_hard.append(player)
            elif len(radiant_safe) == 1:
                radiant_safe.append(player)
            elif len(radiant_mid) == 0:
                radiant_mid.append(player)
        elif not player['isRadiant']:
            if len(dire_hard) == 1:
                dire_hard.append(player)
            elif len(dire_safe) == 1:
                dire_safe.append(player)
            elif len(dire_mid) == 0:
                dire_mid.append(player)
    return radiant_hard, radiant_safe, radiant_mid, dire_hard, dire_safe, dire_mid


def get_strats_graph_match(map_id=None):
    if map_id is None:
        query = '''{
          live{
            matches(request:{gameStates:GAME_IN_PROGRESS, isParsing:true, isCompleted: false, orderBy:SPECTATOR_COUNT, take: 100}){
              matchId
              players{
                numLastHits
                playbackData{
                  positionEvents{
                    time
                    x
                    y
                  }
                }
                heroId
                steamAccountId
                position
                isRadiant
                steamAccount{
                  name
                }
              }

            }


          }
        }

        '''
    else:
        query = '''{
          live{
            match(id:%s){
              players{
              steamAccountId
            heroId
            isRadiant
            networth
            playbackData{
            goldEvents{
            networth
            time
          }
          csEvents{
            time
          }
          positionEvents{
            time
            x
            y
          }
        }
      }
    }


  }
}
        '''%map_id

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Origin": "https://api.stratz.com",
        "User-Agent": "STRATZ_API",
        "Authorization": f"Bearer {api_token}"
    }
    response = requests.post('https://api.stratz.com/graphql?key=jE1M9jZk28iOXJs2LBfrN3gxWeqfgR2B', json={"query": query}, headers=headers)
    if response.status_code == 200:
        return response
    else:
        print(response.status_code)
        pass


def get_exac_match(response, players_ids, exac_match=None):
    data = json.loads(response.text).get('data', {}).get('live', {}).get('matches', {})
    if data:
        for match in data:
            for player in match['players']:
                if player['steamAccountId'] in players_ids:
                    exac_match = match
                    return exac_match
    else:
        return False

def know_the_position(radiant_safe, check_time, radiant_hard, radiant_mid, dire_safe, dire_hard, dire_mid, heroes_left, radiant, dire, output_message):
    if len(radiant_safe) == 2:
        zero_networth, first_networth, zero_cs, first_cs = 0, 0, 0, 0
        for event in radiant_safe[0]['playbackData']['goldEvents']:
            if event['time'] > check_time:
                zero_networth = event['networth']
                break
        for event in radiant_safe[1]['playbackData']['goldEvents']:
            if event['time'] > check_time:
                first_networth = event['networth']
                break

        for event in radiant_safe[0]['playbackData']['csEvents']:
            if event['time'] < check_time:
                zero_cs += 1
            else:
                break
        for event in radiant_safe[1]['playbackData']['csEvents']:
            if event['time'] < check_time:
                first_cs += 1
            else:
                break
        if zero_networth > first_networth and zero_cs > first_cs:
            radiant['pos1'] = {'hero_id': radiant_safe[0]['heroId'], 'hero_name': id_to_name.translate[radiant_safe[0]['heroId']], 'steamAccountId' : radiant_safe[0]['steamAccountId']}
            radiant['pos5'] = {'hero_id': radiant_safe[1]['heroId'], 'hero_name': id_to_name.translate[radiant_safe[1]['heroId']], 'steamAccountId' : radiant_safe[1]['steamAccountId']}
        elif zero_networth < first_networth and zero_cs < first_cs:
            radiant['pos1'] = {'hero_id': radiant_safe[1]['heroId'] , 'hero_name': id_to_name.translate[radiant_safe[1]['heroId']], 'steamAccountId' : radiant_safe[1]['steamAccountId']}
            radiant['pos5'] = {'hero_id': radiant_safe[0]['heroId'], 'hero_name': id_to_name.translate[radiant_safe[0]['heroId']], 'steamAccountId' : radiant_safe[0]['steamAccountId']}
    if len(radiant_hard) == 2:
        zero_networth, first_networth, zero_cs, first_cs = 0, 0, 0, 0
        for event in radiant_hard[0]['playbackData']['goldEvents']:
            if event['time'] > check_time:
                zero_networth = event['networth']
                break
        for event in radiant_hard[1]['playbackData']['goldEvents']:
            if event['time'] > check_time:
                first_networth = event['networth']
                break
        for event in radiant_hard[0]['playbackData']['csEvents']:
            if event['time'] < check_time:
                zero_cs += 1
            else:
                break
        for event in radiant_hard[1]['playbackData']['csEvents']:
            if event['time'] < check_time:
                first_cs += 1
            else:
                break
        if zero_networth > first_networth and zero_cs > first_cs:
            radiant['pos3'] = {'hero_id': radiant_hard[0]['heroId'], 'hero_name': id_to_name.translate[radiant_hard[0]['heroId']], 'steamAccountId' : radiant_hard[0]['steamAccountId']}
            radiant['pos4'] = {'hero_id': radiant_hard[1]['heroId'], 'hero_name': id_to_name.translate[radiant_hard[1]['heroId']], 'steamAccountId' : radiant_hard[1]['steamAccountId']}
        elif zero_networth < first_networth and zero_cs < first_cs:
            radiant['pos3']= {'hero_id': radiant_hard[1]['heroId'], 'hero_name': id_to_name.translate[radiant_hard[1]['heroId']], 'steamAccountId' : radiant_hard[1]['steamAccountId']}
            radiant['pos4']= {'hero_id': radiant_hard[0]['heroId'], 'hero_name': id_to_name.translate[radiant_hard[0]['heroId']], 'steamAccountId' : radiant_hard[0]['steamAccountId']}
    if len(dire_safe) == 2:
        zero_networth, first_networth, zero_cs, first_cs = 0, 0, 0, 0
        for event in dire_safe[0]['playbackData']['goldEvents']:
            if event['time'] > check_time:
                zero_networth = event['networth']
                break
        for event in dire_safe[1]['playbackData']['goldEvents']:
            if event['time'] > check_time:
                first_networth = event['networth']
                break
        for event in dire_safe[0]['playbackData']['csEvents']:
            if event['time'] < check_time:
                zero_cs += 1
            else:
                break
        for event in dire_safe[1]['playbackData']['csEvents']:
            if event['time'] < check_time:
                first_cs += 1
            else:
                break
        if zero_networth > first_networth and zero_cs > first_cs:
            dire['pos1']= {'hero_id': dire_safe[0]['heroId'], 'hero_name': id_to_name.translate[dire_safe[0]['heroId']], 'steamAccountId' : dire_safe[0]['steamAccountId']}
            dire['pos5']= {'hero_id': dire_safe[1]['heroId'], 'hero_name': id_to_name.translate[dire_safe[1]['heroId']], 'steamAccountId' : dire_safe  [1]['steamAccountId']}
        if zero_networth < first_networth and zero_cs < first_cs:
            dire['pos1']= {'hero_id': dire_safe[1]['heroId'], 'hero_name': id_to_name.translate[dire_safe[1]['heroId']], 'steamAccountId' : dire_safe[1]['steamAccountId']}
            dire['pos5']= {'hero_id': dire_safe[0]['heroId'], 'hero_name': id_to_name.translate[dire_safe[0]['heroId']], 'steamAccountId' : dire_safe[0]['steamAccountId']}
    if len(dire_hard) == 2:
        zero_networth, first_networth, zero_cs, first_cs = 0, 0, 0, 0
        for event in dire_hard[0]['playbackData']['goldEvents']:
            if event['time'] > check_time:
                zero_networth = event['networth']
                break
        for event in dire_hard[1]['playbackData']['goldEvents']:
            if event['time'] > check_time:
                first_networth = event['networth']
                break
        for event in dire_hard[0]['playbackData']['csEvents']:
            if event['time'] < check_time:
                zero_cs += 1
            else:
                break
        for event in dire_hard[1]['playbackData']['csEvents']:
            if event['time'] < check_time:
                first_cs += 1
            else:
                break
        if zero_networth > first_networth and zero_cs > first_cs:
            dire['pos3']= {'hero_id': dire_hard[0]['heroId'], 'hero_name': id_to_name.translate[dire_hard[0]['heroId']], 'steamAccountId' : dire_hard[0]['steamAccountId']}
            dire['pos4']= {'hero_id': dire_hard[1]['heroId'], 'hero_name': id_to_name.translate[dire_hard[1]['heroId']], 'steamAccountId' : dire_hard[1]['steamAccountId']}
        elif zero_networth < first_networth and zero_cs < first_cs:
            dire['pos3']= {'hero_id': dire_hard[1]['heroId'], 'hero_name': id_to_name.translate[dire_hard[1]['heroId']], 'steamAccountId' : dire_hard[1]['steamAccountId']}
            dire['pos4']= {'hero_id': dire_hard[0]['heroId'], 'hero_name': id_to_name.translate[dire_hard[0]['heroId']], 'steamAccountId' : dire_hard[0]['steamAccountId']}
    if len(radiant_mid) == 1:
        radiant['pos2']= {'hero_id': radiant_mid[0]['heroId'], 'hero_name': id_to_name.translate[radiant_mid[0]['heroId']], 'steamAccountId' : radiant_mid[0]['steamAccountId']}
    elif len(radiant_mid) == 2:
        zero_networth, first_networth, zero_cs, first_cs = 0, 0, 0, 0
        for event in radiant_mid[0]['playbackData']['goldEvents']:
            if event['time'] > check_time:
                zero_networth = event['networth']
                break
        for event in radiant_mid[1]['playbackData']['goldEvents']:
            if event['time'] > check_time:
                first_networth = event['networth']
                break
        for event in radiant_mid[0]['playbackData']['csEvents']:
            if event['time'] < check_time:
                zero_cs += 1
            else:
                break
        for event in radiant_mid[1]['playbackData']['csEvents']:
            if event['time'] < check_time:
                first_cs += 1
            else:
                break
        if zero_networth > first_networth and zero_cs > first_cs:
            radiant['pos2']= {'hero_id': radiant_mid[0]['heroId'], 'hero_name': id_to_name.translate[radiant_mid[0]['heroId']], 'steamAccountId' : radiant_mid[0]['steamAccountId']}
            heroes_left.append(radiant_mid[1])
        elif zero_networth < first_networth and zero_cs < first_cs:
            radiant['pos2']= {'hero_id': radiant_mid[1]['heroId'], 'hero_name': id_to_name.translate[radiant_mid[1]['heroId']], 'steamAccountId' : radiant_mid[1]['steamAccountId']}
            heroes_left.append(radiant_mid[0])
    if len(dire_mid) == 1:
        dire['pos2']= {'hero_id': dire_mid[0]['heroId'], 'hero_name': id_to_name.translate[dire_mid[0]['heroId']], 'steamAccountId' : dire_mid[0]['steamAccountId']}
    elif len(dire_mid) == 2:
        zero_networth, first_networth, zero_cs, first_cs = 0, 0, 0, 0
        for event in dire_mid[0]['playbackData']['goldEvents']:
            if event['time'] > check_time:
                zero_networth = event['networth']
                break
        for event in dire_mid[1]['playbackData']['goldEvents']:
            if event['time'] > check_time:
                first_networth = event['networth']
                break
        for event in dire_mid[0]['playbackData']['csEvents']:
            if event['time'] < check_time:
                zero_cs += 1
            else:
                break
        for event in dire_mid[1]['playbackData']['csEvents']:
            if event['time'] < check_time:
                first_cs += 1
            else:
                break
        if zero_networth > first_networth and zero_cs > first_cs:
            dire['pos2']= {'hero_id': dire_mid[0]['heroId'], 'hero_name': id_to_name.translate[dire_mid[0]['heroId']], 'steamAccountId' : dire_mid[0]['steamAccountId']}
            heroes_left.append(dire_mid[1])
        elif zero_networth < first_networth and zero_cs < first_cs:
            dire['pos2']= {'hero_id': dire_mid[1]['heroId'], 'hero_name': id_to_name.translate[dire_mid[1]['heroId']], 'steamAccountId' : dire_mid[1]['steamAccountId']}
            heroes_left.append(dire_mid[0])
    return radiant, dire, heroes_left, output_message




def get_picks(check_time, players):
    radiant, dire, heroes_left, index = {}, {}, [], None
    radiant_hard, radiant_safe, dire_hard, dire_safe, radiant_mid, dire_mid = [], [], [], [], [], []
    for player in players:
        coordinates = player['playbackData']['positionEvents']
        if coordinates[1]['time'] >= 600:
            return
        for time in coordinates:
            if time['time'] >= check_time:
                if time['x'] > 90 and time['x'] < 150 and time['y'] > 110 and time['y'] < 150:
                    if player['isRadiant']:
                        radiant_mid.append(player)
                        break
                    else:
                        dire_mid.append(player)
                        break
                elif time['x'] > 70 and time['x'] < 110 and time['y'] > 130 and time['y'] < 170:
                    if player['isRadiant']:
                        radiant_hard.append(player)
                        break
                    else:
                        dire_safe.append(player)
                        break
                elif time['x'] > 120 and time['x'] < 180 and time['y'] > 70 and time['y'] < 120:
                    if not player['isRadiant']:
                        dire_hard.append(player)
                        break
                    else:
                        radiant_safe.append(player)
                        break
                else:
                    heroes_left.append(player)
                    break
    output_message=''
    radiant_hard, radiant_safe, radiant_mid, dire_hard, dire_safe, dire_mid = spread_heroes_left(heroes_left, radiant_hard, radiant_safe, radiant_mid, dire_hard, dire_safe, dire_mid)
    radiant, dire, heroes_left, output_message = know_the_position(radiant_safe, check_time, radiant_hard, radiant_mid, dire_safe, dire_hard, dire_mid, heroes_left, radiant, dire, output_message)
    radiant_hard, radiant_safe, radiant_mid, dire_hard, dire_safe, dire_mid = spread_heroes_left(heroes_left, radiant_hard, radiant_safe, radiant_mid, dire_hard, dire_safe, dire_mid)
    radiant, dire, heroes_left, output_message = know_the_position(radiant_safe, check_time, radiant_hard, radiant_mid, dire_safe, dire_hard, dire_mid, heroes_left, radiant, dire, output_message)
    positions = ['pos 1', 'pos 2', 'pos 3', 'pos 4', 'pos 5']
    if len(radiant) == 4:
        positions_copy = positions.copy()
        for key in radiant:
            positions_copy.remove(key)
        for player in heroes_left:
            if player['isRadiant']:
                radiant[positions_copy[0]]= {'hero_id':player['heroId'] , 'hero_name': id_to_name.translate[player['heroId']], 'steamAccountId' : player['steamAccountId']}

    if len(dire) == 4:
        positions_copy = positions.copy()
        for key in dire:
            positions_copy.remove(key)
        for player in heroes_left:
            if not player['isRadiant']:
                radiant[positions_copy[0]]= {'hero_id': player['heroId'], 'hero_name': id_to_name.translate[player['heroId']], 'steamAccountId' : player['steamAccountId']}
    return radiant, dire, output_message


def check_player(player, isRadiant, hero_id, pos, radiant_impact, dire_impact, steam_account_id, flag = False):
    for hero_perfomance in player['heroesPerformance']:
        check_hero_id = hero_perfomance['hero']['id']
        for position_score in hero_perfomance['positionScore']:
            if position_score['id'] == pos.replace('pos', 'POSITION_') \
                    and check_hero_id == int(hero_id) and position_score['matchCount'] >= 2:
                impact = position_score['imp']
                if isRadiant:
                    radiant_impact[steam_account_id] = impact
                    flag = True
                else:
                    dire_impact[steam_account_id] = impact
                    flag = True
    return radiant_impact, dire_impact, flag

def get_picks_and_pos(match_id):
    response = get_strats_graph_match(match_id)
    players = json.loads(response.text)['data']['live']['match']['players']
    check_time = 75
    limit = 600
    while check_time < limit:
        result = get_picks(check_time, players)
        if result is not None:
            radiant, dire, output_message = result
        else:
            return False
        radiant_heroes = set([item['hero_name'] for pos, item, in radiant.items()])
        dire_heroes = set([item['hero_name'] for pos, item, in dire.items()])
        if len(radiant_heroes) == 5 and len(dire_heroes) == 5:
            return radiant, dire, match_id, output_message
        else:
            check_time += 15


def check_players_skill(radiant_heroes_and_pos, dire_heroes_and_pos, output_message=''):
    start_time = time.time()

    with open('./egb/players_imp_data.txt', 'r') as f3:
        players_data = json.load(f3)

    R_pos_strng, D_pos_strng = dict(), dict()
    radiant_hidden, dire_hidden, radiant_hidden_found, dire_hidden_found = 0, 0, 0, 0
    radiant_steam_account_ids = [player['steamAccountId'] for player in radiant_heroes_and_pos.values()]
    dire_steam_account_ids = [player['steamAccountId'] for player in dire_heroes_and_pos.values()]
    radiant_hero_ids = [player['hero_id'] for player in radiant_heroes_and_pos.values()]
    dire_hero_ids = [player['hero_id'] for player in dire_heroes_and_pos.values()]
    #radiant
    radiant_impact, dire_impact, players_check = {},{},False
    for hero_ids, steam_account_ids in zip([radiant_hero_ids, dire_hero_ids], [radiant_steam_account_ids, dire_steam_account_ids]):
        player_query = '''
        {
          players(steamAccountIds:%s){
            steamAccount{
              isAnonymous
              id
            }
            heroesPerformance(request:{startDateTime:1716508800, take: 100,gameModeIds:[22,2], heroIds:%s,positionIds:[POSITION_1, POSITION_2, POSITION_3, POSITION_4, POSITION_5]}){
              positionScore{
                matchCount
                id
                imp
              }
              hero{
                id
              }
        
            }
        
        
          }
        }
         '''% (steam_account_ids, hero_ids)
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Origin": "https://api.stratz.com",
            "User-Agent": "STRATZ_API",
            "Authorization": f"Bearer {api_token}"
        }
        response = requests.post('https://api.stratz.com/graphql',
                                 json={"query": player_query}, headers=headers)
        data = json.loads(response.text)
        for player in data['data']['players']:
            steam_account_id = str(player['steamAccount']['id'])
            if int(steam_account_id) in radiant_steam_account_ids:
                isRadiant = True
                for position in radiant_heroes_and_pos:
                    if radiant_heroes_and_pos[position]['steamAccountId'] == int(steam_account_id):
                        hero_id = str(dire_heroes_and_pos[position]['hero_id'])
                        pos = position.replace('POSITION_', 'pos')
            else:
                isRadiant = False
                for position in dire_heroes_and_pos:
                    if dire_heroes_and_pos[position]['steamAccountId'] == int(steam_account_id):
                        hero_id = str(dire_heroes_and_pos[position]['hero_id'])
                        pos = position.replace('POSITION_', 'pos')
            if player['steamAccount']['isAnonymous']:
                if isRadiant: radiant_hidden += 1
                else: dire_hidden += 1
                player_data = players_data.get(steam_account_id, {}).get(hero_id, {}).get(pos.replace('pos', 'POSITION_'), {})
                if len(player_data) > 2:
                    avg_imp = sum(player_data)/len(player_data)
                    if isRadiant:
                        radiant_hidden_found += 1
                        radiant_impact[steam_account_id] = avg_imp
                    else:
                        dire_hidden_found += 1
                        dire_impact[steam_account_id] = avg_imp
                else:
                    if isRadiant:
                        R_pos_strng[
                            steam_account_id] = f'{id_to_name.translate[int(hero_id)]} {pos}' \
                                                f' не играл на этом герое за последний месяц '
                    else:
                        D_pos_strng[
                            steam_account_id] = f'{id_to_name.translate[int(hero_id)]} {pos}' \
                                                f' не играл на этом герое за последний месяц'

            else:
                radiant_impact, dire_impact, flag = check_player(player, isRadiant, hero_id, pos,
                                                                 radiant_impact, dire_impact, steam_account_id)
                if flag is True:
                    continue
                if isRadiant:
                    R_pos_strng[steam_account_id] = f'{id_to_name.translate[int(hero_id)]} {pos}' \
                                                    f' не играл на этом герое за последний месяц'
                else:
                    D_pos_strng[steam_account_id] = f'{id_to_name.translate[int(hero_id)]} {pos}' \
                                                    f' не играл на этом герое за последний месяц'


    if 0 not in [len(dire_impact), len(radiant_impact)]:
        output_message =(f'Radiant:\n· Найдено {len(radiant_impact)}/5 игроков,'
                         f' Найдено: {radiant_hidden_found}/{radiant_hidden} скрытых\n'
                         f'Dire:\n· Найдено {len(dire_impact)}/5 игроков,'
                         f' Найдено: {dire_hidden_found}/{dire_hidden} скрытых\n')

        radiant_average_impact = sum(radiant_impact.values())/len(radiant_impact)
        dire_average_impact = sum(dire_impact.values())/len(dire_impact)
        if radiant_average_impact > dire_average_impact:
            impact_diff = radiant_average_impact - dire_average_impact
            output_message += (f'\nRadiant impact лучше в среднем на {impact_diff}\n\n')
            if len(R_pos_strng) < len(D_pos_strng):
                radiant_impactandplayers = True
                if impact_diff < 0: impact_diff *= -1
                if impact_diff >= 10:
                    radiant_players_check = True
            elif len(R_pos_strng) <= len(D_pos_strng):
                radiant_impactandplayers = True

        elif radiant_average_impact < dire_average_impact:
            impact_diff = dire_average_impact - radiant_average_impact
            output_message += (f'\nDire impact лучше в среднем на {impact_diff}\n\n')
            if len(D_pos_strng) < len(R_pos_strng):
                dire_impactandplayers = True
                if impact_diff < 0: impact_diff *= -1
                if impact_diff >= 10:
                    dire_players_check = True
            elif len(D_pos_strng) <= len(R_pos_strng):
                dire_impactandplayers = True

        else:
            impact_diff = 0
    else:
        impact_diff = None
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Время выполнения функции check_players_skill: {execution_time} секунд")
    return output_message
