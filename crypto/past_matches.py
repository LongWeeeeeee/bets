import re, requests, json
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# Today's date in the format YYYY-MM-DD
start_date = datetime.now().strftime('%Y-%m-%d')

# Target date to stop the loop
target_date = '2024-10-03'

# Convert start_date and target_date to datetime objects
current_date = datetime.strptime(start_date, '%Y-%m-%d')
target_date_obj = datetime.strptime(target_date, '%Y-%m-%d')

# Loop to print each date from start_date to target_date
while current_date >= target_date_obj:
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
    response = requests.get(url, headers=headers, verify=False, proxies=proxies)
    if response.status_code != 200:
        print(response.status_code)
        continue
    soup = BeautifulSoup(response.text, 'lxml')
    matches_blocks = soup.find_all('div', class_='result__matches-item__body')
    database = []
    for match in matches_blocks:
        path = match.find('a')['href'].replace('https://dltv.org', '')
        match_url = f"https://{ip_address}{path}"
        response = requests.get(match_url, headers=headers, verify=False, proxies=proxies)
        if response.status_code != 200:
            print(response.status_code)
            continue
        soup = BeautifulSoup(response.text, 'lxml')
        maps = soup.find_all('div', class_='map__finished-v2')
        for game_map in maps:
            radiant_heroes_and_pos, dire_heroes_and_pos, winner_side = {}, {}, None
            picks = game_map.find('div', class_='map__finished-v2__pick')
            teams = game_map.find_all('div', class_='team')
            for team in teams:
                winner = team.find('div', class_='winner')
                side = team.find('span', class_='side').text
                if winner:
                    winner_side = side.lower()
            heroes = picks.find_all('div', class_="heroes__player")
            for i in range(0,5):
                hero_name = heroes[i].find('div', class_='pick')['data-tippy-content']
                pos = 'pos' + heroes[i].find('div', class_="pick__role").get('class')[1].replace('role__bg-', '')
                radiant_heroes_and_pos[pos] = {'hero_name': hero_name}
            for i in range(5, 10):
                hero_name = heroes[i].find('div', class_='pick')['data-tippy-content']
                pos = 'pos' + heroes[i].find('div', class_="pick__role").get('class')[1].replace('role__bg-', '')
                dire_heroes_and_pos[pos] = {'hero_name': hero_name}
            if winner_side and radiant_heroes_and_pos and dire_heroes_and_pos:
                database.append({'winner': winner_side, 'radiant_heroes_and_pos': radiant_heroes_and_pos, 'dire_heroes_and_pos': dire_heroes_and_pos})
    with open('past_matches_maps.txt', 'w') as f:
        json.dump(database, f)