url = 'https://yandex.ru/internet'
import requests
import bs4
import random
proxies_list = [
    "socks5://66.29.154.105:1080",
]
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
}
proxy = random.choice(proxies_list)
proxies = {
    "http": proxy,
    "https": proxy,
}
try:
    response = requests.get(url, proxies=proxies, headers=headers, timeout=5, verify=False)
    soup = bs4.BeautifulSoup(response.text, 'lxml')
    ip=soup.find('div', class_='list-info__renderer').text
    print(f'мой ip: {ip}')
except Exception as e:
    print(f"Ошибка при использовании прокси {proxy}: {e}")