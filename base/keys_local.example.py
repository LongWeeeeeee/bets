Token = ""
Chat_id = ""
STEAM_API_KEY = ""

start_date_time_739 = "1747872000"
start_date_time_738 = "1740096000"
start_date_time_736 = "1"
start_date_time = "1747872000"

BOOKMAKER_PROXY_RAW = ""
BOOKMAKER_PROXY_URL = ""
BOOKMAKER_PROXIES = (
    {
        "http": BOOKMAKER_PROXY_URL,
        "https": BOOKMAKER_PROXY_URL,
    }
    if BOOKMAKER_PROXY_URL
    else {}
)

# Format: {proxy_url: stratz_api_token}
api_to_proxy = {}
