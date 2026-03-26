import json
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


def _load_env_files() -> None:
    if load_dotenv is None:
        return

    repo_root = Path(__file__).resolve().parents[1]
    candidates = (
        repo_root / ".env",
        repo_root / ".env.local",
    )
    for env_path in candidates:
        if env_path.exists():
            load_dotenv(env_path, override=False)


def _env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip()


def _normalize_proxy_url(raw_value: str) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""
    if "://" in value:
        return value
    if "@" in value:
        host_port, user_pass = value.split("@", 1)
        if ":" in host_port and ":" in user_pass:
            host, port = host_port.rsplit(":", 1)
            user, password = user_pass.split(":", 1)
            return f"http://{user}:{password}@{host}:{port}"
    return value


def _load_proxy_api_map(default_proxy_url: str) -> dict[str, str]:
    raw_json = _env("STRATZ_PROXY_MAP_JSON", "")
    if raw_json:
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid STRATZ_PROXY_MAP_JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise RuntimeError("STRATZ_PROXY_MAP_JSON must decode to a JSON object")

        proxy_map: dict[str, str] = {}
        for raw_proxy_url, raw_token in parsed.items():
            proxy_url = _normalize_proxy_url(str(raw_proxy_url or ""))
            token = str(raw_token or "").strip()
            if proxy_url and token:
                proxy_map[proxy_url] = token
        return proxy_map

    proxy_url = _normalize_proxy_url(_env("STRATZ_PROXY_URL", default_proxy_url))
    api_token = _env("STRATZ_API_TOKEN", "")
    if proxy_url and api_token:
        return {proxy_url: api_token}
    return {}


_load_env_files()


Token = _env("TELEGRAM_BOT_TOKEN", "")
Chat_id = _env("TELEGRAM_CHAT_ID", "")
STEAM_API_KEY = _env("STEAM_API_KEY", "")

start_date_time_739 = _env("START_DATE_TIME_739", "1747872000")
start_date_time_738 = _env("START_DATE_TIME_738", "1740096000")
start_date_time_736 = _env("START_DATE_TIME_736", "1")
start_date_time = _env("START_DATE_TIME", "1747872000")

BOOKMAKER_PROXY_RAW = _env("BOOKMAKER_PROXY_RAW", "")
BOOKMAKER_PROXY_URL = _normalize_proxy_url(_env("BOOKMAKER_PROXY_URL", BOOKMAKER_PROXY_RAW))
BOOKMAKER_PROXIES = (
    {
        "http": BOOKMAKER_PROXY_URL,
        "https": BOOKMAKER_PROXY_URL,
    }
    if BOOKMAKER_PROXY_URL
    else {}
)

api_to_proxy = _load_proxy_api_map(BOOKMAKER_PROXY_URL)
