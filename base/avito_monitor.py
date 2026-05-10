from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import logging
import os
import re
import signal
import time
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup

try:
    import camoufox
except Exception:  # pragma: no cover - depends on server environment
    camoufox = None

from functions import send_message


logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATE_PATH = Path.home() / ".local" / "state" / "ingame" / "avito_monitor_state.json"
AVITO_STATE_PATH = Path(os.getenv("AVITO_MONITOR_STATE_PATH", str(DEFAULT_STATE_PATH)))
AVITO_LOCK_PATH = Path(os.getenv("AVITO_MONITOR_LOCK_PATH", str(AVITO_STATE_PATH) + ".lock"))

AVITO_DEFAULT_PROXY_URL = os.getenv("AVITO_PROXY_URL", "").strip()
AVITO_POLL_INTERVAL_SECONDS = int(os.getenv("AVITO_POLL_INTERVAL_SECONDS", "600"))
AVITO_ACTIVE_FROM = os.getenv("AVITO_ACTIVE_FROM", "06:00").strip()
AVITO_ACTIVE_UNTIL = os.getenv("AVITO_ACTIVE_UNTIL", "23:59").strip()
AVITO_TIMEZONE = os.getenv("AVITO_MONITOR_TZ", "Europe/Moscow").strip() or "Europe/Moscow"
AVITO_PAGE_TIMEOUT_MS = int(os.getenv("AVITO_PAGE_TIMEOUT_MS", "45000"))
AVITO_SCROLL_STEPS = int(os.getenv("AVITO_SCROLL_STEPS", "0"))
AVITO_CAMOUFOX_LOCALE = os.getenv("AVITO_CAMOUFOX_LOCALE", "ru-RU").strip()
AVITO_CAMOUFOX_OS = os.getenv("AVITO_CAMOUFOX_OS", "windows").strip()
AVITO_CAMOUFOX_HUMANIZE = os.getenv("AVITO_CAMOUFOX_HUMANIZE", "0.7").strip()
AVITO_CAMOUFOX_WINDOW = os.getenv("AVITO_CAMOUFOX_WINDOW", "1366x768").strip()
AVITO_CAMOUFOX_PROFILE_DIR = os.getenv(
    "AVITO_CAMOUFOX_PROFILE_DIR",
    str(Path.home() / ".local" / "state" / "ingame" / "avito_camoufox_profile"),
).strip()
AVITO_CAMOUFOX_BLOCK_WEBRTC = str(os.getenv("AVITO_CAMOUFOX_BLOCK_WEBRTC", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}
AVITO_CAMOUFOX_ENABLE_CACHE = str(os.getenv("AVITO_CAMOUFOX_ENABLE_CACHE", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}
AVITO_CAMOUFOX_GEOIP = str(os.getenv("AVITO_CAMOUFOX_GEOIP", "0")).strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}
AVITO_NOTIFY_ADMIN_ONLY = str(os.getenv("AVITO_NOTIFY_ADMIN_ONLY", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}

ITEM_ID_RE = re.compile(r"_(\d{6,})(?:[/?#]|$)")
AVITO_HOST_RE = re.compile(r"(^|\.)avito\.ru$", re.IGNORECASE)


@dataclass(frozen=True)
class AvitoItem:
    item_id: str
    url: str
    title: str = ""
    price: str = ""


def _now_iso() -> str:
    return datetime.now(tz=ZoneInfo(AVITO_TIMEZONE)).isoformat(timespec="seconds")


def _default_state() -> dict[str, Any]:
    return {"version": 1, "watches": []}


@contextlib.contextmanager
def _state_lock():
    AVITO_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    with AVITO_LOCK_PATH.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _read_state_unlocked() -> dict[str, Any]:
    try:
        raw = AVITO_STATE_PATH.read_text(encoding="utf-8")
    except FileNotFoundError:
        return _default_state()
    except OSError as exc:
        logger.warning("Failed to read Avito monitor state %s: %s", AVITO_STATE_PATH, exc)
        return _default_state()
    if not raw.strip():
        return _default_state()
    try:
        data = json.loads(raw)
    except ValueError as exc:
        logger.warning("Failed to parse Avito monitor state %s: %s", AVITO_STATE_PATH, exc)
        return _default_state()
    if not isinstance(data, dict):
        return _default_state()
    watches = data.get("watches")
    if not isinstance(watches, list):
        data["watches"] = []
    data.setdefault("version", 1)
    return data


def _write_state_unlocked(state: dict[str, Any]) -> None:
    AVITO_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = AVITO_STATE_PATH.with_name(f".{AVITO_STATE_PATH.name}.tmp")
    tmp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp_path, AVITO_STATE_PATH)


def load_state() -> dict[str, Any]:
    with _state_lock():
        return _read_state_unlocked()


def _normalize_avito_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        raise ValueError("empty url")
    parsed = urlparse(raw)
    if not parsed.scheme:
        raw = "https://" + raw
        parsed = urlparse(raw)
    host = (parsed.hostname or "").lower()
    if parsed.scheme not in {"http", "https"} or not AVITO_HOST_RE.search(host):
        raise ValueError("нужна ссылка avito.ru")
    normalized = parsed._replace(scheme="https", fragment="")
    return urlunparse(normalized)


def _watch_id_for_url(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:10]


def add_watch_url(url: str) -> tuple[bool, str]:
    try:
        normalized_url = _normalize_avito_url(url)
    except ValueError as exc:
        return False, f"Avito: не добавил ссылку: {exc}"

    watch_id = _watch_id_for_url(normalized_url)
    with _state_lock():
        state = _read_state_unlocked()
        watches = state.setdefault("watches", [])
        for watch in watches:
            if not isinstance(watch, dict):
                continue
            if watch.get("id") == watch_id or watch.get("url") == normalized_url:
                watch["enabled"] = True
                _write_state_unlocked(state)
                return True, f"Avito: ссылка уже есть в пуле\nid={watch_id}"
        watches.append(
            {
                "id": watch_id,
                "url": normalized_url,
                "enabled": True,
                "created_at": _now_iso(),
                "known_items": {},
                "last_checked_at": "",
                "last_ok_at": "",
                "last_error": "",
            }
        )
        _write_state_unlocked(state)
    return True, (
        "Avito: ссылка добавлена в пул.\n"
        f"id={watch_id}\n"
        "Первый успешный опрос сохранит текущие объявления как baseline без отправки старых."
    )


def remove_watch(selector: str) -> tuple[bool, str]:
    needle = str(selector or "").strip()
    if not needle:
        return False, "Avito: укажи номер, id или URL для удаления"

    normalized_url = ""
    if "avito.ru" in needle:
        with contextlib.suppress(ValueError):
            normalized_url = _normalize_avito_url(needle)

    with _state_lock():
        state = _read_state_unlocked()
        watches = [watch for watch in state.get("watches", []) if isinstance(watch, dict)]
        remove_index: int | None = None
        if needle.isdigit():
            index = int(needle) - 1
            if 0 <= index < len(watches):
                remove_index = index
        if remove_index is None:
            lowered = needle.lower()
            for index, watch in enumerate(watches):
                watch_id = str(watch.get("id") or "").lower()
                watch_url = str(watch.get("url") or "")
                if watch_id == lowered or watch_id.startswith(lowered):
                    remove_index = index
                    break
                if normalized_url and watch_url == normalized_url:
                    remove_index = index
                    break
        if remove_index is None:
            return False, f"Avito: не нашёл ссылку для удаления: {needle}"
        removed = watches.pop(remove_index)
        state["watches"] = watches
        _write_state_unlocked(state)
    return True, f"Avito: удалил из пула\nid={removed.get('id')}\n{removed.get('url')}"


def format_watch_list() -> str:
    state = load_state()
    watches = [watch for watch in state.get("watches", []) if isinstance(watch, dict)]
    if not watches:
        return (
            "Avito: пул пуст.\n"
            "Добавить: avito add <url>\n"
            "Список: avito list\n"
            "Удалить: avito del <номер|id|url>"
        )
    lines = ["Avito: пул ссылок"]
    for index, watch in enumerate(watches, start=1):
        known_count = len(watch.get("known_items") or {})
        status = "on" if watch.get("enabled", True) else "off"
        last_ok = str(watch.get("last_ok_at") or "нет")
        last_error = str(watch.get("last_error") or "").strip()
        lines.append(
            f"{index}. id={watch.get('id')} status={status} known={known_count} last_ok={last_ok}\n"
            f"{watch.get('url')}"
        )
        if last_error:
            lines.append(f"   error={last_error[:180]}")
    lines.append("\nКоманды: avito add <url> | avito del <номер|id|url> | avito list")
    return "\n".join(lines)


def handle_telegram_admin_command(raw_text: str) -> str:
    text = str(raw_text or "").strip()
    text = re.sub(r"^/(avito_add|avito_del|avito_remove|avito_list)(?:@[A-Za-z0-9_]+)?\b", r"/\1", text, flags=re.IGNORECASE)
    text = re.sub(r"^/avito(?:@[A-Za-z0-9_]+)?\b", "/avito", text, flags=re.IGNORECASE)
    lowered = text.lower()
    if lowered.startswith("/avito_add"):
        tail = text.split(None, 1)[1] if len(text.split(None, 1)) > 1 else ""
        return add_watch_url(tail)[1]
    if lowered.startswith("/avito_del") or lowered.startswith("/avito_remove"):
        tail = text.split(None, 1)[1] if len(text.split(None, 1)) > 1 else ""
        return remove_watch(tail)[1]
    if lowered.startswith("/avito_list"):
        return format_watch_list()
    if lowered in {"avito", "/avito"}:
        return format_watch_list()
    if lowered.startswith("/avito "):
        text = "avito " + text.split(None, 1)[1]
        lowered = text.lower()

    if lowered.startswith("avito "):
        parts = text.split(None, 2)
        action = parts[1].lower() if len(parts) > 1 else ""
        tail = parts[2].strip() if len(parts) > 2 else ""
        if action in {"add", "добавить", "+"}:
            return add_watch_url(tail)[1]
        if action in {"del", "delete", "remove", "rm", "удалить", "-"}:
            return remove_watch(tail)[1]
        if action in {"list", "ls", "список"}:
            return format_watch_list()
    return (
        "Avito: команды\n"
        "avito add <url>\n"
        "avito list\n"
        "avito del <номер|id|url>"
    )


def _parse_proxy(proxy_url: str) -> dict[str, Any]:
    raw = str(proxy_url or "").strip()
    if not raw:
        return {}
    if "://" not in raw:
        host_first = re.match(r"^(?P<host>[^:@/\s]+):(?P<port>\d+)@(?P<username>[^:@/\s]+):(?P<password>.+)$", raw)
        if host_first:
            raw = (
                f"http://{host_first.group('username')}:{host_first.group('password')}"
                f"@{host_first.group('host')}:{host_first.group('port')}"
            )
        else:
            raw = f"http://{raw}"
    parsed = urlparse(raw)
    host = parsed.hostname
    port = parsed.port
    if not host or not port:
        raise ValueError("proxy must include host and port")
    proxy = {"server": f"http://{host}:{port}"}
    if parsed.username:
        proxy["username"] = parsed.username
    if parsed.password:
        proxy["password"] = parsed.password
    return {"proxy": proxy}


def _safe_text(value: str, *, limit: int = 180) -> str:
    return " ".join(str(value or "").split())[:limit]


def _parse_humanize(value: str) -> bool | float | None:
    raw = str(value or "").strip().lower()
    if not raw or raw in {"0", "false", "no", "off"}:
        return None
    if raw in {"1", "true", "yes", "on"}:
        return True
    try:
        return float(raw)
    except ValueError:
        return True


def _parse_window(value: str) -> tuple[int, int] | None:
    match = re.match(r"^\s*(\d{3,5})\s*[xX]\s*(\d{3,5})\s*$", str(value or ""))
    if not match:
        return None
    width = int(match.group(1))
    height = int(match.group(2))
    if width < 800 or height < 600:
        return None
    return width, height


def parse_avito_items(html: str, base_url: str) -> list[AvitoItem]:
    soup = BeautifulSoup(html or "", "lxml")
    by_id: dict[str, AvitoItem] = {}

    def _is_other_cities_heading(tag) -> bool:
        if not getattr(tag, "name", None):
            return False
        if tag.name not in {"h1", "h2", "h3", "div", "section"}:
            return False
        text = _safe_text(tag.get_text(" ", strip=True), limit=300).lower()
        return bool(re.search(r"\bобъявлен\w*\s+есть\s+в\s+других\s+город", text))

    def _iter_city_listing_cards():
        seen: set[int] = set()
        root = soup.body or soup
        for tag in root.descendants:
            if not getattr(tag, "name", None):
                continue
            if _is_other_cities_heading(tag):
                break
            if tag.get("data-marker") != "item":
                continue
            marker = id(tag)
            if marker in seen:
                continue
            seen.add(marker)
            yield tag

    def _iter_listing_anchors():
        yielded = False
        for card in _iter_city_listing_cards():
            yielded = True
            title_anchor = card.select_one('[data-marker="item-title"][href]')
            if title_anchor is not None:
                yield title_anchor
                continue
            first_listing_anchor = None
            for anchor in card.find_all("a", href=True):
                if ITEM_ID_RE.search(urlparse(str(anchor.get("href") or "")).path):
                    first_listing_anchor = anchor
                    break
            if first_listing_anchor is not None:
                yield first_listing_anchor
        if not yielded:
            for anchor in soup.find_all("a", href=True):
                if ITEM_ID_RE.search(urlparse(str(anchor.get("href") or "")).path):
                    yield anchor

    for anchor in _iter_listing_anchors():
        href = str(anchor.get("href") or "").strip()
        absolute_url = urljoin(base_url, href)
        parsed = urlparse(absolute_url)
        host = (parsed.hostname or "").lower()
        if not AVITO_HOST_RE.search(host):
            continue
        match = ITEM_ID_RE.search(parsed.path)
        if not match:
            continue
        item_id = match.group(1)
        item_url = urlunparse(parsed._replace(scheme="https", fragment=""))
        title = _safe_text(anchor.get_text(" ", strip=True))
        card = anchor.find_parent(attrs={"data-marker": re.compile(r"item", re.IGNORECASE)})
        if card is None:
            card = anchor.find_parent("div")
        price = ""
        if card is not None:
            price_node = card.find(attrs={"data-marker": re.compile(r"price", re.IGNORECASE)})
            if price_node is not None:
                price = _safe_text(price_node.get_text(" ", strip=True), limit=80)
            if not title:
                title_node = card.find(attrs={"itemprop": "name"})
                if title_node is not None:
                    title = _safe_text(title_node.get_text(" ", strip=True))
        current = by_id.get(item_id)
        if current is None or (not current.title and title):
            by_id[item_id] = AvitoItem(item_id=item_id, url=item_url, title=title, price=price)
    return list(by_id.values())


def _detect_avito_block(html: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")
    title = _safe_text(soup.title.get_text(" ", strip=True) if soup.title else "", limit=300)
    body = _safe_text(soup.get_text(" ", strip=True), limit=1200)
    lowered = f"{title} {body}".lower()
    if "доступ ограничен" in lowered and "проблема с ip" in lowered:
        return "Avito ограничил доступ по IP"
    if "капч" in lowered or "captcha" in lowered:
        return "Avito запросил капчу"
    return ""


def _load_page_html(browser, url: str) -> str:
    page = browser.new_page()
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=AVITO_PAGE_TIMEOUT_MS)
        with contextlib.suppress(Exception):
            page.wait_for_load_state("networkidle", timeout=min(15000, AVITO_PAGE_TIMEOUT_MS))
        with contextlib.suppress(Exception):
            page.wait_for_timeout(1500)
        for _ in range(max(0, AVITO_SCROLL_STEPS)):
            with contextlib.suppress(Exception):
                page.mouse.wheel(0, 1600)
                page.wait_for_timeout(500)
        return str(page.content() or "")
    finally:
        with contextlib.suppress(Exception):
            page.close()


def _record_watch_error(watch_id: str, message: str) -> None:
    with _state_lock():
        state = _read_state_unlocked()
        for watch in state.get("watches", []):
            if isinstance(watch, dict) and watch.get("id") == watch_id:
                watch["last_checked_at"] = _now_iso()
                watch["last_error"] = str(message)[:500]
                break
        _write_state_unlocked(state)


def _merge_watch_items(watch_id: str, items: list[AvitoItem]) -> tuple[bool, list[AvitoItem]]:
    now = _now_iso()
    with _state_lock():
        state = _read_state_unlocked()
        watches = state.get("watches", [])
        watch = next((item for item in watches if isinstance(item, dict) and item.get("id") == watch_id), None)
        if watch is None:
            return False, []
        known_items = watch.get("known_items")
        if not isinstance(known_items, dict):
            known_items = {}
            watch["known_items"] = known_items

        first_success = not bool(known_items)
        new_items: list[AvitoItem] = []
        for item in items:
            payload = {
                "url": item.url,
                "title": item.title,
                "price": item.price,
                "last_seen_at": now,
            }
            if item.item_id not in known_items:
                payload["first_seen_at"] = now
                known_items[item.item_id] = payload
                if not first_success:
                    new_items.append(item)
            else:
                previous = known_items[item.item_id]
                if isinstance(previous, dict):
                    previous.update(payload)

        watch["last_checked_at"] = now
        watch["last_ok_at"] = now
        watch["last_error"] = ""
        _write_state_unlocked(state)
    return first_success, new_items


def _format_new_items_message(watch: dict[str, Any], items: list[AvitoItem]) -> str:
    lines = [
        f"Avito: новые объявления ({len(items)})",
        str(watch.get("url") or ""),
        "",
    ]
    for index, item in enumerate(items, start=1):
        title = item.title or f"Объявление {item.item_id}"
        lines.append(f"{index}. {title}")
        if item.price:
            lines.append(item.price)
        lines.append(item.url)
        lines.append("")
    return "\n".join(lines).strip()


def _send_new_items(watch: dict[str, Any], items: list[AvitoItem]) -> None:
    if not items:
        return
    batch: list[AvitoItem] = []
    for item in items:
        batch.append(item)
        message = _format_new_items_message(watch, batch)
        if len(message) < 3200 and len(batch) < 8:
            continue
        if len(batch) > 1:
            to_send = batch[:-1]
            batch = batch[-1:]
        else:
            to_send = batch
            batch = []
        send_message(
            _format_new_items_message(watch, to_send),
            admin_only=AVITO_NOTIFY_ADMIN_ONLY,
            mirror_to_vk=False,
        )
    if batch:
        send_message(
            _format_new_items_message(watch, batch),
            admin_only=AVITO_NOTIFY_ADMIN_ONLY,
            mirror_to_vk=False,
        )


def run_once(*, proxy_url: str = "") -> dict[str, Any]:
    if camoufox is None:
        raise RuntimeError("Camoufox unavailable. Install/enable camoufox in venv_catboost.")

    state = load_state()
    watches = [
        dict(watch)
        for watch in state.get("watches", [])
        if isinstance(watch, dict) and watch.get("enabled", True) and str(watch.get("url") or "").strip()
    ]
    result = {"checked": 0, "new": 0, "errors": 0, "baseline": 0}
    if not watches:
        return result

    proxy_kwargs = _parse_proxy(proxy_url or AVITO_DEFAULT_PROXY_URL)
    browser_kwargs = {"headless": True, **proxy_kwargs}
    if AVITO_CAMOUFOX_LOCALE:
        browser_kwargs["locale"] = AVITO_CAMOUFOX_LOCALE
    if AVITO_CAMOUFOX_OS:
        browser_kwargs["os"] = AVITO_CAMOUFOX_OS
    humanize = _parse_humanize(AVITO_CAMOUFOX_HUMANIZE)
    if humanize is not None:
        browser_kwargs["humanize"] = humanize
    window = _parse_window(AVITO_CAMOUFOX_WINDOW)
    if window is not None:
        browser_kwargs["window"] = window
    if AVITO_CAMOUFOX_PROFILE_DIR:
        profile_dir = Path(AVITO_CAMOUFOX_PROFILE_DIR).expanduser()
        profile_dir.mkdir(parents=True, exist_ok=True)
        browser_kwargs["persistent_context"] = True
        browser_kwargs["user_data_dir"] = str(profile_dir)
    browser_kwargs["block_webrtc"] = AVITO_CAMOUFOX_BLOCK_WEBRTC
    browser_kwargs["enable_cache"] = AVITO_CAMOUFOX_ENABLE_CACHE
    if proxy_kwargs and AVITO_CAMOUFOX_GEOIP:
        browser_kwargs["geoip"] = True
    with camoufox.Camoufox(**browser_kwargs) as browser:
        for watch in watches:
            watch_id = str(watch.get("id") or "")
            url = str(watch.get("url") or "")
            try:
                html = _load_page_html(browser, url)
                block_message = _detect_avito_block(html)
                if block_message:
                    raise RuntimeError(block_message)
                items = parse_avito_items(html, url)
                if not items:
                    raise RuntimeError("на странице не найдены объявления; возможно, блокировка или капча")
                first_success, new_items = _merge_watch_items(watch_id, items)
                result["checked"] += 1
                if first_success:
                    result["baseline"] += 1
                    logger.info("Avito baseline saved for %s: %d items", watch_id, len(items))
                    continue
                if new_items:
                    result["new"] += len(new_items)
                    _send_new_items(watch, new_items)
                    logger.info("Avito new items for %s: %d", watch_id, len(new_items))
            except Exception as exc:
                result["errors"] += 1
                logger.warning("Avito check failed for %s: %s", watch_id or url, exc)
                _record_watch_error(watch_id, str(exc))
    return result


def _parse_hhmm(value: str) -> dt_time:
    match = re.match(r"^(\d{1,2}):(\d{2})$", str(value or "").strip())
    if not match:
        raise ValueError(f"invalid HH:MM time: {value!r}")
    hour = int(match.group(1))
    minute = int(match.group(2))
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"invalid HH:MM time: {value!r}")
    return dt_time(hour=hour, minute=minute)


def _is_active_time(now: datetime) -> bool:
    start = _parse_hhmm(AVITO_ACTIVE_FROM)
    end = _parse_hhmm(AVITO_ACTIVE_UNTIL)
    current = now.time().replace(second=0, microsecond=0)
    if start <= end:
        return start <= current <= end
    return current >= start or current <= end


def _seconds_until_active(now: datetime) -> float:
    start = _parse_hhmm(AVITO_ACTIVE_FROM)
    candidate = datetime.combine(now.date(), start, tzinfo=now.tzinfo)
    if candidate <= now:
        candidate += timedelta(days=1)
    return max(1.0, (candidate - now).total_seconds())


def run_forever(*, proxy_url: str = "", poll_interval_seconds: int = AVITO_POLL_INTERVAL_SECONDS) -> None:
    stop_requested = False

    def _request_stop(_signum, _frame) -> None:
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    tz = ZoneInfo(AVITO_TIMEZONE)
    while not stop_requested:
        now = datetime.now(tz=tz)
        if not _is_active_time(now):
            sleep_seconds = min(_seconds_until_active(now), 300)
            logger.info("Avito monitor sleeping outside active window for %.0fs", sleep_seconds)
            time.sleep(sleep_seconds)
            continue
        try:
            result = run_once(proxy_url=proxy_url)
            logger.info("Avito monitor check result: %s", result)
        except Exception as exc:
            logger.exception("Avito monitor iteration failed: %s", exc)
        deadline = time.time() + max(1, int(poll_interval_seconds))
        while not stop_requested and time.time() < deadline:
            time.sleep(min(5, deadline - time.time()))


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor Avito search URLs and notify Telegram about new items.")
    parser.add_argument("--once", action="store_true", help="Run one check and exit.")
    parser.add_argument("--proxy-url", default=AVITO_DEFAULT_PROXY_URL, help="Proxy URL for Camoufox.")
    parser.add_argument("--poll-interval-seconds", type=int, default=AVITO_POLL_INTERVAL_SECONDS)
    parser.add_argument("--add-url", default="", help="Add Avito URL to the monitor state and exit.")
    parser.add_argument("--list", action="store_true", help="Print watched URLs and exit.")
    args = parser.parse_args()

    logging.basicConfig(
        level=os.getenv("AVITO_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    if args.add_url:
        ok, message = add_watch_url(args.add_url)
        print(message)
        raise SystemExit(0 if ok else 1)
    if args.list:
        print(format_watch_list())
        return
    if args.once:
        print(json.dumps(run_once(proxy_url=args.proxy_url), ensure_ascii=False))
        return
    run_forever(proxy_url=args.proxy_url, poll_interval_seconds=args.poll_interval_seconds)


if __name__ == "__main__":
    main()
