import sys
sys.path.insert(0, "/root/main/base")
from cyberscore_try import (
    _get_cyberscore_html_via_camoufox,
    _extract_cyberscore_live_cards_from_html,
    _extract_cyberscore_match_id_from_href,
    _extract_cyberscore_match_item_from_html,
    CYBERSCORE_MATCHES_URL,
)

html = _get_cyberscore_html_via_camoufox(CYBERSCORE_MATCHES_URL)
print(f"html_len={len(html) if html else 0}")

heads, _ = _extract_cyberscore_live_cards_from_html(html or "")
for c in heads:
    href = str(c.get("href") or c.get("data-cyberscore-href") or "")
    mid = _extract_cyberscore_match_id_from_href(href)
    if not mid:
        continue
    item = _extract_cyberscore_match_item_from_html(html or "", match_id=mid)
    if not isinstance(item, dict):
        print(f"mid={mid}: no item")
        continue
    rad = item.get("team_radiant") or {}
    dire = item.get("team_dire") or {}
    print(
        f"mid={mid} map={item.get('game_map_number')} "
        f"time={item.get('game_time')} picks={len(item.get('picks') or [])} "
        f"score={item.get('score_team_radiant')}:{item.get('score_team_dire')} "
        f"radiant={rad.get('name')} (id={item.get('team_radiant_id')}) "
        f"dire={dire.get('name')} (id={item.get('team_dire_id')})"
    )
