import sys, os
sys.path.insert(0, "/root/main/base")
from cyberscore_try import _get_cyberscore_html_via_camoufox, _extract_cyberscore_match_item_from_html

MID = os.environ.get("MID", "174455")
html = _get_cyberscore_html_via_camoufox(f"https://cyberscore.live/en/matches/{MID}/")
print(f"html_len={len(html) if html else 0}")
item = _extract_cyberscore_match_item_from_html(html or "", match_id=MID)
if not item:
    print("NO item")
    sys.exit(0)
print(f"status={item.get('status')} game_time={item.get('game_time')} picks={len(item.get('picks') or [])} rad={item.get('score_team_radiant')} dire={item.get('score_team_dire')} map={item.get('game_map_number')}")
