import sys
sys.path.insert(0, "/root/main/base")
from cyberscore_try import _get_cyberscore_html_via_camoufox, _extract_cyberscore_match_item_from_html, _extract_cyberscore_live_cards_from_html, _extract_cyberscore_match_id_from_href

html = _get_cyberscore_html_via_camoufox(
    "https://cyberscore.live/en/matches/?type=liveOrUpcoming"
)
heads, _ = _extract_cyberscore_live_cards_from_html(html or "")
for c in heads:
    href = str(c.get("href") or c.get("data-cyberscore-href") or "")
    mid = _extract_cyberscore_match_id_from_href(href)
    # Try to parse item for this match id from the listing html
    item = _extract_cyberscore_match_item_from_html(html or "", match_id=mid)
    gt = (item or {}).get("game_time")
    rs = (item or {}).get("score_team_radiant")
    ds = (item or {}).get("score_team_dire")
    st = (item or {}).get("status")
    print(f"mid={mid} status={st} game_time={gt} score={rs}:{ds}")
