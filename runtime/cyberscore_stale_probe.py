import sys, time, re
sys.path.insert(0, "/root/main/base")
from cyberscore_try import (
    _shared_camoufox_session,
    _get_cyberscore_html_via_camoufox,
    _extract_cyberscore_live_cards_from_html,
    _extract_cyberscore_match_id_from_href,
    _extract_cyberscore_match_item_from_html,
)

listing_html = _get_cyberscore_html_via_camoufox(
    "https://cyberscore.live/en/matches/?type=liveOrUpcoming"
)
heads, _ = _extract_cyberscore_live_cards_from_html(listing_html or "")
match_ids = []
for c in heads:
    href = str(c.get("href") or c.get("data-cyberscore-href") or "")
    mid = _extract_cyberscore_match_id_from_href(href)
    if mid:
        match_ids.append(mid)

if not match_ids:
    print("no live match")
    sys.exit(1)

MATCH_ID = match_ids[0]
# Override with specific live match if passed via env
import os as _os
env_mid = str(_os.environ.get("PROBE_MATCH_ID") or "").strip()
if env_mid:
    MATCH_ID = env_mid
print(f"match_id={MATCH_ID}")

def _job(browser):
    page = browser.new_page()
    page.goto(f"https://cyberscore.live/en/matches/{MATCH_ID}/", wait_until="domcontentloaded", timeout=60000)
    # 6 snapshots spaced by 10 seconds, no goto between
    snaps = []
    for i in range(6):
        time.sleep(10)
        html = page.content() or ""
        item = _extract_cyberscore_match_item_from_html(html, match_id=MATCH_ID)
        snaps.append({
            "i": i,
            "len": len(html),
            "game_time": (item or {}).get("game_time") if isinstance(item, dict) else None,
            "radiant_score": (item or {}).get("score_team_radiant") if isinstance(item, dict) else None,
            "dire_score": (item or {}).get("score_team_dire") if isinstance(item, dict) else None,
            "picks_len": len((item or {}).get("picks") or []) if isinstance(item, dict) else 0,
        })
    return snaps

snaps = _shared_camoufox_session.submit("stale-probe", _job, timeout=180)
print("snap_num | page_len | game_time | rad_score | dire_score | picks")
for s in snaps:
    print(f"#{s['i']} | {s['len']:7} | {str(s['game_time']):8} | {str(s['radiant_score']):4} | {str(s['dire_score']):4} | {s['picks_len']}")

# Now test: reload (goto) same URL, does it give fresher data?
def _job_reload(browser):
    page = browser.new_page()
    page.goto(f"https://cyberscore.live/en/matches/{MATCH_ID}/", wait_until="domcontentloaded", timeout=60000)
    time.sleep(2)
    html1 = page.content() or ""
    item1 = _extract_cyberscore_match_item_from_html(html1, match_id=MATCH_ID)
    time.sleep(30)
    # reload
    page.goto(f"https://cyberscore.live/en/matches/{MATCH_ID}/?_r={int(time.time())}", wait_until="domcontentloaded", timeout=60000)
    time.sleep(2)
    html2 = page.content() or ""
    item2 = _extract_cyberscore_match_item_from_html(html2, match_id=MATCH_ID)
    return (
        (item1 or {}).get("game_time"),
        (item2 or {}).get("game_time"),
    )

gt_initial, gt_after_reload = _shared_camoufox_session.submit("stale-probe-reload", _job_reload, timeout=180)
print(f"\nReload test: game_time initial={gt_initial}, after +30s + reload={gt_after_reload}")
