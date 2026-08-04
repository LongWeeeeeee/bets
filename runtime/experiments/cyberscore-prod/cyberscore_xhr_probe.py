import sys, json, re, time
sys.path.insert(0, "/root/main/base")
from cyberscore_try import (
    _shared_camoufox_session,
    _cyberscore_camoufox_proxy_kwargs,
    _get_cyberscore_html_via_camoufox,
    _extract_cyberscore_live_cards_from_html,
    _extract_cyberscore_match_id_from_href,
)

# Step 1: find a live match from listing
listing_html = _get_cyberscore_html_via_camoufox(
    "https://cyberscore.live/en/matches/?type=liveOrUpcoming&tournament_tier=1%2C2"
)
if not listing_html:
    print("ERROR: cannot fetch listing")
    sys.exit(1)

heads, bodies = _extract_cyberscore_live_cards_from_html(listing_html)
live_match_ids = []
for c in heads:
    href = str(c.get("href") or c.get("data-cyberscore-href") or "")
    mid = _extract_cyberscore_match_id_from_href(href)
    if mid:
        live_match_ids.append(mid)
print(f"live_match_ids={live_match_ids[:5]}")
if not live_match_ids:
    print("no live match; try without tier filter")
    # fall back to any live
    listing_html = _get_cyberscore_html_via_camoufox(
        "https://cyberscore.live/en/matches/?type=liveOrUpcoming"
    )
    heads, bodies = _extract_cyberscore_live_cards_from_html(listing_html or "")
    for c in heads:
        href = str(c.get("href") or c.get("data-cyberscore-href") or "")
        mid = _extract_cyberscore_match_id_from_href(href)
        if mid:
            live_match_ids.append(mid)
    print(f"live_match_ids(no-tier)={live_match_ids[:5]}")

if not live_match_ids:
    print("ERROR: no live match at all")
    sys.exit(1)

MATCH_ID = live_match_ids[0]
print(f"=== Probing match {MATCH_ID} ===")

def _job(browser):
    page = browser.new_page()
    responses = []

    def _on_response(resp):
        try:
            url = str(getattr(resp, "url", "") or "")
            if "cyberscore" not in url:
                return
            headers = resp.headers or {}
            ctype = str(headers.get("content-type") or headers.get("Content-Type") or "").lower()
            method = "?"
            try:
                req = resp.request
                method = str(getattr(req, "method", "") or "?")
            except Exception:
                pass
            txt = ""
            try:
                txt = resp.text()
            except Exception:
                txt = ""
            snippet = (txt or "")[:800]
            responses.append({
                "url": url,
                "ctype": ctype,
                "method": method,
                "size": len(txt or ""),
                "head": snippet,
            })
        except Exception:
            pass

    def _on_websocket(ws):
        try:
            ws.on("framereceived", lambda payload: responses.append({
                "url": f"WS:{getattr(ws, 'url', '?')}",
                "ctype": "ws-frame",
                "method": "WS",
                "size": len(str(payload) if payload else ""),
                "head": str(payload)[:800] if payload else "",
            }))
        except Exception:
            pass

    page.on("response", _on_response)
    page.on("websocket", _on_websocket)
    page.goto(f"https://cyberscore.live/en/matches/{MATCH_ID}/", wait_until="domcontentloaded", timeout=60000)
    # stay 20s to capture polling/websocket traffic
    time.sleep(25)
    return responses

resp = _shared_camoufox_session.submit("xhr-probe", _job, timeout=120)
print(f"total_responses={len(resp)}")

# Group by url prefix (strip query), take first-seen of each prefix
uniq = {}
for r in resp:
    key = r["url"].split("?")[0].split("#")[0]
    if key not in uniq:
        uniq[key] = r

# Filter to interesting ones: ones that might contain match data
interesting = []
for k, r in uniq.items():
    head = r["head"]
    if (
        MATCH_ID in head
        or "game_time" in head
        or "radiant_lead" in head
        or "/api/" in k
        or "/live/" in k
        or "rsc" in r["ctype"]
        or r["ctype"].startswith("application/json")
    ):
        interesting.append(r)

print(f"interesting_responses={len(interesting)}")
for r in interesting[:40]:
    print("=" * 80)
    print(f"URL : {r['url'][:220]}")
    print(f"MTD : {r['method']}  CT : {r['ctype']}  SIZE: {r['size']}")
    print(f"HEAD: {r['head'][:500]}")

print("\n\n==== ALL UNIQUE URLs ====")
for k, r in sorted(uniq.items()):
    print(f"[{r['method']:4}] [{r['size']:6}] [{r['ctype'][:30]:30}] {k[:180]}")

print("\n\n==== All responses containing 'id' and MATCH_ID in body ====")
for r in resp:
    if MATCH_ID in (r.get('head') or ''):
        print("=" * 80)
        print(f"URL : {r['url'][:220]}")
        print(f"MTD : {r['method']}  CT : {r['ctype']}  SIZE: {r['size']}")
        print(f"HEAD: {r['head'][:700]}")
