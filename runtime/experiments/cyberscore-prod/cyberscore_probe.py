import sys, json, re
sys.path.insert(0, "/root/main/base")
from cyberscore_try import (
    _get_cyberscore_html_via_camoufox,
    _extract_cyberscore_match_item_from_html,
    _decode_next_flight_chunks_from_html,
)

html = _get_cyberscore_html_via_camoufox("https://cyberscore.live/en/matches/174451/")
print("html_len=", len(html) if html else 0)

chunks = _decode_next_flight_chunks_from_html(html or "")
print("chunks_count=", len(chunks))

joined = "\n".join(chunks)
print("joined_len=", len(joined))

item_ids = re.findall(r'"item":\s*\{\s*"id":\s*(\d+)', joined)
print("item_ids=", item_ids[:30])

game_map_numbers = re.findall(r'"game_map_number":\s*(\d+)', joined)
print("game_map_numbers=", game_map_numbers[:30])

game_time_blocks = re.findall(r'"game_time":\s*(\d+)', joined)
print("game_time_blocks=", game_time_blocks[:30])

hits_174451 = joined.count("174451")
print("174451_mentions=", hits_174451)

item = _extract_cyberscore_match_item_from_html(html or "", match_id="174451")
if item:
    print("item.keys=", list(item.keys())[:40])
    print("item.id=", item.get("id"))
    print("item.game_map_number=", item.get("game_map_number"))
    print("item.game_time=", item.get("game_time"))
    print("item.picks_count=", len(item.get("picks") or []))
    print("item.status=", item.get("status"))
else:
    print("NO item found for 174451")
