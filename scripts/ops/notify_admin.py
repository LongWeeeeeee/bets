#!/usr/bin/env python3
"""Send one message to the owner's admin Telegram chat (keys.Chat_id via keys.Token).

Why: 25.08–01.09.2026 the corpus top-up stalled for 8 nights and the snapshot
went to production 9 days old; the only symptom was a log line nobody reads.
Ops scripts call this from their failure AND success branches so that silence
becomes distinguishable from success. Never raises: exit code is always 0.

Usage: notify_admin.py "text"   |   echo "text" | notify_admin.py
"""
import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "base"))


def main() -> int:
    text = " ".join(sys.argv[1:]).strip() or sys.stdin.read().strip()
    if not text:
        return 0
    text = text[:3500]
    try:
        import keys  # local secrets module, never committed
        # 01.09.2026: local keys.Token answers 401 (rotated); the signal bot is the one
        # that already talks to the owner, so it goes first, keys.Token stays as fallback.
        token = getattr(keys, "signal_bot_token", None) or keys.Token
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": keys.Chat_id,
            "text": text,
            "disable_web_page_preview": "true",
        }).encode()
        with urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=20) as resp:
            ok = bool(json.load(resp).get("ok"))
        print("notify_admin:", "ok" if ok else "telegram refused", flush=True)
    except Exception as exc:  # must never break the caller
        print(f"notify_admin: failed: {exc!r}", file=sys.stderr, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
