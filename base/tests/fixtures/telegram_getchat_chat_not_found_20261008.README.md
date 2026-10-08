# telegram_getchat_chat_not_found_20261008.jsonl

Captured 2026-10-08 22:42 MSK on serv1 with the production bot token.

Command (token redacted), run once per chat id (`312550797`, `1179836674`):

```python
requests.get("https://api.telegram.org/bot<Token>/getChat", params={"chat_id": cid})
```

Each line is `{"chat_id", "http_status", "body"}` with the verbatim HTTP status and
JSON body. `getChat` answers a deleted/unknown chat with the same 400 description
that `sendMessage` returns for it, which is what the prod log shows on every
broadcast:

```
Telegram send HTTP error: 400 ... | Bad Request: chat not found
Removing Telegram subscriber 1179836674 after terminal error: ...
```

Used by `base/tests/test_telegram_dead_subscribers.py` to build the mocked
`sendMessage` 400 response for the two dead chats (outbound HTTP is the only
thing that test mocks).
