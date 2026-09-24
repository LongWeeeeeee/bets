# DLTV `/api/v1/search` captures (2026-09-24)

Real captured responses for `GET https://dltv.org/api/v1/search?q=<account_id>`
(plain HTTP JSON, no JS challenge — unlike the team HTML pages).

- `search_216733371.json` — Dragon Knight player, role 2
- `search_212037295.json` — Ancient Apparition player, role 5
- `search_177411785.json` — Bristleback player, role 3
- `search_154921394.json` — Dark Willow player, role 4
- `search_970130166.json` — Io player, role 1
- `search_111111111_notfound.json` — unknown account, `players == []`

The five player captures are the Daxak (dire, team_id 10271241) roster from the
2026-09-24 incident, match 9013821098 (BetBoom Streamers Battle).

Capture command:

```sh
curl -s -A 'Mozilla/5.0' 'https://dltv.org/api/v1/search?q=<id>'
```

Only a player whose `int(steam_id)` equals the queried account id is accepted;
the search is fuzzy, so other entries must be ignored. Roles are accepted only
when `int` in 1..5.
