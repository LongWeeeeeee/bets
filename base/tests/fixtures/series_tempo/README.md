Unedited SourceTV snapshots captured 2026-09-24 in a 60 s loop with
`ssh serv1 'cat /root/main/runtime/sourcetv_matches.json' > snap_<local MSK ts>.json`.
File names use local MSK (UTC+3) capture time. The 21:57:35 and 22:19:01
snapshots contain match 9014406398; the 22:43:35 snapshot first contains
9014519155. Both report series_game_number=2 despite being distinct consecutive
maps of the same team pair. The 22:09:49 snapshot remains as an intermediate poll.
