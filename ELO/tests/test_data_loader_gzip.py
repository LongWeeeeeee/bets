"""Gzip corpus loading against two unmodified captured STRATZ matches.

Captured 2026-10-02 from 7.39_part001.json. The fixture is embedded to keep
this node within its four owned paths. Exact capture command:

/Users/alex/Documents/ingame/venv_catboost/bin/python3 - <<'PY_CAPTURE'
from itertools import islice
from pathlib import Path
import ijson, orjson
source = Path('/Users/alex/Documents/ingame/pro_heroes_data/json_parts_split_from_object/7.39_part001.json')
with source.open('rb') as fh:
    matches = dict(islice(ijson.kvitems(fh, '', use_float=True), 2))
Path('/private/tmp/pro-corpus-gzip-fixture.json').write_bytes(orjson.dumps(matches))
PY_CAPTURE
"""
import gzip
import json

import pytest

from ELO.data_loader import load_matches


CAPTURED_CORPUS_JSON = (
    b'{"8311776153":{"id":8311776153,"didRadiantWin":true,"towerDeaths":[{"time":619,"isRadiant":false,"np'
    b'cId":28},{"time":662,"isRadiant":false,"npcId":26},{"time":763,"isRadiant":false,"npcId":27},{"time"'
    b':1276,"isRadiant":false,"npcId":30},{"time":1329,"isRadiant":false,"npcId":31},{"time":1382,"isRadia'
    b'nt":false,"npcId":34},{"time":1393,"isRadiant":false,"npcId":46},{"time":1398,"isRadiant":false,"npc'
    b'Id":49},{"time":1413,"isRadiant":false,"npcId":33},{"time":1428,"isRadiant":false,"npcId":45},{"time'
    b'":1430,"isRadiant":false,"npcId":48},{"time":1498,"isRadiant":false,"npcId":51}],"bottomLaneOutcome"'
    b':"RADIANT_VICTORY","topLaneOutcome":"RADIANT_VICTORY","midLaneOutcome":"RADIANT_VICTORY","winRates":'
    b'[0.43,0.44,0.5,0.48,0.65,0.8,0.69,0.63,0.71,0.74,0.84,0.83,0.84,0.88,0.9,0.92,0.89,0.94,0.98,0.97,0.'
    b'97,0.98,0.98,0.99,0.99],"firstBloodTime":136,"averageImp":-4,"regionId":3,"radiantTeam":{"name":"4Pi'
    b'rates","id":9586122},"direTeam":{"name":"\xd0\xb1\xd0\xb0\xd1\x80\xd1\x81\xd0\xb5\xd0\xbb\xd0\xbe\xd0\xbd\xd0\xb0","id":9791482},"startDateTime":174845337'
    b'5,"durationSeconds":1498,"leagueId":18120,"series":{"id":980124,"type":"BEST_OF_ONE"},"direKills":[0'
    b',0,0,0,1,0,3,2,0,1,0,0,0,0,0,0,3,0,0,1,0,0,0,0,0,0],"radiantKills":[0,0,0,1,3,2,1,0,0,2,3,1,0,2,1,1,'
    b'3,3,4,1,1,2,1,4,0,5],"radiantNetworthLeads":[0,50,9,164,551,1321,2766,1394,1066,2438,3266,5160,6543,'
    b'7412,9223,10101,11120,9758,11542,14987,14479,14503,16958,18384,22335,27456],"radiantExperienceLeads"'
    b':[0,0,39,35,83,900,2185,1175,837,1614,2021,3811,4691,4677,5795,7029,6918,6863,9792,15082,14674,15210'
    b',18278,15819,20767,21048],"players":[{"position":"POSITION_1","isRadiant":true,"kills":14,"assists":'
    b'14,"numDenies":15,"numLastHits":192,"goldPerMinute":725,"networth":18382,"experiencePerMinute":617,"'
    b'level":16,"heroDamage":22278,"heroHealing":460,"towerDamage":7968,"item0Id":154,"item1Id":123,"item2'
    b'Id":50,"item3Id":117,"item4Id":141,"item5Id":36,"backpack0Id":4204,"backpack1Id":null,"backpack2Id":'
    b'null,"heroId":136,"neutral0Id":71,"invisibleSeconds":0,"dotaPlusHeroXp":2749,"imp":23,"deaths":3,"in'
    b'tentionalFeeding":false,"steamAccount":{"id":193564777,"smurfFlag":0,"isAnonymous":true}},{"position'
    b'":"POSITION_3","isRadiant":true,"kills":5,"assists":17,"numDenies":7,"numLastHits":200,"goldPerMinut'
    b'e":596,"networth":14770,"experiencePerMinute":681,"level":17,"heroDamage":15123,"heroHealing":1728,"'
    b'towerDamage":3602,"item0Id":50,"item1Id":178,"item2Id":125,"item3Id":86,"item4Id":127,"item5Id":81,"'
    b'backpack0Id":111,"backpack1Id":null,"backpack2Id":null,"heroId":99,"neutral0Id":565,"invisibleSecond'
    b's":0,"dotaPlusHeroXp":25834,"imp":15,"deaths":3,"intentionalFeeding":false,"steamAccount":{"id":3837'
    b'88462,"smurfFlag":0,"isAnonymous":false}},{"position":"POSITION_2","isRadiant":true,"kills":9,"assis'
    b'ts":11,"numDenies":11,"numLastHits":177,"goldPerMinute":611,"networth":15295,"experiencePerMinute":6'
    b'93,"level":17,"heroDamage":19263,"heroHealing":0,"towerDamage":1315,"item0Id":1,"item1Id":36,"item2I'
    b'd":63,"item3Id":41,"item4Id":174,"item5Id":108,"backpack0Id":240,"backpack1Id":null,"backpack2Id":nu'
    b'll,"heroId":120,"neutral0Id":71,"invisibleSeconds":2193,"dotaPlusHeroXp":60595,"imp":15,"deaths":1,"'
    b'intentionalFeeding":false,"steamAccount":{"id":113112046,"smurfFlag":0,"isAnonymous":true}},{"positi'
    b'on":"POSITION_4","isRadiant":true,"kills":10,"assists":10,"numDenies":2,"numLastHits":29,"goldPerMin'
    b'ute":375,"networth":8618,"experiencePerMinute":458,"level":14,"heroDamage":15106,"heroHealing":250,"'
    b'towerDamage":1636,"item0Id":20,"item1Id":36,"item2Id":29,"item3Id":141,"item4Id":240,"item5Id":43,"b'
    b'ackpack0Id":null,"backpack1Id":null,"backpack2Id":null,"heroId":21,"neutral0Id":828,"invisibleSecond'
    b's":0,"dotaPlusHeroXp":2749,"imp":35,"deaths":2,"intentionalFeeding":false,"steamAccount":{"id":11416'
    b'2163,"smurfFlag":0,"isAnonymous":false}},{"position":"POSITION_5","isRadiant":true,"kills":3,"assist'
    b's":26,"numDenies":1,"numLastHits":33,"goldPerMinute":355,"networth":7917,"experiencePerMinute":558,"'
    b'level":15,"heroDamage":8860,"heroHealing":12367,"towerDamage":1353,"item0Id":null,"item1Id":269,"ite'
    b'm2Id":79,"item3Id":73,"item4Id":88,"item5Id":73,"backpack0Id":218,"backpack1Id":null,"backpack2Id":n'
    b'ull,"heroId":91,"neutral0Id":1599,"invisibleSeconds":0,"dotaPlusHeroXp":18965,"imp":35,"deaths":2,"i'
    b'ntentionalFeeding":false,"steamAccount":{"id":891154650,"smurfFlag":0,"isAnonymous":false}},{"positi'
    b'on":"POSITION_5","isRadiant":false,"kills":1,"assists":6,"numDenies":3,"numLastHits":7,"goldPerMinut'
    b'e":175,"networth":3137,"experiencePerMinute":265,"level":10,"heroDamage":5775,"heroHealing":0,"tower'
    b'Damage":0,"item0Id":43,"item1Id":265,"item2Id":36,"item3Id":29,"item4Id":88,"item5Id":37,"backpack0I'
    b'd":16,"backpack1Id":188,"backpack2Id":null,"heroId":3,"neutral0Id":359,"invisibleSeconds":0,"dotaPlu'
    b'sHeroXp":1850,"imp":-40,"deaths":10,"intentionalFeeding":false,"steamAccount":{"id":404651426,"smurf'
    b'Flag":0,"isAnonymous":true}},{"position":"POSITION_2","isRadiant":false,"kills":3,"assists":1,"numDe'
    b'nies":6,"numLastHits":143,"goldPerMinute":402,"networth":9904,"experiencePerMinute":451,"level":14,"'
    b'heroDamage":10861,"heroHealing":0,"towerDamage":0,"item0Id":36,"item1Id":116,"item2Id":178,"item3Id"'
    b':41,"item4Id":1,"item5Id":63,"backpack0Id":null,"backpack1Id":null,"backpack2Id":null,"heroId":19,"n'
    b'eutral0Id":828,"invisibleSeconds":0,"dotaPlusHeroXp":5675,"imp":-30,"deaths":6,"intentionalFeeding":'
    b'false,"steamAccount":{"id":852452671,"smurfFlag":0,"isAnonymous":false}},{"position":"POSITION_1","i'
    b'sRadiant":false,"kills":2,"assists":5,"numDenies":3,"numLastHits":153,"goldPerMinute":409,"networth"'
    b':9491,"experiencePerMinute":435,"level":13,"heroDamage":17791,"heroHealing":0,"towerDamage":0,"item0'
    b'Id":36,"item1Id":265,"item2Id":116,"item3Id":596,"item4Id":63,"item5Id":149,"backpack0Id":null,"back'
    b'pack1Id":16,"backpack2Id":null,"heroId":72,"neutral0Id":1604,"invisibleSeconds":0,"dotaPlusHeroXp":2'
    b'0695,"imp":-44,"deaths":9,"intentionalFeeding":false,"steamAccount":{"id":344143907,"smurfFlag":0,"i'
    b'sAnonymous":true}},{"position":"POSITION_3","isRadiant":false,"kills":3,"assists":5,"numDenies":1,"n'
    b'umLastHits":118,"goldPerMinute":348,"networth":7689,"experiencePerMinute":445,"level":14,"heroDamage'
    b'":8656,"heroHealing":0,"towerDamage":0,"item0Id":36,"item1Id":1,"item2Id":63,"item3Id":21,"item4Id":'
    b'16,"item5Id":75,"backpack0Id":null,"backpack1Id":38,"backpack2Id":16,"heroId":97,"neutral0Id":71,"in'
    b'visibleSeconds":0,"dotaPlusHeroXp":72050,"imp":-30,"deaths":8,"intentionalFeeding":false,"steamAccou'
    b'nt":{"id":175824034,"smurfFlag":0,"isAnonymous":true}},{"position":"POSITION_4","isRadiant":false,"k'
    b'ills":2,"assists":5,"numDenies":0,"numLastHits":26,"goldPerMinute":211,"networth":3616,"experiencePe'
    b'rMinute":302,"level":11,"heroDamage":9883,"heroHealing":0,"towerDamage":0,"item0Id":180,"item1Id":34'
    b',"item2Id":null,"item3Id":31,"item4Id":null,"item5Id":null,"backpack0Id":null,"backpack1Id":null,"ba'
    b'ckpack2Id":null,"heroId":79,"neutral0Id":840,"invisibleSeconds":0,"dotaPlusHeroXp":2749,"imp":-24,"d'
    b'eaths":8,"intentionalFeeding":false,"steamAccount":{"id":137340624,"smurfFlag":0,"isAnonymous":true}'
    b'}],"league":{"id":18120,"tier":"UNKNOWN"}},"8310044973":{"id":8310044973,"didRadiantWin":true,"tower'
    b'Deaths":[{"time":385,"isRadiant":false,"npcId":27},{"time":716,"isRadiant":false,"npcId":26},{"time"'
    b':730,"isRadiant":true,"npcId":18},{"time":1255,"isRadiant":false,"npcId":28},{"time":1457,"isRadiant'
    b'":false,"npcId":31},{"time":1486,"isRadiant":false,"npcId":30},{"time":1565,"isRadiant":false,"npcId'
    b'":29},{"time":1597,"isRadiant":false,"npcId":33},{"time":1664,"isRadiant":false,"npcId":45},{"time":'
    b'1680,"isRadiant":false,"npcId":48},{"time":1698,"isRadiant":false,"npcId":34},{"time":1863,"isRadian'
    b't":false,"npcId":46},{"time":1865,"isRadiant":false,"npcId":49},{"time":1872,"isRadiant":false,"npcI'
    b'd":37},{"time":2002,"isRadiant":false,"npcId":32},{"time":2009,"isRadiant":false,"npcId":44},{"time"'
    b':2012,"isRadiant":false,"npcId":47},{"time":2020,"isRadiant":false,"npcId":37},{"time":2033,"isRadia'
    b'nt":false,"npcId":35},{"time":2042,"isRadiant":false,"npcId":37},{"time":2044,"isRadiant":false,"npc'
    b'Id":35},{"time":2048,"isRadiant":false,"npcId":37},{"time":2055,"isRadiant":false,"npcId":37},{"time'
    b'":2071,"isRadiant":false,"npcId":51}],"bottomLaneOutcome":"DIRE_VICTORY","topLaneOutcome":"RADIANT_V'
    b'ICTORY","midLaneOutcome":"RADIANT_VICTORY","winRates":[0.51,0.52,0.57,0.62,0.63,0.71,0.78,0.68,0.77,'
    b'0.78,0.54,0.64,0.67,0.64,0.64,0.58,0.53,0.64,0.6,0.58,0.74,0.78,0.88,0.9,0.93,0.9,0.88,0.89,0.93,0.9'
    b'4,0.93,0.95,0.96,0.96,0.98],"firstBloodTime":119,"averageImp":-2,"regionId":18,"radiantTeam":{"name"'
    b':"\xe9\x82\xa6\xe5\x8a\xa0\xe6\x8b\x89\xe4\xbb\x80","id":9782803},"direTeam":{"name":"\xe6\x89\x8e\xe7\x94\xb7","id":9780897},"startDateTime":1748348133,"'
    b'durationSeconds":2071,"leagueId":18043,"series":{"id":979810,"type":"BEST_OF_ONE"},"direKills":[0,0,'
    b'0,1,0,0,0,0,1,0,5,0,0,1,0,3,3,0,0,0,1,0,1,0,0,0,1,0,0,0,0,1,1,0,0,2],"radiantKills":[0,0,1,1,2,0,1,0'
    b',3,1,0,3,0,0,2,1,1,3,0,0,5,0,3,0,1,0,3,0,3,0,0,3,2,1,2,1],"radiantNetworthLeads":[50,50,71,775,661,1'
    b'183,1873,2548,2466,3190,3889,182,1840,1556,1172,2121,318,-134,2385,2137,2340,5854,8361,9859,10806,12'
    b'057,12657,12797,13666,16115,16754,15925,18227,18614,20878,25358],"radiantExperienceLeads":[0,0,-166,'
    b'233,301,433,1381,1456,1247,1960,2548,-89,1352,1420,1699,2065,-78,-1014,1281,602,406,5827,7675,12178,'
    b'14103,15131,13807,14614,14637,17980,22844,21400,26285,25945,29677,33549],"players":[{"position":"POS'
    b'ITION_2","isRadiant":true,"kills":14,"assists":14,"numDenies":14,"numLastHits":268,"goldPerMinute":6'
    b'73,"networth":22788,"experiencePerMinute":863,"level":23,"heroDamage":26569,"heroHealing":7944,"towe'
    b'rDamage":3326,"item0Id":116,"item1Id":108,"item2Id":53,"item3Id":48,"item4Id":77,"item5Id":137,"back'
    b'pack0Id":4204,"backpack1Id":113,"backpack2Id":null,"heroId":36,"neutral0Id":1602,"invisibleSeconds":'
    b'0,"dotaPlusHeroXp":2749,"imp":13,"deaths":3,"intentionalFeeding":false,"steamAccount":{"id":17610624'
    b'0,"smurfFlag":0,"isAnonymous":false}},{"position":"POSITION_3","isRadiant":true,"kills":8,"assists":'
    b'22,"numDenies":7,"numLastHits":167,"goldPerMinute":513,"networth":17716,"experiencePerMinute":641,"l'
    b'evel":20,"heroDamage":16498,"heroHealing":1912,"towerDamage":4031,"item0Id":1,"item1Id":50,"item2Id"'
    b':81,"item3Id":9,"item4Id":598,"item5Id":178,"backpack0Id":null,"backpack1Id":null,"backpack2Id":null'
    b',"heroId":29,"neutral0Id":1602,"invisibleSeconds":0,"dotaPlusHeroXp":2749,"imp":30,"deaths":2,"inten'
    b'tionalFeeding":false,"steamAccount":{"id":166263635,"smurfFlag":0,"isAnonymous":false}},{"position":'
    b'"POSITION_5","isRadiant":true,"kills":3,"assists":20,"numDenies":1,"numLastHits":62,"goldPerMinute":'
    b'344,"networth":10478,"experiencePerMinute":481,"level":17,"heroDamage":12869,"heroHealing":0,"towerD'
    b'amage":2586,"item0Id":90,"item1Id":50,"item2Id":125,"item3Id":73,"item4Id":6,"item5Id":null,"backpac'
    b'k0Id":null,"backpack1Id":null,"backpack2Id":null,"heroId":14,"neutral0Id":359,"invisibleSeconds":0,"'
    b'dotaPlusHeroXp":54190,"imp":-3,"deaths":9,"intentionalFeeding":false,"steamAccount":{"id":98894135,"'
    b'smurfFlag":0,"isAnonymous":false}},{"position":"POSITION_4","isRadiant":true,"kills":2,"assists":29,'
    b'"numDenies":2,"numLastHits":27,"goldPerMinute":312,"networth":8791,"experiencePerMinute":546,"level"'
    b':18,"heroDamage":14108,"heroHealing":195,"towerDamage":316,"item0Id":218,"item1Id":100,"item2Id":1,"'
    b'item3Id":188,"item4Id":36,"item5Id":180,"backpack0Id":null,"backpack1Id":null,"backpack2Id":null,"he'
    b'roId":87,"neutral0Id":1638,"invisibleSeconds":0,"dotaPlusHeroXp":5075,"imp":23,"deaths":4,"intention'
    b'alFeeding":false,"steamAccount":{"id":249596261,"smurfFlag":0,"isAnonymous":false}},{"position":"POS'
    b'ITION_1","isRadiant":true,"kills":16,"assists":22,"numDenies":8,"numLastHits":344,"goldPerMinute":79'
    b'1,"networth":26110,"experiencePerMinute":756,"level":22,"heroDamage":40065,"heroHealing":3318,"tower'
    b'Damage":16408,"item0Id":116,"item1Id":263,"item2Id":158,"item3Id":911,"item4Id":63,"item5Id":75,"bac'
    b'kpack0Id":5,"backpack1Id":26,"backpack2Id":null,"heroId":53,"neutral0Id":1603,"invisibleSeconds":0,"'
    b'dotaPlusHeroXp":2749,"imp":45,"deaths":3,"intentionalFeeding":false,"steamAccount":{"id":1662434684,'
    b'"smurfFlag":0,"isAnonymous":false}},{"position":"POSITION_2","isRadiant":false,"kills":5,"assists":1'
    b'1,"numDenies":6,"numLastHits":130,"goldPerMinute":385,"networth":12000,"experiencePerMinute":481,"le'
    b'vel":17,"heroDamage":20326,"heroHealing":0,"towerDamage":18,"item0Id":41,"item1Id":63,"item2Id":267,'
    b'"item3Id":116,"item4Id":34,"item5Id":null,"backpack0Id":null,"backpack1Id":null,"backpack2Id":null,"'
    b'heroId":107,"neutral0Id":1640,"invisibleSeconds":0,"dotaPlusHeroXp":31075,"imp":-15,"deaths":6,"inte'
    b'ntionalFeeding":false,"steamAccount":{"id":121791324,"smurfFlag":0,"isAnonymous":false}},{"position"'
    b':"POSITION_5","isRadiant":false,"kills":3,"assists":14,"numDenies":2,"numLastHits":49,"goldPerMinute'
    b'":302,"networth":7598,"experiencePerMinute":393,"level":15,"heroDamage":15220,"heroHealing":0,"tower'
    b'Damage":135,"item0Id":11,"item1Id":254,"item2Id":null,"item3Id":214,"item4Id":null,"item5Id":34,"bac'
    b'kpack0Id":null,"backpack1Id":null,"backpack2Id":null,"heroId":5,"neutral0Id":675,"invisibleSeconds":'
    b'7266,"dotaPlusHeroXp":6950,"imp":-25,"deaths":11,"intentionalFeeding":false,"steamAccount":{"id":179'
    b'8494396,"smurfFlag":0,"isAnonymous":false}},{"position":"POSITION_4","isRadiant":false,"kills":3,"as'
    b'sists":10,"numDenies":6,"numLastHits":92,"goldPerMinute":313,"networth":9413,"experiencePerMinute":2'
    b'98,"level":13,"heroDamage":16895,"heroHealing":333,"towerDamage":286,"item0Id":34,"item1Id":16,"item'
    b'2Id":98,"item3Id":214,"item4Id":65,"item5Id":188,"backpack0Id":null,"backpack1Id":null,"backpack2Id"'
    b':null,"heroId":110,"neutral0Id":1602,"invisibleSeconds":570,"dotaPlusHeroXp":2000,"imp":-30,"deaths"'
    b':10,"intentionalFeeding":false,"steamAccount":{"id":368751872,"smurfFlag":0,"isAnonymous":false}},{"'
    b'position":"POSITION_1","isRadiant":false,"kills":3,"assists":3,"numDenies":7,"numLastHits":305,"gold'
    b'PerMinute":574,"networth":18195,"experiencePerMinute":754,"level":22,"heroDamage":17990,"heroHealing'
    b'":0,"towerDamage":0,"item0Id":1,"item1Id":50,"item2Id":36,"item3Id":143,"item4Id":145,"item5Id":116,'
    b'"backpack0Id":null,"backpack1Id":null,"backpack2Id":null,"heroId":70,"neutral0Id":1605,"invisibleSec'
    b'onds":0,"dotaPlusHeroXp":9650,"imp":-42,"deaths":7,"intentionalFeeding":false,"steamAccount":{"id":2'
    b'53059168,"smurfFlag":0,"isAnonymous":false}},{"position":"POSITION_3","isRadiant":false,"kills":6,"a'
    b'ssists":13,"numDenies":12,"numLastHits":175,"goldPerMinute":437,"networth":13610,"experiencePerMinut'
    b'e":474,"level":17,"heroDamage":22215,"heroHealing":2810,"towerDamage":386,"item0Id":90,"item1Id":11,'
    b'"item2Id":79,"item3Id":36,"item4Id":180,"item5Id":108,"backpack0Id":null,"backpack1Id":4204,"backpac'
    b'k2Id":null,"heroId":108,"neutral0Id":1640,"invisibleSeconds":0,"dotaPlusHeroXp":7600,"imp":-21,"deat'
    b'hs":9,"intentionalFeeding":false,"steamAccount":{"id":175574734,"smurfFlag":0,"isAnonymous":false}}]'
    b',"league":{"id":18043,"tier":"UNKNOWN"}}}'
)


def test_gzip_matches_plain_captured_part(tmp_path):
    plain, compressed = tmp_path / "plain", tmp_path / "compressed"
    plain.mkdir()
    compressed.mkdir()
    (plain / "7.39_part001.json").write_bytes(CAPTURED_CORPUS_JSON)
    with gzip.open(compressed / "7.39_part001.json.gz", "wb") as fh:
        fh.write(CAPTURED_CORPUS_JSON)
    expected, summary = load_matches(plain)
    assert [match.match_id for match in expected] == [8310044973, 8311776153]
    assert [match.source_patch for match in expected] == ["7.39", "7.39"]
    assert load_matches(compressed) == (expected, summary)


def test_mixed_parts_use_sorted_file_names_and_match_order(tmp_path):
    captured = list(json.loads(CAPTURED_CORPUS_JSON).items())
    # Create the later filename first; filesystem creation order is irrelevant.
    with gzip.open(tmp_path / "7.39_part002.json.gz", "wb") as fh:
        fh.write(json.dumps(dict(captured[:1])).encode())
    (tmp_path / "7.39_part001.json").write_text(json.dumps(dict(captured[1:])))
    progress = []
    matches, summary = load_matches(
        tmp_path, progress=lambda path, count: progress.append((path.name, count)))
    assert [match.match_id for match in matches] == [8310044973, 8311776153]
    assert [match.source_patch for match in matches] == ["7.39", "7.39"]
    assert summary == {"files": 2, "raw_matches": 2, "seen_matches": 2,
                       "loaded_matches": 2}
    assert progress == [("7.39_part001.json", 1), ("7.39_part002.json.gz", 2)]


@pytest.mark.parametrize("suffix", [".json", ".json.gz"])
def test_sidecars_keep_existing_validation_skip_behavior(tmp_path, suffix):
    # HEAD scans JSON sidecars and rejects their values through validation;
    # processed_ids.txt is excluded by the glob, rather than by a denylist.
    plain, candidate = tmp_path / "plain", tmp_path / "candidate"
    plain.mkdir()
    candidate.mkdir()
    sidecars = {
        "scan_manifest": {"7.39_part001.json": [14841, 1, [8311776153]]},
        "part_counters": {"7.39": 1},
        "merge_patch_summary": {"unique_matches_added": 2, "patches": {}},
    }
    for name, contents in sidecars.items():
        payload = json.dumps(contents).encode()
        (plain / (name + ".json")).write_bytes(payload)
        path = candidate / (name + suffix)
        if suffix.endswith(".gz"):
            with gzip.open(path, "wb") as fh:
                fh.write(payload)
        else:
            path.write_bytes(payload)
    for directory in (plain, candidate):
        (directory / "processed_ids.txt").write_text("not JSON")
        (directory / "processed_ids.txt.gz").write_bytes(b"not gzip")
    expected = load_matches(plain)
    assert expected == ([], {"files": 3, "raw_matches": 4, "seen_matches": 4,
                            "skipped_non_dict": 3, "skipped_invalid": 1})
    assert load_matches(candidate) == expected
