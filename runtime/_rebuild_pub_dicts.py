import sys, runpy
# base FIRST so base/maps_research.py (hero_position_stats.json) wins over root copy
sys.path.insert(0, "/root/main")
sys.path.insert(0, "/root/main/base")
import keys
keys.start_date_time_739 = 0
import maps_research
assert maps_research.__file__.startswith("/root/main/base"), maps_research.__file__
assert maps_research._has_position_catalog(), "position catalog empty"
print("position catalog OK, stats:", len(maps_research.HERO_POSITION_STATS), "src:", maps_research.__file__, flush=True)
import analise_database
runpy.run_path("/root/main/base/explore_database.py", run_name="__main__")
