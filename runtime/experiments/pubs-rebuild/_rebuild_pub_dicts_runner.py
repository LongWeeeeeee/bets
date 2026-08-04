import sys
import runpy

# base ДОЛЖЕН быть первым, чтобы импортировалась base/maps_research.py
# (в /root/main/ лежит устаревшая копия maps_research.py с несуществующим
#  hero_valid_positions_simple.json → пустой каталог позиций → все матчи режутся)
sys.path.insert(0, '/root/main')
sys.path.insert(0, '/root/main/base')

import keys
# Для пересборки public-словарей берём ВСЕ патчи из json_parts_split_from_object (7.41+)
keys.start_date_time_739 = 0

runpy.run_path('/root/main/base/explore_database.py', run_name='__main__')
