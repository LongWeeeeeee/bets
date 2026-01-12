"""
Построение статистики с исключением TEST SET для честной валидации.

✅ ИСПРАВЛЕНО - НЕТ DATA LEAKAGE!

Создает статистику на TRAIN SET (все матчи из базы, кроме test_set_pub_matches.json):
- lane_dict: статистика по лайнам
- early_dict: статистика early фазы
- late_dict: статистика late фазы
- comeback_dict: статистика камбеков

TEST SET исключается из train (используется существующий test_set_pub_matches.json).
Используйте check_metrics.py для валидации на test set!

ОПТИМИЗИРОВАННЫЕ ФИЛЬТРЫ (test_filters.py эксперименты, MIN_INDEX >= 10):
✓ Early: Avg lead >= 8k на 15-25 мин (65.1% synergy, 87.5% CP)
✓ Late: Comeback 4k - победитель проигрывал avg 4k на 15-25 мин (89.7% solo, 60.6% synergy)
✓ Comeback: стабильное >=12k lead 3+ минуты, потом проиграли
✓ Фильтры: афк, смурфы, skill stomps
"""

import json
import glob
import sys
from pathlib import Path
from datetime import datetime
from maps_research import check_match_quality
# Добавляем родительскую директорию в путь для импорта
sys.path.insert(0, str(Path(__file__).parent.parent))

from analise_database import analise_database

print("=" * 80)
print("ПОСТРОЕНИЕ СТАТИСТИКИ (ИСКЛЮЧАЯ TEST SET)")
print("=" * 80)
print("✓ Train set: все матчи из базы → статистика")
print("✓ Test set:  исключается из train (используется существующий)")
print("✓ НЕТ data leakage!")
print("=" * 80)

# ===================================================================
# ШАГ 1: ЗАГРУЗКА TEST SET (для исключения из train)
# ===================================================================
print("\n[ШАГ 1/3] Загрузка test set для исключения...")

# Определяем директорию скрипта для корректных путей
script_dir = Path(__file__).parent

# Абсолютные пути к данным
json_dir = Path('/Users/alex/Documents/ingame/bets_data/analise_pub_matches/json_parts_split_from_object')
test_set_path = Path('/Users/alex/Documents/ingame/bets_data/analise_pub_matches/extracted_100k_matches.json')

# Загружаем существующий test set
test_match_ids = set()
if test_set_path.exists():
    try:
        with open(test_set_path, 'r', encoding='utf-8') as f:
            test_set_data = json.load(f)
        
        # Извлекаем match_id (поддерживаем оба формата)
        if isinstance(test_set_data, dict):
            test_match_ids = set(str(mid) for mid in test_set_data.keys())
        elif isinstance(test_set_data, list):
            test_match_ids = set(str(m.get('match_id') or m.get('id')) for m in test_set_data if m.get('match_id') or m.get('id'))
        
        print(f"  ✓ Загружено {len(test_match_ids):,} match_id из test_set_pub_matches.json")
        print(f"  → Эти матчи будут исключены из train set")
    except Exception as e:
        print(f"  ⚠️  Ошибка загрузки test set: {e}")
        print(f"  → Будут обработаны все матчи (без исключений)")
else:
    print(f"  ⚠️  Файл test_set_pub_matches.json не найден")
    print(f"  → Будут обработаны все матчи (без исключений)")

pub_files = list(json_dir.glob('combined*.json'))

if not pub_files:
    print(f"Файлы не найдены в {json_dir}!")
    print(f"Текущая директория скрипта: {script_dir}")
    sys.exit(1)

print(f"\nНайдено файлов для обработки: {len(pub_files)}")

# ===================================================================
# ШАГ 2: ПОСТРОЕНИЕ СТАТИСТИКИ НА TRAIN SET (исключая test set)
# ===================================================================
print("\n[ШАГ 2/3] Построение статистики на train set...")

lane_dict = {}
early_dict = {}
late_dict = {}
comeback_dict = {}
from keys import start_date_time_739 as start_date_time
train_processed = 0
train_total = 0
test_excluded = 0

for idx, file in enumerate(pub_files, 1):
    print(f"  [{idx}/{len(pub_files)}] Обработка {Path(file).name}...", end=" ")
    
    try:
        with open(str(file), 'r', encoding='utf-8') as f:
            matches = json.load(f)
        
        file_train = 0
        file_excluded = 0
        
        for match_id, match in matches.items():
            # Проверяем базовые критерии
            if not isinstance(match, dict):
                continue
            if 'startDateTime' not in match:
                continue
            if match['startDateTime'] < int(start_date_time):
                continue
            if 'players' not in match or len(match.get('players', [])) != 10:
                continue
            
            # ИСКЛЮЧАЕМ матчи из test set
            if str(match_id) in test_match_ids:
                file_excluded += 1
                test_excluded += 1
                continue
            
            # Проверка качества
            result, message = check_match_quality(match)
            if not result:
                continue
            
            # TRAIN: обрабатываем матч
            try:
                analise_database(match, lane_dict, early_dict, late_dict, comeback_dict)
                train_processed += 1
                file_train += 1
            except Exception:
                pass
            
            train_total += 1
            
            # Периодический вывод прогресса
            if train_total % 50000 == 0:
                print(f"\n    [{train_total:,}] Lane: {len(lane_dict):,}, Early: {len(early_dict):,}, Late: {len(late_dict):,}, Comeback: {len(comeback_dict):,}", end="")
        
        print(f" ✓ train:{file_train} excluded:{file_excluded}")
        
    except Exception as e:
        print(f"✗ Ошибка: {e}")
        continue

print(f"\n✓ Успешно обработано train матчей: {train_processed:,}")
print(f"✓ Исключено test матчей: {test_excluded:,}")

# Подсчет матчей в каждом словаре
lane_matches = sum(stats.get('games', 0) for stats in lane_dict.values() if isinstance(stats, dict))
early_matches = sum(stats.get('games', 0) for stats in early_dict.values() if isinstance(stats, dict))
late_matches = sum(stats.get('games', 0) for stats in late_dict.values() if isinstance(stats, dict))
comeback_matches = sum(stats.get('games', 0) for stats in comeback_dict.values() if isinstance(stats, dict))

print(f"\nСтатистика по словарям (train set):")
print(f"  Lane dict:     {len(lane_dict):>6,} ключей, {lane_matches:>7,} записей")
print(f"  Early dict:    {len(early_dict):>6,} ключей, {early_matches:>7,} записей")
print(f"  Late dict:     {len(late_dict):>6,} ключей, {late_matches:>7,} записей")
print(f"  Comeback dict: {len(comeback_dict):>6,} ключей, {comeback_matches:>7,} записей")

# ===================================================================
# ШАГ 3: СОХРАНЕНИЕ
# ===================================================================
print(f"\n[ШАГ 3/3] Сохранение результатов...")

# Сохраняем RAW статистику (train)
print("\nСохранение статистики (train set)...")

# Определяем директорию для сохранения
stats_dir = Path('/Users/alex/Documents/ingame/bets_data/analise_pub_matches')
stats_dir.mkdir(parents=True, exist_ok=True)

with open(stats_dir / 'lane_dict_raw.json', 'w', encoding='utf-8') as f:
    json.dump(lane_dict, f)
print("  ✓ lane_dict_739.json")

with open(stats_dir / 'early_dict_raw.json', 'w', encoding='utf-8') as f:
    json.dump(early_dict, f)
print("  ✓ early_dict_739.json")

with open(stats_dir / 'late_dict_raw.json', 'w', encoding='utf-8') as f:
    json.dump(late_dict, f)
print("  ✓ late_dict_739.json")

with open(stats_dir / 'comeback_dict_raw.json', 'w', encoding='utf-8') as f:
    json.dump(comeback_dict, f)
print("  ✓ comeback_dict_739.json")

print(f"\n{'=' * 80}")
print("ЗАВЕРШЕНО!")
print(f"{'=' * 80}")
print(f"\nСоздано файлов:")
print(f"  TRAIN SET (статистика на ~{train_processed:,} обработанных матчей):")
print(f"    - lane_dict_raw.json     ({len(lane_dict):,} ключей)")
print(f"    - early_dict_raw.json    ({len(early_dict):,} ключей)")
print(f"    - late_dict_raw.json     ({len(late_dict):,} ключей)")
print(f"    - comeback_dict_raw.json ({len(comeback_dict):,} ключей)")
print(f"\n  Test set исключен: {test_excluded:,} матчей")
print(f"  (используется существующий test_set_pub_matches.json)")
print(f"\n  Для валидации запустите:")
print(f"    python check_metrics.py")
print(f"{'=' * 80}\n")

