#!/usr/bin/env python3
"""
Объединяет файлы из temp_files в json_parts_split_from_object
без дублей с разделением по патчам:
- 7.40: от 15 декабря 2024
- 7.41: от 24 марта 2025
"""

import json
import os
from datetime import datetime
from pathlib import Path

# Патчи: timestamp границы
PATCH_7_40_START = int(datetime(2024, 12, 15).timestamp())
PATCH_7_41_START = int(datetime(2025, 3, 24).timestamp())

def get_patch_name(start_timestamp):
    """Определяет патч по timestamp матча"""
    if start_timestamp >= PATCH_7_41_START:
        return "7.41"
    elif start_timestamp >= PATCH_7_40_START:
        return "7.40"
    else:
        return "pre_7.40"

def main():
    temp_dir = Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches/temp_files")
    output_dir = Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches/json_parts_split_from_object")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Словари для каждого патча
    patches = {
        "pre_7.40": {},
        "7.40": {},
        "7.41": {}
    }

    # Читаем все файлы из temp_files
    temp_files = sorted(temp_dir.glob("*.txt"))
    total_files = len(temp_files)

    print(f"Найдено {total_files} файлов в temp_files")

    processed = 0
    for temp_file in temp_files:
        processed += 1
        if processed % 50 == 0:
            print(f"Обработано {processed}/{total_files} файлов...")

        try:
            with open(temp_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Распределяем матчи по патчам
            for match_id, match_data in data.items():
                start_time = match_data.get("startDateTime", 0)
                patch = get_patch_name(start_time)

                # Добавляем только если нет дубля
                if match_id not in patches[patch]:
                    patches[patch][match_id] = match_data

        except Exception as e:
            print(f"Ошибка при обработке {temp_file.name}: {e}")
            continue

    # Сохраняем результаты
    for patch_name, matches in patches.items():
        if not matches:
            print(f"Патч {patch_name}: нет данных")
            continue

        output_file = output_dir / f"matches_{patch_name}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(matches, f, ensure_ascii=False, separators=(',', ':'))

        print(f"Патч {patch_name}: {len(matches)} уникальных матчей → {output_file.name}")

    total_matches = sum(len(m) for m in patches.values())
    print(f"\nВсего уникальных матчей: {total_matches}")

if __name__ == "__main__":
    main()
