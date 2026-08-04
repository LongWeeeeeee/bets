#!/usr/bin/env python3
"""
Analyze hero positions from pub match data.
Processes 30x 500MB JSON files using streaming and multiprocessing.
"""

import json
import orjson
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, Tuple
import sys

# Load hero registry
HERO_FEATURES_PATH = Path(__file__).parent / "base" / "hero_features_processed.json"
MATCH_DATA_DIR = Path(__file__).parent / "bets_data" / "analise_pub_matches" / "json_parts_split_from_object"
OUTPUT_PATH = Path(__file__).parent / "hero_position_stats.json"


def load_hero_registry() -> Dict[int, str]:
    """Load hero ID to name mapping."""
    with open(HERO_FEATURES_PATH, 'r') as f:
        heroes = json.load(f)
    return {data['hero_id']: data['hero_name'] for data in heroes.values()}


def process_single_file(file_path: Path) -> Dict[int, Dict[str, int]]:
    """
    Process a single JSON file and count hero positions.
    Returns: {hero_id: {position: count}}
    """
    hero_position_counts = defaultdict(lambda: defaultdict(int))

    print(f"Processing {file_path.name}...", file=sys.stderr)

    try:
        # Stream parse large JSON file
        with open(file_path, 'rb') as f:
            # Read entire file (orjson is fast enough for 500MB)
            data = orjson.loads(f.read())

        match_count = 0
        for match_id, match_data in data.items():
            if 'players' not in match_data:
                continue

            for player in match_data['players']:
                hero_id = player.get('heroId')
                position = player.get('position')

                if hero_id and position:
                    # Extract position number (e.g., "POSITION_1" -> "1")
                    if isinstance(position, str) and position.startswith('POSITION_'):
                        pos_num = position.split('_')[1]
                        hero_position_counts[hero_id][pos_num] += 1

            match_count += 1
            if match_count % 10000 == 0:
                print(f"  {file_path.name}: {match_count} matches processed", file=sys.stderr)

        print(f"Completed {file_path.name}: {match_count} matches", file=sys.stderr)

    except Exception as e:
        print(f"Error processing {file_path.name}: {e}", file=sys.stderr)
        return {}

    # Convert defaultdict to regular dict for serialization
    return {hero_id: dict(positions) for hero_id, positions in hero_position_counts.items()}


def merge_results(results: list) -> Dict[int, Dict[str, int]]:
    """Merge results from multiple files."""
    merged = defaultdict(lambda: defaultdict(int))

    for result in results:
        for hero_id, positions in result.items():
            for pos, count in positions.items():
                merged[hero_id][pos] += count

    return {hero_id: dict(positions) for hero_id, positions in merged.items()}


def calculate_percentages(hero_stats: Dict[int, Dict[str, int]],
                         hero_names: Dict[int, str]) -> Dict[str, dict]:
    """
    Calculate percentages and format final output.
    Returns: {hero_id: {name, positions: {pos: {games, pct}}}}
    """
    output = {}

    for hero_id, positions in hero_stats.items():
        total_games = sum(positions.values())
        hero_name = hero_names.get(hero_id, f"Unknown_{hero_id}")

        position_data = {}
        for pos in ['1', '2', '3', '4', '5']:
            games = positions.get(pos, 0)
            pct = (games / total_games * 100) if total_games > 0 else 0.0
            position_data[pos] = {
                'games': games,
                'percentage': round(pct, 2)
            }

        output[str(hero_id)] = {
            'hero_id': hero_id,
            'hero_name': hero_name,
            'total_games': total_games,
            'positions': position_data
        }

    return output


def main():
    print("Loading hero registry...", file=sys.stderr)
    hero_names = load_hero_registry()
    print(f"Loaded {len(hero_names)} heroes", file=sys.stderr)

    # Find all JSON files
    json_files = sorted(MATCH_DATA_DIR.glob("*.json"))
    print(f"Found {len(json_files)} JSON files to process", file=sys.stderr)

    if not json_files:
        print(f"No JSON files found in {MATCH_DATA_DIR}", file=sys.stderr)
        return

    # Process files in parallel
    print(f"Processing with {min(8, len(json_files))} workers...", file=sys.stderr)
    results = []

    with ProcessPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(process_single_file, f): f for f in json_files}

        for future in as_completed(futures):
            file_path = futures[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"Failed to process {file_path.name}: {e}", file=sys.stderr)

    print("Merging results...", file=sys.stderr)
    merged_stats = merge_results(results)

    print("Calculating percentages...", file=sys.stderr)
    final_output = calculate_percentages(merged_stats, hero_names)

    # Write output using orjson for speed
    print(f"Writing output to {OUTPUT_PATH}...", file=sys.stderr)
    with open(OUTPUT_PATH, 'wb') as f:
        f.write(orjson.dumps(final_output, option=orjson.OPT_INDENT_2))

    # Print summary
    print("\n=== Summary ===", file=sys.stderr)
    print(f"Total heroes analyzed: {len(final_output)}", file=sys.stderr)
    print(f"Output written to: {OUTPUT_PATH}", file=sys.stderr)

    # Show sample for top 3 heroes by games
    top_heroes = sorted(final_output.values(), key=lambda x: x['total_games'], reverse=True)[:3]
    print("\nTop 3 heroes by total games:", file=sys.stderr)
    for hero in top_heroes:
        print(f"  {hero['hero_name']}: {hero['total_games']:,} games", file=sys.stderr)
        for pos in ['1', '2', '3', '4', '5']:
            pos_data = hero['positions'][pos]
            print(f"    Pos {pos}: {pos_data['games']:,} ({pos_data['percentage']:.1f}%)", file=sys.stderr)


if __name__ == '__main__':
    main()
