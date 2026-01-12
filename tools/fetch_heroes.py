#!/usr/bin/env python3
"""Fetch heroes list from OpenDota API and save to data/heroes.json."""

import json
import time
from pathlib import Path
from typing import Dict

import requests


def fetch_heroes() -> Dict[int, str]:
    """Fetch heroes from OpenDota API with retry logic."""
    url = "https://api.opendota.com/api/heroes"
    
    for attempt in range(3):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            heroes_data = response.json()
            
            # Convert to {id: localized_name} format
            return {hero["id"]: hero["localized_name"] for hero in heroes_data}
        except requests.RequestException as e:
            if attempt < 2:
                wait_time = 2 ** attempt
                print(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise


def main() -> None:
    print("Fetching heroes from OpenDota API...")
    heroes = fetch_heroes()
    
    output_path = Path(__file__).parent.parent / "data" / "heroes.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(heroes, f, ensure_ascii=False, indent=2)
    
    print(f"Saved {len(heroes)} heroes to {output_path}")


if __name__ == "__main__":
    main()
