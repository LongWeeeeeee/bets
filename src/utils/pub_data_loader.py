"""
Streaming loader for large pub match JSON files.

Uses ijson for memory-efficient parsing of large JSON files.
Processes files one by one, yielding matches without loading everything into memory.
"""

import json
import logging
from glob import glob
from pathlib import Path
from typing import Any, Callable, Dict, Generator, Optional

logger = logging.getLogger(__name__)

# Default path to pub matches
DEFAULT_PUB_DIR = 'bets_data/analise_pub_matches/json_parts_split_from_object'

# Try to import ijson for streaming, fallback to orjson or standard json
try:
    import ijson
    IJSON_AVAILABLE = True
except ImportError:
    IJSON_AVAILABLE = False
    logger.warning("ijson not available, will use standard json (higher memory usage)")

try:
    import orjson
    ORJSON_AVAILABLE = True
except ImportError:
    ORJSON_AVAILABLE = False


def iter_matches_from_file(file_path: str) -> Generator[tuple[str, Dict[str, Any]], None, None]:
    """
    Iterate over matches in a single JSON file using streaming parser.
    
    Yields:
        (match_id, match_data) tuples
    """
    if IJSON_AVAILABLE:
        # Use ijson for streaming - memory efficient
        with open(file_path, 'rb') as f:
            # Parse as key-value pairs from root object
            parser = ijson.kvitems(f, '')
            for match_id, match_data in parser:
                yield match_id, match_data
    elif ORJSON_AVAILABLE:
        # orjson is faster but still loads whole file
        with open(file_path, 'rb') as f:
            data = orjson.loads(f.read())
        for match_id, match_data in data.items():
            yield match_id, match_data
    else:
        # Standard json - slowest, loads whole file
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for match_id, match_data in data.items():
            yield match_id, match_data


def iter_all_pub_matches(
    input_dir: str = DEFAULT_PUB_DIR,
    file_pattern: str = 'combined*.json'
) -> Generator[tuple[str, Dict[str, Any]], None, None]:
    """
    Iterate over ALL pub matches from directory, streaming file by file.
    
    Memory efficient - only one match in memory at a time.
    
    Args:
        input_dir: Directory with JSON files
        file_pattern: Glob pattern for files
        
    Yields:
        (match_id, match_data) tuples
    """
    all_files = sorted(glob(f"{input_dir}/{file_pattern}"))
    logger.info(f"Found {len(all_files)} pub match files in {input_dir}")
    
    if not all_files:
        logger.warning(f"No files matching {file_pattern} in {input_dir}")
        return
    
    for file_path in all_files:
        file_name = Path(file_path).name
        logger.info(f"Processing {file_name}...")
        
        try:
            match_count = 0
            for match_id, match_data in iter_matches_from_file(file_path):
                match_count += 1
                yield match_id, match_data
            
            logger.info(f"  {file_name}: {match_count} matches")
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")


def process_pub_matches_streaming(
    processor_fn: Callable[[str, Dict[str, Any]], None],
    input_dir: str = DEFAULT_PUB_DIR,
    file_pattern: str = 'combined*.json',
    show_progress: bool = True
) -> int:
    """
    Process all pub matches with a custom processor function.
    
    Args:
        processor_fn: Function(match_id, match_data) to call for each match
        input_dir: Directory with JSON files
        file_pattern: Glob pattern for files
        show_progress: Show tqdm progress bar
        
    Returns:
        Total number of matches processed
    """
    total_matches = 0
    
    if show_progress:
        try:
            from tqdm import tqdm
            # First count total files for progress
            all_files = sorted(glob(f"{input_dir}/{file_pattern}"))
            
            for file_path in tqdm(all_files, desc="Processing files"):
                for match_id, match_data in iter_matches_from_file(file_path):
                    processor_fn(match_id, match_data)
                    total_matches += 1
        except ImportError:
            show_progress = False
    
    if not show_progress:
        for match_id, match_data in iter_all_pub_matches(input_dir, file_pattern):
            processor_fn(match_id, match_data)
            total_matches += 1
    
    return total_matches


def count_pub_matches(input_dir: str = DEFAULT_PUB_DIR) -> int:
    """Count total matches without loading data."""
    count = 0
    for _ in iter_all_pub_matches(input_dir):
        count += 1
    return count
