from maps_research import merge_temp_files_by_size
mkdir = '/Users/alex/Documents/ingame/bets_data/analise_pub_matches'
merged_files = merge_temp_files_by_size(
        mkdir=mkdir,
        max_size_mb=500,
        cleanup=True  # Удаляем temp_files после объединения
    )