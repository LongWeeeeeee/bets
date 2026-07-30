import argparse
import json
import random
from pathlib import Path


def iter_matches(files):
    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            for match_id, match in data.items():
                yield str(match_id), match
        else:
            for match in data:
                match_id = match.get("id") if isinstance(match, dict) else None
                if match_id is None:
                    continue
                yield str(match_id), match


def reservoir_sample(files, k, seed):
    rng = random.Random(seed)
    sample = []
    n = 0
    for match_id, match in iter_matches(files):
        n += 1
        if len(sample) < k:
            sample.append((match_id, match))
        else:
            j = rng.randrange(n)
            if j < k:
                sample[j] = (match_id, match)
    return sample, n


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=str, default="/Users/alex/Documents/ingame/bets_data/analise_pub_matches/json_parts_split_from_object")
    parser.add_argument("--out-file", type=str, default="/Users/alex/Documents/ingame/bets_data/analise_pub_matches/extracted_100k_matches.json")
    parser.add_argument("--size", type=int, default=100000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    files = sorted(data_dir.glob("combined*.json"))
    if not files:
        raise FileNotFoundError(f"Нет combined*.json в {data_dir}")

    print(f"Файлов: {len(files)}")
    print(f"Сэмплинг {args.size:,} матчей (seed={args.seed})...")
    sample, total = reservoir_sample(files, args.size, args.seed)

    if len(sample) < args.size:
        print(f"⚠️  В данных всего {total:,} матчей, получено {len(sample):,}")
    else:
        print(f"✅ Отобрано {len(sample):,} матчей из {total:,}")

    out_path = Path(args.out_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_dict = {match_id: match for match_id, match in sample}
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out_dict, f)

    meta_path = out_path.with_suffix(".meta.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(
            {"size": len(out_dict), "total": total, "seed": args.seed, "data_dir": str(data_dir)},
            f,
            ensure_ascii=False,
            indent=2,
        )
    print(f"Сохранено: {out_path}")
    print(f"Сохранено: {meta_path}")


if __name__ == "__main__":
    main()
