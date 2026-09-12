"""Build an isolated hash-verified E281 serving directory, without activation."""
import argparse
from datetime import datetime, timezone
from pathlib import Path
import shutil

from kills_transfer_data import atomic_json, atomic_npz, load_rows, sha256


def build(candidate, pro_rows, output):
    import json
    candidate, output = Path(candidate), Path(output)
    if output.exists():
        raise ValueError("Use a new output directory; never overwrite a serving bundle")
    output.mkdir(parents=True)
    history = load_rows(pro_rows)
    atomic_npz(output / "history.npz", **history)
    files = ["history.npz", "profiles.joblib", "public_models.joblib"]
    selected = {}
    for target in ("total", "side"):
        selected[target] = json.loads((candidate / f"{target}_selection.json").read_text())["chosen"]
        files.extend([f"{target}_{selected[target]}.cbm",
                      f"{target}_{selected[target]}_calibration.joblib"])
    for name in files[1:]:
        temporary = output / (name + ".tmp")
        shutil.copyfile(candidate / name, temporary)
        temporary.replace(output / name)
    cutoff = int(history["ends"].max())
    manifest = {"schema": 1, "selected": selected, "history_last_end": cutoff,
                "history_date": datetime.fromtimestamp(cutoff, timezone.utc).strftime("%d.%m.%Y"),
                "history_rows": len(history["mids"]),
                "history_summary_sha256": sha256(Path(pro_rows) / "summary.json"),
                "files": {name: sha256(output / name) for name in files}}
    atomic_json(output / "manifest.json", manifest)
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--pro-rows", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    print(build(args.candidate, args.pro_rows, args.output))
