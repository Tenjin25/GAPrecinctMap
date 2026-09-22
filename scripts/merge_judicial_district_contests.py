"""Merge validated judicial district overlays into the production folders."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lines-year", type=int, choices=(2022, 2024), required=True)
    args = parser.parse_args()
    source = Path(f"Data/judicial_district_contests_{args.lines_year}")
    target = Path(f"Data/district_contests_{args.lines_year}")
    incoming = json.loads((source / "manifest.json").read_text(encoding="utf-8"))["files"]
    manifest_path = target / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    incoming_names = {row["file"] for row in incoming}
    incoming_keys = {(row["scope"], row["contest_type"], int(row["year"])) for row in incoming}
    manifest["files"] = [
        row for row in manifest.get("files", [])
        if (row.get("scope"), row.get("contest_type"), int(row.get("year", 0))) not in incoming_keys
    ]
    for name in incoming_names:
        shutil.copy2(source / name, target / name)
    manifest["files"].extend(incoming)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"Merged {len(incoming)} judicial files into {target}")


if __name__ == "__main__":
    main()
