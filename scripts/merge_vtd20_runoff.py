"""Merge a separately-built same-year runoff contest into a VTD20 manifest."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", type=Path, required=True)
    ap.add_argument("--target", type=Path, required=True)
    args = ap.parse_args()
    source_manifest = json.loads((args.source / "manifest.json").read_text(encoding="utf-8"))
    item = next(x for x in source_manifest.get("contests", []) if x.get("office") == "U.S. Senate")
    source_file = args.source / "vtd20" / "U.S._Senate.json"
    target_dir = args.target / "vtd20"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / "U.S._Senate_Runoff.json"
    shutil.copyfile(source_file, target_file)
    manifest_path = args.target / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["contests"] = [x for x in manifest.get("contests", []) if x.get("slug") != "U.S._Senate_Runoff"]
    entry = dict(item)
    entry["office"] = "U.S. Senate Runoff"
    entry["slug"] = "U.S._Senate_Runoff"
    entry["outputs"] = {"vtd20": {"path": str(target_file).replace("\\", "/"), "rows": item.get("outputs", {}).get("vtd20", {}).get("rows", 0)}}
    manifest["contests"].append(entry)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Merged {target_file}")


if __name__ == "__main__":
    main()
