"""Rebuild a VTD20 contest manifest from already-generated contest JSON files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--contests-dir", type=Path, required=True)
    ap.add_argument("--csv", required=True)
    args = ap.parse_args()
    rows = []
    for path in sorted((args.contests_dir / "vtd20").glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        rows.append({"office": payload.get("office", ""), "district": payload.get("district", ""),
                     "slug": path.stem, "outputs": {"vtd20": {"path": str(path).replace("\\", "/"), "rows": len(payload.get("results", {}))}}})
    (args.contests_dir / "manifest.json").write_text(json.dumps({"csv": args.csv, "levels": ["vtd20"], "contests": rows}, indent=2), encoding="utf-8")
    print(f"Wrote {args.contests_dir / 'manifest.json'} ({len(rows)} contests)")


if __name__ == "__main__":
    main()
