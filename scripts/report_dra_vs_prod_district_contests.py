"""
Compare production district_contests vs district_contests_dra outputs.

Writes a JSON report with per-file vote deltas and summary counts.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def sum_party_totals(payload: dict) -> dict[str, int]:
    out = {"dem": 0, "rep": 0, "other": 0, "total": 0}
    results = (((payload or {}).get("general") or {}).get("results") or {})
    if not isinstance(results, dict):
        return out
    for row in results.values():
        if not isinstance(row, dict):
            continue
        out["dem"] += int(row.get("dem_votes", 0) or 0)
        out["rep"] += int(row.get("rep_votes", 0) or 0)
        out["other"] += int(row.get("other_votes", 0) or 0)
        out["total"] += int(row.get("total_votes", 0) or 0)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prod-dir", type=Path, default=Path("Data/district_contests_2024"))
    ap.add_argument("--dra-dir", type=Path, default=Path("Data/district_contests_dra"))
    ap.add_argument("--out", type=Path, default=Path("Data/district_contests_dra/dra_vs_prod_report.json"))
    args = ap.parse_args()

    prod_manifest = load_json(args.prod_dir / "manifest.json")
    dra_manifest = load_json(args.dra_dir / "manifest.json")
    prod_files = {f["file"] for f in (prod_manifest.get("files") or []) if isinstance(f, dict) and f.get("file")}
    dra_files = {f["file"] for f in (dra_manifest.get("files") or []) if isinstance(f, dict) and f.get("file")}

    common = sorted(prod_files & dra_files)
    only_prod = sorted(prod_files - dra_files)
    only_dra = sorted(dra_files - prod_files)

    diffs = []
    equal_files = 0
    changed_files = 0
    for f in common:
        p = load_json(args.prod_dir / f)
        d = load_json(args.dra_dir / f)
        ps = sum_party_totals(p)
        ds = sum_party_totals(d)
        delta = {k: ds[k] - ps[k] for k in ps.keys()}
        same = delta == {"dem": 0, "rep": 0, "other": 0, "total": 0}
        if same:
            equal_files += 1
        else:
            changed_files += 1
        diffs.append(
            {
                "file": f,
                "same_totals": same,
                "prod_totals": ps,
                "dra_totals": ds,
                "delta_dra_minus_prod": delta,
            }
        )

    report = {
        "prod_dir": str(args.prod_dir),
        "dra_dir": str(args.dra_dir),
        "counts": {
            "prod_manifest_files": len(prod_files),
            "dra_manifest_files": len(dra_files),
            "common_files": len(common),
            "equal_total_files": equal_files,
            "changed_total_files": changed_files,
            "only_in_prod": len(only_prod),
            "only_in_dra": len(only_dra),
        },
        "only_in_prod": only_prod,
        "only_in_dra": only_dra,
        "per_file": diffs,
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Wrote {args.out}")
    print(json.dumps(report["counts"], indent=2))


if __name__ == "__main__":
    main()

