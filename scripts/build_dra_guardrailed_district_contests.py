"""
Build a guardrailed district_contests set from production + DRA outputs.

Strategy:
- Start from production Data/district_contests.
- Replace with DRA file only if:
  1) district row counts are equal, and
  2) absolute statewide total-vote delta is <= threshold.

Outputs a merged directory + audit report.
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def sum_total_votes(payload: dict) -> int:
    results = (((payload or {}).get("general") or {}).get("results") or {})
    return sum(int((row or {}).get("total_votes", 0) or 0) for row in results.values())


def row_count(payload: dict) -> int:
    results = (((payload or {}).get("general") or {}).get("results") or {})
    return len(results)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prod-dir", type=Path, default=Path("Data/district_contests"))
    ap.add_argument("--dra-dir", type=Path, default=Path("Data/district_contests_dra_csv"))
    ap.add_argument("--out-dir", type=Path, default=Path("Data/district_contests_dra_guarded"))
    ap.add_argument("--max-total-delta", type=int, default=1000)
    ap.add_argument("--allow-row-mismatch", action="store_true")
    args = ap.parse_args()

    prod_manifest = load_json(args.prod_dir / "manifest.json")
    files = [f["file"] for f in (prod_manifest.get("files") or []) if isinstance(f, dict) and f.get("file")]

    args.out_dir.mkdir(parents=True, exist_ok=True)
    decisions = []

    for fname in files:
        prod_file = args.prod_dir / fname
        dra_file = args.dra_dir / fname
        out_file = args.out_dir / fname

        use_dra = False
        reason = "prod_default"
        if dra_file.exists():
            p = load_json(prod_file)
            d = load_json(dra_file)
            p_rows = row_count(p)
            d_rows = row_count(d)
            p_total = sum_total_votes(p)
            d_total = sum_total_votes(d)
            total_delta = d_total - p_total
            rows_ok = args.allow_row_mismatch or (p_rows == d_rows)
            totals_ok = abs(total_delta) <= args.max_total_delta
            if rows_ok and totals_ok:
                use_dra = True
                reason = "dra_pass"
            else:
                reason = f"prod_guard(rows {p_rows}/{d_rows}, delta {total_delta})"
        else:
            reason = "prod_no_dra_file"

        chosen = dra_file if use_dra else prod_file
        out_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(chosen, out_file)
        decisions.append({"file": fname, "chosen": "dra" if use_dra else "prod", "reason": reason})

    # Write manifest copied from production structure but pointing to merged files.
    merged_manifest = json.loads(json.dumps(prod_manifest))
    for entry in merged_manifest.get("files", []):
        if isinstance(entry, dict) and entry.get("file"):
            pass
    (args.out_dir / "manifest.json").write_text(json.dumps(merged_manifest, indent=2), encoding="utf-8")

    report = {
        "prod_dir": str(args.prod_dir),
        "dra_dir": str(args.dra_dir),
        "out_dir": str(args.out_dir),
        "max_total_delta": args.max_total_delta,
        "allow_row_mismatch": args.allow_row_mismatch,
        "chosen_dra": sum(1 for d in decisions if d["chosen"] == "dra"),
        "chosen_prod": sum(1 for d in decisions if d["chosen"] == "prod"),
        "decisions": decisions,
    }
    (args.out_dir / "guardrail_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(
        f"Wrote {args.out_dir} with {report['chosen_dra']} DRA files and "
        f"{report['chosen_prod']} production files."
    )


if __name__ == "__main__":
    main()

