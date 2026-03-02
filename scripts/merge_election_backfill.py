"""
Merge a small backfill results CSV into a base OpenElections-style CSV.

Typical use:
  - base:    Data/20210105__ga__runoff.csv (missing some counties)
  - backfill: output from scripts/ingest_county_precincts.py for the missing counties

By default, if a county appears in the backfill, all rows for that county are removed
from the base before appending the backfill rows.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


PREFERRED_ORDER = ["county", "precinct", "office", "district", "party", "candidate", "total_votes", "votes"]


def _norm_county(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip().str.upper()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True, help="Base CSV (will be kept except overwritten counties)")
    ap.add_argument("--backfill", type=Path, required=True, help="Backfill CSV (wins for its counties)")
    ap.add_argument("--out", type=Path, required=True, help="Output merged CSV")
    ap.add_argument("--county-col", default="county", help="County column name (default: county)")
    args = ap.parse_args()

    base = pd.read_csv(args.base, dtype=str, low_memory=False)
    back = pd.read_csv(args.backfill, dtype=str, low_memory=False)

    if args.county_col not in base.columns:
        raise SystemExit(f"--base missing county column: {args.county_col!r}")
    if args.county_col not in back.columns:
        raise SystemExit(f"--backfill missing county column: {args.county_col!r}")

    base_counties = set(_norm_county(base[args.county_col]).unique()) - {""}
    back_counties = set(_norm_county(back[args.county_col]).unique()) - {""}
    if not back_counties:
        raise SystemExit("Backfill has zero counties; nothing to merge.")

    keep = base[~_norm_county(base[args.county_col]).isin(back_counties)].copy()

    # Column alignment: preserve any extra columns found in either file.
    all_cols = list(dict.fromkeys(list(base.columns) + list(back.columns)))
    keep = keep.reindex(columns=all_cols)
    back = back.reindex(columns=all_cols)

    merged = pd.concat([keep, back], ignore_index=True)

    # Prefer a standard-ish ordering if columns exist.
    ordered = [c for c in PREFERRED_ORDER if c in merged.columns]
    remaining = [c for c in merged.columns if c not in ordered]
    merged = merged[ordered + remaining]

    args.out.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(args.out, index=False)

    merged_counties = set(_norm_county(merged[args.county_col]).unique()) - {""}

    print(f"Base counties: {len(base_counties)}")
    print(f"Backfill counties: {len(back_counties)}")
    print(f"Merged counties: {len(merged_counties)}")
    print(f"Wrote: {args.out}")


if __name__ == "__main__":
    main()

