"""
List contests found in an OpenElections-style CSV.

Expected columns:
  county, precinct (optional), office, district (optional), party, candidate, votes/total_votes

Usage:
  py scripts/list_contests.py --csv Data/20221108__ga__general__precinct.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def _pick_votes_col(columns: list[str]) -> str:
    cols = {c.strip(): c for c in columns}
    if "total_votes" in cols:
        return cols["total_votes"]
    if "votes" in cols:
        return cols["votes"]
    raise SystemExit("CSV must contain a 'votes' or 'total_votes' column.")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, type=Path, help="Input CSV path")
    ap.add_argument("--office", default=None, help="Filter to a single office (exact match)")
    args = ap.parse_args()

    df = pd.read_csv(args.csv, dtype=str)
    votes_col = _pick_votes_col(list(df.columns))

    required = {"county", "office", "candidate", votes_col}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"Missing required columns: {sorted(missing)}")

    df["district"] = df.get("district", "").fillna("").astype(str).str.strip()
    df["office"] = df["office"].fillna("").astype(str).str.strip()
    if args.office:
        df = df[df["office"] == args.office]

    contests = (
        df[["office", "district"]]
        .drop_duplicates()
        .sort_values(["office", "district"], kind="mergesort")
        .reset_index(drop=True)
    )
    print(f"{args.csv}: {len(contests)} contests")
    for _, r in contests.iterrows():
        office = r["office"]
        district = r["district"]
        if district:
            print(f"- {office} / {district}")
        else:
            print(f"- {office}")


if __name__ == "__main__":
    main()

