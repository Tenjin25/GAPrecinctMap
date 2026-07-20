"""
Batch-run VTD10→VTD20 block-weighted contest reallocation for older years.

Wraps scripts/reallocate_vtd10_contests_to_vtd20.py for multiple election CSVs.

Usage:
  py scripts/batch_reallocate_vtd10_blockpath.py --years 2012,2014,2016,2018

Defaults:
  - Scans Data/ for '*__ga__general__precinct*.csv' (same priority as batch_build_vtd20_years)
  - Statewide + top-ticket federal offices in each CSV (PSC / US House / legislative seats excluded)
  - Year VTD20 crosswalk + supplemental keymap as unmatched-precinct fallback
  - Out: Data/derived_vtd20_blockpath/<year>/contests
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd

# Reuse CSV discovery from the shortcut batch builder.
_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from batch_build_vtd20_years import _default_csvs, _find_year_from_filename  # noqa: E402
import build_contest_jsons as bcj  # noqa: E402


# Statewide + top-ticket federal offices only (matched after normalize_office_loose).
# Exclude US House, State House/Senate legislative seats, DA, and locals.
# Keep variants that appear across OpenElections years.
DEFAULT_OFFICE_NORMS = {
    "PRESIDENT",
    "PRESIDENT THE UNITED STATES",
    "GOVERNOR",
    "LIEUTENANT GOVERNOR",
    "SECRETARY STATE",
    "ATTORNEY GENERAL",
    "COMMISSIONER AGRICULTURE",
    "COMMISSIONER INSURANCE",
    "COMMISSIONER LABOR",
    "STATE SCHOOL SUPERINTENDENT",
    "UNITED STATES SENATOR",
    "U S SENATE",
    "U S SENATE SPECIAL",
}

PSC_RE = re.compile(r"PUBLIC\s+SERVICE\s+COMMISSION", re.IGNORECASE)


def _select_offices(csv_path: Path, offices_arg: str | None) -> list[str]:
    df = pd.read_csv(csv_path, usecols=["office"], dtype=str)
    available = sorted({str(x).strip() for x in df["office"].dropna().tolist() if str(x).strip()})
    if offices_arg:
        requested = {bcj.normalize_office_loose(x) for x in offices_arg.split(",") if x.strip()}
        return [o for o in available if bcj.normalize_office_loose(o) in requested]

    selected: list[str] = []
    for o in available:
        if PSC_RE.search(o):
            continue
        if bcj.normalize_office_loose(o) in DEFAULT_OFFICE_NORMS:
            selected.append(o)
    return selected


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-dir", type=Path, default=Path("Data"))
    ap.add_argument("--years", default="2012,2014,2016,2018,2020,2022,2024")
    ap.add_argument("--csvs", default=None, help="Comma-separated CSV paths (overrides --years scan)")
    ap.add_argument(
        "--offices",
        default=None,
        help="Optional comma-separated office names (exact CSV names or loose-normalized match). "
        "Default: statewide + top-ticket federal set (no PSC / US House / legislative seats).",
    )
    ap.add_argument("--out-base", type=Path, default=Path("Data/derived_vtd20_blockpath"))
    ap.add_argument("--crosswalk-base", type=Path, default=Path("Data/derived_vtd20"))
    ap.add_argument("--vtd10-to-vtd20", type=Path, default=Path("Data/vtd10_to_vtd20_crosswalk.csv"))
    ap.add_argument("--vtd20-supplemental-keymap", type=Path, default=Path("Data/vtd10_keys_to_vtd20_geoid.json"))
    ap.add_argument("--aliases", type=Path, default=Path("Data/crosswalks/precinct_name_aliases.json"))
    ap.add_argument("--fuzzy-threshold", type=float, default=0.85)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--write-match-reports", action="store_true")
    args = ap.parse_args()

    if args.csvs:
        csvs = [Path(x.strip()) for x in args.csvs.split(",") if x.strip()]
    else:
        csvs = _default_csvs(args.data_dir)

    years_filter = {y.strip() for y in args.years.split(",") if y.strip()} if args.years else set()

    selected: list[tuple[str, Path]] = []
    for p in csvs:
        y = _find_year_from_filename(p.name)
        if not y:
            continue
        if years_filter and y not in years_filter:
            continue
        selected.append((y, p))

    if not selected:
        raise SystemExit("No matching CSVs found for the requested years.")

    for year, csv_path in selected:
        offices = _select_offices(csv_path, args.offices)
        print(f"\n=== {year} ({csv_path}) ===")
        if not offices:
            print("Skipping: no matching statewide/federal offices in CSV.")
            continue

        out_dir = args.out_base / year / "contests"
        crosswalk = args.crosswalk_base / year / f"vtd20_crosswalk_{year}.json"
        match_report = args.out_base / year / "match_report.json"

        cmd = [
            sys.executable,
            "scripts/reallocate_vtd10_contests_to_vtd20.py",
            "--csv",
            str(csv_path),
            "--year",
            year,
            "--out",
            str(out_dir),
            "--vtd10-to-vtd20",
            str(args.vtd10_to_vtd20),
            "--vtd20-supplemental-keymap",
            str(args.vtd20_supplemental_keymap),
            "--fuzzy-threshold",
            str(args.fuzzy_threshold),
            "--only-offices",
            ",".join(offices),
            "--aliases",
            str(args.aliases),
        ]
        if crosswalk.exists():
            cmd.extend(["--vtd20-crosswalk", str(crosswalk)])
        else:
            print(f"WARNING: missing year crosswalk {crosswalk}; fallback will use keymap/geojson only")
        if args.write_match_reports:
            cmd.extend(["--write-match-report", str(match_report)])
        if args.dry_run:
            cmd.append("--dry-run")

        print("Offices:", ", ".join(offices))
        print("Run:", " ".join(cmd))
        subprocess.check_call(cmd)


if __name__ == "__main__":
    main()
