"""
Batch-build VTD20 (GEOID20)-keyed contest JSONs for multiple election CSVs/years.

This glues together:
  - scripts/build_vtd20_crosswalk.py (name-fuzzy crosswalk, year-specific)
  - scripts/build_vtd10_key_to_vtd20_keymap.py (pre-2020 supplemental key map via VTD10->VTD20)
  - scripts/build_contest_jsons.py   (writes vtd20 results keyed by GEOID20)

Usage:
  py scripts/batch_build_vtd20_years.py --offices "Governor,Lieutenant Governor,Secretary of State" --years 2014,2018,2022

Defaults:
  - Scans Data/ for '*__ga__general__precinct*.csv' and '*__ga__general__precinct-level.csv'
  - Output under Data/derived_vtd20/<year>/
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd


def _normalize_office_loose(office: str) -> str:
    s = (office or "").replace("\u00a0", " ").strip().upper()
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace(" OF ", " ")
    return s


def _find_year_from_filename(name: str) -> str | None:
    m = re.match(r"^(?P<date>\d{8})__ga__general", name)
    if m:
        return m.group("date")[:4]
    m = re.match(r"^(?P<date>\d{8})__ga__runoff", name)
    if m:
        return m.group("date")[:4]
    return None


def _default_csvs(data_dir: Path) -> list[Path]:
    pats = [
        "*__ga__general__precinct.csv",
        "*__ga__general__precinct-level.csv",
    ]
    out: list[Path] = []
    for pat in pats:
        out.extend(sorted(data_dir.glob(pat)))
    # Keep deterministic order by filename
    return sorted(out, key=lambda p: p.name)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", type=Path, default=Path("Data"))
    ap.add_argument("--years", default=None, help="Comma-separated list of years to include (e.g. 2014,2018,2022)")
    ap.add_argument("--csvs", default=None, help="Comma-separated list of CSV paths (overrides --years scan)")
    ap.add_argument(
        "--offices",
        required=True,
        help="Comma-separated list of offices (exact match) to build, e.g. 'Governor,Lieutenant Governor,Secretary of State'",
    )
    ap.add_argument("--vtd20-geojson", type=Path, default=Path("Data/tl_2020_13_vtd20.geojson"))
    ap.add_argument("--vtd20-join-prop", default="join_key_name")
    ap.add_argument("--threshold", type=float, default=0.9, help="Crosswalk fuzzy threshold")
    ap.add_argument("--vtd10-zip", type=Path, default=Path("Data/tl_2012_13_vtd10.zip"))
    ap.add_argument("--county-geojson", type=Path, default=Path("Data/tl_2020_13_county20.geojson"))
    ap.add_argument("--vtd10-to-vtd20-best", type=Path, default=Path("Data/vtd10_to_vtd20_best.json"))
    ap.add_argument("--vtd10-keymap", type=Path, default=Path("Data/vtd10_keys_to_vtd20_geoid.json"))
    ap.add_argument("--rebuild-vtd10-keymap", action="store_true")
    ap.add_argument("--out-base", type=Path, default=Path("Data/derived_vtd20"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.csvs:
        csvs = [Path(x.strip()) for x in args.csvs.split(",") if x.strip()]
    else:
        csvs = _default_csvs(args.data_dir)

    years_filter: set[str] = set()
    if args.years:
        years_filter = {y.strip() for y in args.years.split(",") if y.strip()}

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

    selected_pre2020 = [(y, p) for y, p in selected if y.isdigit() and int(y) < 2020]
    if selected_pre2020 and (args.rebuild_vtd10_keymap or not args.vtd10_keymap.exists()):
        cmd0 = [
            sys.executable,
            "scripts/build_vtd10_key_to_vtd20_keymap.py",
            "--vtd10-zip",
            str(args.vtd10_zip),
            "--county-geojson",
            str(args.county_geojson),
            "--best",
            str(args.vtd10_to_vtd20_best),
            "--out",
            str(args.vtd10_keymap),
        ]
        print("\nPre-2020 supplemental keymap:", " ".join(cmd0))
        if not args.dry_run:
            subprocess.check_call(cmd0)

    for year, csv_path in selected:
        try:
            available = pd.read_csv(csv_path, usecols=["office"], dtype=str)["office"].dropna().astype(str).tolist()
        except Exception:
            available = []
        available_norm = {_normalize_office_loose(x) for x in available if x and x.strip()}
        requested_norm = {_normalize_office_loose(x) for x in args.offices.split(",") if x.strip()}
        if requested_norm and available_norm and not (available_norm & requested_norm):
            print(f"\n=== {year} ({csv_path}) ===")
            print("Skipping: none of the requested offices appear in this CSV.")
            continue

        out_dir = args.out_base / year
        crosswalk = out_dir / f"vtd20_crosswalk_{year}.json"

        cmd1 = [
            sys.executable,
            "scripts/build_vtd20_crosswalk.py",
            "--vtd-geojson",
            str(args.vtd20_geojson),
            "--csv",
            str(csv_path),
            "--out",
            str(crosswalk),
            "--threshold",
            str(args.threshold),
        ]
        cmd2 = [
            sys.executable,
            "scripts/build_contest_jsons.py",
            "--csv",
            str(csv_path),
            "--out",
            str(out_dir / "contests"),
            "--levels",
            "vtd20",
            "--vtd20-geojson",
            str(args.vtd20_geojson),
            "--vtd20-join-prop",
            str(args.vtd20_join_prop),
            "--vtd20-crosswalk",
            str(crosswalk),
            "--only-offices",
            str(args.offices),
        ]
        if args.dry_run:
            cmd2.append("--dry-run")
        if year.isdigit() and int(year) < 2020:
            cmd2.extend(["--vtd20-supplemental-keymap", str(args.vtd10_keymap)])

        print(f"\n=== {year} ({csv_path}) ===")
        print("Crosswalk:", " ".join(cmd1))
        if not args.dry_run:
            out_dir.mkdir(parents=True, exist_ok=True)
            subprocess.check_call(cmd1)
        print("Build vtd20:", " ".join(cmd2))
        subprocess.check_call(cmd2)


if __name__ == "__main__":
    main()
