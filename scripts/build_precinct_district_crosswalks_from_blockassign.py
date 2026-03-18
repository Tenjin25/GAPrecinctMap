"""
Build precinct(VTD20)-to-district weighted crosswalk CSVs from Census BlockAssign.

Reads:
  - Data/BlockAssign_ST13_GA.zip
    - BlockAssign_ST13_GA_VTD.txt   (BLOCKID, COUNTYFP, DISTRICT)
    - BlockAssign_ST13_GA_CD.txt    (BLOCKID, DISTRICT)
    - BlockAssign_ST13_GA_SLDL.txt  (BLOCKID, DISTRICT)
    - BlockAssign_ST13_GA_SLDU.txt  (BLOCKID, DISTRICT)

Writes:
  - Data/crosswalks/precinct_to_cd118.csv
  - Data/crosswalks/precinct_to_2022_state_house.csv
  - Data/crosswalks/precinct_to_2024_state_house.csv
  - Data/crosswalks/precinct_to_2022_state_senate.csv
  - Data/crosswalks/precinct_to_2024_state_senate.csv

The weight is block-count share within each precinct key:
  area_weight = blocks_in_(precinct,district) / total_blocks_in_precinct
"""

from __future__ import annotations

import argparse
import csv
import io
import math
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Any


STATE_FIPS = "13"


def normalize_district_number(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    digits = "".join(ch for ch in s if ch.isdigit())
    if digits:
        return str(int(digits))
    return s.upper()


def sort_district_key(raw: str) -> Any:
    return int(raw) if str(raw).isdigit() else str(raw)


def build_block_to_precinct_map(zip_path: Path, vtd_member: str) -> dict[str, str]:
    block_to_precinct: dict[str, str] = {}
    with zipfile.ZipFile(zip_path) as zf:
        with zf.open(vtd_member) as fh:
            txt = io.TextIOWrapper(fh, encoding="utf-8-sig", errors="replace", newline="")
            reader = csv.DictReader(txt, delimiter="|")
            needed = {"BLOCKID", "COUNTYFP", "DISTRICT"}
            if not needed.issubset(set(reader.fieldnames or [])):
                raise SystemExit(f"{vtd_member} is missing required columns: {sorted(needed)}")

            for row in reader:
                blockid = str(row.get("BLOCKID") or "").strip()
                countyfp = str(row.get("COUNTYFP") or "").strip().zfill(3)
                district = str(row.get("DISTRICT") or "").strip().upper()
                if not blockid or not countyfp or not district:
                    continue
                precinct_key = f"{STATE_FIPS}{countyfp}{district}"
                block_to_precinct[blockid] = precinct_key
    return block_to_precinct


def build_weight_rows(
    zip_path: Path,
    *,
    block_to_precinct: dict[str, str],
    district_member: str,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    pair_counts: dict[tuple[str, str], int] = defaultdict(int)
    precinct_totals: dict[str, int] = defaultdict(int)
    matched_blocks = 0
    skipped_blocks = 0

    with zipfile.ZipFile(zip_path) as zf:
        with zf.open(district_member) as fh:
            txt = io.TextIOWrapper(fh, encoding="utf-8-sig", errors="replace", newline="")
            reader = csv.DictReader(txt, delimiter="|")
            needed = {"BLOCKID", "DISTRICT"}
            if not needed.issubset(set(reader.fieldnames or [])):
                raise SystemExit(f"{district_member} is missing required columns: {sorted(needed)}")

            for row in reader:
                blockid = str(row.get("BLOCKID") or "").strip()
                district_raw = str(row.get("DISTRICT") or "").strip()
                district_num = normalize_district_number(district_raw)
                precinct_key = block_to_precinct.get(blockid, "")
                if not blockid or not precinct_key or not district_num:
                    skipped_blocks += 1
                    continue
                matched_blocks += 1
                pair_counts[(precinct_key, district_num)] += 1
                precinct_totals[precinct_key] += 1

    rows: list[dict[str, Any]] = []
    for (precinct_key, district_num), block_count in pair_counts.items():
        total = precinct_totals.get(precinct_key, 0)
        if total <= 0:
            continue
        area_weight = block_count / total
        if not math.isfinite(area_weight) or area_weight <= 0:
            continue
        rows.append(
            {
                "precinct_key": precinct_key,
                "district_num": district_num,
                "area_weight": f"{area_weight:.10f}",
                "block_count": str(block_count),
                "precinct_block_count": str(total),
            }
        )

    rows.sort(key=lambda r: (str(r["precinct_key"]), sort_district_key(str(r["district_num"]))))
    stats = {
        "precincts": len(precinct_totals),
        "rows": len(rows),
        "matched_blocks": matched_blocks,
        "skipped_blocks": skipped_blocks,
    }
    return rows, stats


def write_crosswalk_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["precinct_key", "district_num", "area_weight", "block_count", "precinct_block_count"]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--blockassign-zip", type=Path, default=Path("Data/BlockAssign_ST13_GA.zip"))
    ap.add_argument("--vtd-member", default="BlockAssign_ST13_GA_VTD.txt")
    ap.add_argument("--cd-member", default="BlockAssign_ST13_GA_CD.txt")
    ap.add_argument("--sldl-member", default="BlockAssign_ST13_GA_SLDL.txt")
    ap.add_argument("--sldu-member", default="BlockAssign_ST13_GA_SLDU.txt")
    ap.add_argument("--out-dir", type=Path, default=Path("Data/crosswalks"))
    ap.add_argument(
        "--copy-2022-to-2024",
        action="store_true",
        help="Also write 2024 legislative files from the same source rows. "
        "By default, existing 2024 files are preserved.",
    )
    args = ap.parse_args()

    if not args.blockassign_zip.exists():
        raise SystemExit(f"Missing block assignment zip: {args.blockassign_zip}")

    block_to_precinct = build_block_to_precinct_map(args.blockassign_zip, args.vtd_member)
    print(f"Loaded block->precinct map: {len(block_to_precinct)} blocks")

    cd_rows, cd_stats = build_weight_rows(
        args.blockassign_zip,
        block_to_precinct=block_to_precinct,
        district_member=args.cd_member,
    )
    cd_path = args.out_dir / "precinct_to_cd118.csv"
    write_crosswalk_csv(cd_path, cd_rows)
    print(
        f"Wrote {cd_path} ({cd_stats['rows']} rows, {cd_stats['precincts']} precincts, "
        f"{cd_stats['matched_blocks']} matched blocks)"
    )

    sldl_rows, sldl_stats = build_weight_rows(
        args.blockassign_zip,
        block_to_precinct=block_to_precinct,
        district_member=args.sldl_member,
    )
    sldl_2022_path = args.out_dir / "precinct_to_2022_state_house.csv"
    sldl_2024_path = args.out_dir / "precinct_to_2024_state_house.csv"
    write_crosswalk_csv(sldl_2022_path, sldl_rows)
    wrote_house_2024 = False
    if args.copy_2022_to_2024 or not sldl_2024_path.exists():
        write_crosswalk_csv(sldl_2024_path, sldl_rows)
        wrote_house_2024 = True
    if wrote_house_2024:
        print(
            f"Wrote {sldl_2022_path} and {sldl_2024_path} ({sldl_stats['rows']} rows, "
            f"{sldl_stats['precincts']} precincts, {sldl_stats['matched_blocks']} matched blocks)"
        )
    else:
        print(
            f"Wrote {sldl_2022_path} ({sldl_stats['rows']} rows, {sldl_stats['precincts']} precincts, "
            f"{sldl_stats['matched_blocks']} matched blocks); preserved existing {sldl_2024_path}"
        )

    sldu_rows, sldu_stats = build_weight_rows(
        args.blockassign_zip,
        block_to_precinct=block_to_precinct,
        district_member=args.sldu_member,
    )
    sldu_2022_path = args.out_dir / "precinct_to_2022_state_senate.csv"
    sldu_2024_path = args.out_dir / "precinct_to_2024_state_senate.csv"
    write_crosswalk_csv(sldu_2022_path, sldu_rows)
    wrote_senate_2024 = False
    if args.copy_2022_to_2024 or not sldu_2024_path.exists():
        write_crosswalk_csv(sldu_2024_path, sldu_rows)
        wrote_senate_2024 = True
    if wrote_senate_2024:
        print(
            f"Wrote {sldu_2022_path} and {sldu_2024_path} ({sldu_stats['rows']} rows, "
            f"{sldu_stats['precincts']} precincts, {sldu_stats['matched_blocks']} matched blocks)"
        )
    else:
        print(
            f"Wrote {sldu_2022_path} ({sldu_stats['rows']} rows, {sldu_stats['precincts']} precincts, "
            f"{sldu_stats['matched_blocks']} matched blocks); preserved existing {sldu_2024_path}"
        )


if __name__ == "__main__":
    main()
