"""Rebuild 2022 statewide district slices from the RDH precinct archive.

The existing VTD20 path can lose or duplicate 2022 precincts after the 2022
precinct splits.  RDH supplies one statewide precinct table plus separate
congressional, state-house, and state-senate tables.  When a precinct is split
between districts, the district-specific table contains one row per piece.
The district-race votes on those rows provide a stable voter-weighted split.

This script writes the district JSON shape consumed by the app and optional
CSV crosswalks.  It deliberately does not modify the raw archives.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import struct
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable


def dbf_rows(zf: zipfile.ZipFile, member: str) -> Iterable[dict[str, str]]:
    data = zf.read(member)
    record_count = struct.unpack("<I", data[4:8])[0]
    header_len = struct.unpack("<H", data[8:10])[0]
    record_len = struct.unpack("<H", data[10:12])[0]
    fields: list[tuple[str, int]] = []
    pos = 32
    while data[pos] != 13:
        name = data[pos : pos + 11].decode("ascii", "replace").replace("\x00", "").strip()
        fields.append((name, data[pos + 16]))
        pos += 32
    for i in range(record_count):
        record = data[header_len + i * record_len : header_len + (i + 1) * record_len]
        if not record or record[0:1] == b"*":
            continue
        out: dict[str, str] = {}
        offset = 1
        for name, width in fields:
            out[name] = record[offset : offset + width].decode("latin1").strip()
            offset += width
        yield out


def key(row: dict[str, str]) -> str:
    county = str(row.get("COUNTYFP") or "").strip().zfill(3)
    precinct = str(row.get("precinct") or "").upper().strip()
    precinct = re.sub(r"[^A-Z0-9]+", "", precinct)
    return f"{county}:{precinct}"


def number(raw: Any) -> float:
    try:
        value = float(str(raw or "").replace(",", ""))
    except ValueError:
        return 0.0
    return value if value == value and value > 0 else 0.0


def district_number(raw: Any) -> str:
    match = re.search(r"\d+", str(raw or ""))
    return str(int(match.group(0))) if match else ""


def load_split_crosswalk(zf: zipfile.ZipFile, member: str, district_field: str, prefix: str) -> dict[str, list[tuple[str, float]]]:
    pieces: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for row in dbf_rows(zf, member):
        k = key(row)
        district = district_number(row.get(district_field))
        if not k or not district:
            continue
        race_votes = sum(number(value) for name, value in row.items() if name.startswith(prefix))
        pieces[k].append((district, race_votes))

    out: dict[str, list[tuple[str, float]]] = {}
    for k, rows in pieces.items():
        by_district: dict[str, float] = defaultdict(float)
        for district, weight in rows:
            by_district[district] += weight
        total = sum(by_district.values())
        if total <= 0:
            total = float(len(by_district))
            by_district = {district: 1.0 for district in by_district}
        out[k] = sorted(
            [(district, weight / total) for district, weight in by_district.items()],
            key=lambda item: int(item[0]),
        )
    return out


def write_crosswalk(path: Path, crosswalk: dict[str, list[tuple[str, float]]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["precinct_key", "district_num", "area_weight"])
        writer.writeheader()
        for precinct, assignments in sorted(crosswalk.items()):
            for district, weight in assignments:
                writer.writerow({"precinct_key": precinct, "district_num": district, "area_weight": f"{weight:.10f}"})


CONTESTS = {
    "governor": ("G22GOVDABR", "G22GOVRKEM", ("G22GOVLHAZ",)),
    # Burt Jones (JON) was the Republican nominee; Ryan Graham (GRA) was the
    # Libertarian nominee.  Keeping those columns in their proper buckets is
    # essential because these values seed all three district views.
    "lieutenant_governor": ("G22LTGDBAI", "G22LTGRJON", ("G22LTGLGRA",)),
    "secretary_of_state": ("G22SOSDNGU", "G22SOSRRAF", ("G22SOSLMET",)),
    "attorney_general": ("G22ATGDJOR", "G22ATGRCAR", ("G22ATGLCOW",)),
    "agriculture_commissioner": ("G22AGRDHEM", "G22AGRRHAR", ("G22AGRLRAU",)),
    "insurance_commissioner": ("G22INSDROB", "G22INSRKIN", ()),
    "labor_commissioner": ("G22LABDBOD", "G22LABRTHO", ("G22LABLAND",)),
    "superintendent": ("G22SUPDSEA", "G22SUPRWOO", ()),
    "us_senate": ("G22USSDWAR", "G22USSRWAL", ("G22USSLOLI",)),
}


def aggregate(rows: list[dict[str, str]], crosswalk: dict[str, list[tuple[str, float]]], contest: str) -> tuple[dict[str, dict[str, Any]], int, int]:
    dem_col, rep_col, other_cols = CONTESTS[contest]
    totals: dict[str, dict[str, float]] = defaultdict(lambda: {"dem": 0.0, "rep": 0.0, "other": 0.0})
    input_total = 0
    matched_total = 0
    for row in rows:
        dem = number(row.get(dem_col))
        rep = number(row.get(rep_col))
        other = sum(number(row.get(col)) for col in other_cols)
        total = dem + rep + other
        input_total += round(total)
        assignments = crosswalk.get(key(row))
        if not assignments:
            continue
        matched_total += round(total)
        for district, weight in assignments:
            totals[district]["dem"] += dem * weight
            totals[district]["rep"] += rep * weight
            totals[district]["other"] += other * weight

    result: dict[str, dict[str, Any]] = {}
    for district, values in sorted(totals.items(), key=lambda item: int(item[0])):
        dem = round(values["dem"])
        rep = round(values["rep"])
        other = round(values["other"])
        total = dem + rep + other
        signed = ((rep - dem) / total * 100.0) if total else 0.0
        winner = "Republican" if rep >= dem and rep >= other else ("Democratic" if dem >= other else "Other")
        result[district] = {
            "total_votes": total,
            "dem_votes": dem,
            "rep_votes": rep,
            "other_votes": other,
            "dem_candidate": "Raphael Warnock" if contest == "us_senate" else "Stacey Abrams" if contest == "governor" else "",
            "rep_candidate": "Herschel Walker" if contest == "us_senate" else "Brian Kemp" if contest == "governor" else "",
            "winner": winner,
            "winner_party": "REP" if winner == "Republican" else "DEM" if winner == "Democratic" else "OTH",
            "margin_pct": signed,
        }
    return result, input_total, matched_total


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--archive", type=Path, default=Path("Data/ga_2022_gen_prec.zip"))
    ap.add_argument("--out-dir", type=Path, default=Path("Data/district_contests_2022"))
    ap.add_argument("--crosswalk-dir", type=Path, default=Path("Data/crosswalks_rdh_2022"))
    args = ap.parse_args()

    with zipfile.ZipFile(args.archive) as zf:
        state_member = "ga_2022_gen_st_prec/ga_2022_gen_st_prec.dbf"
        rows = list(dbf_rows(zf, state_member))
        maps = {
            "congressional": load_split_crosswalk(zf, "ga_2022_gen_cong_prec/ga_2022_gen_cong_prec.dbf", "CONG_DIST", "GCON"),
            "state_house": load_split_crosswalk(zf, "ga_2022_gen_sldl_prec/ga_2022_gen_sldl_prec.dbf", "SLDL_DIST", "GSL"),
            "state_senate": load_split_crosswalk(zf, "ga_2022_gen_sldu_prec/ga_2022_gen_sldu_prec.dbf", "SLDU_DIST", "GSU"),
        }

    args.crosswalk_dir.mkdir(parents=True, exist_ok=True)
    for scope, crosswalk in maps.items():
        write_crosswalk(args.crosswalk_dir / f"precinct_to_{scope}.csv", crosswalk)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    manifest: list[dict[str, Any]] = []
    generated = dt.datetime.now(dt.UTC).isoformat()
    for scope, crosswalk in maps.items():
        for contest in CONTESTS:
            result, input_total, matched_total = aggregate(rows, crosswalk, contest)
            if not result:
                continue
            path = args.out_dir / f"{scope}_{contest}_2022.json"
            path.write_text(json.dumps({
                "meta": {
                    "scope": scope, "contest_type": contest, "year": 2022,
                    "district_lines_year": 2022, "source": "rdh_2022_precinct_archive",
                    "generated_at_utc": generated,
                    "match_coverage_pct": (matched_total / input_total * 100.0) if input_total else 0.0,
                    "total_input_votes": input_total, "matched_input_votes": matched_total,
                },
                "general": {"results": result},
            }, indent=2) + "\n", encoding="utf-8")
            manifest.append({
                "scope": scope,
                "contest_type": contest,
                "year": 2022,
                "file": path.name,
                "rows": len(result),
                "districts": len(result),
                "total_votes": sum(row["total_votes"] for row in result.values()),
                "dem_total": sum(row["dem_votes"] for row in result.values()),
                "rep_total": sum(row["rep_votes"] for row in result.values()),
                "other_total": sum(row["other_votes"] for row in result.values()),
                "major_party_contested": True,
                "match_coverage_pct": (matched_total / input_total * 100.0) if input_total else 0.0,
            })
            print(f"Wrote {path} ({len(result)} districts, coverage {(matched_total / input_total * 100.0) if input_total else 0:.2f}%)")
    manifest_path = args.out_dir / "manifest.json"
    existing: dict[str, Any] = {}
    if manifest_path.exists():
        try:
            existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            existing = {}
    old_files = [entry for entry in existing.get("files", []) if not (entry.get("year") == 2022)]
    manifest_path.write_text(json.dumps({
        "source": "Data/ga_2022_gen_prec.zip",
        "district_lines_year": 2022,
        "generated_at_utc": generated,
        "files": old_files + manifest,
    }, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
