"""Allocate statewide appellate-court VTD20 results into district layers."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import defaultdict
from pathlib import Path


SCOPES = {
    "congressional": {2022: "precinct_to_cd118.csv", 2024: "precinct_to_cd119.csv"},
    "state_house": {2022: "precinct_to_2022_state_house.csv", 2024: "precinct_to_2024_state_house.csv"},
    "state_senate": {2022: "precinct_to_2022_state_senate.csv", 2024: "precinct_to_2024_state_senate.csv"},
}

# These are alignment labels for visualization, not ballot party labels.
ALIGNED_CANDIDATES = {
    (2024, "Supreme Court Justice", "Pinson"): ("Barrow", "Pinson"),
    (2026, "Supreme Court Justice", "Bethel"): ("Rankin", "Bethel"),
    (2026, "Supreme Court Justice", "Warren"): ("Jordan", "Warren"),
    (2026, "Appeals Court Judge", "Brown, III"): ("Wooten", "Brown"),
    (2026, "Appeals Court Judge", "Gobeil"): ("Felton", "Gobeil"),
}

SOURCE_FILES = {
    2012: Path("Data/20120731__ga__general__primary__judicial__precinct.csv"),
    2014: Path("Data/20140520__ga__general__primary__judicial__precinct.csv"),
    2016: Path("Data/20160524__ga__general__primary__judicial__precinct.csv"),
    2018: Path("Data/20180522__ga__general__primary__judicial__precinct.csv"),
    2020: Path("Data/20200609__ga__general__primary__judicial__precinct.csv"),
    2022: Path("Data/20220524__ga__general__primary__judicial__precinct.csv"),
    2024: Path("Data/20240521__ga__general__primary__judicial__precinct.csv"),
    2026: Path("Data/20260519__ga__general__primary__precinct.csv"),
}


def clean_candidate(name: str) -> str:
    value = re.sub(r"\s+", " ", (name or "").replace('""', '"')).strip()
    value = re.sub(r"\s*\((?:I|In)\)?\s*$", "", value, flags=re.I).strip()
    value = re.sub(r"\s*\*+\s*$", "", value).strip()
    # Prefer a ballot nickname over the formal given name: Charles "Charlie"
    # Bethel becomes Charlie Bethel, avoiding two first names in the UI.
    value = re.sub(r'^\S+(?:\s+\S+)?\s+"([^"]+)"\s+(.+)$', r"\1 \2", value).strip()
    return value


def candidate_key(name: str) -> str:
    value = clean_candidate(name)
    if "write" in value.casefold():
        return "WRITE-IN"
    words = re.findall(r"[A-Za-z]+", value.upper())
    return words[-1] if words else value.upper()


def slug(value: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", value.lower())).strip("_")


def largest_remainder(values: dict[str, float]) -> dict[str, int]:
    floors = {k: int(math.floor(v)) for k, v in values.items()}
    target = int(round(sum(values.values())))
    order = sorted(values, key=lambda k: (values[k] - floors[k], k), reverse=True)
    for key in order[: max(0, target - sum(floors.values()))]:
        floors[key] += 1
    return floors


def read_crosswalk(path: Path) -> dict[str, list[tuple[str, float]]]:
    out: dict[str, list[tuple[str, float]]] = defaultdict(list)
    with path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            out[str(row["precinct_key"]).strip()].append(
                (str(row["district_num"]).strip(), float(row["area_weight"]))
            )
    return out


def read_county_fips(path: Path) -> dict[str, str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        str(name).strip().upper(): str(row.get("county_fips") or "").strip()
        for name, row in (payload.get("counties") or {}).items()
    }


def read_source_county_totals(path: Path, office: str, seat: str, county_fips: dict[str, str]) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    with path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if str(row.get("office") or "") != office or str(row.get("district") or "") != seat:
                continue
            fips = county_fips.get(str(row.get("county") or "").strip().upper(), "")
            if fips:
                out[fips][candidate_key(str(row.get("candidate") or ""))] += int(row.get("votes") or 0)
    return out


def reconcile_vtd_to_counties(
    vtd_results: dict[str, dict[str, object]],
    county_totals: dict[str, dict[str, int]],
    all_vtd_geoids: set[str],
) -> dict[str, dict[str, float]]:
    """Scale mapped VTD candidate totals to exact source county totals."""
    vtds_by_county: dict[str, list[str]] = defaultdict(list)
    mapped: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    base: dict[str, dict[str, float]] = {}
    for geoid in all_vtd_geoids:
        fips = str(geoid)[:5]
        vtds_by_county[fips].append(str(geoid))
        base[str(geoid)] = defaultdict(float)
    for geoid, row in vtd_results.items():
        fips = str(geoid)[:5]
        if str(geoid) not in base:
            vtds_by_county[fips].append(str(geoid))
            base[str(geoid)] = defaultdict(float)
        for name, votes in (row.get("candidate_votes") or {}).items():
            key = candidate_key(str(name))
            n = float(votes or 0)
            base[str(geoid)][key] += n
            mapped[fips][key] += n

    for fips, totals in county_totals.items():
        geoids = vtds_by_county.get(fips, [])
        if not geoids:
            continue
        turnout = {g: sum(base[g].values()) for g in geoids}
        turnout_sum = sum(turnout.values())
        for key, target in totals.items():
            current = mapped[fips].get(key, 0.0)
            if current > 0:
                factor = target / current
                for geoid in geoids:
                    base[geoid][key] *= factor
            else:
                for geoid in geoids:
                    weight = turnout[geoid] / turnout_sum if turnout_sum > 0 else 1 / len(geoids)
                    base[geoid][key] = target * weight
    return base


def choose_sides(year: int, office: str, seat: str, totals: dict[str, int]) -> tuple[str, str, str]:
    real = [k for k, v in totals.items() if v > 0 and k != "WRITE-IN"]
    alignment = ALIGNED_CANDIDATES.get((year, office, seat))
    if alignment:
        left = next((k for k in real if alignment[0].upper() in k), "")
        right = next((k for k in real if alignment[1].upper() in k), "")
        return left, right, "documented_partisan_backing"
    ordered = sorted(real)
    return (ordered[0] if ordered else "", ordered[1] if len(ordered) > 1 else "", "unclassified_nonpartisan")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--derived-base", type=Path, default=Path("Data/judicial_derived_vtd20"))
    ap.add_argument("--crosswalk-dir", type=Path, default=Path("Data/crosswalks"))
    ap.add_argument("--lines-year", type=int, choices=(2022, 2024), required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--county-demographics", type=Path, default=Path("Data/ga_county_demographics.json"))
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    # This directory is owned by this builder. Remove stale contest files so
    # races filtered out on a later run cannot remain discoverable.
    for stale in args.out_dir.glob("*.json"):
        stale.unlink()
    crosswalks = {
        scope: read_crosswalk(args.crosswalk_dir / names[args.lines_year])
        for scope, names in SCOPES.items()
    }
    all_vtd_geoids = {geoid for crosswalk in crosswalks.values() for geoid in crosswalk}
    county_fips = read_county_fips(args.county_demographics)
    manifest_rows = []

    for year_dir in sorted(p for p in args.derived_base.iterdir() if p.is_dir() and p.name.isdigit()):
        year = int(year_dir.name)
        manifest_path = year_dir / "contests" / "manifest.json"
        if not manifest_path.exists():
            continue
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for contest in manifest.get("contests", []):
            office = str(contest.get("office") or "")
            if office not in {"Supreme Court Justice", "Appeals Court Judge"}:
                continue
            seat = str(contest.get("district") or "")
            rel = contest.get("outputs", {}).get("vtd20", {}).get("path")
            if not rel:
                continue
            payload = json.loads(Path(rel).read_text(encoding="utf-8"))
            vtd_results = payload.get("results", {})
            source_county_totals = read_source_county_totals(
                SOURCE_FILES[year], office, seat, county_fips
            )
            reconciled_vtd = reconcile_vtd_to_counties(vtd_results, source_county_totals, all_vtd_geoids)

            display_votes: dict[str, defaultdict[str, int]] = defaultdict(lambda: defaultdict(int))
            canonical_totals: dict[str, int] = defaultdict(int)
            total_input = 0
            for fips_totals in source_county_totals.values():
                for key, n in fips_totals.items():
                    canonical_totals[key] += n
                    total_input += n
            with SOURCE_FILES[year].open(encoding="utf-8-sig", newline="") as handle:
                for source_row in csv.DictReader(handle):
                    if str(source_row.get("office") or "") == office and str(source_row.get("district") or "") == seat:
                        key = candidate_key(str(source_row.get("candidate") or ""))
                        display_votes[key][clean_candidate(str(source_row.get("candidate") or ""))] += int(source_row.get("votes") or 0)
            display = {
                key: max(names.items(), key=lambda item: (item[1], item[0]))[0]
                for key, names in display_votes.items()
            }
            named_candidates = [
                key for key, votes in canonical_totals.items()
                if votes > 0 and key != "WRITE-IN"
            ]
            named_vote_total = sum(canonical_totals[key] for key in named_candidates)
            material_candidates = [
                key for key in named_candidates
                if named_vote_total and canonical_totals[key] / named_vote_total >= 0.01
            ]
            if len(material_candidates) < 2:
                continue
            side_a, side_b, alignment = choose_sides(year, office, seat, canonical_totals)

            for scope, crosswalk in crosswalks.items():
                district_float: dict[str, defaultdict[str, float]] = defaultdict(lambda: defaultdict(float))
                matched_input = 0
                for geoid, cand_votes in reconciled_vtd.items():
                    pieces = crosswalk.get(str(geoid), [])
                    if not pieces:
                        continue
                    matched_input += sum(float(v or 0) for v in cand_votes.values())
                    for district, weight in pieces:
                        for key, votes in cand_votes.items():
                            district_float[district][key] += float(votes or 0) * weight

                candidate_district_int: dict[str, dict[str, int]] = defaultdict(dict)
                for key in canonical_totals:
                    alloc = largest_remainder({d: vals.get(key, 0.0) for d, vals in district_float.items()})
                    for district, votes in alloc.items():
                        candidate_district_int[district][key] = votes

                results = {}
                for district in sorted(candidate_district_int, key=lambda x: int(x) if x.isdigit() else x):
                    votes = candidate_district_int[district]
                    a = int(votes.get(side_a, 0))
                    b = int(votes.get(side_b, 0))
                    other = sum(v for k, v in votes.items() if k not in {side_a, side_b})
                    total = a + b + other
                    signed = ((a - b) / total * 100) if total else 0.0
                    results[district] = {
                        "total_votes": total,
                        "dem_votes": a,
                        "rep_votes": b,
                        "other_votes": other,
                        "dem_candidate": display.get(side_a, ""),
                        "rep_candidate": display.get(side_b, ""),
                        "candidate_a": display.get(side_a, ""),
                        "candidate_a_votes": a,
                        "candidate_b": display.get(side_b, ""),
                        "candidate_b_votes": b,
                        "nonpartisan": True,
                        "alignment": alignment,
                        "winner": "Candidate A" if a > b else ("Candidate B" if b > a else "Tie"),
                        "winner_party": "DEM" if a > b else ("REP" if b > a else "TIE"),
                        "margin_pct": -signed,
                    }

                court_slug = "ga_supreme_court" if office == "Supreme Court Justice" else "ga_court_of_appeals"
                contest_type = f"{court_slug}_{slug(seat)}_seat"
                file_name = f"{scope}_{contest_type}_{year}.json"
                out_payload = {
                    "meta": {
                        "scope": scope,
                        "contest_type": contest_type,
                        "year": year,
                        "district_lines_year": args.lines_year,
                        "source": "Georgia SOS precinct detail allocated through VTD20 district crosswalk",
                        "allocation_weight": "area_weight",
                        "match_coverage_pct": round(100 * matched_input / total_input, 4) if total_input else 0,
                        "total_input_votes": total_input,
                        "matched_input_votes": round(matched_input),
                        "county_reconciled": True,
                        "nonpartisan": True,
                        "alignment": alignment,
                    },
                    "general": {"results": results},
                }
                (args.out_dir / file_name).write_text(json.dumps(out_payload, indent=2), encoding="utf-8")
                manifest_rows.append({
                    "scope": scope,
                    "contest_type": contest_type,
                    "year": year,
                    "file": file_name,
                    "rows": len(results),
                    "districts": len(results),
                    "total_votes": sum(r["total_votes"] for r in results.values()),
                    "match_coverage_pct": out_payload["meta"]["match_coverage_pct"],
                    "nonpartisan": True,
                })

    (args.out_dir / "manifest.json").write_text(json.dumps({
        "source": str(args.derived_base),
        "district_lines_year": args.lines_year,
        "files": manifest_rows,
    }, indent=2), encoding="utf-8")
    print(f"Wrote {len(manifest_rows)} district contest files to {args.out_dir}")


if __name__ == "__main__":
    main()
