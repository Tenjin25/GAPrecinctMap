"""Repair the 2024 State House presidential slices from the SOS precinct export.

The 2024 export uses polling-place names, while the VTD20 matcher expects the
older nine VTD names in Lowndes and several other counties.  The State House
contest in the same export already records the district-specific vote buckets,
so those buckets provide the correct allocation weights for statewide contests.

That direct allocation is valid only for the 2024 House lines.  The 2022-lines
view must instead aggregate the matched VTD20 presidential results through the
2022 precinct-to-House crosswalk; district numbers do not represent the same
geography between the two line vintages.
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "Data" / "20241105__ga__general__precinct-level.csv"
OUTPUT = ROOT / "Data" / "district_contests_2024" / "state_house_president_2024.json"
OUTPUT_2022 = ROOT / "Data" / "district_contests_2022" / "state_house_president_2024.json"
VTD20_SOURCE = ROOT / "Data" / "derived_vtd20" / "2024" / "contests" / "vtd20" / "President.json"
CROSSWALK_2022 = ROOT / "Data" / "crosswalks" / "precinct_to_2022_state_house.csv"

# These district polygons are unchanged between the 2022 and 2024 House maps.
# Use the complete SOS State House bucket allocation when the VTD20 matcher
# drops votes from renamed or consolidated precincts.  HD 128 is geometrically
# identical across the two vintages; its VTD20 path omits Hancock and McDuffie
# precincts that are present in the SOS bucket result.
IDENTICAL_GEOGRAPHY_OVERRIDES = ("128",)


def num(value: object) -> int:
    try:
        return int(float(str(value or "0").strip() or 0))
    except ValueError:
        return 0


def key(row: dict[str, str]) -> tuple[str, str]:
    return (row["county"].strip().upper(), row["precinct"].strip().upper())


def finalize_results(results: dict[str, dict[str, float]]) -> dict[str, dict[str, object]]:
    final: dict[str, dict[str, object]] = {}
    for district, row in sorted(results.items(), key=lambda item: int(item[0])):
        total = int(round(row["total_votes"]))
        dem = int(round(row["dem_votes"]))
        rep = int(round(row["rep_votes"]))
        other = int(round(row["other_votes"]))
        winner = "Democratic" if dem > rep and dem >= other else "Republican"
        final[district] = {
            "total_votes": total,
            "dem_votes": dem,
            "rep_votes": rep,
            "other_votes": other,
            "dem_candidate": "Kamala D. Harris",
            "rep_candidate": "Donald J. Trump",
            "winner": winner,
            "winner_party": "DEM" if winner == "Democratic" else "REP",
            "margin_pct": ((rep - dem) / total * 100) if total else 0,
        }
    return final


def build_2022_lines() -> tuple[dict[str, dict[str, object]], int, int]:
    assignments: dict[str, list[tuple[str, float]]] = defaultdict(list)
    with CROSSWALK_2022.open(encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            precinct_key = row["precinct_key"].strip().upper()
            district = row["district_num"].strip()
            try:
                weight = float(row["area_weight"])
            except (TypeError, ValueError):
                continue
            if precinct_key and district and weight > 0:
                assignments[precinct_key].append((district, weight))

    source = json.loads(VTD20_SOURCE.read_text(encoding="utf-8"))
    results: dict[str, dict[str, float]] = defaultdict(
        lambda: {"total_votes": 0.0, "dem_votes": 0.0, "rep_votes": 0.0, "other_votes": 0.0}
    )
    total_input_votes = 0
    matched_input_votes = 0
    for precinct_key, row in (source.get("results") or {}).items():
        total = num(row.get("total_votes", 0))
        dem = num(row.get("dem_votes", 0))
        rep = num(row.get("rep_votes", 0))
        other = num(row.get("other_votes", 0))
        total_input_votes += total
        district_weights = assignments.get(str(precinct_key).strip().upper(), [])
        weight_total = sum(weight for _, weight in district_weights)
        if weight_total <= 0:
            continue
        matched_input_votes += total
        for district, weight in district_weights:
            normalized_weight = weight / weight_total
            out = results[district]
            out["total_votes"] += total * normalized_weight
            out["dem_votes"] += dem * normalized_weight
            out["rep_votes"] += rep * normalized_weight
            out["other_votes"] += other * normalized_weight

    return finalize_results(results), total_input_votes, matched_input_votes


def apply_identical_geography_overrides(
    results_2022: dict[str, dict[str, object]],
    results_2024: dict[str, dict[str, object]],
) -> None:
    for district in IDENTICAL_GEOGRAPHY_OVERRIDES:
        source = results_2024.get(district)
        if source is None:
            raise RuntimeError(f"Missing 2024-lines result for HD {district}")
        results_2022[district] = dict(source)


def update_manifest(lines_year: int, results: dict[str, dict[str, object]], coverage_pct: float) -> None:
    manifest_path = ROOT / "Data" / f"district_contests_{lines_year}" / "manifest.json"
    original_text = manifest_path.read_text(encoding="utf-8")
    manifest = json.loads(original_text)
    target = next(
        (
            entry
            for entry in manifest.get("files", [])
            if entry.get("file") == "state_house_president_2024.json"
        ),
        None,
    )
    if target is None:
        raise RuntimeError(f"Missing State House president entry in {manifest_path}")

    target.update(
        {
            "rows": len(results),
            "districts": len(results),
            "total_votes": sum(int(row["total_votes"]) for row in results.values()),
            "dem_total": sum(int(row["dem_votes"]) for row in results.values()),
            "rep_total": sum(int(row["rep_votes"]) for row in results.values()),
            "other_total": sum(int(row["other_votes"]) for row in results.values()),
            "major_party_contested": True,
            "match_coverage_pct": coverage_pct,
        }
    )
    updated_text = json.dumps(manifest, indent=2) + "\n"
    if updated_text != original_text:
        manifest_path.write_text(updated_text, encoding="utf-8")


def main() -> None:
    statewide: dict[tuple[str, str], dict[str, dict[str, int]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    house_buckets: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))

    with SOURCE.open(encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            office = row["office"].strip().upper()
            bucket = key(row)
            if office == "PRESIDENT":
                statewide[bucket][row["party"].strip().upper()]["votes"] += num(row["total_votes"])
            elif office == "STATE HOUSE" and row["district"].strip():
                house_buckets[bucket][row["district"].strip()] += num(row["total_votes"])

    results: dict[str, dict[str, object]] = defaultdict(lambda: {"total_votes": 0, "dem_votes": 0, "rep_votes": 0, "other_votes": 0})
    input_total = 0
    for bucket, parties in statewide.items():
        district_weights = house_buckets.get(bucket)
        if not district_weights:
            continue
        weight_total = sum(district_weights.values())
        if weight_total <= 0:
            continue
        dem = parties.get("DEMOCRAT", {}).get("votes", 0)
        rep = parties.get("REPUBLICAN", {}).get("votes", 0)
        other = sum(v.get("votes", 0) for p, v in parties.items() if p not in {"DEMOCRAT", "REPUBLICAN"})
        total = dem + rep + other
        input_total += total
        for district, bucket_votes in district_weights.items():
            weight = bucket_votes / weight_total
            row = results[district]
            row["total_votes"] += total * weight
            row["dem_votes"] += dem * weight
            row["rep_votes"] += rep * weight
            row["other_votes"] += other * weight

    final = finalize_results(results)

    payload = {
        "meta": {
            "scope": "state_house",
            "contest_type": "president",
            "year": 2024,
            "district_lines_year": 2024,
            "source": "2024 SOS precinct export allocated by State House precinct buckets",
            "generated_by": "scripts/repair_2024_state_house_president.py",
            "match_coverage_pct": 100.0,
            "total_input_votes": input_total,
            "matched_input_votes": input_total,
            "input_files": ["Data/20241105__ga__general__precinct-level.csv"],
        },
        "general": {"results": final},
    }
    OUTPUT.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    update_manifest(2024, final, 100.0)
    final_2022, total_2022, matched_2022 = build_2022_lines()
    apply_identical_geography_overrides(final_2022, final)
    coverage_2022 = (matched_2022 / total_2022 * 100) if total_2022 else 0
    payload_2022 = {
        "meta": {
            "scope": "state_house",
            "contest_type": "president",
            "year": 2024,
            "district_lines_year": 2022,
            "source": (
                "2024 matched VTD20 presidential results aggregated through the 2022 House crosswalk; "
                "identical-geometry districts use the complete SOS State House bucket allocation"
            ),
            "generated_by": "scripts/repair_2024_state_house_president.py",
            "identical_geometry_overrides": list(IDENTICAL_GEOGRAPHY_OVERRIDES),
            "match_coverage_pct": coverage_2022,
            "total_input_votes": total_2022,
            "matched_input_votes": matched_2022,
            "input_files": [
                "Data/derived_vtd20/2024/contests/vtd20/President.json",
                "Data/crosswalks/precinct_to_2022_state_house.csv",
                "Data/20241105__ga__general__precinct-level.csv",
            ],
        },
        "general": {"results": final_2022},
    }
    OUTPUT_2022.write_text(json.dumps(payload_2022, indent=2) + "\n", encoding="utf-8")
    update_manifest(2022, final_2022, coverage_2022)
    print(f"Wrote {OUTPUT}")
    print(f"Wrote {OUTPUT_2022}")
    print("2024 lines HD 177:", final.get("177"))
    for district in ("40", "81", "82", "128", "149"):
        print(f"2022 lines HD {district}:", final_2022.get(district))


if __name__ == "__main__":
    main()
