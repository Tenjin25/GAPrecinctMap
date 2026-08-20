"""Repair 2024 Georgia district presidential slices from complete spatial inputs.

The 2024 export uses polling-place names, while the VTD20 matcher expects the
older nine VTD names in Lowndes and several other counties.  The State House,
State Senate, and U.S. House contests in the same export record district-specific
vote buckets, so those buckets provide complete allocation weights for the
statewide presidential contest.

For the 2022-lines view, a precomputed spatial crosswalk remaps the complete SOS
2024 district totals onto each old map.  Unchanged districts retain their
complete SOS totals exactly; redrawn districts use party-specific weights
derived from the New York Times' public 2024 presidential precinct geography.
"""

from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "Data" / "20241105__ga__general__precinct-level.csv"
RESULT_FIELDS = ("total_votes", "dem_votes", "rep_votes", "other_votes")
SCOPE_CONFIGS = {
    "state_house": {
        "label": "State House",
        "office": "STATE HOUSE",
        "district_count": 180,
        "file": "state_house_president_2024.json",
        "remap_file": "Data/crosswalks/state_house_2024_to_2022_president_2024_weights.json",
        "abbr": "HD",
    },
    "state_senate": {
        "label": "State Senate",
        "office": "STATE SENATE",
        "district_count": 56,
        "file": "state_senate_president_2024.json",
        "remap_file": "Data/crosswalks/state_senate_2024_to_2022_president_2024_weights.json",
        "abbr": "SD",
    },
    "congressional": {
        "label": "Congressional",
        "office": "U.S. HOUSE",
        "district_count": 14,
        "file": "congressional_president_2024.json",
        "remap_file": "Data/crosswalks/congressional_2024_to_2022_president_2024_weights.json",
        "abbr": "CD",
    },
}


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


def allocate_integer_shares(total: int, weights: dict[str, object]) -> dict[str, int]:
    parsed = {
        str(district): float(weight)
        for district, weight in weights.items()
        if float(weight) > 0
    }
    weight_total = sum(parsed.values())
    if weight_total <= 0:
        raise RuntimeError("Cannot allocate votes with empty remap weights")
    exact = {
        district: total * weight / weight_total
        for district, weight in parsed.items()
    }
    allocated = {
        district: int(math.floor(value))
        for district, value in exact.items()
    }
    remainder = total - sum(allocated.values())
    order = sorted(
        allocated,
        key=lambda district: (exact[district] - allocated[district], -int(district)),
        reverse=True,
    )
    for district in order[:remainder]:
        allocated[district] += 1
    return allocated


def build_2022_lines(
    results_2024: dict[str, dict[str, object]],
    config: dict[str, object],
) -> tuple[dict[str, dict[str, object]], dict[str, object]]:
    remap_path = ROOT / str(config["remap_file"])
    remap = json.loads(remap_path.read_text(encoding="utf-8"))
    meta = remap.get("meta") or {}
    weights = remap.get("weights") or {}
    unchanged = {str(district) for district in meta.get("unchanged_districts", [])}
    changed = {str(district) for district in meta.get("changed_districts", [])}
    district_count = int(config["district_count"])
    label = str(config["label"])
    abbr = str(config["abbr"])
    expected = {str(district) for district in range(1, district_count + 1)}
    if unchanged | changed != expected or unchanged & changed:
        raise RuntimeError(
            f"Remap district classification does not partition all {district_count} {label} districts"
        )

    final: dict[str, dict[str, object]] = {
        district: dict(results_2024[district])
        for district in unchanged
    }
    allocated: dict[str, dict[str, float]] = defaultdict(
        lambda: {field: 0.0 for field in RESULT_FIELDS}
    )
    for current_district in sorted(changed, key=int):
        source = results_2024.get(current_district)
        district_weights = weights.get(current_district)
        if source is None or not isinstance(district_weights, dict):
            raise RuntimeError(f"Missing remap inputs for 2024-lines {abbr} {current_district}")
        for field in RESULT_FIELDS:
            field_weights = district_weights.get(field)
            if not isinstance(field_weights, dict):
                raise RuntimeError(f"Missing {field} remap weights for {abbr} {current_district}")
            field_allocation = allocate_integer_shares(int(source[field]), field_weights)
            for old_district, votes in field_allocation.items():
                if old_district not in changed:
                    raise RuntimeError(
                        f"Changed {abbr} {current_district} unexpectedly maps into unchanged "
                        f"{abbr} {old_district}"
                    )
                allocated[old_district][field] += votes

    final.update(finalize_results(allocated))
    final = dict(sorted(final.items(), key=lambda item: int(item[0])))
    if set(final) != expected:
        missing = sorted(expected - set(final), key=int)
        raise RuntimeError(f"Missing remapped 2022-lines {label} districts: {missing}")

    for field in RESULT_FIELDS:
        source_total = sum(int(row[field]) for row in results_2024.values())
        output_total = sum(int(row[field]) for row in final.values())
        if source_total != output_total:
            raise RuntimeError(
                f"2022-lines {field} does not conserve the 2024-lines total: "
                f"{output_total} != {source_total}"
            )
    return final, meta


def update_manifest(
    lines_year: int,
    file_name: str,
    results: dict[str, dict[str, object]],
    coverage_pct: float,
) -> None:
    manifest_path = ROOT / "Data" / f"district_contests_{lines_year}" / "manifest.json"
    original_text = manifest_path.read_text(encoding="utf-8")
    manifest = json.loads(original_text)
    target = next(
        (
            entry
            for entry in manifest.get("files", [])
            if entry.get("file") == file_name
        ),
        None,
    )
    if target is None:
        raise RuntimeError(f"Missing {file_name} entry in {manifest_path}")

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


def build_current_lines(
    statewide: dict[tuple[str, str], dict[str, int]],
    district_buckets: dict[tuple[str, str], dict[str, int]],
    district_count: int,
) -> tuple[dict[str, dict[str, object]], int]:
    results: dict[str, dict[str, float]] = defaultdict(
        lambda: {field: 0.0 for field in RESULT_FIELDS}
    )
    input_total = 0
    for bucket, parties in statewide.items():
        district_weights = district_buckets.get(bucket)
        if not district_weights:
            continue
        weight_total = sum(district_weights.values())
        if weight_total <= 0:
            continue
        dem = parties.get("DEMOCRAT", 0)
        rep = parties.get("REPUBLICAN", 0)
        other = sum(
            votes
            for party, votes in parties.items()
            if party not in {"DEMOCRAT", "REPUBLICAN"}
        )
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
    expected = {str(district) for district in range(1, district_count + 1)}
    if set(final) != expected:
        missing = sorted(expected - set(final), key=int)
        raise RuntimeError(f"Current-lines bucket allocation is missing districts: {missing}")
    return final, input_total


def write_scope_results(
    scope: str,
    config: dict[str, object],
    statewide: dict[tuple[str, str], dict[str, int]],
    district_buckets: dict[tuple[str, str], dict[str, int]],
    statewide_total: int,
) -> None:
    label = str(config["label"])
    file_name = str(config["file"])
    final_2024, input_total = build_current_lines(
        statewide,
        district_buckets,
        int(config["district_count"]),
    )
    if input_total != statewide_total:
        raise RuntimeError(
            f"{label} bucket allocation covers {input_total:,} of {statewide_total:,} presidential votes"
        )

    output_2024 = ROOT / "Data" / "district_contests_2024" / file_name
    payload_2024 = {
        "meta": {
            "scope": scope,
            "contest_type": "president",
            "year": 2024,
            "district_lines_year": 2024,
            "source": f"2024 SOS precinct export allocated by {label} precinct buckets",
            "generated_by": "scripts/repair_2024_state_house_president.py",
            "match_coverage_pct": 100.0,
            "total_input_votes": input_total,
            "matched_input_votes": input_total,
            "input_files": ["Data/20241105__ga__general__precinct-level.csv"],
        },
        "general": {"results": final_2024},
    }
    output_2024.write_text(json.dumps(payload_2024, indent=2) + "\n", encoding="utf-8")
    update_manifest(2024, file_name, final_2024, 100.0)

    final_2022, remap_meta = build_2022_lines(final_2024, config)
    output_2022 = ROOT / "Data" / "district_contests_2022" / file_name
    payload_2022 = {
        "meta": {
            "scope": scope,
            "contest_type": "president",
            "year": 2024,
            "district_lines_year": 2022,
            "source": (
                f"Complete 2024 SOS {label} bucket allocation remapped to the 2022 lines; "
                "unchanged districts retain exact SOS totals and redrawn districts use constrained "
                "NYT precinct-geography weights"
            ),
            "generated_by": "scripts/repair_2024_state_house_president.py",
            "remap_method": remap_meta.get("method"),
            "unchanged_geometry_districts": remap_meta.get("unchanged_districts", []),
            "spatially_remapped_districts": remap_meta.get("changed_districts", []),
            "match_coverage_pct": 100.0,
            "total_input_votes": input_total,
            "matched_input_votes": input_total,
            "allocated_output_votes": sum(
                int(row["total_votes"])
                for row in final_2022.values()
            ),
            "input_files": [
                "Data/20241105__ga__general__precinct-level.csv",
                str(config["remap_file"]),
                remap_meta.get("district_map_2022"),
                remap_meta.get("district_map_2024"),
            ],
            "source_urls": [remap_meta.get("source_url")],
            "spatial_source_sha256": remap_meta.get("source_sha256"),
        },
        "general": {"results": final_2022},
    }
    output_2022.write_text(json.dumps(payload_2022, indent=2) + "\n", encoding="utf-8")
    update_manifest(2022, file_name, final_2022, 100.0)
    print(f"Wrote {output_2024}")
    print(f"Wrote {output_2022}")


def main() -> None:
    statewide: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))
    district_buckets: dict[str, dict[tuple[str, str], dict[str, int]]] = {
        scope: defaultdict(lambda: defaultdict(int))
        for scope in SCOPE_CONFIGS
    }
    office_to_scope = {
        str(config["office"]): scope
        for scope, config in SCOPE_CONFIGS.items()
    }

    with SOURCE.open(encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            office = row["office"].strip().upper()
            bucket = key(row)
            if office == "PRESIDENT":
                party = row["party"].strip().upper()
                statewide[bucket][party] += num(row["total_votes"])
                continue
            scope = office_to_scope.get(office)
            district = row["district"].strip()
            if scope and district:
                district_buckets[scope][bucket][district] += num(row["total_votes"])

    statewide_total = sum(sum(parties.values()) for parties in statewide.values())
    for scope, config in SCOPE_CONFIGS.items():
        write_scope_results(
            scope,
            config,
            statewide,
            district_buckets[scope],
            statewide_total,
        )


if __name__ == "__main__":
    main()
