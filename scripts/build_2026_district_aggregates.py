"""Build 2026 statewide-contest aggregates on the current Georgia districts.

The 2026 SOS export reports statewide and district contests on the same
county/precinct keys.  U.S. House, State House, and State Senate vote totals
therefore provide same-election allocation weights for precincts that contain
more than one district.  Each output is reconciled to the canonical statewide
party totals and written into the app's 2024-lines archive, which is also the
configured 2026 district-map vintage.

The same statewide contests are also remapped to the repository's 2022-lines
congressional and legislative archives. The directly reported U.S. House
elections are included only in the current-lines congressional archive.
"""

from __future__ import annotations

import csv
import json
import math
import re
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "Data/20261103__ga__general__precinct.csv"
OUT = ROOT / "Data/district_contests_2024"
YEAR = 2026
FIELDS = ("dem_votes", "rep_votes", "other_votes")

SCOPES = {
    "congressional": ("U.S. House", 14),
    "state_house": ("State House", 180),
    "state_senate": ("State Senate", 56),
}

REMAPS_2022 = {
    "congressional": ROOT / "Data/crosswalks/congressional_2024_to_2022_president_2024_weights.json",
    "state_house": ROOT / "Data/crosswalks/state_house_2024_to_2022_president_2024_weights.json",
    "state_senate": ROOT / "Data/crosswalks/state_senate_2024_to_2022_president_2024_weights.json",
}

CONTESTS = {
    "U.S. Senate": "us_senate",
    "Governor": "governor",
    "Lieutenant Governor": "lieutenant_governor",
    "Secretary of State": "secretary_of_state",
    "Attorney General": "attorney_general",
    "Commissioner of Agriculture": "agriculture_commissioner",
    "Commissioner of Insurance": "insurance_commissioner",
    "State School Superintendent": "superintendent",
    "Commissioner of Labor": "labor_commissioner",
}


def norm(value: object) -> str:
    return re.sub(r"[^A-Z0-9]+", "", str(value or "").upper())


def precinct_key(row: dict[str, str]) -> tuple[str, str]:
    return norm(row.get("county")), norm(row.get("precinct"))


def votes(row: dict[str, str]) -> int:
    return int(float(str(row.get("votes") or row.get("total_votes") or 0).replace(",", "")))


def party_field(party: str) -> str:
    value = norm(party)
    if value.startswith("DEM"):
        return "dem_votes"
    if value.startswith("REP"):
        return "rep_votes"
    return "other_votes"


def district_number(raw: str) -> str:
    return str(int(float(raw)))


def allocate(total: int, weights: dict[str, float]) -> dict[str, int]:
    positive = {key: max(0, value) for key, value in weights.items() if value > 0}
    denominator = sum(positive.values())
    if denominator <= 0:
        return {}
    exact = {key: total * value / denominator for key, value in positive.items()}
    result = {key: math.floor(value) for key, value in exact.items()}
    remainder = total - sum(result.values())
    order = sorted(
        result,
        key=lambda key: (exact[key] - result[key], -int(key)),
        reverse=True,
    )
    for key in order[:remainder]:
        result[key] += 1
    return result


def load_rows() -> list[dict[str, str]]:
    with SOURCE.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def canonical(contest_type: str) -> tuple[dict[str, int], str, str]:
    path = ROOT / f"Data/contests/{contest_type}_{YEAR}.json"
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    totals = {field: 0 for field in FIELDS}
    names: dict[str, defaultdict[str, int]] = {
        "dem_votes": defaultdict(int),
        "rep_votes": defaultdict(int),
    }
    for row in payload.get("rows", []):
        for field in FIELDS:
            totals[field] += int(row.get(field) or 0)
        if row.get("dem_candidate"):
            names["dem_votes"][str(row["dem_candidate"])] += int(row.get("dem_votes") or 0)
        if row.get("rep_candidate"):
            names["rep_votes"][str(row["rep_candidate"])] += int(row.get("rep_votes") or 0)
    dem = max(names["dem_votes"], key=names["dem_votes"].get, default="Democratic")
    rep = max(names["rep_votes"], key=names["rep_votes"].get, default="Republican")
    return totals, dem, rep


def build_buckets(
    rows: list[dict[str, str]],
) -> dict[str, dict[tuple[str, str], dict[str, int]]]:
    result = {scope: defaultdict(lambda: defaultdict(int)) for scope in SCOPES}
    office_to_scope = {office: scope for scope, (office, _) in SCOPES.items()}
    for row in rows:
        scope = office_to_scope.get(str(row.get("office") or "").strip())
        if not scope or not str(row.get("district") or "").strip():
            continue
        result[scope][precinct_key(row)][district_number(row["district"])] += votes(row)
    return result


def reconcile(results: dict[str, dict[str, int]], targets: dict[str, int]) -> None:
    for field, target in targets.items():
        assigned = allocate(target, {district: row[field] for district, row in results.items()})
        if sum(assigned.values()) != target:
            raise RuntimeError(f"Could not reconcile {field}: {sum(assigned.values())} != {target}")
        for district, value in assigned.items():
            results[district][field] = value


def write_result(
    scope: str,
    contest_type: str,
    source_rows: list[dict[str, str]],
    buckets: dict[tuple[str, str], dict[str, int]],
    office: str,
) -> tuple[dict[str, object], dict[str, dict[str, object]]]:
    statewide: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in source_rows:
        if str(row.get("office") or "").strip() == office:
            statewide[precinct_key(row)][party_field(row.get("party", ""))] += votes(row)

    results: dict[str, dict[str, int]] = defaultdict(lambda: {field: 0 for field in FIELDS})
    total_votes = matched_votes = 0
    for key, parties in statewide.items():
        total = sum(parties.values())
        total_votes += total
        weights = buckets.get(key)
        if not weights:
            continue
        matched_votes += total
        for field in FIELDS:
            for district, value in allocate(parties.get(field, 0), weights).items():
                results[district][field] += value

    expected = {str(value) for value in range(1, SCOPES[scope][1] + 1)}
    if set(results) != expected:
        missing = sorted(expected - set(results), key=int)
        raise RuntimeError(f"{scope} {contest_type} is missing districts: {missing}")

    targets, dem_name, rep_name = canonical(contest_type)
    reconcile(results, targets)
    final: dict[str, dict[str, object]] = {}
    for district, row in sorted(results.items(), key=lambda item: int(item[0])):
        total = sum(row.values())
        dem = row["dem_votes"]
        rep = row["rep_votes"]
        winner = "Democratic" if dem > rep else "Republican"
        final[district] = {
            "total_votes": total,
            **row,
            "dem_candidate": dem_name,
            "rep_candidate": rep_name,
            "winner": winner,
            "winner_party": "DEM" if winner == "Democratic" else "REP",
            "margin_pct": ((rep - dem) / total * 100) if total else 0,
        }

    coverage = matched_votes / total_votes * 100 if total_votes else 0
    file_name = f"{scope}_{contest_type}_{YEAR}.json"
    path = OUT / file_name
    payload = {
        "meta": {
            "scope": scope,
            "contest_type": contest_type,
            "year": YEAR,
            "district_lines_year": 2024,
            "source": "2026 SOS precinct export allocated by direct same-election district ballot buckets",
            "status": "preliminary",
            "generated_by": "scripts/build_2026_district_aggregates.py",
            "match_coverage_pct": coverage,
            "total_input_votes": total_votes,
            "matched_input_votes": matched_votes,
            "input_files": [
                "Data/20261103__ga__general__precinct.csv",
                f"Data/contests/{contest_type}_{YEAR}.json",
            ],
        },
        "general": {"results": final},
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {file_name}: {coverage:.6f}% direct-bucket coverage")
    entry = {
        "scope": scope,
        "contest_type": contest_type,
        "year": YEAR,
        "file": file_name,
        "rows": len(final),
        "districts": len(final),
        "total_votes": sum(int(row["total_votes"]) for row in final.values()),
        "dem_total": sum(int(row["dem_votes"]) for row in final.values()),
        "rep_total": sum(int(row["rep_votes"]) for row in final.values()),
        "other_total": sum(int(row["other_votes"]) for row in final.values()),
        "major_party_contested": True,
        "match_coverage_pct": coverage,
        "county_reconciled": True,
        "status": "preliminary",
    }
    return entry, final


def write_us_house_result(source_rows: list[dict[str, str]]) -> dict[str, object]:
    results: dict[str, dict[str, int]] = defaultdict(lambda: {field: 0 for field in FIELDS})
    names: dict[str, dict[str, defaultdict[str, int]]] = defaultdict(
        lambda: {"dem_votes": defaultdict(int), "rep_votes": defaultdict(int)}
    )
    for row in source_rows:
        if str(row.get("office") or "").strip() != "U.S. House":
            continue
        district = district_number(row.get("district", ""))
        field = party_field(row.get("party", ""))
        count = votes(row)
        results[district][field] += count
        if field in names[district] and row.get("candidate"):
            names[district][field][str(row["candidate"]).strip()] += count

    expected = {str(value) for value in range(1, SCOPES["congressional"][1] + 1)}
    if set(results) != expected:
        raise RuntimeError(f"2026 U.S. House results are missing districts: {sorted(expected - set(results), key=int)}")

    final: dict[str, dict[str, object]] = {}
    for district, row in sorted(results.items(), key=lambda item: int(item[0])):
        total = sum(row.values())
        dem = row["dem_votes"]
        rep = row["rep_votes"]
        other = row["other_votes"]
        if dem >= rep and dem >= other:
            winner, winner_party = "Democratic", "DEM"
        elif rep >= other:
            winner, winner_party = "Republican", "REP"
        else:
            winner, winner_party = "Other", "OTH"
        final[district] = {
            "total_votes": total,
            **row,
            "dem_candidate": max(names[district]["dem_votes"], key=names[district]["dem_votes"].get, default=""),
            "rep_candidate": max(names[district]["rep_votes"], key=names[district]["rep_votes"].get, default=""),
            "winner": winner,
            "winner_party": winner_party,
            "margin_pct": ((rep - dem) / total * 100) if total else 0,
        }

    file_name = f"congressional_us_house_{YEAR}.json"
    path = OUT / file_name
    payload = {
        "meta": {
            "scope": "congressional",
            "contest_type": "us_house",
            "year": YEAR,
            "district_lines_year": 2024,
            "source": "Direct 2026 Georgia SOS U.S. House precinct results",
            "status": "preliminary",
            "generated_by": "scripts/build_2026_district_aggregates.py",
            "match_coverage_pct": 100.0,
            "total_input_votes": sum(int(row["total_votes"]) for row in final.values()),
            "matched_input_votes": sum(int(row["total_votes"]) for row in final.values()),
            "input_files": ["Data/20261103__ga__general__precinct.csv"],
        },
        "general": {"results": final},
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {file_name}: {len(final)} directly reported districts")
    return {
        "scope": "congressional",
        "contest_type": "us_house",
        "year": YEAR,
        "file": file_name,
        "rows": len(final),
        "districts": len(final),
        "total_votes": sum(int(row["total_votes"]) for row in final.values()),
        "dem_total": sum(int(row["dem_votes"]) for row in final.values()),
        "rep_total": sum(int(row["rep_votes"]) for row in final.values()),
        "other_total": sum(int(row["other_votes"]) for row in final.values()),
        "major_party_contested": True,
        "match_coverage_pct": 100.0,
        "county_reconciled": True,
        "status": "preliminary",
    }


def remap_to_2022_lines(
    scope: str,
    contest_type: str,
    current: dict[str, dict[str, object]],
) -> tuple[dict[str, dict[str, object]], dict[str, object]]:
    remap = json.loads(REMAPS_2022[scope].read_text(encoding="utf-8"))
    meta = remap.get("meta") or {}
    weights = remap.get("weights") or {}
    unchanged = {str(value) for value in meta.get("unchanged_districts", [])}
    changed = {str(value) for value in meta.get("changed_districts", [])}
    expected = {str(value) for value in range(1, SCOPES[scope][1] + 1)}
    if unchanged | changed != expected or unchanged & changed:
        raise RuntimeError(f"Invalid 2022 remap partition for {scope}")

    remapped: dict[str, dict[str, int]] = {
        district: {field: int(current[district][field]) for field in FIELDS}
        for district in unchanged
    }
    allocated: dict[str, dict[str, int]] = defaultdict(lambda: {field: 0 for field in FIELDS})
    for current_district in sorted(changed, key=int):
        source = current[current_district]
        district_weights = weights.get(current_district) or {}
        for field in FIELDS:
            field_weights = district_weights.get(field) or district_weights.get("total_votes") or {}
            assignment = allocate(
                int(source[field]),
                {str(district): float(weight) for district, weight in field_weights.items()},
            )
            if sum(assignment.values()) != int(source[field]):
                raise RuntimeError(
                    f"Could not remap {scope} {contest_type} district {current_district} {field}"
                )
            for old_district, value in assignment.items():
                if old_district not in changed:
                    raise RuntimeError(
                        f"Changed {scope} district {current_district} maps to unchanged {old_district}"
                    )
                allocated[old_district][field] += value
    remapped.update(allocated)
    if set(remapped) != expected:
        raise RuntimeError(f"2022-lines {scope} {contest_type} is incomplete")

    first = next(iter(current.values()))
    final: dict[str, dict[str, object]] = {}
    for district, row in sorted(remapped.items(), key=lambda item: int(item[0])):
        total = sum(row.values())
        dem = row["dem_votes"]
        rep = row["rep_votes"]
        winner = "Democratic" if dem > rep else "Republican"
        final[district] = {
            "total_votes": total,
            **row,
            "dem_candidate": first["dem_candidate"],
            "rep_candidate": first["rep_candidate"],
            "winner": winner,
            "winner_party": "DEM" if winner == "Democratic" else "REP",
            "margin_pct": ((rep - dem) / total * 100) if total else 0,
        }
    for field in FIELDS:
        if sum(int(row[field]) for row in final.values()) != sum(int(row[field]) for row in current.values()):
            raise RuntimeError(f"2022-lines {scope} {contest_type} does not conserve {field}")
    return final, meta


def write_2022_result(
    scope: str,
    contest_type: str,
    current: dict[str, dict[str, object]],
) -> dict[str, object]:
    final, remap_meta = remap_to_2022_lines(scope, contest_type, current)
    out_dir = ROOT / "Data/district_contests_2022"
    file_name = f"{scope}_{contest_type}_{YEAR}.json"
    path = out_dir / file_name
    payload = {
        "meta": {
            "scope": scope,
            "contest_type": contest_type,
            "year": YEAR,
            "district_lines_year": 2022,
            "source": (
                "Complete 2026 SOS same-election district-bucket allocation remapped to "
                "2022 legislative lines; unchanged districts retain their direct totals and "
                "redrawn districts use the repository's constrained map-overlap weights"
            ),
            "status": "preliminary",
            "generated_by": "scripts/build_2026_district_aggregates.py",
            "remap_method": remap_meta.get("method"),
            "unchanged_geometry_districts": remap_meta.get("unchanged_districts", []),
            "spatially_remapped_districts": remap_meta.get("changed_districts", []),
            "match_coverage_pct": 100.0,
            "input_files": [
                f"Data/district_contests_2024/{scope}_{contest_type}_{YEAR}.json",
                str(REMAPS_2022[scope].relative_to(ROOT)).replace("\\", "/"),
            ],
        },
        "general": {"results": final},
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote 2022-lines {file_name}: {len(final)} districts")
    return {
        "scope": scope,
        "contest_type": contest_type,
        "year": YEAR,
        "file": file_name,
        "rows": len(final),
        "districts": len(final),
        "total_votes": sum(int(row["total_votes"]) for row in final.values()),
        "dem_total": sum(int(row["dem_votes"]) for row in final.values()),
        "rep_total": sum(int(row["rep_votes"]) for row in final.values()),
        "other_total": sum(int(row["other_votes"]) for row in final.values()),
        "major_party_contested": True,
        "match_coverage_pct": 100.0,
        "county_reconciled": True,
        "status": "preliminary",
    }


def update_manifest(path: Path, updates: list[dict[str, object]]) -> None:
    manifest = json.loads(path.read_text(encoding="utf-8-sig"))
    update_keys = {
        (entry["scope"], entry["contest_type"], entry["year"]): entry
        for entry in updates
    }
    kept = [
        entry for entry in manifest.get("files", [])
        if (entry.get("scope"), entry.get("contest_type"), entry.get("year")) not in update_keys
    ]
    kept.extend(updates)
    kept.sort(key=lambda entry: (entry["scope"], entry["contest_type"], int(entry["year"])))
    manifest["files"] = kept
    path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    rows = load_rows()
    buckets = build_buckets(rows)
    updates = []
    current_results: dict[tuple[str, str], dict[str, dict[str, object]]] = {}
    for scope in SCOPES:
        for office, contest_type in CONTESTS.items():
            entry, results = write_result(scope, contest_type, rows, buckets[scope], office)
            updates.append(entry)
            current_results[(scope, contest_type)] = results
    updates.append(write_us_house_result(rows))
    update_manifest(OUT / "manifest.json", updates)
    print(f"Updated {OUT / 'manifest.json'} with {len(updates)} preliminary 2026 slices")

    old_updates = []
    for scope in REMAPS_2022:
        for contest_type in CONTESTS.values():
            old_updates.append(write_2022_result(scope, contest_type, current_results[(scope, contest_type)]))
    old_manifest = ROOT / "Data/district_contests_2022/manifest.json"
    update_manifest(old_manifest, old_updates)
    print(f"Updated {old_manifest} with {len(old_updates)} preliminary 2026 slices")


if __name__ == "__main__":
    main()
