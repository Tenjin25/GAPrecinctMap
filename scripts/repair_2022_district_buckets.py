"""Repair 2022-lines district overlays from direct Georgia SOS ballot buckets.

The statewide contests and the U.S. House/State House/State Senate contests in
the 2022 precinct export share county+precinct keys. District-contest vote totals
therefore provide same-election allocation weights for precincts split between
districts. These direct ballot buckets supersede modeled VTD crosswalks for the
2022-lines archive only.
"""

from __future__ import annotations

import csv
import json
import math
import re
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
GENERAL = ROOT / "Data/20221108__ga__general__official__precinct.csv"
DISTRICT_BUCKETS = ROOT / "Data/20221108__ga__general__precinct.csv"
OUT = ROOT / "Data/district_contests_2022"

SCOPES = {
    "congressional": ("U.S. House", 14),
    "state_house": ("State House", 180),
    "state_senate": ("State Senate", 56),
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
FIELDS = ("dem_votes", "rep_votes", "other_votes")


def norm(value: object) -> str:
    return re.sub(r"[^A-Z0-9]+", "", str(value or "").upper())


def precinct_key(row: dict[str, str]) -> tuple[str, str]:
    return norm(row.get("county")), norm(row.get("precinct"))


def votes(row: dict[str, str]) -> int:
    return int(float(str(row.get("total_votes") or 0).replace(",", "")))


def party_field(party: str) -> str:
    value = norm(party)
    if value.startswith("DEM"): return "dem_votes"
    if value.startswith("REP"): return "rep_votes"
    return "other_votes"


def district_number(raw: str) -> str:
    return str(int(float(raw)))


def allocate(total: int, weights: dict[str, int]) -> dict[str, int]:
    denominator = sum(max(0, value) for value in weights.values())
    if denominator <= 0:
        return {}
    exact = {key: total * value / denominator for key, value in weights.items() if value > 0}
    result = {key: math.floor(value) for key, value in exact.items()}
    remainder = total - sum(result.values())
    order = sorted(result, key=lambda key: (exact[key] - result[key], -int(key)), reverse=True)
    for key in order[:remainder]: result[key] += 1
    return result


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", errors="replace", newline="") as handle:
        return list(csv.DictReader(handle))


def canonical(contest_type: str) -> tuple[dict[str, int], str, str]:
    payload = json.loads((ROOT / f"Data/contests/{contest_type}_2022.json").read_text(encoding="utf-8-sig"))
    totals = {field: 0 for field in FIELDS}
    names: dict[str, defaultdict[str, int]] = {
        "dem_votes": defaultdict(int), "rep_votes": defaultdict(int)
    }
    for row in payload.get("rows", []):
        for field in FIELDS: totals[field] += int(row.get(field) or 0)
        if row.get("dem_candidate"): names["dem_votes"][str(row["dem_candidate"])] += int(row.get("dem_votes") or 0)
        if row.get("rep_candidate"): names["rep_votes"][str(row["rep_candidate"])] += int(row.get("rep_votes") or 0)
    dem = max(names["dem_votes"], key=names["dem_votes"].get, default="Democratic")
    rep = max(names["rep_votes"], key=names["rep_votes"].get, default="Republican")
    if rep.casefold() == "herschel junior walker":
        rep = "Herschel J. Walker"
    return totals, dem, rep


def build_buckets(rows: list[dict[str, str]]) -> dict[str, dict[tuple[str, str], dict[str, int]]]:
    result = {scope: defaultdict(lambda: defaultdict(int)) for scope in SCOPES}
    office_to_scope = {office: scope for scope, (office, _) in SCOPES.items()}
    for row in rows:
        scope = office_to_scope.get(str(row.get("office") or "").strip())
        if not scope or not str(row.get("district") or "").strip(): continue
        result[scope][precinct_key(row)][district_number(row["district"])] += votes(row)
    return result


def reconcile(results: dict[str, dict[str, int]], targets: dict[str, int]) -> None:
    for field, target in targets.items():
        assigned = allocate(target, {district: row[field] for district, row in results.items()})
        for district, value in assigned.items(): results[district][field] = value


def write_result(scope: str, contest_type: str, source_rows: list[dict[str, str]],
                 buckets: dict[tuple[str, str], dict[str, int]], office: str) -> dict[str, object]:
    statewide: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in source_rows:
        if str(row.get("office") or "").strip() != office: continue
        statewide[precinct_key(row)][party_field(row.get("party", ""))] += votes(row)

    results: dict[str, dict[str, int]] = defaultdict(lambda: {field: 0 for field in FIELDS})
    total_votes = matched_votes = 0
    for key, parties in statewide.items():
        total = sum(parties.values()); total_votes += total
        weights = buckets.get(key)
        if not weights: continue
        matched_votes += total
        for field in FIELDS:
            for district, value in allocate(parties.get(field, 0), weights).items():
                results[district][field] += value

    expected = SCOPES[scope][1]
    if set(results) != {str(value) for value in range(1, expected + 1)}:
        raise RuntimeError(f"{scope} {contest_type} did not produce all {expected} districts")
    targets, dem_name, rep_name = canonical(contest_type)
    reconcile(results, targets)
    final = {}
    for district, row in sorted(results.items(), key=lambda item: int(item[0])):
        total = sum(row.values()); dem=row["dem_votes"]; rep=row["rep_votes"]
        winner = "Democratic" if dem > rep else "Republican"
        final[district] = {"total_votes": total, **row, "dem_candidate": dem_name,
                           "rep_candidate": rep_name, "winner": winner,
                           "winner_party": "DEM" if winner == "Democratic" else "REP",
                           "margin_pct": ((rep-dem)/total*100) if total else 0}
    coverage = matched_votes / total_votes * 100 if total_votes else 0
    path = OUT / f"{scope}_{contest_type}_2022.json"
    payload = {"meta": {"scope": scope, "contest_type": contest_type, "year": 2022,
               "district_lines_year": 2022, "source": "2022 SOS precinct export allocated by direct district ballot buckets",
               "generated_by": "scripts/repair_2022_district_buckets.py", "match_coverage_pct": coverage,
               "total_input_votes": total_votes, "matched_input_votes": matched_votes,
               "input_files": [str(GENERAL.relative_to(ROOT)).replace("\\", "/"),
                               str(DISTRICT_BUCKETS.relative_to(ROOT)).replace("\\", "/")]},
               "general": {"results": final}}
    path.write_text(json.dumps(payload, indent=2)+"\n", encoding="utf-8")
    print(f"Wrote {path.name}: {coverage:.2f}% direct-bucket coverage")
    return {"file": path.name, "rows": len(final), "districts": len(final),
            "total_votes": sum(x["total_votes"] for x in final.values()),
            "dem_total": sum(x["dem_votes"] for x in final.values()),
            "rep_total": sum(x["rep_votes"] for x in final.values()),
            "other_total": sum(x["other_votes"] for x in final.values()),
            "major_party_contested": True, "match_coverage_pct": coverage, "county_reconciled": False}


def main() -> None:
    general = load_rows(GENERAL); all_updates=[]
    buckets = build_buckets(load_rows(DISTRICT_BUCKETS))
    for scope in SCOPES:
        for office, contest_type in CONTESTS.items():
            all_updates.append((scope, contest_type, write_result(scope, contest_type, general, buckets[scope], office)))
    manifest_path=OUT/"manifest.json"; manifest=json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    updates={(scope, contest, 2022): row for scope,contest,row in all_updates}
    for entry in manifest["files"]:
        key=(entry.get("scope"),entry.get("contest_type"),int(entry.get("year") or 0))
        if key in updates: entry.update(updates[key])
    manifest_path.write_text(json.dumps(manifest,indent=2)+"\n",encoding="utf-8")


if __name__ == "__main__": main()
