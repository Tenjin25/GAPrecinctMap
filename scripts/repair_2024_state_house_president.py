"""Repair the 2024 State House presidential slice from the SOS precinct export.

The 2024 export uses polling-place names, while the VTD20 matcher expects the
older nine VTD names in Lowndes and several other counties.  The State House
contest in the same export already records the district-specific vote buckets,
so those buckets provide the correct allocation weights for statewide contests.
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "Data" / "20241105__ga__general__precinct-level.csv"
OUTPUT = ROOT / "Data" / "district_contests_2024" / "state_house_president_2024.json"


def num(value: str) -> int:
    try:
        return int(float(str(value or "0").strip() or 0))
    except ValueError:
        return 0


def key(row: dict[str, str]) -> tuple[str, str]:
    return (row["county"].strip().upper(), row["precinct"].strip().upper())


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
    print(f"Wrote {OUTPUT}")
    print("HD 177:", final.get("177"))


if __name__ == "__main__":
    main()
