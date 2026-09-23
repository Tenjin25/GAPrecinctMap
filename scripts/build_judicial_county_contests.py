"""Build contested Georgia appellate-court county slices for Data/contests."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path

from build_judicial_district_aggregates import (
    SOURCE_FILES,
    candidate_key,
    choose_sides,
    clean_candidate,
    slug,
)


COLORS = {
    "DEM": ["#deebf7", "#9ecae1", "#6baed6", "#3182bd", "#08519c", "#08306b"],
    "REP": ["#fee0d2", "#fcae91", "#fb6a4a", "#de2d26", "#a50f15", "#67000d"],
}


def color_for_margin(margin_pct: float, winner: str) -> str:
    if winner == "TIE":
        return "#fee8c8"
    index = min(5, int(abs(margin_pct) // 10) + 1)
    return COLORS[winner][index]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", type=Path, default=Path("Data/contests"))
    parser.add_argument("--county-demographics", type=Path, default=Path("Data/ga_county_demographics.json"))
    args = parser.parse_args()
    counties = sorted(json.loads(args.county_demographics.read_text(encoding="utf-8"))["counties"])
    manifest_path = args.out_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    generated = []

    for year, source in SOURCE_FILES.items():
        grouped: dict[tuple[str, str], dict[str, dict[str, int]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(int))
        )
        display_votes: dict[tuple[str, str], dict[str, dict[str, int]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(int))
        )
        with source.open(encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                office = str(row.get("office") or "")
                if office not in {"Supreme Court Justice", "Appeals Court Judge"}:
                    continue
                seat = str(row.get("district") or "")
                county = str(row.get("county") or "").strip().upper()
                name = clean_candidate(str(row.get("candidate") or ""))
                key = candidate_key(name)
                votes = int(row.get("votes") or 0)
                grouped[(office, seat)][county][key] += votes
                display_votes[(office, seat)][key][name] += votes

        for (office, seat), county_votes in grouped.items():
            totals: dict[str, int] = defaultdict(int)
            for votes in county_votes.values():
                for key, value in votes.items():
                    totals[key] += value
            named = [key for key, votes in totals.items() if votes > 0 and key != "WRITE-IN"]
            named_total = sum(totals[key] for key in named)
            material = [key for key in named if named_total and totals[key] / named_total >= 0.01]
            if len(material) < 2:
                continue
            side_a, side_b, alignment = choose_sides(year, office, seat, totals)
            display = {
                key: max(names.items(), key=lambda item: (item[1], item[0]))[0]
                for key, names in display_votes[(office, seat)].items()
            }
            rows = []
            for county in counties:
                votes = county_votes.get(county, {})
                a = int(votes.get(side_a, 0))
                b = int(votes.get(side_b, 0))
                other = sum(int(value) for key, value in votes.items() if key not in {side_a, side_b})
                total = a + b + other
                margin = a - b
                margin_pct = (margin / total * 100) if total else 0.0
                winner = "DEM" if margin > 0 else "REP" if margin < 0 else "TIE"
                rows.append({
                    "county": county,
                    "dem_votes": a,
                    "rep_votes": b,
                    "other_votes": other,
                    "total_votes": total,
                    "dem_candidate": display.get(side_a, ""),
                    "rep_candidate": display.get(side_b, ""),
                    "candidate_a": display.get(side_a, ""),
                    "candidate_a_votes": a,
                    "candidate_b": display.get(side_b, ""),
                    "candidate_b_votes": b,
                    "nonpartisan": True,
                    "alignment": alignment,
                    "margin": margin,
                    "margin_pct": round(margin_pct, 4),
                    "winner": winner,
                    "color": color_for_margin(margin_pct, winner),
                })
            prefix = "ga_supreme_court" if office == "Supreme Court Justice" else "ga_court_of_appeals"
            contest_type = f"{prefix}_{slug(seat)}_seat"
            file_name = f"{contest_type}_{year}.json"
            (args.out_dir / file_name).write_text(
                json.dumps({"contest_type": contest_type, "year": year, "rows": rows}, indent=2) + "\n",
                encoding="utf-8",
            )
            generated.append({"contest_type": contest_type, "year": year, "file": file_name, "rows": len(rows)})

    keys = {(row["contest_type"], int(row["year"])) for row in generated}
    manifest["files"] = [
        row for row in manifest["files"]
        if (row.get("contest_type"), int(row.get("year", 0))) not in keys
    ] + generated
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {len(generated)} contested judicial county slices")


if __name__ == "__main__":
    main()
