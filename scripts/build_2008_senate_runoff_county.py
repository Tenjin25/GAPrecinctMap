"""Build the 2008 Georgia U.S. Senate runoff county layer from a source CSV."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


COLORS = {
    "dem": ["#deebf7", "#9ecae1", "#6baed6", "#3182bd", "#08519c", "#08306b"],
    "rep": ["#fee0d2", "#fcae91", "#fb6a4a", "#de2d26", "#a50f15", "#67000d"],
}


def color_for_margin(margin_pct: float, winner: str) -> str:
    if abs(margin_pct) < 1e-9:
        return "#fee8c8"
    scale = min(5, int(abs(margin_pct) // 10) + 1)
    return COLORS["dem" if winner == "DEM" else "rep"][scale]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()

    totals: dict[str, dict[str, int | str]] = {}
    with args.source.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            if row.get("office", "").strip() != "U.S. Senate":
                continue
            county = row["county"].strip().upper()
            candidate = row["candidate"].strip()
            votes = int(row["votes"])
            item = totals.setdefault(county, {"dem_votes": 0, "rep_votes": 0})
            if candidate == "Jim Martin":
                item["dem_votes"] = int(item["dem_votes"]) + votes
            elif candidate == "Saxby Chambliss":
                item["rep_votes"] = int(item["rep_votes"]) + votes

    rows = []
    for county in sorted(totals):
        item = totals[county]
        dem = int(item["dem_votes"])
        rep = int(item["rep_votes"])
        total = dem + rep
        margin = dem - rep
        margin_pct = (margin / total * 100) if total else 0.0
        winner = "DEM" if margin > 0 else "REP" if margin < 0 else "TIE"
        rows.append({
            "county": county,
            "dem_votes": dem,
            "rep_votes": rep,
            "other_votes": 0,
            "total_votes": total,
            "dem_candidate": "Jim Martin",
            "rep_candidate": "Saxby Chambliss",
            "margin": margin,
            "margin_pct": round(margin_pct, 4),
            "winner": winner,
            "color": color_for_margin(margin_pct, winner),
        })

    if len(rows) != 159:
        raise SystemExit(f"Expected 159 counties, found {len(rows)}")
    dem_total = sum(row["dem_votes"] for row in rows)
    rep_total = sum(row["rep_votes"] for row in rows)
    if (dem_total, rep_total) != (909923, 1228033):
        raise SystemExit(f"Unexpected statewide totals: DEM={dem_total}, REP={rep_total}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps({"contest_type": "us_senate_runoff", "year": 2008, "rows": rows}, separators=(",", ":")),
        encoding="utf-8",
    )
    print(f"Wrote {args.output} ({len(rows)} counties)")


if __name__ == "__main__":
    main()
