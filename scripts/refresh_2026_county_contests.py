"""Refresh November 2026 county contest slices from the precinct export."""

import csv
import json
from collections import defaultdict
from pathlib import Path

from build_2026_district_aggregates import CONTESTS, ROOT


COLORS = {
    "DEM": [(40, "#08306b"), (30, "#08519c"), (20, "#3182bd"),
            (10, "#6baed6"), (5.5, "#9ecae1"), (1, "#c6dbef"),
            (0.5, "#e1f5fe"), (0, "#bfdbfe")],
    "REP": [(40, "#67000d"), (30, "#a50f15"), (20, "#cb181d"),
            (10, "#ef3b2c"), (5.5, "#fb6a4a"), (1, "#fcae91"),
            (0.5, "#fee8c8"), (0, "#fecaca")],
}


def main() -> None:
    source = ROOT / "Data/20261103__ga__general__precinct.csv"
    totals = defaultdict(lambda: defaultdict(int))
    with source.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            contest = CONTESTS.get(row["office"])
            if contest:
                party = row["party"].upper()
                field = f"{party.lower()}_votes" if party in ("DEM", "REP") else "other_votes"
                totals[(contest, row["county"].upper())][field] += int(row["votes"])

    for contest in CONTESTS.values():
        path = ROOT / f"Data/contests/{contest}_2026.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        for row in payload["rows"]:
            votes = totals[(contest, row["county"])]
            dem, rep, other = (votes[field] for field in ("dem_votes", "rep_votes", "other_votes"))
            total = dem + rep + other
            margin = dem - rep
            pct = round(margin / total * 100, 4) if total else 0
            winner = "DEM" if margin > 0 else "REP" if margin < 0 else "TIE"
            color = "#9ca3af" if winner == "TIE" else next(
                shade for threshold, shade in COLORS[winner] if abs(pct) >= threshold
            )
            row.update(dem_votes=dem, rep_votes=rep, other_votes=other, total_votes=total,
                       margin=margin, margin_pct=pct, winner=winner, color=color)
        if len(payload["rows"]) != 159:
            raise ValueError(f"{contest}: expected 159 counties")
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(contest, sum(row["total_votes"] for row in payload["rows"]))


if __name__ == "__main__":
    main()
