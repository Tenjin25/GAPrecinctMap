"""Compare unmatched 2024 Fulton shapes with Fulton's November 2025 precinct layer.

This is a geometry comparison, not an election-result crosswalk. Later precinct
boundaries cannot establish where November 2024 votes were reported.
"""

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

from pyproj import Transformer
from shapely.geometry import shape
from shapely.ops import transform, unary_union
from shapely.strtree import STRtree


SOURCE = (
    "https://services1.arcgis.com/AQDHTHDrZzfsFsB5/ArcGIS/rest/services/"
    "Voting_Precincts_Nov_2025/FeatureServer/0/query"
)


def build(data_dir: Path, new_geojson: Path | None = None) -> None:
    if new_geojson:
        newer = json.loads(new_geojson.read_text(encoding="utf-8"))
    else:
        query = urlencode({"where": "1=1", "outFields": "VoterDist", "returnGeometry": "true",
                           "outSR": "4326", "f": "geojson"})
        with urlopen(f"{SOURCE}?{query}", timeout=60) as response:
            newer = json.load(response)
    older = json.loads((data_dir / "Voting_Precincts_2024.geojson").read_text(encoding="utf-8"))
    with (data_dir / "precinct_2024_match_audit.csv").open(encoding="utf-8", newline="") as handle:
        missing = {row["precinct_id"] for row in csv.DictReader(handle)
                   if row["county"] == "FULTON" and row["method"] == "unmatched"}
    if len(missing) != 15:
        raise ValueError(f"Expected 15 unmatched Fulton codes, found {len(missing)}")

    with (data_dir / "cvap_aggregates/precinct_shapes_2024__cvap24.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        cvap = {row["precinct_id"]: int(row["CVAP_TOT24"]) for row in csv.DictReader(handle)}
    votes_2022 = defaultdict(int)
    present_2022 = set()
    with (data_dir / "20221108__ga__general__official__precinct.csv").open(
        encoding="utf-8-sig", newline=""
    ) as handle:
        for row in csv.DictReader(handle):
            if row["county"].upper() == "FULTON" and row["office"] == "Governor":
                code = row["precinct"].strip().upper()
                present_2022.add(code)
                votes_2022[code] += int(row["total_votes"] or 0)

    to_equal_area = Transformer.from_crs("EPSG:4326", "EPSG:5070", always_xy=True).transform
    by_new_code = defaultdict(list)
    for feature in newer["features"]:
        code = str(feature["properties"].get("VoterDist") or "").strip().upper()
        if code:
            by_new_code[code].append(transform(to_equal_area, shape(feature["geometry"])))
    codes = sorted(by_new_code)
    geometries = [unary_union(by_new_code[code]) for code in codes]
    tree = STRtree(geometries)

    rows = []
    for feature in older["features"]:
        props = feature["properties"]
        code = props["prec_id"]
        if props["county_norm"] != "FULTON" or code not in missing:
            continue
        geometry = transform(to_equal_area, shape(feature["geometry"]))
        overlaps = sorted(
            ((codes[index], 100 * geometry.intersection(geometries[index]).area / geometry.area)
             for index in tree.query(geometry)),
            key=lambda item: item[1], reverse=True,
        )
        overlaps = [(successor, share) for successor, share in overlaps if share > 0.0001]
        main_code, main_share = overlaps[0] if overlaps else ("", 0.0)
        rows.append({
            "precinct_2024": code,
            "governor_votes_2022": votes_2022[code] if code in present_2022 else "",
            "cvap_2024": cvap[f"FULTON - {code}"],
            "code_exists_nov2025": "yes" if code in by_new_code else "no",
            "largest_nov2025_overlap": main_code,
            "largest_overlap_pct": f"{main_share:.2f}",
            "other_overlaps_at_least_1pct": "; ".join(
                f"{successor} {share:.2f}%" for successor, share in overlaps[1:] if share >= 1
            ),
            "nov2025_coverage_pct": f"{sum(share for _, share in overlaps):.2f}",
        })
    if len(rows) != len(missing):
        raise ValueError("Unmatched audit and 2024 geometries disagree")
    output = data_dir / "precinct_2024_fulton_nov2025_comparison.csv"
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda row: row["precinct_2024"]))
    print(f"Compared {len(rows)} Fulton fragments to {len(codes)} November 2025 precincts: {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("Data"))
    parser.add_argument("--new-geojson", type=Path, help="Previously downloaded Fulton November 2025 layer")
    args = parser.parse_args()
    build(args.data_dir, args.new_geojson)
