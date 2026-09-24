"""Clip Fulton tabblocks to unmatched 2024 precinct pieces for review.

The internal point determines which 2024 precinct gets a block's full CVAP
vector, matching rebuild_precinct_cvap.py. This does not estimate votes.
"""

import argparse
import csv
import io
import json
import zipfile
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen, urlretrieve

import shapefile
from shapely.geometry import Point, mapping, shape
from shapely.strtree import STRtree


BLOCK_SOURCE = (
    "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/TABBLOCK/2020/"
    "tl_2020_13121_tabblock20.zip"
)
PRECINCT_SOURCE = (
    "https://services1.arcgis.com/AQDHTHDrZzfsFsB5/ArcGIS/rest/services/"
    "Voting_Precincts_Nov_2025/FeatureServer/0/query"
)


def build(data_dir: Path, block_zip: Path, cvap_zip: Path, newer_geojson: Path | None) -> None:
    with (data_dir / "precinct_2024_match_audit.csv").open(encoding="utf-8", newline="") as handle:
        missing = {row["precinct_id"] for row in csv.DictReader(handle)
                   if row["county"] == "FULTON" and row["method"] == "unmatched"}
    if len(missing) != 15:
        raise ValueError(f"Expected 15 unmatched Fulton precincts, found {len(missing)}")
    older = json.loads((data_dir / "Voting_Precincts_2024.geojson").read_text(encoding="utf-8"))
    fragments = [feature for feature in older["features"]
                 if feature["properties"]["county_norm"] == "FULTON"
                 and feature["properties"]["prec_id"] in missing]
    if len(fragments) != len(missing):
        raise ValueError("Unmatched audit and 2024 geometry disagree")
    fragment_geoms = [shape(feature["geometry"]) for feature in fragments]
    fragment_tree = STRtree(fragment_geoms)

    if newer_geojson:
        newer = json.loads(newer_geojson.read_text(encoding="utf-8"))
    else:
        query = urlencode({"where": "1=1", "outFields": "VoterDist", "returnGeometry": "true",
                           "outSR": "4326", "f": "geojson"})
        with urlopen(f"{PRECINCT_SOURCE}?{query}", timeout=60) as response:
            newer = json.load(response)
    new_codes = [str(feature["properties"].get("VoterDist") or "").strip().upper()
                 for feature in newer["features"]]
    new_tree = STRtree([shape(feature["geometry"]) for feature in newer["features"]])

    with zipfile.ZipFile(cvap_zip) as archive:
        with io.TextIOWrapper(archive.open("ga_cvap_2024_2020_b.csv"), encoding="utf-8-sig") as handle:
            cvap = {row["GEOID20"]: int(row["CVAP_TOT24"] or 0) for row in csv.DictReader(handle)
                    if row["GEOID20"].startswith("13121")}
    if not block_zip.exists():
        block_zip.parent.mkdir(parents=True, exist_ok=True)
        urlretrieve(BLOCK_SOURCE, block_zip)

    clipped = []
    intersecting = Counter()
    assigned = Counter()
    allocated_cvap = Counter()
    successor_blocks = defaultdict(Counter)
    successor_cvap = defaultdict(Counter)
    assigned_geoids = set()
    with zipfile.ZipFile(block_zip) as archive:
        names = archive.namelist()
        shp, shx, dbf = (next(name for name in names if name.lower().endswith(f".{ext}"))
                         for ext in ("shp", "shx", "dbf"))
        reader = shapefile.Reader(shp=io.BytesIO(archive.read(shp)),
                                  shx=io.BytesIO(archive.read(shx)),
                                  dbf=io.BytesIO(archive.read(dbf)))
        for record in reader.iterShapeRecords():
            fields = record.record.as_dict()
            block = shape(record.shape.__geo_interface__)
            geoid = fields["GEOID20"]
            internal_point = Point(float(fields["INTPTLON20"]), float(fields["INTPTLAT20"]))
            for index in fragment_tree.query(block):
                part = block.intersection(fragment_geoms[index])
                if part.is_empty or part.area == 0:
                    continue
                code = fragments[index]["properties"]["prec_id"]
                inside = fragment_geoms[index].covers(internal_point)
                successor = ""
                allocated = 0
                if inside:
                    if geoid in assigned_geoids:
                        raise ValueError(f"Block internal point assigned twice: {geoid}")
                    assigned_geoids.add(geoid)
                    allocated = cvap.get(geoid, 0)
                    destinations = new_tree.query(internal_point, predicate="intersects")
                    if len(destinations) != 1:
                        raise ValueError(f"Expected one 2025 precinct for block {geoid}, found {len(destinations)}")
                    successor = new_codes[destinations[0]]
                    assigned[code] += 1
                    allocated_cvap[code] += allocated
                    successor_blocks[code][successor] += 1
                    successor_cvap[code][successor] += allocated
                intersecting[code] += 1
                clipped.append({"type": "Feature", "properties": {
                    "precinct_2024": code,
                    "block_geoid20": geoid,
                    "internal_point_in_fragment": inside,
                    "allocated_cvap24": allocated,
                    "nov2025_precinct_at_point": successor,
                }, "geometry": mapping(part)})

    with (data_dir / "cvap_aggregates/precinct_shapes_2024__cvap24.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        expected = {row["precinct_id"]: int(row["CVAP_TOT24"]) for row in csv.DictReader(handle)}
    for code in missing:
        if allocated_cvap[code] != expected[f"FULTON - {code}"]:
            raise ValueError(f"Block CVAP differs from existing 2024 allocation: {code}")

    geojson_path = data_dir / "precinct_2024_fulton_tabblocks.geojson"
    geojson_path.write_text(json.dumps({"type": "FeatureCollection", "features": clipped},
                                       separators=(",", ":")), encoding="utf-8")
    rows = [{
        "precinct_2024": code,
        "intersecting_tabblocks": intersecting[code],
        "assigned_tabblocks": assigned[code],
        "allocated_cvap24": allocated_cvap[code],
        "nov2025_precincts_by_block_point": "; ".join(
            f"{successor} {count}" for successor, count in successor_blocks[code].most_common()
        ),
        "nov2025_cvap_by_block_point": "; ".join(
            f"{successor} {value}" for successor, value in successor_cvap[code].most_common()
        ),
    } for code in sorted(missing)]
    with (data_dir / "precinct_2024_fulton_tabblock_audit.csv").open(
        "w", encoding="utf-8", newline=""
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"{len(clipped)} clipped block pieces; {len(assigned_geoids)} internal points; "
          f"{sum(allocated_cvap.values())} CVAP across {len(rows)} precinct fragments")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("Data"))
    parser.add_argument("--block-zip", type=Path, default=Path("Data/_external/tl_2020_13121_tabblock20.zip"))
    parser.add_argument("--cvap-zip", type=Path, default=Path("Data/ga_cvap_2024_2020_b_csv.zip"))
    parser.add_argument("--new-geojson", type=Path)
    args = parser.parse_args()
    build(args.data_dir, args.block_zip, args.cvap_zip, args.new_geojson)
