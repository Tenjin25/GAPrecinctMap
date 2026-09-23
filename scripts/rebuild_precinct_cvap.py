"""Allocate 2020-block CVAP24 to each dated Georgia precinct boundary set.

Each block's Census internal point assigns its entire published CVAP vector to
one precinct in the same county. This preserves each county's block sum and avoids a
false exact spatial claim for blocks divided by precinct lines.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import zipfile
from collections import defaultdict
from pathlib import Path

import numpy as np
import shapefile
from shapely import STRtree, points
from shapely.geometry import shape


YEARS = (2012, 2014, 2016, 2018, 2020, 2022, 2024)


def block_inputs(data_dir: Path):
    with zipfile.ZipFile(data_dir / "ga_cvap_2024_2020_b_csv.zip") as archive:
        with io.TextIOWrapper(archive.open("ga_cvap_2024_2020_b.csv"), encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            fields = [field for field in reader.fieldnames if field != "GEOID20"]
            values = {row["GEOID20"]: np.array([int(row[field] or 0) for field in fields], dtype=np.int64)
                      for row in reader}

    by_county = defaultdict(lambda: {"lon": [], "lat": [], "values": []})
    with zipfile.ZipFile(data_dir / "tl_2020_13_tabblock20.zip") as archive:
        dbf = next(name for name in archive.namelist() if name.lower().endswith(".dbf"))
        reader = shapefile.Reader(dbf=io.BytesIO(archive.read(dbf)))
        for record in reader.iterRecords():
            row = record.as_dict()
            vector = values.get(row["GEOID20"])
            if vector is None or not vector.any():
                continue
            county = row["COUNTYFP20"]
            group = by_county[county]
            group["lon"].append(float(row["INTPTLON20"]))
            group["lat"].append(float(row["INTPTLAT20"]))
            group["values"].append(vector)
    county_names = {feature["properties"]["COUNTYFP20"]: feature["properties"]["county_norm"]
                    for feature in json.loads((data_dir / "tl_2020_13_county20.geojson").read_text(encoding="utf-8"))["features"]}
    return fields, by_county, county_names


def build_year(year: int, data_dir: Path, fields, blocks, county_names):
    features = json.loads((data_dir / f"Voting_Precincts_{year}.geojson").read_text(encoding="utf-8"))["features"]
    by_county = defaultdict(list)
    for feature in features:
        by_county[feature["properties"]["county_norm"]].append(feature)

    totals = defaultdict(lambda: np.zeros(len(fields), dtype=np.int64))
    nearest_count = 0
    overlap_count = 0
    for fips, group in blocks.items():
        county = county_names[fips]
        shapes = by_county.get(county, [])
        if not shapes:
            raise ValueError(f"No precinct geometry for {year} {county}")
        geoms = [shape(feature["geometry"]) for feature in shapes]
        tree = STRtree(geoms)
        block_points = points(np.asarray(group["lon"]), np.asarray(group["lat"]))
        hit_points, hit_shapes = tree.query(block_points, predicate="intersects")
        assignments = np.full(len(block_points), -1, dtype=np.int32)
        for point_index, shape_index in zip(hit_points, hit_shapes):
            if assignments[point_index] >= 0:
                overlap_count += 1
                continue
            assignments[point_index] = shape_index
        missing = np.flatnonzero(assignments < 0)
        if len(missing):
            assignments[missing] = tree.nearest(block_points[missing])
            nearest_count += len(missing)
        vectors = np.asarray(group["values"], dtype=np.int64)
        precinct_totals = np.zeros((len(shapes), len(fields)), dtype=np.int64)
        np.add.at(precinct_totals, assignments, vectors)
        if not np.array_equal(precinct_totals.sum(axis=0), vectors.sum(axis=0)):
            raise AssertionError(f"CVAP total mismatch for {year} {county}")
        for feature, vector in zip(shapes, precinct_totals):
            totals[feature["properties"]["precinct_norm"]] += vector

    output = data_dir / "cvap_aggregates" / f"precinct_shapes_{year}__cvap24.csv"
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["precinct_id", *fields])
        for precinct, vector in sorted(totals.items()):
            writer.writerow([precinct, *vector.tolist()])
    print(year, len(totals), "precinct keys;", nearest_count, "blocks assigned to nearest precinct;", overlap_count, "overlap hits")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("years", nargs="*", type=int, choices=YEARS, default=YEARS)
    parser.add_argument("--data-dir", type=Path, default=Path("Data"))
    args = parser.parse_args()
    fields, blocks, county_names = block_inputs(args.data_dir)
    for year in args.years:
        build_year(year, args.data_dir, fields, blocks, county_names)
