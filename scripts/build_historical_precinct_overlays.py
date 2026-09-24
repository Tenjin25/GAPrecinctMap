"""Build dated precinct boundaries and directly matched election results."""

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

from shapely.geometry import mapping, shape

from build_2024_precinct_overlay import friendly_name, match_name, match_precincts, read_shapes


SOURCES = {
    2012: ("voting-precinct-2012.zip", "20121106", "President", "precinct"),
    2014: ("vtd2014-shape.zip", "20141104", "Governor", "precinct"),
    2016: ("vtd2016-shape.zip", "20161108", "President of the United States", "precinct"),
    2018: ("vtd2018-shapefile.zip", "20181106", "Governor", "precinct"),
    2020: ("vtd2020-shape-4-20-21.zip", "20201103", "President", "precinct"),
    2022: ("ga-precincts2022-shape.zip", "20221108", "Governor", "official__precinct"),
}


def classify_party(party: str, candidate: str, learned: dict, key: tuple[str, str, str]) -> str:
    token = re.sub(r"[^A-Z]", "", party.upper())
    if token in {"D", "DEM", "DEMOCRAT", "DEMOCRATIC"}:
        return "dem_votes"
    if token in {"R", "REP", "REPUBLICAN"}:
        return "rep_votes"
    if not token:
        known = learned.get(key)
        if known:
            return known
        suffix = re.search(r"\(([DR])\)\s*$", candidate.upper())
        if suffix:
            return "dem_votes" if suffix.group(1) == "D" else "rep_votes"
    return "other_votes"


def build(year: int, data_dir: Path) -> None:
    zip_name, date, probe_office, suffix = SOURCES[year]
    results_csv = data_dir / f"{date}__ga__general__{suffix}.csv"
    prior_names = json.loads((data_dir / "precinct_friendly_names_latest.json").read_text(encoding="utf-8"))["counties"]
    polygons, centroids, raw_names = [], [], []
    shape_index = {}
    for record, geometry in read_shapes(data_dir / zip_name):
        county = str(record.get("COUNTY_NAM") or record.get("CTYNAME") or record.get("COUNTY") or "").strip().upper()
        code = str(record.get("PRECINCT_I") or "").strip().upper()
        raw_name = str(record.get("PRECINCT_N") or "").strip()
        if not county or not code or not raw_name:
            continue
        geom = shape(geometry)
        if geom.is_empty:
            continue
        if not geom.is_valid:
            geom = geom.buffer(0)
        geom = geom.simplify(0.00003, preserve_topology=True)
        key = (county, code)
        if key in shape_index:
            # A few source shapefiles store disconnected pieces of one voting
            # precinct as separate records. Keep one result and one CVAP key.
            index = shape_index[key]
            merged = shape(polygons[index]["geometry"]).union(geom)
            polygons[index]["geometry"] = mapping(merged)
            point = merged.representative_point()
            centroids[index]["geometry"] = {"type": "Point", "coordinates": [point.x, point.y]}
            continue
        point = geom.representative_point()
        geoid = f"{year}-{len(polygons)}"
        name = friendly_name(prior_names.get(county, {}).get(code) or raw_name, code)
        props = {"id": geoid, "county_nam": county.title(), "county_norm": county,
                 "prec_id": code, "precinct_name": f"{county.title()} - {code}",
                 "precinct_norm": f"{county} - {code}", "precinct_full_name": name}
        polygons.append({"type": "Feature", "properties": props, "geometry": mapping(geom)})
        shape_index[key] = len(polygons) - 1
        raw_names.append(raw_name)
        centroids.append({"type": "Feature", "properties": {**props, "has_polygon": True},
                          "geometry": {"type": "Point", "coordinates": [point.x, point.y]}})

    votes = defaultdict(dict)
    with results_csv.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["office"] == probe_office:
                votes[row["county"].strip().upper()][row["precinct"].strip()] = None
    # Match against the boundary's own source name; friendly labels come afterward.
    for feature, raw_name in zip(polygons, raw_names):
        feature["properties"]["precinct_full_name"] = raw_name
    aliases = {}
    with (data_dir / "precinct_historical_aliases.csv").open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if int(row["year"]) == year:
                aliases[(row["county"].strip().upper(), row["precinct_id"].strip().upper())] = row["source_name"].strip()
    code_names = {(county, code): name for county, names in prior_names.items() for code, name in names.items()}
    matches, audit = match_precincts(polygons, votes, aliases, code_names)
    for feature in polygons:
        props = feature["properties"]
        props["precinct_full_name"] = friendly_name(prior_names.get(props["county_norm"], {}).get(props["prec_id"]) or props["precinct_full_name"], props["prec_id"])

    by_source = {(feature["properties"]["county_norm"], matches[feature["properties"]["id"]][0]): feature["properties"]["id"]
                 for feature in polygons if feature["properties"]["id"] in matches}
    audit_by_shape = {(row["county"], row["precinct_id"]): row for row in audit}
    shape_by_key = {(feature["properties"]["county_norm"], feature["properties"]["prec_id"]): feature["properties"]["id"]
                    for feature in polygons}
    with (data_dir / "precinct_historical_split_aliases.csv").open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if int(row["year"]) != year:
                continue
            county, code = row["county"].strip().upper(), row["precinct_id"].strip().upper()
            geoid = shape_by_key[(county, code)]
            source_names = [name.strip() for name in row["source_names"].split("|")]
            if not source_names or any(name not in votes[county] for name in source_names):
                raise ValueError(f"Split alias source missing: {year} {county} {code}")
            for source_name in source_names:
                prior_geoid = by_source.get((county, source_name))
                if prior_geoid and prior_geoid != geoid:
                    raise ValueError(f"Split alias source already assigned: {year} {county} {source_name}")
                by_source[(county, source_name)] = geoid
            audit_by_shape[(county, code)]["source_name"] = "; ".join(source_names)
            audit_by_shape[(county, code)]["method"] = "reviewed_split_alias"
    # Some 2016 exports split one precinct's votes into district-suffixed rows,
    # e.g. "03 Browns Bridge-7" and "03 Browns Bridge-9". Both belong to one
    # boundary when the unsuffixed name identifies exactly one shape in county.
    by_name = defaultdict(list)
    for feature, raw_name in zip(polygons, raw_names):
        by_name[(feature["properties"]["county_norm"], match_name(raw_name))].append(feature["properties"]["id"])
    for county, source_names in votes.items():
        for source_name in source_names:
            if (county, source_name) in by_source:
                continue
            base = re.sub(r"\s*-\s*\d{1,3}\s*$", "", source_name)
            if base == source_name:
                continue
            targets = by_name.get((county, match_name(base)), [])
            if len(targets) != 1:
                continue
            geoid = targets[0]
            by_source[(county, source_name)] = geoid
            feature = next(item for item in polygons if item["properties"]["id"] == geoid)
            row = audit_by_shape[(county, feature["properties"]["prec_id"])]
            row["source_name"] = "; ".join(filter(None, [row["source_name"], source_name]))
            row["method"] = "district_split_name"
    manifest = json.loads((data_dir / f"derived_vtd20/{year}/contests/manifest.json").read_text(encoding="utf-8"))
    slugs = {(row["office"], str(row.get("district") or "")): row["slug"] for row in manifest["contests"]}
    if "President" in probe_office:
        slugs[(probe_office, "")] = "President"
    contests = defaultdict(lambda: defaultdict(lambda: {"dem_votes": 0, "rep_votes": 0, "other_votes": 0, "total_votes": 0}))
    party_by_candidate = {}
    with results_csv.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            key = (row["office"], row["district"], row["candidate"])
            column = classify_party(row["party"], row["candidate"], {}, key)
            if column != "other_votes":
                party_by_candidate[key] = column
    with results_csv.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            slug = slugs.get((row["office"], row["district"]))
            geoid = by_source.get((row["county"].strip().upper(), row["precinct"].strip()))
            if not slug or not geoid:
                continue
            key = (row["office"], row["district"], row["candidate"])
            column = classify_party(row["party"], row["candidate"], party_by_candidate, key)
            count = int(row["total_votes"] or 0)
            contests[slug][geoid][column] += count
            contests[slug][geoid]["total_votes"] += count

    def write(name, payload):
        (data_dir / name).write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")

    write(f"Voting_Precincts_{year}.geojson", {"type": "FeatureCollection", "features": polygons})
    write(f"precinct_centroids_{year}.geojson", {"type": "FeatureCollection", "features": centroids})
    write(f"precinct_contests_{year}.json", {"year": year, "level": f"precinct_{year}", "contests": contests})
    with (data_dir / f"precinct_{year}_match_audit.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["county", "precinct_id", "boundary_name", "source_name", "method"])
        writer.writeheader()
        writer.writerows(audit)
    print(year, len(polygons), "shapes,", sum(row["method"] != "unmatched" for row in audit),
          "matched,", dict(Counter(row["method"] for row in audit)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("years", nargs="*", type=int, choices=tuple(SOURCES), default=list(SOURCES))
    parser.add_argument("--data-dir", type=Path, default=Path("Data"))
    args = parser.parse_args()
    for year in args.years:
        build(year, args.data_dir)
