"""Build a 2024 precinct overlay from Georgia's 2024 precinct shapefile.

The source ZIP is published by the Georgia General Assembly. The election CSV is
the normalized Secretary of State precinct export already used by this project.
Only unique, auditable name/code matches are assigned election results.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import zipfile
from collections import Counter, defaultdict
from difflib import SequenceMatcher
from pathlib import Path

import shapefile
from shapely.geometry import mapping, shape


def compact(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def match_name(value: str) -> str:
    text = str(value or "").upper().strip()
    text = re.sub(r"\s*\([^)]*\)\s*$", "", text)
    for pattern, replacement in (
        (r"\bCNTY\b|\bCTY\b|\bCO\b", "COUNTY"),
        (r"\bCTR\b", "CENTER"),
        (r"\bCOMM\b", "COMMUNITY"),
        (r"\bSCH\b", "SCHOOL"),
        (r"\bSTA\b", "STATION"),
        (r"\bELEM\b", "ELEMENTARY"),
        (r"\bMID\b", "MIDDLE"),
        (r"\bRD\b", "ROAD"),
        (r"\bMT\b", "MOUNT"),
    ):
        text = re.sub(pattern, replacement, text)
    return compact(text)


def friendly_name(value: str) -> str:
    """Keep the latest official name, expanding only unambiguous abbreviations."""
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if re.fullmatch(r"[A-Z]{1,4}[0-9]{1,5}[A-Z0-9-]*", text, flags=re.I):
        return text.upper()
    expansions = {
        "CTY": "County", "CNTY": "County", "CTR": "Center",
        "COMM": "Community", "SCH": "School", "ELEM": "Elementary",
        "MID": "Middle", "RD": "Road", "MT": "Mount",
        "GOVT": "Government", "REC": "Recreation", "DEPT": "Department",
        "SR": "Senior", "UWG": "University of West Georgia",
        "STA": "Station", "EDU": "Education", "RM": "Room",
    }
    for abbreviation, expanded in expansions.items():
        text = re.sub(rf"\b{abbreviation}\b", expanded, text, flags=re.I)
    return text.title() if text.isupper() else text


def source_code(value: str) -> str:
    match = re.match(r"^\s*([0-9]+(?:-[0-9]+)?[A-Z]?|[A-Z]{1,3}[0-9]+[A-Z]?)\b", str(value).upper())
    return match_code(match.group(1)) if match else ""


def match_code(value: str) -> str:
    return re.sub(r"[0-9]+", lambda match: str(int(match.group())), compact(value))


def read_shapes(path: Path) -> list[tuple[dict, object]]:
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        def member(ext: str) -> io.BytesIO:
            return io.BytesIO(archive.read(next(name for name in names if name.lower().endswith(ext))))
        reader = shapefile.Reader(shp=member(".shp"), shx=member(".shx"), dbf=member(".dbf"))
        return [(item.record.as_dict(), item.shape.__geo_interface__) for item in reader.iterShapeRecords()]


def read_president_votes(path: Path) -> dict[str, dict[str, dict[str, int]]]:
    votes: dict[str, dict[str, dict[str, int]]] = defaultdict(lambda: defaultdict(lambda: {"dem_votes": 0, "rep_votes": 0, "other_votes": 0, "total_votes": 0}))
    with path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("office") != "President":
                continue
            county = str(row.get("county") or "").strip().upper()
            precinct = str(row.get("precinct") or "").strip()
            if not county or not precinct:
                continue
            party = str(row.get("party") or "").strip().upper()
            column = "dem_votes" if party.startswith("DEM") else "rep_votes" if party.startswith("REP") else "other_votes"
            count = int(row.get("total_votes") or 0)
            votes[county][precinct][column] += count
            votes[county][precinct]["total_votes"] += count
    return votes


def read_aliases(path: Path) -> dict[tuple[str, str], str]:
    aliases = {}
    with path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            key = (str(row["county"]).strip().upper(), str(row["precinct_id"]).strip().upper())
            if key in aliases:
                raise ValueError(f"Duplicate alias: {key}")
            aliases[key] = str(row["source_name"]).strip()
    return aliases


def match_precincts(features: list[dict], votes: dict, aliases: dict[tuple[str, str], str]) -> tuple[dict[str, tuple[str, str]], list[dict]]:
    by_county: dict[str, list[dict]] = defaultdict(list)
    for feature in features:
        by_county[feature["properties"]["county_norm"]].append(feature)
    matches: dict[str, tuple[str, str]] = {}
    audit: list[dict] = []
    for county, shapes in by_county.items():
        source_names = list(votes.get(county, {}))
        used: set[str] = set()
        name_index: dict[str, list[str]] = defaultdict(list)
        code_index: dict[str, list[str]] = defaultdict(list)
        for name in source_names:
            name_index[match_name(name)].append(name)
            code = source_code(name)
            if code:
                code_index[code].append(name)

        for feature in shapes:
            props = feature["properties"]
            geoid = props["id"]
            name = props["precinct_full_name"]
            code = match_code(props["prec_id"])
            candidates = name_index.get(match_name(name), [])
            method = "name"
            if len(candidates) != 1:
                candidates = code_index.get(code, [])
                method = "code"
            if len(candidates) == 1 and candidates[0] not in used:
                matches[geoid] = (candidates[0], method)
                used.add(candidates[0])

        for feature in shapes:
            props = feature["properties"]
            geoid = props["id"]
            alias = aliases.get((county, props["prec_id"]))
            if not alias:
                continue
            if alias not in source_names:
                raise ValueError(f"Alias source not found: {county} {props['prec_id']} -> {alias}")
            if geoid in matches and matches[geoid][0] != alias:
                raise ValueError(f"Alias conflicts with direct match: {county} {props['prec_id']}")
            if alias in used and matches.get(geoid, (None,))[0] != alias:
                raise ValueError(f"Alias source reused: {county} {alias}")
            matches[geoid] = (alias, "reviewed_alias")
            used.add(alias)

        if len(shapes) == 1 and len(source_names) == 1 and not matches.get(shapes[0]["properties"]["id"]):
            name = source_names[0]
            matches[shapes[0]["properties"]["id"]] = (name, "single_precinct_county")
            used.add(name)

        proposals = []
        for feature in shapes:
            props = feature["properties"]
            geoid = props["id"]
            if geoid in matches:
                continue
            options = sorted(
                ((SequenceMatcher(a=match_name(props["precinct_full_name"]), b=match_name(name)).ratio(), name) for name in source_names if name not in used),
                reverse=True,
            )
            if options and options[0][0] >= 0.86 and (len(options) == 1 or options[0][0] - options[1][0] >= 0.10):
                proposals.append((options[0][0], geoid, options[0][1]))
        for _, geoid, name in sorted(proposals, reverse=True):
            if geoid not in matches and name not in used:
                matches[geoid] = (name, "unique_name_similarity")
                used.add(name)

        remaining_shapes = [feature for feature in shapes if feature["properties"]["id"] not in matches]
        remaining_names = [name for name in source_names if name not in used]
        if (len(shapes) == len(source_names) and len(remaining_shapes) == len(remaining_names) == 1
                and SequenceMatcher(a=match_name(remaining_shapes[0]["properties"]["precinct_full_name"]),
                                    b=match_name(remaining_names[0])).ratio() >= 0.5):
            geoid = remaining_shapes[0]["properties"]["id"]
            matches[geoid] = (remaining_names[0], "single_remainder")
            used.add(remaining_names[0])

        for feature in shapes:
            props = feature["properties"]
            geoid = props["id"]
            source_name, method = matches.get(geoid, ("", "unmatched"))
            audit.append({"county": county, "precinct_id": props["prec_id"], "boundary_name": props["precinct_full_name"], "source_name": source_name, "method": method})
    return matches, audit


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shape-zip", type=Path, default=Path("Data/_external/gaprec_2024-website-shapefile.zip"))
    parser.add_argument("--results-csv", type=Path, default=Path("Data/20241105__ga__general__precinct-level.csv"))
    parser.add_argument("--aliases", type=Path, default=Path("Data/precinct_2024_aliases.csv"))
    parser.add_argument("--out-dir", type=Path, default=Path("Data"))
    args = parser.parse_args()

    polygons = []
    centroids = []
    skipped = 0
    for record, geometry in read_shapes(args.shape_zip):
        county = str(record.get("COUNTY") or "").strip().upper()
        fips = str(record.get("FIPS") or "").strip()
        code = str(record.get("PRECINCT_I") or "").strip().upper()
        name = str(record.get("PRECINCT_N") or "").strip()
        if not county or not fips or not code or not name:
            skipped += 1
            continue
        geoid = f"2024-{fips}-{code}"
        geom = shape(geometry)
        if geom.is_empty:
            skipped += 1
            continue
        if not geom.is_valid:
            geom = geom.buffer(0)
        geom = geom.simplify(0.00003, preserve_topology=True)
        point = geom.representative_point()
        props = {
            "id": geoid,
            "county_nam": county.title(),
            "county_norm": county,
            "prec_id": code,
            "precinct_name": f"{county.title()} - {code}",
            "precinct_norm": f"{county} - {code}",
            "precinct_full_name": name,
        }
        polygons.append({"type": "Feature", "properties": props, "geometry": mapping(geom)})
        centroids.append({"type": "Feature", "properties": {**props, "has_polygon": True}, "geometry": {"type": "Point", "coordinates": [point.x, point.y]}})

    votes = read_president_votes(args.results_csv)
    matches, audit = match_precincts(polygons, votes, read_aliases(args.aliases))
    results = {}
    for feature in polygons:
        geoid = feature["properties"]["id"]
        if geoid in matches:
            source_name, _ = matches[geoid]
            county = feature["properties"]["county_norm"]
            results[geoid] = votes[county][source_name]

    # The same geographic match applies to every contest in this election export.
    # Keep contest and district separate so split-district precinct rows do not mix.
    manifest_path = args.out_dir / "derived_vtd20/2024/contests/manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    slug_by_contest = {(row["office"], str(row.get("district") or "")): row["slug"] for row in manifest["contests"]}
    by_source = {(feature["properties"]["county_norm"], matches[feature["properties"]["id"]][0]): feature["properties"]["id"]
                 for feature in polygons if feature["properties"]["id"] in matches}
    contest_results = defaultdict(lambda: defaultdict(lambda: {"dem_votes": 0, "rep_votes": 0, "other_votes": 0, "total_votes": 0}))
    with args.results_csv.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            slug = slug_by_contest.get((row["office"], row["district"]))
            geoid = by_source.get((row["county"].strip().upper(), row["precinct"].strip()))
            if not slug or not geoid:
                continue
            party = row["party"].strip().upper()
            column = "dem_votes" if party.startswith("DEM") else "rep_votes" if party.startswith("REP") else "other_votes"
            count = int(row["total_votes"] or 0)
            contest_results[slug][geoid][column] += count
            contest_results[slug][geoid]["total_votes"] += count

    prior_names = json.loads((args.out_dir / "precinct_friendly_names.json").read_text(encoding="utf-8"))
    latest_names = defaultdict(dict, {county: dict(names) for county, names in prior_names["counties"].items()})
    for feature in polygons:
        props = feature["properties"]
        name = friendly_name(props["precinct_full_name"])
        prior = latest_names[props["county_norm"]].get(props["prec_id"], "")
        if compact(name) == compact(props["prec_id"]) and prior and compact(prior) != compact(props["prec_id"]):
            name = prior
        props["precinct_full_name"] = name
        latest_names[props["county_norm"]][props["prec_id"]] = name
    for feature in centroids:
        props = feature["properties"]
        props["precinct_full_name"] = latest_names[props["county_norm"]][props["prec_id"]]

    args.out_dir.mkdir(parents=True, exist_ok=True)
    def write_json(name: str, payload: dict) -> None:
        (args.out_dir / name).write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    write_json("Voting_Precincts_2024.geojson", {"type": "FeatureCollection", "features": polygons})
    write_json("precinct_centroids_2024.geojson", {"type": "FeatureCollection", "features": centroids})
    write_json("precinct_contests_2024.json", {"year": 2024, "level": "precinct_2024", "contests": contest_results})
    friendly_payload = {
        "version": 1,
        "generated_from": [
            "Data/precinct_friendly_names.json",
            "Data/_external/gaprec_2024-website-shapefile.zip",
        ],
        "counties": {
            county: {code: label for code, label in sorted(names.items())
                     if code.strip().casefold() != label.strip().casefold()}
            for county, names in sorted(latest_names.items())
            if any(code.strip().casefold() != label.strip().casefold() for code, label in names.items())
        },
    }
    (args.out_dir / "precinct_friendly_names_latest.json").write_text(
        json.dumps(friendly_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    with (args.out_dir / "precinct_2024_match_audit.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["county", "precinct_id", "boundary_name", "source_name", "method"])
        writer.writeheader()
        writer.writerows(audit)
    counts = Counter(row["method"] for row in audit)
    print(f"2024 precinct shapes: {len(polygons)}; skipped unlabeled shapes: {skipped}")
    print(f"Matched: {len(results)}; methods: {dict(counts)}")
    print(f"Mapped votes: {sum(row['total_votes'] for row in results.values())} / {sum(row['total_votes'] for county in votes.values() for row in county.values())}")


if __name__ == "__main__":
    main()
