"""Join the January 2021 Senate runoffs to the 2020 precinct boundaries."""

import csv
import json
from collections import Counter, defaultdict
from pathlib import Path

from build_2024_precinct_overlay import match_name, match_precincts


def build(data_dir: Path) -> None:
    geometry = json.loads((data_dir / "Voting_Precincts_2020.geojson").read_text(encoding="utf-8"))
    features = geometry["features"]
    results_path = data_dir / "20210105__ga__runoff__official__precinct.csv"
    votes = defaultdict(dict)
    with results_path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["office"] == "U.S. Senate":
                votes[row["county"].strip().upper()][row["precinct"].strip()] = None
    code_names = {
        (county, code): name
        for county, names in json.loads((data_dir / "precinct_friendly_names_latest.json").read_text(encoding="utf-8"))["counties"].items()
        for code, name in names.items()
    }
    matches, audit = match_precincts(features, votes, {}, code_names)
    shape_counts = Counter((feature["properties"]["county_norm"], feature["properties"]["prec_id"].upper()) for feature in features)
    assigned = {(feature["properties"]["county_norm"], source_name) for feature in features
                if (source_name := matches.get(feature["properties"]["id"], ("",))[0])}
    audit_by_shape = {(row["county"], row["precinct_id"]): row for row in audit}
    for feature in features:
        props = feature["properties"]
        county, code, geoid = props["county_norm"], props["prec_id"].upper(), props["id"]
        row = audit_by_shape[(county, props["prec_id"])]
        if row["method"] != "unmatched" or shape_counts[(county, code)] != 1:
            continue
        source_name = next((name for name in votes[county] if name.upper() == code), "")
        if source_name and (county, source_name) not in assigned:
            matches[geoid] = (source_name, "exact_precinct_code")
            row["source_name"], row["method"] = source_name, "exact_precinct_code"
            assigned.add((county, source_name))
    with (data_dir / "precinct_2020_match_audit.csv").open(encoding="utf-8", newline="") as handle:
        prior_names = {(row["county"], row["precinct_id"]): row["source_name"] for row in csv.DictReader(handle)}
    for feature in features:
        props = feature["properties"]
        county, code, geoid = props["county_norm"], props["prec_id"], props["id"]
        row = audit_by_shape[(county, code)]
        if row["method"] != "unmatched":
            continue
        prior_name = prior_names.get((county, code), "")
        if not prior_name or ";" in prior_name:
            continue
        candidates = [name for name in votes[county] if match_name(name) == match_name(prior_name)
                      and (county, name) not in assigned]
        if len(candidates) == 1:
            source_name = candidates[0]
            matches[geoid] = (source_name, "prior_election_name")
            row["source_name"], row["method"] = source_name, "prior_election_name"
            assigned.add((county, source_name))
    by_source = {
        (feature["properties"]["county_norm"], matches[feature["properties"]["id"]][0]): feature["properties"]["id"]
        for feature in features if feature["properties"]["id"] in matches
    }
    manifest = json.loads((data_dir / "derived_vtd20/2021/contests/manifest.json").read_text(encoding="utf-8"))
    slugs = {(row["office"], str(row.get("district") or "")): row["slug"] for row in manifest["contests"]}
    contests = defaultdict(lambda: defaultdict(lambda: {"dem_votes": 0, "rep_votes": 0, "other_votes": 0, "total_votes": 0}))
    with results_path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            geoid = by_source.get((row["county"].strip().upper(), row["precinct"].strip()))
            slug = slugs.get((row["office"], row["district"]))
            if not geoid or not slug:
                continue
            party = row["party"].strip().upper()
            column = "dem_votes" if party.startswith("DEM") else "rep_votes" if party.startswith("REP") else "other_votes"
            count = int(row["total_votes"] or 0)
            contests[slug][geoid][column] += count
            contests[slug][geoid]["total_votes"] += count
    (data_dir / "precinct_contests_2021.json").write_text(
        json.dumps({"year": 2021, "level": "precinct_2020", "contests": contests}, separators=(",", ":")), encoding="utf-8"
    )
    with (data_dir / "precinct_2021_match_audit.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["county", "precinct_id", "boundary_name", "source_name", "method"])
        writer.writeheader()
        writer.writerows(audit)
    print("2021 runoffs on 2020 geometry:", sum(row["method"] != "unmatched" for row in audit), "/", len(audit),
          "matched", dict(Counter(row["method"] for row in audit)))


if __name__ == "__main__":
    build(Path("Data"))
