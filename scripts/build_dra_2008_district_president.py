"""Publish DRA's 2008 Georgia presidential VTD results as district overlays.

The DRA 2020-VTD Georgia release stores the contest in each feature's
``properties.datasets.E_08_PRES`` object.  Those VTD keys align directly with
the project's precinct-to-district crosswalks, so this script writes a durable
derived-VTD input and publishes slices for both current district-line vintages.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import zipfile
from pathlib import Path

# Keep the aggregation rules and output shape identical to the regular
# district-contest builder.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_district_contests_from_derived_vtd20 as district_builder


SOURCE_LABEL = "DRA 2020_VTD Georgia v07 / E_08_PRES"
ZIP_MEMBER = "GA_2020_VD_tabblock.vtd.datasets.geojson"


def read_dra_results(zip_path: Path) -> dict[str, dict[str, object]]:
    with zipfile.ZipFile(zip_path) as archive:
        with archive.open(ZIP_MEMBER) as handle:
            feature_collection = json.load(handle)

    results: dict[str, dict[str, object]] = {}
    for feature in feature_collection.get("features", []):
        props = feature.get("properties") or {}
        geoid = district_builder.normalize_precinct_key(str(props.get("id") or ""))
        votes = (props.get("datasets") or {}).get("E_08_PRES") or {}
        if not geoid or not votes:
            continue
        dem = district_builder.parse_vote_int(votes.get("Dem"))
        rep = district_builder.parse_vote_int(votes.get("Rep"))
        total = district_builder.parse_vote_int(votes.get("Total"))
        # DRA's Total can include third-party votes. Preserve it rather than
        # deriving it from the two major-party columns.
        if total <= 0:
            total = dem + rep
        results[geoid] = {
            "total_votes": total,
            "dem_votes": dem,
            "rep_votes": rep,
            "other_votes": max(0, total - dem - rep),
            "dem_candidate": "Barack Obama",
            "rep_candidate": "John McCain",
        }
    return results


def write_derived_input(root: Path, results: dict[str, dict[str, object]]) -> Path:
    contest_dir = root / "Data" / "derived_vtd20" / "2008" / "contests"
    output = contest_dir / "vtd20" / "President.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(
            {
                "office": "President",
                "district": "",
                "level": "vtd20",
                "keys": "DRA 2020 VTD feature properties.id",
                "method": "dra_e_08_pres",
                "source": SOURCE_LABEL,
                "dem_candidate": "Barack Obama",
                "rep_candidate": "John McCain",
                "results": results,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    manifest = {
        "source": SOURCE_LABEL,
        "levels": ["vtd20"],
        "method": "dra_e_08_pres",
        "contests": [
            {
                "office": "President",
                "district": "",
                "slug": "President",
                "outputs": {"vtd20": {"path": str(output).replace("\\", "/"), "rows": len(results)}},
            }
        ],
    }
    (contest_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return output


def crosswalk_map(root: Path, scope: str, lines_year: int):
    crosswalk_names = {
        ("congressional", 2022): "precinct_to_cd118.csv",
        ("congressional", 2024): "precinct_to_cd119.csv",
        ("state_house", 2022): "precinct_to_2022_state_house.csv",
        ("state_house", 2024): "precinct_to_2024_state_house.csv",
        ("state_senate", 2022): "precinct_to_2022_state_senate.csv",
        ("state_senate", 2024): "precinct_to_2024_state_senate.csv",
    }
    path = root / "Data" / "crosswalks" / crosswalk_names[(scope, lines_year)]
    return district_builder.load_weighted_crosswalk(path)


def publish(root: Path, input_path: Path, lines_year: int, scope: str) -> dict[str, object]:
    entry = district_builder.GroupEntry(
        scope=scope,
        contest_type="president",
        year=2008,
        office="President",
        district="",
        slug="President",
        path=input_path,
    )
    agg = district_builder.aggregate_group([entry], crosswalk_map(root, scope, lines_year))
    results = agg["results"]
    total = sum(int(row["total_votes"]) for row in results.values())
    dem = sum(int(row["dem_votes"]) for row in results.values())
    rep = sum(int(row["rep_votes"]) for row in results.values())
    filename = f"{scope}_president_2008.json"
    out_dir = root / "Data" / f"district_contests_{lines_year}"
    payload = {
        "meta": {
            "scope": scope,
            "contest_type": "president",
            "year": 2008,
            "district_lines_year": lines_year,
            "source": SOURCE_LABEL,
            "generated_at_utc": dt.datetime.now(dt.UTC).isoformat(),
            "match_coverage_pct": agg["match_coverage_pct"],
            "total_input_votes": agg["total_input_votes"],
            "matched_input_votes": agg["matched_input_votes"],
            "input_files": agg["input_files"],
        },
        "general": {"results": results},
    }
    (out_dir / filename).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "scope": scope,
        "contest_type": "president",
        "year": 2008,
        "file": filename,
        "rows": len(results),
        "districts": len(results),
        "total_votes": total,
        "dem_total": dem,
        "rep_total": rep,
        "other_total": max(0, total - dem - rep),
        "major_party_contested": True,
        "match_coverage_pct": float(agg["match_coverage_pct"]),
    }


def update_manifest(root: Path, lines_year: int, new_entries: list[dict[str, object]]) -> None:
    path = root / "Data" / f"district_contests_{lines_year}" / "manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    files = [
        entry for entry in manifest.get("files", [])
        if not (entry.get("contest_type") == "president" and entry.get("year") == 2008)
    ]
    # Preserve the current manifest ordering. It intentionally reflects the
    # published inventory, which can include files staged independently of a
    # full regeneration.
    files.extend(new_entries)
    manifest["files"] = files
    manifest["generated_at_utc"] = dt.datetime.now(dt.UTC).isoformat()
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dra-zip", type=Path, default=Path("Data/dra_cache/Geojson_GA.v07.zip"))
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    root = args.root.resolve()
    results = read_dra_results(args.dra_zip)
    if len(results) < 2000:
        raise SystemExit(f"Expected about 2,700 DRA VTDs; found only {len(results)}")
    input_path = write_derived_input(root, results)
    for lines_year in (2022, 2024):
        entries = [publish(root, input_path, lines_year, scope) for scope in ("congressional", "state_house", "state_senate")]
        update_manifest(root, lines_year, entries)
        print(f"Published 2008 President for {lines_year} district lines.")


if __name__ == "__main__":
    main()
