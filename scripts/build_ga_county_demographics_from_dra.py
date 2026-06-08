import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def normalize_county_token(raw: str) -> str:
    return str(raw or "").strip().upper()


def numeric(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def rounded_pct(part: float, whole: float):
    if whole <= 0:
        return None
    return round((part / whole) * 100.0, 2)


def main():
    parser = argparse.ArgumentParser(description="Build Georgia county demographics JSON from DRA precinct datasets.")
    parser.add_argument("county_geojson", type=Path)
    parser.add_argument("dra_geojson", type=Path)
    parser.add_argument("output_json", type=Path)
    args = parser.parse_args()

    county_fc = json.loads(args.county_geojson.read_text(encoding="utf-8"))
    dra_fc = json.loads(args.dra_geojson.read_text(encoding="utf-8"))

    county_names_by_fips = {}
    for feature in county_fc.get("features", []):
        props = feature.get("properties", {}) or {}
        county_fips = str(props.get("GEOID20") or "").strip()
        county_name = str(props.get("NAME20") or "").strip()
        if county_fips and county_name:
            county_names_by_fips[county_fips] = county_name

    totals_by_county = {}
    for feature in dra_fc.get("features", []):
        props = feature.get("properties", {}) or {}
        precinct_id = str(props.get("id") or "").strip()
        if len(precinct_id) < 5:
            continue
        county_fips = precinct_id[:5]
        county_name = county_names_by_fips.get(county_fips)
        if not county_name:
            continue

        datasets = props.get("datasets", {}) or {}
        census = datasets.get("T_20_CENS", {}) or {}
        vap = datasets.get("V_20_VAP", {}) or {}

        bucket = totals_by_county.setdefault(
            county_fips,
            {
                "county": county_name,
                "county_norm": normalize_county_token(county_name),
                "county_fips": county_fips,
                "total_population": 0.0,
                "vap_18plus": 0.0,
                "white_pop": 0.0,
                "black_pop": 0.0,
                "native_pop": 0.0,
                "asian_pop": 0.0,
                "pacific_pop": 0.0,
                "hispanic_pop": 0.0,
            },
        )

        bucket["total_population"] += numeric(census.get("Total"))
        bucket["vap_18plus"] += numeric(vap.get("Total"))
        bucket["white_pop"] += numeric(census.get("White"))
        bucket["black_pop"] += numeric(census.get("Black"))
        bucket["native_pop"] += numeric(census.get("Native"))
        bucket["asian_pop"] += numeric(census.get("Asian"))
        bucket["pacific_pop"] += numeric(census.get("Pacific"))
        bucket["hispanic_pop"] += numeric(census.get("Hispanic"))

    counties = {}
    for county in totals_by_county.values():
        total_population = county["total_population"]
        county["total_population"] = round(total_population)
        county["vap_18plus"] = round(county["vap_18plus"])
        county["white_pop"] = round(county["white_pop"])
        county["black_pop"] = round(county["black_pop"])
        county["native_pop"] = round(county["native_pop"])
        county["asian_pop"] = round(county["asian_pop"])
        county["pacific_pop"] = round(county["pacific_pop"])
        county["hispanic_pop"] = round(county["hispanic_pop"])
        county["multiracial_pop_pct"] = None
        county["white_pop_pct"] = rounded_pct(county["white_pop"], total_population)
        county["black_pop_pct"] = rounded_pct(county["black_pop"], total_population)
        county["native_pop_pct"] = rounded_pct(county["native_pop"], total_population)
        county["asian_pop_pct"] = rounded_pct(county["asian_pop"], total_population)
        county["pacific_pop_pct"] = rounded_pct(county["pacific_pop"], total_population)
        county["hispanic_pop_pct"] = rounded_pct(county["hispanic_pop"], total_population)
        counties[county["county_norm"]] = county

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "description": "Aggregated from DRA Georgia VTD tabblock datasets included in this repository.",
            "population_race_source": "T_20_CENS (2020 Census total population by race/ethnicity).",
            "vap_source": "V_20_VAP (2020 voting-age population totals by race/ethnicity).",
        },
        "counties": dict(sorted(counties.items())),
    }

    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
