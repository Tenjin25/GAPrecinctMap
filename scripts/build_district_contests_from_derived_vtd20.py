"""
Aggregate VTD20-keyed precinct contest results to district lines.

Inputs:
  - Data/derived_vtd20/<year>/contests/manifest.json
  - Data/derived_vtd20/<year>/contests/vtd20/*.json

Preferred weighted mappings (CSV):
  - Data/crosswalks/precinct_to_cd118.csv
  - Data/crosswalks/precinct_to_2022_state_house.csv
  - Data/crosswalks/precinct_to_2024_state_house.csv
  - Data/crosswalks/precinct_to_2022_state_senate.csv
  - Data/crosswalks/precinct_to_2024_state_senate.csv

Fallback boundaries (when CSV crosswalks are missing):
  - Data/tl_2020_13_vtd20.geojson
  - Data/tl_2022_13_cd118.geojson
  - Data/tl_2022_13_sldl.geojson
  - Data/tl_2022_13_sldu.geojson

Outputs:
  - Data/district_contests/<scope>_<contest_type>_<year>.json
  - Data/district_contests/manifest.json
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import geopandas as gpd
except Exception:  # pragma: no cover - optional fallback dependency
    gpd = None


SCOPE_TO_JOIN_FIELD = {
    "congressional": "CD118FP",
    "state_house": "SLDLST",
    "state_senate": "SLDUST",
}


def normalize_district_number(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    m = re.search(r"\d+", s)
    if not m:
        return s.upper()
    n = int(m.group(0))
    return str(n)


def normalize_precinct_key(raw: str) -> str:
    return (raw or "").strip().upper()


def normalize_json_relpath(raw_path: str) -> str:
    raw = (raw_path or "").strip()
    if not raw:
        return ""
    return raw.replace("\\", "/").lstrip("./").lstrip("/")


def slugify_for_contest_type(value: str) -> str:
    s = (value or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "contest"


def office_to_contest_type(office: str, district: str, slug: str) -> str:
    o = re.sub(r"[^A-Z0-9 ]+", " ", (office or "").upper())
    o = re.sub(r"\s+", " ", o).strip()
    d = (district or "").strip()
    s = (slug or "").lower()

    if d:
        if ("U S HOUSE" in o) or ("U S REPRESENTATIVE" in o) or ("UNITED STATES REPRESENTATIVE" in o):
            return "us_house"
        if ("STATE HOUSE" in o) or ("STATE REPRESENTATIVE" in o):
            return "state_house"
        if "STATE SENATE" in o:
            return "state_senate"
        if "PUBLIC SERVICE COMMISSION" in o:
            dn = normalize_district_number(d)
            return f"public_service_commission_{dn}" if dn else "public_service_commission"

    if "PRESIDENT" in o:
        return "president"
    if "LIEUTENANT GOVERNOR" in o:
        return "lieutenant_governor"
    if ("U S SENATE" in o) or ("UNITED STATES SENATOR" in o):
        if "SPECIAL" in o or "special" in s:
            return "us_senate_special"
        return "us_senate"
    if "GOVERNOR" in o:
        return "governor"
    if "ATTORNEY GENERAL" in o:
        return "attorney_general"
    if "SECRETARY OF STATE" in o or "SECRETARY STATE" in o:
        return "secretary_of_state"
    if "TREASURER" in o:
        return "treasurer"
    if "AUDITOR" in o:
        return "auditor"
    if "LABOR" in o and "COMMISSIONER" in o:
        return "labor_commissioner"
    if "INSURANCE" in o and "COMMISSIONER" in o:
        return "insurance_commissioner"
    if "AGRICULTURE" in o and "COMMISSIONER" in o:
        return "agriculture_commissioner"
    if "SUPERINTENDENT" in o:
        return "superintendent"

    return slugify_for_contest_type(o)


def is_district_specific_office(office: str, district: str) -> bool:
    if not (district or "").strip():
        return False
    o = re.sub(r"[^A-Z0-9 ]+", " ", (office or "").upper())
    o = re.sub(r"\s+", " ", o).strip()
    return (
        ("U S HOUSE" in o)
        or ("U S REPRESENTATIVE" in o)
        or ("UNITED STATES REPRESENTATIVE" in o)
        or ("STATE HOUSE" in o)
        or ("STATE REPRESENTATIVE" in o)
        or ("STATE SENATE" in o)
    )


def scope_for_district_office(office: str) -> str | None:
    o = re.sub(r"[^A-Z0-9 ]+", " ", (office or "").upper())
    o = re.sub(r"\s+", " ", o).strip()
    if ("U S HOUSE" in o) or ("U S REPRESENTATIVE" in o) or ("UNITED STATES REPRESENTATIVE" in o):
        return "congressional"
    if ("STATE HOUSE" in o) or ("STATE REPRESENTATIVE" in o):
        return "state_house"
    if "STATE SENATE" in o:
        return "state_senate"
    return None


def parse_vote_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            return 0
        return int(round(value))
    s = str(value or "").strip().replace(",", "")
    if not s:
        return 0
    try:
        return int(s)
    except ValueError:
        try:
            f = float(s)
        except ValueError:
            return 0
        if not math.isfinite(f):
            return 0
        return int(round(f))


@dataclass
class GroupEntry:
    scope: str
    contest_type: str
    year: int
    office: str
    district: str
    slug: str
    path: Path
    district_specific: bool


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def resolve_manifest_output_path(*, raw_path: str, fallback: Path, project_root: Path) -> Path:
    rel = normalize_json_relpath(raw_path)
    candidates: list[Path] = []
    if rel:
        p = Path(rel)
        candidates.append(p)
        candidates.append(project_root / rel)
    candidates.append(fallback)
    for c in candidates:
        if c.exists():
            return c
    return fallback


def load_weighted_crosswalk(path: Path) -> dict[str, list[tuple[str, float]]]:
    by_precinct: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            precinct_key = normalize_precinct_key(
                str(row.get("precinct_key") or row.get("precinct") or row.get("vtd_name20") or "")
            )
            district_num = normalize_district_number(
                str(row.get("district_num") or row.get("district_code") or row.get("district") or "")
            )
            if not precinct_key or not district_num:
                continue

            weight_raw = row.get("area_weight")
            if weight_raw in (None, ""):
                weight_raw = row.get("weight")
            try:
                weight = float(weight_raw)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(weight) or weight <= 0:
                continue

            by_precinct[precinct_key][district_num] += weight

    out: dict[str, list[tuple[str, float]]] = {}
    for precinct_key, district_map in by_precinct.items():
        total = sum(w for w in district_map.values() if w > 0)
        if total <= 0:
            continue
        weights = [(d, w / total) for d, w in district_map.items() if w > 0]
        weights.sort(key=lambda item: (int(item[0]) if item[0].isdigit() else item[0]))
        out[precinct_key] = weights
    return out


def build_weighted_map_from_geometry(
    *,
    vtd20_geojson: Path,
    district_geojson: Path,
    district_join_field: str,
) -> dict[str, list[tuple[str, float]]]:
    if gpd is None:
        raise SystemExit(
            f"geopandas is required for geometry fallback but is not available. "
            f"Provide CSV crosswalks instead of {district_geojson}."
        )

    vtd = gpd.read_file(vtd20_geojson).to_crs("EPSG:4326")
    if "GEOID20" not in vtd.columns:
        raise SystemExit(f"Missing GEOID20 in {vtd20_geojson}")

    points = vtd[["GEOID20", "geometry"]].copy()
    points["geometry"] = points.geometry.representative_point()

    target = gpd.read_file(district_geojson).to_crs("EPSG:4326")
    if district_join_field not in target.columns:
        raise SystemExit(f"Missing {district_join_field} in {district_geojson}")
    target = target[[district_join_field, "geometry"]].copy()

    joined = gpd.sjoin(points, target, how="left", predicate="within")
    out: dict[str, list[tuple[str, float]]] = {}
    for _, row in joined.iterrows():
        geoid = normalize_precinct_key(str(row.get("GEOID20") or ""))
        district = normalize_district_number(str(row.get(district_join_field) or ""))
        if not geoid or not district:
            continue
        out[geoid] = [(district, 1.0)]
    return out


def aggregate_group(entries: list[GroupEntry], geoid_to_district: dict[str, list[tuple[str, float]]]) -> dict[str, Any]:
    by_district: dict[str, dict[str, float]] = {}
    total_input_votes = 0
    matched_input_votes = 0
    input_files: list[str] = []

    for e in entries:
        payload = load_json(e.path)
        input_files.append(str(e.path).replace("\\", "/"))
        results = payload.get("results") or {}
        if not isinstance(results, dict):
            continue

        fixed_district = normalize_district_number(e.district) if e.district_specific else ""

        for geoid, row in results.items():
            geoid_key = normalize_precinct_key(str(geoid))
            if not geoid_key or not isinstance(row, dict):
                continue

            total_votes = parse_vote_int(row.get("total_votes"))
            dem_votes = parse_vote_int(row.get("dem_votes"))
            rep_votes = parse_vote_int(row.get("rep_votes"))
            other_votes = parse_vote_int(row.get("other_votes"))
            total_input_votes += total_votes

            if fixed_district:
                assignments = [(fixed_district, 1.0)]
            else:
                assignments = geoid_to_district.get(geoid_key, [])
            if not assignments:
                continue

            weight_sum = sum(max(0.0, float(w)) for _, w in assignments)
            if weight_sum <= 0:
                continue

            matched_input_votes += total_votes
            for district_num, weight in assignments:
                w = max(0.0, float(weight)) / weight_sum
                if w <= 0:
                    continue
                node = by_district.setdefault(
                    district_num,
                    {
                        "total_votes": 0.0,
                        "dem_votes": 0.0,
                        "rep_votes": 0.0,
                        "other_votes": 0.0,
                    },
                )
                node["total_votes"] += total_votes * w
                node["dem_votes"] += dem_votes * w
                node["rep_votes"] += rep_votes * w
                node["other_votes"] += other_votes * w

    finalized: dict[str, dict[str, Any]] = {}
    for district_num, row in by_district.items():
        total = int(round(row.get("total_votes", 0.0)))
        dem = int(round(row.get("dem_votes", 0.0)))
        rep = int(round(row.get("rep_votes", 0.0)))
        other = int(round(row.get("other_votes", 0.0)))

        if total > 0:
            signed_margin_pct = ((rep - dem) / total) * 100.0
        else:
            signed_margin_pct = 0.0

        if rep > dem and rep >= other:
            winner = "Republican"
            winner_party = "REP"
        elif dem > rep and dem >= other:
            winner = "Democratic"
            winner_party = "DEM"
        elif other > rep and other > dem:
            winner = "Other"
            winner_party = "OTH"
        else:
            winner = "Tie"
            winner_party = "TIE"

        finalized[district_num] = {
            "total_votes": total,
            "dem_votes": dem,
            "rep_votes": rep,
            "other_votes": other,
            "dem_candidate": "",
            "rep_candidate": "",
            "winner": winner,
            "winner_party": winner_party,
            "margin_pct": signed_margin_pct,
        }

    coverage_pct = 0.0
    if total_input_votes > 0:
        coverage_pct = (matched_input_votes / total_input_votes) * 100.0

    sorted_results = dict(
        sorted(finalized.items(), key=lambda kv: (int(kv[0]) if kv[0].isdigit() else kv[0]))
    )
    uniq_inputs = sorted(set(input_files))
    return {
        "results": sorted_results,
        "total_input_votes": total_input_votes,
        "matched_input_votes": matched_input_votes,
        "match_coverage_pct": coverage_pct,
        "input_files": uniq_inputs,
    }


def build_groups_for_year(year_dir: Path, derived_base: Path, project_root: Path) -> list[GroupEntry]:
    manifest_path = year_dir / "contests" / "manifest.json"
    if not manifest_path.exists():
        return []

    year = int(year_dir.name)
    manifest = load_json(manifest_path)
    contests = manifest.get("contests") or []
    if not isinstance(contests, list):
        return []

    out: list[GroupEntry] = []
    for c in contests:
        if not isinstance(c, dict):
            continue

        office = str(c.get("office") or "").strip()
        district = str(c.get("district") or "").strip()
        slug = str(c.get("slug") or "").strip()
        raw_path = str((c.get("outputs") or {}).get("vtd20", {}).get("path") or "")
        fallback_path = year_dir / "contests" / "vtd20" / f"{slug}.json"
        path = resolve_manifest_output_path(raw_path=raw_path, fallback=fallback_path, project_root=project_root)
        if not path.exists():
            continue

        contest_type = office_to_contest_type(office, district, slug)
        district_specific = is_district_specific_office(office, district)

        if district_specific:
            scope = scope_for_district_office(office)
            if not scope:
                continue
            out.append(
                GroupEntry(
                    scope=scope,
                    contest_type=contest_type,
                    year=year,
                    office=office,
                    district=district,
                    slug=slug,
                    path=path,
                    district_specific=True,
                )
            )
        else:
            for scope in ("congressional", "state_house", "state_senate"):
                out.append(
                    GroupEntry(
                        scope=scope,
                        contest_type=contest_type,
                        year=year,
                        office=office,
                        district=district,
                        slug=slug,
                        path=path,
                        district_specific=False,
                    )
                )
    return out


def select_scope_crosswalk(
    scope: str,
    *,
    crosswalk_maps: dict[str, dict[str, list[tuple[str, float]]]],
) -> dict[str, list[tuple[str, float]]]:
    if scope == "congressional":
        # Lock congressional overlays to the current CD118 (2022 cycle) lines.
        return crosswalk_maps["congressional"]
    if scope == "state_house":
        # Lock legislative overlays to 2024 district plan.
        return crosswalk_maps["state_house_2024"]
    if scope == "state_senate":
        return crosswalk_maps["state_senate_2024"]
    raise ValueError(f"Unknown scope: {scope}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--derived-base", type=Path, default=Path("Data/derived_vtd20"))
    ap.add_argument("--out-dir", type=Path, default=Path("Data/district_contests"))
    ap.add_argument("--years", default="", help="Optional comma-separated years, e.g. 2014,2016,2018")

    ap.add_argument("--crosswalk-cd", type=Path, default=Path("Data/crosswalks/precinct_to_cd118.csv"))
    ap.add_argument(
        "--crosswalk-state-house-2022", type=Path, default=Path("Data/crosswalks/precinct_to_2022_state_house.csv")
    )
    ap.add_argument(
        "--crosswalk-state-house-2024", type=Path, default=Path("Data/crosswalks/precinct_to_2024_state_house.csv")
    )
    ap.add_argument(
        "--crosswalk-state-senate-2022",
        type=Path,
        default=Path("Data/crosswalks/precinct_to_2022_state_senate.csv"),
    )
    ap.add_argument(
        "--crosswalk-state-senate-2024",
        type=Path,
        default=Path("Data/crosswalks/precinct_to_2024_state_senate.csv"),
    )

    ap.add_argument("--vtd20-geojson", type=Path, default=Path("Data/tl_2020_13_vtd20.geojson"))
    ap.add_argument("--cd-geojson", type=Path, default=Path("Data/tl_2022_13_cd118.geojson"))
    ap.add_argument("--sldl-geojson", type=Path, default=Path("Data/tl_2022_13_sldl.geojson"))
    ap.add_argument("--sldu-geojson", type=Path, default=Path("Data/tl_2022_13_sldu.geojson"))
    args = ap.parse_args()

    if not args.derived_base.exists():
        raise SystemExit(f"Missing derived base: {args.derived_base}")

    project_root = Path.cwd()
    years_filter: set[int] = set()
    if args.years.strip():
        for token in args.years.split(","):
            token = token.strip()
            if not token:
                continue
            years_filter.add(int(token))

    print("Loading precinct->district weighted maps...")
    crosswalk_maps: dict[str, dict[str, list[tuple[str, float]]]] = {}

    if args.crosswalk_cd.exists():
        crosswalk_maps["congressional"] = load_weighted_crosswalk(args.crosswalk_cd)
        print(f"  congressional CSV: {len(crosswalk_maps['congressional'])} precincts")
    else:
        crosswalk_maps["congressional"] = build_weighted_map_from_geometry(
            vtd20_geojson=args.vtd20_geojson,
            district_geojson=args.cd_geojson,
            district_join_field=SCOPE_TO_JOIN_FIELD["congressional"],
        )
        print(f"  congressional geometry fallback: {len(crosswalk_maps['congressional'])} precincts")

    if args.crosswalk_state_house_2022.exists():
        crosswalk_maps["state_house_2022"] = load_weighted_crosswalk(args.crosswalk_state_house_2022)
        print(f"  state_house_2022 CSV: {len(crosswalk_maps['state_house_2022'])} precincts")
    else:
        crosswalk_maps["state_house_2022"] = build_weighted_map_from_geometry(
            vtd20_geojson=args.vtd20_geojson,
            district_geojson=args.sldl_geojson,
            district_join_field=SCOPE_TO_JOIN_FIELD["state_house"],
        )
        print(f"  state_house_2022 geometry fallback: {len(crosswalk_maps['state_house_2022'])} precincts")

    if args.crosswalk_state_house_2024.exists():
        crosswalk_maps["state_house_2024"] = load_weighted_crosswalk(args.crosswalk_state_house_2024)
        print(f"  state_house_2024 CSV: {len(crosswalk_maps['state_house_2024'])} precincts")
    else:
        crosswalk_maps["state_house_2024"] = crosswalk_maps["state_house_2022"]
        print("  state_house_2024 missing -> using state_house_2022 mapping")

    if args.crosswalk_state_senate_2022.exists():
        crosswalk_maps["state_senate_2022"] = load_weighted_crosswalk(args.crosswalk_state_senate_2022)
        print(f"  state_senate_2022 CSV: {len(crosswalk_maps['state_senate_2022'])} precincts")
    else:
        crosswalk_maps["state_senate_2022"] = build_weighted_map_from_geometry(
            vtd20_geojson=args.vtd20_geojson,
            district_geojson=args.sldu_geojson,
            district_join_field=SCOPE_TO_JOIN_FIELD["state_senate"],
        )
        print(f"  state_senate_2022 geometry fallback: {len(crosswalk_maps['state_senate_2022'])} precincts")

    if args.crosswalk_state_senate_2024.exists():
        crosswalk_maps["state_senate_2024"] = load_weighted_crosswalk(args.crosswalk_state_senate_2024)
        print(f"  state_senate_2024 CSV: {len(crosswalk_maps['state_senate_2024'])} precincts")
    else:
        crosswalk_maps["state_senate_2024"] = crosswalk_maps["state_senate_2022"]
        print("  state_senate_2024 missing -> using state_senate_2022 mapping")

    all_entries: list[GroupEntry] = []
    for year_dir in sorted(args.derived_base.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        year = int(year_dir.name)
        if years_filter and year not in years_filter:
            continue
        entries = build_groups_for_year(year_dir, args.derived_base, project_root)
        all_entries.extend(entries)

    grouped: dict[tuple[str, str, int], list[GroupEntry]] = defaultdict(list)
    for e in all_entries:
        grouped[(e.scope, e.contest_type, e.year)].append(e)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    manifest_files: list[dict[str, Any]] = []

    for (scope, contest_type, year), entries in sorted(grouped.items(), key=lambda x: (x[0][0], x[0][1], x[0][2])):
        geoid_to_district = select_scope_crosswalk(scope, crosswalk_maps=crosswalk_maps)
        agg = aggregate_group(entries, geoid_to_district)
        results = agg["results"]
        if not results:
            continue

        total_votes = sum(int(v.get("total_votes") or 0) for v in results.values())
        dem_total = sum(int(v.get("dem_votes") or 0) for v in results.values())
        rep_total = sum(int(v.get("rep_votes") or 0) for v in results.values())
        other_total = max(0, total_votes - dem_total - rep_total)

        out_name = f"{scope}_{contest_type}_{year}.json"
        out_path = args.out_dir / out_name
        payload = {
            "meta": {
                "scope": scope,
                "contest_type": contest_type,
                "year": year,
                "source": "derived_vtd20",
                "generated_at_utc": dt.datetime.now(dt.UTC).isoformat(),
                "match_coverage_pct": agg["match_coverage_pct"],
                "total_input_votes": agg["total_input_votes"],
                "matched_input_votes": agg["matched_input_votes"],
                "input_files": agg["input_files"],
            },
            "general": {"results": results},
        }
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Wrote {out_path} ({len(results)} districts, coverage {agg['match_coverage_pct']:.2f}%)")

        major_party_contested = dem_total > 0 and rep_total > 0
        manifest_files.append(
            {
                "scope": scope,
                "contest_type": contest_type,
                "year": year,
                "file": out_name,
                "rows": len(results),
                "districts": len(results),
                "total_votes": int(total_votes),
                "dem_total": int(dem_total),
                "rep_total": int(rep_total),
                "other_total": int(other_total),
                "major_party_contested": bool(major_party_contested),
                "match_coverage_pct": float(agg["match_coverage_pct"]),
            }
        )

    manifest = {
        "source": str(args.derived_base).replace("\\", "/"),
        "generated_at_utc": dt.datetime.now(dt.UTC).isoformat(),
        "files": sorted(manifest_files, key=lambda x: (x["scope"], x["contest_type"], x["year"])),
    }
    manifest_path = args.out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {manifest_path} ({len(manifest_files)} files)")


if __name__ == "__main__":
    main()
