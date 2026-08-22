"""
Aggregate VTD20-keyed precinct contest results to district lines.

Inputs:
  - Data/derived_vtd20/<year>/contests/manifest.json
  - Data/derived_vtd20/<year>/contests/vtd20/*.json

Preferred weighted mappings (CSV):
  - Data/crosswalks/precinct_to_cd118.csv
  - Data/crosswalks/precinct_to_cd119.csv (preferred for congressional overlays)
  - Data/crosswalks/precinct_to_2022_state_house.csv
  - Data/crosswalks/precinct_to_2024_state_house.csv
  - Data/crosswalks/precinct_to_2022_state_senate.csv
  - Data/crosswalks/precinct_to_2024_state_senate.csv

Fallback boundaries (when CSV crosswalks are missing):
  - Data/tl_2020_13_vtd20.geojson
  - Data/tl_2022_13_cd118.geojson
  - Data/tl_2022_13_sldl.geojson
  - Data/tl_2022_13_sldu.geojson

Outputs (statewide / top-ticket federal overlays only), by district-lines vintage:
  - Data/district_contests_2022/  (CD118 + 2022 SH/SS crosswalks)
  - Data/district_contests_2024/  (CD119 + 2024 SH/SS crosswalks)
  - <out-dir>/<scope>_<contest_type>_<year>.json
  - <out-dir>/manifest.json

Excluded from published overlays: District Attorney, PSC, US House seats,
State House/Senate seats, and other local contests. Seat-race derived files may
still be read as supplemental precinct->district fallbacks.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import math
import re
import zipfile
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

# Statewide / top-ticket federal offices only (district overlays).
# Exclude: DA, PSC, US House seats, State House/Senate seats, locals.
STATEWIDE_OVERLAY_CONTEST_TYPES = frozenset(
    {
        "president",
        "governor",
        "lieutenant_governor",
        "us_senate",
        "us_senate_special",
        "us_senate_runoff",
        "attorney_general",
        "secretary_of_state",
        "treasurer",
        "auditor",
        "labor_commissioner",
        "insurance_commissioner",
        "agriculture_commissioner",
        "superintendent",
    }
)


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
        if "RUNOFF" in o or "runoff" in s:
            return "us_senate_runoff"
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


def is_statewide_overlay_contest(office: str, district: str, slug: str) -> bool:
    """True for President / Governor / US Senate / statewide executives only."""
    if is_district_specific_office(office, district):
        return False
    contest_type = office_to_contest_type(office, district, slug)
    if contest_type not in STATEWIDE_OVERLAY_CONTEST_TYPES:
        return False
    # Belt-and-suspenders: never treat DA / solicitor / PSC as statewide overlays
    # even if naming variants confuse office_to_contest_type.
    o = re.sub(r"[^A-Z0-9 ]+", " ", (office or "").upper())
    o = re.sub(r"\s+", " ", o).strip()
    if "DISTRICT ATTORNEY" in o or re.search(r"\bDA\b", o) or "SOLICITOR" in o:
        return False
    if "PUBLIC SERVICE COMMISSION" in o:
        return False
    return True


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


def normalize_party_bucket(raw_party: str) -> str:
    token = re.sub(r"[^A-Z]", "", (raw_party or "").strip().upper())
    if token.startswith("I") and len(token) > 1:
        token = token[1:]
    if token.endswith("I") and len(token) > 1:
        token = token[:-1]
    if token in {"D", "DEM", "DEMOCRAT", "DEMOCRATIC"} or token.startswith("DEM") or "DEMOCRAT" in token:
        return "DEM"
    if token in {"R", "REP", "REPUBLICAN"} or token.startswith("REP") or "REPUBLICAN" in token:
        return "REP"
    return "OTH"


def clean_candidate_label(raw: str) -> str:
    name = re.sub(r"\s+", " ", (raw or "").strip())
    if not name:
        return ""
    # Drop trailing party tags sometimes present on winner labels: "Name (D)", "Name (R*)".
    name = re.sub(r"\s*\(\s*[A-Z]{1,3}\s*\*?\s*\)\s*$", "", name, flags=re.IGNORECASE)
    return name.strip()


def pick_top_candidate_name(vote_map: dict[str, float]) -> str:
    if not vote_map:
        return ""
    ranked = sorted(vote_map.items(), key=lambda kv: (-float(kv[1]), kv[0]))
    return clean_candidate_label(ranked[0][0]) if ranked else ""


def infer_party_candidates_from_results(results: dict[str, Any]) -> tuple[str, str]:
    """
    Infer statewide (or contest-wide) DEM/REP nominee labels from VTD/precinct rows.

    Preference order per row:
      1) explicit dem_candidate / rep_candidate fields
      2) winner_candidate when winner_party is DEM/REP
    Vote-weight by the corresponding party/total votes so the modal nominee wins.
    """
    dem_votes: dict[str, float] = defaultdict(float)
    rep_votes: dict[str, float] = defaultdict(float)

    if not isinstance(results, dict):
        return "", ""

    for row in results.values():
        if not isinstance(row, dict):
            continue
        dem_name = clean_candidate_label(str(row.get("dem_candidate") or ""))
        rep_name = clean_candidate_label(str(row.get("rep_candidate") or ""))
        dem_n = float(parse_vote_int(row.get("dem_votes")))
        rep_n = float(parse_vote_int(row.get("rep_votes")))
        if dem_name:
            dem_votes[dem_name] += max(1.0, dem_n)
        if rep_name:
            rep_votes[rep_name] += max(1.0, rep_n)

        if dem_name and rep_name:
            continue

        winner_party = normalize_party_bucket(str(row.get("winner_party") or ""))
        winner_name = clean_candidate_label(str(row.get("winner_candidate") or ""))
        if not winner_name:
            continue
        if winner_party == "DEM" and not dem_name:
            dem_votes[winner_name] += max(1.0, dem_n or float(parse_vote_int(row.get("winner_votes"))))
        elif winner_party == "REP" and not rep_name:
            rep_votes[winner_name] += max(1.0, rep_n or float(parse_vote_int(row.get("winner_votes"))))

    return pick_top_candidate_name(dem_votes), pick_top_candidate_name(rep_votes)


@dataclass
class GroupEntry:
    scope: str
    contest_type: str
    year: int
    office: str
    district: str
    slug: str
    path: Path


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_county_name(raw: str) -> str:
    value = re.sub(r"[^A-Z0-9]+", " ", (raw or "").strip().upper())
    value = re.sub(r"\s+", " ", value).strip()
    return re.sub(r"\s+COUNTY$", "", value).strip()


def load_county_name_to_fips(path: Path) -> dict[str, str]:
    payload = load_json(path)
    counties = payload.get("counties") or {}
    out: dict[str, str] = {}
    if not isinstance(counties, dict):
        return out
    for county_key, row in counties.items():
        if not isinstance(row, dict):
            continue
        county_name = normalize_county_name(
            str(row.get("county_norm") or row.get("county") or county_key or "")
        )
        county_fips = str(row.get("county_fips") or "").strip()
        if county_name and re.fullmatch(r"13\d{3}", county_fips):
            out[county_name] = county_fips
    return out


def load_canonical_county_totals(
    *,
    contests_dir: Path,
    contest_type: str,
    year: int,
    county_name_to_fips: dict[str, str],
) -> dict[str, dict[str, float]]:
    path = contests_dir / f"{contest_type}_{year}.json"
    if not path.exists():
        return {}
    payload = load_json(path)
    rows = payload.get("rows") or []
    if not isinstance(rows, list):
        return {}

    out: dict[str, dict[str, float]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        county_name = normalize_county_name(str(row.get("county") or ""))
        county_fips = county_name_to_fips.get(county_name, "")
        if not county_fips:
            continue
        dem = max(0, parse_vote_int(row.get("dem_votes")))
        rep = max(0, parse_vote_int(row.get("rep_votes")))
        total = max(0, parse_vote_int(row.get("total_votes")))
        other = max(0, parse_vote_int(row.get("other_votes")))
        if other == 0 and total > dem + rep:
            other = total - dem - rep
        out[county_fips] = {
            "dem_votes": float(dem),
            "rep_votes": float(rep),
            "other_votes": float(other),
            "total_votes": float(dem + rep + other),
        }
    return out


def load_vtd20_turnout_weights(path: Path) -> dict[str, float]:
    """Load VEST 2020 VTD turnout as a spatial fallback for wholly unmatched counties."""
    if not path.exists():
        return {}
    with zipfile.ZipFile(path) as zf:
        members = [name for name in zf.namelist() if name.lower().endswith(".csv")]
        if not members:
            return {}
        with zf.open(members[0]) as raw, io.TextIOWrapper(raw, encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            out: dict[str, float] = {}
            for row in reader:
                geoid = normalize_precinct_key(str(row.get("GEOID20") or ""))
                if not geoid:
                    continue
                turnout = 0.0
                for field in ("G20PREDBID", "G20PRERTRU", "G20PRELJOR"):
                    try:
                        turnout += max(0.0, float(row.get(field) or 0.0))
                    except (TypeError, ValueError):
                        pass
                if turnout > 0:
                    out[geoid] = turnout
            return out


def apportion_integer_total(values: dict[str, float], target_total: int) -> dict[str, int]:
    """Round non-negative district values while preserving an exact statewide total."""
    if not values:
        return {}
    cleaned = {key: max(0.0, float(value)) for key, value in values.items()}
    value_total = sum(cleaned.values())
    if value_total <= 0:
        keys = sorted(cleaned, key=lambda key: int(key) if key.isdigit() else key)
        base, remainder = divmod(max(0, int(target_total)), len(keys))
        return {key: base + (1 if idx < remainder else 0) for idx, key in enumerate(keys)}

    exact = {key: (value / value_total) * max(0, int(target_total)) for key, value in cleaned.items()}
    rounded = {key: int(math.floor(value)) for key, value in exact.items()}
    remainder = max(0, int(target_total)) - sum(rounded.values())
    order = sorted(
        exact,
        key=lambda key: (
            -(exact[key] - rounded[key]),
            int(key) if key.isdigit() else key,
        ),
    )
    for key in order[:remainder]:
        rounded[key] += 1
    return rounded


def reconcile_numeric_totals_to_counties(
    *,
    by_district: dict[str, dict[str, float]],
    by_county_district: dict[str, dict[str, dict[str, float]]],
    canonical_counties: dict[str, dict[str, float]],
    county_district_fallback_weights: dict[str, dict[str, float]],
) -> dict[str, Any]:
    """
    Scale each county's mapped district pieces to authoritative county totals.

    This restores votes from precincts that could not be joined to a VTD while
    retaining the mapped within-county district distribution for each party.
    """
    fields = ("dem_votes", "rep_votes", "other_votes")
    unresolved_counties: list[str] = []
    mapped_before = {field: 0.0 for field in fields}
    targets = {field: 0.0 for field in fields}

    for county_fips, target in canonical_counties.items():
        county_nodes = by_county_district.get(county_fips) or {}
        for field in fields:
            targets[field] += max(0.0, float(target.get(field, 0.0)))
            mapped_before[field] += sum(max(0.0, float(node.get(field, 0.0))) for node in county_nodes.values())
        if not county_nodes:
            fallback_districts = county_district_fallback_weights.get(county_fips) or {}
            if fallback_districts:
                county_nodes = {}
                by_county_district[county_fips] = county_nodes
                for district in fallback_districts:
                    county_nodes[district] = {
                        "total_votes": 0.0,
                        "dem_votes": 0.0,
                        "rep_votes": 0.0,
                        "other_votes": 0.0,
                    }
                    by_district.setdefault(
                        district,
                        {
                            "total_votes": 0.0,
                            "dem_votes": 0.0,
                            "rep_votes": 0.0,
                            "other_votes": 0.0,
                            "dem_cand_votes": defaultdict(float),
                            "rep_cand_votes": defaultdict(float),
                        },
                    )
            else:
                unresolved_counties.append(county_fips)
                continue

        configured_fallback = county_district_fallback_weights.get(county_fips) or {}
        fallback_weights = {
            district: max(
                0.0,
                float(configured_fallback.get(district, 0.0)),
                float(node.get("total_votes", 0.0)),
            )
            for district, node in county_nodes.items()
        }
        fallback_total = sum(fallback_weights.values())
        if fallback_total <= 0:
            fallback_weights = {district: 1.0 for district in county_nodes}
            fallback_total = float(len(fallback_weights))

        for field in fields:
            field_target = max(0.0, float(target.get(field, 0.0)))
            field_current = sum(max(0.0, float(node.get(field, 0.0))) for node in county_nodes.values())
            for district, county_node in county_nodes.items():
                old_value = max(0.0, float(county_node.get(field, 0.0)))
                if field_current > 0:
                    new_value = field_target * (old_value / field_current)
                else:
                    new_value = field_target * (fallback_weights[district] / fallback_total)
                county_node[field] = new_value
                district_node = by_district[district]
                district_node[field] = float(district_node.get(field, 0.0)) + (new_value - old_value)

        for district, county_node in county_nodes.items():
            old_total = max(0.0, float(county_node.get("total_votes", 0.0)))
            new_total = sum(float(county_node.get(field, 0.0)) for field in fields)
            county_node["total_votes"] = new_total
            district_node = by_district[district]
            district_node["total_votes"] = float(district_node.get("total_votes", 0.0)) + (new_total - old_total)

    return {
        "method": "county_constrained_party_scaling",
        "canonical_counties": len(canonical_counties),
        "reconciled_counties": len(canonical_counties) - len(unresolved_counties),
        "unresolved_county_fips": unresolved_counties,
        "mapped_before": {field: int(round(value)) for field, value in mapped_before.items()},
        "canonical_target": {field: int(round(value)) for field, value in targets.items()},
    }


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


def build_scope_year_supplemental_assignments(
    all_entries: list[GroupEntry],
) -> dict[tuple[str, int], dict[str, list[tuple[str, float]]]]:
    """
    Build fallback geoid->district assignments from district-specific contest files.

    This rescues precinct keys that are present in contest outputs but absent from
    the precinct->district crosswalk (for example newly named/split precinct codes).
    """
    by_scope_year: dict[tuple[str, int], dict[str, dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(int))
    )

    for e in all_entries:
        if not is_district_specific_office(e.office, e.district):
            continue
        expected_scope = scope_for_district_office(e.office)
        if not expected_scope or expected_scope != e.scope:
            continue

        district_num = normalize_district_number(e.district)
        if not district_num:
            continue

        payload = load_json(e.path)
        results = payload.get("results") or {}
        if not isinstance(results, dict):
            continue

        bucket = by_scope_year[(e.scope, e.year)]
        for geoid, row in results.items():
            geoid_key = normalize_precinct_key(str(geoid))
            if not geoid_key or not isinstance(row, dict):
                continue
            # Prefer higher-vote observations when a key appears multiple times.
            votes = max(1, parse_vote_int(row.get("total_votes")))
            bucket[geoid_key][district_num] += votes

    out: dict[tuple[str, int], dict[str, list[tuple[str, float]]]] = {}
    for scope_year, geoid_map in by_scope_year.items():
        mapped: dict[str, list[tuple[str, float]]] = {}
        for geoid_key, district_votes in geoid_map.items():
            if not district_votes:
                continue
            winner = sorted(
                district_votes.items(),
                key=lambda item: (-item[1], int(item[0]) if item[0].isdigit() else item[0]),
            )[0][0]
            mapped[geoid_key] = [(winner, 1.0)]
        out[scope_year] = mapped
    return out


def aggregate_group(
    entries: list[GroupEntry],
    geoid_to_district: dict[str, list[tuple[str, float]]],
    supplemental_assignments: dict[str, list[tuple[str, float]]] | None = None,
    canonical_counties: dict[str, dict[str, float]] | None = None,
    vtd20_turnout_weights: dict[str, float] | None = None,
) -> dict[str, Any]:
    by_district: dict[str, dict[str, float]] = {}
    by_county_district: dict[str, dict[str, dict[str, float]]] = defaultdict(dict)
    total_input_votes = 0
    matched_input_votes = 0
    input_files: list[str] = []
    contest_dem_votes: dict[str, float] = defaultdict(float)
    contest_rep_votes: dict[str, float] = defaultdict(float)

    county_district_fallback_weights: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for geoid_key, assignments in geoid_to_district.items():
        county_fips = geoid_key[:5] if re.match(r"^13\d{3}", geoid_key) else ""
        if not county_fips or not assignments:
            continue
        base_weight = max(0.0, float((vtd20_turnout_weights or {}).get(geoid_key, 1.0)))
        assignment_total = sum(max(0.0, float(weight)) for _, weight in assignments)
        if base_weight <= 0 or assignment_total <= 0:
            continue
        for district_num, weight in assignments:
            county_district_fallback_weights[county_fips][district_num] += (
                base_weight * max(0.0, float(weight)) / assignment_total
            )

    for e in entries:
        payload = load_json(e.path)
        input_files.append(str(e.path).replace("\\", "/"))
        results = payload.get("results") or {}
        if not isinstance(results, dict):
            continue

        # Contest-level labels (written by newer blockpath builds).
        top_dem = clean_candidate_label(str(payload.get("dem_candidate") or ""))
        top_rep = clean_candidate_label(str(payload.get("rep_candidate") or ""))
        if top_dem:
            contest_dem_votes[top_dem] += 1.0
        if top_rep:
            contest_rep_votes[top_rep] += 1.0
        inferred_dem, inferred_rep = infer_party_candidates_from_results(results)
        if inferred_dem:
            contest_dem_votes[inferred_dem] += 1.0
        if inferred_rep:
            contest_rep_votes[inferred_rep] += 1.0

        entry_scope = scope_for_district_office(e.office)
        entry_district = normalize_district_number(e.district)
        direct_entry_fallback_ok = bool(entry_scope and entry_scope == e.scope and entry_district)

        for geoid, row in results.items():
            geoid_key = normalize_precinct_key(str(geoid))
            if not geoid_key or not isinstance(row, dict):
                continue

            total_votes = parse_vote_int(row.get("total_votes"))
            dem_votes = parse_vote_int(row.get("dem_votes"))
            rep_votes = parse_vote_int(row.get("rep_votes"))
            other_votes = parse_vote_int(row.get("other_votes"))
            total_input_votes += total_votes

            # Always reallocate through the target crosswalk so all years/offices
            # are expressed on the configured baseline district lines.
            assignments = geoid_to_district.get(geoid_key, [])
            if not assignments and supplemental_assignments:
                assignments = supplemental_assignments.get(geoid_key, [])
            if not assignments and direct_entry_fallback_ok:
                assignments = [(entry_district, 1.0)]
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
                        "dem_cand_votes": defaultdict(float),
                        "rep_cand_votes": defaultdict(float),
                    },
                )
                node["total_votes"] += total_votes * w
                node["dem_votes"] += dem_votes * w
                node["rep_votes"] += rep_votes * w
                node["other_votes"] += other_votes * w

                county_fips = geoid_key[:5] if re.match(r"^13\d{3}", geoid_key) else ""
                if county_fips:
                    county_node = by_county_district[county_fips].setdefault(
                        district_num,
                        {
                            "total_votes": 0.0,
                            "dem_votes": 0.0,
                            "rep_votes": 0.0,
                            "other_votes": 0.0,
                        },
                    )
                    county_node["total_votes"] += total_votes * w
                    county_node["dem_votes"] += dem_votes * w
                    county_node["rep_votes"] += rep_votes * w
                    county_node["other_votes"] += other_votes * w

                dem_name = clean_candidate_label(str(row.get("dem_candidate") or ""))
                rep_name = clean_candidate_label(str(row.get("rep_candidate") or ""))
                if not dem_name or not rep_name:
                    winner_party = normalize_party_bucket(str(row.get("winner_party") or ""))
                    winner_name = clean_candidate_label(str(row.get("winner_candidate") or ""))
                    if winner_name and winner_party == "DEM" and not dem_name:
                        dem_name = winner_name
                    elif winner_name and winner_party == "REP" and not rep_name:
                        rep_name = winner_name
                if dem_name:
                    node["dem_cand_votes"][dem_name] += max(w, dem_votes * w)
                if rep_name:
                    node["rep_cand_votes"][rep_name] += max(w, rep_votes * w)

    reconciliation: dict[str, Any] | None = None
    if canonical_counties:
        reconciliation = reconcile_numeric_totals_to_counties(
            by_district=by_district,
            by_county_district=by_county_district,
            canonical_counties=canonical_counties,
            county_district_fallback_weights=county_district_fallback_weights,
        )

    contest_dem = pick_top_candidate_name(contest_dem_votes)
    contest_rep = pick_top_candidate_name(contest_rep_votes)

    rounded_fields: dict[str, dict[str, int]] = {}
    if reconciliation and not reconciliation.get("unresolved_county_fips"):
        for field in ("dem_votes", "rep_votes", "other_votes"):
            field_target = int((reconciliation.get("canonical_target") or {}).get(field, 0))
            rounded_fields[field] = apportion_integer_total(
                {district: float(row.get(field, 0.0)) for district, row in by_district.items()},
                field_target,
            )

    finalized: dict[str, dict[str, Any]] = {}
    for district_num, row in by_district.items():
        if rounded_fields:
            dem = rounded_fields["dem_votes"].get(district_num, 0)
            rep = rounded_fields["rep_votes"].get(district_num, 0)
            other = rounded_fields["other_votes"].get(district_num, 0)
            total = dem + rep + other
        else:
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

        dem_candidate = pick_top_candidate_name(row.get("dem_cand_votes") or {}) or contest_dem
        rep_candidate = pick_top_candidate_name(row.get("rep_cand_votes") or {}) or contest_rep

        finalized[district_num] = {
            "total_votes": total,
            "dem_votes": dem,
            "rep_votes": rep,
            "other_votes": other,
            "dem_candidate": dem_candidate,
            "rep_candidate": rep_candidate,
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
        "reconciliation": reconciliation,
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
            # Keep seat races only for supplemental precinct->district fallbacks;
            # they are not published as district_contests overlays.
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
                )
            )
        elif is_statewide_overlay_contest(office, district, slug):
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
                    )
                )
    return out


def select_scope_crosswalk(
    scope: str,
    *,
    lines_year: int,
    crosswalk_maps: dict[str, dict[str, list[tuple[str, float]]]],
) -> dict[str, list[tuple[str, float]]]:
    if lines_year not in (2022, 2024):
        raise ValueError(f"Unsupported lines_year: {lines_year}")
    if scope == "congressional":
        if lines_year == 2024:
            return crosswalk_maps.get("congressional_2024") or crosswalk_maps["congressional"]
        return crosswalk_maps["congressional"]
    if scope == "state_house":
        return crosswalk_maps[f"state_house_{lines_year}"]
    if scope == "state_senate":
        return crosswalk_maps[f"state_senate_{lines_year}"]
    raise ValueError(f"Unknown scope: {scope}")


def default_out_dir_for_lines_year(lines_year: int) -> Path:
    return Path(f"Data/district_contests_{lines_year}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--derived-base", type=Path, default=Path("Data/derived_vtd20"))
    ap.add_argument(
        "--lines-year",
        type=int,
        choices=[2022, 2024],
        default=2024,
        help="District-lines vintage for crosswalks and default out dir (2022 or 2024).",
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Output directory (default: Data/district_contests_<lines-year>).",
    )
    ap.add_argument(
        "--years",
        default="",
        help="Optional comma-separated years, e.g. 2014,2016,2018; preserves other manifest entries.",
    )
    ap.add_argument(
        "--reconcile-county-totals",
        action="store_true",
        help=(
            "Scale mapped district pieces within each county to the authoritative "
            "Data/contests county totals. Restores votes from unmatched precincts."
        ),
    )
    ap.add_argument("--county-contests-dir", type=Path, default=Path("Data/contests"))
    ap.add_argument(
        "--county-demographics",
        type=Path,
        default=Path("Data/ga_county_demographics.json"),
        help="County-name to FIPS lookup used by --reconcile-county-totals.",
    )
    ap.add_argument(
        "--vtd20-turnout-weights",
        type=Path,
        default=Path("Data/ga_2020_2020_vtd.zip"),
        help="VEST VTD20 turnout weights for counties with no matched precincts.",
    )

    ap.add_argument("--crosswalk-cd", type=Path, default=Path("Data/crosswalks/precinct_to_cd118.csv"))
    ap.add_argument("--crosswalk-cd119", type=Path, default=Path("Data/crosswalks/precinct_to_cd119.csv"))
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
    lines_year = int(args.lines_year)
    out_dir = args.out_dir if args.out_dir is not None else default_out_dir_for_lines_year(lines_year)

    if not args.derived_base.exists():
        raise SystemExit(f"Missing derived base: {args.derived_base}")

    project_root = Path.cwd()
    county_name_to_fips: dict[str, str] = {}
    vtd20_turnout_weights: dict[str, float] = {}
    if args.reconcile_county_totals:
        if not args.county_contests_dir.exists():
            raise SystemExit(f"Missing county contests directory: {args.county_contests_dir}")
        if not args.county_demographics.exists():
            raise SystemExit(f"Missing county/FIPS lookup: {args.county_demographics}")
        county_name_to_fips = load_county_name_to_fips(args.county_demographics)
        if not county_name_to_fips:
            raise SystemExit(f"No county/FIPS mappings found in {args.county_demographics}")
        print(f"County-total reconciliation enabled ({len(county_name_to_fips)} county/FIPS mappings)")
        if args.vtd20_turnout_weights.exists():
            vtd20_turnout_weights = load_vtd20_turnout_weights(args.vtd20_turnout_weights)
            print(f"Loaded {len(vtd20_turnout_weights)} VTD20 turnout fallback weights")
        else:
            print(f"WARNING: missing VTD20 turnout fallback weights: {args.vtd20_turnout_weights}")
    years_filter: set[int] = set()
    if args.years.strip():
        for token in args.years.split(","):
            token = token.strip()
            if not token:
                continue
            years_filter.add(int(token))

    print(f"District lines year: {lines_year}")
    print(f"Output directory: {out_dir}")
    print("Loading precinct->district weighted maps...")
    crosswalk_maps: dict[str, dict[str, list[tuple[str, float]]]] = {}

    if args.crosswalk_cd.exists():
        crosswalk_maps["congressional"] = load_weighted_crosswalk(args.crosswalk_cd)
        print(f"  congressional CSV (CD118): {len(crosswalk_maps['congressional'])} precincts")
    else:
        crosswalk_maps["congressional"] = build_weighted_map_from_geometry(
            vtd20_geojson=args.vtd20_geojson,
            district_geojson=args.cd_geojson,
            district_join_field=SCOPE_TO_JOIN_FIELD["congressional"],
        )
        print(f"  congressional geometry fallback: {len(crosswalk_maps['congressional'])} precincts")

    if args.crosswalk_cd119.exists():
        crosswalk_maps["congressional_2024"] = load_weighted_crosswalk(args.crosswalk_cd119)
        print(f"  congressional_2024 CSV (CD119): {len(crosswalk_maps['congressional_2024'])} precincts")
    else:
        print("  congressional_2024 CSV missing -> will use CD118 for congressional overlays")

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

    args.out_dir = out_dir
    supplemental_by_scope_year = build_scope_year_supplemental_assignments(all_entries)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    manifest_files: list[dict[str, Any]] = []
    existing_manifest_files: list[dict[str, Any]] = []
    existing_manifest: dict[str, Any] = {}
    if args.years:
        existing_manifest_path = args.out_dir / "manifest.json"
        if existing_manifest_path.exists():
            try:
                existing_manifest = load_json(existing_manifest_path)
                existing_manifest_files = list((existing_manifest.get("files") or []))
            except (OSError, json.JSONDecodeError):
                existing_manifest = {}
                existing_manifest_files = []

    for (scope, contest_type, year), entries in sorted(grouped.items(), key=lambda x: (x[0][0], x[0][1], x[0][2])):
        # Publish statewide/top-ticket overlays only (not seat races, DA, PSC, etc.).
        # Seat types (state_house / state_senate / us_house) are intentionally absent from
        # STATEWIDE_OVERLAY_CONTEST_TYPES — never publish member elections as overlays.
        if contest_type not in STATEWIDE_OVERLAY_CONTEST_TYPES:
            continue

        geoid_to_district = select_scope_crosswalk(
            scope, lines_year=lines_year, crosswalk_maps=crosswalk_maps
        )
        supplemental = supplemental_by_scope_year.get((scope, year), {})
        canonical_counties: dict[str, dict[str, float]] = {}
        if args.reconcile_county_totals:
            canonical_counties = load_canonical_county_totals(
                contests_dir=args.county_contests_dir,
                contest_type=contest_type,
                year=year,
                county_name_to_fips=county_name_to_fips,
            )
            if not canonical_counties:
                print(f"  WARNING: no canonical county slice for {contest_type} {year}; reconciliation skipped")
        agg = aggregate_group(
            entries,
            geoid_to_district,
            supplemental,
            canonical_counties=canonical_counties,
            vtd20_turnout_weights=vtd20_turnout_weights,
        )
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
                "district_lines_year": lines_year,
                "source": (
                    "derived_vtd20_county_reconciled"
                    if agg.get("reconciliation")
                    else "derived_vtd20"
                ),
                "generated_at_utc": dt.datetime.now(dt.UTC).isoformat(),
                "match_coverage_pct": agg["match_coverage_pct"],
                "total_input_votes": agg["total_input_votes"],
                "matched_input_votes": agg["matched_input_votes"],
                "input_files": agg["input_files"],
                "county_reconciliation": agg.get("reconciliation"),
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
                "county_reconciled": bool(agg.get("reconciliation")),
            }
        )

    if years_filter:
        replacements = {
            (x["scope"], x["contest_type"], int(x["year"])): x
            for x in manifest_files
        }
        merged_manifest_files: list[dict[str, Any]] = []
        for existing in existing_manifest_files:
            key = (
                existing.get("scope"),
                existing.get("contest_type"),
                int(existing.get("year") or 0),
            )
            merged_manifest_files.append(replacements.pop(key, existing))
        merged_manifest_files.extend(
            sorted(replacements.values(), key=lambda x: (x["scope"], x["contest_type"], x["year"]))
        )
        manifest_files = merged_manifest_files
    else:
        manifest_files = sorted(manifest_files, key=lambda x: (x["scope"], x["contest_type"], x["year"]))

    manifest = {
        "source": existing_manifest.get("source") or str(args.derived_base).replace("\\", "/"),
        "district_lines_year": lines_year,
        "generated_at_utc": dt.datetime.now(dt.UTC).isoformat(),
        "files": manifest_files,
    }
    manifest_path = args.out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {manifest_path} ({len(manifest_files)} files)")


if __name__ == "__main__":
    main()
