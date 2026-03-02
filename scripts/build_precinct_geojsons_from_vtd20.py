"""
Build precinct polygon + centroid GeoJSONs for this repo's precinct overlay.

This repo's map expects:
  - Data/Voting_Precincts.geojson (polygons)
  - Data/precinct_centroids.geojson (points)

Georgia does not ship one canonical precinct boundary file inside this repo. As a practical
stand-in, we use TIGER VTD20 geometry (Voting Districts) for Georgia and expose properties
that match index.html's precinct overlay expectations:

  - county_nam (Title Case)
  - county_norm (UPPER)
  - prec_id (VTD NAME20, trimmed)
  - precinct_name (e.g. "Appling - 1B")
  - precinct_norm (UPPER normalized "COUNTY - PREC_ID")
  - id (GEOID20)

Centroids are representative points (guaranteed inside polygons).

Inputs:
  - Data/tl_2020_13_vtd20.geojson (recommended; produced by scripts/prepare_tiger_2020_geojsons.py)

Outputs:
  - Data/Voting_Precincts.geojson
  - Data/precinct_centroids.geojson

Usage:
  py scripts/build_precinct_geojsons_from_vtd20.py
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path


def _norm_token(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 .\-]", "", (s or "").strip().upper())).strip()


def _title_county(county_norm: str) -> str:
    # Reasonable display form for tooltips ("De Kalb" isn't ideal, but acceptable).
    parts = (county_norm or "").strip().split()
    return " ".join(p[:1].upper() + p[1:].lower() for p in parts if p)

def _clean_precinct_label(name: str) -> str:
    """
    Try to make TIGER VTD NAME20 align better with election results precinct labels.

    This is intentionally conservative: mostly strips trailing parenthetical codes
    and normalizes common street-type words.
    """
    s = (name or "").strip()
    if not s:
        return ""

    # Drop trailing parenthetical suffixes like "(DUN)".
    s = re.sub(r"\s*\([^)]*\)\s*$", "", s).strip()

    # Normalize common street-type words to common abbreviations seen in results exports.
    # Operate in uppercase so joins are stable; display uses original-ish casing after.
    u = s.upper()
    rep: list[tuple[str, str]] = [
        (r"\bROAD\b", "RD"),
        (r"\bSTREET\b", "ST"),
        (r"\bAVENUE\b", "AVE"),
        (r"\bDRIVE\b", "DR"),
        (r"\bBOULEVARD\b", "BLVD"),
        (r"\bHIGHWAY\b", "HWY"),
        (r"\bMOUNTAIN\b", "MTN"),
        (r"\bELEMENTARY\b", "ELEM"),
        (r"\bMIDDLE\b", "MID"),
        (r"\bCOMMUNITY\s+CENTER\b", "COMM CENTER"),
    ]
    for pat, repl in rep:
        u = re.sub(pat, repl, u)
    u = re.sub(r"\s+", " ", u).strip()

    # Title-case for nicer display while keeping abbreviations legible.
    out_parts = []
    for token in u.split(" "):
        if token in {"RD", "ST", "AVE", "DR", "BLVD", "HWY", "MTN", "ELEM", "MID"}:
            out_parts.append(token.title())  # Rd, St...
        elif token in {"COMM", "CENTER"}:
            out_parts.append(token.title())
        elif re.fullmatch(r"\d{1,4}", token):
            out_parts.append(token)
        else:
            out_parts.append(token[:1].upper() + token[1:].lower() if token else token)
    return " ".join(out_parts).strip()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--vtd20",
        type=Path,
        default=Path("Data/tl_2020_13_vtd20.geojson"),
        help="Input VTD20 GeoJSON (EPSG:4326).",
    )
    ap.add_argument(
        "--out-polygons",
        type=Path,
        default=Path("Data/Voting_Precincts.geojson"),
        help="Output precinct polygon GeoJSON.",
    )
    ap.add_argument(
        "--out-centroids",
        type=Path,
        default=Path("Data/precinct_centroids.geojson"),
        help="Output precinct centroid GeoJSON.",
    )
    args = ap.parse_args()

    try:
        import geopandas as gpd
    except Exception as e:  # pragma: no cover
        raise SystemExit(f"geopandas is required: {e}")

    if not args.vtd20.exists():
        raise SystemExit(f"Missing input: {args.vtd20}")

    gdf = gpd.read_file(args.vtd20)
    if gdf.crs is None:
        # The prepared GeoJSONs are EPSG:4326, but be safe.
        gdf = gdf.set_crs("EPSG:4326")
    else:
        gdf = gdf.to_crs("EPSG:4326")

    required = {"NAME20", "GEOID20"}
    missing = required - set(gdf.columns)
    if missing:
        raise SystemExit(f"Input is missing required columns: {sorted(missing)}")

    # county_norm is present when using scripts/prepare_tiger_2020_geojsons.py. If absent, infer from COUNTYFP20 only
    # is not possible without county lookup, so require it.
    if "county_norm" not in gdf.columns:
        raise SystemExit("Input must include 'county_norm'. Run scripts/prepare_tiger_2020_geojsons.py first.")

    def build_row(row) -> dict[str, object]:
        county_norm = _norm_token(str(row.get("county_norm") or ""))
        prec_id_raw = str(row.get("NAME20") or "").strip()
        prec_id = _clean_precinct_label(prec_id_raw) or re.sub(r"\s+", " ", prec_id_raw).strip()
        county_nam = _title_county(county_norm)
        precinct_name = f"{county_nam} - {prec_id}" if (county_nam and prec_id) else (county_nam or prec_id)
        precinct_norm = _norm_token(f"{county_nam} - {prec_id}") if (county_nam and prec_id) else _norm_token(precinct_name)
        return {
            "county_nam": county_nam,
            "county_norm": county_norm,
            "prec_id": prec_id,
            "precinct_name": precinct_name,
            "precinct_norm": precinct_norm,
            "id": str(row.get("GEOID20") or "").strip() or None,
        }

    # Polygons
    poly = gdf.copy()
    rows = [build_row(r) for _, r in poly.iterrows()]
    for k in rows[0].keys():
        poly[k] = [rr[k] for rr in rows]
    poly = poly[["geometry", "county_nam", "county_norm", "prec_id", "precinct_name", "precinct_norm", "id"]]
    poly = poly.set_geometry("geometry")

    # Centroids: representative points are safer than centroids for concave polygons.
    pts = gdf.copy()
    pts["geometry"] = pts.geometry.representative_point()
    rows2 = [build_row(r) for _, r in pts.iterrows()]
    for k in rows2[0].keys():
        pts[k] = [rr[k] for rr in rows2]
    pts["has_polygon"] = True
    pts = pts[["geometry", "county_nam", "county_norm", "prec_id", "precinct_name", "precinct_norm", "id", "has_polygon"]]
    pts = pts.set_geometry("geometry")

    args.out_polygons.parent.mkdir(parents=True, exist_ok=True)
    args.out_centroids.parent.mkdir(parents=True, exist_ok=True)

    poly.to_file(args.out_polygons, driver="GeoJSON")
    pts.to_file(args.out_centroids, driver="GeoJSON")

    print(f"Wrote polygons:  {args.out_polygons} ({len(poly)} features)")
    print(f"Wrote centroids: {args.out_centroids} ({len(pts)} features)")


if __name__ == "__main__":
    main()
