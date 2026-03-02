"""
Prepare TIGER 2020 Georgia county + VTD GeoJSONs with join keys for this repo.

Inputs (zips in Data/):
  - Data/tl_2020_13_county20.zip
  - Data/tl_2020_13_vtd20.zip

Outputs:
  - Data/tl_2020_13_county20.geojson
  - Data/tl_2020_13_vtd20.geojson

County output adds:
  - county_norm: normalized NAME20
  - join_key: county_norm

VTD output adds:
  - county_norm: from county NAME20 via COUNTYFP20
  - vtd_name_norm: normalized NAME20
  - join_key_name: "<COUNTY_NORM> - <VTD_NAME_NORM>"
  - join_key_code: "<COUNTY_NORM> - <VTDST20>"
  - join_key: join_key_name (default)
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path


def normalize_name_loose(name: str) -> str:
    base = (name or "").replace("\u00a0", " ").strip().upper()
    base = re.sub(r"[^A-Z0-9 .\-]", "", base)
    return re.sub(r"\s+", " ", base).strip()


def strip_leading_code(name: str) -> str:
    s = (name or "").strip()
    if not s:
        return ""
    # Common TIGER VTD naming patterns:
    # - "0110001 - ANDERSON"
    # - "133008 - OLD SALEM"
    # - "01 ANDERSON"
    s = re.sub(r"^\s*[A-Z0-9]{2,10}\s*-\s*", "", s)
    s = re.sub(r"^\s*\d{1,3}\s+", "", s)
    return s.strip()

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--county-zip", type=Path, default=Path("Data/tl_2020_13_county20.zip"))
    ap.add_argument("--vtd-zip", type=Path, default=Path("Data/tl_2020_13_vtd20.zip"))
    ap.add_argument("--out-county", type=Path, default=Path("Data/tl_2020_13_county20.geojson"))
    ap.add_argument("--out-vtd", type=Path, default=Path("Data/tl_2020_13_vtd20.geojson"))
    args = ap.parse_args()

    try:
        import geopandas as gpd
    except Exception as e:  # pragma: no cover
        raise SystemExit(f"geopandas is required: {e}")

    if not args.county_zip.exists():
        raise SystemExit(f"Missing {args.county_zip}")
    if not args.vtd_zip.exists():
        raise SystemExit(f"Missing {args.vtd_zip}")

    county = gpd.read_file(f"zip://{args.county_zip}").to_crs("EPSG:4326")
    if "COUNTYFP20" not in county.columns or "NAME20" not in county.columns:
        raise SystemExit("County zip must include COUNTYFP20 and NAME20.")
    county["county_norm"] = county["NAME20"].astype(str).map(normalize_name_loose)
    county["join_key"] = county["county_norm"]

    county_map = dict(zip(county["COUNTYFP20"].astype(str), county["county_norm"].astype(str)))

    vtd = gpd.read_file(f"zip://{args.vtd_zip}").to_crs("EPSG:4326")
    if "COUNTYFP20" not in vtd.columns or "NAME20" not in vtd.columns or "VTDST20" not in vtd.columns:
        raise SystemExit("VTD zip must include COUNTYFP20, VTDST20, and NAME20.")

    vtd["county_norm"] = vtd["COUNTYFP20"].astype(str).map(lambda c: county_map.get(c, ""))
    vtd["vtd_name_norm_raw"] = vtd["NAME20"].astype(str).map(normalize_name_loose)
    vtd["vtd_name_norm"] = vtd["NAME20"].astype(str).map(strip_leading_code).map(normalize_name_loose)
    vtd["join_key_name_raw"] = (vtd["county_norm"] + " - " + vtd["vtd_name_norm_raw"]).str.replace(
        r"\s+", " ", regex=True
    ).str.strip()
    vtd["join_key_name"] = (vtd["county_norm"] + " - " + vtd["vtd_name_norm"]).str.replace(
        r"\s+", " ", regex=True
    ).str.strip()
    vtd["join_key_code"] = (vtd["county_norm"] + " - " + vtd["VTDST20"].astype(str).str.strip().str.upper()).str.replace(
        r"\s+", " ", regex=True
    ).str.strip()
    vtd["join_key"] = vtd["join_key_name"]

    args.out_county.parent.mkdir(parents=True, exist_ok=True)
    args.out_vtd.parent.mkdir(parents=True, exist_ok=True)
    county.to_file(args.out_county, driver="GeoJSON")
    vtd.to_file(args.out_vtd, driver="GeoJSON")

    print(f"Wrote {args.out_county} ({len(county)} features)")
    print(f"Wrote {args.out_vtd} ({len(vtd)} features)")


if __name__ == "__main__":
    main()
